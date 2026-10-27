<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0009: DynamoDB Local runs every integration test

- Status: Accepted
- Date: 2026-10-26

## Context

Plan D13 promises integration tests that run against any DynamoDB-compatible endpoint, and
names two candidates: DynamoDB Local, AWS's closed-source emulator, and ExtendDB, an
open-source server (Apache-2.0) that stores data in PostgreSQL. M3 must pick which one every
change is tested against (the former spike S7). Neither emulates Global Tables or Lambda
event source mappings, so the multi-region simulation harness and the AWS end-to-end suite
keep those.

What Dynaflux relies on from the endpoint:

- **Keys:** sort keys compared as UTF-8 bytes, which the key encoding is built on
  ([encoding.md §2](../spec/encoding.md#2-tables-and-items)); key limits of 2048 and 1024
  bytes; the 400 KB item limit.
- **Numbers:** exact N values up to 38 digits, from 1e-130 to 9.99…e125, and the stored text
  that readers rescale (§5.2).
- **Conditional writes:** `attribute_not_exists(pk)` and `_v = :v`, the only conditions the
  store sends ([ADR 0004](0004-runtime-storage-boundary.md)). With
  `ReturnValuesOnConditionCheckFailure = ALL_OLD`, a failed condition returns the stored
  item, so a lost race needs no read. The compare-and-set fence of the backfill handoff is
  one such write on a unit item.
- **Batch reads:** `BatchGetItem` returns items in no order, rejects repeated keys, and
  returns `UnprocessedKeys` past 16 MB.
- **Streams:** `NEW_AND_OLD_IMAGES` records, each exactly once and in order within a shard,
  with 21 to 40 digit sequence numbers; no record for a write that changes nothing; TTL
  deletes as `REMOVE` records.
- **Transactions** (M6, `SYNC` views): cancellation reasons that carry the stored item.

The evaluation ran on 2026-10-26, locally in Docker, with loopback endpoints and fake static
credentials. Nothing touched AWS.

- DynamoDB Local 3.3.2, `amazon/dynamodb-local@sha256:6ada631ec7aa5fc83446855eb17233e3dad184fec47477e8140c6c569cfe0c86`,
  run with `-inMemory -disableTelemetry`.
- ExtendDB 0.1.14, `public.ecr.aws/extenddb/extenddb-postgres@sha256:9f54c9f4b7a4b909ada8a5e0c930b4febfb4190d7702821bbb670bcdb1e46087`,
  on PostgreSQL 16.10.
- The client was aws-sdk-go-v2 (`service/dynamodb` v1.72.1, `service/dynamodbstreams`
  v1.46.2), built from an explicit `aws.Config` that refuses any non-loopback host.

A 70-row comparison came out of it. These rows decided the question:

| Behavior | AWS documents | DynamoDB Local | ExtendDB |
|----------|---------------|----------------|----------|
| Sort-key order across ASCII, U+00E9 to U+FFFF, U+10000, U+1F600 and U+10FFFF | UTF-8 bytes ([data types][types]) | UTF-8 bytes | UTF-8 bytes |
| N returned for `12.50`, `1E+2`, `007`, `-0` | normalized | `12.5`, `100`, `7`, `0` | same |
| N range and precision: `1e-130`, `9.99e125`; 39 digits; `1e-131`, `1e126` | in range; rejected | as AWS | as AWS |
| pk 2049 bytes, sk 1025 bytes, item of 409,601 bytes | rejected ([quotas][quotas]) | rejected; limits count bytes | same |
| Failed conditional `PutItem` with `ALL_OLD` | `Item` is the stored item | yes | yes |
| `_v = 5.000` against a stored `5` | numeric comparison | holds | holds |
| Transaction cancellation reasons, `Item` included | as documented ([transact][transact]) | identical | identical |
| **A reader following `NextShardIterator` while 16 writers commit** | each record exactly once, in order per shard ([Streams][streams]) | 0 of 2,880 records missed, 5 runs | **4 to 9 of 2,880 missed in each of 6 runs** |
| U+0000 inside a string | not documented | stored and returned | **HTTP 500** `InternalServerError` |
| Inverted `BETWEEN` | `ValidationException` | `ValidationException` | **empty result** |
| `BatchGetItem` of 100 × 200 KB | partial at 16 MB, `UnprocessedKeys` ([BatchGetItem][batchget]) | 83 items, 17 unprocessed | **100 items, none unprocessed** |
| `BatchGetItem` response order | unordered | unordered | request order |
| Query bound over 1024 bytes | not documented | `ValidationException` | accepted |
| Identical `PutItem` (no change) | no stream record ([Streams][streams]) | none | **`MODIFY` with equal images** |
| Expired item | deleted by TTL, `REMOVE` with `userIdentity` ([TTL and Streams][ttl]) | **never deleted** (0 of 3 in 482 s) | deleted after 14 s and 53 s, `userIdentity` as AWS |
| Shards per stream | one per partition | **one**, AWS-like ID | four fixed shards |
| `ApproximateCreationDateTime` | rounded down to the second ([StreamRecord][streamrecord]) | **rounded down to the minute** | seconds |
| Region in stream ARNs and records | the table's region | **`ddblocal`** | the server's region |
| `TransactionConflictException` under contention | possible | never ([usage notes][usage]) | never |
| Ready after `up`, images cached | — | 0.8–1.4 s | 4.2–4.9 s (four services, credential import, TLS) |
| `CreateTable` to `ACTIVE`; whole probe suite | — | about 10 ms; 7.9 s | about 0.9 s; 17.8 s |
| Telemetry | — | **on by default** ([telemetry][telemetry]) | none found |

ExtendDB's stream loss comes from its design, not from a bug a retry can work around: it
takes each record's sequence number from a PostgreSQL sequence inside the write's
transaction, so transactions commit in a different order than their numbers, and a reader
that has passed number n never sees a record below n that commits later. The projector
would then drop that record as a duplicate, because its high-water marks assume the AWS
guarantee. Its own differences document lists none of the rows marked in bold.

For the archive, D13 names ministack. MiniStack 1.5.32 (MIT) accepts Firehose Direct PUT and
answers `PutRecordBatch` with one result per record. moto returns an empty result list for
Iceberg streams. LocalStack needs an auth token on every tier since release 2026.03.0. No
emulator injects partial `PutRecordBatch` failures.

## Decision

**DynamoDB Local is the only emulator.** Every integration test runs against it, in one
lane: `make integration`, which CI's `integration` job runs on every push and pull
request.

- It passes every row the encoding and the store rely on.
- It delivered every stream record under concurrent writers.
- Its validation is closer to the AWS documentation (inverted `BETWEEN`, `UnprocessedKeys`,
  unordered `BatchGetItem`, sequence-number length), so a backend bug fails here.
- It starts in about a second as a single container with no credentials setup.

**What only ExtendDB produced is tested with crafted events.** The three behaviors
DynamoDB Local lacks matter only as the Lambda event the projector receives, and the
projector's handler is what the suite tests. Each test takes a real record that DynamoDB
Local delivered and changes one field before calling the handler:

- a TTL delete: the `REMOVE` record of a `DeleteItem` with `userIdentity` added as AWS
  documents it ([TTL and Streams][ttl]); the core skips every `REMOVE` anyway;
- several shards per stream: the stream's records delivered as batches with different
  `shardId`s, since a delivery unit is the event's `shardId`;
- a `MODIFY` with equal images: an `INSERT` record turned into a `MODIFY` whose `OldImage`
  equals its `NewImage`.

ExtendDB's records would not be AWS evidence either; that comes only from the AWS
end-to-end suite.

**No capability flags.** With one emulator, the checks of the strict-validation and U+0000
rows are plain tests, and nothing reads a `DYNAFLUX_EMULATOR` variable.

**Rules for every test client:**

- The endpoint comes from `DYNAFLUX_DYNAMODB_ENDPOINT` and must be on a loopback address;
  any other host fails the test before a request is sent.
- Credentials are static and fake, with one fixed region and access key. No test client
  uses the credentials or endpoint that `config.LoadDefaultConfig` finds in `~/.aws` and
  `AWS_*` variables. A child process that must call it gets an environment built from
  scratch, with `AWS_ENDPOINT_URL` set to a closed loopback port so that no service can
  resolve an AWS endpoint.
- Under the `integration` build tag, a missing endpoint fails the test instead of skipping
  it, so a lane can't pass by running nothing.

**Container:** `scripts/emulators/compose.yaml` with DynamoDB Local only, pinned by
digest, its port bound to `127.0.0.1`, run with both `-disableTelemetry` and
`DDB_LOCAL_TELEMETRY=0`.

**Firehose:** no emulator. The writer's logic, including partial failures, is tested with a
fake sender, and the real SDK client against an in-process fake of `PutRecordBatch`. No
emulator injects partial failures, and MiniStack's Iceberg path writes through DuckDB, so
it would say nothing about how Firehose parses Iceberg types; the AWS experiment of the
end-to-end runbook does.

### Rejected

- **ExtendDB as the default:** it loses stream records under concurrent writers, answers a
  valid string containing U+0000 with a 500 (which the store treats as transient, so a
  write would be retried forever), accepts several requests AWS rejects, and takes four
  times as long to start.
- **ExtendDB as a second lane** for TTL deletes, several shards and equal-image `MODIFY`s:
  crafted events through the real handler cover the same code. The lane would cost four
  services, TLS, a credentials script, capability flags, a one-writer rule to avoid its
  own record loss, and a 60 s wait for its TTL sweep. Revisited when its upstream fixes
  the stream ordering.
- **MiniStack** for Firehose: it would only re-test the SDK's `PutRecordBatch`
  serialization, which the in-process fake behind the real client already covers.
- **LocalStack** for Firehose: it requires an auth token. **moto** for Firehose: it returns
  no per-record results for Iceberg streams, so the writer would treat every record as
  failed.

## Consequences

- Tests that touch the bold rows avoid them: the archive's `_arrival` is checked for
  presence on DynamoDB Local's records (minute rounding) and for seconds on a crafted
  record, and no test reads a stream record's region.
- DynamoDB Local closes and splits no shards, raises no `TransactionConflictException`,
  and runs no event source mapping. Shard lineage and conflicts stay with the simulation
  harness and the AWS end-to-end suite. Lambda's delivery rules are reproduced by explicit
  handler calls over `internal/esmlocal`, a reader of the emulator's stream that only tests
  and `dynaflux dev` use; the rules come from the AWS documentation and the measured
  results of [ADR 0003](0003-shard-identity-from-tumbling-window.md).
- When the emulator and the AWS documentation disagree, or neither settles a behavior,
  the item goes to the maintainer's one-shot run on real DynamoDB: U+0000 in a string, Query
  bounds over 1024 bytes and keys over 2048 bytes, the N text returned for `1e-130`,
  `9.99e125` and for numbers nested in lists and maps, two actions on one item in a
  transaction, whether the store's item size bounds DynamoDB's own, and the precision of
  `ApproximateCreationDateTime` in the Lambda event.
- Without `-sharedDb`, DynamoDB Local keeps one database per client region and access key
  (a `us-west-2` client gets `ResourceNotFoundException` for a table created through
  `us-east-1`). So every test client, and every child process, uses the suite's one region
  and key. A test that needs a second origin region sets the engine's region option over
  the same client, never a client of another region. The M4 end-to-end suite's emulator
  mode uses the same effect on purpose: two client regions give two regions separate data.
- ExtendDB issues worth reporting upstream, most important first: the lost stream records,
  the 500 on U+0000 and on a fractional TTL value, the empty result of an inverted `BETWEEN`,
  the ignored 16 MB `BatchGetItem` limit, and the `MODIFY` record for a write that changes
  nothing.
- AWS's usage notes say DynamoDB Local ignores `Scan`'s `Segment` and `TotalSegments`
  ([usage notes][usage]). Version 3.3.2 splits them as AWS documents: the store suite's
  segment check, each item in exactly one of 2 and of 5 segments, passes on it, so it runs
  there like every other check.
- DynamoDB Local numbers the stream records of every table in one sequence, and a shard
  iterator it returned for a shard holding no record fails a later `GetRecords` with
  `TrimmedDataAccessException` once another table's record took the next number.
  `internal/esmlocal` takes a new `TRIM_HORIZON` iterator on each read until the shard
  yields a record.
- DynamoDB Local is closed source and changes without notice. The digest pin keeps the lane
  reproducible, and a version bump reruns the fidelity tests of `internal/store/dynamo`.

[types]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
[quotas]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html
[transact]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
[streams]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
[batchget]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
[ttl]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/time-to-live-ttl-streams.html
[streamrecord]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamRecord.html
[usage]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html
[telemetry]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocalTelemetry.html

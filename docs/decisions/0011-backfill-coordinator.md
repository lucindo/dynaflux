<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0011: Backfills run as a resumable step, driven by Step Functions, with their state in the meta table

- Status: Accepted
- Date: 2026-10-26

## Context

A new view version is filled by a backfill while live projection runs. The protocol is
settled and proven in the simulation harness: the fenced handoff of spike S1b as amended
by spike S9's rule 4, which applies every kept record under one high-water-mark namespace
`B`, in one fixed order by (delivery unit key, sequence number), a chunk at a time, each
chunk only after every row of the one before it succeeded. In M2 the coordinator that
drives it lives only in the harness (`internal/simulation/backfill.go`). M5 needs it in
production, where:

- **A backfill outlives any invocation.** A Lambda invocation lasts at most 900 seconds. A
  year of archive at the plan's 1,000 events per second is about 31.5 billion records; with
  64 rows written at once and about 15 ms per read-modify-write, 1 billion records whose
  events touch 200 million rows take about 13 hours.
- **Workers can overlap.** Stopping a Step Functions execution doesn't stop a Lambda
  invocation already running; a step retried after a lost response runs twice; a
  continuation can start while its predecessor's last step still runs.
- **No source returns the backfill's order.** Athena's `UNLOAD` writes several files, each
  sorted only within itself ([UNLOAD][unload]); a `SELECT … ORDER BY` over the order's major
  key, the unit hash, can't prune the `day(event_time)` partitions, so slicing it to fit
  Athena's query timeout costs one full scan per slice ([quotas][athena-quota]). A `Scan`
  returns items in hash order.
- **A stream-table item carries no delivery unit or sequence number.** Both are positions
  in one region's change stream; `Scan` and export return only attributes. So the fence,
  which is a sequence number, can't split table items between the backfill and live
  projection. The plan accepted "an approximate boundary" there, whose error goes both ways
  (events counted twice and events lost, each about the projector's backlog times the event
  rate).
- **Step Functions limits:** an execution history holds at most 25,000 events, state input
  and output at most 256 KiB, and a failed execution can be redriven for 14 days
  ([quotas][sfn-quotas], [redrive][sfn-redrive]). `StartExecution` with the name and input
  of a running Standard execution is idempotent ([StartExecution][sfn-start]).
- **DynamoDB Local** has no point-in-time recovery, so no export ([usage notes][ddblocal]).
  The notes also say it ignores `Segment` and `TotalSegments`, but version 3.3.2 splits
  them ([ADR 0009](0009-emulator-choice.md)). `dynaflux dev` and the integration suite run
  on it.

The M2 harness found one sharp rule while this was researched:
with the real `ApplyBackfill` under write faults, three workers walking the same order with
different chunk sizes are exact in 200 of 200 seeds when each starts at a completed prefix.
A worker that walks its chunks backwards undercounts in 140–145 seeds, and workers that
start from a cursor saved before its chunk finished undercount in 159.

## Decision

**1. One resumable step.** A new package, `internal/backfill`, exports

```go
func Step(ctx context.Context, env *Env, version string, attempt int, execution string) (wait time.Duration, done bool, err error)
```

Each call reads the version's job item from the region's meta table, does as much of the
current phase as fits before `ctx`'s deadline less a reserve, writes what it achieved, and
returns. All state lives in the job item, the version's state item and staged files;
nothing lives in the caller.
The Lambda backfill mode, `dynaflux dev` (in process) and the simulation harness drive the
same `Step`.

Phases, in order: `activate`, `fence` (archive sources), `fresh` (archive sources,
[ADR 0012](0012-view-version-activation.md)), `watermark` (archive) or `wait` (table),
`stage`, `apply`, `live`. Each writes its facts once. With the fence split, `apply` reads
each unit's fence, consistently, when the merge first reaches the unit's records: after the
export, so every unit with an archived record that a projector applied the version to has
its fence by then. Fences never change, so no `select` phase stores them; a stored copy
could also be overwritten by a stale worker after a restage.

**2. Step Functions drives it.** Each region's `regional.yaml` gets a Standard state machine
`{prefix}{store}-backfill`: `Step` (a `lambda:invoke` of the function
`{prefix}{store}-backfill` with `{job, attempt}` and the execution's ARN), then `done` →
succeed; 1,000 steps → start a continuation of the same machine and end; `wait_seconds` >
0 → `Wait` → `Step`; else `Step`. A loop iteration with a wait records about 10 history
events, so 1,000 steps leave room for retries under the 25,000-event limit. The input
carries `steps: 0` from the start: JSONata's `steps + 1` on a missing field is undefined,
not an error, and the loop would never continue. Lambda service errors are retried 6 times
from 2 s, other errors 5 times from 30 s. Executions are named
`<view>-v<N>-<h>-a<attempt>` (`-c<k>` for continuations), where `<h>` is 6 hex characters
of a hash of the version and the job's creation time: a job re-created after its item was
lost never meets a name used in the last 90 days, which Standard executions refuse
([StartExecution][sfn-start]). A hash of the version replaces the view when the name,
with room for a 3-digit attempt and a 5-digit continuation, would pass 80 characters.
`Step` writes the ARN of the execution that ran it to the job item when it changed, so a
`SUCCEEDED` execution of a running job points to its continuation.

**3. The job item is the record.** One item per version per region (`pk` `job`, `sk` the
version ID, [encoding.md §8.3](../spec/encoding.md#83-job-item)), kept after the job ends.
It holds the source and split, `state` (`running`, `cancelled`, `done`, `broken`),
`phase`, `attempt`, the execution, the facts each phase established (cut, staging
location, committed runs, counts), the cursor, the rate and the last error. Every write is
conditional on `_v`. The split is written on the version's state item in the same write
that makes it `activating` ([ADR 0012](0012-view-version-activation.md)), and a later job
takes it from there, so losing the job item can't change it. A `cancelled` job keeps its
item; only `done` and `broken` ones expire, after 400 days.

- **Resume** starts attempt + 1 from the cursor in a new execution; `RedriveExecution` isn't
  used, so a resume works after 14 days and with a changed state machine.
- **Cancel** sets `cancelled`, then calls `StopExecution`. A step that finds `cancelled`, or
  an attempt that isn't its own, ends with `done`.
- **No lease.** The attempt check only saves work. Correctness rests on the completed-prefix
  rule: a worker starts at the cursor, and the cursor moves only after every row of a chunk
  succeeded. Any number of workers, with any chunk sizes, then keep the handoff's
  invariant: a row whose `B` mark is m holds every kept record for it up to m.

**4. Order through sorted runs.** The `stage` phase turns source input into runs: records
sorted by their `B` mark in a buffer of at most 512 MB, written as independently compressed
blocks of about 256 KB with an index of each block's first mark, under
`backfill/<version>/<created_at>/` in the region's bucket. Each run gets a unique name and
counts only once the job item lists it, in the same conditional write that moves the
source position: a stale worker, whose run extents depend on its deadline and on pages that
TTL deletes move, can then add garbage but never replace a committed run or leave a range
in none. The `apply` phase merges the runs, drops repeated records, applies the split, and
hands `projector.ApplyBackfill` what the split keeps of each 2,000 records it takes from the
merge, so the cursor moves past skipped records as often as past applied ones. The cursor is
the mark of the last record of the last completed chunk; a step seeks each run past it
through the block index. A merge holds one decoded block per input, so its fan-in comes from
the step's memory, at most 128 on Lambda; with more inputs, `apply` first runs resumable
merge passes whose outputs and position the job item records. A lifecycle rule expires staged objects
after 30 days and their older versions (the bucket is versioned) after one day; a job that
finds a run gone stages again, which the cursor makes safe.

- **Merge passes.** A run is written as a stream of blocks, its index last, so neither a
  staging buffer nor a pass's output has to fit in memory. A pass commits an output run every
  buffer's worth, or at the step's cutoff, with the last mark it holds. A pass's outputs
  follow one another in mark order, so a merge reads them as one chain, one block at a time:
  each pass leaves fewer chains however many steps it spans. Passes merge the chains with the
  fewest records first, so no record is rewritten much more often than others.
- **Resuming an export (M5).** The job item has no archive resume point until M6. A staged
  run's name ends with `-f<n>`: the committed runs hold the export's first n files in full,
  and staging resumes at file n. Records of a file that a run holds in part are read again;
  the merge drops the repeats. A run is committed only while every run its count rests on is
  still listed, so a stale step can't claim files whose runs a restage dropped.
- **A failed chunk** is applied again up to three times within the step before the step
  fails: rows skip what their marks hold, and a new step would read the job and open the
  runs again.

**5. Sources.**

- **Archive:** read through a `backfill.Archive` interface. Records are identified by
  (`_shard`, `_seq`), not by `event_id`: a `DEDUP BY` event appended again after its item
  expired is a second record, which live projection also counts. The read waits for the
  archive's watermark to pass the cut. M5 proves this path in the simulation harness and
  against DynamoDB Local; on AWS it uses it only for additive versions of fresh streams,
  which need no read, and a job whose stream got an event before its fresh check waits at
  its watermark for M6, its execution ended without the alarm (`waits` in its item). An
  idempotent version reads the table of a fresh stream: it goes live as soon, and needs no
  archive when an event comes first. The
  Athena reader (sliced `UNLOAD`s of the Iceberg table, each slice retried into a new empty
  prefix, plus the records under `errors/firehose/<stream>/`) and the `DataFreshness`
  watermark are M6's, as the plan schedules them.
- **Table:** a consistent parallel `Scan` of the region's replica. It costs about $3.3 per
  100 GB read against $10 for an export, needs no point-in-time recovery, starts at once,
  and runs on DynamoDB Local. A scan's resume point per segment is in the job item.

**6. Splits.**

- **Fence**, for every archive source: S1b with S9 rule 4. Every version is fenced, the
  idempotent ones too ([ADR 0012](0012-view-version-activation.md)), and the backfill keeps
  archived records at or below each unit's fence.
- **Time**, for an additive version read from the table: the job records a split time
  `B` = `activated_at` + 90 s + ε in the version's state item at activation. Live projection
  applies the version only to `INSERT`s with `_it` (`ingest_time`) > `B`; the backfill only
  to table items with `_it` ≤ `B`, read no earlier than `B` + ε + L + 60 s. Both sides judge
  the same attribute of the same event, so nothing is applied twice. With ε = 5 minutes
  (writer clocks) and L = 15 minutes (an event lands in the region within L of its `_it`),
  nothing is missed either. The projector reports a breach of L: an `INSERT` with `_it` ≤ `B`
  in a batch it starts at or after `B` + ε + L by its own clock opens the version's breaker
  with cause `split`. The scan starts 60 s after that bound, so an item it missed landed
  after the scan began and is handled after the bound, whatever the record's
  `ApproximateCreationDateTime` means for a replica, which AWS doesn't document; the cost
  is a break when the projector is about 20 minutes behind during the activation. A
  `MODIFY` whose old and new images have `_it` on different sides of `B` breaks the version
  too: one `DEDUP BY` event appended in two regions, whose stream `INSERT` and surviving
  table item fall on different sides. The backfill's mark for these records is
  (`event_time`, `event_id`), which fits the `B` encoding as it is; a record's `SPLIT` item
  is `bucket(event_id, n)`.
- **None**, for an idempotent version read from the table: the backfill applies every item,
  read no earlier than `activated_at` + 90 s; repeats change nothing. A version with
  `uniqExact` counts as additive here (lifecycle.md §1): its value counts only for the record
  whose marker holds it, so a marker that a live attempt wrote and no retry finished would
  leave the value uncounted.

**7. The function.** `{prefix}{store}-backfill`, `DYNAFLUX_MODE=backfill`, 900 seconds,
3,008 MB, applying at most 64 rows at once, and pausing while the region's projector is
more than 5 minutes behind: a new on-demand view table starts at about 4,000 writes per
second, and the projector writes to it too. Its timeout doesn't depend on `W`: S9's proof
bounds the lifetime of invocations that skip records by progress and then write live
high-water marks, and a backfill worker does neither; a stale write of a row fails its `_v`
condition. A step holds three quarters of the function's memory
(`AWS_LAMBDA_FUNCTION_MEMORY_SIZE`), and writes each run with one `PutObject` from memory,
so the role has no multipart grants. The function takes its bucket's account from the
invoked function's ARN. Its role may list the bucket only under `backfill/`, and S3 may
answer such a role's `GetObject` of a missing key with 403 instead of 404, so the function
lists the key before it calls a run missing and stages again.

**Rejected:**

- **The cursor in the execution payload** (M4's draft): `status`, `-watch` and a resume
  can't see it, and two copies of a cursor drift.
- **Step Functions Distributed Map over chunks:** parallel chunks break S9 rule 4's single
  order.
- **The CLI as the driver:** a backfill of hours would depend on the operator's machine.
- **A container task (ECS, Fargate):** a new service dependency for a loop that
  Lambda and Step Functions already run.
- **A lease with expiry:** the completed-prefix rule already makes overlapping workers safe;
  a lease would add a clock-dependent rule to prove.
- **Export to S3 as the table source:** it costs more, has no completion-time promise
  ([export][export]), needs a second parser, and doesn't run on DynamoDB Local. It stays an
  option for very large tables whose read capacity can't be spared.
- **Athena's sort:** see the context; it stays the fallback if staging proves too slow.
- **The plan's approximate boundary** for the table: errors in both directions that nothing
  reports.
- **Sharded `B` namespaces** (several independent orders per version, for speed): needs its
  own harness proof, and a v1 store at typical rates backfills in hours.

## Consequences

- The harness drives `backfill.Step` instead of its own coordinator. Its controls of the
  coordinator (C7, C14, C28, C35, C36, C53) mutate `internal/backfill`, and new controls
  cover the cursor (saved before its chunk finished, C66), the order (a worker of a resumed
  attempt walking backwards, C67), the error prefix (C68), the fresh rule (C56–C58), runs
  rewritten instead of committed, each bound of the time split, and a table source not split
  read before every projector knows the version is activating (C73, by a scripted test; the
  `guards-table` scenario runs older builds through table-source activations, and catches it
  in a few seeds of a hundred). C66–C68 are m5-plan's C49–C51, whose numbers earlier controls
  took.
- The time split changes the projector: a version's state item gains `split_at`, and live
  projection has one more rule. It needs a harness scenario with writer clock offsets and
  late landings before it lands. Writer clocks are an assumption the projector can't check;
  the plan's "approximate boundary" becomes "a time split, exact while writer clocks stay
  within 5 minutes; a late event breaks the version instead of being lost silently".
- `regional.yaml` gains, in every region, the backfill function, its role and log group,
  the state machine, its role and log group, the `backfill-failed` alarm, a dashboard
  widget and a bucket lifecycle rule for `backfill/`; M6 adds the backfill workgroup. The
  backfill role lists every stream and view table by exact ARN, so the statements that
  grow go in managed policies of their own, each checked against IAM's 6,144 characters.
- A backfill at the plan's ceiling takes days. The e2e suite measures apply throughput; if
  it is too slow, sharded namespaces get a harness spike before v1.
- The archive's JSON forms in `UNLOAD` output (timestamps, decimals, arrays) and
  `DataFreshness` on an idle stream are confirmed on AWS by the maintainer's e2e run before
  M6 calls the archive source done.
- `dev` scans with one segment, and the emulator suite drives `Step` against DynamoDB Local
  with a local directory for staging.

[unload]: https://docs.aws.amazon.com/athena/latest/ug/unload.html
[athena-quota]: https://docs.aws.amazon.com/athena/latest/ug/service-limits.html
[sfn-quotas]: https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html
[sfn-redrive]: https://docs.aws.amazon.com/step-functions/latest/dg/redrive-executions.html
[sfn-start]: https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html
[ddblocal]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html
[export]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html

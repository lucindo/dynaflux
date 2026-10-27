<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# End-to-end suite

The end-to-end suite runs the [two-region example](../../examples/analytics/README.md), with
one view added, and checks it as an application sees it. It has two modes:

- **Emulator mode** runs on DynamoDB Local, in seconds. Anyone can run it.
- **AWS mode** deploys the store to a sandbox AWS account, runs the scenarios against it,
  and tears it down. It covers what no local test can: CloudFormation, IAM, the event source
  mapping, Global Table replication, Firehose, Iceberg, Athena and the alarms. Only the
  maintainer runs it, with their own credentials. AI assistants and CI never do.

| Path | What it is |
|------|------------|
| `examples/analytics/e2e/` | The suite: Go tests behind the `e2e` build tag, with the flag `-target=emulator` or `-target=aws`. |
| `examples/analytics/e2e/testdata/e2e.sql` | The view `e2e_ratio`, which a page view with `duration_ms` 0 breaks (scenario 5). The example itself doesn't have it. |
| `examples/analytics/e2e/testdata/queries.sql` | Queries that read the view columns the example's queries leave out. The suite runs them; nothing deploys them. |
| `examples/analytics/e2e/fis.yaml` | The AWS FIS experiment that pauses replication (scenario 3). Never generated for users. |
| `scripts/e2e/deploy.sh`, `teardown.sh` | Deploy and delete one AWS run. |

The suite lives in the example's tree because it uses the example's generated client, and Go
lets only packages under `examples/analytics` import it.

## 1. Prerequisites

For emulator mode: Docker, and the Go version in `go.mod`.

For AWS mode:

- A sandbox AWS account that holds no store you care about: teardown deletes resources by
  name. Set `E2E_ACCOUNT` to its ID: the scripts and the suite stop when the credentials are
  for another account.
- Credentials in the default chain (`AWS_PROFILE`, environment variables or SSO) for a role
  that can:
  - deploy CloudFormation stacks that create IAM roles, Lambda functions, DynamoDB tables,
    S3 buckets, Firehose streams, Athena workgroups, CloudWatch alarms, dashboards and log
    groups, and AWS FIS experiment templates;
  - run the suite's checks: DynamoDB reads and writes on the store's tables, `Scan` and
    `DeleteItem` included, and DynamoDB Streams reads; Lambda `GetFunctionConfiguration`
    and `ListEventSourceMappings`; CloudWatch Logs `FilterLogEvents`; CloudWatch
    `DescribeAlarmHistory`; Athena queries in the store's workgroup, with the Glue and S3
    reads they need; Glue `GetTable` and `GetTableOptimizer`; Lake Formation
    `GetDataLakeSettings`; Firehose `DescribeDeliveryStream`; IAM `SimulatePrincipalPolicy`,
    `PutRolePolicy`, `GetRolePolicy` and `DeleteRolePolicy`; FIS experiments; and STS
    `GetCallerIdentity`;
  - tear down: DynamoDB `UpdateTable` and `DeleteTable`; Glue `GetTables`,
    `DeleteTableOptimizer`, `GetDatabase` and `DeleteDatabase`; and S3 object versions.

  An administrator role in the sandbox covers all of them.
- Lake Formation at its defaults: `IAM_ALLOWED_PRINCIPALS` has `ALL` on new databases and
  tables, so plain IAM access to the Glue Data Catalog works
  ([infra.md §11](../spec/infra.md#11-deploying)). Scenario 18 checks it.
- The example's regions, `us-east-1` (the home region) and `us-west-2`. Both have Firehose
  delivery to Iceberg and Glue table optimizers.
- The SAM CLI, the AWS CLI v2, the Go version in `go.mod`, and network access for Go
  modules. The scripts run on the bash that macOS ships.

**Before the first run:** confirm the FIS target key for the replication pause.
`aws fis get-action --id aws:dynamodb:global-table-pause-replication --region us-west-2`
should list a target named `Tables`, which `fis.yaml` uses. Record the answer in
[§10](#10-results).

## 2. Emulator mode

```sh
make e2e-emulator
```

It starts DynamoDB Local unless it runs, and leaves it running for other work. Then it runs
`go test -race -tags e2e ./examples/analytics/e2e -target=emulator`:

- Each region of the store is a database of its own in one DynamoDB Local, which keeps a
  database per client region. Nothing replicates between them.
- The tables come from the manifest that the templates are rendered from, so the templates
  and the runtime must agree on keys, `_ttl` and the stream view.
- On the new store, every view version is set live with `meta.SetLive` before the first
  append. Until the backfill arrives in M5, nothing else does that.
- The projector is the Lambda binary itself: the test binary runs `runtime.Run`, as the
  generated `main.go` does, in a child process per region, with the environment the
  template gives the function. A fake Lambda Runtime API hands it the events that
  `internal/esmlocal` reads from the stream. A fake Firehose keeps the archive records.
- Nothing injects faults, so every invocation must succeed the first time, with no failed
  records. The child has the race detector, as the test binary does, and stops at the
  first race.
- It runs scenarios 1, 4, 5 and 8 ([§8](#8-coded-scenarios)). It doesn't cover
  CloudFormation, IAM, the event source mapping, tumbling windows, replication, Firehose,
  Iceberg, Athena or the alarms.

## 3. AWS mode

```sh
make e2e-aws E2E_RUN=<id> E2E_ACCOUNT=<sandbox account ID>
```

`<id>` is 1 to 8 lowercase letters or digits, new for each run: every resource name starts
with `e2e-<id>-`. The target first checks both variables and that the credentials are for
`E2E_ACCOUNT`. Then it runs three steps, and runs the last one even after a failure:

1. `scripts/e2e/deploy.sh`:
   1. copies the example's `dynaflux.yaml`, lock and SQL to a temporary directory, or to
      `E2E_WORK` ([§9](#9-manual-experiments)), adds `e2e.sql`, and makes it a Go module
      that uses this checkout of Dynaflux;
   2. runs `dynaflux generate` there, then `sam build` with `CGO_ENABLED=0` and
      `GOFLAGS=-tags=lambda.norpc`;
   3. deploys `global.yaml` in `us-east-1` with `Prefix=e2e-<id>-`, without the stack
      policy, so that teardown can delete the stack;
   4. deploys `regional.yaml` in `us-east-1` and `us-west-2`;
   5. deploys `fis.yaml` in `us-west-2`.
2. `go test -tags e2e -timeout 120m -v ./examples/analytics/e2e -target=aws`, with
   `E2E_RUN` and `E2E_ACCOUNT` set. It first checks the account, and that each region was
   deployed from the same sources: the projector's `DYNAFLUX_ARCHIVE_SCHEMA` and the view
   tables. Then it sets the view versions live on the new store, as emulator mode does.
3. `scripts/e2e/teardown.sh` ([§5](#5-teardown)).

To run the steps one at a time, for example to rerun one scenario:

```sh
export E2E_RUN=a1 E2E_ACCOUNT=<sandbox account ID>
scripts/e2e/deploy.sh
go test -tags e2e -timeout 120m -v ./examples/analytics/e2e -target=aws -run 'TestE2E/1_appends'
scripts/e2e/teardown.sh
```

A deployed store keeps what earlier runs did to it, so run the scenarios once per
`E2E_RUN`. Scenario 5 in particular fails on a second run: `e2e_ratio` stays broken by the
first run's event.

A full run takes about 90 minutes: about 15 to deploy, about 65 for the scenarios (scenario
3 alone takes 20, and scenarios 4 and 8 wait for Firehose's 5-minute buffer), and 10 to 20
to tear down.

## 4. Watch

- CloudFormation events in both regions during the deploys.
- `sam build` output: one Go build for all functions. Record it in [§10](#10-results).
- The dashboards `e2e-<id>-analytics-us-east-1` and `e2e-<id>-analytics-us-west-2`.
- The FIS console in `us-west-2` during scenario 3.
- Test output: each scenario is a subtest, `TestE2E/<number>_<name>`, and each failure names
  the region and what differed. Scenario 2 logs a count to record.

## 5. Teardown

`scripts/e2e/teardown.sh` runs after every `make e2e-aws`. Run it by hand, with the same
`E2E_RUN` and `E2E_ACCOUNT`, after an interrupted run. It skips what is already gone, and
waits for what is already being deleted, so it can run again after a failure. It works in
every region that holds a replica of a stream table, so a third region that experiment 17
added goes too. In order, it:

1. removes the inline policy `dynaflux-e2e-deny` from the projector role, if an interrupted
   scenario 23 left it; stops a replication pause that an interrupted scenario 3 left
   pending or running, and waits until it ends, because FIS denies replication on the
   tables while it lasts;
2. deletes the regional stacks, then the global stack, then the FIS stack;
3. deletes what the stacks retain: for each stream table, it turns off deletion protection
   on every replica, removes the other replicas, then deletes the table in `us-east-1`; in
   each region, it deletes the Glue table optimizers, then the Glue database, which takes
   its tables and views with it, then empties the bucket, versions and delete markers
   included, and deletes it;
4. deletes the scratch table of [§9](#9-manual-experiments)'s checks;
5. fails if a table whose name starts with `e2e-<id>-` is left in any region.

Afterwards, nothing whose name starts with `e2e-<id>-` (or `e2e_<id>_` in Glue) is left.
Teardown leaves alone what other stacks share: the Glue optimizer log groups
(`/aws-glue/iceberg-compaction/logs`, `/aws-glue/iceberg-retention/logs`) and the
deployment bucket of SAM's `aws-sam-cli-managed-default` stack.

## 6. What a failure means

| Failure | Likely cause |
|---------|--------------|
| A script or the suite stops at once: "the AWS credentials are for account …" | The default credential chain points at an account other than `E2E_ACCOUNT`: check `AWS_PROFILE` and the environment. |
| Emulator mode: "an invocation failed", or "reported failed records" | The projector returned an error or a partial reply with no fault injected. Its output follows the message. |
| Emulator mode: "the projector exited", with `WARNING: DATA RACE` in its output | A data race in the projector or the runtime. |
| `deploy.sh`: a stream lookup fails with "deploy `global.yaml` first" | The global stack is missing in that region, or was deployed with another `Prefix`. |
| The suite stops at once: the projector's `DYNAFLUX_ARCHIVE_SCHEMA` differs, or a view table is missing | The stacks were deployed from another checkout, or without `e2e.sql`. Deploy and test from the same checkout, with `deploy.sh`. |
| A region's views differ from the oracle after convergence (1, 3, 4, 5, 23) | A projector bug, or records the projector missed. Keep the function logs and the meta table before teardown. |
| Scenario 4: the views lost an event | The projector applied a `REMOVE` record. |
| Scenario 5: `e2e_ratio` isn't broken, or another version is | The breaker didn't open, or opened on the wrong view. The meta table's view-state items say which event broke what. |
| Scenario 5: the `views-broken` alarm never went into `ALARM` | The `BreakerOpened` metric didn't arrive: check the projector's `batch metrics` log line and its embedded metric format. |
| Scenario 5, emulator mode: the projector's `WARN` and `ERROR` lines aren't one `batch metrics` line with `BreakerOpened` 1 | The breaker's metric line is missing or wrong, or something else failed: the lines follow the message. |
| Scenario 2: "none of 10 … pairs was stored in both regions" | Replication was faster than the two appends. Rerun the scenario. |
| Scenario 2: "the stream read ended before the scenario's records" | `GetRecords` returned more empty pages in a row than the read allows (`emptyPages`). Rerun the scenario. |
| Scenario 2: no `DivergentDuplicates` line | The projector didn't count the replicated `MODIFY` as a divergent duplicate. |
| Scenario 3: no experiment template, or the experiment fails | `fis.yaml` wasn't deployed, its target key is wrong ([§1](#1-prerequisites)), or the FIS role lacks a permission: the experiment's state names it. |
| Scenario 3: a region shows the other's appends during the pause | Replication went on: the pause didn't take, or took longer than 5 minutes to start. |
| Scenario 6: a mapping differs from the expected settings | The template or CloudFormation changed a fixed value of [infra.md §8](../spec/infra.md#8-fixed-values). |
| Scenario 7: an access denial | A grant in [infra.md §9](../spec/infra.md#9-iam) is missing, or an SCP, a resource policy or a permissions boundary denies it. Record the action and the role. |
| Scenario 7: an optimizer is missing | The setup function couldn't create it: look for its error in the setup function's log. |
| Scenario 8: the archive differs, or stays empty | The archive's column types and the projector's records disagree, or Firehose can't write: check the Firehose log group, `errors/firehose/` in the bucket and the `failed-rows` alarm. A difference only in how a value reads may be the suite's text for that type (`archive_test.go`). |
| Scenario 8: a `_shard` or a unit item names no real shard | The event source mapping doesn't pass the stream's `ShardId` as `shardId` ([ADR 0003](../decisions/0003-shard-identity-from-tumbling-window.md)). |
| Scenario 4 or 8: a copy's `_arrival` is outside the run, its `_seq` isn't digits, or an event has two records in one shard | The projector's archive record is wrong: the arrival's unit, the sequence number, or a copy of a record that isn't the event's `INSERT`. |
| Scenario 18: Lake Formation's default permissions lack `IAM_ALLOWED_PRINCIPALS`, `glue:GetTable` isn't allowed, or a table is registered with Lake Formation | The account's Lake Formation settings govern the archive ([§1](#1-prerequisites)), or the Firehose role's grants are missing. |
| Scenario 23: no `rows failed` or `batch failed` line | IAM took longer than the 2-minute windows to apply the deny, or the projector handled the failures without logging them. |
| `teardown.sh` stops on a waiter | Deleting a replica or a stack can take longer than the AWS CLI waits. Run the script again. |
| `teardown.sh`: "Tables of run … are left" | A table outside the stacks and the stream tables, such as a replica of a failed run whose table in `us-east-1` is gone. Delete it by hand. |
| `teardown.sh`: an experiment "is still … after 10 minutes" | The replication pause didn't stop. Stop it in the AWS FIS console, then run the script again. |
| `teardown.sh` stops on `DELETE_FAILED` | Look at the stack's events, delete what blocks it by hand, and run the script again. |

## 7. Cost

A few dollars per run. AWS FIS bills per action-minute, about 10 minutes per run; check its
price page before the first run. The tables are on demand, the Firehose and Athena volumes
are tiny, and the optimizers run on little data. Experiments 15 and 16 run for hours and
cost more Lambda time.

## 8. Coded scenarios

Every scenario appends through the example's generated client and feeds the same events to
an in-memory store with the same catalog, the oracle (`fluxtest`). After convergence, each
region's query results must equal its oracle's, for the example's queries and those of
`testdata/queries.sql`, which together read every view column: in AWS mode, every append
reaches every region; in emulator mode, only its own. Numbers are stable identifiers; gaps are scenarios
that were dropped from the design or became manual experiments. AWS mode runs them in the
order of the table: scenario 7 reads the logs of the ones before it, and scenario 23 denies
the projector on purpose.

| # | Scenario | Asserts | Emulator | AWS |
|---|----------|---------|----------|-----|
| 6 | Mapping settings | Exactly one mapping per stream table in each region, to the projector, and each mapping's settings equal the fixed values of [infra.md §8](../spec/infra.md#8-fixed-values). | — | yes |
| 18 | Lake Formation precondition | Lake Formation's defaults give `IAM_ALLOWED_PRINCIPALS` `ALL` on new databases and tables, IAM allows the Firehose role `glue:GetTable` on the catalog, the database and each archive table, and no table is registered with Lake Formation. | — | yes |
| 1 | Appends in every region | Each region's views equal the oracle. | yes | yes |
| 4 | `DeleteItem` on a stream item | The `REMOVE` record is skipped: the views equal the oracle, which never deleted anything, the stream lost the event, and the archive keeps it with no copy from the `REMOVE`. | yes | yes |
| 5 | A poison event (zero divisor) | That event breaks `e2e_ratio`, every other version stays live and exact, and the shard goes on. On AWS, the `views-broken` alarm goes into `ALARM`. In emulator mode, the projector's one `WARN` or `ERROR` line is the `batch metrics` line with `BreakerOpened` 1. | yes | yes |
| 2 | One dedup key in both regions at once, 10 times each way | With the same payload, the views count it once. With different payloads, each region counts one of them once, and a `DivergentDuplicates` metric line appears. It logs how many `MODIFY` records hold equal payloads (all but `_r`, `_it` and `_ttl`) for the same-payload keys that both regions stored. | — | yes |
| 8 | Archive contents | Each region's archive (`<stream>_dedup` through Athena; the fake Firehose locally) holds each event of its stream table once, with every column as stored: strings, `Nullable`, `Array`, `UUID`, `UInt32` and the time columns. Every copy has a `_shard`, a `_seq` of 40 digits, one per event in its shard, and an `_arrival` during the run; in emulator mode, each event has one copy. Every `_shard`, and every unit item's ID, is a `ShardId` that `DescribeStream` lists. | yes | yes |
| 7 | Least privilege | No `AccessDenied` in any function or Firehose log, other than scenario 23's deny, Athena DDL and optimizer creation included. Both optimizers exist on every archive table, so `iam:PassRole` worked. | — | yes |
| 3 | Replication pause: FIS `aws:dynamodb:global-table-pause-replication` for 10 minutes | 5.5 minutes after the action starts (replication can go on for 5 minutes), appends in both regions: before the pause ends, each region's views show only its own, and its stream table lacks the other's. After the pause, both converge. | — | yes |
| 23 | Partial failures and function errors under the 900-second window | With steady appends, a deny of `PutItem` on one view table for 2 minutes (row failures, so partial replies), then of the meta table for 2 minutes (function errors, so retries with bisection). The views end equal to the oracle, and the projector's log has `rows failed` (`WARN`, with `failedSeq`) and `batch failed` (`ERROR`) lines. | — | yes |

## 9. Manual experiments

Each experiment runs once, or again when a Dynaflux change touches what it measures. Record
the answer with the template in [§10](#10-results), then in the ADR or specification named
there. Run them on a store deployed with `scripts/e2e/deploy.sh`, unless the steps say
otherwise, and tear it down with `scripts/e2e/teardown.sh`.

Experiments 10, 17, 19, 20, 21 and 22 change the generated project and deploy it again. For
them, set `E2E_WORK` to a new or empty directory when you deploy: `deploy.sh` keeps the
project there, instead of in a temporary directory that it deletes, and prints where it is.
Their steps run in that directory, the kept project. There, `bin/dynaflux` is the build of
this checkout, and these commands generate, build and deploy as `deploy.sh` does:

```sh
export E2E_RUN=a1 E2E_ACCOUNT=<sandbox account ID>
E2E_WORK=<dir> scripts/e2e/deploy.sh
cd <dir>
bin/dynaflux generate
CGO_ENABLED=0 GOFLAGS=-tags=lambda.norpc sam build -t infra/dynaflux/regional.yaml
sam deploy -t infra/dynaflux/global.yaml --stack-name "e2e-$E2E_RUN-analytics-global" \
  --region us-east-1 --resolve-s3 --parameter-overrides "Prefix=e2e-$E2E_RUN-" \
  --no-fail-on-empty-changeset
sam deploy --stack-name "e2e-$E2E_RUN-analytics-regional" --region <region> \
  --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --resolve-s3 \
  --parameter-overrides "Prefix=e2e-$E2E_RUN-" --no-fail-on-empty-changeset
```

`sam deploy` without `-t` deploys what `sam build` last built, so build again after each
change to `regional.yaml`. The project doesn't depend on the run, because `Prefix` is a
deploy parameter. `teardown.sh` leaves the directory: delete it yourself.

| # | Experiment | Steps | Answers |
|---|------------|-------|---------|
| 10 | Schema evolution | In the kept project, add a column with a `DEFAULT` to the e2e stream, run `generate`, build, and deploy `regional.yaml` in one region. Append events with the new column. | `ArchiveSchema` finishes before the projector update; the time from the deploy to the first delivered value; older rows read the `DEFAULT` in `<stream>_dedup`. |
| 13 | Firehose error output | Turn on CloudTrail S3 data events for the bucket. Put a record whose value doesn't match its column type straight into a Firehose stream. | Where it lands under `errors/firehose/<stream>/`; `failed-rows` fires; whether the Firehose role ever calls `s3:DeleteObject`. |
| 14 | Dedup view pruning | Run `EXPLAIN ANALYZE` on a `<stream>_dedup` query with and without an `event_time` filter. | Whether the filter cuts the data scanned. |
| 15 | Tumbling window across a shard rollover | Append one event every few seconds for 5 hours. | Every invocation's `shardId` is a real shard (`DescribeStream`); the views equal the oracle. |
| 16 | Stuck shard past 24 hours | In one region, add a statement that denies the projector role `dynamodb:PutItem` on its view tables. Keep appending for 25 hours, then remove the statement. | `gap-risk` fires after 6 hours; whether trimmed records reach `aws/lambda/<mapping UUID>/` with the record age unlimited; the projector resumes; what `dynaflux status` (M5) must report. |
| 17 | Add and remove a region | In the kept project, add a third region with Firehose Iceberg delivery and Glue optimizers, such as `eu-west-1`, and follow [infra.md §12](../spec/infra.md#12-changes-after-the-first-deploy). Time the replica creation. Then remove it, first without `--unprotect-region`: `generate` refuses (DF6018), so move the old `global.yaml` out of `infra.out` before running it. | The new region's views need a backfill; whether the replica's initial copy appears in its stream; whether deletion protection blocks the removal (if it doesn't, `--unprotect-region` goes). |
| 19 | Flip `split` under load | While appending, change `runtime.split` from `store` to `stream` in the kept project, generate, and deploy `regional.yaml`. | The views equal the oracle; `progress` never moves backward; whether the old and new functions process the same shard at overlapping times. |
| 20 | In-place Firehose update | Change a Firehose stream's buffering in the kept project's `infra/dynaflux/regional.yaml`, build, and deploy it. | The update succeeds on a stream created with `AppendOnly`, without a replacement. |
| 21 | Regional before global | With a new `E2E_RUN`, deploy `regional.yaml` from an earlier run's kept project before `global.yaml`. Then deploy `global.yaml` and `regional.yaml` again. | The stream lookup fails; the rollback leaves no bucket or Glue database; the retry succeeds. |
| 22 | Re-create and re-archive | Run a query in the archive workgroup. Delete one regional stack, then create it again from the kept project with `--import-existing-resources`. Then take that region out of `archive.regions` there, generate and deploy; put it back, generate and deploy. | The delete finishes, the workgroup and its query history with it; the new stack adopts the retained bucket; `ArchiveSchema` goes on with the retained Glue database and tables after the re-create and after the archive comes back. |

### One-shot checks of the backend and the runtime

These settle what neither DynamoDB Local nor the AWS documentation does
([ADR 0009](../decisions/0009-emulator-choice.md), [ADR 0010](../decisions/0010-runtime-entry-point.md)).
Checks B1 to B6 and B8 need only a scratch table with string keys `pk` and `sk`:

```sh
aws dynamodb create-table --table-name e2e-<id>-scratch --region us-east-1 --billing-mode PAY_PER_REQUEST \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

`teardown.sh` deletes it with the run. The other checks need a deployed store, or a scratch
function for B11.

| # | Check | Steps | Answers | Record in |
|---|-------|-------|---------|-----------|
| B1 | Strings and numbers | Put items whose `sk` values hold characters from several Unicode planes, and query them in order. Put the `N` values `12.50`, `100`, `1E+2`, `1e-130` and `9.99e125`, alone and inside `L` and `M` values, and read them back. | The sort order across planes; the `N` text that DynamoDB returns for each value. | [encoding.md §5.2](../spec/encoding.md#52-exact-numbers), ADR 0009 |
| B2 | U+0000 | Put an item with U+0000 inside a string attribute, and one with it inside `sk`. | Whether each is accepted and read back unchanged. | encoding.md, ADR 0009 |
| B3 | Key size limits | Query with a sort-key bound over 1,024 bytes, and with a partition key over 2,048 bytes; `GetItem` with each oversized key. | The error of each, or that it is accepted. | [encoding.md §3.6](../spec/encoding.md#36-size-limits) |
| B4 | One item twice in a transaction | `TransactWriteItems` with two actions on one item. | The error. | ADR 0009 |
| B5 | `ApproximateCreationDateTime` | Put an item and read its record with `GetRecords`; compare with a record in a projector's Lambda event on a deployed store. | The precision in each. | ADR 0009, [ADR 0003](../decisions/0003-shard-identity-from-tumbling-window.md) |
| B6 | Query page size | Put about 2 MB of items under one `pk` and query it. | Items per page near 1 MB (informational). | encoding.md |
| B7 | `DescribeTable` in a replica region | On a deployed store, `aws dynamodb describe-table` on a stream table in `us-west-2`. | Whether `Replicas` lists `us-west-2` itself. | [infra.md §6.2](../spec/infra.md#62-stream-arn-lookup) |
| B8 | Glue optimizer with a new role | `aws glue create-table-optimizer` naming a role created seconds before. | Glue's error code and message, which the setup function retries as access denied. | [ADR 0008](../decisions/0008-archive-schema-migration.md) |
| B9 | Firehose encodings (E1 additions) | On a deployed store, put records straight into a Firehose stream: `Int64` max as a string, a missing key and an explicit `null`, a 1,024,001-byte record, and 4 records of 1,000 KiB in one `PutRecordBatch`. `Decimal(38,10)` and `Array(Bool)` need a scratch store with such columns. | What each reads as in Athena, and which calls fail. | [infra.md §7.3](../spec/infra.md#73-tables-and-columns), ADR 0008 |
| B10 | Item size bound | `PutItem` items whose `store.ItemSize` is exactly 409,600 bytes, with deep `L` and `M` nesting, many `N` values and long attribute names. | Whether all succeed. If one fails, `store.ItemSize` isn't an upper bound, and a row near the limit gets an invalid-request error instead of `RowTooLarge`. | [encoding.md §7.8](../spec/encoding.md#78-size-bound) |
| B11 | Clocks across a freeze | Deploy a scratch Go function on `provided.al2023` whose handler logs `time.Since` of a package-level `time.Now()` (monotonic), the same with `.Round(0)` (wall clock), and its `Lambda-Runtime-Deadline-Ms`. Invoke it twice, about 5 minutes apart. | Whether the monotonic and wall-clock differences agree with the difference of the deadlines. | ADR 0010 |
| B12 | Metrics at a raised log level | Deploy a new run, set the projector's application log level to `ERROR` (`aws lambda update-function-configuration --logging-config LogFormat=JSON,ApplicationLogLevel=ERROR`), and run only scenario 5 (`-run 'TestE2E/5_poison'`). | Whether the `BreakerOpened` datapoint still arrives: the scenario's alarm check passes. | ADR 0010 |

## 10. Results

Copy this block for each experiment or check into the results log of the release being
tested:

```markdown
### Experiment <#>: <name>

- Date, Dynaflux commit, account type, regions:
- Steps run, and any change to them:
- Observed (numbers, log lines, screenshots):
- Answer:
- Recorded in (ADR or spec section):
```

The open questions, and what answers each one:

| Question | Answered by | Answer | Recorded in |
|----------|-------------|--------|-------------|
| Is the FIS target key of the replication pause `Tables`? | Prerequisites | | `fis.yaml` |
| Does replication write a `MODIFY` record with equal payloads for one dedup key appended in both regions with the same payload? | Scenario 2's log line | | [encoding.md §6](../spec/encoding.md#6-stream-items) |
| Do records trimmed after a 24-hour stall reach `aws/lambda/` with the record age unlimited? | 16 | | [ADR 0003](../decisions/0003-shard-identity-from-tumbling-window.md) |
| How long does Firehose take to deliver an added column? | 10 | | [ADR 0008](../decisions/0008-archive-schema-migration.md) |
| Does Firehose write with `AppendOnly` to a table that Athena created, with the archive's value encodings? | 8, B9 | | ADR 0008 |
| Where do failed records land under `errors/firehose/`, and does Firehose use `s3:DeleteObject`? | 13 | | [infra.md §9.3](../spec/infra.md#93-firehose-role) |
| Does an in-place update of a Firehose stream's Iceberg settings work with `AppendOnly`? | 20 | | [infra.md §7.4](../spec/infra.md#74-firehose-streams) |
| Does `<stream>_dedup` prune partitions on an `event_time` filter? | 14 | | [infra.md §7.3](../spec/infra.md#73-tables-and-columns) |
| Does deletion protection block removing a replica? | 17 | | [infra.md §13](../spec/infra.md#13-checks-in-generate) |
| Does a new replica's initial copy appear in its stream? | 17 | | backfill (M5) |
| Can the old function's last batch overlap the new function's first after a `split` change? | 19 | | [infra.md §12](../spec/infra.md#12-changes-after-the-first-deploy) |
| Does a failed first regional deploy roll back cleanly? | 21 | | infra.md §12 |
| Does a regional stack delete finish with the workgroup, and does a new stack adopt the retained bucket and go on with the Glue database? | 22 | | infra.md §12 |
| Does `sam build` compile the binary once for all functions? | Every run | | [infra.md §6.1](../spec/infra.md#61-functions) |
| Do the setup role's grants cover the optimizer calls, `iam:PassRole` included? | 7 | | [infra.md §9.2](../spec/infra.md#92-setup-role) |
| How does DynamoDB sort strings across planes, and what `N` text does it return? | B1 | | encoding.md, [ADR 0009](../decisions/0009-emulator-choice.md) |
| Does DynamoDB accept U+0000 in a string and in a key? | B2 | | ADR 0009 |
| What do oversized keys and Query bounds return? | B3 | | encoding.md §3.6 |
| What does a transaction with two actions on one item return? | B4 | | ADR 0009 |
| What is the precision of `ApproximateCreationDateTime` in `GetRecords` and in the Lambda event? | B5 | | ADR 0009 |
| Does `DescribeTable` in a replica region list that region in `Replicas`? | B7 | | infra.md §6.2 |
| What error does Glue return for an optimizer role it can't assume yet? | B8 | | ADR 0008 |
| Does `store.ItemSize` bound DynamoDB's own item size? | B10 | | encoding.md §7.8 |
| Do the monotonic and wall clocks agree with the deadlines across a freeze? | B11 | | [ADR 0010](../decisions/0010-runtime-entry-point.md) |
| Does a `BreakerOpened` datapoint arrive with the application log level at `ERROR`? | B12 | | ADR 0010 |

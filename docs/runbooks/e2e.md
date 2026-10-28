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
    S3 buckets, Firehose streams, Athena workgroups, Step Functions state machines,
    CloudWatch alarms, dashboards and log groups, and AWS FIS experiment templates, and for
    experiments Q1 and Q2 API Gateway APIs, ACM certificates, and Route 53 records and
    health checks;
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
- For scenario 24 and the lifecycle experiments, a role you can assume that has only the
  grants of [infra.md §9.8](../spec/infra.md#98-operators), to run the `dynaflux` commands.
- For scenarios 34 and 35, `E2E_APP_ROLE`: the ARN of a role in the sandbox account that
  your credentials can assume, with at least DynamoDB read and write access to the store's
  tables, such as `AmazonDynamoDBFullAccess`. The suite assumes it with the application
  policy of the [example's README](../../examples/analytics/README.md#permissions) as the
  session policy, with the run's account and prefix, so the two scenarios run with that
  policy's grants and nothing more. The role serves every run.
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
  append, in place of a backfill.
- The projector is the Lambda binary itself: the test binary runs `runtime.Run`, as the
  generated `main.go` does, in a child process per region, with the environment the
  template gives the function. A fake Lambda Runtime API hands it the events that
  `internal/esmlocal` reads from the stream. A fake Firehose keeps the archive records.
- Nothing injects faults, so every invocation must succeed the first time, with no failed
  records. The child has the race detector, as the test binary does, and stops at the
  first race.
- It runs scenarios 1, 4, 5, 8, 34 and 35 ([§8](#8-coded-scenarios)). It doesn't cover
  CloudFormation, IAM, the event source mapping, tumbling windows, replication, Firehose,
  Iceberg, Athena or the alarms. DynamoDB Local never cancels a transaction for a
  conflict, so scenario 34's appends retry only on failed conditions.

## 3. AWS mode

```sh
make e2e-aws E2E_RUN=<id> E2E_ACCOUNT=<sandbox account ID> E2E_APP_ROLE=<role ARN>
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
   `E2E_RUN`, `E2E_ACCOUNT` and `E2E_APP_ROLE` set. It first checks the account, and that
   each region was deployed from the same sources: the projector's
   `DYNAFLUX_ARCHIVE_SCHEMA` and the view tables. Then it sets the view versions live on
   the new store, as emulator mode does.
3. `scripts/e2e/teardown.sh` ([§5](#5-teardown)).

To run the steps one at a time, for example to rerun one scenario:

```sh
export E2E_RUN=a1 E2E_ACCOUNT=<sandbox account ID> E2E_APP_ROLE=<role ARN>
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
  the region and what differed. Scenarios 2, 34 and 35 log counts and times to record.
- In CloudWatch, the `ConsumedWriteCapacityUnits` and `TransactionConflict` metrics of the
  `quota_usage` table during scenario 34, and the meta table's `ConsumedWriteCapacityUnits`
  during scenario 35. Record them in [§10](#10-results).

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
| A region's views differ from the oracle after convergence (1, 3, 4, 5, 23, 34, 35) | A projector bug, or records the projector missed. Keep the function logs and the meta table before teardown. |
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
| Scenario 34 or 35 stops at once: "E2E_APP_ROLE is …" | The variable is unset, or names a role in another account ([§1](#1-prerequisites)). |
| Scenario 34 or 35: `AccessDenied` from `AssumeRole`, an append or a query | Your credentials can't assume `E2E_APP_ROLE`, the role lacks a grant, or a statement of the README's policy is missing or too narrow. Record the action and the ARN. |
| Scenario 34: a charge fails 3 times | Every append of the charge ran out of attempts on the hot row. The error names the row, and the log line's cancellation reasons show why. |
| Scenario 34: `QuotaUsed` misses charges made in its region | A `SYNC` append didn't apply the view in its transaction, or the query didn't read strongly consistently. |
| Scenario 34: no `AccessDeniedException` for the `PutItem` or the `Query` | The `EnclosingOperation` or `LeadingKeys` condition doesn't limit what [infra.md §9.9](../spec/infra.md#99-applications) says it does. |
| Scenario 35: `ErrWaitTimeout` | The projector wrote no receipt within 10 seconds: check its log, and that the event has `_rcpt`. |
| Scenario 36: an `AccessDenied` of the backfill role in CloudTrail, or a job broken with Athena error 234 or 1700 | A statement of the backfill role is missing or too narrow, or Lake Formation governs the archive's catalog ([infra.md §9.6](../spec/infra.md#96-backfill-role)). Record the action and the ARN. |
| Scenario 36: a job failed because the archive isn't complete | `status` names the rule that failed (DF7219). A gap or a hole the scenario didn't make means a deploy, a projector or Firehose lost records. |
| Scenario 36: a version reads the table where the archive was complete | `backfill` recorded no origin on the first deploy (DF7218), or a rule failed: `status` shows which. |
| A `dynaflux` command fails with `AccessDenied` under the operator role | A row of [infra.md §9.8](../spec/infra.md#98-operators) is missing or too narrow. Record the command, the action and the ARN. |
| `backfill-failed` fires outside scenario 24's deny | A backfill step failed after its retries. `dynaflux status` shows the job interrupted (DF7216), and the backfill function's log has the error. |
| `teardown.sh` stops on a waiter | Deleting a replica or a stack can take longer than the AWS CLI waits. Run the script again. |
| `teardown.sh`: "Tables of run … are left" | A table outside the stacks and the stream tables, such as a replica of a failed run whose table in `us-east-1` is gone. Delete it by hand. |
| `teardown.sh`: an experiment "is still … after 10 minutes" | The replication pause didn't stop. Stop it in the AWS FIS console, then run the script again. |
| `teardown.sh` stops on `DELETE_FAILED` | Look at the stack's events, delete what blocks it by hand, and run the script again. |

## 7. Cost

A few dollars per run. AWS FIS bills per action-minute, about 10 minutes per run; check its
price page before the first run. The tables are on demand, the Firehose and Athena volumes
are tiny, and the optimizers run on little data. Experiments 15 and 16 run for hours and
cost more Lambda time. Scenario 24 and experiments 28 and 30 add the backfill function's
time at 3,008 MB and a few hundred thousand writes. Scenario 36 and experiments 37 to 44
add Athena's $5 per TB scanned: cents on the e2e archive, and a few dollars for
experiment 39's synthetic table. Experiments Q1 and Q2 add the hosted zone's monthly fee
and the health checks' fees for the hours they run.

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
| 34 | `SYNC` appends to a hot row, as the application | In each region at once, 4 workers charge one account 25 times each with `AppendAPICall`, as the application with the README's policy, and send a charge again after an error. Right after, each region's `QuotaUsed` counts every charge made there (in emulator mode, exactly those), and a charge sent again gets `ErrDuplicate`. On AWS, `PutItem` on a `quota_usage` table outside a transaction, and a `Query` of the meta table's `catalog` partition, get `AccessDeniedException`. The views converge to the oracle. It logs the charges' latencies, how many the application sent again, the `TransactWriteItems` calls and their cancellation reasons by code and by whether each holds `Item`, and on AWS the projector's `rows failed` lines. | yes | yes |
| 35 | Waits, as the application | In each region, 20 views of a page of its own with `RecordPageView` from the example's `app.go`, which appends each with `Waitable` and runs `PageStats` with `WaitFor` and a 10-second timeout, as the application with the README's policy. Each returns the page's day with every view so far, and the views converge to the oracle. It logs how long each took, against the batching window. | yes | yes |
| 7 | Least privilege | No `AccessDenied` in any function or Firehose log, other than scenario 23's deny, Athena DDL and optimizer creation included. Both optimizers exist on every archive table, so `iam:PassRole` worked. | — | yes |
| 3 | Replication pause: FIS `aws:dynamodb:global-table-pause-replication` for 10 minutes | 5.5 minutes after the action starts (replication can go on for 5 minutes), appends in both regions: before the pause ends, each region's views show only its own, and its stream table lacks the other's. After the pause, both converge. | — | yes |
| 23 | Partial failures and function errors under the 900-second window | With steady appends, a deny of `PutItem` on one view table for 2 minutes (row failures, so partial replies), then of the meta table for 2 minutes (function errors, so retries with bisection). The views end equal to the oracle, and the projector's log has `rows failed` (`WARN`, with `failedSeq`) and `batch failed` (`ERROR`) lines. | — | yes |

M5 changes the suite as below ([ADR 0012](../decisions/0012-view-version-activation.md)),
and none of it is coded yet. `deploy.sh` gets a step after deploying `regional.yaml` that
runs `dynaflux backfill -all -yes -prefix e2e-<id>-`: the store has no events yet, so each
job makes its version live by the fresh rule, without a read
([lifecycle.md §4.3](../spec/lifecycle.md#43-fresh-streams)). The suite then stops setting
the versions live in AWS mode, scenario 1 asserts what the backfill did, and scenario 24
joins the table.

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | Appends in both regions, after the first deploy's `backfill -all` | Every version is `live` in both regions, by the fresh rule where the region archives the stream with `events: all` (the job item says `fresh`); the views equal the oracle in each region. |
| 24 | Backfill under traffic. While appends go on in both regions: deploy `regional.yaml` again with `testdata/backfill.sql`, which adds an additive and an idempotent view of the e2e stream; `dynaflux backfill -all -yes -rate 20 -source table`, so that applying takes minutes (without `-source table`, both would read the complete archive, as in scenario 36); during `apply`, deny the backfill role `dynamodb:PutItem` on one new view table for 1 minute, then again until the execution fails (up to about 16 minutes of retries), then remove the deny and run `backfill` again; `backfill -cancel` during another job's `apply`, then `backfill` to resume it; `backfill -rate 200` on a running job. | Both versions go `live` in both regions from the stream table, the additive one by the time split, about 30 minutes after its activation. The 1-minute deny fails no execution. The long one fails the execution: `backfill-failed` fires, and `status` shows the job interrupted (DF7216) with the command that resumes it. After `-cancel`, the job is `cancelled` and the version `activating`. `status` shows the new rate. The views of both versions equal the oracle in each region. |

M6 adds a scenario that reads the archive ([ADR 0016](../decisions/0016-archive-export-with-athena.md),
[ADR 0017](../decisions/0017-archive-completeness.md)), not coded yet either. Run every
`dynaflux` command in it with the operator role of [§1](#1-prerequisites). The backfill
function, not the operator, calls Athena: Athena then reads the Glue catalog and the archive,
and writes the export, with the backfill role's credentials, which no grants test sees. So
turn on CloudTrail data events for the bucket before it, and record every `AccessDenied`
event of the backfill role ([infra.md §9.6](../spec/infra.md#96-backfill-role)).

| # | Scenario | Asserts |
|---|----------|---------|
| 36 | Archive backfill. On a new run, `dynaflux backfill -all -yes` right after the first deploy records each stream's origin in both regions. Append for an hour in both regions, and delete a sample of stream items by hand, as TTL would. Rebuild an additive and an idempotent view of the e2e stream (`dynaflux generate -rebuild`), deploy `regional.yaml`, and run `dynaflux backfill -all -yes` while appends go on. During a slice's export, run `backfill -cancel`, then `backfill` again. Then take `us-west-2` out of `archive.regions`, generate and deploy, put it back, generate and deploy; rebuild a view again, deploy, and run `backfill -all -yes`, then `backfill -source archive <view>` in `us-west-2`. | `status` shows each stream's archive "complete since" its origin in both regions before the hole. Both new versions go `live` in both regions by the archive source, split by fences; their views equal the oracle, the deleted events included. `-cancel` stops the export's query (`StopQueryExecution`), and the job resumes with a new try. `status` shows the bytes each job scanned, about the size of the days it exported, and its job item's `errors.foreign` is 0. CloudTrail has no `AccessDenied` for the backfill role. After the hole, `status` shows `us-west-2`'s archive "not complete" with the deploy's time, the new version fills from the table there and from the archive in `us-east-1`, and `-source archive` in `us-west-2` is refused (DF7503). |

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

### Lifecycle and operations

These check the view lifecycle and the operator commands
([lifecycle.md](../spec/lifecycle.md)). Run every `dynaflux` command in them, and in
scenario 24, with the operator role of [§1](#1-prerequisites): an `AccessDenied` is a
finding for [infra.md §9.8](../spec/infra.md#98-operators).

| # | Experiment | Steps | Answers |
|---|------------|-------|---------|
| 25 | Retire in two environments | Deploy a second run from the same project, with a new `E2E_RUN`. Change a view so that a new version is current, generate, deploy both runs and backfill the new version in both. Run `dynaflux retire <view>@v1` for the first run's prefix only, generate, and deploy `regional.yaml` in both regions of the first run, then of the second, with `dynaflux status` on the second run during its update. | In the first run, CloudFormation deletes the version's table at the end of the update, and no shard blocks (no DF7209, no lag). In the second, the version stays `live` until its own deploy: the new build's projectors, or the end of the update, set it `retired`, and its table goes. |
| 26 | Rollback of a catalog-changing deploy | In experiment 25, before the second run's deploy, with appends stopped: deploy its `regional.yaml` in one region, and cancel the update (`aws cloudformation cancel-update-stack`) once `Catalog` is updated. Read `catalog/current` and the retired version's state in the meta table, and the setup function's log. Then go on with experiment 25. | `current` returns to the old catalog's hash, and the version that the new build lists as retired stays `live`; the requests that the rollback sends `Catalog`, in order. |
| 27 | `status` with a foreign and a disabled mapping | Create an event source mapping from one stream table's stream to a scratch function, and disable Dynaflux's mapping of another stream (`aws lambda update-event-source-mapping --no-enabled`). Run `dynaflux status`. Undo both, and run it again. | DF7206 names the scratch function and DF7208 the disabled mapping, with exit status 1; after the undo, neither. |
| 28 | Import with a re-run | Write 100,000 events of `api_calls`, which has `DEDUP BY`, as JSON Lines with `event_time` over the last week. Run `dynaflux import api_calls -file <file> -yes` in one region, then the same command again. | The first run stores 100,000 events, and the second finds each one already present and stores none; the views equal the oracle in both regions; each run's time and consumed write units. |
| 29 | `StartExecution` idempotency | Start the backfill state machine by hand with a new name and the input of a job that ended, twice in a row, and again after the execution ended; then with that name and another input. | Whether a start with the same name and input returns the first execution's ARN, while it runs and after it ended; the error with another input. |
| 30 | Backfill speed and cost | After experiment 28, add a view of `api_calls` with a row per `request_id`, generate, deploy and backfill it. Read the job's progress in `dynaflux status`, and the stream and view tables' consumed capacity and throttles in CloudWatch. | Rows applied per second, at most 64 at once; whether the new on-demand view table throttles, and from what rate; the scan's read units against the table's size divided by 4 KB, as [lifecycle.md §5.4](../spec/lifecycle.md#54-jobs) estimates. |
| 31 | Replicated landing time | Append events in `us-east-1`, and poll a consistent `GetItem` of each in `us-west-2` until it appears. Read their records in `us-west-2`'s stream with `GetRecords`. | How each record's `ApproximateCreationDateTime` compares with when its item first appeared in `us-west-2`: whether the projector's late check could judge by that attribute instead of its own clock. |
| 32 | Staged runs gone | Backfill a new version with `-rate 10`. During `apply`, delete the job's objects under `backfill/` in the bucket. | Whether S3 answers the backfill role's `GetObject` with 404 or 403; that the job stages again and the version goes `live`. |
| 33 | State machine definition | Run `aws stepfunctions test-state` on the backfill state machine's `Route` state with a step's output at 1,000 steps, with a wait above 0, and when done; and on `Continue` with the input of a job that ended. | `Route` goes to `Continue`, `Wait` and `Done`; the continuation's name is `<name>-a<attempt>-c<k+1>`, and its input has all five fields, with `steps` 0. |

### Archive backfill

These check what [ADR 0016](../decisions/0016-archive-export-with-athena.md) assumes of
Athena, S3 and CloudWatch. Run them after scenario 36, on its store: take each statement
from the backfill workgroup's query history (`aws athena list-query-executions --work-group
e2e-<id>-analytics-backfill`), and run it in that workgroup, with your own credentials.
Only scenario 36 checks the backfill role's grants, since only the deployed backfill
function runs with them.

| # | Experiment | Steps | Answers |
|---|------------|-------|---------|
| 37 | `UNLOAD` value forms | Run the export statement of one slice, and the same with `SELECT *` in place of its projection, each into a new prefix. Read the files and the data manifest. | The JSON text of each type: times with 6 fractional digits, decimals and doubles exact, a `NULL` written or left out, the key names; the manifest's form and place; gzip. Record real files for the adapter tests. |
| 38 | Partition listing | Run the `"<stream>$partitions"` statement. | The name and form of the day partition, and how its sizes compare with the objects in S3. |
| 39 | Slice speed | On a synthetic table, export slices of about 10, 50 and 100 GB of Parquet. | Seconds per GB, to set the slice target and see its margin under the 30-minute query timeout. |
| 40 | Token memory | Start an export with a `ClientRequestToken` used before: after it succeeded, after it failed, a day later, and with another statement. | Whether Athena returns the first query's ID, and for how long; the error for another statement. |
| 41 | Idle freshness | Stop appending to one stream for an hour, then append one event. Put a record whose value doesn't fit its column straight into its Firehose stream. | Whether `DeliveryToIceberg.DataFreshness` has datapoints while Firehose holds nothing; whether the error object is written before the freshness passes the failed record. |
| 42 | Error objects | After experiment 41, list `errors/firehose/<stream>/` in the bucket and read the object. | The key layout, the compression, the field names, and whether `rawData` is the bytes sent. |
| 43 | Cancel an export | Run `aws athena stop-query-execution` while an export runs, and on a query that has finished. | The files a stopped export leaves, whether it writes a manifest, and what stopping a finished query returns. |
| 44 | Concurrent exports | Start 21 exports at once in one region, with Athena's default quota of 20 queries. | The `TooManyRequestsException` and its `Reason`. |
| 45 | Rollback order | In the kept project, take `us-west-2` out of `archive.regions`, generate, and deploy `regional.yaml` there; cancel the update (`aws cloudformation cancel-update-stack`) once a projector function is updated. Read the setup function's log, the stream item, and the projector functions' `LastModified`. | Whether CloudFormation rolls back `ArchiveMode` before or after the projector functions, and how far apart: [ADR 0017](../decisions/0017-archive-completeness.md) §3 item 4 waits 16 minutes after the last hole, which covers either order when they are less than a minute apart. |
| 46 | Writes without records | In an hour with no other write to `api_calls`, run experiment 28's second import again. Read that hour's `ConsumedWriteCapacityUnits` of the stream table by `Source`, `ConditionalCheckFailedRequests`, and `SuccessfulRequestLatency` `SampleCount` by `Operation`; and the same for an hour of scenario 34's `SYNC` appends. | Whether conditional writes that fail consume write capacity, which makes an hour no projector needs to follow look silent to [ADR 0017](../decisions/0017-archive-completeness.md) §4 item 3; and whether DynamoDB reports `TransactWriteItems` per table, which counting writes by successful requests would need. |

### Query front doors

These check the doors of `query.yaml` ([infra.md §6.6](../spec/infra.md#66-query-function),
[http.md](../spec/http.md)). Run them in a kept project whose `dynaflux.yaml` gets the
`http` section that each step names, and whose `queries.sql` exposes a query. After each
change, generate, then build and deploy `query.yaml` in both regions as
[infra.md §11](../spec/infra.md#11-deploying) says. Steps with a custom domain need a public
Route 53 hosted zone in the sandbox account, and `HostedZoneId` and `DomainName` in
`--parameter-overrides`; use a subdomain that nothing else uses.

| # | Experiment | Steps | Answers |
|---|------------|-------|---------|
| Q1 | Each door end to end | (1) `function_url`, `iam`: call each region's `QueryEndpoint` with the signed Python client of [http.md §9](../spec/http.md#9-a-python-client), and invoke the function directly. (2) `http_api`, `jwt`, with `http.domain`: a Cognito user pool with the resource server scope `dynaflux/query` in `jwt.scopes` and a client-credentials app client in `jwt.audience`; call the domain with an access token, then with an ID token. Read each health check with `aws route53 get-health-check-status`. Deploy a new version of the exposed query's view, backfill it with `-region us-east-1` only, and deploy `query.yaml` in both regions; then backfill `us-west-2`. During that window, send 50 requests a second for a minute to `us-west-2`'s own `/health`. (3) `rest_api`, `api_key`: a key generated in `us-east-1` and imported into `us-west-2` ([http.md §8.4](../spec/http.md#84-api-keys)); call each region's endpoint with it, and without it. With `usage_plan: {rate: 1, burst: 2}`, send 10 requests a second with the key; then set the plan's burst to 0 (`aws apigateway update-usage-plan`, path `/throttle/burstLimit`) and call again. | Each door answers the same bodies as direct invoke, and the stages' access logs have a line per request, so they take the log group ARN that ends in `:*`; an ID token is refused when `jwt.scopes` is set; the health checks are healthy while the views are live; how long after its new version is deployed `us-west-2` leaves the domain, and returns after its backfill; how many of the 50 requests a second get `429`, and whether the health check stays healthy meanwhile: it fails the experiment if `us-west-2` leaves the domain during the flood ([http.md §6](../spec/http.md#6-health)); a call without the key gets `403`; about 1 request a second passes the usage plan, the rest get `429`; whether a burst of 0 refuses every request. |
| Q2 | Door edge cases | (1) Set the query function's `DYNAFLUX_CATALOG_HASH` to another value (`aws lambda update-function-configuration`), so that its Init fails, and call each door of Q1; deploy `query.yaml` again after. (2) With `http.domain`, delete `us-west-2`'s query stack, then list the hosted zone's records and describe `us-east-1`'s certificate. (3) `rest_api`, `cognito`, with a user pool in `us-east-1`: call `us-west-2` with an ID token; then set both `cognito.scopes` and `cognito.app_clients`, and call with an access token and with an ID token. (4) `http_api`, `iam`: sign a request with SigV4a (botocore's `SigV4AsymAuth`, region set `*`). | Each door's status and body for a crashed invocation; whether the certificate validation record stays, and `us-east-1`'s certificate with it; whether a pool of another region works; which token passes with both Cognito settings; that API Gateway refuses SigV4a, which DF6608 assumes. |

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
| Do the operator grants cover each command? | 24–33, 36 | | [infra.md §9.8](../spec/infra.md#98-operators) |
| Does a rollback make the old catalog current again, retiring nothing? What does it send `Catalog`? | 26 | | [infra.md §6.4](../spec/infra.md#64-catalog-registration) |
| Does `StartExecution` with the name and input of an existing execution return it? | 29 | | [ADR 0011](../decisions/0011-backfill-coordinator.md) |
| How fast does a backfill apply, does a new view table throttle, and what does the scan read? | 30 | | [lifecycle.md §5.4](../spec/lifecycle.md#54-jobs) |
| Does a replicated record's `ApproximateCreationDateTime` follow its arrival in the replica? | 31 | | ADR 0011 |
| Does S3 answer the backfill role's `GetObject` of a missing staged run with 404 or 403? | 32 | | [infra.md §9.6](../spec/infra.md#96-backfill-role) |
| Does the backfill state machine route and continue as written? | 24, 33 | | [infra.md §6.5](../spec/infra.md#65-backfill) |
| Does each door answer as http.md says, and does a stage take a log group ARN that ends in `:*`? | Q1 | | [infra.md §6.6](../spec/infra.md#66-query-function) |
| Does a usage plan throttle API key callers, and does one with a burst of 0 refuse everything? | Q1 | | [infra.md §16](../spec/infra.md#16-to-be-confirmed-on-aws) |
| Do the health checks follow the views, and does flooding `/health` take a region out of the domain? | Q1 | | infra.md §6.6 |
| What does each door answer for a crashed invocation? | Q2 | | [http.md §5](../spec/http.md#5-errors) |
| Does deleting one region's query stack remove the shared certificate validation record? | Q2 | | infra.md §6.6, [§15](../spec/infra.md#15-not-in-these-templates-yet) |
| Does a REST Cognito authorizer take a pool of another region, and which tokens pass with both scopes and app clients? | Q2 | | [http.md §8.5](../spec/http.md#85-cognito) |
| Does API Gateway refuse SigV4a? | Q2 | | [ADR 0018](../decisions/0018-query-handler-and-front-doors.md) |
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
| How long do `SYNC` appends to a hot row take, how often does DynamoDB cancel them, and does it return `Item` in every cancellation reason? | 34's log lines | | [ADR 0014](../decisions/0014-sync-views-in-the-append-transaction.md) |
| Does the projector's retry in place absorb its conflicts with `SYNC` appends? | 34's count of `rows failed` lines | | ADR 0014 |
| Does `dynamodb:EnclosingOperation` limit an application's `PutItem` on view tables to its transactions? | 34 | | [infra.md §9.9](../spec/infra.md#99-applications) |
| How long does a wait take, against the batching window? | 35's log lines | | [ADR 0015](../decisions/0015-wait-tokens-and-receipts.md) |
| What do `SYNC` appends and receipts cost in write units? | 34, 35 and the metrics of [§4](#4-watch) | | ADR 0014, ADR 0015 |
| Do the backfill role's grants cover the reads and writes Athena makes for it? | 36 | | [infra.md §9.6](../spec/infra.md#96-backfill-role) |
| Does a backfill split by fences fill a version exactly from the archive, and does a hole make the next one read the table? | 36 | | [ADR 0016](../decisions/0016-archive-export-with-athena.md), [ADR 0017](../decisions/0017-archive-completeness.md) |
| What text does `UNLOAD` write for each type, with and without the projection? | 37 | | ADR 0016 |
| What do `$partitions` rows hold? | 38 | | ADR 0016 |
| How fast does an export run, and what slice target fits the query timeout? | 39 | | ADR 0016 |
| How long does Athena remember a `ClientRequestToken`? | 40 | | ADR 0016 |
| Does `DataFreshness` go silent when Firehose is empty, and does an error object land before the freshness passes its record? | 41 | | ADR 0016 |
| What does Firehose write under `errors/firehose/`? | 42 | | ADR 0016, [infra.md §9.3](../spec/infra.md#93-firehose-role) |
| What does a stopped export leave, and what does stopping a finished query return? | 43 | | ADR 0016 |
| What does Athena return past its query quota? | 44 | | ADR 0016 |

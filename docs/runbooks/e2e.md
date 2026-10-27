<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# End-to-end suite on AWS

The end-to-end suite deploys a two-region store to a sandbox AWS account, runs scenarios
against it, and tears it down. It covers what no local test can: CloudFormation, IAM, the
event source mapping, Global Table replication, Firehose, Iceberg, Athena and the alarms.
Only the maintainer runs it. AI assistants and CI never do.

This page is a skeleton. The suite (`internal/e2e`), its scripts (`scripts/e2e/`), the
replication-pause template (`internal/e2e/fis.yaml`) and the `make e2e-aws` target arrive
later in M4; until then, the sections below say what they will do. The manual experiments
in [§8](#8-manual-experiments) don't depend on the suite.

## 1. Prerequisites

- A sandbox AWS account that holds no store you care about: teardown deletes resources by
  name.
- Permissions to deploy CloudFormation stacks that create IAM roles, Lambda functions,
  DynamoDB tables, S3 buckets, Firehose streams, Glue databases, Athena workgroups, Step
  Functions state machines, CloudWatch alarms and dashboards, and AWS FIS experiment
  templates.
- For scenario 24 and the lifecycle experiments, a role you can assume that has only the
  grants of [infra.md §9.8](../spec/infra.md#98-operators), to run the `dynaflux` commands.
- Lake Formation at its defaults, so plain IAM access to the Glue Data Catalog works
  ([infra.md §11](../spec/infra.md#11-deploying)).
- Two regions with Firehose delivery to Iceberg and Glue table optimizers. The suite uses
  `us-east-1`, the home region, and `us-west-2`.
- The SAM CLI, the AWS CLI, the Go version in `go.mod`, and `shellcheck`.

## 2. Run

```sh
make e2e-aws E2E_RUN=<id>
```

`<id>` is short, lowercase and new for each run: every resource name starts with
`e2e-<id>-`. The target runs `scripts/e2e/deploy.sh`, which:

1. copies `examples/analytics` to a temporary directory, adds
   `internal/e2e/testdata/e2e.sql` (a view that fails on a zero divisor, for the breaker
   scenario) and runs `dynaflux generate` there;
2. runs `sam build`;
3. deploys `global.yaml` in `us-east-1` with `Prefix=e2e-<id>-` and `--resolve-s3`, without
   a stack policy, so that teardown stays one command;
4. deploys `regional.yaml` in `us-east-1` and `us-west-2`;
5. runs `dynaflux backfill -all -yes -prefix e2e-<id>-`: the store has no events yet, so
   each job makes its version live by the fresh rule, without a read
   ([lifecycle.md §4.3](../spec/lifecycle.md#43-fresh-streams));
6. deploys `fis.yaml` in `us-west-2`.

Then it runs `go test -tags e2e ./internal/e2e -target=aws`, and always runs
`scripts/e2e/teardown.sh`, even after a failure.

## 3. Watch

- CloudFormation events in both regions during the deploys.
- `sam build` output: one Go build for all functions.
- The dashboards `e2e-<id>-analytics-us-east-1` and `e2e-<id>-analytics-us-west-2`.
- Test output: each failure names the scenario, the region and the assertion.

## 4. Teardown

`scripts/e2e/teardown.sh` runs at the end of every run, and by hand with the same
`E2E_RUN` after an interrupted one:

1. Delete the regional stacks, then the global stack, then the FIS stack.
2. Delete the retained resources by name: turn off deletion protection on the stream tables
   and delete them; empty each region's bucket, versions and delete markers included, and
   delete it; delete the Glue tables and the Glue database.
3. Leave the Glue optimizer log groups alone: every store in the account shares them.

Afterwards, nothing whose name starts with `e2e-<id>-` (or `e2e_<id>_` in Glue) is left.

## 5. What a failure means

| Failure | Likely cause |
|---------|--------------|
| A stream lookup fails with "deploy `global.yaml` first" | The global stack is missing in that region, or was deployed with another `Prefix`. |
| `AccessDenied` in a function or Firehose log | A grant in [infra.md §9](../spec/infra.md#9-iam) is missing. Record the action and the role. |
| A region's views differ from the oracle after convergence | A projector bug, or records the projector missed. Keep the logs and the meta items before teardown. |
| `failed-rows` fires, or records appear under `errors/firehose/` | The archive's column types and the projector's records disagree. |
| A `dynaflux` command fails with `AccessDenied` under the operator role | A row of [infra.md §9.8](../spec/infra.md#98-operators) is missing or too narrow. Record the command, the action and the ARN. |
| `backfill-failed` fires outside scenario 24's deny | A backfill step failed after its retries. `dynaflux status` shows the job interrupted (DF7216), and the backfill function's log has the error. |

This table grows as the suite lands.

## 6. Cost

A few dollars per run. AWS FIS bills per action-minute, about 10 minutes per run; check its
price page before the first run. The tables are on demand and the Firehose volume is tiny.
Experiments 15 and 16 run for hours and cost more Lambda time. Scenario 24 and experiments 28
and 30 add the backfill function's time at 3,008 MB and a few hundred thousand writes.

## 7. Coded scenarios

Every scenario appends through the generated clients and feeds the same events to an
in-memory store with the same catalog, the oracle. After convergence, each region's query
results must equal the oracle's. Numbers are stable identifiers; gaps are scenarios that were
dropped from the design.

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | Appends in both regions, after the first deploy's `backfill -all` | Every version is `live` in both regions, by the fresh rule where the region archives the stream with `events: all` (the job item says `fresh`); the views equal the oracle in each region. |
| 2 | One dedup key in both regions, with the same and with different payloads | No double count; `DivergentDuplicates` is emitted; whether identical replicated `MODIFY` records appear. |
| 3 | Replication pause: FIS `aws:dynamodb:global-table-pause-replication` for 10 minutes | After at least 5 minutes (replication can continue that long after the action starts), write on both sides: the views diverge. After the pause ends, they converge. |
| 4 | `DeleteItem` on a stream item | The `REMOVE` record is skipped; the views don't change. |
| 5 | A poison event (zero divisor) | That view is `broken`, the others stay exact, and the shard isn't blocked; the `views-broken` alarm fires. |
| 6 | Mapping settings | `GetEventSourceMapping` matches the template; there is exactly one mapping per stream table per region. |
| 7 | Least privilege | No `AccessDenied` in any function or Firehose log, including Athena DDL, optimizer creation and `iam:PassRole`. |
| 8 | Archive contents | Once data freshness allows, `<stream>_dedup` equals the oracle; `_shard`, `_seq` and `_arrival` are set; every column type round-trips. |
| 18 | Lake Formation precondition | The Firehose role can call `glue:GetTable` on the archive table. |
| 24 | Backfill under traffic. While appends go on in both regions: deploy `regional.yaml` again with `testdata/backfill.sql`, which adds an additive and an idempotent view of the e2e stream; `dynaflux backfill -all -yes -rate 20`, so that applying takes minutes; during `apply`, deny the backfill role `dynamodb:PutItem` on one new view table for 1 minute, then again until the execution fails (up to about 16 minutes of retries), then remove the deny and run `backfill` again; `backfill -cancel` during another job's `apply`, then `backfill` to resume it; `backfill -rate 200` on a running job. | Both versions go `live` in both regions from the stream table, the additive one by the time split, about 30 minutes after its activation. The 1-minute deny fails no execution. The long one fails the execution: `backfill-failed` fires, and `status` shows the job interrupted (DF7216) with the command that resumes it. After `-cancel`, the job is `cancelled` and the version `activating`. `status` shows the new rate. The views of both versions equal the oracle in each region. |

## 8. Manual experiments

Each experiment runs once, or again when a Dynaflux change touches what it measures. Record
the answer with the template in [§9](#9-results), then in the ADR or specification named
there. Run them on a store deployed as in [§2](#2-run), unless the steps say otherwise.

**Before the first run:** confirm the FIS target key for the replication pause.
`aws fis get-action --id aws:dynamodb:global-table-pause-replication` should list a target
named `Tables`, which `fis.yaml` uses.

| # | Experiment | Steps | Answers |
|---|------------|-------|---------|
| 10 | Schema evolution | Add a column with a `DEFAULT` to the e2e stream, run `generate`, build, and deploy `regional.yaml` in one region. Append events with the new column. | `ArchiveSchema` finishes before the projector update; the time from the deploy to the first delivered value; older rows read the `DEFAULT` in `<stream>_dedup`. |
| 13 | Firehose error output | Turn on CloudTrail S3 data events for the bucket. Put a record whose value doesn't match its column type straight into a Firehose stream. | Where it lands under `errors/firehose/<stream>/`; `failed-rows` fires; whether the Firehose role ever calls `s3:DeleteObject`. |
| 14 | Dedup view pruning | Run `EXPLAIN ANALYZE` on a `<stream>_dedup` query with and without an `event_time` filter. | Whether the filter cuts the data scanned. |
| 15 | Tumbling window across a shard rollover | Append one event every few seconds for 5 hours. | Every invocation's `shardId` is a real shard (`DescribeStream`); the views equal the oracle. |
| 16 | Stuck shard past 24 hours | In one region, add a statement that denies the projector role `dynamodb:PutItem` on its view tables. Keep appending for 25 hours, then remove the statement. | `gap-risk` fires after 6 hours; whether trimmed records reach `aws/lambda/<mapping UUID>/` with the record age unlimited; the projector resumes; what `dynaflux status` (M5) must report. |
| 17 | Add and remove a region | Add a third region with Firehose Iceberg delivery and Glue optimizers, such as `eu-west-1`, and follow [infra.md §12](../spec/infra.md#12-changes-after-the-first-deploy). Time the replica creation. Then remove it, first without `--unprotect-region`: `generate` refuses (DF6018), so move the old `global.yaml` out of `infra.out` before running it. | The new region's views need a backfill; whether the replica's initial copy appears in its stream; whether deletion protection blocks the removal (if it doesn't, `--unprotect-region` goes). |
| 19 | Flip `split` under load | While appending, change `runtime.split` from `store` to `stream`, generate, and deploy `regional.yaml`. | The views equal the oracle; `progress` never moves backward; whether the old and new functions process the same shard at overlapping times. |
| 20 | In-place Firehose update | Change a Firehose stream's buffering in the generated template and deploy it. | The update succeeds on a stream created with `AppendOnly`, without a replacement. |
| 21 | Regional before global | With a new `E2E_RUN`, deploy `regional.yaml` before `global.yaml`. Then deploy `global.yaml` and `regional.yaml` again. | The stream lookup fails; the rollback leaves no bucket or Glue database; the retry succeeds. |
| 22 | Re-create and re-archive | Run a query in the archive workgroup. Delete one regional stack, then create it again with `--import-existing-resources`. Then take that region out of `archive.regions`, generate and deploy; put it back, generate and deploy. | The delete finishes, the workgroup and its query history with it; the new stack adopts the retained bucket; `ArchiveSchema` goes on with the retained Glue database and tables after the re-create and after the archive comes back. |

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

## 9. Results

Copy this block for each experiment into the results log of the release being tested:

```markdown
### Experiment <#>: <name>

- Date, Dynaflux commit, account type, regions:
- Steps run, and any change to them:
- Observed (numbers, log lines, screenshots):
- Answer:
- Recorded in (ADR or spec section):
```

The open questions, and the experiment that answers each one:

| Question | Experiment | Answer | Recorded in |
|----------|------------|--------|-------------|
| Do records trimmed after a 24-hour stall reach `aws/lambda/` with the record age unlimited? | 16 | | [ADR 0003](../decisions/0003-shard-identity-from-tumbling-window.md) |
| How long does Firehose take to deliver an added column? | 10 | | [ADR 0008](../decisions/0008-archive-schema-migration.md) |
| Does Firehose write with `AppendOnly` to a table that Athena created in a CloudFormation database, with the archive's value encodings? | 8 | | ADR 0008 |
| Where do failed records land under `errors/firehose/`, and does Firehose use `s3:DeleteObject`? | 13 | | [infra.md §9.3](../spec/infra.md#93-firehose-role) |
| Does an in-place update of a Firehose stream's Iceberg settings work with `AppendOnly`? | 20 | | [infra.md §7.4](../spec/infra.md#74-firehose-streams) |
| Does `<stream>_dedup` prune partitions on an `event_time` filter? | 14 | | [infra.md §7.3](../spec/infra.md#73-tables-and-columns) |
| Does deletion protection block removing a replica? | 17 | | [infra.md §13](../spec/infra.md#13-checks-in-generate) |
| Does a new replica's initial copy appear in its stream? | 17 | | backfill (M5) |
| Can the old function's last batch overlap the new function's first after a `split` change? | 19 | | [infra.md §12](../spec/infra.md#12-changes-after-the-first-deploy) |
| Does a failed first regional deploy roll back cleanly? | 21 | | infra.md §12 |
| Does a regional stack delete finish with the workgroup, and does a new stack adopt the retained bucket and go on with the Glue database? | 22 | | infra.md §12 |
| Does `sam build` compile the binary once for all functions? | every run | | [infra.md §6.1](../spec/infra.md#61-functions) |
| Do the setup role's grants cover the optimizer calls, `iam:PassRole` included? | 7 | | [infra.md §9.2](../spec/infra.md#92-setup-role) |
| Do the operator grants cover each command? | 24–33 | | [infra.md §9.8](../spec/infra.md#98-operators) |
| Does a rollback make the old catalog current again, retiring nothing? What does it send `Catalog`? | 26 | | [infra.md §6.4](../spec/infra.md#64-catalog-registration) |
| Does `StartExecution` with the name and input of an existing execution return it? | 29 | | [ADR 0011](../decisions/0011-backfill-coordinator.md) |
| How fast does a backfill apply, does a new view table throttle, and what does the scan read? | 30 | | [lifecycle.md §5.4](../spec/lifecycle.md#54-jobs) |
| Does a replicated record's `ApproximateCreationDateTime` follow its arrival in the replica? | 31 | | ADR 0011 |
| Does S3 answer the backfill role's `GetObject` of a missing staged run with 404 or 403? | 32 | | [infra.md §9.6](../spec/infra.md#96-backfill-role) |
| Does the backfill state machine route and continue as written? | 24, 33 | | [infra.md §6.5](../spec/infra.md#65-backfill) |

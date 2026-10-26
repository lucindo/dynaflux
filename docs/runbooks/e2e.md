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
  DynamoDB tables, S3 buckets, Firehose streams, Glue databases, Athena workgroups,
  CloudWatch alarms and dashboards, and AWS FIS experiment templates.
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
5. deploys `fis.yaml` in `us-west-2`.

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

This table grows as the suite lands.

## 6. Cost

A few dollars per run. AWS FIS bills per action-minute, about 10 minutes per run; check its
price page before the first run. The tables are on demand and the Firehose volume is tiny.
Experiments 15 and 16 run for hours and cost more Lambda time.

## 7. Coded scenarios

Every scenario appends through the generated clients and feeds the same events to an
in-memory store with the same catalog, the oracle. After convergence, each region's query
results must equal the oracle's. Numbers are stable identifiers; gaps are scenarios that were
dropped from the design.

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | Appends in both regions | The views equal the oracle in each region. |
| 2 | One dedup key in both regions, with the same and with different payloads | No double count; `DivergentDuplicates` is emitted; whether identical replicated `MODIFY` records appear. |
| 3 | Replication pause: FIS `aws:dynamodb:global-table-pause-replication` for 10 minutes | After at least 5 minutes (replication can continue that long after the action starts), write on both sides: the views diverge. After the pause ends, they converge. |
| 4 | `DeleteItem` on a stream item | The `REMOVE` record is skipped; the views don't change. |
| 5 | A poison event (zero divisor) | That view is `broken`, the others stay exact, and the shard isn't blocked; the `views-broken` alarm fires. |
| 6 | Mapping settings | `GetEventSourceMapping` matches the template; there is exactly one mapping per stream table per region. |
| 7 | Least privilege | No `AccessDenied` in any function or Firehose log, including Athena DDL, optimizer creation and `iam:PassRole`. |
| 8 | Archive contents | Once data freshness allows, `<stream>_dedup` equals the oracle; `_shard`, `_seq` and `_arrival` are set; every column type round-trips. |
| 18 | Lake Formation precondition | The Firehose role can call `glue:GetTable` on the archive table. |

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

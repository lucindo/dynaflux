<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0008: A custom resource served by the runtime binary migrates the archive schema

- Status: Accepted
- Date: 2026-10-26

## Context

Each archiving region has one Firehose stream and one Iceberg table in the Glue Data
Catalog per archived stream. Four facts make the table's schema a deploy-time problem:

- **Firehose silently skips JSON fields the table lacks** ([Iceberg considerations][fh-cons]).
  A column added to a stream must reach the Iceberg table *before* any projector sends it, or
  its values never reach the archive, which is the permanent record of events.
- **On first deploy the table must exist before its Glue table optimizers**, whose creation
  reads the table.
- **The dedup view (`<stream>_dedup`) must be re-created on every column change**, because
  an Athena view stores its column list. CloudFormation has no Athena view resource, and a
  Glue Data Catalog view needs Lake Formation-managed tables, while the archive uses plain
  IAM.
- **`AWS::Glue::TableOptimizer` exists in only 15 regions** ([optimizer regions][glue-reg]),
  and CloudFormation checks a template's resource types before it evaluates conditions
  ([region parity][cfn-parity]; cfn-lint 1.58 reports E3006 for a conditioned optimizer in
  us-west-1). One optimizer resource in `regional.yaml` would fail the stack in every other
  region of `regions`, archiving or not.

The plan for the archive left one choice to infrastructure generation (M4): a migration
step ordered before the projector, run either as a CloudFormation custom resource or as a
CLI step. Research for M4 (no AWS access; AWS documentation, cfn-lint 1.58 and AWS sample
templates) found:

| Finding | Source |
|---------|--------|
| `AWS::Glue::Table` now accepts an Iceberg schema, partition spec and location (`OpenTableFormatInput.IcebergInput.IcebergTableInput`), and lists updates as "No interruption". The earlier assumption that "a CloudFormation schema change replaces the table" holds for `AWS::S3Tables::Table`, not for Glue. | [CFN Glue::Table][cfn-glue], [IcebergTableInput][cfn-ice] |
| What a stack update of that property does to a live Iceberg table (its snapshots, `metadata_location`, a concurrent Firehose commit) isn't documented. A rollback would re-apply the old schema. | same |
| Iceberg field IDs would have to be written in the template and kept forever, since sql.md lets a new column sit anywhere in the declaration. `IcebergTableInput` types are strings, which can't express `list<…>`. | [IcebergTableInput][cfn-ice], [IcebergStructField][glue-field] |
| Firehose writes only Iceberg v2 tables, so v3 column defaults (`initial-default`) can't stand in for the view's `coalesce`. | [Firehose prerequisites][fh-prereq] |
| Athena DDL (`CREATE TABLE … PARTITIONED BY (day(event_time))`, `ALTER TABLE ADD COLUMNS`, `CREATE OR REPLACE VIEW`) is a documented way to create and evolve Glue Iceberg tables that Firehose can write. | [Athena Iceberg][athena-ice], [AWS blog][blog-fh] |
| Athena DDL needs `glue:GetDatabase`, `GetDatabases` and `CreateDatabase` on the catalog and `database/default` in each region, and `CREATE TABLE` needs `glue:GetDatabases`, `GetTables`, `GetPartitions` too. | [Athena access to Glue][athena-glue] |
| "If a resource with a condition isn't created, any resources that depend on that resource aren't created either", so a static `DependsOn` from the projector to an archive-only resource would drop the projector in regions that don't archive (cfn-lint E3005). | [CFN Condition][cfn-cond] |
| A custom resource takes `ServiceTimeout` from 1 to 3600 s; its `ServiceToken` can't change. CloudFormation calls it again only when its properties change. | [CFN CustomResource][cfn-cr] |

A second deploy-time need appeared in the same research: an event source mapping needs the
exact stream ARN of the **local** replica, and a regional stack can't get it. `GetAtt
StreamArn` on `AWS::DynamoDB::GlobalTable` covers only the region of the stack that owns the
table, cross-region triggers aren't supported, and the ARN ends in a label chosen when the
stream was created ([CFN GlobalTable][cfn-gt], [Lambda with DynamoDB][ddb-esm]).

## Decision

**The archive's Glue database, its schema and its table optimizers are owned by a custom
resource in `regional.yaml`, `Custom::DynafluxArchiveSchema`, served by the runtime binary in
a new mode, `setup`.**

- **The function:** `{prefix}{store}-setup`, the same binary as every other function,
  with `DYNAFLUX_MODE=setup` (`catalog.EnvModeSetup`), a 900-second timeout and its own role.
  It exists in every region.
- **Properties:** the Glue database and its location (`warehouse/<database>/`), the Athena
  workgroup (`{prefix}{store}`), the optimizer role and settings, and one entry per table
  archived in this region: its location (`warehouse/<database>/<stream>/iceberg/`) and its
  ordered columns (name, Athena type, and the `DEFAULT` as an Athena literal, from which
  the handler writes the dedup view). The partition spec, `day(event_time)`, is fixed in
  the handler. Plus `Fingerprint`, a SHA-256 of the tables without the prefix-dependent
  fields, computed by `generate`. The properties
  hold no SQL: a `DEFAULT` is a plain string, never `Fn::Sub`, so a `${` in one stays as it
  is. The desired state is in the properties, so a change set shows schema changes,
  and CloudFormation sends `Update` only when they change. `ServiceTimeout` is 960, a
  minute past the function's timeout, so a `FAILED` reply arrives in time even when
  Lambda's queue held the request. The
  handler works from the properties alone; it doesn't consult the binary's catalog. It
  checks that the names, types and defaults have the forms that `internal/archive` writes:
  they become Athena statements, and anyone allowed to invoke the function can send them.
- **Create and Update,** each step idempotent, and each call retried for about 60 seconds
  on Iceberg commit conflicts and on `AccessDenied`, while a role or grant that the same
  deploy creates or changes takes effect:
  1. Create the database at its location if it is missing (`glue:CreateDatabase`, the call
     that `AWS::Glue::Database` makes).
  2. For each table: if missing, `CREATE TABLE` (Iceberg v2, Parquet, zstd) at its
     location. Otherwise read it (`glue:GetTable`) and fail if its location or table type
     differs: the handler never moves, repartitions or converts a table. It doesn't compare
     the partition spec, which doesn't change what a query returns and which only
     Dynaflux's DDL sets at that location. Then
     `ALTER TABLE ADD COLUMNS` for the missing columns, and fail on a column whose type
     differs. Columns the table has and the properties lack are left alone. Nothing is ever
     dropped or retyped, and the handler issues no `DROP`.
  3. For each table, create or update its compaction and snapshot-retention optimizers
     (`glue:CreateTableOptimizer` / `UpdateTableOptimizer`, run as the optimizer role).
     `validate` refuses an archive region where Glue has no table optimizers (error DF6505).
  4. `CREATE OR REPLACE VIEW` each `<stream>_dedup`.
  5. Reply `SUCCESS` with a constant `PhysicalResourceId` (the database name), so an update
     is never a replacement, and the data attribute `Fingerprint` (the property's value).
- **Delete** replies `SUCCESS` without touching anything. The resource is
  `DeletionPolicy: Retain` anyway; the database, the tables and their optimizers outlive
  the stack, and a new stack, or an archive turned back on, goes on with them.
- **Order:** `ArchiveSchema` depends on every stream lookup (below), so a missing replica
  fails the deploy before any table is written. Each projector's environment holds
  `DYNAFLUX_ARCHIVE_SCHEMA: !If [InArchive, !GetAtt ArchiveSchema.Fingerprint, !Ref
  AWS::NoValue]`. That makes the projector depend on the migration only in archiving
  regions, and changes the function's configuration exactly when the schema changes.
  Firehose streams carry the same condition and a plain `DependsOn`.
- **Guard in the projector:** it compares `DYNAFLUX_ARCHIVE_SCHEMA` with its own catalog's
  fingerprint for `AWS_REGION`, and requires the variable exactly when the catalog archives
  in this region. On a mismatch it fails the whole batch. The shard blocks, `IteratorAge`
  alarms fire, and no column is lost silently, even when a projector is updated outside
  CloudFormation. This is the only check for skew between the templates and the binary;
  `sam build` compiles the binary from the tree `generate` wrote, and `sam deploy` ships the
  template copied by that build, so skew needs a hand-made deploy.
- **Identifiers** are always quoted in generated SQL (backticks in DDL, double quotes in
  `SELECT`): Athena needs it for names starting with `_` and for reserved words such as
  `date`, which are valid Dynaflux column names.

**The same `setup` function serves `Custom::DynafluxStreamArn`,** one per stream table in
each region: `DescribeTable` in the stack's region, `PhysicalResourceId` = `Data.StreamArn` =
`LatestStreamArn`. It accepts `TableStatus` `ACTIVE` or `UPDATING` (a replica add, billing or
TTL change elsewhere leaves `LatestStreamArn` valid) when the stream is present and the local
replica's `ReplicaStatus` is `ACTIVE`. It fails with a clear reason when the table or replica
is missing ("deploy `global.yaml` first, with the same `Prefix`"), the replica isn't
`ACTIVE`, or the stream view isn't `NEW_AND_OLD_IMAGES`. It retries `AccessDenied` for
about 60 seconds, because the setup role may be new and IAM is eventually consistent.
`ServiceTimeout` is 300. MREC replicas can't disable their stream, so the ARN never
changes under a running stack. CloudFormation calls the lookup again only when its
properties change, so a stream table or replica re-created by hand leaves a stale ARN. Each
lookup gets the template parameter `LookupGeneration`, and a deploy with a new value runs
them all again ([infra.md](../spec/infra.md#62-stream-arn-lookup)); re-creating the
regional stack would delete its view tables. One function for both
keeps one deploy-time binary path and one fixed `ServiceToken`; the stream lookup adds only
read-only `dynamodb:DescribeTable` to the role.

**IAM of the setup role:** `dynamodb:DescribeTable` on the store's stream tables in this
region and its own log group; in archiving regions:

- `athena:StartQueryExecution`, `GetQueryExecution` and `StopQueryExecution` (to cancel a
  query when the handler runs out of time or can't read its state) on the workgroup;
- `glue:GetDatabase`, `GetDatabases` and `CreateDatabase` on the catalog and
  `database/default`, which Athena requires in every region;
- `glue:CreateDatabase`, `CreateTable`, `GetDatabase`, `GetDatabases`, `GetPartitions`,
  `GetTable`, `GetTables`, `UpdateTable`, `CreateTableOptimizer` and
  `UpdateTableOptimizer` on the catalog, the archive database and `table/<database>/*`;
- `iam:PassRole` of the optimizer role, with `iam:PassedToService` = `glue.amazonaws.com`;
- `s3:GetObject` and `PutObject` on `warehouse/<database>/*`, `s3:ListBucket` with
  `s3:prefix` limited to `warehouse/<database>` and `warehouse/<database>/*`, and
  `s3:GetBucketLocation` in its own statement (it has no prefix key, so a prefix condition
  would never match it).

No `s3:DeleteObject`: `CREATE TABLE`, `ADD COLUMNS` and `CREATE OR REPLACE VIEW` don't delete
files (Iceberg keeps old metadata files by default), and apart from the optimizers no
principal should be able to delete archive files. It is added only if the end-to-end suite
shows `AccessDenied` for it.

### Rejected

- **CloudFormation-native `AWS::Glue::Table`:** the update semantics on a live table are
  undocumented, field IDs would have to be persisted (for example in `dynaflux.lock`), `Array`
  columns would need Hive `TableInput` columns whose updates are the riskiest part, and the
  dedup view would need an undocumented "Presto view" encoding in `VIRTUAL_VIEW` tables.
- **CloudFormation-native `AWS::Glue::TableOptimizer`:** see Context; it would make
  `regional.yaml` undeployable in most regions.
- **A CLI step before the regional deploy:** nothing enforces the order in every region on
  every schema change, a forgotten step loses data silently, the first deploy needs two
  phases (bucket and database from the stack, the table from the CLI, then the optimizers),
  and the CLI would change infrastructure, against "Dynaflux never deploys; your IaC
  does".
- **A custom resource calling the Glue Iceberg API (`UpdateTable` with `add-schema`):** it
  must assign field IDs itself, and still needs Athena for the view.
- **Inline Python (`Code.ZipFile`):** a second language runtime in users' stacks, untested
  with the rest.
- **A fingerprint check in the setup handler** (fail when the binary's catalog disagrees
  with the properties): skew between templates and binary needs a hand-made deploy (see the
  projector guard), the projector guard already catches it, and the check was the only part
  of this design that could fail a rollback.
- **For the stream ARN, a template parameter per stream:** the lookup goes stale just the
  same when a table is re-created, but a parameter adds a manual per-region input to every
  deploy.
- **An `AWS::Glue::Database` resource,** `RetainExceptOnCreate` like the bucket, as the plan
  had it: a retained database made re-creating the regional stack, or turning the archive
  back on, fail with `AlreadyExists`, and importing it needs a change set. Deleted with the
  stack instead, it would take the tables' entries with it.

## Consequences

- The runtime binary has four modes: `projector`, `query` (M6), `backfill` (M5), `setup`.
  The `setup` handlers land with the runtime package in M3; M4 renders the resources that
  call them. `catalog` gains `EnvModeSetup` and `EnvArchiveSchema`;
  [config.md](../spec/config.md#resource-names) gains the setup function (P ≤ 58) and the
  Athena workgroup (P ≤ 128). The deploy stays one `sam deploy` per region with no manual
  input. [infra.md](../spec/infra.md) describes the resources.
- `internal/archive` owns the type map, the column list, the table locations, the DDL, the
  dedup SQL and the fingerprint. The template builder, the `setup` handler, the projector
  guard and the Firehose writer use it, so they can't disagree.
- The plan kept the schema out of CloudFormation because a schema change would replace the
  table; that holds only for S3 Tables. The archive has no `_sv` column either: events carry
  no schema version in v1 ([sql.md §8.2](../spec/sql.md#82-columns)). The custom resource
  and the projector guard enforce the order, so `dynaflux diff` and `status` (M5) don't
  have to.
- The table optimizers aren't CloudFormation resources: a change set shows their settings
  as properties of `ArchiveSchema`, an optimizer deleted by hand comes back only on the next
  property change, and they outlive a deleted stack along with the tables.
- A deploy that changes the schema briefly fails projector batches while CloudFormation
  updates the function's configuration and code in two calls. They retry; the error alarms
  need three periods to fire.
- A handler crash before it replies holds the stack for up to `ServiceTimeout` (16 minutes)
  before it fails and rolls back. `cfn.LambdaWrap` from `aws-lambda-go` turns returned errors
  into `FAILED` replies.
- Re-creating a deleted regional stack meets the retained bucket, which it imports
  ([infra.md](../spec/infra.md#12-changes-after-the-first-deploy)); the database and tables
  need nothing.
- `archive.catalog: s3tables` would need its own handler path (its schema is
  replacement-only and Athena access needs Lake Formation); this decision covers `glue` only.
- **Unverified, for the AWS end-to-end suite:**
  - How soon a running Firehose stream delivers a column added by `ALTER TABLE ADD COLUMNS`.
    If it caches the schema, the handler waits out the measured lag before replying, within
    `ServiceTimeout`.
  - That a table Athena created, in a database that `glue:CreateDatabase` created, takes
    Firehose `AppendOnly` writes with the archive's value encodings. The Firehose
    prerequisites ask for databases and tables "created through Iceberg's GlueCatalog
    API"; AWS's own Firehose-to-Iceberg sample creates its database with
    `AWS::Glue::Database`, which makes the same call, so this is probably fine.
  - The exact IAM the Firehose role and the setup role need (`s3:DeleteObject` for
    Firehose; `iam:PassRole` for the optimizer calls).
  - That Glue still lists a column dropped from an archive table, with
    `iceberg.field.current` set to `false`. The handler skips such a column, so a column
    that someone dropped by hand is added again.
  - That the account's Lake Formation settings leave plain IAM access in place (the default);
    [infra.md](../spec/infra.md#11-deploying) states it as a precondition.

[fh-cons]: https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-considerations.html
[fh-prereq]: https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-prereq.html
[glue-reg]: https://docs.aws.amazon.com/glue/latest/dg/regions-optimizers.html
[cfn-parity]: https://aws.amazon.com/blogs/mt/handling-region-parity-with-infrastructure-as-code
[athena-glue]: https://docs.aws.amazon.com/athena/latest/ug/fine-grained-access-to-glue-resources.html
[cfn-glue]: https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-glue-table.html
[cfn-ice]: https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-glue-table-icebergtableinput.html
[glue-field]: https://docs.aws.amazon.com/glue/latest/webapi/API_IcebergStructField.html
[athena-ice]: https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-evolving-table-schema.html
[blog-fh]: https://aws.amazon.com/blogs/big-data/stream-real-time-data-into-apache-iceberg-tables-in-amazon-s3-using-amazon-data-firehose/
[cfn-cond]: https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-attribute-condition.html
[cfn-cr]: https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-cloudformation-customresource.html
[cfn-gt]: https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-dynamodb-globaltable.html
[ddb-esm]: https://docs.aws.amazon.com/lambda/latest/dg/services-dynamodb-eventsourcemapping.html

<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# `dynaflux.yaml` reference

`dynaflux.yaml` configures one Dynaflux store: its name, its AWS regions, where the SQL
definitions live, and how code and infrastructure are generated. This page is the normative
schema.

Every command that loads a project (`dynaflux validate`, `dynaflux explain`, and later
`dynaflux generate`) applies all the rules below, including those for keys that only later
milestones use.

The SQL dialect is specified in [sql.md](sql.md), how Dynaflux stores data in
[encoding.md](encoding.md), and the infrastructure that `dynaflux generate` writes in
[infra.md](infra.md).

## Minimal example

```yaml
store: analytics
regions: [us-east-1, us-west-2]
```

Every other key is optional. SQL files are read from `dynaflux/**/*.sql`.

## Complete example

`store` and `regions` are required. Every other key shows its default unless the comment
says `not default`. The example assumes the SQL files declare the streams `page_views` and
`debug_events` and the view `page_daily`.

```yaml
store: analytics
prefix: prod-                         # not default (default: no prefix)
regions: [us-east-1, us-west-2, ap-northeast-1]
sql: ["dynaflux/**/*.sql"]

go:
  package: flux
  out: internal/flux

runtime:
  out: cmd/dynaflux-runtime
  arch: arm64                         # arm64 | x86_64
  timeout_seconds: 60
  prune_window_seconds: 960
  memory_mb: 1024
  batching_window_seconds: 1
  split: store                        # store | stream

infra:
  format: sam
  out: infra/dynaflux

defaults:
  capacity: on_demand                 # on_demand | provisioned
  view_mode: async                    # async | sync
  log_retention: 2d                   # <n>h | <n>d | <n>w | forever

tables:                               # not default (default: no overrides)
  page_daily:                         # a view: applies to the table of every version
    capacity: provisioned
    autoscaling:
      read:  {min: 5, max: 500}       # target_percent defaults to 70
      write: {min: 5, max: 1000, target_percent: 60}

archive:
  enabled: true
  catalog: glue                       # glue | s3tables
  regions: all                        # all | a list of entries from regions
  events: all                         # all | local
  streams:                            # not default (default: no overrides)
    debug_events: {enabled: false}    # validate warns (DF6504)

http:
  frontend: http_api                  # not default (default: function_url)
  auth: iam                           # allowed values depend on frontend
  allow_public: false
  domain:                             # not default (default: none)
    name: flux.example.com
    routing: latency
```

## Finding the file

Commands find `dynaflux.yaml` the way the `go` command finds `go.mod`:

1. Start in the working directory.
2. If it holds an entry named `dynaflux.yaml`, use it. It must be a regular file or a link
   to one: a directory, FIFO or other entry of that name stops the command (exit status 2).
3. Otherwise move to the parent directory and repeat.
4. If no directory up to the filesystem root holds one, stop with `DF6001` (exit status 2).

The directory that holds `dynaflux.yaml` is the **project root**, normally your Go module
root. Every path in the file is relative to it, and Dynaflux reads SQL files and writes
generated files only inside it. The lock file `dynaflux.lock`, which numbers view versions,
lives there too.

## File format

- The file holds one YAML document whose top level is a mapping (`DF6004`). An empty file, or
  one with only comments, is `DF6004` too. A YAML syntax error is `DF6003`.
- Keys are case-sensitive and written in `snake_case`.
- **Unknown keys are errors** (`DF6005`) at every level. There is no `explain:` section:
  workload inputs for `dynaflux explain` are command-line flags.
- Duplicate keys are errors (`DF6006`).
- Anchors (`&a`), aliases (`*a`), merge keys (`<<`) and explicit tags (`!!str`, `!x`) are
  errors (`DF6014`). Write each value out in full.
- A value of the wrong YAML type is an error (`DF6007`), for example a single string where a
  list is needed, or a value that YAML reads as another type, such as `log_retention: 48`
  (an integer, not a duration). See [Scalars](#scalars) for the spellings each type accepts.
- Keys are scalars. A list or a mapping used as a key, as in `? [a, b]`, is `DF6007`.
- A key set to `null`, `~` or nothing (`prefix:`) counts as not set, so its default applies.
  For a required key that is `DF6008`.

### Scalars

YAML accepts several spellings for one value. Dynaflux accepts one:

| Type | Accepted | Rejected with `DF6007` |
|------|----------|------------------------|
| Boolean | `true` or `false` | `yes`, `on`, `True`, `"true"` |
| Integer | Plain decimal digits, with an optional leading `-` and no leading zeros: `0`, `1024` | `1024.0`, `1_024`, `0x400`, `0o2000`, `"1024"` |
| String | A plain or quoted scalar that YAML reads as a string: `analytics`, `"123"` | A plain `123`, `true` or `1.5` |

- An integer that doesn't fit in 64 bits is out of range (`DF6010`).
- A list entry can't be null: `regions: [us-east-1, null]` is `DF6007`.
- A null entry under `tables` or `archive.streams`, as in `tables: {page_daily: }`, is an
  entry with nothing set, so its required key is missing (`DF6008`).
- Allowed values (such as `on_demand`, `arm64` and `all`), `forever` and duration units are
  lowercase, and case matters: `ON_DEMAND` is `DF6009`, and `2D` is `DF6401`.

### Value types

| Type | Syntax | Examples |
|------|--------|----------|
| Region | An AWS region code: lowercase words joined by `-`, ending in a number (`^[a-z]+(-[a-z]+)+-[0-9]+$`). Dynaflux checks the form only; it never calls AWS. | `us-east-1`, `ap-northeast-1` |
| Path | A directory relative to the project root, with `/` as the separator. It must not be empty, be absolute, contain `\` or a `..` segment, or clean to the project root (`.`) (`DF6302`). These checks come before cleaning, so `internal/../flux` is an error. Then the path is cleaned: `./a/` becomes `a`. | `internal/flux` |
| Duration | A whole number of at least 1, with no sign or leading zeros, followed by one unit with no space: `h` (hour), `d` (day, 24 h) or `w` (week, 7 d). One pair only: write `36h`, not `1d12h` (`DF6401`). | `2d`, `36h`, `1w` |

Durations have no month or year unit, because their length varies. For calendar retention,
use `LOG RETENTION n MONTH` or `LOG RETENTION n YEAR` on the stream in SQL.

## Keys

The **Used by** column names who reads each value. `validate` checks every key.

| Tag | Reader |
|-----|--------|
| `validate` | Only the checks on this page. |
| `explain` | The cost model of `dynaflux explain` (M1). |
| `catalog` | Resolved into the catalog that the generated code embeds and the runtime reads (M2). |
| `codegen` | The Go files that `dynaflux generate` writes (M2). |
| `infra` | The generated CloudFormation/SAM templates ([infra.md](infra.md); HTTP front doors in M6). |
| `names` | The [resource names](#resource-names). The operator commands find resources by these names. |

A clause in SQL always wins over a config default: `MODE` over `defaults.view_mode`, and
`LOG RETENTION` over `defaults.log_retention`.

### Top level

| Key | Type | Default | Rules | Used by |
|-----|------|---------|-------|---------|
| `store` | string | required (`DF6008`) | Lowercase letters, digits and `-`. Starts with a letter and doesn't end with `-` (`DF6101`). Its length is limited by the derived names (`DF6110`). | `names`, `catalog`, `infra` |
| `prefix` | string | `""` (no prefix) | Empty, or lowercase letters, digits and `-`, starting with a letter and ending with `-`, such as `prod-` (`DF6102`). | `names`, `infra` |
| `regions` | list of regions | required (`DF6008`) | At least one entry (`DF6011`), no repeats (`DF6012`), each a region code (`DF6103`) where Dynaflux can place its bucket (`DF6104`). | `catalog` (Global Table replicas, archive placement), `infra`, `explain` |
| `sql` | list of glob patterns | `["dynaflux/**/*.sql"]` | At least one pattern (`DF6011`), no repeats after cleaning (`DF6012`). See [SQL file patterns](#sql-file-patterns). | `validate` |

- `store` and `prefix` allow neither uppercase letters nor `_`: S3 bucket names allow
  neither, and the Glue database name replaces `-` with `_`, so a `_` would let two stores
  map to the same database.
- `{prefix}{store}` must not start with `xn--`, `sthree-` or `amzn-s3-demo-`, which S3
  reserves at the start of bucket names (`DF6112`).
- `prefix` usually names an environment, such as `prod-` or `staging-`. It isn't part of
  the catalog, so the generated Go code is the same for every prefix. The runtime reads its
  prefix from the `DYNAFLUX_PREFIX` environment variable.
- The generated templates take the prefix as their `Prefix` parameter, whose default is
  this key, so one set of templates serves every environment from the same checkout: deploy
  them with `Prefix=staging-`, for example ([infra.md](infra.md#2-parameters)). `validate`
  checks the names with this key's value. The parameter's maximum length is the longest
  prefix with which every name fits, so CloudFormation refuses a longer one. The operator
  commands take `-prefix`, whose default is this key, and work on one environment per run
  ([lifecycle.md §10](lifecycle.md#10-regions-and-environments)).
- The order of `regions` and `archive.regions` doesn't matter: the catalog stores them
  sorted.
- All of `regions` must be in one AWS partition, because a Global Table can't span
  partitions: `cn-north-1` and `us-east-1` can't be mixed. `validate` checks only the form
  of each code.
- Add or remove one region per deploy: CloudFormation adds or removes only one Global Table
  replica per stack update, and `dynaflux generate` refuses a bigger change (`DF6017`). It
  also refuses to remove a region whose replicas still have deletion protection (`DF6018`).
  Removing a region deletes that region's replica of every stream table, with its data.
  [infra.md](infra.md#12-changes-after-the-first-deploy) gives the steps.
- Dynaflux puts an S3 bucket in every region, named in the S3 account regional namespace.
  `me-south-1` and `me-central-1` don't offer that namespace, so they can't be in
  `regions` (`DF6104`).

### `go`

| Key | Type | Default | Rules | Used by |
|-----|------|---------|-------|---------|
| `go.package` | string | `flux` | A lowercase Go identifier (`^[a-z][a-z0-9]*$`) that isn't a Go keyword, `main`, `dynaflux`, `runtime` or `log` (`DF6301`). Generated and user code import the Dynaflux packages `dynaflux` and `runtime`, and the runtime binary's `main.go` imports the standard `log`, so each name would need an import alias. | `codegen` |
| `go.out` | path | `internal/flux` | Path rules (`DF6302`). Must differ from `runtime.out` and `infra.out` (`DF6303`). | `codegen` |

### `runtime`

These settings apply to every Lambda function Dynaflux generates, because the projector,
setup, query and backfill functions all run the same binary. The one exception is the setup
function's timeout, always 900 seconds: it runs deploy-time steps and never writes view rows
([infra.md](infra.md#61-functions)).

| Key | Type | Default | Rules | Used by |
|-----|------|---------|-------|---------|
| `runtime.out` | path | `cmd/dynaflux-runtime` | Path rules (`DF6302`, `DF6303`). `main.go` imports the `go.out` package, so if `go.out` has an `internal` segment, `runtime.out` must be inside the directory that holds the last one (`DF6304`). For example, with `go.out: pkg/internal/flux`, `runtime.out` must be under `pkg/`. | `codegen`: writes `main.go` here |
| `runtime.arch` | string | `arm64` | `arm64` or `x86_64` (`DF6009`). | `infra`, `explain` (Lambda price) |
| `runtime.timeout_seconds` | integer | `60` | 1 to 900, the Lambda maximum (`DF6010`). | `infra` |
| `runtime.prune_window_seconds` | integer | `960` | 1 to 90000, which is 25 hours (`DF6010`), and at least `timeout_seconds + 60` (`DF6305`). Below 960 it warns (`DF6306`). | `catalog` (when the projector prunes high-water marks), `explain` (row size) |
| `runtime.memory_mb` | integer | `1024` | 128 to 10240, the Lambda range (`DF6010`). | `infra`, `explain` |
| `runtime.batching_window_seconds` | integer | `1` | 0 to 300, the event source mapping range (`DF6010`). | `infra` (`MaximumBatchingWindowInSeconds`), `explain` |
| `runtime.split` | string | `store` | `store`: one projector function for the store. `stream`: one per stream (`DF6009`). | `infra`, `names` |

Each view row keeps a high-water mark per delivery unit
([sql.md](sql.md#75-state-classes)). `prune_window_seconds` sets how long the projector
keeps a unit's mark after the unit's progress has passed it
([encoding.md §7.6](encoding.md#76-high-water-marks)). Dropping a mark sooner would
let an invocation that is still running apply some events twice, so the window must
outlast every running invocation: its timeout, plus 60 seconds for writes that land late
and for clock differences between hosts.

- The default, 960, is the 900-second Lambda maximum plus 60, so it is safe for every
  timeout. A shorter window keeps hot rows smaller: `dynaflux explain` shows the row sizes.
- With a window below 960 (`DF6306`), lower it together with `timeout_seconds` in two
  deploys: first the timeout, then, once the old timeout has passed, the window. Lowered in
  one deploy, invocations that started under the old timeout can outlive the new window.

### `infra`

| Key | Type | Default | Rules | Used by |
|-----|------|---------|-------|---------|
| `infra.format` | string | `sam` | `sam` (CloudFormation/SAM) is the only format (`DF6009`). | `infra` |
| `infra.out` | path | `infra/dynaflux` | Path rules (`DF6302`, `DF6303`). | `infra`: writes `global.yaml`, `global.stack-policy.json` and `regional.yaml` here ([infra.md](infra.md)) |

### `defaults`

| Key | Type | Default | Rules | Used by |
|-----|------|---------|-------|---------|
| `defaults.capacity` | string | `on_demand` | `on_demand` or `provisioned` (`DF6009`). `provisioned` needs `defaults.autoscaling` (`DF6404`), and `on_demand` forbids it (`DF6405`). Applies to every table not listed in `tables`: streams, view versions and the meta table. | `infra`, `explain` |
| `defaults.autoscaling` | [autoscaling](#autoscaling) | none | Required when `defaults.capacity` is `provisioned`. | `infra`, `explain` |
| `defaults.view_mode` | string | `async` | `async` or `sync` (`DF6009`). Applies to views without a `MODE` clause. Such views get the same `SYNC` checks as views that declare it. | `catalog`, `explain` |
| `defaults.log_retention` | duration or `forever` | `2d` | Duration syntax (`DF6401`). From 1 day to 36525 days, which is 100 years of 365.25 days (`DF6402`): `24h` or `1d` up to `876600h`, `5217w` or `36525d`. These are the bounds of `LOG RETENTION` ([sql.md](sql.md#86-log-retention)). `forever` is allowed, with warning `DF6403`. Applies to streams without a `LOG RETENTION` clause. | `catalog` (stream item TTL), `explain` |

### `tables`

Overrides capacity for single tables.

| Key | Type | Default | Rules | Used by |
|-----|------|---------|-------|---------|
| `tables` | mapping from a stream or view name to an entry | none | Each key names a stream or a view in the SQL files (`DF6407`). Streams and views share one namespace (`DF1303` in [sql.md](sql.md#2-files-and-statements)), so a key names exactly one of them. | `infra`, `explain` |
| `tables.<name>.capacity` | string | required (`DF6008`) | Same values and rules as `defaults.capacity` (`DF6009`, `DF6404`, `DF6405`). | `infra`, `explain` |
| `tables.<name>.autoscaling` | [autoscaling](#autoscaling) | none | Required when `tables.<name>.capacity` is `provisioned`. | `infra`, `explain` |

An entry replaces the default capacity of that table. It doesn't inherit
`defaults.autoscaling`. For a view, the entry applies to the table of every version.

### `autoscaling`

Provisioned tables always scale automatically. For a stream's Global Table, CloudFormation
accepts provisioned write capacity only as an auto scaling policy, which it applies to every
replica.

```yaml
autoscaling:
  read:  {min: 5, max: 500, target_percent: 70}
  write: {min: 5, max: 500, target_percent: 70}
```

| Key | Type | Default | Rules |
|-----|------|---------|-------|
| `read`, `write` | mapping | required (`DF6008`) | |
| `<read\|write>.min` | integer | required (`DF6008`) | At least 1 (`DF6010`). |
| `<read\|write>.max` | integer | required (`DF6008`) | At least 1 (`DF6010`), and at least `min` (`DF6406`). |
| `<read\|write>.target_percent` | integer | `70` | 20 to 90, the DynamoDB auto scaling range (`DF6010`). |

Values are capacity units for one table in one region. `explain` prices provisioned
capacity at the target utilization.

### `archive`

| Key | Type | Default | Rules | Used by |
|-----|------|---------|-------|---------|
| `archive.enabled` | boolean | `true` | Applies to every stream not listed in `archive.streams`. | `catalog`, `infra`, `explain` |
| `archive.catalog` | string | `glue` | `glue` (Iceberg tables in the Glue Data Catalog) or `s3tables` (Amazon S3 Tables) (`DF6009`). `s3tables` isn't supported yet (`DF6506`). | `infra`, `names` |
| `archive.regions` | `all` or list of regions | `all` | `all`, or a list of entries from `regions` (`DF6501`) with at least one entry (`DF6011`) and no repeats (`DF6012`). Any other string is `DF6009`. Each region must be able to hold the archive (`DF6505`, below). | `catalog`, `infra`, `explain` |
| `archive.events` | string | `all` | `all`: each archiving region archives every event, local and replicated. `local`: each region archives only the events it accepted, which requires `archive.regions: all` (`DF6502`). Other values are `DF6009`. | `catalog`, `infra`, `explain` |
| `archive.streams` | mapping from a stream name to an entry | none | Each key names a stream (`DF6503`). | `catalog`, `infra`, `explain` |
| `archive.streams.<name>.enabled` | boolean | required (`DF6008`) | Overrides `archive.enabled` for this stream, in either direction. | `catalog`, `infra`, `explain` |

A stream that keeps events for a limited time and isn't archived gets warning `DF6504`: its
older events are gone for good.

When at least one stream is archived, each archive region must support Firehose delivery
to Apache Iceberg Tables and AWS Glue table optimizers, which compact the archive's small
files and expire its old snapshots (`DF6505`). With `archive.regions: all`, that is every
region in `regions`; if one of them lacks either service, list the regions to archive in.
Firehose doesn't deliver to Iceberg in some regions, including the China and AWS GovCloud
(US) regions ([Firehose list](https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-considerations.html)),
and Glue offers table optimizers in 15 regions
([Glue list](https://docs.aws.amazon.com/glue/latest/dg/regions-optimizers.html)).

Choosing a placement:

| Setting | Result |
|---------|--------|
| `regions: all`, `events: all` (default) | Every region has a complete archive. Athena and backfills work in any region, including during an outage. Costs one copy per region. |
| `regions: [r]`, `events: all` | One complete archive, in `r`. If `r` is down, archiving pauses and catches up when it recovers. Other regions backfill from their own tables, which hold only the events of their log window; reading `r`'s archive from another region comes after v1. |
| `regions: all`, `events: local` | One copy in total, but the full history is the union of the regional tables. |

How each placement affects backfills is in [lifecycle.md §5.2](lifecycle.md#52-sources).

### `http`

These settings apply only to named queries annotated `-- expose: http`. Direct Lambda invoke
with IAM always works, whatever the settings.

| Key | Type | Default | Rules | Used by |
|-----|------|---------|-------|---------|
| `http.frontend` | string | `function_url` | The front door: `function_url` (Lambda function URL), `http_api` (API Gateway HTTP API) or `rest_api` (API Gateway REST API) (`DF6009`). | `infra` |
| `http.auth` | string | `iam` | A value the front door supports (`DF6009`, `DF6601`; see the table below). `none` needs `allow_public: true` (`DF6602`) and then warns (`DF6603`). `api_key` warns (`DF6607`). | `infra` |
| `http.allow_public` | boolean | `false` | Confirms `auth: none`. With any other `auth` it has no effect (`DF6604`). | `validate` |
| `http.domain` | mapping | none | Only with `http_api` or `rest_api` (`DF6605`). | `infra` |
| `http.domain.name` | string | required (`DF6008`) | A lowercase DNS name: at least two labels, each 1 to 63 letters, digits or `-`, not starting or ending with `-`, 253 characters at most (`DF6606`). | `infra` |
| `http.domain.routing` | string | `latency` | `latency`: Route 53 latency records across the regions, so one hostname serves every region and fails over (`DF6009`). | `infra` |

| Front door | `auth` values |
|------------|---------------|
| `function_url` | `iam` (SigV4-signed requests), `none` (public) |
| `http_api` | `iam`, `jwt` (Cognito or any OIDC provider), `lambda` (Lambda authorizer) |
| `rest_api` | `iam`, `api_key` (API keys with usage plans), `lambda`, `cognito` (Cognito user pool) |

The settings that `jwt`, `lambda`, `cognito` and `api_key` need (issuer, authorizer
function, user pool, usage plan limits), and the custom domain's certificate, are specified
with the HTTP query endpoints in M6.

## SQL file patterns

`sql` lists glob patterns. Dynaflux reads every file they match and classifies each file by
its statements (see [sql.md](sql.md)).

- Patterns are relative to the project root and use `/` on every operating system.
- A pattern must not be absolute or have a `..` segment (`DF6201`). An empty pattern, or one
  that ends in `/`, is `DF6202`. These checks come before the pattern is cleaned, so
  `./dynaflux/**/*.sql` works and `dynaflux/**/../*.sql` is an error.
- Each path segment uses Go's `path.Match` syntax: `*` (any characters except `/`), `?` (one
  character), `[a-z]` (a character class) and `\` (escape). Bad syntax is `DF6202`.
- `**` as a whole segment matches zero or more directories: `dynaflux/**/*.sql` matches both
  `dynaflux/a.sql` and `dynaflux/views/daily/b.sql`. `**` inside a segment, as in
  `dynaflux/**.sql`, is `DF6203`.
- A wildcard segment doesn't match a name that starts with `.` or `_`, unless the segment
  starts with that same character: `dynaflux/*.sql` skips `_draft.sql`, and
  `dynaflux/_*.sql` matches it. Wildcards never match a directory named `testdata`. These
  are the names the `go` command ignores. A name written literally always matches, as in
  `dynaflux/_drafts/*.sql`.
- Matching is case-sensitive: `*.sql` doesn't match `Q.SQL`.
- Patterns match regular files and links to regular files. Directories and other entries
  are skipped, even when their names match.
- Every pattern must match at least one file (`DF6204`). This includes the default pattern,
  so a project needs at least one SQL file.
- Files are read through the project root only. Links to directories aren't followed. A
  matched file that is a link is `DF6205` when the link is absolute or its target leaves the
  project root. A link whose target leaves the root through another link, such as a link
  to a directory outside, can't be read: `DF6002`.
- A file matched by several patterns is read once. Files are processed in byte order of
  their paths, so the output is deterministic.

## Resource names

Dynaflux derives every AWS resource name from `prefix`, `store` and the SQL names. The
pattern is `{prefix}{store}-{kind}-{name}`, all lowercase. Because the names are
deterministic, the CLI finds every resource from the project's files alone.

`validate` computes each name below for the current SQL and config, and checks it against
the AWS rules (`DF6110`, `DF6111`). View version numbers come from `dynaflux.lock`
([sql.md §9.9](sql.md#99-view-versions)), which `validate` reads from M2 and only
`dynaflux generate` writes:

- The table name of every version in the catalog, current and retained, is checked.
- For each view in the SQL files, the number the view would get if its definition is new is
  checked too: one more than the highest number in the lock for that view, or 1 when the
  lock doesn't list the view. That number has the most digits, so it gives the longest name.
- Without a lock file, every view is version 1.
- A lock that `validate` can't use, for example one with merge-conflict markers, is
  `DF6013`. Dynaflux never falls back to version 1 for it.

The examples use `prefix: prod-`, `store: analytics`, the stream `page_views` and version 2
of the view `page_daily`. In the **Budget** column, `P` is the length of `prefix` plus the
length of `store`.

| Resource | Name | Example | AWS rule | Budget |
|----------|------|---------|----------|--------|
| Stream table (Global Table) | `{prefix}{store}-stream-{stream}` | `prod-analytics-stream-page_views` | DynamoDB table: 3 to 255 characters from `A-Z a-z 0-9 _ - .` | P + stream ≤ 247 |
| View table (per version, per region) | `{prefix}{store}-view-{view}-v{N}` | `prod-analytics-view-page_daily-v2` | DynamoDB table | P + view + digits of N ≤ 247 |
| Meta table (per region) | `{prefix}{store}-meta` | `prod-analytics-meta` | DynamoDB table | P ≤ 250 |
| Projector function, `split: store` | `{prefix}{store}-projector` | `prod-analytics-projector` | Lambda function name: 64 characters from `A-Z a-z 0-9 _ -` | P ≤ 54 |
| Projector function, `split: stream` | `{prefix}{store}-projector-{stream}` | `prod-analytics-projector-page_views` | Lambda function name | P + stream ≤ 53 |
| Setup function | `{prefix}{store}-setup` | `prod-analytics-setup` | Lambda function name | P ≤ 58 |
| Query function | `{prefix}{store}-query` | `prod-analytics-query` | Lambda function name | P ≤ 58 |
| Backfill function | `{prefix}{store}-backfill` | `prod-analytics-backfill` | Lambda function name | P ≤ 55 |
| Backfill state machine | `{prefix}{store}-backfill` | `prod-analytics-backfill` | Step Functions: 80 characters; only `A-Z a-z 0-9 _ -` for CloudWatch Logs logging | P ≤ 71 |
| S3 bucket (per region) | `{prefix}{store}-{account}-{region}-an` | `prod-analytics-123456789012-us-east-1-an` | S3 bucket in the account regional namespace: 3 to 63 characters from `a-z 0-9 . -`, and no prefix that S3 reserves (`DF6112`); see the [bucket naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html) | P + region ≤ 46, for every region in `regions` |
| Firehose stream (per archived stream) | `{prefix}{store}-archive-{stream}` | `prod-analytics-archive-page_views` | 64 characters from `A-Z a-z 0-9 _ . -` | P + stream ≤ 55 |
| Glue database or S3 Tables namespace | `{prefix}{store}` with every `-` replaced by `_` | `prod_analytics` | Iceberg on Glue: `[a-z0-9_]`, 1 to 252 characters. S3 Tables: 1 to 255 characters, must not start with `aws` (`DF6111`). | P ≤ 252 |
| Iceberg table | `{stream}` | `page_views` | `[a-z0-9_]`, 1 to 255 characters | stream ≤ 255 |
| Athena view with one row per event | `{stream}_dedup` | `page_views_dedup` | 255 characters | stream ≤ 249 |
| Athena workgroup | `{prefix}{store}` | `prod-analytics` | 1 to 128 characters from `A-Z a-z 0-9 _ . -` | P ≤ 128 |
| Backfill workgroup (Athena) | `{prefix}{store}-backfill` | `prod-analytics-backfill` | Athena workgroup | P ≤ 119 |
| Runtime environment variables | `DYNAFLUX_STORE={store}`, `DYNAFLUX_PREFIX={prefix}`, `DYNAFLUX_MODE` (`projector`, `setup`, `query` or `backfill`), and for projectors in archive regions `DYNAFLUX_ARCHIVE_SCHEMA` (the archive schema's fingerprint) | | | |

- The tightest limits are the bucket name, P + region ≤ 46 for every region in `regions`
  (P ≤ 37 with `us-east-1`, and P ≤ 32 with a 14-character region such as
  `ap-northeast-1`), and the 64-character Lambda function name, P ≤ 54.
- For every archived stream, P + stream ≤ 55 (its Firehose name). With `split: stream`,
  every stream also needs P + stream ≤ 53 (its projector function).
- Stream names that end in `_dedup` are reserved for the Athena views (DF2101 in
  [sql.md](sql.md)).
- Changing `store` or `prefix` renames every resource. CloudFormation replaces a DynamoDB
  table whose name changes, so the renamed tables start empty. Treat both keys as fixed once
  a store is deployed.
- Names are unique within one store, but two stores in the same account and region can
  collide, because `store` may contain `-`. This happens only when one store's
  `{prefix}{store}` is another's followed by `-stream`, `-view`, `-projector` or
  `-backfill`. For example, the meta table of store `analytics-stream` is
  `analytics-stream-meta`, which is also the table of stream `meta` in store `analytics`,
  and the Athena workgroup of store `analytics-backfill` is the backfill workgroup of store
  `analytics`. `validate` sees one store, so it can't detect this, and the second deploy
  fails. Avoid store names whose last `-` part is `stream`, `view`, `projector` or
  `backfill`.

### Names that `validate` doesn't check

`regional.yaml` is deployed in every region of one account, so a name that must be unique
in the account includes the region or is left to CloudFormation.
[infra.md](infra.md#3-names-tags-and-logical-ids) lists every name.

| Resource | Name | AWS rule |
|----------|------|----------|
| IAM roles | Chosen by CloudFormation | Unique in the account, which IAM shares across regions. |
| CloudWatch dashboard (per region) | `{prefix}{store}-{region}` | Unique in the account. 255 characters from `A-Z a-z 0-9 - _`. |
| CloudWatch alarms | `{resource name}-{signal}`, or `{prefix}{store}-{signal}` for the store-wide ones | 255 characters. |
| Log groups | `/aws/lambda/{function}` and `/aws/kinesisfirehose/{prefix}{store}-archive` | 512 characters. |
| Tags | `dynaflux:store`, `dynaflux:kind` and `dynaflux:name`; view tables also carry `dynaflux:version`, the view version number, and `dynaflux:fingerprint` | Values of at most 256 characters. |

The dashboard and log group names always fit once the function names do. `generate` fails
if an alarm name would be too long.

## Diagnostics

Config diagnostics use the same format as [SQL diagnostics](sql.md#a1-output-format):

```
dynaflux.yaml:15:14: error DF6010: runtime.memory_mb is 64; it must be between 128 and 10240
  hint: Lambda functions take 128 to 10240 MB of memory
```

- Lines and columns start at 1 and point at the value at fault.
  - For an unknown or duplicate key, and for a `tables` or `archive.streams` key that names
    nothing (`DF6407`, `DF6503`), they point at the key.
  - For a missing key, they point at the mapping that should hold it, or at the entry's key
    when the entry is null. An empty file points at 1:1.
  - `DF6013` points at the parse error in `dynaflux.lock`, and at line 1, column 1 for its
    other problems. `DF6015` points at line 1, column 1 of `dynaflux.lock`, and so does
    `DF6110` for a view that only the lock still holds.
- Rules about a derived name point at the stream or view name in its SQL file when the
  resource belongs to one that the SQL defines, and at `store` otherwise.
- `DF6403` points at `defaults.log_retention`. `DF6504` points at
  `archive.streams.<name>.enabled` when that entry turns archiving off, and at the stream
  name in its SQL file otherwise.
- `DF6104` points at the region in `regions`. `DF6505` points at the region in
  `archive.regions`, or in `regions` when `archive.regions` is `all`.
- `DF6016` points at line 1, column 1 of `dynaflux.lock`, `DF6017` and `DF6018` at
  `regions`, and `DF6019` at line 1, column 1 of `dynaflux.yaml`.
- Output is sorted by path, line, column, code and message.
- Codes are stable: a code keeps its meaning and is never reused.

`validate` runs these steps in order:

1. Find and parse `dynaflux.yaml`: `DF6001` to `DF6004`, `DF6006` and `DF6014`. It stops
   on an error, so a duplicate key can hide other mistakes until it's fixed.
2. Check every key, and the names built from `prefix`, `store` and `regions` alone:
   `DF6111`, `DF6112`, and `DF6110` for the meta table, the store-wide functions, the state
   machine, the Glue database, the Athena workgroup and the bucket in each region. It
   reports every diagnostic of this step, then stops if any is an error.
3. Expand the `sql` patterns (`DF6204`, `DF6205`), and read `dynaflux.lock` when it
   exists (`DF6002`, `DF6013`). It stops on an error.
4. Read and check the SQL files ([sql.md](sql.md)), then run the checks that need both:
   `DF6110` for names that include a stream or view, `DF6403`, `DF6407`, `DF6503`,
   `DF6504`, `DF6505`, and `DF6015`, which also needs the lock. These use every name the
   SQL defines, even in a statement with errors.

`dynaflux generate` runs the same steps, checks the size of the catalog (`DF6019`), then
compares its output with the templates it wrote last time: `DF6016` to `DF6018`
([infra.md](infra.md#13-checks-in-generate)).

| Exit status | Meaning |
|-------------|---------|
| 0 | No errors. Warnings are allowed. |
| 1 | At least one error. |
| 2 | A usage error, or `dynaflux.yaml` wasn't found, or a file couldn't be read (`DF6001`, `DF6002`). |

### Codes

Unlike `DF1xxx` to `DF5xxx`, a `DF6xxx` code's number doesn't tell its severity: see the
Severity column. The `DF7xxx` codes of the commands that read and change a deployed store,
such as `status` and `backfill`, are in [lifecycle.md §12](lifecycle.md#12-diagnostics).

| Code | Severity | Meaning | Hint |
|------|----------|---------|------|
| DF6001 | error | No `dynaflux.yaml` in the working directory or any parent. | Run the command inside your project, or create `dynaflux.yaml` at its root. |
| DF6002 | error | A file can't be read: `dynaflux.yaml`, a file matched by `sql`, or, from M2, `dynaflux.lock` when it exists. The message carries the operating system's error. | Check the path and its permissions. |
| DF6003 | error | YAML syntax error. The message carries the parser's error. | Fix the YAML at the position shown. |
| DF6004 | error | The file isn't one YAML mapping: it's empty or has only comments, it has more than one document, or its top level is a list or a scalar. | Keep one document of `key: value` lines. |
| DF6005 | error | Unknown key. | `did you mean "<key>"?` when a known key is close; otherwise the keys allowed there. |
| DF6006 | error | Duplicate key. | Keep one; the message gives the line of the first. |
| DF6007 | error | Wrong YAML type, such as a string where a list is needed, `yes` for a boolean, `48` for a duration, a null list entry, or a list or a mapping as a key. See [Scalars](#scalars). | The expected form, such as `regions: [us-east-1]`. |
| DF6008 | error | A required key is missing. | An example value. |
| DF6009 | error | The value isn't one of the allowed values. | The allowed values. |
| DF6010 | error | An integer is out of range. | The range and where it comes from, such as the Lambda limit. |
| DF6011 | error | A list that needs at least one entry is empty. | Add at least one entry, or, for a key with a default, remove the key. |
| DF6012 | error | A list has the same entry twice. | Remove the repeat. |
| DF6013 | error | `dynaflux.lock` can't be used: it isn't valid JSON in the lock format (for example, it has merge-conflict markers), it has an unknown field or `format`, a key that appears twice in one object (as "accept both" in a merge tool can leave) or isn't in lower case, a view repeats a version number, an entry's definition doesn't match its fingerprint (a hand edit), a `retired` entry holds more than its `number`, `state` and `fingerprint`, or a rebuild count is below 1, names a view the lock doesn't hold, or is below the count of one of the view's versions (a merge lost it). The message gives the reason. Reported from M2. | If it has merge conflicts, keep the lock from the branch that merged first, then run `dynaflux generate`. Undo hand edits. Don't delete the file: that renumbers every view. |
| DF6014 | error | The file uses an anchor (`&a`), an alias (`*a`), a merge key (`<<`) or an explicit tag (`!!str`, `!x`). | Write each value out in full. Quote a string instead of tagging it. |
| DF6015 | error | A view version that `dynaflux.lock` retains can't be maintained: the stream it reads is gone, the stream no longer has a column it reads with the type it reads, the stream now has a column named like the version's `ARRAY JOIN` alias (the column would win), or a stream now has the view's name. The message names the version and the reason. | Restore the stream or the column, or rename the stream or the new column; or retire the version with `dynaflux retire`. |
| DF6016 | error | Reported by `generate` only. A view version's table in the `regional.yaml` that `generate` wrote last time is missing from the new one, and `dynaflux.lock` doesn't mark the version retired; or a version keeps its number but its fingerprint changed. Deploying would delete a table the application reads, or write rows of a new definition into an old version's table. This happens when the lock was lost, reverted or merged badly. | Restore `dynaflux.lock` from version control and run `dynaflux generate` again. To start over on purpose, delete the old templates first. |
| DF6017 | error | Reported by `generate` only. `regions` differs by more than one region from the replica regions of the `global.yaml` that `generate` wrote last time. CloudFormation adds or removes one Global Table replica per stack update. | Add or remove one region, generate and deploy `global.yaml`; then the next region. |
| DF6018 | error | Reported by `generate` only. `regions` drops a region whose stream table replicas have deletion protection in the `global.yaml` that `generate` wrote last time. Turning protection off is the first step of removing a region. | Put the region back, run `dynaflux generate -unprotect-region <region>` and deploy `global.yaml`; then remove the region. |
| DF6019 | error | Reported by `generate` only. The catalog takes more than 350,000 bytes compressed with gzip; the message gives its size. Each region's meta table stores the deployed catalog in one item, which DynamoDB limits to 400 KB ([encoding.md §8.4](encoding.md#84-deployed-catalog)). A store within CloudFormation's limit of 500 resources per stack stays below it. | Retire view versions that are no longer needed, remove views, queries or streams, or split the store. |
| DF6101 | error | `store` isn't a valid store name. | Use lowercase letters, digits and `-`; start with a letter; don't end with `-`. For example `analytics`. |
| DF6102 | error | `prefix` isn't a valid prefix. | Use lowercase letters, digits and `-`; start with a letter; end with `-`. For example `prod-`. |
| DF6103 | error | A region isn't an AWS region code, or is a FIPS pseudo-region such as `fips-us-east-1`, which the AWS SDK rewrites to another name ([ADR 0014](../decisions/0014-sync-views-in-the-append-transaction.md)). | Use a code such as `us-east-1`. For FIPS endpoints, set `AWS_USE_FIPS_ENDPOINT=true`. |
| DF6104 | error | `regions` holds `me-south-1` or `me-central-1`, which don't offer the S3 account regional namespace, so Dynaflux can't name its bucket there. | Remove the region. |
| DF6110 | error | A derived resource name is longer than the AWS limit. The message gives the name, its length and the limit. | Shorten the stream or view name, `store` or `prefix`. The hint gives the budget from [Resource names](#resource-names). |
| DF6111 | error | With `archive.catalog: s3tables`, the S3 Tables namespace starts with `aws`, which S3 Tables reserves. | Change `prefix` or `store`, or use `archive.catalog: glue`. |
| DF6112 | error | `{prefix}{store}` starts with `xn--`, `sthree-` or `amzn-s3-demo-`, which S3 reserves at the start of bucket names. | Change `prefix` or `store`. |
| DF6201 | error | An `sql` pattern is absolute or has a `..` segment. | Write the pattern relative to the project root. |
| DF6202 | error | An `sql` pattern has bad syntax, such as an unclosed `[`, or is empty or ends in `/`. | Patterns use `*`, `?`, `[a-z]`, and `**` for any number of directories. |
| DF6203 | error | `**` is part of a path segment, as in `dynaflux/**.sql`. | Write `dynaflux/**/*.sql`. |
| DF6204 | error | An `sql` pattern matches no files. | Patterns are relative to the project root (the message gives it), and wildcards skip names that start with `.` or `_`, and `testdata`. |
| DF6205 | error | A matched file is a link that is absolute, or whose target leaves the project root. A link that leaves through another link is `DF6002`. | Move the file into the project. |
| DF6301 | error | `go.package` isn't a valid package name, or is `dynaflux`, `runtime` or `log`. | Use a short lowercase name such as `flux`. |
| DF6302 | error | An output path is empty or absolute, contains `\` or a `..` segment, or is the project root. | Use a directory below the project root, such as `internal/flux`. |
| DF6303 | error | Two of `go.out`, `runtime.out` and `infra.out` are the same directory. | Give each output its own directory. |
| DF6304 | error | `runtime.out` can't import `go.out`: `go.out` has an `internal` segment, and `runtime.out` is outside the directory that holds it. | Move `runtime.out` inside that directory (the message names it), or move `go.out`. |
| DF6305 | error | `runtime.prune_window_seconds` is less than `runtime.timeout_seconds` + 60. The projector could drop a high-water mark that a running invocation still needs, and count some events twice. | Set it to at least `timeout_seconds` + 60 (the message gives the number), or remove it to use the default, 960. |
| DF6306 | warning | `runtime.prune_window_seconds` is less than 960. If you later lower it together with `timeout_seconds` in one deploy, invocations started under the old timeout can outlive the new window. | Remove it to use the default, 960, which is safe for every timeout. |
| DF6401 | error | A value isn't a valid duration. | Write a whole number and one unit, such as `36h`, `2d` or `1w`, or write `forever`. |
| DF6402 | error | `defaults.log_retention` is shorter than 1 day (`24h`) or longer than 36525 days (`36525d`). DynamoDB deletes expired items lazily, often days late, and a region cut off from the others may delete events before they replicate, so shorter windows are refused. | Use at least `1d`. For more than 36525 days, use `forever`. |
| DF6403 | warning | At least one stream inherits `defaults.log_retention: forever`. Every event stays in DynamoDB, so the table, and the cost of projecting it, keep growing. The message lists the streams. | Keep a short log (the default is `2d`) and use the archive as the permanent record. `dynaflux explain` shows the cost. |
| DF6404 | error | `capacity: provisioned` without `autoscaling`. | Add `autoscaling: {read: {min: 5, max: 100}, write: {min: 5, max: 100}}` with your own numbers. |
| DF6405 | error | `autoscaling` is set, but `capacity` is `on_demand`. | Remove `autoscaling`, or set `capacity: provisioned`. |
| DF6406 | error | An autoscaling `min` is greater than its `max`. | Set `min` to at most `max`. |
| DF6407 | error | A `tables` key names no stream or view. | `did you mean "<name>"?` when a name is close; otherwise the stream and view names the SQL files define. |
| DF6501 | error | `archive.regions` lists a region that isn't in `regions`. | Add it to `regions`, or remove it from `archive.regions`. |
| DF6502 | error | `archive.events: local` without `archive.regions: all`. With `local`, events accepted in a region that doesn't archive would never reach the archive. | Set `archive.regions: all`, or `archive.events: all`. |
| DF6503 | error | An `archive.streams` key names no stream. | `did you mean "<name>"?` when a name is close; otherwise the stream names the SQL files define. |
| DF6504 | warning | A stream keeps events for a limited time and isn't archived. Older events are deleted for good, and new views can be backfilled only from the events still in DynamoDB. | Archive the stream, or give it `LOG RETENTION FOREVER`. |
| DF6505 | error | At least one stream is archived, and an archive region doesn't support Firehose delivery to Apache Iceberg Tables or doesn't offer AWS Glue table optimizers. The message names the region and the missing service. | Remove the region from `archive.regions`. With `archive.regions: all`, list the regions to archive in. |
| DF6506 | error | `archive.catalog: s3tables`. Dynaflux doesn't support Amazon S3 Tables yet. | Use `archive.catalog: glue`, the default. |
| DF6601 | error | `http.auth` isn't supported by `http.frontend`. | The `auth` values this front door supports. |
| DF6602 | error | `http.auth: none` without `http.allow_public: true`. | Set `allow_public: true` to confirm that anyone may call the exposed queries, or use `auth: iam`. |
| DF6603 | warning | `http.auth` is `none`: anyone with the URL can call every query annotated `-- expose: http`. Reported whether or not a query is exposed yet. | Use `auth: iam` unless the queries must be public. |
| DF6604 | warning | `http.allow_public: true` has no effect, because `auth` isn't `none`. | Remove `allow_public`. |
| DF6605 | error | `http.domain` is set with `frontend: function_url`. | Function URLs have no custom domain option. Use `frontend: http_api` for one hostname across regions. |
| DF6606 | error | `http.domain.name` isn't a valid DNS name. | Use lowercase labels of letters, digits and `-`, such as `flux.example.com`. |
| DF6607 | warning | `http.auth: api_key`. AWS advises against using API keys for authentication or authorization: they identify clients for usage plans. | Use `iam`, `lambda` or `cognito` to control access. |

## Implementation notes

- `internal/config` parses `dynaflux.yaml` with `go.yaml.in/yaml/v4` into a `yaml.Node`
  tree, with `yaml.WithUniqueKeys(false)` so that a duplicate key is `DF6006`, not a syntax
  error. It walks the tree to find duplicate keys, anchors, aliases, merge keys and tags,
  and stops if it finds any.
- Then it walks the tree again and reads every key by hand. Decoding into structs with
  `yaml.WithKnownFields()` doesn't work: the library's errors carry no `DF6xxx` code and
  don't list the keys allowed where an unknown key is, and the library accepts spellings
  the [scalar rules](#scalars) reject. It decodes `yes` as `true`, `1024.0` and `0x400` as
  `1024`, and a plain `123` into a string. The walk checks each scalar's tag, style and
  value instead.
- The `sql` walk runs on `root.FS()` of an `os.Root` opened at the project root, which
  refuses paths and links that escape it.
- No check calls AWS.

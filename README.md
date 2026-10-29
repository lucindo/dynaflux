<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Dynaflux

Dynaflux is a Go library and CLI for event-sourced fact streams and materialized views on
DynamoDB Global Tables. You declare streams, views and queries in ClickHouse-flavored SQL.
Dynaflux rejects, at build time, any query that DynamoDB can't serve efficiently, and
estimates what each event and query costs. It generates a typed Go client and the
CloudFormation templates that run your store active-active in several AWS regions. None of
it has run on AWS yet: see [Status](#status).

## Why

DynamoDB Global Tables is one of the few managed databases that runs active-active across
regions, with little operational work, even during a region outage. Its constraints are hard
for developers who come from SQL, though. A common mistake is hand-written code that writes
many items per event to serve every lookup.

ClickHouse gives developers a productive model: append-only events, materialized views that
update on insert, and pre-registered queries with parameters. But ClickHouse doesn't run
active-active across regions. Dynaflux aims to give the ClickHouse model, for the subset
DynamoDB serves well, with DynamoDB's multi-region guarantees. Its design follows a few
rules:

- **Multi-region first.** Every region accepts writes, and the surviving regions keep
  working during an outage, with no coordination between regions.
- **Pay at write time, not read time.** Views do the work as events arrive, so every read is
  a key lookup or a bounded range read.
- **Events never change.** All derived state can be rebuilt from them.
- **A library, not a service.** No Dynaflux server sits in the request path, and you own the
  generated infrastructure.

Every query is declared ahead of time and there are no joins. For ad-hoc questions, Athena
reads the archive of every event that Dynaflux keeps in S3.

## What you get

- `dynaflux validate` checks a project and reports each problem with its file, line and a
  hint on how to fix it. `dynaflux explain` shows the table layout and the estimated cost of
  a stream, view or query.
- `dynaflux generate` writes a typed Go client package, the `main.go` of the Lambda binary
  that runs the store's functions, and the CloudFormation templates. It numbers view
  versions in `dynaflux.lock`, a file you commit.
- The library appends events and runs queries. `DEDUP BY` makes appends safe to retry,
  `SYNC` views update in the append's transaction, and wait tokens let an application read
  its own writes.
- In each region, a Lambda function, the projector, keeps the views current. It also
  archives every event to Apache Iceberg tables on S3, for Athena and for backfills.
- The operator commands work on a deployed store: `status`, `diff`, `backfill`, `retire`,
  `inspect`, `query` and `import`.
- `dynaflux dev` runs the store on DynamoDB Local while you build the application, and
  `fluxtest` gives its tests an in-memory store.
- A query marked `-- expose: http` is served over HTTP, for clients in other languages.

## Status

Dynaflux is preparing its first release, v1.0.0. Release candidates come first, tagged
`v1.0.0-rc.1` and up. [CHANGELOG.md](CHANGELOG.md) lists what each release changes.

Everything above is tested without AWS: unit tests, a simulation of three regions with
faults, integration tests on DynamoDB Local, and an end-to-end suite that runs the
[example store](examples/analytics) on DynamoDB Local. Nothing has run on AWS yet. Before
v1.0.0, a release candidate must pass two runs on AWS: the end-to-end suite, and the
[getting started](docs/guide/getting-started.md) guide followed from the first command to
the teardown.

Until v1.0.0, anything may still change. From v1.0.0 on, every v1.x release keeps the Go
API, your projects, the stored data, the AWS resource names, the CLI's commands and flags,
and the wire formats compatible, as the
[compatibility promise](docs/spec/compatibility.md) lists.

## Install

Dynaflux needs Go 1.26 or later. A project that uses it is a Go module. In the module's
directory, add the CLI as a Go tool, so that `go.mod` pins its version and the library's:

<!-- doctest: skip -->
```sh
go get -tool github.com/lucindo/dynaflux/cmd/dynaflux@latest
go tool dynaflux version
```

`version` shows the versions of the CLI, of the library that `go.mod` requires, and of the
code that `generate` writes.

Or install the CLI on your `PATH`. Then `dynaflux init` tells you to add the library to
your module with `go get`:

```sh
go install github.com/lucindo/dynaflux/cmd/dynaflux@latest
```

## Example

This example counts page views. The commands use `dynaflux`; if you added it as a tool, run
`go tool dynaflux` instead.

At the root of your project, create `dynaflux.yaml` with the store's name and its regions:

<!-- doctest: file dynaflux.yaml -->
```yaml
store: analytics
regions: [us-east-1, us-west-2]
```

Then create `dynaflux/pages.sql` with a stream of page views, a view with daily totals per
page, and a query over that view:

<!-- doctest: file dynaflux/pages.sql -->
```sql
CREATE STREAM page_views (
    user_id     String,
    page        String,
    duration_ms UInt32
)
PRIMARY KEY (user_id);

CREATE MATERIALIZED VIEW page_daily
ORDER BY (page, day)
PRIMARY KEY (page)
AS SELECT
    page,
    toStartOfDay(event_time) AS day,
    count()                  AS views,
    uniqExact(user_id)       AS visitors,
    avg(duration_ms)         AS avg_ms
FROM page_views
GROUP BY page, day;

-- name: PageStats :many
SELECT day, views, visitors, avg_ms
FROM page_daily
WHERE page = {page:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;
```

- The stream's `PRIMARY KEY` groups events by user. Every event also has implicit columns,
  such as the `event_time` the view uses.
- The view keeps one row per page and day. Its `PRIMARY KEY` is the DynamoDB partition key,
  and the rest of `ORDER BY` sorts rows within a partition.
- `PageStats` fixes the partition key and takes a range of days, so it costs one DynamoDB
  `Query` on one partition.

Check the project. No output means no problems:

```console
$ dynaflux validate
```

Ask what the stream and its view cost at 100 events per second:

```console
$ dynaflux explain -rate 100 page_views
stream page_views
  assumptions  -rate 100  -event-kb 1  -rows page_daily=1000  -uniq-new page_daily=0.3
               (traffic isn't in the SQL or dynaflux.yaml; set it with these flags)
  prices       us-east-1 list prices, ±30%: for comparing designs, not a quote

layout, in each of 2 regions (us-east-1, us-west-2)
  table        analytics-stream-page_views: Global Table, on-demand
  key          pk user_id; sk event_time, event_id
  log          LOG RETENTION 2 DAY: ~19 GB in ~2 partitions, each a change-stream shard
  projector    batching window 1 s, batches of at most 100 events: ~2 invocations/s, 50 events each on average
  marks        a view row keeps a shard's high-water mark for 960 s after the shard's progress passes it (runtime.prune_window_seconds)
  archive      Firehose to Iceberg in us-east-1, us-west-2

per event, per region                                     write units  read units
  event put, local or replicated                                 1.00
  TTL delete, replicated from the region that expires it         0.50
  page_daily, shared rows, 1,000 active                          1.07        1.07
    uniqExact markers                                            0.30        1.00
  progress item, one per batch                                   0.02
  total                                                          2.89        2.07
  per second, all events                                          289         207

monthly                        per region  2 regions
  DynamoDB requests                  $543     $1,087
    stream and meta tables           $250       $499
    page_daily                       $294       $587
  DynamoDB storage of the log       $4.75      $9.50
  Lambda, the projector               $10        $20
  Firehose                            $14        $28
  S3 archive, year-1 average        $7.25        $15
  replication transfer              $2.63      $5.26
  total                              $582     $1,165
  provisioned capacity at a 70% target would cut requests to $224 per region (-59%), but costs more than on-demand if it must cover peaks over 2.42 times the average
  not modeled: query reads, view and marker storage, backfills, archive compaction, CloudWatch
```

`dynaflux explain page_daily` and `dynaflux explain PageStats` show the view and the query
on their own.

Now add a query that DynamoDB can't serve without scanning every page. Create
`dynaflux/top_pages.sql`:

<!-- doctest: file dynaflux/top_pages.sql -->
```sql
-- name: TopPages :many
SELECT page, views
FROM page_daily
WHERE day = {day:Date}
ORDER BY views DESC
LIMIT 10;
```

`validate` rejects it, and exits with status 1:

<!-- doctest: exit 1 -->
```console
$ dynaflux validate
dynaflux/top_pages.sql:4:1: error DF4101: TopPages: view page_daily is partitioned by (page), but WHERE does not bind "page" with = or IN. Every query must fix the partition key.
  hint: add page = {page:String} to WHERE, or declare a view whose PRIMARY KEY matches this query
```

[Getting started](docs/guide/getting-started.md) builds a store like this one step by step,
runs it on your machine, then deploys it to two AWS regions.

## Documentation

The guides:

- [Getting started](docs/guide/getting-started.md): build a store, run it on your machine,
  deploy it to two AWS regions, and delete it.
- [Concepts](docs/guide/concepts.md): the parts of a store, how an event flows, and what to
  expect across regions and during an outage.
- [Writing the application](docs/guide/application.md): the generated Go client, values,
  errors and tests.
- [SQL recipes](docs/guide/dialect.md): streams, views and queries for common tasks.
- [Operating a store](docs/guide/operations.md): `status`, change procedures, backfills,
  alarms and the archive.
- [What a store costs](docs/guide/cost.md): where the money goes, the settings that lower
  it, and how to read `explain`.

[docs/README.md](docs/README.md) also lists the specifications, which define what Dynaflux
accepts and does, and the architecture decision records.
[examples/analytics](examples/analytics) is a larger store in two regions, with every
generated file committed.

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) explains how to build and test Dynaflux, and how to keep
the docs' outputs current.

## License

Apache License 2.0. See [LICENSE](LICENSE).

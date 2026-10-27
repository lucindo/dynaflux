<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Dynaflux

Dynaflux is a Go library and CLI for event-sourced fact streams and materialized views on
DynamoDB Global Tables. You declare streams, views and queries in ClickHouse-flavored SQL.
Dynaflux rejects, at build time, any query that DynamoDB can't serve efficiently, and
estimates what each event and query costs. The goal is to turn these definitions into
typed Go code and infrastructure that run active-active in several AWS regions. None of it
has run on AWS yet: see [Status](#status).

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

Every query is declared ahead of time and there are no joins. Ad-hoc exploration will run in
Athena, over an archive of events that Dynaflux writes to S3.

## Status

Dynaflux is in early development, at milestone M5: the operator commands. What works:

- The SQL dialect and `dynaflux.yaml` are specified in [docs/spec](docs/spec).
- `dynaflux validate` checks a project and reports each problem with its file, line and a
  hint on how to fix it.
- `dynaflux explain` shows the table layout and the estimated cost of a stream, view or
  query.
- `dynaflux generate` writes the typed Go client package of a project and numbers its view
  versions in `dynaflux.lock`, a file you commit. It also writes the store's AWS
  infrastructure as CloudFormation templates ([docs/spec/infra.md](docs/spec/infra.md)),
  and the `main.go` of the Lambda binary that its functions run.
- The generated client appends events and runs queries against an in-memory store from
  `fluxtest`, with the projector keeping views current.
  [examples/analytics](examples/analytics) compiles and passes its tests this way.
- `dynaflux.Open` returns a store backed by DynamoDB. The Lambda binary runs the projector,
  which keeps views current and archives events through Firehose, the setup function that
  the templates' custom resources call, and the backfill function that each region's state
  machine steps. The integration tests run the store, the projector and the commands against
  DynamoDB Local.
- The operator commands work on a deployed store, one environment per run. `status` shows
  each view version's state in each region and what needs an action, `diff` compares the
  project with the deployed store, `backfill` fills new view versions so they go live, and
  `retire` removes the ones no longer current. `inspect` and `query` read events, rows and
  named queries, and `import` loads past events.
  [docs/spec/lifecycle.md](docs/spec/lifecycle.md) specifies them.
- `dynaflux init` starts a project, and `dynaflux dev` runs its store on DynamoDB Local for
  an application under development.

Nothing has run on AWS yet. Milestone M6 adds `SYNC` views and wait tokens, backfills that
read the archive of a stream that already has events, and HTTP query endpoints. Until v1.0,
the dialect, the configuration and the commands may change.

## Install

Dynaflux needs Go 1.26 or later. There is no tagged release yet, so `@latest` installs the
newest commit on `main`.

Add the CLI to your project as a Go tool, so its version is pinned in your `go.mod`:

```sh
go get -tool github.com/lucindo/dynaflux/cmd/dynaflux@latest
go tool dynaflux help
```

Or install it on your `PATH`:

```sh
go install github.com/lucindo/dynaflux/cmd/dynaflux@latest
```

## Example

This example counts page views. The commands use `dynaflux`; if you added it as a tool, run
`go tool dynaflux` instead.

At the root of your project, create `dynaflux.yaml` with the store's name and its regions:

```yaml
store: analytics
regions: [us-east-1, us-west-2]
```

Then create `dynaflux/pages.sql` with a stream of page views, a view with daily totals per
page, and a query over that view:

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

```sql
-- name: TopPages :many
SELECT page, views
FROM page_daily
WHERE day = {day:Date}
ORDER BY views DESC
LIMIT 10;
```

`validate` rejects it, and exits with status 1:

```console
$ dynaflux validate
dynaflux/top_pages.sql:4:1: error DF4101: TopPages: view page_daily is partitioned by (page), but WHERE does not bind "page" with = or IN. Every query must fix the partition key.
  hint: add page = {page:String} to WHERE, or declare a view whose PRIMARY KEY matches this query
```

## Documentation

- [docs/spec/sql.md](docs/spec/sql.md): the SQL dialect, with every diagnostic code.
- [docs/spec/config.md](docs/spec/config.md): the `dynaflux.yaml` reference.
- [docs/spec/encoding.md](docs/spec/encoding.md): how Dynaflux stores data in DynamoDB.
- [docs/spec/infra.md](docs/spec/infra.md): the CloudFormation templates that
  `dynaflux generate` writes.
- [docs/spec/lifecycle.md](docs/spec/lifecycle.md): view versions from deploy to removal,
  and the operator commands.
- [docs/decisions](docs/decisions/README.md): architecture decision records.
- [docs/README.md](docs/README.md): an index of all the docs.

## Development

- `make help` lists the development targets. `make verify` runs the formatters,
  `go mod tidy`, a check that no module needs a Go newer than 1.26, the linters, the
  vulnerability check, the tests and a `linux/arm64` build of the example's Lambda binary.
  If the linter reports an unknown linter, your `golangci-lint` is older than
  `.golangci.yml`: run `make tools-update`.
- `Float64` results must be the same on every CPU, so `make cross-arch` runs the
  cross-architecture tests as amd64: natively on an amd64 machine, and through Rosetta on
  Apple silicon. It isn't part of `make verify`.
- A test fails when `dynaflux generate` would change the files generated in
  [examples/analytics](examples/analytics): its Go code, CloudFormation templates and lock.
  After changing what generate writes, run `go run ../../cmd/dynaflux generate` in that
  directory.
- `make test` runs a smoke tier of the simulation harness. `make sim` and `make sim-controls`
  run its full tier and its negative controls, as
  [docs/runbooks/simulation.md](docs/runbooks/simulation.md) describes.
- `make integration` runs the packages that have `*_integration_test.go` files against
  DynamoDB Local in Docker. Each run recreates the container, which takes 1–2 s, so it
  starts from an empty emulator: DynamoDB Local keeps about 200 MB from every run even after
  the tests delete their tables, and its requests time out once it holds a few GB. Run one
  at a time on a machine, because a second run would recreate the emulator under the first.
  The container keeps running afterwards; `make emulators-down` stops it, and
  `make emulators-up` starts it without recreating a running one, for tests you run by hand.
  These tests never call AWS: they reach DynamoDB Local at the loopback address in
  `DYNAFLUX_DYNAMODB_ENDPOINT`, which the target sets, and fail without it. CI runs
  `make verify` and `make integration` on every push and pull request. The lane takes about
  25 s on an Apple M2 Max, or 40 s with an empty build cache; it must stay under 3 minutes.
  `runtime`'s tests also check every DynamoDB and Firehose call the projector made against
  the projector's role in the templates: a call the role doesn't allow fails the run and,
  after a run of all the tests, so does an action the role allows that no call used.

## License

Apache License 2.0. See [LICENSE](LICENSE).

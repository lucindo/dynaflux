<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# What a store costs

Almost all of a Dynaflux store's bill is DynamoDB requests. This guide shows where the
money goes, why the defaults are what they are, which settings lower the bill, and how to
read `dynaflux explain`, which estimates the cost of your own design before you deploy it.

Every number here comes from `explain`'s model, at us-east-1 list prices, ±30%. None of
them has been measured on AWS yet: [section 7](#7-measured-against-the-model) will compare
the model with measurements once there are some.

<!--
The prose quotes numbers from the explain outputs next to it. When go test -run TestDocs
-update changes an output, update those numbers too.
-->

## The example store

The examples use the page-views store that `dynaflux init` writes, the same one as in the
getting-started guide: one stream, `page_views`, and one view, `page_daily`, with page
views, unique visitors and average time per page and day, in two regions. The examples edit
its files to compare designs, so run them in a new, empty directory, not in a project you
deploy:

```sh
go mod init example.com/shop
go get -tool github.com/lucindo/dynaflux/cmd/dynaflux@latest
go tool dynaflux init -store shop -region us-east-1 -region us-west-2
```

`explain` reads only the project's files. It never calls AWS.

## 1. Where the money goes

Without `-rate`, `explain` assumes 1,000 events a second, about 86 million a day:

```console
$ go tool dynaflux explain page_views
stream page_views
  assumptions  -rate 1000  -event-kb 1  -rows page_daily=1000  -uniq-new page_daily=0.3
               (traffic isn't in the SQL or dynaflux.yaml; set it with these flags)
  prices       us-east-1 list prices, ±30%: for comparing designs, not a quote

layout, in each of 2 regions (us-east-1, us-west-2)
  table        shop-stream-page_views: Global Table, on-demand
  key          pk user_id; sk event_time, event_id
  log          LOG RETENTION 2 DAY: ~190 GB in ~20 partitions, each a change-stream shard
  projector    batching window 1 s, batches of at most 100 events: ~20 invocations/s, 50 events each on average
  marks        a view row keeps a shard's high-water mark for 960 s after the shard's progress passes it (runtime.prune_window_seconds)
  archive      Firehose to Iceberg in us-east-1, us-west-2

per event, per region                                     write units  read units
  event put, local or replicated                                 1.00
  TTL delete, replicated from the region that expires it         0.50
  page_daily, shared rows, 1,000 active                          1.07        1.07
    uniqExact markers                                            0.30        1.00
  progress item, one per batch                                   0.02
  total                                                          2.89        2.07
  per second, all events                                        2,893       2,073

monthly                        per region  2 regions
  DynamoDB requests                $5,434    $10,867
    stream and meta tables         $2,497     $4,993
    page_daily                     $2,937     $5,874
  DynamoDB storage of the log         $48        $95
  Lambda, the projector              $102       $203
  Firehose                           $142       $284
  S3 archive, year-1 average          $73       $145
  replication transfer                $26        $53
  total                            $5,824    $11,647
  provisioned capacity at a 70% target would cut requests to $2,242 per region (-59%), but costs more than on-demand if it must cover peaks over 2.42 times the average
  not modeled: query reads, view and marker storage, backfills, archive compaction, CloudWatch
```

Of the $5,824 a month per region, $5,434 is DynamoDB requests: 93%. Storage of the log, the
projector's Lambda function, Firehose, the S3 archive and replication transfer share the
rest. A view adds requests and almost nothing else, so with more views the share of
requests grows.

So a design costs what its requests cost, and a request's price is set by the units it uses.
DynamoDB bills a *write unit* for each write of an item up to 1 KB, and one more for each
further KB, rounded up. A *read unit* reads up to 4 KB with strong consistency.

## 2. Writes per event, per region

The report's middle table counts the units that each event costs in each region:

| Line | Units | Why |
|------|-------|-----|
| event put, local or replicated | 1.00 write | Every region stores every event. The region that accepts an append writes it, and DynamoDB copies it to the other regions as a replicated write, which costs the same. An event of 1.5 KB costs 2. |
| TTL delete, replicated | 0.50 write | When an event expires, DynamoDB deletes it for free in one region, and the delete replicates to the other regions as a write. With R regions, that is (R − 1)/R writes per region. A stream kept `FOREVER` has no such line. |
| `page_daily` | 1.07 write, 1.07 read | The projector reads each row it updates, then writes it. A batch of 50 events updates about 49 different rows of the 1,000 in use, so almost every event costs a row update. `explain` adds 10% for writes that conflict and retry. |
| `uniqExact` markers | 0.30 write, 1.00 read | `uniqExact(user_id)` keeps a marker item for each distinct user of a row. Each event reads its marker, and writes one only when the user is new to the row: 30% of the time, unless you set `-uniq-new`. |
| progress item | 0.02 write | The projector records how far it got once per batch: 1 write per 50 events. |

In total, each event costs 2.89 write units and 2.07 read units in each region.

**Views in each region cost no more than your own fan-out.** Without Dynaflux, a common
DynamoDB design writes an item for each lookup it must serve, N items per event, into a
global table. DynamoDB copies each item to the other regions at the same price, so in R
regions an event costs N × R writes. Dynaflux copies only the event. Each region builds the
views from it in its own tables, so a view costs about one row write per event in each
region: N × R in total again, and less when a batch updates a row once for several events.
View tables aren't copied between regions, so two regions never overwrite each other's rows.
What Dynaflux adds is the event itself: its put in every region, and its replicated TTL
delete.

## 3. Why the log keeps 2 days

The log is the stream's DynamoDB table. The longer it keeps events, the larger it gets, and
its size drives the cost of the projection, not only of storage:

1. DynamoDB splits a table into partitions of about 10 GB. At 1,000 events a second of
   1 KB, each day of log adds about 95 GB, so about 10 partitions.
2. Each partition is one *shard* of the table's change stream, and the projector reads
   each shard on its own. Events spread over every shard, so the more shards there are, the
   fewer events each one delivers.
3. Smaller batches combine fewer writes. A batch of 50 events that holds three visits to
   one page updates that page's row once. With batches of 1 event, every event writes a
   row in every view.
4. Each view row keeps a *high-water mark*, the last event it applied, for each shard that
   wrote it recently: a mark stays for 960 seconds after its shard has moved past it
   (`runtime.prune_window_seconds`). That is how the projector applies each event exactly
   once. Any shard can write a row of `page_daily`, so the more shards there are, the more
   marks each row holds, and the more write units each write of it costs.
5. After an outage, the projector catches up with one invocation per shard, and Lambda
   allows 1,000 concurrent executions per account and region by default.

To see it, keep the events for a year. Replace `dynaflux/streams/page_views.sql` with:

<!-- doctest: file dynaflux/streams/page_views.sql -->
```sql
CREATE STREAM page_views (
    user_id     String,
    page        String,
    duration_ms UInt32
)
PRIMARY KEY (user_id)
LOG RETENTION 1 YEAR;
```

```console
$ go tool dynaflux explain page_views
stream page_views
  assumptions  -rate 1000  -event-kb 1  -rows page_daily=1000  -uniq-new page_daily=0.3
               (traffic isn't in the SQL or dynaflux.yaml; set it with these flags)
  prices       us-east-1 list prices, ±30%: for comparing designs, not a quote

layout, in each of 2 regions (us-east-1, us-west-2)
  table        shop-stream-page_views: Global Table, on-demand
  key          pk user_id; sk event_time, event_id
  log          LOG RETENTION 1 YEAR: ~34,690 GB in ~3,469 partitions, each a change-stream shard
  projector    batching window 1 s, batches of at most 100 events: ~869 invocations/s, 1.15 events each on average
  marks        a view row keeps a shard's high-water mark for 960 s after the shard's progress passes it (runtime.prune_window_seconds)
  archive      Firehose to Iceberg in us-east-1, us-west-2

per event, per region                                     write units  read units
  event put, local or replicated                                 1.00
  TTL delete, replicated from the region that expires it         0.50
  page_daily, shared rows, 1,000 active                         24.20        6.60
    uniqExact markers                                            0.30        1.00
  progress item, one per batch                                   0.87
  total                                                         26.87        7.60
  per second, all events                                       26,867       7,600

monthly                        per region  2 regions
  DynamoDB requests               $46,625    $93,251
    stream and meta tables         $3,891     $7,781
    page_daily                    $42,735    $85,469
  DynamoDB storage of the log      $8,672    $17,345
  Lambda, the projector            $1,440     $2,880
  Firehose                           $142       $284
  S3 archive, year-1 average          $73       $145
  replication transfer                $26        $53
  total                           $56,978   $113,957
  provisioned capacity at a 70% target would cut requests to $19,242 per region (-59%), but costs more than on-demand if it must cover peaks over 2.42 times the average
  not modeled: query reads, view and marker storage, backfills, archive compaction, CloudWatch

warning page_views: batches average 1.15 events over 3,469 shards, so view writes rarely combine: most events cost a row write in every view
  hint: shorten LOG RETENTION, which means fewer shards, or raise runtime.batching_window_seconds
warning page_views: ~3,469 shards per region: after an outage, the projector catches up with one invocation per shard, over Lambda's default quota of 1,000 concurrent executions, which all of the account's functions in a region share
  hint: shorten LOG RETENTION, or raise the account's Lambda concurrency quota
warning page_daily: rows reach 21.1 KB with ~894 high-water marks, one for each shard that wrote the row within the prune window (the stream has 3,469 shards); every write costs 22 write units
  hint: shorten LOG RETENTION on page_views (fewer shards), or runtime.prune_window_seconds
```

Compared with the 2-day log of section 1:

| | 2 days | 1 year |
|-|--------|--------|
| Log | ~190 GB | ~34,690 GB |
| Shards per region | ~20 | ~3,469 |
| Events per batch | 50 | 1.15 |
| `page_daily` write units per event | 1.07 | 24.20 |
| Monthly total, 2 regions | $11,647 | $113,957 |

The year costs about ten times as much. Storage is less than a fifth of the difference:
most of it is `page_daily`, whose rows carry ~894 high-water marks, so that every write
costs 22 write units. The three warnings say why, and what to change. At 100 events a
second, a year of log is a tenth as large, but still hundreds of shards: the same effect,
on a smaller bill.

A short log gives up little:

- The archive keeps every event, and Athena reads it. Only stream queries are limited to
  the events still in the log.
- A new view version fills from the archive, so it covers every event, not only those in
  the log ([lifecycle.md §5.2](../spec/lifecycle.md#52-sources)).

Keep the log longer than any regional outage you plan for: a region cut off from the others
for longer may delete events before they replicate. The log can't be shorter than a day
([sql.md §8.6](../spec/sql.md#86-log-retention)). A stream kept `FOREVER` gets warning
DF2501, or DF6403 when it inherits `defaults.log_retention: forever`: its log, and the cost
of projecting it, grow without end. `explain` evaluates such a stream at the age you give
with `-horizon`.

## 4. Levers

Each setting below trades something for cost. Change it, run `explain` again, and compare.

### Log retention

Section 3. Set it for a stream with `LOG RETENTION`, or for every stream without the clause
with `defaults.log_retention` in `dynaflux.yaml` ([config.md](../spec/config.md#defaults)).

### SPLIT for rows that every shard writes

A week of log is a middle ground. Replace the stream file again:

<!-- doctest: file dynaflux/streams/page_views.sql -->
```sql
CREATE STREAM page_views (
    user_id     String,
    page        String,
    duration_ms UInt32
)
PRIMARY KEY (user_id)
LOG RETENTION 7 DAY;
```

Then ask about the view alone:

```console
$ go tool dynaflux explain page_daily
view page_daily, from stream page_views
  assumptions  -rate 1000  -event-kb 1  -rows page_daily=1000  -uniq-new page_daily=0.3
               (traffic isn't in the SQL or dynaflux.yaml; set it with these flags)
  prices       us-east-1 list prices, ±30%: for comparing designs, not a quote

layout, in each of 2 regions (us-east-1, us-west-2)
  table        shop-view-page_daily-v1: regional, on-demand
  mode         ASYNC: the projector applies events in batches
  rows         shared: GROUP BY (page, day) leaves out user_id, the PRIMARY KEY of page_views, so any shard can write any row; ~1,000 active
  backfill     fenced handoff: count and avg are additive

row, estimated (String values are taken as 32 bytes)
  pk        page                                                     34 B
  sk        day                                                       6 B
  page      key column                                               36 B
  day       key column                                                9 B
  views     count()                                                  13 B
  visitors  uniqExact(user_id)                                       16 B
  avg_ms    avg(duration_ms)                                         26 B
  _v        row version                                              10 B
  _hwm      ~71.5 high-water marks of ~24 B                       1.68 KB
  total     2 write units, 1 read unit; the item limit is 400 KB  1.83 KB

per event, per region
  rows         2.18 write units and 1.09 read units: a batch of 14.9 events updates 14.8 rows, and 10% of writes conflict and retry
  markers      0.30 write units and 1.00 read units: a marker read per uniqExact per source row, and a write per new value (30%)
  per row      ~2.18 write and 1.09 read units per second on average; a partition takes 1,000 and 3,000
  monthly      DynamoDB requests $4,769 per region, $9,537 for 2 regions

warning page_daily: rows reach 1.83 KB with ~71.5 high-water marks, one for each shard that wrote the row within the prune window (the stream has 67 shards); every write costs 2 write units
  hint: SPLIT 2 keeps each item at 1 write unit, saving ~$1,794 a month per region; queries then read 2 items per row
```

The view's report lists the row's attributes. Its high-water marks, `_hwm`, are 1.68 KB of
the 1.83 KB row, so each write costs 2 write units instead of 1. The warning names the fix:
`SPLIT 2` stores each row as 2 items, and each shard writes only one of them, so each item
holds about half the marks. Add it to `dynaflux/views/page_daily.sql`:

<!-- doctest: file dynaflux/views/page_daily.sql -->
```sql
CREATE MATERIALIZED VIEW page_daily
ORDER BY (page, day)
PRIMARY KEY (page)
SPLIT 2
AS SELECT
    page,
    toStartOfDay(event_time) AS day,
    count()                  AS views,
    uniqExact(user_id)       AS visitors,
    avg(duration_ms)         AS avg_ms
FROM page_views
GROUP BY page, day;
```

```console
$ go tool dynaflux explain page_daily
view page_daily, from stream page_views
  assumptions  -rate 1000  -event-kb 1  -rows page_daily=1000  -uniq-new page_daily=0.3
               (traffic isn't in the SQL or dynaflux.yaml; set it with these flags)
  prices       us-east-1 list prices, ±30%: for comparing designs, not a quote

layout, in each of 2 regions (us-east-1, us-west-2)
  table        shop-view-page_daily-v1: regional, on-demand
  mode         ASYNC: the projector applies events in batches
  rows         shared: GROUP BY (page, day) leaves out user_id, the PRIMARY KEY of page_views, so any shard can write any row; ~1,000 active
  split        SPLIT 2: each row is 2 items, chosen by shard; a query reads all 2
  backfill     fenced handoff: count and avg are additive

row, estimated (String values are taken as 32 bytes)
  pk        page                                                    34 B
  sk        day and the item number                                  7 B
  page      key column                                              36 B
  day       key column                                               9 B
  views     count()                                                 13 B
  visitors  uniqExact(user_id)                                      16 B
  avg_ms    avg(duration_ms)                                        26 B
  _v        row version                                             10 B
  _hwm      ~35.7 high-water marks of ~24 B                        865 B
  total     1 write unit, 1 read unit; the item limit is 400 KB  1,016 B

per event, per region
  rows         1.09 write units and 1.09 read units: a batch of 14.9 events updates 14.8 rows, and 10% of writes conflict and retry
  markers      0.30 write units and 1.00 read units: a marker read per uniqExact per source row, and a write per new value (30%)
  per row      ~0.546 write and 0.546 read units per second on average; a partition takes 1,000 and 3,000
  monthly      DynamoDB requests $2,974 per region, $5,949 for 2 regions
```

Each item costs 1 write unit, and the view's requests fall from $4,769 to $2,974 a month
per region. The price:

- A query reads every item of a row, so reads cost twice as much.
- `SPLIT` changes how the view stores its rows, so it makes a new view version, which a
  backfill fills ([sql.md §9.9](../spec/sql.md#99-view-versions)).
- A `MODE SYNC` view can't use `SPLIT` (DF3304).

A view whose `GROUP BY` holds the stream's `PRIMARY KEY`, such as a row per user, doesn't
need it: one shard writes each of its rows, so a row keeps one mark.

### The batching window

`runtime.batching_window_seconds`, 1 second by default, is how long the projector gathers
events from a shard before it applies them ([config.md](../spec/config.md#runtime)). A
longer window makes bigger batches when shards are quiet: fewer invocations, fewer progress
writes, and more row updates that cover several events. It costs view lag: an `ASYNC` view
shows an event about one window after the append. A batch holds at most 100 events. Once
shards fill batches before the window ends, a longer window saves nothing, and `explain`
warns about it.

### uniqExact or uniq

`uniqExact` pays for its markers: a read for every source row, and a write for every new
value. Set `-uniq-new` to how often a value is new. Counting distinct sessions, for example,
writes a marker for almost every event.

`uniq` keeps a HyperLogLog sketch in the row instead. It writes no markers, its estimate is
within about 1.6%, and queries can combine it across rows with `uniqMerge`, which
`uniqExact` can't do (DF4201). But the sketch is part of the row: 2^12 registers, up to
2.5 KB, smaller while it holds few values. Its size is fixed, and `explain` prices it at its
largest, which adds about 3 write units to each write of the row.

So `uniqExact` pays per new value, and `uniq` pays on every write of the row. Run `explain`
on both versions of the view to see which is cheaper for your traffic
([sql.md §7.2](../spec/sql.md#72-function-reference)).

### Regions

Every region stores every event and runs every view, so each region costs about as much as
the first. A region's share of the TTL deletes and of the replication transfer grows too,
from 1/2 with 2 regions to (R − 1)/R with R. Choose regions for availability and for where
your users are; `explain`'s monthly table shows the cost per region and for all of them.

### Where the archive lives

By default, each region archives every event, so the archive costs one copy per region in
Firehose and S3. `archive.regions` can name fewer regions, and `archive.events: local` keeps
one copy in total, spread over the regions. Each gives up something during an outage or for
backfills ([config.md](../spec/config.md#archive)). These lines are small, $142 and $73 a
month per region in section 1, unless Firehose rounds each record up to 5 KB (section 7).

### Provisioned capacity

The last lines of the report in section 1 say: "provisioned capacity at a 70% target would
cut requests to $2,242 per region (-59%), but costs more than on-demand if it must cover
peaks over 2.42 times the average".

On-demand tables bill each request. Provisioned tables bill capacity by the hour, at a lower
price per unit, and autoscaling keeps the capacity at about `target_percent` use. Capacity
must cover the peak, so provisioned pays off for steady traffic. Autoscaling reacts in
minutes, so a sudden burst can be throttled, and a new table starts at `min`. Set it for
every table in `dynaflux.yaml`, or for one table under `tables`
([config.md](../spec/config.md#autoscaling)). The values are capacity units per table and
region:

<!-- doctest: file dynaflux.yaml -->
```yaml
store: shop
regions: [us-east-1, us-west-2]
sql: ["dynaflux/**/*.sql"]
defaults:
  capacity: provisioned
  autoscaling:
    read:  {min: 100, max: 10000}
    write: {min: 100, max: 10000}
```

```console
$ go tool dynaflux validate
```

## 5. Reading explain

`go tool dynaflux explain <name>` reports on a stream, a view or a named query. Traffic
isn't in the SQL or in `dynaflux.yaml`, so you give it with flags, and the report starts
with the values it used. Copy that line and change one value to try another workload.

| Flag | Default | What it is |
|------|---------|------------|
| `-rate` | 1000 | Average events per second appended to the stream, all regions together. It must be over 0: for a store with almost no traffic, use a tiny rate such as 0.01. |
| `-event-kb` | 1 | Average size of an event as DynamoDB stores it, in KB, column names included. |
| `-rows view=N` | 1000 | For a view whose rows any shard writes: how many rows it writes to at a time, such as the pages visited today for `GROUP BY page, day`. Fewer rows means hotter rows. |
| `-uniq-new view=F` | 0.3 | The fraction of a view's source rows that bring its `uniqExact` a new value. |
| `-pass view=F` | 1 | A view's source rows per event: the fraction of events that its `WHERE` keeps, times its `ARRAY JOIN` fan-out. |
| `-horizon age` | 365d | For a stream kept `FOREVER`: the age of the log to evaluate, such as `30d` or `36h`. |

`-rows`, `-uniq-new` and `-pass` take one view each and can be repeated. `explain` refuses a
flag that can't apply, such as `-pass` for a view without `WHERE` or `ARRAY JOIN`.

A stream's report has these parts:

| Part | What it says |
|------|--------------|
| `assumptions`, `prices` | The workload, and the price basis. With a region outside the US, it says that prices there are higher. |
| `layout` | The table and its key; the log's size and partitions; the projector's batching window, invocations and events per batch; how long marks stay; where the archive is. |
| `per event, per region` | The units of section 2. `per second, all events` is the load on each region, which provisioned capacity must cover. |
| `monthly` | Dollars by line, per region and for every region; the provisioned-capacity estimate when it is cheaper; what isn't modeled (section 6). |
| `warning`, `error` | What makes the design costly or broken, each with a hint. An error, such as rows that would outgrow DynamoDB's 400 KB item limit, makes `explain` exit with status 1. |

A view's report adds:

| Part | What it says |
|------|--------------|
| `mode` | `ASYNC`, or `SYNC`, whose rows change in the append's transaction at twice the write units. |
| `rows` | *Shared*, when `GROUP BY` leaves out a column of the stream's `PRIMARY KEY`, so any shard can write any row and `-rows` applies; or *a row per entity*, written by one shard. |
| `split`, `backfill` | The `SPLIT` count; whether a backfill of a new version needs the fenced handoff ([sql.md §7.5](../spec/sql.md#75-state-classes)). |
| `row, estimated` | Each attribute and its size, and the write and read units of a row. String values are taken as 32 bytes. |
| `per event, per region` | Units for rows and markers; for shared rows, the load on one row, against the 1,000 write units and 3,000 read units per second that a DynamoDB partition takes. |

## 6. What the model leaves out

The monthly table ends with what it doesn't count. The first item is query reads. A query's
report gives the read units of one call, so multiply them by your calls:

```console
$ go tool dynaflux explain PageStats
query PageStats :many, from view page_daily
  assumptions  -rate 1000  -event-kb 1  -rows page_daily=1000  -uniq-new page_daily=0.3
               (traffic isn't in the SQL or dynaflux.yaml; set it with these flags)
  prices       us-east-1 list prices, ±30%: for comparing designs, not a quote

  read         Query of page = {page} AND day >= {from} AND day <= {to}
  calls        1 Query per call, one per key combination
  items        every row in the range, 2 items per row (SPLIT 2)
  read cost    0.992 KB per item, and a Query rounds its total up to 4 KB: ~0.124 read units per item eventually consistent, ~0.248 with WaitFor, which reads with strong consistency
```

The rest:

- **View and marker storage.** View rows and `uniqExact` markers stay until the view's `TTL`
  removes them, if it has one.
- **Backfills.** A new view version reads the archive with Athena, then writes every row of
  its table.
- **Archive compaction.** AWS Glue compacts the archive's files.
- **CloudWatch.** For this store, 12 alarms over 13 alarm metrics, which CloudWatch bills
  by, and a dashboard in each region
  ([infra.md §10](../spec/infra.md#10-alarms-and-dashboard)), and 30 days of logs.
- **Point-in-time recovery** of the stream tables, billed by their size: small with a short
  log ([infra.md §4](../spec/infra.md#4-globalyaml)).

The prices are us-east-1 list prices, and most regions outside the US cost more. The model
is ±30%: use it to compare designs. For a budget, use the
[AWS Pricing Calculator](https://calculator.aws/).

`explain`'s numbers and wording can change in any release, for example when measurements
correct the model: they are outside the v1 compatibility promise
([ADR 0019](../decisions/0019-v1-compatibility-promise.md)).

## 7. Measured against the model

Nothing has been measured on AWS yet. After the first throughput run on AWS, this table
gives the measured value next to each assumption of the model.

| Quantity | The model | Measured |
|----------|-----------|----------|
| Shards per region | one per 10 GB of log, and at least one per 1,000 write units a second | not yet |
| Events per invocation | the events a shard delivers in one batching window, at most 100 | not yet |
| Write units per event, stream table | the event put and the replicated TTL delete | not yet |
| Write and read units per event, each view | the row updates, plus 10% for conflicts, and the markers | not yet |
| Projector duration | 30 ms per invocation, plus 2 ms per event | not yet |
| Firehose | $0.045 per GB of records, with no rounding per record | not yet |
| Replication transfer | $0.02 per GB copied to each other region | not yet |

**The Firehose line may be low.** The projector sends each event to Firehose as one record.
Firehose's [pricing page](https://aws.amazon.com/firehose/pricing/) says that delivery to
Apache Iceberg tables is billed per GB, with no 5 KB increments, and `explain` assumes so.
Its [quota page](https://docs.aws.amazon.com/firehose/latest/dev/limits.html) says that
ingestion is billed per record, rounded up to 5 KB. If that rounding applies, the Firehose
line, $142 a month per region in section 1, is about 4 times too low for `explain`'s
default 1 KB events, and more for smaller events. The first bill of the throughput run
settles it.

<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# SQL recipes

This guide shows how to write streams, views and queries for common tasks, and why each
recipe works. [sql.md](../spec/sql.md) is the full specification of the dialect, with every
rule and every diagnostic code; the recipes link to the sections they rely on.

## Contents

- [Before you start](#before-you-start)
- Streams
  - [1. Choose a stream's PRIMARY KEY](#1-choose-a-streams-primary-key)
  - [2. Idempotent appends with DEDUP BY](#2-idempotent-appends-with-dedup-by)
  - [3. LOG RETENTION and the archive](#3-log-retention-and-the-archive)
  - [4. The ACCEPT window](#4-the-accept-window)
- Views
  - [5. Start from the query](#5-start-from-the-query-then-derive-the-views-keys)
  - [6. Time buckets](#6-time-buckets)
  - [7. Choose aggregates](#7-choose-aggregates)
  - [8. Filters and ARRAY JOIN](#8-filters-and-array-join)
  - [9. Expire rows with TTL](#9-expire-rows-with-ttl)
  - [10. Hot rows and SPLIT](#10-hot-rows-and-split)
  - [11. Read your writes with SYNC](#11-read-your-writes-with-sync)
  - [12. Count entities by their latest state](#12-count-entities-by-their-latest-state)
- Queries
  - [13. Query shapes](#13-query-shapes)
  - [14. Serve a query over HTTP](#14-serve-a-query-over-http)
- Changes and errors
  - [15. What makes a new version](#15-what-makes-a-new-version)
  - [16. Common diagnostics and their fixes](#16-common-diagnostics-and-their-fixes)
  - [17. How it differs from ClickHouse](#17-how-it-differs-from-clickhouse)

## Before you start

The recipes build the SQL of one store, an online shop, one file at a time. Each SQL block
is a complete file: create it at the path given above it, or replace the file of an earlier
recipe that has the same path. Then run `validate`, which prints nothing when the project
has no problems. A block that shows a mistake is followed by the error that `validate`
prints, with exit status 1, and then by the fix.

Run the recipes in a new, empty directory, not in a project you deploy:

```sh
go mod init example.com/shop
go get -tool github.com/lucindo/dynaflux/cmd/dynaflux@latest
```

Then create `dynaflux.yaml` with the store's name and its regions:

<!-- doctest: file dynaflux.yaml -->
```yaml
store: shop
regions: [us-east-1, us-west-2]
```

## 1. Choose a stream's PRIMARY KEY

A stream holds facts that never change. In the shop, each event of `orders` is an order
that a customer placed. Create `dynaflux/streams/orders.sql`:

<!-- doctest: file dynaflux/streams/orders.sql -->
```sql
CREATE STREAM orders (
    customer_id String,
    order_id    String,
    country     LowCardinality(String),
    skus        Array(String),
    amount      Decimal(12, 2),
    coupon      Nullable(String)
)
PRIMARY KEY (customer_id);
```

```console
$ go tool dynaflux validate
```

Why it works:

- `PRIMARY KEY` is the partition key of the stream's DynamoDB table. One customer's orders
  are stored together, sorted by `event_time`, so a query over the stream reads them with
  one DynamoDB `Query`. Such a query must bind every key column with `=` or `IN`
  ([recipe 13](#queries-over-a-stream)).
- Choose the key that a read of raw events is about: a customer, a device, an account.
  Views don't need to match it: each view groups events by its own keys.
- The key also spreads writes. A key that many events share, such as `country`, sends them
  all to one DynamoDB partition, which takes about 1,000 write units a second. `SHARDS n`
  spreads each key value over n partitions, and a query over the stream then reads all n
  ([sql.md §8.4](../spec/sql.md#84-primary-key-and-shards)).
- Key columns can't be `Nullable` (DF2104), and the key can't change after the first
  deploy ([sql.md §8.9](../spec/sql.md#89-changing-a-stream)).
- Every event also has `event_id`, `event_time`, `ingest_time` and `origin_region`
  ([sql.md §8.3](../spec/sql.md#83-implicit-columns)). `event_time` is when the order was
  placed: the application sets it, or the append uses the writer's clock.

## 2. Idempotent appends with DEDUP BY

A checkout whose append times out can't tell whether the order was stored. Without
`DEDUP BY`, a retry stores it twice, and every view counts it twice. Add `DEDUP BY` before
the first deploy, because it can't change later. Replace `dynaflux/streams/orders.sql`:

<!-- doctest: file dynaflux/streams/orders.sql -->
```sql
CREATE STREAM orders (
    customer_id String,
    order_id    String,
    country     LowCardinality(String),
    skus        Array(String),
    amount      Decimal(12, 2),
    coupon      Nullable(String)
)
PRIMARY KEY (customer_id)
DEDUP BY (order_id);
```

```console
$ go tool dynaflux validate
```

Why it works ([sql.md §8.5](../spec/sql.md#85-dedup-by)):

- Two appends are duplicates when their `PRIMARY KEY` values, `event_time` and `DEDUP BY`
  values are equal. The second one changes nothing, and returns an error that matches
  `dynaflux.ErrDuplicate`, with the stored event's token.
- So the application must set `event_time`, and send the same value on every retry: keep
  the time of the order with the order. An append without `event_time` fails.
- A retry that the AWS SDK makes inside one append call isn't a duplicate: the append finds
  the event it stored and succeeds.
- A dedup key must name one fact with one payload. If two regions each accept order `A1`,
  with the same `event_time` but different amounts, before either copy replicates, the
  regions may keep different amounts.
- On a `DEDUP BY` stream, a view that reads `ingest_time` or `origin_region` gets warning
  DF3504: a duplicate accepted in two regions has a different value in each.

## 3. LOG RETENTION and the archive

The stream's DynamoDB table is a log that keeps each event for a limited time. The archive
keeps every event. Keep a week of orders in the log, so that a query over the stream can
list a customer's orders of the last 7 days. Replace `dynaflux/streams/orders.sql`:

<!-- doctest: file dynaflux/streams/orders.sql -->
```sql
CREATE STREAM orders (
    customer_id String,
    order_id    String,
    country     LowCardinality(String),
    skus        Array(String),
    amount      Decimal(12, 2),
    coupon      Nullable(String)
)
PRIMARY KEY (customer_id)
DEDUP BY (order_id)
LOG RETENTION 7 DAY;
```

```console
$ go tool dynaflux validate
```

Why it works ([sql.md §8.6](../spec/sql.md#86-log-retention)):

- The log keeps each event for 7 days after its `ingest_time`. Without the clause, a
  stream keeps `defaults.log_retention` from `dynaflux.yaml`: 2 days.
- Views keep the totals of expired events, because the projector applied each event when it
  arrived. Queries over the stream read only the log.
- The archive is a copy of every event in Apache Iceberg tables on S3, which Athena can
  query ([config.md](../spec/config.md#archive)). A backfill fills a new view version from
  the region's archive. Where the region has no complete archive, it reads the log, and the
  version counts only the events still in it
  ([lifecycle.md §5.2](../spec/lifecycle.md#52-sources)).
- If you turn the archive off for a stream with a limited log, its older events are gone
  for good (DF6504).
- A longer log costs more storage and more change-stream shards: compare
  `go tool dynaflux explain -rate 100 orders` with 2 and 7 days. `LOG RETENTION FOREVER`
  gets warning DF2501.
- The log keeps at least 1 day (DF2109). DynamoDB deletes expired events late, and on its
  own in each region, so a region cut off from the others for longer than the log may
  delete events before they replicate. Keep the log longer than any outage you plan for.
- A changed retention applies to new events only.

## 4. The ACCEPT window

`ACCEPT` limits the `event_time` that an append may carry, measured against the writer's
clock. The shop's app queues orders while it is offline, and sends them later. A first
attempt sets only a past bound. Replace `dynaflux/streams/orders.sql`:

<!-- doctest: file dynaflux/streams/orders.sql -->
```sql
CREATE STREAM orders (
    customer_id String,
    order_id    String,
    country     LowCardinality(String),
    skus        Array(String),
    amount      Decimal(12, 2),
    coupon      Nullable(String)
)
PRIMARY KEY (customer_id)
DEDUP BY (order_id)
LOG RETENTION 7 DAY
ACCEPT event_time >= now() - INTERVAL 30 DAY;
```

<!-- doctest: exit 1 -->
```console
$ go tool dynaflux validate
dynaflux/streams/orders.sql:12:1: error DF2113: orders: ACCEPT needs an upper bound on event_time
  hint: ACCEPT event_time <= now() + INTERVAL 5 MINUTE is the default
```

The upper bound is required. Keep the default's 5 minutes and add the past bound:

<!-- doctest: file dynaflux/streams/orders.sql -->
```sql
CREATE STREAM orders (
    customer_id String,
    order_id    String,
    country     LowCardinality(String),
    skus        Array(String),
    amount      Decimal(12, 2),
    coupon      Nullable(String)
)
PRIMARY KEY (customer_id)
DEDUP BY (order_id)
LOG RETENTION 7 DAY
ACCEPT event_time BETWEEN now() - INTERVAL 30 DAY AND now() + INTERVAL 5 MINUTE;
```

```console
$ go tool dynaflux validate
```

Why it works ([sql.md §8.7](../spec/sql.md#87-accept-and-event-time)):

- Without `ACCEPT`, the window is `event_time <= now() + INTERVAL 5 MINUTE`: no past
  bound, and at most 5 minutes in the future.
- The upper bound stops a clock that runs far ahead: its event would win every
  `argMax(x, event_time)` and fill time buckets that haven't started yet.
- A past bound refuses events too old to matter, such as an order that waited on a device
  for months. The log counts from `ingest_time`, so a late event still stays in it for the
  full 7 days.
- An append outside the window fails with an error that matches
  `dynaflux.ErrOutsideWindow`. `dynaflux import` and appends with `dynaflux.AsImport()` skip
  the window, to load past events.

## 5. Start from the query, then derive the view's keys

A view exists to serve queries, so write the query first. The shop's dashboard shows a
country's orders and sales per day, for a range of days. Create
`dynaflux/queries/country.sql`:

<!-- doctest: file dynaflux/queries/country.sql -->
```sql
-- name: CountrySales :many
SELECT day, orders, sales
FROM country_daily
WHERE country = {country:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;
```

The query names what the view needs:

- The columns it fixes with `=` or `IN`, here `country`, are the view's `PRIMARY KEY`: the
  DynamoDB partition key.
- The column it takes a range over, `day`, comes next in `ORDER BY`: the sort key.
- The values it reads, `orders` and `sales`, are the view's aggregates.

A first attempt orders the view by time, as ClickHouse tables often are. Create
`dynaflux/views/country_daily.sql`:

<!-- doctest: file dynaflux/views/country_daily.sql -->
```sql
CREATE MATERIALIZED VIEW country_daily
ORDER BY (day, country)
AS SELECT
    country,
    toDate(event_time) AS day,
    count()            AS orders,
    sum(amount)        AS sales
FROM orders
GROUP BY country, day;
```

<!-- doctest: exit 1 -->
```console
$ go tool dynaflux validate
dynaflux/queries/country.sql:4:1: error DF4101: CountrySales: view country_daily is partitioned by (day, country), but WHERE does not bind "day" with = or IN. Every query must fix the partition key.
  hint: add day = {day:Date} to WHERE, or declare a view whose PRIMARY KEY matches this query
```

Without `PRIMARY KEY`, the partition key is the whole `ORDER BY`, and every query must fix
both columns. Put the column that the query fixes first, and make it the `PRIMARY KEY`:

<!-- doctest: file dynaflux/views/country_daily.sql -->
```sql
CREATE MATERIALIZED VIEW country_daily
ORDER BY (country, day)
PRIMARY KEY (country)
AS SELECT
    country,
    toDate(event_time) AS day,
    count()            AS orders,
    sum(amount)        AS sales
FROM orders
GROUP BY country, day;
```

```console
$ go tool dynaflux validate
```

Why it works ([sql.md §9.4](../spec/sql.md#94-keys-and-the-dynamodb-layout)):

- `CountrySales` reads one partition, `country`, and a range of its sort key, `day`: one
  DynamoDB `Query` per call. `validate` rejects any query that would scan a table.
- `ORDER BY` lists exactly the `GROUP BY` keys (DF3101). The columns that queries bind
  with `=` or `IN` go first, then the one they take a range over. Conditions on columns
  after the range only filter the rows read
  ([sql.md §10.4](../spec/sql.md#104-key-binding-over-a-view)).
- Without `PRIMARY KEY`, a view serves only lookups of whole keys. That is the safe
  default.
- One view serves every query with the same partition key. A query with another partition
  key needs another view, which costs writes on every event:
  `go tool dynaflux explain country_daily` shows how many.

## 6. Time buckets

Group by a function of `event_time`, with an alias, to count per minute, hour, day, week or
month. Create `dynaflux/views/country_hourly.sql` with orders per country and hour:

<!-- doctest: file dynaflux/views/country_hourly.sql -->
```sql
CREATE MATERIALIZED VIEW country_hourly
ORDER BY (country, hour)
PRIMARY KEY (country)
AS SELECT
    country,
    toStartOfHour(event_time) AS hour,
    count()                   AS orders
FROM orders
GROUP BY country, hour;
```

A first attempt at the last 24 hours asks the database for the time, in
`dynaflux/queries/hourly.sql`:

<!-- doctest: file dynaflux/queries/hourly.sql -->
```sql
-- name: CountryLastDay :many
SELECT hour, orders
FROM country_hourly
WHERE country = {country:String}
  AND hour >= now() - INTERVAL 24 HOUR
ORDER BY hour;
```

<!-- doctest: exit 1 -->
```console
$ go tool dynaflux validate
dynaflux/queries/hourly.sql:5:15: error DF5405: CountryLastDay: now() is only allowed in ACCEPT
  hint: pass the time as a parameter
dynaflux/queries/hourly.sql:5:23: error DF5403: CountryLastDay: INTERVAL is only allowed in TTL and ACCEPT
```

Pass the time instead, as a parameter that the caller sets to 24 hours ago:

<!-- doctest: file dynaflux/queries/hourly.sql -->
```sql
-- name: CountryLastDay :many
SELECT hour, orders
FROM country_hourly
WHERE country = {country:String}
  AND hour >= {since:DateTime64(6)}
ORDER BY hour;
```

```console
$ go tool dynaflux validate
```

The bucket functions, the type of the key they make, and a parameter type to compare with
it ([sql.md §6](../spec/sql.md#6-scalar-functions)):

| Bucket | Expression | Key type | Parameter |
|--------|------------|----------|-----------|
| Minute | `toStartOfMinute(event_time)` | `DateTime` | `DateTime64(6)` |
| Five minutes | `toStartOfFiveMinutes(event_time)` | `DateTime` | `DateTime64(6)` |
| Hour | `toStartOfHour(event_time)` | `DateTime` | `DateTime64(6)` |
| Day | `toDate(event_time)` | `Date` | `Date` |
| Week, from Monday | `toStartOfWeek(event_time, 1)` | `Date` | `Date` |
| Month | `toStartOfMonth(event_time)` | `Date` | `Date` |

Why it works:

- A query has no clock: `now()` is allowed only in `ACCEPT`. So the same parameters return
  the same rows in every region and on every run.
- A `DateTime64(6)` parameter compared with a `DateTime` key bounds the read exactly.
- All times are UTC, and the functions take no time zone (DF5210): a day is a UTC day. For
  users in one time zone with a whole-hour offset, keep hourly rows and add up the hours of
  a local day in the query.
- Bucket `event_time`, when the fact happened, not `ingest_time`, when Dynaflux accepted
  it.
- Quarters and years have no function in v1 (DF5201): use months, and add them up in a
  query.
- Smaller buckets mean more rows, 24 per country and day here.
  [Recipe 9](#9-expire-rows-with-ttl) deletes old ones.

## 7. Choose aggregates

Add distinct customers, and the orders that used a coupon, to `country_daily`. A first
attempt counts customers exactly. Replace `dynaflux/views/country_daily.sql`:

<!-- doctest: file dynaflux/views/country_daily.sql -->
```sql
CREATE MATERIALIZED VIEW country_daily
ORDER BY (country, day)
PRIMARY KEY (country)
AS SELECT
    country,
    toDate(event_time)          AS day,
    count()                     AS orders,
    sum(amount)                 AS sales,
    uniqExact(customer_id)      AS customers,
    countIf(coupon IS NOT NULL) AS coupon_orders
FROM orders
GROUP BY country, day;
```

Then a query for the distinct customers over a range of days, in
`dynaflux/queries/customers.sql`:

<!-- doctest: file dynaflux/queries/customers.sql -->
```sql
-- name: CountryCustomers :one
SELECT uniqMerge(customers) AS distinct_customers
FROM country_daily
WHERE country = {country:String}
  AND day BETWEEN {from:Date} AND {to:Date};
```

<!-- doctest: exit 1 -->
```console
$ go tool dynaflux validate
dynaflux/queries/customers.sql:2:8: error DF4201: CountryCustomers: uniqMerge(customers) cannot be combined across rows (the query combines all rows of country_daily).
  hint: declare a view grouped by (country), or make customers a uniq() column and use uniqMerge(customers) for an approximate count
```

`uniqExact` keeps the exact set of customers of each day, and the sets of different days
can't be combined. Use `uniq`, whose estimates can:

<!-- doctest: file dynaflux/views/country_daily.sql -->
```sql
CREATE MATERIALIZED VIEW country_daily
ORDER BY (country, day)
PRIMARY KEY (country)
AS SELECT
    country,
    toDate(event_time)          AS day,
    count()                     AS orders,
    sum(amount)                 AS sales,
    uniq(customer_id)           AS customers,
    countIf(coupon IS NOT NULL) AS coupon_orders
FROM orders
GROUP BY country, day;
```

```console
$ go tool dynaflux validate
```

Why it works ([sql.md §7.1](../spec/sql.md#71-summary)):

- `uniq` is a HyperLogLog sketch, stored in the row, with about 1.6% standard error.
  `uniqMerge` merges the sketches of the days into one estimate for the range.
- `uniqExact` is exact within one row, such as one day, and costs about one more write for
  each new value. Use it when each query reads one row.
- An `-If` aggregate takes only the events that pass its condition, so one view counts
  every order and those with a coupon
  ([sql.md §7.3](../spec/sql.md#73-the--if-combinator)).
- Use `Decimal` for money: `sum` over it is exact.
- Also use `Decimal`, such as `Decimal(20, 0)`, for an integer column whose values your
  callers choose. Two `Int64` or `UInt64` values near the limit take a `sum` past its type,
  and then every read of that row fails
  ([sql.md §7.2](../spec/sql.md#72-function-reference)).

How a query combines each aggregate across rows
([sql.md §7.4](../spec/sql.md#74-read-time-aggregation-and--merge)):

| You need | In the view | Across rows, in a query |
|----------|-------------|-------------------------|
| Number of events | `count()` | `sum(c)` |
| A total | `sum(x)` | `sum(c)` |
| An average | `avg(x)` | `avgMerge(c)`; `avg(c)` is DF4202 |
| Smallest or largest | `min(x)`, `max(x)` | `min(c)`, `max(c)` |
| Latest value | `argMax(x, event_time)` | `argMaxMerge(c)` |
| Distinct values, exact | `uniqExact(x)` | none: DF4201 |
| Distinct values, estimated | `uniq(x)` | `uniqMerge(c)` |
| A percentile of integers or `Float64` | `quantile(0.9)(x)` | `quantileMerge(0.9)(c)` |

## 8. Filters and ARRAY JOIN

`WHERE` in a view keeps only some events. Count the orders and sales of each coupon code
per day, without the orders that have no coupon. A first attempt groups by the `Nullable`
column, in `dynaflux/views/coupon_daily.sql`:

<!-- doctest: file dynaflux/views/coupon_daily.sql -->
```sql
CREATE MATERIALIZED VIEW coupon_daily
ORDER BY (coupon, day)
PRIMARY KEY (coupon)
AS SELECT
    coupon,
    toDate(event_time) AS day,
    count()            AS orders,
    sum(amount)        AS sales
FROM orders
WHERE coupon IS NOT NULL
GROUP BY coupon, day;
```

<!-- doctest: exit 1 -->
```console
$ go tool dynaflux validate
dynaflux/views/coupon_daily.sql:5:5: error DF3103: coupon_daily: key coupon is Nullable
  hint: group by coalesce(coupon, '') AS coupon_key
```

A view key can't be `NULL`, even after `WHERE` has dropped the `NULL` values. Follow the
hint:

<!-- doctest: file dynaflux/views/coupon_daily.sql -->
```sql
CREATE MATERIALIZED VIEW coupon_daily
ORDER BY (coupon_key, day)
PRIMARY KEY (coupon_key)
AS SELECT
    coalesce(coupon, '') AS coupon_key,
    toDate(event_time)   AS day,
    count()              AS orders,
    sum(amount)          AS sales
FROM orders
WHERE coupon IS NOT NULL
GROUP BY coupon_key, day;
```

```console
$ go tool dynaflux validate
```

`ARRAY JOIN` turns each event into one source row per element of an array. Count the units
sold of each SKU per day, in `dynaflux/views/sku_daily.sql`:

<!-- doctest: file dynaflux/views/sku_daily.sql -->
```sql
CREATE MATERIALIZED VIEW sku_daily
ORDER BY (sku, day)
PRIMARY KEY (sku)
AS SELECT
    sku,
    toDate(event_time) AS day,
    count()            AS units,
    uniq(customer_id)  AS buyers
FROM orders
ARRAY JOIN skus AS sku
GROUP BY sku, day;
```

```console
$ go tool dynaflux validate
```

Why it works ([sql.md §9.5](../spec/sql.md#95-array-join)):

- An event that the view's `WHERE` drops doesn't write to the view.
- `WHERE` in a query is different: on a column that isn't a key, it filters the rows read,
  and doesn't make the read cheaper ([sql.md §10.6](../spec/sql.md#106-residual-filters)).
- An order with three SKUs makes three source rows, and `count()` counts them. An order
  with an empty array makes none.
- `WHERE` runs after `ARRAY JOIN`, so it can test the element: `WHERE sku != ''`.
- A view takes one array (DF3203). `SYNC` views (DF3303) and queries (DF4003) can't use
  `ARRAY JOIN`.
- `explain` assumes one source row per event. For about three SKUs per order, run
  `go tool dynaflux explain -pass sku_daily=3 sku_daily`.

## 9. Expire rows with TTL

Without `TTL`, a view keeps its rows forever: here 24 a day for each country. Keep 30 days
of hourly rows. A first attempt bases the `TTL` on the event's time. Replace
`dynaflux/views/country_hourly.sql`:

<!-- doctest: file dynaflux/views/country_hourly.sql -->
```sql
CREATE MATERIALIZED VIEW country_hourly
ORDER BY (country, hour)
PRIMARY KEY (country)
TTL event_time + INTERVAL 30 DAY
AS SELECT
    country,
    toStartOfHour(event_time) AS hour,
    count()                   AS orders
FROM orders
GROUP BY country, hour;
```

<!-- doctest: exit 1 -->
```console
$ go tool dynaflux validate
dynaflux/views/country_hourly.sql:4:5: error DF3302: country_hourly: TTL column event_time is not a Date, DateTime or DateTime64 key of the view
```

A row's expiry must depend on its key alone. Use the bucket:

<!-- doctest: file dynaflux/views/country_hourly.sql -->
```sql
CREATE MATERIALIZED VIEW country_hourly
ORDER BY (country, hour)
PRIMARY KEY (country)
TTL hour + INTERVAL 30 DAY
AS SELECT
    country,
    toStartOfHour(event_time) AS hour,
    count()                   AS orders
FROM orders
GROUP BY country, hour;
```

```console
$ go tool dynaflux validate
```

Why it works ([sql.md §9.6](../spec/sql.md#96-ttl)):

- Each row expires at a time fixed by its key, so every region computes the same expiry,
  and a row and its `uniqExact` markers expire together.
- Queries never return an expired row, in any region, even before DynamoDB deletes it.
- An event for a row that has already expired is skipped: it doesn't bring the row back.
- Streams use `LOG RETENTION` instead ([recipe 3](#3-log-retention-and-the-archive)).

## 10. Hot rows and SPLIT

A row that every event updates is hot. The shop's live dashboard shows the orders of the
whole shop per minute, in `dynaflux/views/sales_minute.sql`:

<!-- doctest: file dynaflux/views/sales_minute.sql -->
```sql
CREATE MATERIALIZED VIEW sales_minute
ORDER BY (minute)
TTL minute + INTERVAL 2 DAY
AS SELECT
    toStartOfMinute(event_time) AS minute,
    count()                     AS orders,
    sum(amount)                 AS sales
FROM orders
GROUP BY minute;
```

Ask `explain` about 1,000 orders a second. `-rows sales_minute=1` says that the view writes
to one row at a time:

```console
$ go tool dynaflux explain -rate 1000 -rows sales_minute=1 sales_minute
view sales_minute, from stream orders
  assumptions  -rate 1000  -event-kb 1  -rows sales_minute=1
               (traffic isn't in the SQL or dynaflux.yaml; set it with these flags)
  prices       us-east-1 list prices, ±30%: for comparing designs, not a quote

layout, in each of 2 regions (us-east-1, us-west-2)
  table        shop-view-sales_minute-v1: regional, on-demand
  mode         ASYNC: the projector applies events in batches
  rows         shared: GROUP BY (minute) leaves out customer_id, the PRIMARY KEY of orders, so any shard can write any row; ~1 active
  backfill     fenced handoff: count and sum are additive

row, estimated (String values are taken as 32 bytes)
  pk      minute                                                    6 B
  sk      a constant                                                3 B
  minute  key column                                               12 B
  orders  count()                                                  14 B
  sales   sum(amount)                                              17 B
  _v      row version                                              10 B
  _ttl    expiry                                                   10 B
  _hwm    ~71.5 high-water marks of ~24 B                       1.68 KB
  total   2 write units, 1 read unit; the item limit is 400 KB  1.75 KB

per event, per region
  rows         0.15 write units and 0.07 read units: a batch of 14.9 events updates 1 row, and 10% of writes conflict and retry
  per row      ~147 write and 73.7 read units per second on average; a partition takes 1,000 and 3,000
  monthly      DynamoDB requests $266 per region, $533 for 2 regions

warning sales_minute: rows reach 1.75 KB with ~71.5 high-water marks, one for each shard that wrote the row within the prune window (the stream has 67 shards); every write costs 2 write units
  hint: SPLIT 2 keeps each item at 1 write unit, saving ~$121 a month per region; queries then read 2 items per row
```

Follow the hint at the end. Replace `dynaflux/views/sales_minute.sql`:

<!-- doctest: file dynaflux/views/sales_minute.sql -->
```sql
CREATE MATERIALIZED VIEW sales_minute
ORDER BY (minute)
SPLIT 2
TTL minute + INTERVAL 2 DAY
AS SELECT
    toStartOfMinute(event_time) AS minute,
    count()                     AS orders,
    sum(amount)                 AS sales
FROM orders
GROUP BY minute;
```

```console
$ go tool dynaflux validate
```

Why it works ([sql.md §9.8](../spec/sql.md#98-split)):

- The projector applies events in batches and writes the row once per batch, not once per
  event. The `per row` line shows the row's write rate, well under what a DynamoDB
  partition takes.
- What grows is the row. It keeps a high-water mark for each change-stream shard that wrote
  it in the last 16 minutes (`runtime.prune_window_seconds`), and every shard writes this
  row. Once the marks make a write cost more than one write unit, `explain` warns, and
  names the smallest `SPLIT` that fixes it.
- `SPLIT n` stores the row as n items, each written by fewer shards. Queries read all n and
  combine them, so results don't change, and reads cost n times more.
- `SYNC` views can't use `SPLIT` (DF3304).

## 11. Read your writes with SYNC

The shop limits what a customer can spend in a day. Before it accepts an order, it reads
what the customer spent today, which must include the order placed a second ago. `MODE SYNC`
updates the view in the append's own transaction. Create
`dynaflux/views/customer_daily.sql`:

<!-- doctest: file dynaflux/views/customer_daily.sql -->
```sql
CREATE MATERIALIZED VIEW customer_daily
MODE SYNC
ORDER BY (customer_id, day)
PRIMARY KEY (customer_id)
TTL day + INTERVAL 7 DAY
AS SELECT
    customer_id,
    toDate(event_time) AS day,
    count()            AS orders,
    sum(amount)        AS spent
FROM orders
GROUP BY customer_id, day;
```

And the check, in `dynaflux/queries/customer.sql`:

<!-- doctest: file dynaflux/queries/customer.sql -->
```sql
-- name: CustomerDay :one
SELECT orders, spent
FROM customer_daily
WHERE customer_id = {customer_id:String}
  AND day = {day:Date};
```

```console
$ go tool dynaflux validate
```

Why it works ([sql.md §9.7](../spec/sql.md#97-mode)):

- A `SYNC` append writes the event and the row in one DynamoDB transaction, in the
  writer's region. A query there sees the order as soon as the append returns. Other
  regions apply it through their projector, about a second later.
- The view groups by `customer_id`, the stream's `PRIMARY KEY`, so only one customer's
  orders update a row. A `SYNC` view whose rows many customers share gets warning DF3501:
  concurrent appends to one row conflict and retry.
- `SYNC` costs more. A transactional write costs twice as much, and so does a query of a
  `SYNC` view, which reads with strong consistency.
- It gives read-your-writes, not a lock: two checkouts at the same moment can both read the
  total from before either order.
- `CustomerDay` returns an error that matches `dynaflux.ErrNotFound` before the customer's
  first order of the day.
- `dynaflux import` and appends with `dynaflux.AsImport()` don't update `SYNC` views in the
  transaction: the projector applies those events.
- An `ASYNC` view can read your writes too, after a wait: append with
  `dynaflux.Waitable()`, and query with `dynaflux.WaitFor(token, timeout)`, which waits
  about a second for the region's projector.

[examples/analytics](../../examples/analytics) runs this pattern as a monthly quota of API
calls: `dynaflux/quota.sql`, with its tests in `quota_test.go`.
[sql.md §12.2](../spec/sql.md#122-quota-style-counter) walks through it.

## 12. Count entities by their latest state

Keep one row per entity with its latest state, and count the rows of a partition by state
in a query. The shop records each step of an order in another stream. Create
`dynaflux/order_status.sql`:

<!-- doctest: file dynaflux/order_status.sql -->
```sql
CREATE STREAM order_updates (
    customer_id String,
    order_id    String,
    status      LowCardinality(String)
)
PRIMARY KEY (customer_id);

CREATE MATERIALIZED VIEW order_status
ORDER BY (customer_id, order_id)
PRIMARY KEY (customer_id)
AS SELECT
    customer_id,
    order_id,
    argMax(status, event_time) AS last_status,
    max(event_time)            AS updated
FROM order_updates
GROUP BY customer_id, order_id;

-- name: CustomerOrderStates :one
SELECT
    count()                                                AS orders,
    countIf(last_status = 'shipped')                       AS shipped,
    countIf(last_status NOT IN ('delivered', 'cancelled')) AS open
FROM order_status
WHERE customer_id = {customer_id:String};
```

```console
$ go tool dynaflux validate
```

Why it works:

- `argMax(status, event_time)` keeps the status of the latest event. Of two events with the
  same `event_time`, the one with the larger `event_id` wins, so every region keeps the same
  status
  ([sql.md §7.2](../spec/sql.md#72-function-reference)).
- The alias can't be `status`, a column of the stream (DF5103).
- `CustomerOrderStates` reads one partition, one row per order, and counts the rows by
  state. It aggregates without `GROUP BY`, so it returns one row, and `:one` is allowed.
- The query reads every order the customer has. `explain` shows the cost per row read.

[examples/analytics](../../examples/analytics) uses the same pattern for the members of a
workspace in `dynaflux/activity.sql`, with a daily `uniq` rollup that a query merges over a
range of days ([sql.md §12.3](../spec/sql.md#123-activity-rollup)).

## 13. Query shapes

A query reads one view or one stream, and must fix its partition key. These are the shapes
that DynamoDB serves without a scan.

### One row with `:one`

A `:one` query returns one row, or an error that matches `dynaflux.ErrNotFound`. A first
attempt at a country's latest day, in `dynaflux/queries/latest.sql`:

<!-- doctest: file dynaflux/queries/latest.sql -->
```sql
-- name: CountryLatestDay :one
SELECT day, orders, sales
FROM country_daily
WHERE country = {country:String}
ORDER BY day DESC;
```

<!-- doctest: exit 1 -->
```console
$ go tool dynaflux validate
dynaflux/queries/latest.sql:1:1: error DF4304: CountryLatestDay: :one query can return more than one row
  hint: use :many, bind every view key with =, or add LIMIT 1
```

A query can be `:one` when it can't return more than one row: it aggregates without
`GROUP BY`, binds every view key with `=`, or ends with `LIMIT 1`
([sql.md §10.9](../spec/sql.md#109-one-and-many)):

<!-- doctest: file dynaflux/queries/latest.sql -->
```sql
-- name: CountryLatestDay :one
SELECT day, orders, sales
FROM country_daily
WHERE country = {country:String}
ORDER BY day DESC
LIMIT 1;
```

`explain` checks the query, and shows the read: one DynamoDB `Query`, newest day first,
that stops after one row.

```console
$ go tool dynaflux explain CountryLatestDay
query CountryLatestDay :one, from view country_daily
  assumptions  -rate 1000  -event-kb 1  -rows country_daily=1000
               (traffic isn't in the SQL or dynaflux.yaml; set it with these flags)
  prices       us-east-1 list prices, ±30%: for comparing designs, not a quote

  read         Query of country = {country}
  calls        1 Query per call, one per key combination
  items        every row of a key combination
  read cost    4.33 KB per row, and a Query rounds its total up to 4 KB: ~0.542 read units per row eventually consistent, ~1.08 with WaitFor, which reads with strong consistency
  limit        LIMIT 1 stops each Query after that many rows
```

### Several keys, ranges and totals

Totals for several countries over a range of days, in `dynaflux/queries/totals.sql`:

<!-- doctest: file dynaflux/queries/totals.sql -->
```sql
-- name: CountryTotals :many
SELECT country, sum(orders) AS total_orders, sum(sales) AS total_sales
FROM country_daily
WHERE country IN {countries:Array(String)}
  AND day BETWEEN {from:Date} AND {to:Date}
GROUP BY country
ORDER BY total_sales DESC;
```

```console
$ go tool dynaflux validate
```

Why it works ([sql.md §10.4](../spec/sql.md#104-key-binding-over-a-view)):

- `IN` reads each country's partition: one DynamoDB `Query` per value. A call reads at most
  100 key combinations, the product of the sizes of its `IN` lists. A longer literal list is
  DF4103; a longer `Array` parameter fails the call with an error that matches
  `dynaflux.ErrTooManyKeys`.
- A range binds the first sort-key column after the ones bound with `=` or `IN`. Two lower
  bounds on one column are DF4102.
- `GROUP BY` in a query combines the rows it read
  ([sql.md §10.7](../spec/sql.md#107-read-time-aggregation)). An alias can't repeat a
  column name of the view (DF5103): `total_orders`, not `orders`.
- Rows that tie in `ORDER BY` come in key order, so the result is the same in every region.
  There is no `OFFSET` in v1: narrow the key range instead.

### Queries over a stream

A customer's recent orders, read from the stream's log, in `dynaflux/queries/history.sql`:

<!-- doctest: file dynaflux/queries/history.sql -->
```sql
-- name: CustomerOrders :many
SELECT event_time, order_id, amount
FROM orders
WHERE customer_id = {customer_id:String}
  AND event_time >= {since:DateTime64(6)}
ORDER BY event_time DESC
LIMIT {n:UInt32};
```

`validate` warns, and exits with status 0:

```console
$ go tool dynaflux validate
dynaflux/queries/history.sql:3:6: warning DF4501: CustomerOrders: reads stream orders, which keeps events for 7 DAY after ingestion (LOG RETENTION). Older events are only in the archive.
  hint: query older events with Athena, or aggregate them in a view
```

Why it works ([sql.md §10.5](../spec/sql.md#105-key-binding-over-a-stream)):

- The query binds the stream's `PRIMARY KEY` with `=`, and takes a range of `event_time`,
  the stream table's sort key.
- `ORDER BY event_time DESC` reads the newest first, and `LIMIT` stops the read after `n`
  events. A `LIMIT` parameter takes an unsigned integer type.
- It sees only the events still in the log, 7 days here. Older ones are in the archive,
  for Athena; the warning says so.

## 14. Serve a query over HTTP

`-- expose: http` above a query serves it from a query function, to clients that don't use
the Go package. Replace `dynaflux/queries/totals.sql`:

<!-- doctest: file dynaflux/queries/totals.sql -->
```sql
-- name: CountryTotals :many
-- expose: http
SELECT country, sum(orders) AS total_orders, sum(sales) AS total_sales
FROM country_daily
WHERE country IN {countries:Array(String)}
  AND day BETWEEN {from:Date} AND {to:Date}
GROUP BY country
ORDER BY total_sales DESC;
```

`validate` still warns about the query over the stream from recipe 13:

```console
$ go tool dynaflux validate
dynaflux/queries/history.sql:3:6: warning DF4501: CustomerOrders: reads stream orders, which keeps events for 7 DAY after ingestion (LOG RETENTION). Older events are only in the archive.
  hint: query older events with Athena, or aggregate them in a view
```

Why it works:

- `generate` then writes a third template, `query.yaml`, with the query function. You deploy
  it in every region with the application. The `http` section of `dynaflux.yaml` chooses its
  front door and its `auth`: by default, a Lambda function URL that takes requests signed
  with IAM credentials ([config.md](../spec/config.md#http)).
- `auth` decides who can call, not which rows they read. Any caller that passes it can read
  every row the query reaches, with any parameters. Expose only queries whose rows
  every caller may see, such as these totals, and serve a customer's own data through your
  own service.
- [http.md](../spec/http.md) is the request and response format, with a Python client.

## 15. What makes a new version

Each definition of a view is a version, such as `country_daily@v1`, with its own table in
each region. `generate` numbers the versions in `dynaflux.lock`, a file you commit. To see
it, start a new project, in another empty directory, with one stream and one view:

<!-- doctest: dir versions -->
```sh
go mod init example.com/shop
go get -tool github.com/lucindo/dynaflux/cmd/dynaflux@latest
```

<!-- doctest: file dynaflux.yaml -->
```yaml
store: shop
regions: [us-east-1, us-west-2]
```

`dynaflux/streams/orders.sql`:

<!-- doctest: file dynaflux/streams/orders.sql -->
```sql
CREATE STREAM orders (
    customer_id String,
    country     LowCardinality(String),
    amount      Decimal(12, 2)
)
PRIMARY KEY (customer_id);
```

`dynaflux/views/country_daily.sql`:

<!-- doctest: file dynaflux/views/country_daily.sql -->
```sql
CREATE MATERIALIZED VIEW country_daily
ORDER BY (country, day)
PRIMARY KEY (country)
AS SELECT
    country,
    toDate(event_time) AS day,
    count()            AS orders,
    sum(amount)        AS sales
FROM orders
GROUP BY country, day;
```

```console
$ go tool dynaflux generate
country_daily@v1: new
```

Add the largest order of each day to the view:

<!-- doctest: file dynaflux/views/country_daily.sql -->
```sql
CREATE MATERIALIZED VIEW country_daily
ORDER BY (country, day)
PRIMARY KEY (country)
AS SELECT
    country,
    toDate(event_time) AS day,
    count()            AS orders,
    sum(amount)        AS sales,
    max(amount)        AS largest
FROM orders
GROUP BY country, day;
```

```console
$ go tool dynaflux generate
country_daily@v1: retained
country_daily@v2: new
```

Version 2 gets a new table, which a backfill fills before queries use it. Version 1 keeps
running until you retire it ([lifecycle.md §2](../spec/lifecycle.md#2-deploying-changes)).

Now add a column to the stream:

<!-- doctest: file dynaflux/streams/orders.sql -->
```sql
CREATE STREAM orders (
    customer_id String,
    country     LowCardinality(String),
    amount      Decimal(12, 2),
    coupon      Nullable(String)
)
PRIMARY KEY (customer_id);
```

`generate` prints nothing: the view keeps its version.

```console
$ go tool dynaflux generate
```

To fill a version again with an unchanged definition, after a gap in its events or a fix in
Dynaflux itself, give it a new number with `-rebuild`
([lifecycle.md §7.3](../spec/lifecycle.md#73-rebuilding-a-version)):

```console
$ go tool dynaflux generate -rebuild country_daily
country_daily@v2: retained
country_daily@v3: new
```

Why it works ([sql.md §9.9](../spec/sql.md#99-view-versions)):

- A version's fingerprint covers what decides its stored data: the stream, `ARRAY JOIN`,
  `WHERE`, the keys in `ORDER BY` order, the `PRIMARY KEY`, `SPLIT`, each aggregate and
  `TTL`. Changing any of them makes a new version.
- These don't: `MODE`, capacity, comments, formatting, the order of the `SELECT` and
  `GROUP BY` items, and equivalent spellings. Neither do additive changes to the stream: a
  new `Nullable` column, a new column with a `DEFAULT`, or a change to `LOG RETENTION` or
  `ACCEPT`.
- `dynaflux diff` refuses other changes to a deployed stream, such as removing a column or
  changing the `PRIMARY KEY` ([sql.md §8.9](../spec/sql.md#89-changing-a-stream)).
- Going back to the definition of a version that is still deployed makes that version
  current again, with no new number and no backfill.

## 16. Common diagnostics and their fixes

| Code | Cause | Fix | Recipe |
|------|-------|-----|--------|
| DF4101 | A query doesn't fix the partition key | Bind it with `=` or `IN`, or add a view keyed for the query | [5](#5-start-from-the-query-then-derive-the-views-keys) |
| DF4201 | A query combines `uniqExact` across rows | `uniq` in the view, `uniqMerge` in the query | [7](#7-choose-aggregates) |
| DF4304 | A `:one` query can return several rows | `LIMIT 1`, bind every key with `=`, or `:many` | [13](#one-row-with-one) |
| DF5405 | `now()` in a view or a query | A parameter | [6](#6-time-buckets) |
| DF3103 | A `Nullable` view key | `coalesce(x, '') AS x_key` | [8](#8-filters-and-array-join) |
| DF3302 | `TTL` on a column that isn't a time key of the view | `TTL` on the bucket key | [9](#9-expire-rows-with-ttl) |
| DF2113 | `ACCEPT` without an upper bound | An upper bound, such as `now() + INTERVAL 5 MINUTE` | [4](#4-the-accept-window) |
| DF5103 | An alias that repeats a column of its source | Another alias | [13](#several-keys-ranges-and-totals) |
| DF3501 | A `SYNC` view whose rows many stream keys share | Add the stream's `PRIMARY KEY` to `GROUP BY`, or use `ASYNC` | [11](#11-read-your-writes-with-sync) |
| DF4501 | A query over a stream: a warning | Athena for events older than the log, or a view | [13](#queries-over-a-stream) |
| DF1104 | A `SELECT` without `-- name:` | `-- name: <Name> :many` above it | |
| DF1102 | `JOIN`, `HAVING`, `CASE`, `LIKE` and other features outside the dialect | The hint, from [Table A.1](../spec/sql.md#table-a1-unsupported-sql-features-df1102) | |
| DF5201 | A ClickHouse function outside the dialect | The hint, from [Table A.2](../spec/sql.md#table-a2-replacements-for-unknown-functions-df5201) | |

Some habits from ClickHouse fail too. This view, in `dynaflux/views/country_stats.sql`,
has four:

<!-- doctest: file dynaflux/views/country_stats.sql -->
```sql
CREATE MATERIALIZED VIEW country_stats
ORDER BY (country)
AS SELECT
    country,
    count(*)                    AS orders,
    count(DISTINCT customer_id) AS customers,
    groupArray(customer_id)     AS buyers,
    sum(amount) / count()       AS avg_amount
FROM orders
GROUP BY country;
```

<!-- doctest: exit 1 -->
```console
$ go tool dynaflux validate
dynaflux/views/country_stats.sql:5:5: error DF5208: country_stats: count(*) is not supported
  hint: use count()
dynaflux/views/country_stats.sql:6:11: error DF5207: country_stats: DISTINCT inside count is not supported
  hint: use uniqExact(x), or uniq(x) for an estimate
dynaflux/views/country_stats.sql:7:5: error DF5213: country_stats: groupArray(customer_id) has unbounded state.
  hint: use uniq(customer_id) or uniqExact(customer_id) to count values, or argMax(customer_id, event_time) for the latest one
dynaflux/views/country_stats.sql:8:5: error DF3004: country_stats: sum(amount) / count() must be a GROUP BY key or a single aggregate
  hint: compute expressions over aggregates in a query
```

Each hint names the replacement:

<!-- doctest: file dynaflux/views/country_stats.sql -->
```sql
CREATE MATERIALIZED VIEW country_stats
ORDER BY (country)
AS SELECT
    country,
    count()                         AS orders,
    uniq(customer_id)               AS customers,
    argMax(customer_id, event_time) AS last_buyer,
    avg(amount)                     AS avg_amount
FROM orders
GROUP BY country;
```

```console
$ go tool dynaflux validate
```

- A view holds aggregates, and a query computes expressions over them: `avg(amount)` here,
  or `sales / orders` in a query.
- `groupArray` would grow without limit, past DynamoDB's 400 KB item limit. Count values
  with `uniq` or `uniqExact`, or keep the latest with `argMax`
  ([sql.md §7.6](../spec/sql.md#76-unsupported-aggregates)).
- [sql.md Appendix A](../spec/sql.md#appendix-a-diagnostics) lists every code with its
  message and hint.

## 17. How it differs from ClickHouse

SQL that Dynaflux accepts means what it means in ClickHouse 26.7 with UTC times, except for
the deviations in [sql.md §11](../spec/sql.md#11-deviations-from-clickhouse). Each one
makes every region compute the same result in any event order, or makes a value fit
DynamoDB. The ones you are most likely to notice:

- `uniq` is a HyperLogLog sketch, so its estimates differ from ClickHouse's.
- `quantile` uses a deterministic sketch, with no interpolation and no random sampling.
- `avg` and `quantile` return `NULL`, not `nan`, when no row contributes.
- `sum` is exact: an integer total never wraps, and fails when read if it's out of range. A
  `Float64` total is rounded once.
- `argMax`, `argMin` and `any` break ties by `event_time` and `event_id`, not by arrival
  order.
- `now()` exists only in `ACCEPT`.
- Expired rows and events are never returned, and results come in a fixed order.

SQL that Dynaflux doesn't accept fails `validate` with a hint; nothing is reinterpreted
([sql.md §1.1](../spec/sql.md#11-the-strict-subset-rule)). The structure differs too:

- `CREATE STREAM` replaces `CREATE TABLE`, and a view owns its `ORDER BY`, `PRIMARY KEY`
  and `TTL`.
- A view reads one stream. There are no joins, subqueries or views over views.
- Every query is declared ahead of time. For ad-hoc questions, query the archive with
  Athena.

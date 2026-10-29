<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Benchmarks

This runbook has two kinds of benchmark:

- **The Go benchmarks** ([§1](#1-go-benchmarks)) measure the CPU time and allocations of the
  hot paths on the in-memory store, not DynamoDB's latency. Anyone can run them. The
  maintainer records the baseline that later runs compare with.
- **The throughput benchmark** ([§2](#2-throughput-benchmark-prerequisites) to
  [§10](#10-results)) loads a deployed store at the plan's scale, 1,000 events per second,
  and measures what each region does with the load: view lag, projector invocations,
  request units and cost, each next to what `dynaflux explain` predicts. It also checks that
  every region's views count exactly what was appended. It runs on DynamoDB Local in CI, at
  a low rate. On AWS, only the maintainer runs it, with their own credentials. AI
  assistants and CI never do.

## 1. Go benchmarks

Each package on a hot path has a `bench_test.go`. The benchmarks run the
[example](../../examples/analytics/README.md)'s streams, views and queries, read once before
the timed loop, so they follow the example as it changes.

| Package | Benchmark | One operation |
|---------|-----------|---------------|
| `internal/keys` | `Key`, `StreamSK`, `EventID` | Encode a view row's key, encode a stream item's sort key, or make one event ID. |
| `internal/keys` | `HWM/units={1,20,100}` | Decode a row's high-water marks, advance one unit and encode them again, as every row write does. For a `page_daily` row at 1,000 events/s, `dynaflux explain page_daily` predicts about 21 marks. |
| `internal/agg` | `RowUpdate/<aggregate>` | The projector's work on one aggregate of a row: decode the stored state, add one input, encode it. |
| `internal/agg` | `Merge/<aggregate>` | A query's work on one more row: decode the row's state, merge it, finalize. |
| `internal/sql` | `Parse` | Parse every SQL file of the example. Also reports MB/s. |
| `internal/validate` | `Compile` | Validate and compile the example project, as `dynaflux validate` and `generate` do. |
| `internal/engine` | `Append/async`, `Append/async_batch100` | Append one page view, or a batch of 100, to `page_views`, whose views are all `ASYNC`. The batch also reports ns/event. |
| `internal/engine` | `Append/sync` | Append one call to `api_calls`, whose `SYNC` view `quota_usage` the append updates in its transaction. |
| `internal/engine` | `Query/{PageStats,TagTotals,RecentPageViews}` | Run one of the example's named queries over filled views, or over a stream partition. |
| `internal/projector` | `Handle/pages={1,50,1000}` | Apply one delivery of 100 page views to the example's views. `pages` is how many rows the batch spreads over: 1 is one hot row, 1,000 nearly one row per event. Also reports ns/event. |

The aggregates are `count`, `sum` of `UInt32` and of `Float64`, `max` of `DateTime64`,
`argMax`, and two sketches at two sizes each: `uniq` sparse (`n=100`) and dense
(`n=100000`), and `quantile` after 100 and 100,000 inputs.

The engine and projector benchmarks check their results, so code that skips its work fails
instead of looking fast:

- The append benchmarks fail on any event that isn't stored. They use a backend that drops
  writes, so every iteration finds the same state. `Append/sync` first stores 100 calls, so
  each iteration merges into a populated row.
- The query benchmarks check their row counts.
- `Handle` checks that `page_daily` and `tag_hourly` counted every event it delivered.

The other benchmarks check only that each call succeeds. Their packages' tests check what
the calls compute.

### Run them

```sh
make bench
```

- It runs every benchmark 6 times (`BENCH_COUNT`), one package at a time, since packages
  benchmarked in parallel slow each other down.
- It prints the results and writes them to `bench.txt` (`BENCH_OUT`), which git ignores.
- The first line is the Go version, such as `go: go1.26.10`. benchstat reads it as part of
  the configuration, since allocations change with the toolchain.
- It exits non-zero when any benchmark fails.
- A run takes about 5 minutes on a 12-core Apple silicon laptop.

`make bench-smoke` runs each benchmark once, with its checks, in under 20 seconds.
`make verify` runs it, and so does CI. CI runs no timing gate: shared runners are too noisy.

### Compare with the baseline

```sh
make bench
make bench-compare
```

`bench-compare` runs [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) over
`docs/runbooks/benchmarks-baseline.txt` and `bench.txt`. It installs benchstat into
`$(go env GOPATH)/bin` when it's missing, and `make tools-update` updates it.

benchstat prints one table per unit: `sec/op`, `B/op` and `allocs/op`, then `sec/event` and
`B/s` for the benchmarks that report ns/event or MB/s. Each row has the median of the
baseline and of the new run, each with its 95% confidence interval (`±`), then the change: a
percentage with its p-value, or `~` when the difference isn't significant.

- Count a change only when benchstat marks it significant (p < 0.05) and it moves by more
  than about 10%.
- Times compare only on the machine that recorded the baseline. Across machines, compare
  only `allocs/op` and `B/op`, with the same Go version.

### Record the baseline

Record a baseline with each release, on an idle machine on AC power, with no other Go
builds running:

```sh
make bench BENCH_COUNT=10
cp bench.txt docs/runbooks/benchmarks-baseline.txt
```

Commit it with the release. Git history keeps the older ones. Ten runs give narrower
confidence intervals than the default six.

### What they show

None of these is a defect. Each is a place to look before optimizing:

- **High-water marks dominate a row write.** `HWM`'s time and allocations grow about in
  step with the number of units, since every row write decodes and encodes the whole map.
  At 20 units it costs many times any scalar aggregate's `RowUpdate`, and
  `Handle/pages=1000`, one row per event, shows it. It's the clearest optimization target,
  and the change would be internal.
- **The projector's CPU per event is small next to `explain`'s model.** `Handle`'s ns/event
  is a small fraction of the 2 ms per event, plus 30 ms per invocation, that `explain`
  assumes. DynamoDB's round trips dominate that model.
- **`uniq` is the most expensive aggregate.** Its `RowUpdate` and `Merge` cost the most of
  all the aggregates, and a dense sketch's `RowUpdate` costs more than a sparse one's.
  `Query/TagTotals` merges a `uniq` state for each row it reads.
- **A batch append saves no engine CPU.** `Append/async_batch100`'s ns/event is about the
  time of one `Append/async`.

## 2. Throughput benchmark: prerequisites

The throughput benchmark runs on the store of the [end-to-end suite](e2e.md): the example
with `e2e.sql` added, so the stream `page_views` feeds three views, `page_daily`,
`tag_hourly` and `e2e_ratio`.

| Path | What it is |
|------|------------|
| `examples/analytics/e2e/throughput_test.go` | `TestThroughput` loads the store, checks the views and writes the result file. `TestThroughputReport` reads that file and CloudWatch, and prints the report. Both skip unless `-throughput.out` is set. |
| `examples/analytics/e2e/throughput_report_test.go` | The report's arithmetic, and its test on the fixture in `testdata/throughput/`. The file has no build tag, so `make test` runs that test. |
| `scripts/e2e/prewarm.sh` | Raises the warm throughput of tables before a run. |
| `scripts/e2e/bench-cost.sh` | Prints what the sandbox account cost, by usage type, from Cost Explorer. |

For a run on AWS:

- Everything that [e2e.md §1](e2e.md#1-prerequisites) lists for AWS mode except
  `E2E_APP_ROLE`. The role also needs CloudWatch `GetMetricData`, DynamoDB `UpdateTable`
  with warm throughput, and Cost Explorer `GetCostAndUsage`. An administrator role in the
  sandbox covers them.
- A budget alert on the sandbox account. A full run with two regions costs about $90
  ([§8](#8-cost)).
- Regions: the example's two, `us-east-1` and `us-west-2`, unless you set `E2E_REGIONS`, a
  list separated by spaces, such as `E2E_REGIONS="us-east-1 us-west-2 eu-west-1"`. The
  scripts and the suite all read it, so every command of a run needs the same value. Take
  `eu-west-1` as the third region: Firehose takes 5 MiB/s per stream only in `us-east-1`,
  `us-west-2` and `eu-west-1`, and 1 MiB/s elsewhere. Each region's archive stream takes
  all 1,000 events per second, which with 1 KB events is close to 1 MiB/s.
- A machine that stays awake on a wired network: the load runs on your machine and sends
  about 1 to 2 MB/s. On macOS, run the target under `caffeinate -i`.

## 3. Local validation

```sh
make bench-emulator
```

It starts DynamoDB Local unless it runs, then runs `TestThroughput` with
`-target=emulator` at 50 events per second: 5 seconds of warm-up, then a 30-second window.
It takes about a minute, and CI's integration job runs it after `make e2e-emulator`.

- Each region is a database of its own in DynamoDB Local, as in
  [e2e.md §2](e2e.md#2-emulator-mode), so nothing replicates: a region's views count only
  its own appends, and a probe is timed only in its own region.
- A loop in each region drains the streams through the projector while the load runs.
- It checks everything that a run on AWS checks ([§4](#4-run)). It prints no report,
  since the report reads CloudWatch.
- It writes the result to `bench-emulator.json` in a new temporary directory, and prints
  its path first.

## 4. Run

Start with a shakedown: 11 minutes at 100 events per second, for about a dollar of load.
It warms `tag_hourly` a little, which tests `prewarm.sh` for cents:

```sh
mkdir -p ~/bench
make bench-aws E2E_RUN=s1 E2E_ACCOUNT=<sandbox account ID> THROUGHPUT_OUT="$HOME/bench/s1.json" \
  BENCH_PREWARM="view-tag_hourly-v1=4500" \
  BENCH_FLAGS="-throughput.rate=100 -throughput.warmup=1m -throughput.duration=10m"
```

Then the full run, at 1,000 events per second, with 10 minutes of warm-up and a 60-minute
window:

```sh
make bench-aws E2E_RUN=b1 E2E_ACCOUNT=<sandbox account ID> THROUGHPUT_OUT="$HOME/bench/b1.json" \
  BENCH_PREWARM="stream-page_views=20000 view-tag_hourly-v1=8300"
```

Each run takes a new `E2E_RUN`. For three regions, add
`E2E_REGIONS="us-east-1 us-west-2 eu-west-1"`. The target:

1. checks `THROUGHPUT_OUT`: set, a file that doesn't exist yet, in a directory that
   exists, and outside the repository. It has no default, and it isn't `BENCH_OUT`, so a
   result never lands in `bench.txt`, and the report never reads an earlier run's result;
2. compiles the suite with `go test -c -tags e2e`;
3. checks `E2E_RUN`, `E2E_ACCOUNT` and `E2E_REGIONS`, and that the credentials are for
   `E2E_ACCOUNT`. Nothing is deployed before this step;
4. deploys the store with `scripts/e2e/deploy.sh` ([e2e.md §3](e2e.md#3-aws-mode));
5. with `BENCH_PREWARM` set, runs `scripts/e2e/prewarm.sh` with its value as arguments;
6. runs `TestThroughput` with `-target=aws` and `BENCH_FLAGS`, for at most 180 minutes;
7. runs `TestThroughputReport` ([§6](#6-report)), even after step 6 failed;
8. tears the run down with `scripts/e2e/teardown.sh`, even after a failure, and exits with
   the status of the first step that failed.

A full run takes 2 to 2½ hours: about 15 minutes to deploy, 10 to 30 to pre-warm, 70 of
load, up to 10 to check the views, 5 waiting for CloudWatch, and 10 to 20 to tear down.

### Pre-warming

A new on-demand table takes 4,000 writes per second at once, and throttles traffic that
more than doubles its peak within 30 minutes. `explain` predicts 5,520 write units per
second on `tag_hourly` in each region at 1,000 events per second, so the full run warms it
to about 1.5 times that.

`explain` also models a 2-day log in steady state: about 20 partitions, each a shard of the
table's change stream. A new table has fewer, so its batches are larger and its rows
smaller, and it costs less than `explain` predicts for a reason the model leaves out.
Warming the stream table to 20,000 write units, 1,000 per partition, should give it about
20 partitions. The result counts the open shards, and the report evaluates `explain` at
both counts.

`prewarm.sh` takes each table as `<table>=<units>`, the table named without the
`e2e-<id>-analytics-` prefix: `stream-page_views`, `view-page_daily-v1`,
`view-tag_hourly-v1` or `view-e2e_ratio-v1`. A stream table is a global table, so the
script raises it in the home region and every replica follows. It raises a view table in
each region, and skips one that is already that warm. Then it waits until every region
reports the new value, for at most an hour. Warm throughput can't be lowered, and each unit
raised is billed once ([§8](#8-cost)).

### The load

- The rate is split evenly over the regions. In each region, the load appends one event per
  call through the example's generated client, as an application does, with
  `-throughput.workers` calls in flight. Its HTTP pool keeps a connection for each worker:
  the SDK's default keeps 10 for each host, and every other call would open a new TLS
  connection.
- Users, pages and tags are drawn uniformly from 10,000 users, 1,000 pages and 200 tags,
  with 0 to 3 tags per event. So `page_daily` writes to about 1,000 rows at a time and
  `tag_hourly` to 200, which the report passes to `explain` as `-rows`.
- Each event is due at a fixed time. When every worker is busy, the load counts the event as
  missed instead of sending it late, so a slow client shows as a lower rate rather than a
  burst. An append's latency counts from the time it was due.
- Every 2 seconds, each region appends a probe: a page view of a page of its own, which
  makes a new `page_daily` row. In each region the append reaches, a poller runs
  `PageStats` every 200 ms until the row shows, which times the lag between each pair of
  regions.
- A failed append may still be stored, after a timeout or an attempt that the SDK retried,
  so its outcome is unknown. More than 100 fail the run.
- Every minute, the test logs each region's appends so far and its rate over the last
  minute.

After the window, the test checks every region's views, for at most 10 minutes:

- `page_daily`: the views and visitors (`uniqExact`) of every page and day, through
  `PageStats`, equal the load's counts.
- `tag_hourly`: each tag's `total_views` through `TagTotals` equals the load's count. Its
  `total_users` (`uniq`) equals the estimate of a sketch the load keeps of the tag: a
  sketch doesn't depend on the order of its inputs, and merges by taking the larger of each
  register.
- No view version of `page_views` is broken.

Each append with an unknown outcome widens these checks by itself: a count may be up to
that much higher, and `total_users` may be up to the estimate of a sketch that includes it.

| Flag | Default | What it sets |
|------|---------|--------------|
| `-throughput.out` | none | The result file. `TestThroughput` rewrites it after each phase, so it stays whole if the run stops. |
| `-throughput.rate` | `1000` | Events per second, all regions together. |
| `-throughput.warmup` | `10m` | How long to load before the window. On AWS, the window then starts at the next whole minute, as CloudWatch's datapoints do. |
| `-throughput.duration` | `60m` | The window. On AWS, it must be whole minutes. |
| `-throughput.workers` | `128` | Calls in flight in each region. |

### One step at a time

To keep the store after the run, for example to read the projector's logs, run the steps by
hand:

```sh
export E2E_RUN=b2 E2E_ACCOUNT=<sandbox account ID>
scripts/e2e/deploy.sh
scripts/e2e/prewarm.sh stream-page_views=20000 view-tag_hourly-v1=8300
go test -tags e2e -timeout 180m -v ./examples/analytics/e2e -run '^TestThroughput$' -target=aws -throughput.out="$HOME/bench/b2.json"
go test -tags e2e -timeout 30m -v ./examples/analytics/e2e -run '^TestThroughputReport$' -target=aws -throughput.out="$HOME/bench/b2.json"
scripts/e2e/teardown.sh
```

If your machine can't keep up with the rate ([§9](#9-what-a-failure-means)), run the
`TestThroughput` line on a small EC2 instance in the home region instead, from a clone of
the same commit with Go installed, and the other steps on your machine. The scripts don't
create the instance.

## 5. Watch

- The test's log: each region's appends and rate every minute.
- The dashboards `e2e-<id>-analytics-<region>`: the projector's iterator age, errors and
  throttles.
- The `WriteThrottleEvents` metric of the stream and view tables, and the `missed` count in
  the log: either one means the result won't be at the rate it asked for.

## 6. Report

`TestThroughputReport` waits until 5 minutes after the window, since Lambda's and DynamoDB's
datapoints land late. Then it reads each statistic over the window from CloudWatch, as one
datapoint, and prints the report. It also writes the statistics beside the result file:
`b1.json` gets `b1.metrics.json`. It starts no harness, so it can run after teardown, for up
to 15 days.

The report starts with the window, the rate the load reached, and the `explain` command
that it evaluates. That command takes the reached rate as `-rate`, the mean tags per event
as `-pass tag_hourly`, and as `-uniq-new page_daily` the share of the window's appends that
brought a new page, day and user.

Then, for each region, it has four columns: `explain` at its own partition count, `explain`
at the shard count measured in the region, the measured value, and the ratio of the measured
value to the second column. A unit per event is the units measured in the region, divided
by the events appended in all regions in the window: `explain`'s "per event, per region".

| Line | Measured from | A large ratio points at |
|------|---------------|-------------------------|
| stream shards | the open shards of the stream table's change stream, after the load | Pre-warming didn't split the table as `explain` assumes. Read the other lines against the second column. |
| invocations/s, events per invocation | Lambda `Invocations`, and the event source mapping's `PolledEventCount` divided by `Invocations` | Batches unlike the model's. The projector serves every stream, but only `page_views` has traffic. |
| duration ms | Lambda `Duration`: average, p50, p99 and maximum | `explain`'s 30 ms per invocation plus 2 ms per event (`invocationSeconds` and `perEventSeconds` in `internal/explain/model.go`). |
| concurrency, max | Lambda `ConcurrentExecutions`, the peak | `explain`'s value is an average: invocations times duration. With no data, Lambda doesn't report the metric for this function; use that product. |
| iterator age ms | Lambda `IteratorAge` | The projector falling behind. |
| stream table writes | `ConsumedWriteCapacityUnits` with `Source=Customer`, the local puts, plus `Source=GlobalTable`, the replicated ones | Events over 1 KB: `explain` takes 1 write unit each. The line "by TableName alone" shows whether that dimension counts replicated writes too. |
| meta table writes | `ConsumedWriteCapacityUnits` of the meta table | Progress items: one per batch. |
| view writes and reads | `ConsumedWriteCapacityUnits` and `ConsumedReadCapacityUnits` of each view table, `uniqExact` markers included | Rows larger than the model's, from high-water marks, or more rows per batch. `dynaflux explain <view>` shows the row it assumes. |
| failed conditional writes | `ConditionalCheckFailedRequests` of each view table | `explain` assumes one row write in ten loses a race and retries (`conflictFactor`). |
| $/month of requests | each table's measured units per second, at `explain`'s on-demand prices | The lines above, in dollars. |
| TTL deletes | not measured: they start 2 days after the first event | |
| $/month of Lambda and Firehose | `explain` only | `bench-cost.sh` gives the bill ([§8](#8-cost)). |
| throttled requests and invocations | `WriteThrottleEvents` and `ReadThrottleEvents` of every table, and Lambda `Throttles` | Any throttle fails the run. |
| replication ms | `ReplicationLatency` of the stream table to each other region | |
| archive | Firehose `IncomingRecords` per event, `IncomingBytes` per record, `ThrottledRecords`, and `DeliveryToIceberg.DataFreshness` | Throttled records fail the run. |

Then come the view lag of each pair of regions, with n, p50, p90, p99, the maximum and the
probes that never showed, then each region's client: its rate in the window, missed and
unknown appends, append latency, and the failed appends by error class, each with a message.

The report ends with its verdict: `passed`, or each reason the run failed. The reasons are
those of [§9](#9-what-a-failure-means), and any throttle, failed invocation,
`BreakerOpened`, `RowTooLarge` or throttled archive record in the metrics.

`make test` checks the report's arithmetic on the fixture in
`examples/analytics/e2e/testdata/throughput/`. Its numbers are made up near `explain`'s,
since no run on AWS has been recorded yet. After the first full run, replace `result.json`
and `metrics.json` with the run's two files, and rewrite `report.txt`:

```sh
go test ./examples/analytics/e2e -run '^TestThroughputReportOfFixture$' -update
```

## 7. Teardown

`make bench-aws` tears down every run. After an interrupted run, run `teardown.sh` by hand
with the same variables, `E2E_REGIONS` included: without it, the teardown finds a third
region only through the stream tables' replicas, and misses its stacks once those are gone.

```sh
E2E_RUN=b1 E2E_ACCOUNT=<sandbox account ID> E2E_REGIONS="us-east-1 us-west-2 eu-west-1" scripts/e2e/teardown.sh
```

[e2e.md §5](e2e.md#5-teardown) says what it deletes. Warm throughput goes with its table.

## 8. Cost

`explain` prices the load on the e2e store. To run it there:

```sh
work="$(mktemp -d)"
cp -R examples/analytics/dynaflux.yaml examples/analytics/dynaflux.lock examples/analytics/dynaflux "$work/"
cp examples/analytics/e2e/testdata/e2e.sql "$work/dynaflux/"
go build -o "$work/bin/dynaflux" ./cmd/dynaflux
(cd "$work" && bin/dynaflux explain page_views -rows tag_hourly=200 -pass tag_hourly=1.5)
```

It gives $34,902 a month with two regions, and $53,200 with a third one in the example's
`regions`, at us-east-1 list prices, ±30%. Almost all of it is DynamoDB requests.

| | 2 regions | 3 regions |
|-|-----------|-----------|
| An hour of load at 1,000 events per second | $48 | $73 |
| A full run: 70 minutes of load, and the drain | $65 | $100 |
| Pre-warming a full run | $27 | $40 |
| The shakedown: 11 minutes at 100 events per second, `tag_hourly` warmed to 4,500 | $1.50 | |

Add the few dollars of the deploy ([e2e.md §7](e2e.md#7-cost)). DynamoDB bills pre-warming
once for each unit raised, at the write unit-hour price ($0.00065 in us-east-1), in each
replica's region: for two regions, the stream table's 16,000 units cost about $21, and
`tag_hourly`'s 4,300 about $6. Check the DynamoDB pricing page before the first run.

The day after a run, read the bill:

```sh
E2E_RUN=b1 E2E_ACCOUNT=<sandbox account ID> scripts/e2e/bench-cost.sh 2026-11-02 2026-11-03
```

It prints the account's cost from the first day up to the second (UTC), by usage type, the
largest first, and the total. Cost Explorer counts the whole account by UTC day, and lags
by up to a day:

- Run `bench-aws`, `e2e-aws` and the guide's acceptance run on different UTC days.
- For a run that crosses midnight, read both days.

The bill checks the prices, where the report checks the units. It also settles whether
Firehose bills delivery to Iceberg per record rounded up to 5 KB, as its quota page says:
compare its Firehose line with the report's bytes per record.

## 9. What a failure means

| Failure | Likely cause |
|---------|--------------|
| `make bench-aws` stops at once about `THROUGHPUT_OUT` | It is unset, the file exists, its directory doesn't exist, or it is inside the repository. |
| "events/s appended in the window, short of the target" | The client can't keep up, and the log's `missed` counts the events that no worker was free to send. Raise `-throughput.workers`, use a wired network, or run the load from EC2 ([§4](#one-step-at-a-time)). |
| "appends throttled", or throttles in the report | A table wasn't warm enough: warm it ([§4](#pre-warming)), or `explain` underestimates its writes. |
| "appends with an unknown outcome, more than 100" | Timeouts or errors. The report's client section lists them by class, each with a message. |
| "probes never showed" | A region didn't apply a probe within 5 minutes: its projector is stuck or far behind. Read its iterator age and errors. |
| "views against the generators" | A region's views don't count what was appended: a projector bug, or records the projector missed. Run the steps one at a time ([§4](#one-step-at-a-time)) to keep the function logs and the meta table. |
| "view version … broke", or `BreakerOpened` in the report | An event broke a view. The load never sends `duration_ms` 0, which breaks `e2e_ratio`, so this is a bug. |
| `RowTooLarge` in the report | A view row passed DynamoDB's 400 KB item limit. |
| Failed projector invocations in the report | The projector's log group has the errors. |
| Throttled archive records in the report | The region's Firehose quota ([§2](#2-throughput-benchmark-prerequisites)). |
| "no data" in the report | CloudWatch has no datapoint for that statistic in the window: the metric isn't emitted for the resource, or the result names the wrong one. |
| `TestThroughputReport`: "the report needs a run on aws, in whole minutes" | The result file is from `bench-emulator`, or isn't a result file. |

## 10. Results

Copy this block for each run into the results log of the release being tested:

```markdown
### Throughput run <E2E_RUN>

- Date (UTC), Dynaflux commit, regions:
- Rate, warm-up, window, workers, BENCH_PREWARM:
- Where the load ran, and its network:
- The report:
- The bill from bench-cost.sh:
- Verdict:
```

The questions a first run answers:

| Question | Answered by | Answer |
|----------|-------------|--------|
| Does warming the stream table to 20,000 write units give about `explain`'s 20 partitions? | The report's stream shards | |
| Does `ConsumedWriteCapacityUnits` with only `TableName` count replicated writes? | The report's "by TableName alone", against local plus replicated | |
| Does Lambda report `ConcurrentExecutions` for a function without reserved concurrency? | The report's concurrency | |
| What are Cost Explorer's usage types for replicated writes and for Firehose delivery to Iceberg? | `bench-cost.sh` | |
| Does Firehose bill delivery to Iceberg per record rounded up to 5 KB? | `bench-cost.sh`, against the report's bytes per record | |
| Can one machine append 1,000 events per second across 2 or 3 regions? | The report's client section | |

Recalibrate `explain`'s constants (`invocationSeconds`, `perEventSeconds` and
`conflictFactor` in `internal/explain/model.go`) from these results, in a separate change
that cites the run.

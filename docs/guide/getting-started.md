<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Getting started

This guide builds a store of page views. You run it on your machine first, against
DynamoDB Local, then deploy it to two AWS regions, use it, and delete it. Every command and
value you need is on this page; the links are for depth.

## Contents

1. [What you'll build](#1-what-youll-build)
2. [Prerequisites](#2-prerequisites)
3. [Start a project](#3-start-a-project)
4. [Estimate the cost](#4-estimate-the-cost)
5. [Generate the code](#5-generate-the-code)
6. [Test with fluxtest](#6-test-with-fluxtest)
7. [Run the store on your machine](#7-run-the-store-on-your-machine)
8. [Load and read events](#8-load-and-read-events)
9. [Run your application against dev](#9-run-your-application-against-dev)
10. [Change a view](#10-change-a-view)
11. [Deploy to two regions](#11-deploy-to-two-regions)
12. [Use the deployed store](#12-use-the-deployed-store)
13. [Tear down](#13-tear-down)
14. [Next](#14-next)

## 1. What you'll build

The store `shop` holds one stream of events, `page_views`, and one view of it,
`page_daily`, with the views, visitors and average time of each page per day. A named
query, `PageStats`, reads the view. Dynaflux generates a typed Go client for them, and the
AWS infrastructure that runs them in `us-east-1` and `us-west-2`.

Steps 3 to 10 run on your machine and cost nothing. Steps 11 to 13 deploy the store to a
sandbox AWS account, use it, and delete it. Do them in one sitting: plan on an hour, most
of it waiting for CloudFormation.

**What the AWS part costs (an estimate).** At the trial's traffic,
`explain -rate 0.01 page_views` estimates the store's DynamoDB requests and storage,
Lambda, Firehose, S3 and replication at well under a dollar a month for both regions.
(`explain` needs a rate above 0, and one event every 100 seconds is more than the trial
sends.) Some resources cost the same whatever the traffic:

- 2 CloudWatch dashboards, one per region: $3 per dashboard a month, after the 3 that an
  account gets free.
- 24 CloudWatch alarms, 12 per region, over 26 alarm metrics: $0.10 per alarm metric a
  month, after the 10 free ones.
- Point-in-time recovery of the stream table, charged per GB stored: the trial stores a few
  kilobytes.

CloudWatch charges dashboards and alarms by the hour, so a trial of a few hours costs well
under a dollar. Check your bill the next day.

## 2. Prerequisites

You need macOS or Linux, with bash or zsh, and Go 1.26 or later and Docker:

<!-- doctest: skip -->
```sh
go version       # go1.26 or later
docker version   # shows a Server section: the Docker daemon is running
```

For the AWS part, steps 11 to 13, you also need:

- An AWS account where you can create and delete anything, such as a sandbox account. An
  administrator role there is enough. [infra.md §9.8](../spec/infra.md#98-operators) lists
  what the `dynaflux` commands need, for a narrower role later.
- The AWS CLI v2 and the SAM CLI, with credentials for that account.
- Lake Formation at its defaults in both regions. Otherwise the deploy succeeds and the
  archive fails later.

Check them:

<!-- doctest: skip -->
```sh
aws --version                 # aws-cli/2.x
sam --version
aws sts get-caller-identity   # the sandbox account's ID
aws lakeformation get-data-lake-settings --region us-east-1
aws lakeformation get-data-lake-settings --region us-west-2
```

In the output of each `get-data-lake-settings`, `CreateDatabaseDefaultPermissions` and
`CreateTableDefaultPermissions` must both list the principal `IAM_ALLOWED_PRINCIPALS` with
the permission `ALL`. If they don't, use another account: v1 reaches the archive with IAM
only ([infra.md §11](../spec/infra.md#11-deploying)).

Last, note whether SAM's deployment stack already exists in each region. The deploy creates
it where it doesn't, and step 13 deletes it only there:

<!-- doctest: skip -->
```sh
aws cloudformation describe-stacks --stack-name aws-sam-cli-managed-default --region us-east-1
aws cloudformation describe-stacks --stack-name aws-sam-cli-managed-default --region us-west-2
```

An error that says the stack does not exist means that the trial will create it.

## 3. Start a project

A Dynaflux project is a Go module. Make a directory for it:

<!-- doctest: skip -->
```console
$ mkdir shop
$ cd shop
```

Every command from here on runs in that directory. Start the module and a Git repository,
and add the `dynaflux` command to the module as a Go tool, so that `go.mod` pins its
version:

```console
$ go mod init example.com/shop
go: creating new go.mod: module example.com/shop
```

<!-- doctest: integration -->
```sh
git init
```

```sh
go get -tool github.com/lucindo/dynaflux/cmd/dynaflux@latest
```

`go get` lists the modules it adds. Check what you got:

<!-- doctest: output any -->
```console
$ go tool dynaflux version
cli             v1.0.0
library         v1.0.0
generated code  version 1
```

`cli` and `library` should both show the latest release. Just after a release, the Go
module proxy can still serve the one before: wait a few minutes and run `go get` again.

Now start the project. `init` writes a page-view example to replace with your own later.
Its `Next:` list names the commands without `go tool`, which you put before each `dynaflux`
command.

```console
$ go tool dynaflux init -store shop -region us-east-1 -region us-west-2
wrote dynaflux.yaml
wrote dynaflux/queries/pages.sql
wrote dynaflux/streams/page_views.sql
wrote dynaflux/views/page_daily.sql

Next:
  dynaflux generate  write the Go client package, the runtime's main.go and the templates
  go mod tidy        add the modules that the generated code imports to go.mod
  dynaflux dev       run the store on DynamoDB Local
```

The four files:

<!-- doctest: integration -->
```console
$ cat dynaflux.yaml
# The Dynaflux store. Every key is described in
# https://github.com/lucindo/dynaflux/blob/main/docs/spec/config.md
store: shop
regions: [us-east-1, us-west-2]
sql: ["dynaflux/**/*.sql"]
$ cat dynaflux/streams/page_views.sql
-- Page views: one event per page a user opens.
CREATE STREAM page_views (
    user_id     String,
    page        String,
    duration_ms UInt32
)
PRIMARY KEY (user_id);
$ cat dynaflux/views/page_daily.sql
-- Views and visitors per page and day.
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
$ cat dynaflux/queries/pages.sql
-- name: PageStats :many
SELECT day, views, visitors, avg_ms
FROM page_daily
WHERE page = {page:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;
```

- `dynaflux.yaml` names the store and its regions. The first region, `us-east-1`, is the
  home region, where the stream table's stack is deployed.
  [config.md](../spec/config.md) describes every key.
- The stream `page_views` holds events. Its `PRIMARY KEY` groups them by user. Every event
  also has implicit columns, such as `event_time`
  ([sql.md §8.3](../spec/sql.md#83-implicit-columns)).
- The materialized view `page_daily` keeps one row per page and day, and updates as events
  arrive. Its `PRIMARY KEY` is the DynamoDB partition key; the rest of `ORDER BY` sorts the
  rows in a partition.
- The query `PageStats` fixes the partition key and takes a range of days, so it costs one
  DynamoDB `Query` on one partition.

Check the project. No output means no problems:

```console
$ go tool dynaflux validate
```

## 4. Estimate the cost

`explain` estimates what a stream and its views cost. Traffic isn't in the SQL, so give
the rate in events a second; without `-rate`, it assumes 1,000.

```console
$ go tool dynaflux explain -rate 100 page_views
stream page_views
  assumptions  -rate 100  -event-kb 1  -rows page_daily=1000  -uniq-new page_daily=0.3
               (traffic isn't in the SQL or dynaflux.yaml; set it with these flags)
  prices       us-east-1 list prices, ±30%: for comparing designs, not a quote

layout, in each of 2 regions (us-east-1, us-west-2)
  table        shop-stream-page_views: Global Table, on-demand
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

- `per event, per region` counts the DynamoDB write and read units that one event costs in
  each region: the event itself, its expiry after `LOG RETENTION`, the view's rows, and
  the items that keep track of them.
- `monthly` turns them into dollars at the rate you gave, in each region and for both.
- `explain page_daily` and `explain PageStats` show the view and the query on their own.

[cost.md §5](cost.md#5-reading-explain) explains every line.

## 5. Generate the code

```console
$ go tool dynaflux generate
page_daily@v1: new
```

`page_daily@v1` is the view's first version. Then add the modules that the generated code
imports to `go.mod`:

```sh
go mod tidy
```

`generate` wrote:

| Path | What it is |
|------|------------|
| `internal/flux/` | The typed Go client: a struct per stream, a method per append and query. |
| `cmd/dynaflux-runtime/main.go` | The Lambda binary that runs the projector and the store's other functions. |
| `infra/dynaflux/` | The CloudFormation templates: `global.yaml`, its stack policy, and `regional.yaml`. |
| `dynaflux.lock` | The view versions and their fingerprints. |

Commit all of it, `dynaflux.lock` included. A change to a view that changes what its rows
hold makes a new version, and `generate` reads the lock to number it
([sql.md §9.9](../spec/sql.md#99-view-versions)). Run `generate` after every change to the
SQL files or `dynaflux.yaml`.

Leave out two files that later steps create: `dev.env`, which holds fake credentials, and
`.aws-sam/`, SAM's build. Create `.gitignore`:

<!-- doctest: file .gitignore -->
```text
dev.env
.aws-sam/
```

## 6. Test with fluxtest

`fluxtest` runs a store in memory, so a unit test needs no DynamoDB. Create
`pages_test.go`:

<!-- doctest: file pages_test.go -->
```go
package shop_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/fluxtest"

	"example.com/shop/internal/flux"
)

func TestPageStats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := flux.New(fluxtest.NewStore(t, flux.Catalog))
		day := time.Now() // 2000-01-01 00:00 UTC, the bubble's clock
		for _, v := range []flux.PageView{
			{UserID: "u1", Page: "/home", DurationMS: 100},
			{UserID: "u2", Page: "/home", DurationMS: 300},
			{UserID: "u1", Page: "/home", DurationMS: 200},
		} {
			v.EventTime = time.Now()
			if _, err := client.AppendPageView(t.Context(), v); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Hour) // returns at once: the bubble's clock moves on
		}
		rows, err := client.PageStats(t.Context(), flux.PageStatsParams{Page: "/home", From: day, To: day})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].Views != 3 || rows[0].Visitors != 2 ||
			rows[0].AvgMS == nil || *rows[0].AvgMS != 200 {
			t.Errorf("PageStats = %+v; want one day with 3 views, 2 visitors and 200 ms", rows)
		}
	})
}
```

<!-- doctest: output any -->
```console
$ go test ./...
ok  	example.com/shop	0.394s
?   	example.com/shop/cmd/dynaflux-runtime	[no test files]
?   	example.com/shop/internal/flux	[no test files]
```

- A fluxtest store updates every view before an append returns, so the query sees the
  events at once. A deployed view lags by about a second.
- `synctest.Test` gives the test a clock of its own that starts at 2000-01-01 00:00 UTC
  and moves only when every goroutine of the test waits, so `time.Sleep(time.Hour)` returns
  at once. The three events land on one day however fast the test runs.
- `AvgMS` is a pointer because `avg` is `Nullable(Float64)`: it is `NULL` when no event
  counts.

## 7. Run the store on your machine

`dynaflux dev` runs the store on DynamoDB Local, so that your application, tests and the
`dynaflux` commands can use it. In a terminal of its own, start DynamoDB Local, and leave it
running:

<!-- doctest: skip -->
```sh
docker run --rm -p 127.0.0.1:8000:8000 -e DDB_LOCAL_TELEMETRY=0 amazon/dynamodb-local:3.3.2 -jar DynamoDBLocal.jar -sharedDb -inMemory -disableTelemetry
```

The first run downloads the image, about 250 MB. If port 8000 is taken, publish another,
such as `-p 127.0.0.1:8001:8000`, and give `dev` and each command
`-endpoint http://127.0.0.1:8001`; `dev.env` follows it.

In a second terminal, in the project directory, start `dev`, and leave it running too:

<!-- doctest: integration -->
<!-- doctest: background -->
<!-- doctest: output any -->
```console
$ go tool dynaflux dev > dev.env
store shop  prefix (none)  endpoint http://127.0.0.1:8000  region us-east-1
created table shop-stream-page_views
created table shop-meta
created table shop-view-page_daily-v1
registered catalog 6256bc167b3a
caught up
page_daily@v1: filling from the stream table, split time
page_daily@v1: live
```

`dev` creates the store's tables, registers what `generate` compiled, and fills the view,
which takes about 7 seconds. Wait for `page_daily@v1: live`: until then, queries over the
view fail with `view version not ready`. Then `dev` projects new events into the view about
once a second, as the projector does on AWS. Its standard output, now in `dev.env`, is the
environment that points an application at DynamoDB Local. Back in the terminal where you
ran steps 3 to 6:

<!-- doctest: integration -->
```console
$ cat dev.env
AWS_ENDPOINT_URL_DYNAMODB=http://127.0.0.1:8000
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=dynafluxlocal
AWS_SECRET_ACCESS_KEY=fake
DYNAFLUX_PREFIX=
```

- `dev` runs what `generate` wrote when it started. Restart it after every `generate`; it
  refuses to start while `dynaflux.lock` is out of date.
- `go tool dynaflux dev -reset` deletes the store's tables first, to start over with no
  events.

## 8. Load and read events

`import` loads events from a file of JSON Lines, one event per line, with each column and
`event_time`. Create `events.jsonl`:

<!-- doctest: file events.jsonl -->
```json
{"user_id":"u1","page":"/home","duration_ms":800,"event_time":"2026-10-01T09:00:00Z"}
{"user_id":"u2","page":"/home","duration_ms":200,"event_time":"2026-10-01T09:05:00Z"}
{"user_id":"u1","page":"/pricing","duration_ms":4000,"event_time":"2026-10-01T09:06:00Z"}
```

Every command that reads or writes a store, except `backfill`, takes `-endpoint` to use
DynamoDB Local instead of AWS; `dev` fills versions itself. `import` asks before it writes;
`-yes` answers for it. The `store` line, like every note of the commands, goes to standard
error, after the result:

<!-- doctest: integration -->
```console
$ go tool dynaflux import -endpoint http://127.0.0.1:8000 -file events.jsonl -yes page_views
3 events: 3 stored, 0 already present, 0 conflicting
store shop  prefix (none)  endpoint http://127.0.0.1:8000  region us-east-1
plan: import 3 events of events.jsonl into page_views in us-east-1, at most 500 a second; an import can't be undone
```

Run it once: `page_views` has no `DEDUP BY`, so a second import stores the events again.
To start over, restart `dev` with `-reset`.

`dev` projects the events within a second or so. Run the query; if it prints no rows, run
it again:

<!-- doctest: integration -->
```console
$ go tool dynaflux query -endpoint http://127.0.0.1:8000 -param page=/home -param from=2026-10-01 -param to=2026-10-01 PageStats
day                   views  visitors  avg_ms
2026-10-01T00:00:00Z  2      2         500
store shop  prefix (none)  endpoint http://127.0.0.1:8000  region us-east-1
```

`inspect` reads a stream's events by their key, or a view's rows, as they are stored. Each
event has an ID, the time it arrived (`ingest_time`) and the region that stored it:

<!-- doctest: integration -->
<!-- doctest: output normalized -->
```console
$ go tool dynaflux inspect -endpoint http://127.0.0.1:8000 -key user_id=u1 page_views
event_id                    event_time                   ingest_time                  origin_region  user_id  page      duration_ms
01M6291QRT9XPNTXR7TVGQQJHY  2026-10-01T09:00:00.000000Z  2026-10-28T22:10:49.242447Z  us-east-1      u1       /home     800
01M6291QRYXY61GY93Z5C28BKM  2026-10-01T09:06:00.000000Z  2026-10-28T22:10:49.246655Z  us-east-1      u1       /pricing  4000
store shop  prefix (none)  endpoint http://127.0.0.1:8000  region us-east-1
note: events that arrived before 2026-10-26T22:10:51Z are past LOG RETENTION 2 DAY; inspect leaves them out, and DynamoDB Local has no archive
```

`status` shows each view version's state, and what needs an action:

<!-- doctest: integration -->
<!-- doctest: output normalized -->
```console
$ go tool dynaflux status -endpoint http://127.0.0.1:8000
version        us-east-1
page_daily@v1  live

us-east-1: catalog 6256bc167b3a (dynaflux v1.0.0), registered 2026-10-28T22:10:22Z
  mappings, metrics, gaps and the jobs' executions aren't available locally
store shop  prefix (none)  endpoint http://127.0.0.1:8000  region us-east-1
```

## 9. Run your application against dev

This program appends a page view and queries its page's stats. Create
`cmd/demo/main.go`:

<!-- doctest: file cmd/demo/main.go -->
```go
// Command demo appends a page view and reads its page's stats.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/lucindo/dynaflux"

	"example.com/shop/internal/flux"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	store, err := dynaflux.Open(dynaflux.Config{AWS: cfg, Prefix: os.Getenv("DYNAFLUX_PREFIX")})
	if err != nil {
		log.Fatal(err)
	}
	client := flux.New(store)

	view := flux.PageView{UserID: "u3", Page: "/docs", DurationMS: 1500, EventTime: time.Now()}
	tok, err := client.AppendPageView(ctx, view, dynaflux.Waitable())
	if err != nil {
		log.Fatal(err)
	}
	day := flux.PageStatsParams{Page: view.Page, From: view.EventTime, To: view.EventTime}
	rows, err := client.PageStats(ctx, day, dynaflux.WaitFor(tok, 5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		avg := "none"
		if row.AvgMS != nil {
			avg = fmt.Sprintf("%.0f ms", *row.AvgMS)
		}
		fmt.Printf("%s %s: views %d, visitors %d, average %s\n",
			row.Day.Format(time.DateOnly), view.Page, row.Views, row.Visitors, avg)
	}
}
```

- `dynaflux.Open` takes your AWS configuration and the prefix of the store's resource
  names. Each environment has its own prefix; `dev.env` sets `DYNAFLUX_PREFIX` to the
  local one, which is empty here.
- The view is updated about a second after the append. `Waitable` makes the append return
  a token, and `WaitFor` makes the query wait until the view holds that event, here for at
  most 5 seconds.
- `page_views` has no `DEDUP BY`, so a retried append stores the event again.

Run it with `dev`'s environment, in a subshell, as one line:

<!-- doctest: integration -->
<!-- doctest: output any -->
```console
$ (set -a; . ./dev.env; exec go run ./cmd/demo)
2026-10-28 /docs: views 1, visitors 1, average 1500 ms
```

The parentheses keep `dev.env` out of your shell. Never load it there: its fake
credentials and its DynamoDB endpoint would capture every later AWS command in that shell,
including the deploy in step 11. And without that environment, the program uses your
default AWS credentials and region, and appends to a store deployed there, if there is one.

For production settings, such as retries and timeouts, see
[application.md §2](application.md#2-opening-a-store).

## 10. Change a view

Add the longest time on a page to the view, and to the query. Replace
`dynaflux/views/page_daily.sql` with:

<!-- doctest: file dynaflux/views/page_daily.sql -->
```sql
-- Views and visitors per page and day.
CREATE MATERIALIZED VIEW page_daily
ORDER BY (page, day)
PRIMARY KEY (page)
AS SELECT
    page,
    toStartOfDay(event_time) AS day,
    count()                  AS views,
    uniqExact(user_id)       AS visitors,
    avg(duration_ms)         AS avg_ms,
    max(duration_ms)         AS max_ms
FROM page_views
GROUP BY page, day;
```

And `dynaflux/queries/pages.sql` with:

<!-- doctest: file dynaflux/queries/pages.sql -->
```sql
-- name: PageStats :many
SELECT day, views, visitors, avg_ms, max_ms
FROM page_daily
WHERE page = {page:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;
```

The rows of `page_daily` now hold more, so `generate` makes a new version of it,
`page_daily@v2`, and keeps `page_daily@v1` until you retire it:

```console
$ go tool dynaflux generate
page_daily@v1: retained
page_daily@v2: new
```

Restart `dev`: stop it with Ctrl-C in its terminal, and start it again.

<!-- doctest: integration -->
<!-- doctest: background -->
<!-- doctest: output any -->
```console
$ go tool dynaflux dev > dev.env
store shop  prefix (none)  endpoint http://127.0.0.1:8000  region us-east-1
created table shop-view-page_daily-v2
registered catalog f81efe672eb9
page_views: 4 records
caught up
page_daily@v2: filling from the stream table, split time
page_daily@v2: live
```

`dev` fills `page_daily@v2` from the events in the stream table, which takes about 7
seconds. Until it prints `page_daily@v2: live`, the query fails with
`view version not ready`.

<!-- doctest: integration -->
```console
$ go tool dynaflux query -endpoint http://127.0.0.1:8000 -param page=/home -param from=2026-10-01 -param to=2026-10-01 PageStats
day                   views  visitors  avg_ms  max_ms
2026-10-01T00:00:00Z  2      2         500     800
store shop  prefix (none)  endpoint http://127.0.0.1:8000  region us-east-1
```

`status` now points at the old version:

<!-- doctest: integration -->
<!-- doctest: output normalized -->
```console
$ go tool dynaflux status -endpoint http://127.0.0.1:8000
version        us-east-1
page_daily@v1  live
page_daily@v2  live

us-east-1: catalog f81efe672eb9 (dynaflux v1.0.0), registered 2026-10-28T22:11:37Z
  mappings, metrics, gaps and the jobs' executions aren't available locally
store shop  prefix (none)  endpoint http://127.0.0.1:8000  region us-east-1
us-east-1: warning DF7220: page_daily@v1 is no longer current, and still live: projectors still update its table
  hint: retire it with dynaflux retire page_daily@v1 -endpoint http://127.0.0.1:8000
```

Retire it. `-yes` answers its question, as for `import`. Then generate again: it prints
nothing, and drops the table of `page_daily@v1` from `regional.yaml`.

<!-- doctest: integration -->
```console
$ go tool dynaflux retire -endpoint http://127.0.0.1:8000 -yes page_daily@v1
page_daily@v1:
  us-east-1      retire   live
  dynaflux.lock  retire   retained
us-east-1: page_daily@v1 retired

Checked prefix (none) in us-east-1.
dynaflux.lock: page_daily@v1 retired
Next: run dynaflux generate, then restart dynaflux dev -endpoint http://127.0.0.1:8000, which registers the catalog that retires page_daily@v1.
Before you deploy another environment with this dynaflux.lock, run dynaflux retire page_daily@v1 -prefix <its prefix> there, with its credentials.
store shop  prefix (none)  endpoint http://127.0.0.1:8000  region us-east-1
retire can't see application builds: one older than the deployed catalog may still query page_daily@v1, and gets ErrViewNotReady once it is retired. Deploy the application first.
$ go tool dynaflux generate
```

The note about other environments concerns those where `page_daily@v1` was deployed. The
AWS store of the next step is new, so it starts at `page_daily@v2`.

Restart `dev` once more, as `retire` says (`-endpoint http://127.0.0.1:8000` is `dev`'s
default). It registers the catalog in which `page_daily@v1` is retired. Then stop it and
DynamoDB Local with Ctrl-C: DynamoDB Local keeps nothing once it stops.

<!-- doctest: integration -->
<!-- doctest: background -->
<!-- doctest: output any -->
```console
$ go tool dynaflux dev > dev.env
store shop  prefix (none)  endpoint http://127.0.0.1:8000  region us-east-1
registered catalog ca49b8b34f3b
page_views: 4 records
caught up
```

## 11. Deploy to two regions

Open a new terminal in the project directory, or check that no `dynafluxlocal` value shows
in your shell's environment:

<!-- doctest: skip -->
```sh
env | grep AWS_
```

> **Two time limits.** Run `backfill -all` below before the store's first event, and
> within 23 hours of deploying `global.yaml`. Then each region's archive of the stream is
> complete from the start, and later view versions can fill from the whole archive. If you
> miss either, nothing breaks, but later view versions fill only from the events of the
> last `LOG RETENTION`, 2 days here, instead of from the archive. For a real store, tear it
> down and deploy it again.

Build the Lambda binary from the tree that `generate` wrote:

<!-- doctest: skip -->
```sh
CGO_ENABLED=0 GOFLAGS=-tags=lambda.norpc sam build -t infra/dynaflux/regional.yaml
```

The settings apply to this command only. Exported, `GOFLAGS` would build everything after it
with the tag too.

Deploy `global.yaml` in the home region. It holds the stream table, a DynamoDB Global Table
with a replica in each region. `--resolve-s3` lets SAM upload the template to a bucket of
its own:

<!-- doctest: skip -->
```sh
sam deploy -t infra/dynaflux/global.yaml --stack-name shop-global \
  --region us-east-1 --resolve-s3
```

Set its stack policy, once, so that no later deploy can replace the stream table and
lose its events:

<!-- doctest: skip -->
```sh
aws cloudformation set-stack-policy --stack-name shop-global --region us-east-1 \
  --stack-policy-body file://infra/dynaflux/global.stack-policy.json
```

Deploy `regional.yaml` in each region: the view and meta tables, the functions, the
archive, the alarms and a dashboard. `sam deploy` without `-t` deploys what `sam build`
built, so both regions get the same binary:

<!-- doctest: skip -->
```sh
for region in us-east-1 us-west-2; do
  sam deploy --stack-name shop-regional --region "$region" \
    --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --resolve-s3
done
```

Right away, make the view live in both regions. Until a backfill makes a view version
live, queries over it return `ErrViewNotReady`. On a store with no events yet, `backfill`
also records where each region's archive of the stream starts. `-watch` follows the jobs
until every version is live, which takes about two minutes:

<!-- doctest: skip -->
```sh
go tool dynaflux backfill -all -yes -watch
```

Then check the store. `status` reads both regions, and shows `page_daily@v2` as `live` in
each:

<!-- doctest: skip -->
```sh
go tool dynaflux status
```

## 12. Use the deployed store

Without `-endpoint`, the commands use your AWS credentials. Import the events into
`us-east-1`, once, as in step 8:

<!-- doctest: skip -->
```sh
go tool dynaflux import -region us-east-1 -file events.jsonl -yes page_views
```

DynamoDB copies them to `us-west-2`, where that region's projector applies them to its own
copy of the view. Query that region:

<!-- doctest: skip -->
```sh
go tool dynaflux query -region us-west-2 -param page=/home -param from=2026-10-01 -param to=2026-10-01 PageStats
```

The output looks like this:

<!-- doctest: skip -->
```console
$ go tool dynaflux query -region us-west-2 -param page=/home -param from=2026-10-01 -param to=2026-10-01 PageStats
day                   views  visitors  avg_ms  max_ms
2026-10-01T00:00:00Z  2      2         500     800
store shop  prefix (none)  account 123456789012  region us-west-2
```

If it prints no rows, wait a few seconds and run it again: replication and the projector
each take about a second. `go tool dynaflux status -region us-west-2` shows the projector's
lag.

Run the program of step 9 against `us-east-1`. Your credentials are enough for the trial;
an application in production gets a policy of its own
([infra.md §9.9](../spec/infra.md#99-applications), and the tested policy of the
[example's README](../../examples/analytics/README.md#permissions)).

<!-- doctest: skip -->
```sh
AWS_REGION=us-east-1 go run ./cmd/demo
```

## 13. Tear down

The stacks keep what holds events when they are deleted: the stream table with its
replicas, each region's bucket, and each region's Glue database with its table optimizers.
The blocks below delete the stacks, then all of those, in 10 to 20 minutes. Run them in
order, in one shell. [operations.md §9](operations.md#9-tearing-down-an-environment)
explains the same steps for any environment.

First define `empty_bucket`, which deletes every object version and delete marker of a
bucket. It stops at the first object that S3 can't delete, and prints why:

<!-- doctest: skip -->
<!-- doctest: same empty-bucket -->
```sh
# empty_bucket deletes every object version and delete marker of a bucket, 1,000 at a time.
empty_bucket() { # bucket region
  local kind objects errors
  for kind in Versions DeleteMarkers; do
    while [ "$(aws s3api list-object-versions --bucket "$1" --region "$2" --no-paginate \
      --query "length(${kind} || \`[]\`)" --output text)" != 0 ]; do
      objects=$(aws s3api list-object-versions --bucket "$1" --region "$2" --no-paginate \
        --query "{Objects: ${kind}[].{Key: Key, VersionId: VersionId}, Quiet: \`true\`}" --output json)
      errors=$(aws s3api delete-objects --bucket "$1" --region "$2" --delete "$objects" \
        --query Errors --output json)
      if [ "$errors" != null ] && [ "$errors" != "[]" ]; then
        echo "$errors" >&2
        return 1
      fi
    done
  done
}
```

1. Delete the stacks: the regional ones, then the global one.

<!-- doctest: skip -->
```sh
for region in us-west-2 us-east-1; do
  aws cloudformation delete-stack --stack-name shop-regional --region "$region"
  aws cloudformation wait stack-delete-complete --stack-name shop-regional --region "$region"
done
aws cloudformation delete-stack --stack-name shop-global --region us-east-1
aws cloudformation wait stack-delete-complete --stack-name shop-global --region us-east-1
```

2. Delete the stream table: turn off deletion protection on each replica, remove the
   `us-west-2` replica, then delete the table in `us-east-1`.

<!-- doctest: skip -->
<!-- doctest: same stream-table -->
```sh
table=shop-stream-page_views
for region in us-east-1 us-west-2; do
  aws dynamodb wait table-exists --table-name "$table" --region "$region"
  aws dynamodb update-table --table-name "$table" --region "$region" \
    --no-deletion-protection-enabled >/dev/null
done
aws dynamodb wait table-exists --table-name "$table" --region us-east-1
aws dynamodb update-table --table-name "$table" --region us-east-1 \
  --replica-updates "Delete={RegionName=us-west-2}" >/dev/null
aws dynamodb wait table-not-exists --table-name "$table" --region us-west-2
aws dynamodb wait table-exists --table-name "$table" --region us-east-1
aws dynamodb delete-table --table-name "$table" --region us-east-1 >/dev/null
aws dynamodb wait table-not-exists --table-name "$table" --region us-east-1
```

3. In each region, delete the archive table's optimizers, then the Glue database, which
   takes its tables with it, then empty the bucket and delete it.

<!-- doctest: skip -->
<!-- doctest: same archive -->
```sh
account="$(aws sts get-caller-identity --query Account --output text)"
for region in us-east-1 us-west-2; do
  for type in compaction retention; do
    aws glue delete-table-optimizer --catalog-id "$account" --database-name shop \
      --table-name page_views --type "$type" --region "$region"
  done
  aws glue delete-database --name shop --region "$region"
  empty_bucket "shop-$account-$region-an" "$region"
  aws s3api delete-bucket --bucket "shop-$account-$region-an" --region "$region"
done
```

In a region where SAM's deployment stack didn't exist before the trial (step 2), delete it
and its bucket too. Set `region` to that region, and run this in the same shell, which
still has `empty_bucket`:

<!-- doctest: skip -->
```sh
region=us-east-1
bucket=$(aws cloudformation describe-stack-resource --stack-name aws-sam-cli-managed-default \
  --logical-resource-id SamCliSourceBucket --region "$region" \
  --query StackResourceDetail.PhysicalResourceId --output text)
empty_bucket "$bucket" "$region"
aws cloudformation delete-stack --stack-name aws-sam-cli-managed-default --region "$region"
aws cloudformation wait stack-delete-complete --stack-name aws-sam-cli-managed-default --region "$region"
if aws s3api head-bucket --bucket "$bucket" --region "$region" 2>/dev/null; then
  aws s3api delete-bucket --bucket "$bucket" --region "$region"
fi
```

Check that nothing is left. Each command prints no names when everything is gone:

<!-- doctest: skip -->
<!-- doctest: same leftovers -->
```sh
for region in us-east-1 us-west-2; do
  aws dynamodb list-tables --region "$region" \
    --query "TableNames[?starts_with(@, 'shop-')]" --output text
  aws glue get-databases --region "$region" \
    --query "DatabaseList[?Name=='shop'].Name" --output text
  aws cloudformation list-stacks --region "$region" \
    --query "StackSummaries[?starts_with(StackName, 'shop-') && StackStatus!='DELETE_COMPLETE'].StackName" \
    --output text
done
aws s3api list-buckets --query "Buckets[?starts_with(Name, 'shop-')].Name" --output text
```

Glue keeps the logs of the table optimizers in two log groups per region,
`/aws-glue/iceberg-compaction/logs` and `/aws-glue/iceberg-retention/logs`, which every
table optimizer of the account shares. If nothing else in the account uses table
optimizers, delete them with `aws logs delete-log-group`.

## 14. Next

- [concepts.md](concepts.md): how the parts of a store work together, across regions.
- [application.md](application.md): the Go client in an application, its errors and its
  tests.
- [dialect.md](dialect.md): recipes for the SQL of streams, views and queries.
- [operations.md §3](operations.md#3-change-procedures): how to deploy each kind of change.
- [cost.md](cost.md): what a store costs, and how to read `explain`.
- [examples/analytics](../../examples/analytics/README.md): a larger store, with
  deduplicated appends, a `SYNC` view for read-your-writes, and a query exposed over HTTP.

The references: [sql.md](../spec/sql.md), the SQL dialect with every diagnostic code and
its fix; [infra.md](../spec/infra.md), what the templates create, their alarms, and IAM;
and [http.md](../spec/http.md), queries over HTTP for clients in other languages.

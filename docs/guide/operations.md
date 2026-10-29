<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Operating a store

This guide is for the people who run a deployed store. It covers how to point the commands
at an environment, read `status`, make each kind of change in the right order, fill and
repair view versions, answer alarms, read the archive, and tear an environment down.
[lifecycle.md](../spec/lifecycle.md) holds the rules that the steps follow, and
[infra.md](../spec/infra.md) describes the templates.

The commands use `dynaflux`; if you added it as a tool, run `go tool dynaflux` instead.
Flags take one dash, such as `-all`, and `dynaflux help <command>` describes each command.

The examples use the store that `dynaflux init` writes, named `shop`, in `us-east-1` and
`us-west-2`, with no prefix:

```console
$ go mod init example.com/shop
go: creating new go.mod: module example.com/shop
$ dynaflux init -store shop -region us-east-1 -region us-west-2
wrote dynaflux.yaml
wrote dynaflux/queries/pages.sql
wrote dynaflux/streams/page_views.sql
wrote dynaflux/views/page_daily.sql

Next:
  go get github.com/lucindo/dynaflux  add the Dynaflux library to go.mod
  dynaflux generate                   write the Go client package, the runtime's main.go and the templates
  go mod tidy                         add the modules that the generated code imports to go.mod
  dynaflux dev                        run the store on DynamoDB Local
$ dynaflux generate
page_daily@v1: new
```

Its home region, where `global.yaml` is deployed, is `us-east-1`, and its stacks are named
`shop-global`, `shop-regional` and `shop-query`. Stack names are yours to choose: Dynaflux
finds resources by name, not by stack.

## Contents

1. [Environments and access](#1-environments-and-access)
2. [Reading status](#2-reading-status)
3. [Change procedures](#3-change-procedures)
4. [Backfill](#4-backfill)
5. [Alarms and status findings](#5-alarms-and-status-findings)
6. [Broken versions, gaps and rebuilds](#6-broken-versions-gaps-and-rebuilds)
7. [Debugging with inspect and query](#7-debugging-with-inspect-and-query)
8. [The archive in Athena](#8-the-archive-in-athena)
9. [Tearing down an environment](#9-tearing-down-an-environment)

## 1. Environments and access

An environment is one deployed copy of the store, such as production or staging. Every
environment deploys the same templates, from one checkout and one `dynaflux.lock`. A prefix
keeps their resources apart: the meta table of `shop` is `shop-meta` in the environment
without a prefix, and `staging-shop-meta` in the one with the prefix `staging-`.

- **Deploying:** add `--parameter-overrides Prefix=staging-` to every `sam deploy` of the
  environment, and start its stack names with the prefix. A new environment is a first
  deploy: follow [infra.md §11](../spec/infra.md#11-deploying), stack policy included, and
  then run `backfill -all -yes` with its prefix.
- **Choosing the environment:** every command that reads or changes a deployed store takes
  `-prefix`, whose default is `prefix` in `dynaflux.yaml`, and works on one environment per
  run.
- **Choosing regions:** `status`, `diff`, `backfill` and `retire` work in every region of
  `regions` at once; repeat `-region` to name some of them. `inspect`, `query` and `import`
  work in one region: the AWS SDK's region, such as `AWS_REGION`, unless `-region` names
  one.
- **Credentials:** the commands use the AWS SDK's default credential chain, such as
  `AWS_PROFILE`. Each one prints, on stderr, the store, prefix, account and regions it
  works on, and a command that asks prints them before its question: read them before you
  answer.
- **Asking:** `backfill`, `retire` and `import` ask before they change anything that can't
  be undone. `-yes` answers for them; a script needs it, since they refuse to ask when
  standard input isn't a terminal.

For example, to read the staging environment in one region:

<!-- doctest: skip -->
```sh
dynaflux status -prefix staging- -region us-west-2
```

**Permissions.** [infra.md §9.8](../spec/infra.md#98-operators) lists the IAM actions
of each command. `status`, `diff`, `inspect` and `query` only read, so a monitor can run
them with read-only credentials. `backfill` and `retire` write meta items, `backfill` also
starts and stops the backfill state machine's executions, and `import` writes events.
`init`, `validate`, `explain`, `generate`, `dev` and `version` never call AWS. Give no one
else `s3:PutObject` on a store's buckets: a backfill rebuilds views from what they hold, so
whoever can write to them can change what a rebuilt view counts
([infra.md §5.2](../spec/infra.md#52-bucket)).

**Locally,** `dynaflux dev` runs the store on DynamoDB Local. `-endpoint
http://127.0.0.1:8000` points `status`, `diff`, `inspect`, `query`, `import` and `retire`
at it, with fake credentials and no AWS permission. `backfill` has no `-endpoint`: `dev`
fills every version itself.

## 2. Reading status

`dynaflux status` reads every region at once and shows what each one runs. On a store with
the streams `page_views` and `signups` and the views `page_counts`, `page_seen` and
`plan_counts`, right after a deploy of a new version of `plan_counts`, it prints:

<!-- doctest: skip -->
<!-- doctest: golden status.txtar -->
```console
$ dynaflux status
version         us-east-1         us-west-2
page_counts@v1  live              live
page_seen@v1    live              live
plan_counts@v1  live              live
plan_counts@v2  pending_backfill  pending_backfill

us-east-1: catalog 6a2c85d90092 (dynaflux v1.0.0), registered 2026-10-29T00:00:00Z
  stream page_views: lag 2s; mapping us-east-1-page_views-shop-projector to shop-projector enabled, last result OK; archive delivered through 2026-10-28T23:57:00Z, complete since 2026-10-26T01:00:00Z
  stream signups: lag 2s; mapping us-east-1-signups-shop-projector to shop-projector enabled, last result OK; archive delivered through 2026-10-28T23:57:00Z, complete since 2026-10-26T01:00:00Z
us-west-2: catalog 6a2c85d90092 (dynaflux v1.0.0), registered 2026-10-29T00:00:00Z
  stream page_views: lag 2s; mapping us-west-2-page_views-shop-projector to shop-projector enabled, last result OK; archive delivered through 2026-10-28T23:57:00Z, complete since 2026-10-26T01:00:00Z
  stream signups: lag 2s; mapping us-west-2-signups-shop-projector to shop-projector enabled, last result OK; archive delivered through 2026-10-28T23:57:00Z, complete since 2026-10-26T01:00:00Z
store shop  prefix (none)  account 123456789012  regions us-east-1, us-west-2
us-east-1: warning DF7202: plan_counts@v2 needs a backfill
  hint: dynaflux backfill plan_counts@v2 -region us-east-1
us-west-2: warning DF7202: plan_counts@v2 needs a backfill
  hint: dynaflux backfill plan_counts@v2 -region us-west-2
```

Read it from the top:

1. **The version table:** the state of every view version in every region.
   `pending_backfill` waits for a backfill, `activating` is being filled, `live` answers
   queries, `broken` stopped at an event it can't apply, and `retired` was removed. Queries
   of a version in any state but `live` return `ErrViewNotReady`
   ([lifecycle.md §3](../spec/lifecycle.md#3-view-version-states)).
2. **Each region:** the catalog its functions run, with the release that generated it.
   Then, for each stream: how far the projector is behind (`lag`), its event source
   mapping, how far Firehose has delivered the archive, and whether the region's archive of
   the stream is complete ([§4.4](#44-the-23-hour-window-of-a-new-stream)). A backfill job
   adds a line with its phase, its progress and why it waits, and a version filled from
   the stream table adds a note that it is partial: it misses the events older than the
   table's log window.
3. **On stderr,** the store, prefix, account and regions that `status` read, then the
   findings: each thing that needs an action, as `<region>: <severity> <code>: <message>`,
   with a `hint:` line that gives the command that takes the action. The hints repeat the
   `-prefix` and `-endpoint` you gave, so you can paste them.
   [§5.2](#52-status-findings) lists every code.

`-verbose` adds, for each shard of a stream, how far the projector got and where backfills
split it. With `-endpoint`, `status` reads DynamoDB Local, where mappings, metrics, gaps and
the jobs' executions aren't available.

**Exit status:** 0 when nothing needs an action or only warnings do, 1 when an error does,
and 2 for usage and AWS access problems, or when a region, or part of one, couldn't be read.
A part that couldn't be read is named on stderr, and the findings that could be read still
print.

**For a monitor,** `-json` prints one JSON document instead, with `"format": 1`. It holds
each region's versions, streams and jobs, and the findings, each with `regions`, `code`,
`severity`, `message` and `hint`. Run it on a schedule with read-only credentials, alert on
exit status 1 or 2, and print the errors:

<!-- doctest: skip -->
```sh
dynaflux status -json | jq -r '.findings[] | select(.severity == "error") | "\(.code) \(.regions | join(",")): \(.message)"'
```

## 3. Change procedures

Each procedure below is a list of steps in the order that
[lifecycle.md §2](../spec/lifecycle.md#2-deploying-changes) requires. Do every step in
every environment that deploys the same `dynaflux.lock`, with that environment's prefix.

### 3.1 Every change

1. Change the SQL files or `dynaflux.yaml`, run `dynaflux generate`, and commit what it
   wrote, `dynaflux.lock` included.
2. Run `dynaflux diff`. It compares the project with what each region runs, and lists the
   changes, then, on stderr, those that would lose data or need an action first. Exit
   status 1 means an error, such as a removed stream column: fix it before you deploy.
3. Build and deploy, as in [§3.2](#32-build-and-deploy): `global.yaml` first when it
   changed, then `regional.yaml` in every region.
4. Take the steps of the change, below.

### 3.2 Build and deploy

Build from the tree that `generate` wrote, so that the binary and the templates agree.
`generate` leaves unchanged files alone, so `git status` shows which templates changed.
Deploy `global.yaml`, which holds the stream tables and their replicas, only when it did.

<!-- doctest: skip -->
```sh
CGO_ENABLED=0 GOFLAGS=-tags=lambda.norpc sam build -t infra/dynaflux/regional.yaml

# When global.yaml changed, in the home region:
sam deploy -t infra/dynaflux/global.yaml --stack-name shop-global \
  --region us-east-1 --resolve-s3

# In every region of regions:
for region in us-east-1 us-west-2; do
  sam deploy --stack-name shop-regional --region "$region" \
    --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --resolve-s3
done
```

`sam deploy` without `-t` deploys the template that `sam build` wrote, so every region gets
the same binary. When `generate` wrote `infra/dynaflux/query.yaml`, deploy it with the
application, as in [§3.6](#36-exposing-a-query-over-http). For alarm notifications, add
`--parameter-overrides AlarmTopicArn=<topic ARN>`, with a topic in that region.

### 3.3 A new view version

A view gets a new version when its definition changes what it stores: its keys, filters or
aggregates. A change of `MODE`, of comments or of the order of `SELECT` items doesn't make
one ([sql.md §9.9](../spec/sql.md#99-view-versions)). For example, add the longest page
view to `page_daily`:

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

1. Run `generate`. The new definition gets a new version, and the old one stays deployed
   as `retained`:

   ```console
   $ dynaflux generate
   page_daily@v1: retained
   page_daily@v2: new
   ```

2. Run `dynaflux diff`. It shows `page_daily@v2: new`, and warns (DF7112) that its queries
   return `ErrViewNotReady` until a backfill makes it `live`.
3. Build, and deploy `regional.yaml` in every region ([§3.2](#32-build-and-deploy)). The
   new version's table now exists, and the version is `pending_backfill`.
4. Check that `dynaflux status` shows no projector lag, then run
   `dynaflux backfill -watch page_daily`. It fills, in every region at once, the version
   that `dynaflux.lock` makes current. [§4](#4-backfill) says how long it takes and what it
   reads.
5. When `status` shows `page_daily@v2` `live` in every region that the application reads
   from, deploy the application, and `query.yaml` when there is one. Until then, the
   deployed application and query function read `page_daily@v1`, which keeps running.
6. Retire `page_daily@v1` ([§3.9](#39-retiring-a-version)).

### 3.4 A new stream

A new stream's first day matters: `backfill` can record where its archive starts only
before its first event, and within 23 hours of the deploy that creates its table
([§4.4](#44-the-23-hour-window-of-a-new-stream)). Do these steps in one sitting.

1. Add the `CREATE STREAM` and its views, and run `generate` and `diff`.
2. Build, and deploy `global.yaml` in the home region: it creates the stream's table, with
   a replica in every region.
3. Deploy `regional.yaml` in every region: the stream's event source mapping, its archive
   table and its alarms.
4. Run `dynaflux backfill -all -yes`. In each region that archives the stream with
   `archive.events: all`, the default, it records the stream's origin. Then it fills the
   new views' versions. The stream has no events, so they go `live` in about two minutes,
   or about 30 minutes in a region that doesn't archive the stream.
5. Run `dynaflux status`: the new versions are `live`, and no finding DF7218 remains.
6. Deploy the application that writes to the stream.

### 3.5 A new column

A deployed stream only gains columns. A new column is `Nullable`, or has a `DEFAULT` (an
`Array` column needs one, such as `DEFAULT []`), so that the events stored before it have a
value. `diff` refuses to remove, rename or retype a column, to change an existing column's
`DEFAULT`, and to change `PRIMARY KEY`, `DEDUP BY` or `SHARDS` (DF7101–DF7105,
[sql.md §8.9](../spec/sql.md#89-changing-a-stream)). For example:

<!-- doctest: file dynaflux/streams/page_views.sql -->
```sql
-- Page views: one event per page a user opens.
CREATE STREAM page_views (
    user_id     String,
    page        String,
    duration_ms UInt32,
    referrer    Nullable(String)
)
PRIMARY KEY (user_id);
```

1. Run `generate`. It prints nothing, since no view version changed:

   ```console
   $ dynaflux generate
   ```

2. Run `dynaflux diff`. It shows `column added: referrer`.
3. Build, and deploy `regional.yaml` in every region. The deploy adds the column to the
   archive table before it updates the projector.
4. Deploy the application that writes the column. Not before: a projector of the older
   build ignores the column, and leaves it out of the events it archives.
5. A view that reads the new column is a new view version: follow
   [§3.3](#33-a-new-view-version).

### 3.6 Exposing a query over HTTP

The query function serves the queries annotated `-- expose: http` to clients that don't use
Go, through the front door that `http` in `dynaflux.yaml` chooses: by default a Lambda
function URL that takes requests signed with AWS SigV4 ([http.md](../spec/http.md)).

<!-- doctest: file dynaflux/queries/pages.sql -->
```sql
-- name: PageStats :many
-- expose: http
SELECT day, views, visitors, avg_ms
FROM page_daily
WHERE page = {page:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;
```

1. Run `generate`. It writes `infra/dynaflux/query.yaml`:

   ```console
   $ dynaflux generate
   ```

2. With `http.frontend: rest_api`, or with `http.domain`, prepare the account first, as
   [infra.md §11](../spec/infra.md#11-deploying) says.
3. Deploy `regional.yaml` when it changed, and finish the backfill of every view version
   that an exposed query reads: the query function answers `503` for a version that isn't
   `live`.
4. With the application, build and deploy `query.yaml` in every region:

<!-- doctest: skip -->
```sh
CGO_ENABLED=0 GOFLAGS=-tags=lambda.norpc sam build -t infra/dynaflux/query.yaml --build-dir .aws-sam/query
for region in us-east-1 us-west-2; do
  sam deploy -t .aws-sam/query/template.yaml --stack-name shop-query --region "$region" \
    --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --resolve-s3
done
```

5. Each region's stack output `QueryEndpoint` is its URL:

<!-- doctest: skip -->
```sh
aws cloudformation describe-stacks --stack-name shop-query --region us-east-1 \
  --query "Stacks[0].Outputs[?OutputKey=='QueryEndpoint'].OutputValue" --output text
```

Later changes:

- A change to an exposed query, or to a view it reads, deploys `query.yaml` in every region
  with the application, after `regional.yaml` and the backfill.
- A change of `http.frontend`, `http.auth` or `http.domain` deploys `query.yaml` in every
  region. Another front door has another `QueryEndpoint`. Another `auth` changes what the
  door asks of callers as soon as it deploys, so give them their new credentials first.
- Without any `-- expose: http`, `generate` deletes `query.yaml` and says so. Delete the
  `shop-query` stack in every region.

### 3.7 Adding a region

Add one region per change, with nothing else in the same deploy: CloudFormation adds one
replica of a stream table per stack update, and `generate` refuses more (DF6017).

<!-- doctest: file dynaflux.yaml -->
```yaml
store: shop
regions: [us-east-1, us-west-2, eu-west-1]
```

1. Add the region to `regions`, and run `generate`:

   ```console
   $ dynaflux generate
   ```

2. Build, and deploy `global.yaml` in the home region. DynamoDB copies every stream's items
   to the new replicas, which can take hours. Wait until the stack update finishes.
3. Deploy `regional.yaml` in every region, the new one included. The other regions'
   templates changed too: the catalog lists the new region, and the backfill function
   reads the age of its replicas.
4. Fill the new region's versions with `dynaflux backfill -all -yes -region eu-west-1`.
   They fill from the stream tables, so they hold only the events of each stream's
   `LOG RETENTION`, and `status` notes that they are partial. The new region's archive of
   each stream is never complete, since its tables held events when they were created.
5. Deploy `query.yaml` in every region, and in the new one after its backfill. Then send
   the application's traffic there.

### 3.8 Removing a region

Never remove the home region. Removing a region deletes its replica of every stream table,
with its data.

1. Move the application's traffic out of the region.
2. Delete the region's query and regional stacks:

<!-- doctest: skip -->
```sh
for stack in shop-query shop-regional; do
  aws cloudformation delete-stack --stack-name "$stack" --region eu-west-1
  aws cloudformation wait stack-delete-complete --stack-name "$stack" --region eu-west-1
done
```

3. Turn off deletion protection on the region's replicas. Run `generate` with
   `-unprotect-region`, and deploy `global.yaml`:

   ```console
   $ dynaflux generate -unprotect-region eu-west-1
   ```

   Without this step, `generate` refuses to remove the region in step 4 (DF6018).

4. Remove the region from `regions`, run `generate`, and deploy `global.yaml`. DynamoDB
   deletes the region's replicas.

<!-- doctest: file dynaflux.yaml -->
```yaml
store: shop
regions: [us-east-1, us-west-2]
```

```console
$ dynaflux generate
```

5. Deploy `regional.yaml` in every other region, since the templates changed, and
   `query.yaml` with the application.
6. The region's bucket, with its archive, and its Glue database stay. Delete them as in
   [§9](#9-tearing-down-an-environment) when you no longer need them.

### 3.9 Retiring a version

A version that `dynaflux.lock` no longer makes current stays deployed as `retained`: its
table remains and the projector keeps applying events to it. `status` reports such a
version that is still `live` (DF7220). Retire it once nothing reads it:

1. Deploy the application, and `query.yaml`, from a `generate` that makes another version
   current, such as `page_daily@v2`.
2. Run `dynaflux retire page_daily@v1`. It checks every region first: no deployed catalog
   makes the version current, the view's current version is `live`, and no backfill of the
   version runs. It lists each check that
   fails, with its fix (DF7301–DF7304), and then changes nothing. Otherwise it prints what
   it will do, warns that it can't see application builds older than the deployed catalog,
   and asks. Then it sets the version `retired` in each region: within 30 seconds the
   projectors stop applying it and its queries return `ErrViewNotReady`. Last, it marks the
   version `retired` in `dynaflux.lock`.
3. Run `generate`, commit `dynaflux.lock`, and deploy `regional.yaml` in every region.
   CloudFormation deletes the version's table.
4. In each other environment that deploys this `dynaflux.lock`, run
   `dynaflux retire page_daily@v1 -prefix <its prefix>` with its credentials, before you
   deploy there. A deploy retires the version without the checks; `diff` reports it as
   DF7115 where that is unsafe.

To retire every version of a view, remove its SQL, run `generate`, which keeps every
version as `retained`, and retire each one.

### 3.10 Importing past events

`dynaflux import` appends past events from a file of JSON Lines: one object per line, with a
key for each column and for `event_time`, in the forms that `-json` output uses. A missing
`Nullable` column is `NULL`, and a missing column with a `DEFAULT` takes it.

<!-- doctest: skip -->
```json
{"event_time":"2026-10-01T12:00:00Z","user_id":"u1","page":"/home","duration_ms":120}
{"event_time":"2026-10-01T12:05:00.250Z","user_id":"u2","page":"/home","duration_ms":80}
{"event_time":"2026-10-02T09:30:00Z","user_id":"u1","page":"/cart","duration_ms":300}
```

1. On a stream that has no events yet, run `dynaflux backfill -all -yes` first. An imported
   event ends the time in which `backfill` can record where the stream's archive starts, so
   `import` refuses such a stream until then (DF7406).
2. Check the file against the deployed stream, with
   `dynaflux import -check -region us-east-1 -file events.jsonl page_views`. It prints each
   problem as `file:line`, or, when there is none,
   `events.jsonl: 3 events fit page_views in us-east-1`.
3. Import it into one region, and replication copies the events to the others:
   `dynaflux import -region us-east-1 -file events.jsonl page_views`. It asks first, then
   prints `3 events: 3 stored, 0 already present, 0 conflicting`.

What to know:

- An import skips the stream's `ACCEPT` window. Each event's `ingest_time` is the time of
  the import: a view that groups by `ingest_time` counts the events then (DF7404 warns), and
  `LOG RETENTION` counts from then.
- Projectors apply imported events to the view versions and archive them, as they do any
  event.
- Each event is its own write, at most `-rate` a second, 500 by default.
- `import` refuses to run on a machine whose clock is more than 60 seconds off DynamoDB's
  (DF7405).
- Run again on a stream with `DEDUP BY`, `import` reports the events already present, and
  those stored before with other values (DF7403). A stream without `DEDUP BY` gets every
  event again: `-skip <n>` resumes after line `n`, which `import` prints as it goes.

A file whose name ends in `.gz` is read with gzip, and `-file -` reads standard input, so
this imports an S3 object:

<!-- doctest: skip -->
```sh
aws s3 cp s3://bucket/key - | dynaflux import -region us-east-1 -file - page_views
```

### 3.11 Rolling back

A rollback, by CloudFormation or by a deploy of an older build, registers the older catalog
again. Before you roll a region back, run `dynaflux status` and check each version that is
`activating` or `live` there:

1. If the older build defines every such version, roll back.
2. Otherwise, don't: the older build can't apply that version, so its projector stops the
   version's whole stream in that region (DF7209). Fix forward instead, with a build that
   still defines the version.
3. If DF7209 appears anyway, deploy `regional.yaml` in that region from a build whose
   catalog defines the version. The message gives the time from which records are lost:
   DynamoDB keeps a change stream's records for 24 hours.

A rollback doesn't undo a retire: a version retired once stays retired. A rollback that
starts or stops archiving a stream records a hole in the region's archive, which is then
never complete again ([lifecycle.md §5.5](../spec/lifecycle.md#55-when-the-archive-is-complete)).

### 3.12 Recovering a lost stack

[infra.md §12](../spec/infra.md#12-changes-after-the-first-deploy) gives the details of each
case:

- **A first `global.yaml` deploy failed part-way.** The tables it created stay. Delete them,
  turning off deletion protection first, or adopt them into the stack with a change set that
  imports existing resources.
- **A first `regional.yaml` deploy failed after the bucket received objects.** The rollback
  ends in `ROLLBACK_FAILED`, since S3 won't delete a bucket that isn't empty. Delete the
  stack, keeping `Bucket`, then import the bucket into the next stack.
- **A regional stack was deleted.** Its bucket stays, with the archive: the only copy of
  events older than the log window. Never delete it to make way: import it when you create
  the new stack. The meta and view tables went with the stack, so run
  `dynaflux backfill -all -yes -region <region>` once the new stack is deployed. The
  versions fill from the stream tables, and are partial.
- **A stream table or a replica was re-created by hand.** Deploy that region's regional
  stack with a new `LookupGeneration` parameter. Don't re-create the regional stack for
  this: that deletes its meta and view tables.

## 4. Backfill

A backfill fills a view version in a region with the events that came before it, and then
makes it `live`. `dynaflux backfill` starts one job per region, in parallel; each region's
backfill state machine runs its job. A new job activates its version, which can't be
undone: the version never returns to `pending_backfill`, and its source is fixed for its
life. So `backfill` prints its plan and asks first; `-yes` answers for it. On a store with
the streams and views of [§2](#2-reading-status):

<!-- doctest: skip -->
<!-- doctest: golden backfill.txtar -->
```console
$ dynaflux backfill -all -yes
us-east-1:
  stream signups: origin recorded; the region's archive of it is complete from now on
  page_seen@v1    start    table source, no split; partial since about 2026-10-27
  plan_counts@v1  start    archive source, split by fences
us-west-2:
  page_counts@v1  start    table source, split by time; partial since about 2026-10-27
  page_seen@v1    start    table source, no split; partial since about 2026-10-27
  plan_counts@v1  start    table source, split by time; partial since about 2026-10-27
us-east-1: page_seen@v1: started in execution page_seen-v1-2fd4c9-a1
us-east-1: plan_counts@v1: started in execution plan_counts-v1-e428b1-a1
us-west-2: page_counts@v1: started in execution page_counts-v1-e6a06b-a1
us-west-2: page_seen@v1: started in execution page_seen-v1-2fd4c9-a1
us-west-2: plan_counts@v1: started in execution plan_counts-v1-e428b1-a1
store shop  prefix (none)  account 123456789012  regions us-east-1, us-west-2
A new job activates its version, which can't be undone: its source and split are fixed for its life.
```

In `us-east-1`, the stream `signups` was fresh, so `backfill` recorded its origin and fills
`plan_counts@v1` from the archive. The other versions fill from the stream tables.

`backfill <view>` fills the version that `dynaflux.lock` makes current, `<view>@v<N>` a given
one, and `-all` every version that isn't `live`, `broken` or `retired`, except those whose
job was cancelled.

### 4.1 Sources

A region fills a version from one of two sources:

- **Its archive,** when the region's archive of the stream is complete. It holds every event
  since the region began to archive the stream. The job splits the events with the
  projector by fences, the point each shard's projector had reached, so the fill is exact.
  It first waits until Firehose has delivered what it holds: minutes under steady writes,
  and 2 hours 30 minutes on a stream that gets no new records. Athena reads the archive at
  $5 per TB.
- **Its stream table,** otherwise. It holds only the events of the stream's
  `LOG RETENTION`, so the version is partial: `status` notes the date its history starts,
  and regions whose tables differ in age can answer one query differently.

A version that counts or sums, filled from the table, splits the events by time between the
backfill and the projector. The job then waits about 28 minutes before it reads the table.
The split is exact while every writer's clock is within 5 minutes of the true time and every
event lands in the region within 15 minutes. An event that lands later breaks the version,
with cause `split` ([§6](#6-broken-versions-gaps-and-rebuilds)). So start such a backfill
when `status` shows no projector lag, and not while a region is cut off from the others.

`-source table` or `-source archive` chooses for a new job. `backfill` refuses `archive`
when the archive isn't complete, and names the rule that failed (DF7503).

[lifecycle.md §5](../spec/lifecycle.md#5-backfill) gives the time and the cost: a billion
records whose events touch 200 million view rows take about 13 hours, and the job's writes
cost about $0.75 per million rows of up to 1 KB.

### 4.2 Watching, pacing and stopping

`-watch` follows the jobs until they end, with each job's phase, the records applied, the
rate and the time left:

<!-- doctest: skip -->
<!-- doctest: golden backfill.txtar -->
```console
$ dynaflux backfill -yes -watch -region us-east-1 page_seen
us-east-1:
  page_seen@v1  start    table source, no split; partial since about 2026-10-27
us-east-1: page_seen@v1: started in execution page_seen-v1-2fd4c9-a1
us-east-1  page_seen@v1  activate  0/0 applied
store shop  prefix (none)  account 123456789012  region us-east-1
A new job activates its version, which can't be undone: its source and split are fixed for its life.
us-east-1  page_seen@v1  apply  1000/3000 applied  200/s  ETA 10s
us-east-1  page_seen@v1  apply  2000/3000 applied  200/s  ETA 5s
us-east-1  page_seen@v1  live
```

- It exits with 0 when every version is `live`, and with 1 when one broke, its job failed,
  or a version was refused. Stopping it, such as with Ctrl-C, leaves the jobs running.
- `-watch -json` prints one JSON line per change of a job, for a script.
- `-rate <n>` limits each job to `n` records a second. Given on a running job, such as
  `dynaflux backfill -rate 200 page_daily`, it changes the rate at the job's next chunk.
- A job pauses by itself while the region's projector is more than 5 minutes behind, and
  continues once it is under 2.5 minutes.
- `-cancel` stops the jobs, and the Athena query a job runs on the archive. The version
  stays `activating`: the projector keeps applying its part, and its queries still return
  `ErrViewNotReady`. `-all` leaves a cancelled job alone; name its version to resume it.

### 4.3 Resuming

Run the same `backfill` command again. An interrupted job continues from the last chunk it
finished, and a job never runs twice at once. `status` reports an interrupted job (DF7216)
with the command that resumes it, and the alarm `{base}-backfill-failed` fires when an
execution fails after its retries.

A job that found the archive incomplete fails for good: its version stays `activating` and
can't go live (DF7219), and `backfill` refuses to resume it (DF7506). Rebuild the view
([§6](#6-broken-versions-gaps-and-rebuilds)): the new version reads the table.

### 4.4 The 23-hour window of a new stream

Nothing in the archive itself shows that it is complete. So a region trusts its archive of a
stream only from a recorded start, the *origin*, as long as nothing was lost since. `backfill`
records the origin when the stream is fresh in the region: its table is less than 23 hours
old and holds no event. That leaves a window:

- **Before the stream's first event,** since an event in the table ends it. Run
  `backfill -all` before the application writes, and before any `import`.
- **Within 23 hours of the `global.yaml` deploy** that created the stream's table.

`status`, `backfill -all` and `import` warn about a young stream without an origin (DF7218),
with the time the window closes:

<!-- doctest: skip -->
<!-- doctest: golden status.txtar -->
```console
us-east-1: warning DF7218: the table of stream signups is less than 23 hours old and has no origin in us-east-1, which archives it with events: all
  hint: run dynaflux backfill -all -region us-east-1 before 2026-10-29T22:00:00Z and before the stream's first event; otherwise the archive of it in us-east-1 is never complete
```

If you miss the window, nothing breaks. The region's archive of the stream is never
complete, so the later versions of its views fill from the table: only the last
`LOG RETENTION` of events.

Later events can also end an archive's completeness, for good: a deploy that starts or stops
archiving the stream in the region, or a gap in the projector's work. `status` names the
rule that failed ([lifecycle.md §5.5](../spec/lifecycle.md#55-when-the-archive-is-complete)).

## 5. Alarms and status findings

### 5.1 Alarms

`regional.yaml` creates these alarms in every region, and `query.yaml` the query alarms:
the certificate's only with `http.domain`. Each alarm's description starts with `critical:`
or `warning:` and says what to do. They notify the SNS topic of the `AlarmTopicArn`
parameter, when it is set, and the dashboard `{base}-{region}`, such as `shop-us-east-1`,
shows their states.

In the names, `{base}` is the prefix and the store, such as `shop`; `{projector}` is the
projector function, `{base}-projector`, or `{base}-projector-{stream}` with
`runtime.split: stream`; `{stream table}` is `{base}-stream-{stream}`; and
`{Firehose stream}` is `{base}-archive-{stream}`. A function's log is the log group
`/aws/lambda/<function>`: read the projector's with
`aws logs tail /aws/lambda/shop-projector --since 1h --region us-east-1`.

| Alarm | Severity | What it means | What to do |
|-------|----------|---------------|------------|
| `{projector}-lag` | warning | The projector is 15 minutes or more behind, so the views are stale. | Look for the alarms below on the projector and the tables, and the dashboard's errors and throttles. Don't start a backfill that splits by time until the lag is gone. |
| `{projector}-gap-risk` | critical | The projector is 6 hours or more behind. DynamoDB trims a change stream's records after 24 hours, and a record trimmed before the projector applies it is lost to the region's views and archive. | Fix the projector's errors or throttles now. If records were lost, `status` reports a gap (DF7203 or DF7204): rebuild the versions it names ([§6](#6-broken-versions-gaps-and-rebuilds)). |
| `{projector}-errors` | warning | The projector failed for 15 minutes, and the shards it reads wait. A deploy that changes the archive schema causes brief failures. | Read the projector's log. `status` reports a projector blocked on a version its build doesn't know (DF7209). |
| `{projector}-throttles` | warning | Lambda throttled the projector for 15 minutes. | Raise the account's concurrent executions quota in the region. |
| `{base}-projector-{stream}-discarded` | critical | The stream's event source mapping sent records to its on-failure destination, under `aws/lambda/` in the bucket, so the region's views miss them. | `status` reports the gap with its exact range (DF7203). Fix what made the projector fail, then rebuild each version it names. |
| `{base}-projector-{stream}-polling` | critical | The stream's mapping hasn't polled for 15 minutes: it is disabled, or the projector can't read the stream. Nothing applies the stream's events in the region. | `status` shows the mapping's state (DF7208). Deploy `regional.yaml` in the region again, and check the projector's role. Act within hours: records are lost after 24. |
| `{stream table}-write-throttles` | warning | DynamoDB throttled writes to the stream table for 15 minutes, so writers retry or fail. | Check the table's capacity on the dashboard. For a provisioned table, raise `write.max` of its `autoscaling` ([config.md](../spec/config.md#tables)). |
| `{base}-views-broken` | critical | A view version stopped at an event it can't apply. The stream's other views go on. | `status` shows the version, the event and the error (DF7201): see [§6](#6-broken-versions-gaps-and-rebuilds). |
| `{base}-rows-too-large` | critical | A view row passed DynamoDB's 400 KB item limit, so the projector can't write it and its shard waits. The projector's log names the view and the row. | The row records which shards wrote it, and it clears once the projector drops the older of these records, after `runtime.prune_window_seconds`. If it lasts, the row gets events from too many shards within that window: `explain` shows the row sizes, and a shorter window keeps rows smaller ([config.md](../spec/config.md#runtime)). |
| `{Firehose stream}-freshness` | warning | Firehose has held records of the stream for more than 15 minutes without writing them to the archive. Backfills from the archive wait for it. | Read Firehose's log group, `/aws/kinesisfirehose/{base}-archive`. `status` reports a stale archive (DF7210). |
| `{Firehose stream}-failed-rows` | critical | Firehose couldn't write rows to the archive table. They are under `errors/firehose/<stream>/` in the bucket: backfills read them, Athena queries don't. | Read Firehose's log group. An object there that doesn't hold events, such as the file list of a failed Iceberg commit, makes the region's archive incomplete for good: the next backfill that reads the archive fails on it. |
| `{base}-backfill-failed` | critical | A backfill execution failed after its retries, so its job stopped. | Read the log of the `{base}-backfill` function. `status` shows the job as interrupted (DF7216), and `dynaflux backfill` resumes it ([§4.3](#43-resuming)). |
| `{base}-query-errors` | warning | The query function crashed, timed out, or got an event from no door it serves; callers got the door's own error. | Read the query function's log. |
| `{base}-query-failures` | warning | The query function answered `500` or `504` for 15 minutes. | Its log gives each request's code and cause. |
| `{base}-query-throttles` | warning | Lambda throttled the query function for 15 minutes, so callers got errors. | Raise `http.max_concurrency`, or the account's concurrent executions quota. |
| `{base}-query-certificate` | warning | With `http.domain`, the domain's certificate expires in less than 45 days: ACM, which renews it from 60 days ahead, couldn't. | Check that its DNS validation record is still in the hosted zone. |
| `{base}-query-views-not-ready` | warning | With `http.domain`, a view that an exposed query reads hasn't been `live` in the region for 2 minutes, or the query function can't read the view states, so the domain sends no queries to the region. | Run `dynaflux status`. |

The dashboard also shows replication latency, throttles on the view and meta tables,
DynamoDB system errors and divergent duplicates, which have no alarm. A divergent duplicate
is one dedup key appended in two regions at once with different payloads: the regions may
then keep different payloads ([sql.md §8.5](../spec/sql.md#85-dedup-by)).

### 5.2 Status findings

| Code | What `status` found | What to do |
|------|---------------------|------------|
| DF7001 | The deployed catalog's format is newer than this CLI reads. | Upgrade `dynaflux` to the release that the hint names. |
| DF7201 | A version is broken, at the event the message names. | See [§6](#6-broken-versions-gaps-and-rebuilds); the hint has the Athena SQL that finds the event. |
| DF7202 | A version needs a backfill, or its job was cancelled. | Run the hint's `dynaflux backfill` command. |
| DF7203 | Lambda discarded records of a stream: the message gives the shard, the sequence range and the versions that miss them. | Rebuild each version it names ([§6](#6-broken-versions-gaps-and-rebuilds)). |
| DF7204 | The region's archive of a stream isn't complete, so a version activating from it can't go live; or a gap is suspected in the stream's views. | Rebuild each version it names, as the hint says. |
| DF7205 | A projector is 15 minutes or more behind. | Read its log, with the hint's command, as for `{projector}-lag`. |
| DF7206 | Another function reads the stream. A stream shard serves at most two readers. | Delete that mapping unless you need it; the hint has the command. |
| DF7208 | A stream's mapping isn't enabled, is missing, or its last result is a problem. | Deploy `regional.yaml` in the region again, and read why the projector fails. |
| DF7209 | Projectors fail every batch of a stream, on a version that their build doesn't define. | Deploy `regional.yaml` in the region from a build that defines the version, before the time in the message ([§3.11](#311-rolling-back)). |
| DF7210 | The archive is stale, or Firehose failed to write rows. | As for the Firehose alarms in [§5.1](#51-alarms). |
| DF7216 | A backfill is interrupted: its execution ended without finishing. | Resume it with the hint's command ([§4.3](#43-resuming)). |
| DF7218 | A stream table is less than 23 hours old and has no origin. | Run `dynaflux backfill -all` before the time in the hint and before the stream's first event ([§4.4](#44-the-23-hour-window-of-a-new-stream)). |
| DF7219 | A backfill job failed on an archive that wasn't complete; its version can't go live. | Rebuild the view, as the hint says ([§6](#6-broken-versions-gaps-and-rebuilds)). |
| DF7220 | A version that is no longer current is still `live`, and projectors still update its table. | Retire it once nothing reads it ([§3.9](#39-retiring-a-version)). |

[lifecycle.md §12](../spec/lifecycle.md#12-diagnostics) lists the codes of every operator
command.

## 6. Broken versions, gaps and rebuilds

**A broken version** stopped at one event. Its region applies nothing more to it, and its
queries return `ErrViewNotReady`; the stream and every other view keep going. `status`
reports it (DF7201) with the event, the error, and the Athena SQL that finds the event in
the archive:

<!-- doctest: skip -->
<!-- doctest: golden status.txtar -->
```console
us-east-1: error DF7201: page_counts@v1 broke at 2026-10-29T00:00:00Z: the record of unit shardId-0001 at sequence number 0000000000000000000000000000000000000007, event e1, failed to evaluate: division by zero
  hint: fix page_counts, then run dynaflux generate, deploy regional.yaml and run dynaflux backfill page_counts; the event in Athena: SELECT * FROM "shop"."page_views_dedup" WHERE "event_id" = 'e1'
```

- **An event failed to evaluate,** such as a division by zero
  ([sql.md §5.8](../spec/sql.md#58-evaluation-errors)). Evaluation is deterministic, so the
  same event breaks the version in every region. Change the view so that the event
  evaluates, then follow [§3.3](#33-a-new-view-version).
- **The cause is `split`:** an event landed after a time split's bound
  ([§4.1](#41-sources)). The definition is fine: rebuild it, below.
- **A retained version broke,** one that is no longer current: retire it
  ([§3.9](#39-retiring-a-version)).

Until the new version is `live`, the application's queries of the broken version fail.
`AllowPartial()`, or `-allow-partial` in `dynaflux query`, reads it anyway, without the
events since the break.

**A gap** is a run of stream records that a region's projector never applied. DynamoDB
trims a change stream's records after 24 hours, so a projector blocked longer loses them,
and so does a mapping that discards records. `status` reports a gap that Lambda recorded in
the bucket (DF7203), and a suspected one (DF7204): a record handled almost 24 hours old, an
hour of writes that no projector run followed, or a stream without a mapping. Either message
names the versions that may miss events. A gap also makes the region's archive of the stream
incomplete for good.

**Rebuild** each version that a gap or a `split` break affects. Its definition doesn't
change, so `generate -rebuild` gives it a new version number:

```console
$ dynaflux generate -rebuild page_daily
page_daily@v2: retained
page_daily@v3: new
```

Then deploy, backfill and retire as for any new version ([§3.3](#33-a-new-view-version)).
After a gap, the region's archive is incomplete, so the new version fills from the stream
table, which still holds the lost events while they are inside the stream's
`LOG RETENTION`. Rebuild before they leave it.

## 7. Debugging with inspect and query

`dynaflux query` runs a named query with the deployed catalog of one region, so it reads
what that region serves. Run it in each region to compare them:

<!-- doctest: skip -->
<!-- doctest: golden deployed.txtar -->
```console
$ dynaflux query -region us-east-1 -param page=/home -param from=2026-10-27 -param to=2026-10-28 PageStats
day                   views  visitors  avg_ms
2026-10-27T00:00:00Z  1      1         300
2026-10-28T00:00:00Z  2      2         150
store shop  prefix (none)  account 123456789012  region us-east-1
```

A query over a version that isn't `live` fails, and the hint names the backfill that fills
it, or `dev` with `-endpoint`. `-allow-partial` reads the version anyway; its rows may then
miss events. `-json` prints the rows as JSON Lines.

`dynaflux inspect` reads one stream or view directly. `-key` gives each key column as
`column=value`. For a view, give the `PRIMARY KEY` columns to read the partition, or every
`ORDER BY` column to read one row. `page_daily` reads the current version, and
`page_daily@v1` a given one that isn't retired:

<!-- doctest: skip -->
<!-- doctest: golden deployed.txtar -->
```console
$ dynaflux inspect -region us-east-1 -key page=/home page_daily@v1
page   day                   views  visitors  avg_ms
/home  2026-10-27T00:00:00Z  1      1         300
/home  2026-10-28T00:00:00Z  2      2         150
store shop  prefix (none)  account 123456789012  region us-east-1
```

`-raw` prints each item as DynamoDB stores it, with its record of the shards that wrote it,
and its size against the 400 KB limit.

For a stream, give every `PRIMARY KEY` column, such as
`dynaflux inspect -region us-east-1 -key user_id=u1 page_views`, and narrow the time with
`-from` and `-to`. It prints the events in `event_time` order. Events past the stream's
`LOG RETENTION` are only in the archive, so it also prints the Athena SQL that reads them:

<!-- doctest: skip -->
<!-- doctest: golden deployed.txtar -->
```console
note: events that arrived before 2026-10-27T00:00:00Z are past LOG RETENTION 2 DAY; the archive has them, in Athena:
  SELECT * FROM "shop"."page_views_dedup" WHERE "user_id" = 'u1' ORDER BY "event_time", "event_id"
```

Values in `-key` and `-param` are written as in `-json` output, without quotes: a `Date`
as `2026-10-26`, a time in RFC 3339, and an array as a ClickHouse literal, such as
`['/a','/b']`.

When a projector falls behind or fails, its log says why: run the `aws logs tail` command
that the hint of DF7205 gives.

## 8. The archive in Athena

The archive keeps every event in Apache Iceberg tables in each region's bucket, in a Glue
database that Athena queries. The names, for the store `shop`:

| What | Name |
|------|------|
| Glue database | `shop`: the prefix and the store, with each `-` replaced by `_`, such as `staging_shop` |
| Iceberg table | the stream's name, such as `page_views` |
| One row per event | the view `page_views_dedup` |
| Athena workgroup | `shop` |

Query the `_dedup` view, not the table: the archive is written at least once, so the table
can hold an event twice. Filter on `event_time`, which the tables are partitioned by day on,
to limit the days Athena reads, at $5 per TB:

<!-- doctest: skip -->
```sql
SELECT page, count(*) AS views
FROM "shop"."page_views_dedup"
WHERE "event_time" >= TIMESTAMP '2026-10-01 00:00:00'
  AND "event_time" < TIMESTAMP '2026-10-08 00:00:00'
GROUP BY page
ORDER BY views DESC;
```

- Each region archives its own copy, in the regions of `archive.regions`. Firehose writes
  every 5 minutes or so, so the newest events appear after a delay.
- Times are `timestamp` in UTC, and a `UInt64` is a `decimal(20,0)`.
  [infra.md §7.3](../spec/infra.md#73-tables-and-columns) gives every column type.
- The view reads a column added later as its `DEFAULT`, or `NULL`, in the events archived
  before it.
- Don't save queries in the workgroup: deleting the regional stack deletes the workgroup's
  saved queries and history.

## 9. Tearing down an environment

These steps delete every resource of the environment, its events included. They are
written for `shop` with no prefix; for another environment, put its prefix before `shop`,
and its Glue database is the prefix and the store with `_` for `-`. Stop the applications
that use it first.

1. Delete the stacks: the query and regional stacks in every region, then the global stack.
   Leave out `shop-query` if you never deployed one.

<!-- doctest: skip -->
```sh
for region in us-east-1 us-west-2; do
  for stack in shop-query shop-regional; do
    aws cloudformation delete-stack --stack-name "$stack" --region "$region"
    aws cloudformation wait stack-delete-complete --stack-name "$stack" --region "$region"
  done
done
aws cloudformation delete-stack --stack-name shop-global --region us-east-1
aws cloudformation wait stack-delete-complete --stack-name shop-global --region us-east-1
```

2. Delete each stream table, which the global stack keeps: turn off deletion protection on
   every replica, remove the replicas outside the home region, then delete the table.

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

3. In each region, delete the Glue database, after the compaction and retention optimizers
   of each of its tables, which the deploys created outside CloudFormation. Then empty the
   region's bucket of every object version and delete marker, and delete it. `empty_bucket`
   stops at the first object that S3 can't delete, and prints why. Run both blocks in one
   shell.

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

4. Check that nothing is left: each command prints no names.

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

The Glue optimizers' log groups and SAM's deployment bucket stay: other stacks in the
account share them.

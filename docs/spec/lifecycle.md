<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# View lifecycle and operations

This page is the normative description of what happens to a view version from deploy to
removal: its states, how a backfill fills it, when queries may read it, and how it is
repaired and retired. It also says what the operator commands `status`, `backfill` and
`retire` change, and lists the diagnostics of every operator command. Related: [sql.md §9.9](sql.md#99-view-versions)
(version numbers and `dynaflux.lock`), [encoding.md §8](encoding.md#8-meta-items) (the meta
items this page names), [infra.md](infra.md) (the resources and the permissions each
command needs), [config.md](config.md).

## Contents

1. [Terms](#1-terms)
2. [Deploying changes](#2-deploying-changes)
3. [View-version states](#3-view-version-states)
4. [Activation](#4-activation)
5. [Backfill](#5-backfill)
6. [Queries](#6-queries)
7. [Broken versions, gaps and rebuilds](#7-broken-versions-gaps-and-rebuilds)
8. [Retiring a version](#8-retiring-a-version)
9. [The deployed catalog](#9-the-deployed-catalog)
10. [Regions and environments](#10-regions-and-environments)
11. [Limits](#11-limits)
12. [Diagnostics](#12-diagnostics)

## 1. Terms

- **View version:** one definition of a view, such as `page_daily@v2`, with its own table
  in every region ([sql.md §9.9](sql.md#99-view-versions)).
- **Additive version:** a version with at least one additive aggregate (`count`, `sum`,
  `avg`, `quantile`, `quantileDD` or their `-If` forms) or a `uniqExact`. An event applied
  twice would count twice, and `uniqExact` counts a value only for the event whose marker
  holds it, so a backfill must split the events exactly with live projection.
- **Idempotent version:** any other version: its aggregates are all idempotent
  ([sql.md §7.5](sql.md#75-state-classes)) and none is `uniqExact`. Applying an event twice
  changes nothing.
- **Region:** each region keeps its own copy of every view, its own meta table and its own
  state for each version. Nothing on this page crosses regions unless it says so.
- **Delivery unit, fence, progress:** a delivery unit is one shard of a stream table's
  change stream in one region. `progress` is the last record of a unit that the projector
  applied and archived. A fence is the value of `progress` when a backfill split the unit
  for a version ([encoding.md §8.1](encoding.md#81-unit-item)).
- **Backfill job:** the work that fills one version in one region, run by the region's
  backfill state machine and recorded in a job item in the meta table.
- **`W`:** [`runtime.prune_window_seconds`](config.md#runtime), 960 seconds by default.

## 2. Deploying changes

Dynaflux never deploys. Your infrastructure tool deploys the templates that
`dynaflux generate` writes ([infra.md §11](infra.md#11-deploying)), and the commands below
move versions through their states. Commands take flags with one dash, such as `-all`.

**First deploy of a store:**

1. `dynaflux generate`, then build the runtime binary.
2. Deploy `global.yaml`, then `regional.yaml` in every region.
3. Run `dynaflux backfill -all -yes`. On a store that has no events yet, every version
   becomes `live` in about two minutes in a region that archives the stream with
   `archive.events: all` (the default), and in about 30 minutes in other regions
   ([§4.3](#43-fresh-streams)). In those regions it also records each stream's origin,
   from which the region's archive of the stream is complete
   ([§5.5](#55-when-the-archive-is-complete)). It can do that only before the stream's
   first event, and within 23 hours of the `global.yaml` deploy.
4. Deploy the application, and `query.yaml` when `generate` wrote one: it holds the query
   function of the queries annotated `-- expose: http`
   ([infra.md §6.6](infra.md#66-query-function)).

**A changed view (a new version):**

1. `dynaflux generate` and `dynaflux diff`. `diff` shows the new version and refuses changes
   that would lose data.
2. Deploy `regional.yaml` in every region.
3. `dynaflux backfill <view>`. It fills the version `dynaflux.lock` makes current, in every
   region at once. Start it while `dynaflux status` shows no projector lag
   ([§5.1](#51-how-events-are-split)).
4. When `dynaflux status` shows the version `live` in every region the application reads
   from, deploy the application and `query.yaml`. Until then the old build and the deployed
   query function keep querying the previous version, which keeps running.
5. Retire the previous version ([§8](#8-retiring-a-version)).

**A new stream:** deploy `global.yaml`, then `regional.yaml` in every region, then run
`dynaflux backfill -all`, then deploy the application that writes to it. Run `backfill`
within 23 hours of the `global.yaml` deploy, so that it records the stream's origin in each
region that archives it with `events: all` ([§5.5](#55-when-the-archive-is-complete)).
`status` warns until it has (DF7218).

**A new region:** see [§10](#10-regions-and-environments).

**Locally:** `dynaflux dev` runs the store on DynamoDB Local. It creates the tables that are
missing, registers the catalog, projects every stream, and fills each version that isn't
`live`, `broken` or `retired` with a job that reads the stream table, as `backfill` does on
AWS but with bounds for one host: an idempotent version is `live` in about a second, an
additive one about 7 seconds after its activation. It runs what `dynaflux generate` wrote
and refuses to start while `generate` would change `dynaflux.lock`. A projector about 6
seconds behind during an activation breaks the version ([§5.1](#51-how-events-are-split)).
It uses the first region of `regions`, applies `SYNC` views and writes wait receipts as on
AWS, and refuses an event appended in another region: give the application that region.

**Rolling back:** don't roll a regional stack back past a version that is `activating` or
`live` in that region. The older build can't apply the version, so the version's stream
stops in that region until a build that defines it is deployed again
([§4.1](#41-guards)). A rollback doesn't undo a retire: a version that a deploy retired
stays `retired` ([§8](#8-retiring-a-version)).

**`SYNC` views and older builds:** builds before M6 don't skip the versions that an
append's transaction applied, so they would apply those events a second time
([§4.4](#44-sync-views)). Deploy `regional.yaml` before an application that appends to a
stream with a `SYNC` view, as above, and don't roll a regional stack back to such a build
while that application runs.

## 3. View-version states

Each version has one state in each region, kept in that region's meta table
([encoding.md §8.2](encoding.md#82-view-state-item)).

| State | Meaning | Projector | Queries |
|-------|---------|-----------|---------|
| `pending_backfill` | Deployed, not activated. A version without a state item is in this state. | Applies every event to an idempotent version; nothing to an additive one. | `ErrViewNotReady` |
| `activating` | A backfill job is filling it. | Applies the events the backfill doesn't ([§5.1](#51-how-events-are-split)). | `ErrViewNotReady` |
| `live` | Filled. | As `activating`. | Read |
| `broken` | An event failed to evaluate, or the version missed events ([§7](#7-broken-versions-gaps-and-rebuilds)). It gets nothing more. | Nothing. Its fences no longer hold anything back. | `ErrViewNotReady` |
| `retired` | Removed by `dynaflux retire` or by a deploy ([§8](#8-retiring-a-version)). | Nothing. Its fences no longer hold anything back. | `ErrViewNotReady` |

Moves:

- Only a backfill job moves a version from `pending_backfill` to `activating`, and from
  `activating` to `live`. Activating also fixes how the version's events are split
  ([§5.1](#51-how-events-are-split)), and with it the source of every later job of the
  version.
- The projector or a backfill job moves a version to `broken`, from any state but
  `retired`. The first failure is kept.
- `dynaflux retire` moves a version to `retired`, from any state. So does a deploy whose
  catalog lists the version as retired, and, before it skips one of the version's events,
  the projector of such a build when it finds the version `activating` or `live`
  ([§8](#8-retiring-a-version)).
- There is no other move. A state never goes back: a cancelled backfill leaves the version
  `activating`, and a version that must be filled again becomes a new version
  ([§7.3](#73-rebuilding-a-version)). Nothing leaves `broken` except to `retired`, and
  nothing leaves `retired`.

Deploying the templates changes no other state.

## 4. Activation

A backfill job activates its version: it sets `activating`, then splits the region's events
between itself and live projection. Three guards keep older projector code from missing
events of a version it doesn't know.

### 4.1 Guards

1. **Deployed code first.** The job activates a version only when the region's deployed
   catalog ([§9](#9-the-deployed-catalog)) defines it. Otherwise it stops and says to deploy
   `regional.yaml` in that region. The deployed catalog is registered after every function
   of the region was updated.
2. **Older code fails, never skips.** A projector fails every batch of new or changed
   events when the region's meta table holds an `activating` or `live` version of the
   batch's stream that its own catalog neither defines nor lists as retired. This protects
   against a projector still running an older build after the activation, and against a
   rollback to an older build after a version went `activating`. The batch is retried until
   a build that defines the version runs again. The projector fails the same way on an
   `activating` or `live` additive version whose split its build doesn't apply, as a build
   from before the time split ([§5.1](#51-how-events-are-split)) would. A version that its
   catalog lists as retired it sets `retired` instead ([§8](#8-retiring-a-version)). `status` explains such a block and says by
   when: DynamoDB trims a change stream 24 hours after a record was written, and records
   trimmed while the stream is blocked are lost to every view and the archive. The fix is
   to deploy a build whose catalog defines the version; `retire` can't help, because the
   version is still current in the newer build.
3. **Every version is fenced when the source is an archive.** With an archive source, the
   job fences every delivery unit for the version, idempotent versions included. A batch of
   older code that is still running can then not record progress past events the version
   hasn't received.

The job waits 90 seconds after setting `activating` before it lists the delivery units:
a projector uses a version state it read for at most 30 seconds, and 60 seconds cover clock
differences and request latency. Every batch that starts after that wait either knows the
version is `activating` or fails under guard 2.

### 4.2 Why no other wait is needed

Older code that is still running when a version activates either finished its batches
before the fences, so their events are at or below the fences and in the archive the
backfill reads, or it reaches a fence it can't account for and fails, so its batch is
retried by code that applies the version. Neither case waits for the longest invocation to
end.

### 4.3 Fresh streams

A stream that has never held an event has nothing to backfill. When `dynaflux backfill`
finds a stream table empty and less than 23 hours old, and the region archives the stream
with `archive.events: all`, it records the stream's origin in the region
([§5.5](#55-when-the-archive-is-complete)), and the job uses the archive source. The job
makes its version `live` right after fencing, without reading the archive, when all of
these hold:

- every fence it wrote or found is zero: no delivery unit had recorded progress;
- the stream table is less than 23 hours old when the check ends, counting from the
  earliest creation time of all its replicas, and no replica was restored from a backup;
- a consistent scan of the region's replica, sent after the fences, finds no item.

Every event is kept for at least one day after it was written, so a table younger than
23 hours has lost none. An empty replica then proves that no event has ever passed through
the region's change stream, and every later event is applied live. This takes about two
minutes, most of it the 90-second wait. If the application wrote an event before the job's
check, the job reads the archive instead ([§5.3](#53-staging)), which takes minutes more.

A region whose archive of the stream isn't complete uses the table source. A scan of an
empty table finds nothing, but with the time split ([§5.1](#51-how-events-are-split)) the
job still waits about 28 minutes before that scan.

### 4.4 `SYNC` views

An append to a stream with a `SYNC` view applies, in its transaction, the view's versions
that it reads as `live`, and records them on the event. The projector of the append's
region skips exactly those versions for that event; other regions apply it as usual
([sql.md §9.7](sql.md#97-mode),
[ADR 0014](../decisions/0014-sync-views-in-the-append-transaction.md)).

- A backfill job makes a version `live` only after it applied its whole part. So an event
  that an append applied came after the job: above every fence and after every table
  scan. The job never meets it, and the archive needs no record of it.
- An append whose states are older than the version's `live` (they can be 30 seconds old),
  an append whose build defines the view as `ASYNC`, and an import leave the version to
  the projector, as for an `ASYNC` view.
- `retire` stops appends from applying a version within 30 seconds, as it stops projectors
  ([§8](#8-retiring-a-version)).
- Builds before M6 don't skip what appends applied: see [§2](#2-deploying-changes).

## 5. Backfill

### 5.1 How events are split

A backfill and live projection must together apply every event of the region exactly once
to an additive version. Dynaflux splits the events one of two ways, decided when the
version is activated and kept for its life:

| Split | Used when | Live projection applies | The backfill applies |
|-------|-----------|-------------------------|----------------------|
| **Fence** | The source is the region's archive | Events above the delivery unit's fence | Archived events at or below the fence, and every archived event of a unit too old to have one |
| **Time** | The source is the stream table | Events whose `ingest_time` is after the split time `B` | Events in the table whose `ingest_time` is at or before `B` |

- **Fence:** a fence is taken from the unit's `progress`, which covers only events already
  archived. The job reads the archive once it holds everything archived before the fences.
  This split is exact.
- **Time:** `B` is the activation time plus 6.5 minutes (the 90-second wait plus 5 minutes
  for writer clocks). The job reads the table 21 minutes after `B`: once every event with
  `ingest_time` up to `B` can have landed in the region, plus a minute for clock
  differences between hosts. This split is exact while two bounds hold:
  - every writer's clock is within 5 minutes of the true time;
  - every event lands in the region within 15 minutes of its `ingest_time`, replication
    from other regions included.

  The projector checks the second bound: an event at or before `B` that the projector
  handles 20 minutes or more after `B` opens the version's breaker (`broken`, cause
  `split`), because it may have landed after the backfill's read began. It does the same
  when two regions appended one `DEDUP BY` event with `ingest_time`s on both sides of `B`:
  the table keeps one copy and the change stream delivered the other, so the two sides
  would disagree. A broken bound is reported, never silent. Two consequences:
  - A projector about 20 minutes behind during the activation breaks the version although
    nothing was missed. Start a time-split backfill when `status` shows no lag, and don't
    start one while a region is cut off from the others.
  - A version can go `live` before the projector has handled a late event. Until it does,
    the version's rows may miss that event; once the projector has caught up, the version
    is exact or `broken`.
- Idempotent versions need no split: live projection applies every event, and the backfill
  applies every event in the table or, from the archive, every archived event at or below
  the fences of guard 3 ([§4.1](#41-guards)). Repeats change nothing.

### 5.2 Sources

| Source | Holds | Used when |
|--------|-------|-----------|
| The region's **archive** | Every event since the region's archive began | The region's archive of the stream is complete ([§5.5](#55-when-the-archive-is-complete)), unless `-source table` asks for the table. |
| The region's **stream table** replica | Events inside the stream's `LOG RETENTION` | Otherwise |

`-source table|archive` chooses for a new job; `backfill` refuses `archive` when the
archive isn't complete, and names the rule that failed (DF7503). With `-source table` on a
complete archive, it notes that the table covers only the stream's `LOG RETENTION`. A
version that is already `activating` keeps the source it was activated with, and `-source`
can't change it, nor that of a running job. A job resumed before its version was activated
takes the source that the region serves then.

A table source holds only the events inside the stream's log window. Events older than
that are gone from the table, and the version never counts them. `status` labels such a
version "partial since <date>", the time its history starts, and queries serve it like any
`live` version. Regions whose tables differ in age can then answer the same
query differently.

A region reads only its own archive and its own table. A region that doesn't archive a
stream with `events: all` fills its versions from its table, even when another region
archives every event.

### 5.3 Staging

The job stages the records it reads in the region's bucket under `backfill/`, sorted,
removes repeats, keeps the records its split gives it, and applies them in one fixed order,
a chunk at a time. The bucket deletes staged files 30 days after they were written, and
their older versions after one day. A job that finds a staged file gone stages again; this
is safe, only slower.

From the archive, the job first waits until Firehose has delivered every record it took
before the cut, the time the last fence was written. The data freshness of the stream's
Firehose stream, in CloudWatch, shows when, with a minute to spare for clocks. When
Firehose holds nothing, the job waits until 2 hours 30 minutes after the cut instead, which
covers Firehose's retries. CloudWatch forgets the metric of a stream that had no records
for 14 days, and without it the job can't tell that Firehose is empty: append one event, or
use `-source table`. `status` says why a job waits. Then the job
([ADR 0016](../decisions/0016-archive-export-with-athena.md)):

1. Reads the records that Firehose couldn't deliver, under `errors/firehose/<stream>/` in
   the bucket. An object there that doesn't hold Dynaflux records, such as the file list of
   an Iceberg commit that failed, makes the region's archive of the stream incomplete for
   good ([§5.5](#55-when-the-archive-is-complete)). The job fails, naming the object, and
   the version needs a rebuild from the table ([§7.3](#73-rebuilding-a-version)), since
   its split can't change.
2. Lists the days of the region's Iceberg table with Athena, and plans slices of days of up
   to about 25 GB of Parquet each.
3. Exports one slice at a time with Athena's `UNLOAD` into its staging prefix, and stages
   each slice as it succeeds. A slice that times out is halved, down to one hour. Each try
   of a slice writes to a new prefix, and the job reads only the files that the succeeded
   query lists.

Athena reads every column of the days it exports, at $5 per TB of Parquet: about $25 per
region and job for a year at 1,000 events per second. `status` shows the bytes a job
scanned. After its last read of the archive, and again before it makes the version `live`,
the job checks that the archive is still complete, and fails as above when it isn't.

### 5.4 Jobs

`dynaflux backfill <view>[@v<N>]` starts one job per region, in parallel. Without `@v<N>`
it fills the version `dynaflux.lock` makes current; a region whose deployed catalog doesn't
have it yet is refused with a hint to deploy `regional.yaml` there. `-all` starts a job for
every version that isn't `live`, `broken` or `retired` and whose job wasn't cancelled, in
every region; it prints "nothing to fill" when there is none. `-region` limits the regions.

First, `backfill` records every origin it can ([§5.5](#55-when-the-archive-is-complete)):
with `-all`, of each stream; otherwise, of the streams of the versions it names. It prints a
line for each, and with `-all` warns about a stream table less than 23 hours old that still
has none (DF7218), saying why: a deploy under way, the wait after a deploy's hole, or an
event already in the table. An origin only records a fact, so `backfill` doesn't ask first.
A read of the archive's history that fails doesn't stop the plan where the job doesn't need
it: `backfill` reports it and exits 2, and with `-source table` starts the job.

Before it changes any job, `backfill` prints what it will do in each region: start, resume
or leave a job, with its source and split. Starting a job can't be undone: the version
never returns to `pending_backfill`, and its source and split are fixed for its life. So
`backfill` asks before it starts a job; `-yes` answers for it, which a script needs.
Resuming, cancelling and changing the rate don't ask.

| Option | Effect |
|--------|--------|
| `-watch` | Follow the jobs: phase, records applied, rate, estimated time left, and why a job pauses or waits. It ends when every job has ended, with exit status 0 when all versions are `live` and 1 when any broke, its job failed, or a version was refused. It follows the jobs started elsewhere when one region refuses. Stopping the command doesn't stop the jobs, and exits with 1. A read that fails is reported in place and tried again. With `-json` it prints a JSON line with `"format": 1`, then one for each change of a job, with the fields of a job in `status -json`; the plan goes to stderr. |
| `-rate n` | Apply at most `n` records per second per region. On a running job it changes the rate at its next chunk. |
| `-cancel` | Stop the jobs, and the Athena query a job's read of the archive runs, as the cancelled job item names it: the listing of the partitions or a slice's export. The version stays `activating`: live projection keeps applying its part, and queries still return `ErrViewNotReady`. `-all` leaves a cancelled job alone; name the version to resume it. A version without a running job is refused (DF7504). |
| `-source table\|archive` | Choose the source of a new job ([§5.2](#52-sources)). |
| `-yes` | Don't ask before starting jobs. |

- **Resuming:** run the same `backfill` command again. A failed job continues from the last
  chunk it finished. Running a job twice at once is never wrong, only wasted work, and the
  job item prevents it.
- **Failing:** an event that fails to evaluate breaks the version, which ends the job. An
  archive found incomplete fails the job ([§5.5](#55-when-the-archive-is-complete)). Any
  other error is retried; after the retries the job shows as interrupted in `status`, which
  prints the command that resumes it.
- **Load:** a job pauses while the region's projector is more than 5 minutes behind, and
  continues once it is under 2.5 minutes. A new view table starts with capacity for about
  4,000 writes per second, and the projector writes to it too.
- **Time:** a job's time is mostly writes to the view table, at most 64 rows at once. A
  billion records whose events touch 200 million rows take about 13 hours.
- **Names:** a malformed version ID is a usage error, and so is a version that
  `dynaflux.lock` doesn't hold.

Cost, at us-east-1 on-demand list prices ([DynamoDB pricing](https://aws.amazon.com/dynamodb/pricing/on-demand/)):

| Item | Cost |
|------|------|
| Each row the job writes | One consistent read and one write: one read unit per 4 KB and one write unit per 1 KB of the row; about $0.75 per million rows of up to 1 KB, so about $150 for the example above |
| The table scan | One read unit per 4 KB of the table: about $3.3 per 100 GB |
| The archive export | Athena's $5 per TB of Parquet read ([§5.3](#53-staging)) |
| Staging | The records read, compressed, kept in S3 for the job's duration plus a day, written again by each merge |
| Lambda and Step Functions | The function at 3,008 MB for the job's duration, and about 10 state transitions per 14-minute step |

The state machine `{prefix}{store}-backfill` drives the job in each region by calling the
function `{prefix}{store}-backfill` one step at a time ([infra.md](infra.md)). Its alarm
`{prefix}{store}-backfill-failed` fires when an execution fails.

### 5.5 When the archive is complete

A region's archive of a stream is complete when it holds every event that the region's
change stream ever carried. Only then can a backfill split by fences and read it. Nothing in
the archive itself shows that, so Dynaflux trusts it from a recorded **origin**, as long as
no **hole** and no **gap** came after it. Each region keeps this history in a stream item
of its meta table ([encoding.md §8.5](encoding.md#85-stream-item),
[ADR 0017](../decisions/0017-archive-completeness.md)).

- **Origin.** `dynaflux backfill -all`, and `backfill <view>` for its view's stream,
  records the origin of a stream in a region that archives it with `events: all`, when the
  stream is fresh there ([§4.3](#43-fresh-streams)): its table is less than 23 hours old
  and holds no event. From then on, the region archives every event the stream gets. A
  stream that got its first event before its origin was recorded never gets one, and its
  versions read the table. `status` warns about a stream younger than 23 hours without an
  origin (DF7218), and `dynaflux import` refuses to write to one that could still record
  one (DF7406). A stream table, or its replica or stream in the region, re-created voids the
  origin there; a new one can be recorded while the new table is fresh. A region that leaves
  the store doesn't void the others' origins.
- **Holes.** A deploy that starts or stops archiving a stream in a region with
  `events: all` (`archive.regions`, `archive.events`, `archive.streams`) records a hole
  there before any function changes: the events in between may be missing (DF7107). A
  rollback records another hole, and nothing removes one. While archiving is off, no origin
  can be recorded, and once a deploy recorded a hole, none is recorded until 16 minutes after
  its last hole and its catalog's registration.
- **Gaps.** Any of these counts as a gap since the origin:
  - a batch that Lambda discarded to `aws/lambda/` in the bucket ([§7.2](#72-gaps));
  - a projector that got a record almost 24 hours old: it records this first, before
    anything that can fail the batch, so a projector that fails until DynamoDB trims the
    records still records it;
  - an hour with writes to the stream table and no successful invocation of its projector in
    that hour or the next 22, from CloudWatch's hourly data;
  - no event source mapping that reads the stream (enabled, or being created, enabled or
    updated), or a stream ARN other than the origin's;
  - an object in the error prefix that a backfill can't read ([§5.3](#53-staging)): the job
    records it, so later jobs choose the table before they pay for an export.

  One blind spot remains with `runtime.split: store`: a stream's mapping disabled for more
  than a day while the other streams keep the shared projector busy, then enabled again
  when its oldest records left are less than 23 hours 50 minutes old.

`backfill` checks these rules before it starts a job, and the job checks them again after
its last read of the archive and before it makes the version `live`. A backfill reads none
of the archive's records that arrived before the stream table was created, and none of the
error objects written before then: they are an older table's. `status` shows, for each
stream that a region archives with `events: all`, "complete since <time>" or the rule that
failed, and reports it (DF7204) for the versions activating with the archive source, which
can't go live from it. Apart from the archive, `status` reports each gap suspected in a
stream's views (DF7204): a record handled almost 24 hours old, anywhere in the stream item's
history, and, with an origin, an hour of writes no projector followed or a missing mapping
affect the versions activated before it, which may miss records. CloudWatch keeps hourly
data for 455 days, so hours older than that which no earlier check covered stay unchecked:
the stream item records it, and `status` shows "not checked before <date>".

## 6. Queries

A named query over a view reads the state of the version it targets, in the store's region.

- `live`: the query runs, also when the version's history is partial
  ([§5.2](#52-sources)).
- Any other state: the query returns an error that matches `dynaflux.ErrViewNotReady` and
  names the version, its state and the region. A broken version's message gives its
  failure.
- The option `dynaflux.AllowPartial()` (`-allow-partial` in `dynaflux query`) skips the
  check. The rows may then miss events: those a backfill hasn't applied yet, or every event
  after a version broke.
- A query reads states it read at most 30 seconds before, so a version that just went live
  can return `ErrViewNotReady` for up to 30 seconds.
- Queries over a stream don't read states.
- A query over a `SYNC` version reads its rows with strong consistency
  ([sql.md §9.7](sql.md#97-mode)).
- With `dynaflux.WaitFor(token, timeout)`, a query first waits until the projector of the
  append's region has handled the token's events. Then it reads the version's state again,
  not from the 30-second cache, and reads the rows with strong consistency. An event that
  breaks the version still ends the wait, so the query then returns `ErrViewNotReady`,
  unless `AllowPartial` is set.

Over HTTP, the query function answers a query over a version that isn't `live` with `503`
`view_not_ready`, and `GET /health` answers `503` until the current version of every view
that an exposed query reads is `live` in the region ([http.md §6](http.md#6-health)). With
`http.domain`, a Route 53 health check of each region's `/health` takes the region out of
the domain about 90 seconds after it turns `503`, and puts it back once it answers `200`.
When no region is healthy, Route 53 answers with every region.

The application's IAM policy needs `dynamodb:Query` on the meta table for the state read,
which appends to a stream with a `SYNC` view make too; without it, they fail with an error
that names the missing permission. [infra.md §9.9](infra.md#99-applications) lists what
else an application needs. `fluxtest` makes every version `live` when it creates a store,
and writes receipts at once, so `WaitFor` returns at once.

## 7. Broken versions, gaps and rebuilds

### 7.1 Broken versions

A version breaks when one event fails to evaluate, such as a division by zero
([sql.md §5.8](sql.md#58-evaluation-errors)), or when it missed an event (cause `split`,
[§5.1](#51-how-events-are-split)). Its region stops applying it, and its stream and every
other view keep going. Evaluation is deterministic, so the same event breaks the version
in every region when that region reaches it. `status` shows the event (delivery unit and
sequence number, or only the `event_id` for an event read from the table), the error, and
the Athena query that finds the event in the archive.

### 7.2 Gaps

A projector blocked for more than 24 hours loses records: DynamoDB trims a change stream
after 24 hours. Neither the views nor the archive of that region get them. When Lambda wrote
the lost batch to the bucket under `aws/lambda/`, `status` reports a gap for the stream and
region with the exact delivery unit and sequence range. `status` also reports a suspected
gap (DF7204) from the other gaps of [§5.5](#55-when-the-archive-is-complete): a record
handled almost 24 hours old, an hour of writes that no successful projector invocation
followed within 23 hours, or a stream without an event source mapping that reads it.

A gap affects every version of the stream in that region that was neither retired nor
broken then. The repair is a rebuild of each from a source that holds the lost events. A
gap also makes the region's archive incomplete for good.

Lambda's records under `aws/lambda/` don't expire. `status` reads at most the newest 100,
and none written before every version that is neither retired nor broken was activated:
no version misses those. It counts the records it didn't read.

### 7.3 Rebuilding a version

A fixed definition gets a new version from `generate`, as any change does. To fill a new
version with an **unchanged** definition, after a gap or after a fix in Dynaflux itself,
run `dynaflux generate -rebuild <view>`. It records a rebuild count for the
view in `dynaflux.lock`, which gives the current definition a new fingerprint and so a new
version number. Then deploy, backfill and retire as for any change.

## 8. Retiring a version

`dynaflux retire <view>@v<N>` removes a version that is no longer current in one
environment:

1. It checks, in every region of the environment named with `-prefix`, or those that
   `-region` names:
   - the version is `retained` (or already `retired`) in `dynaflux.lock`, not current;
   - no deployed catalog makes it current, so no deployed query reads it;
   - the view's current version is `live`, unless the retired one is broken;
   - no backfill job of the version is running.
2. It warns that it can't see application builds or `query.yaml` stacks older than the
   deployed catalog, which may still query the version: a query function answers `503` for
   it once it is retired. Deploy the application and `query.yaml` first.
3. After confirmation, it sets the version `retired` in each region. Within 30 seconds
   projectors and `SYNC` appends stop applying it, queries return `ErrViewNotReady`, and
   its fences stop holding back any delivery unit.
4. It marks the version `retired` in `dynaflux.lock`, when it checked every region: with
   `-region` naming only some, the lock stays as it is, since a deploy would retire the
   version in the others without the checks. Run `retire` without `-region` once the
   others can retire it; the regions that already hold it retired need nothing.

Then run `dynaflux generate` and deploy `regional.yaml` in every region: CloudFormation
deletes the version's table. Each other environment that deploys the same `dynaflux.lock`
needs `retire` too, with its `-prefix` and credentials, before its templates are deployed:
`retire` runs every check again there, and the deploy retires the version without them.
`dynaflux diff` reports DF7115 for a region where a deploy would retire a version while the
view's current version isn't `live` there or a backfill of the version is running.

The catalog lists retired versions, so a deploy retires them in an environment where
`retire` didn't run:

- Before the projector of a build that lists a version as retired skips one of its events,
  it sets the version `retired` when it finds it `activating` or `live`. That tombstone
  stays even when the deploy rolls back, since the build may have skipped events of the
  version.
- A successful deploy sets every version its catalog lists as retired to `retired` in the
  region's meta table, after the update finished, also one that no projector met.

An older build deployed there later therefore finds the version retired, never `live` over a
table that misses events. A region created after the version was retired never held it, and
gets no tombstone for it. Fences stay on older delivery-unit items, which expire with them.

A version number is never reused. A definition equal to a retired one gets a new number.
To retire every version of a view, remove its SQL, run `generate` (every version becomes
retained), and retire each version.

## 9. The deployed catalog

Each region's meta table holds the catalog its functions run, registered at deploy time
by the setup function after every other function of the stack was updated. A rollback
registers the previous catalog again. `status`, `diff`, `inspect`, `query`, `import` and
`backfill` read it, so they always work with what the region runs, not with the local SQL.

Every command refuses a store whose deployed catalog uses a newer catalog format than the
CLI knows ("upgrade dynaflux").

## 10. Regions and environments

- **Regions** are independent. A version can be `live` in one region and `activating` in
  another; each region answers its queries from its own state. `status` shows a version ×
  region table.
- **A region added later** starts with every version `pending_backfill`. Its versions are
  filled from its table: only the last `LOG RETENTION` of events. Run
  `dynaflux backfill -all -region <region>` after its regional deploy. `status` labels these
  versions "partial". Deploy its `query.yaml` after that backfill, as on a first deploy.
- **A re-created regional stack** loses its meta and view tables. Its versions need a
  backfill, which reads the table.
- **A re-created stream table** starts a new history. Versions filled afterwards don't
  include the old table's events, which older versions may still count: the table holds
  none, and a backfill leaves out the archive's records that arrived before the new table
  was created. It voids the origin of the stream's archive
  ([§5.5](#55-when-the-archive-is-complete)).
- **Region names** are canonical, such as `us-east-1`. `dynaflux.Open` and every command
  refuse a FIPS pseudo-region such as `fips-us-east-1`: the AWS SDK sends its requests to
  another region than the name says, so the region an event records would match no
  projector. Set `AWS_USE_FIPS_ENDPOINT` to use FIPS endpoints.
- **Environments:** every command that reads or changes AWS takes `-prefix`, defaulting to
  `prefix` in `dynaflux.yaml`, and works on one environment per run.

## 11. Limits

- A table source covers only the stream's log window. For complete backfills, keep the
  archive on with `events: all`, and let `backfill` record each stream's origin before the
  stream's first event ([§5.5](#55-when-the-archive-is-complete)).
- Reading the archive waits for Firehose to deliver everything taken before the cut:
  minutes under steady writes, and 2 hours 30 minutes when the stream gets no new
  records ([§5.3](#53-staging)).
- The time split relies on writer clocks within 5 minutes and on events landing within
  15 minutes. A late landing breaks the version with cause `split`; a writer clock more
  than 5 minutes fast can lose an event without a report. `dynaflux import` refuses to run
  on a machine whose clock is more than 60 seconds off.
- A projector about 20 minutes behind during a time-split activation breaks the version.
- A rollback past an `activating` or `live` version stops its stream in that region until
  a newer build is deployed again ([§4.1](#41-guards)).
- A backfill applies one chunk at a time in one order per region. At 1,000 events per
  second for a year, that is days of work.
- A job lists its staged runs in its job item, which DynamoDB limits to 400 KB: about 3,800
  runs. A run holds at most what a step sorts in memory, about 56 MB of stream items at the
  backfill function's 3,008 MB, so a job stages at most about 200 GB of a stream table, and
  less when steps end early, such as under `-rate`. A larger source fails staging with an
  error that names this limit.
- `dynaflux import` reads a local file or standard input, not S3. Stream an S3 object in
  with `aws s3 cp s3://bucket/key - | dynaflux import -file - <stream>`, one object per run.

## 12. Diagnostics

Operator commands print diagnostics as `validate` does
([sql.md A.1](sql.md#a1-output-format)): a finding with a source location points at it;
others print `<region>: <severity> <code>: <message>` and a `hint:` line. Severities are
`error` and `warning`. Exit status: 0 when there is no error (warnings allowed), 1 when
there is an error or an operation was refused, 2 for usage, I/O and AWS access problems, or
a region that couldn't be read. A part of a region that `status` can't read, such as its
mappings, metrics, executions, gaps or the completeness of its archive, exits with 2 too,
and the findings read from the meta table still print. Codes are stable as in
[config.md](config.md): a code that no command reports any more stays in the table as
`retired`.

| Code | Severity | Meaning |
|------|----------|---------|
| DF7001 | error | The deployed catalog's format is newer than this CLI. |
| DF7101 | error | `diff`: a stream column was removed or renamed. |
| DF7102 | error | `diff`: a column was retyped (`LowCardinality(String)` ↔ `String` allowed). |
| DF7103 | error | `diff`: the `DEFAULT` of an existing column changed. |
| DF7104 | error | `diff`: `PRIMARY KEY`, `DEDUP BY` or `SHARDS` changed. |
| DF7105 | error | `diff`: a new column is neither `Nullable` nor defaulted. |
| DF7106 | warning | `diff`: a stream is dropped; its table and archive stay. |
| DF7107 | warning | `diff`: a stream stops being archived with `events: all` in a region; Firehose may drop one buffer, and the region's archive gets a hole for good. |
| DF7110 | error | `diff`: a deployed version that the lock neither holds nor retires; deploying drops its table. |
| DF7111 | error | `diff`: the same version number with a different fingerprint. |
| DF7112 | warning | `diff`: the new current version isn't `live` in the region; deploy the app after its backfill. |
| DF7113 | warning | `diff`: `prune_window_seconds` is lower than deployed. |
| DF7114 | error | `diff`: the lock keeps a version that the region's meta has retired. |
| DF7115 | error | `diff`: the lock retires a version that the region's meta doesn't hold as retired, while the view's current version isn't `live` there or a backfill of the version is running; run `retire` for the environment first. |
| DF7201 | error | `status`: a version is broken (cause, delivery unit and sequence number or `event_id`, error, time, Athena SQL). |
| DF7202 | warning | `status`: a version needs a backfill. |
| DF7203 | error | `status`: a gap was found (delivery unit, sequence range, time, affected versions). |
| DF7204 | warning | `status`: a region's archive of a stream isn't complete, which affects the versions activating from it, or a gap is suspected in a stream's views, which affects the versions activated before it ([§5.5](#55-when-the-archive-is-complete)): the rule or the evidence, its time, and the versions it affects. |
| DF7205 | warning | `status`: projector lag is 15 minutes or more. |
| DF7206 | warning | `status`: another function reads the stream. |
| DF7208 | error | `status`: the stream's mapping isn't enabled, or its last result is a problem. |
| DF7209 | error | `status`: projectors block on a version their catalog doesn't know (guard 2); the hint names the build to deploy and the time records start to be lost. |
| DF7210 | warning | `status`: the archive is stale, or rows failed. |
| DF7216 | warning | `status`: a backfill is interrupted (its execution ended without finishing). |
| DF7217 | retired | Was: `status`: a backfill waits for the archive read of a later release. Retired in M6, when backfills read the archive. |
| DF7218 | warning | `status`, `backfill -all`, `import`: a stream table is less than 23 hours old and has no origin in a region that archives it with `events: all`; run `backfill -all` before its first event, or the region's archive of it is never complete ([§5.5](#55-when-the-archive-is-complete)). `backfill -all` says why it recorded none, and `import` warns of a table that holds an event already, whose archive is never complete. |
| DF7301 | error | `retire`: the version isn't `retained` or `retired` in the lock. |
| DF7302 | error | `retire`: a deployed catalog makes the version current. |
| DF7303 | error | `retire`: the view's current version isn't `live` in a region. |
| DF7304 | error | `retire`: a backfill of the version is running. |
| DF7401 | error | `import`: a line doesn't parse or doesn't fit its column. |
| DF7402 | error | `import`: a key isn't a column of the deployed stream. |
| DF7403 | error | `import`: an event already stored with another payload. |
| DF7404 | warning | `import`: a view of the stream buckets by `ingest_time`. |
| DF7405 | error | `import`: this machine's clock differs from DynamoDB's by more than 60 seconds. |
| DF7406 | error | `import`: the stream table is less than 23 hours old and holds no event, and has no origin in a region that archives it with `events: all`; run `backfill -all` first, since an imported event ends the time to record one ([§5.5](#55-when-the-archive-is-complete)). |
| DF7501 | error | `backfill`: the version isn't in the region's deployed catalog; deploy `regional.yaml` there first. |
| DF7502 | error | `backfill`: the version is broken or retired. |
| DF7503 | error | `backfill`: `-source archive`, but the region's archive of the stream isn't complete (the rule that failed, [§5.5](#55-when-the-archive-is-complete)). |
| DF7504 | error | `backfill -cancel`: the version has no running job to cancel. |
| DF7505 | retired | Was: `backfill`: the version's job waits for the archive read of a later release. Retired in M6, when backfills read the archive. |

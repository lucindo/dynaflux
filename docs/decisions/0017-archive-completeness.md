<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0017: A region's archive is trusted for a backfill only from a recorded origin, with no hole and no gap evidence since

- Status: Accepted. Amends [ADR 0012](0012-view-version-activation.md), whose decision
  stands: §4 ("Stream origin") left the record to M6.
- Date: 2026-10-27

## Context

The fence split hands the backfill every event at or below the fences, from the region's
archive ([ADR 0016](0016-archive-export-with-athena.md)). That is exact only when the
archive holds **every** event the region's change stream ever carried, back to the stream's
first event. Three things break that, and none shows in the archive itself:

1. **The archive started late.** A stream that had events before its region archived them
   with `events: all`, such as a store that turned archiving on later.
2. **Holes.** A deploy that switches a region's archiving for a stream off and on again
   (`archive.regions`, `archive.events`, `archive.streams`) leaves the events of the
   interval unarchived. Firehose may also drop one buffer when a stream stops being
   archived (DF7107).
3. **Gaps.** A projector blocked for more than 24 hours loses records that DynamoDB trims
   from the change stream; neither the views nor the archive get them
   ([lifecycle.md §7.2](../spec/lifecycle.md#72-gaps)). Firehose also drops Direct PUT data
   it can't deliver for 24 hours ([delivery failures][fh-retry]).

ADR 0012's F2 and F3 (the table younger than 23 hours by every replica, a consistent scan of
the region's replica finding nothing) prove that the region's change stream carries every
event the stream will have. ADR 0012 deferred recording that fact, under two rules: it
holds only while the registered catalog keeps the exact handoff (`ExactHandoff(R, S)`, the
stream archived in R with `events: all`), and a change of that handoff, recorded before the
functions change, is a hole for good. m5-plan §17 handed the details over, with the review's
corrections.

While designing the record, M6 found a race in §17: a deploy that turns archiving off
writes its hole, then updates the functions, then registers the new catalog. In between,
`catalog/current` still says `ExactHandoff`, so an origin written then is later than the
hole, the hole is ignored, and the events from then on are unarchived.

## Decision

**1. A stream item per stream in each region's meta table** (`pk = stream#<stream>`,
`sk = !`, read and written whole, conditional on `_v`):

| Attribute | Meaning |
|-----------|---------|
| `mode` | `all`, `local` or `off`: the region's archive mode for the stream as the last deploy set it; `ExactHandoff` exactly when `all` |
| `origin` | written when absent or void (§4): `at`, the `catalog/current` hash and `registered_at` then, `born` (the earliest replica `CreationDateTime`), `arn` (the stream ARN in this region), and `holes`, the length of the hole list then |
| `holes` | an ordered list, never removed, of mode changes (`update`, `create`, with `from` and `to`), `lag` (a projector handled a record above progress at least 23 h 50 min old) and `errors` (a job found commit-failure or undecodable error objects); each with `at`, the writer's clock, for display only |
| `checked_through` | how far the last passing check of projector activity reached |
| `unchecked_before` | the hours since the origin before it that no check covered, past CloudWatch's 455 days when a passing check started |

**Order, not clocks.** The item is written whole and conditional on `_v`, so the list's
order is the order of the writes. Only holes at an index at or after `origin.holes` count.
No rule compares `origin.at` (the CLI host's clock) with a hole's `at` (the setup
function's or a projector's clock): with a skew δ, a hole written within δ after the origin
would look older and be ignored.

**2. The archive mode and holes are written before the functions change**, by
`Custom::DynafluxArchiveMode` in every region (served by the setup function, constant
physical ID `<meta table>/archive-mode`), whose properties map each stream to its mode in
that region. Every projector function `DependsOn` it.

- A stream without a stream item gets one with its mode and no hole, on `Create` and
  `Update` alike. Either the stream is new in the region, and no earlier build read it (its
  mapping is created after the function it invokes is updated), or the meta table is new,
  and then an origin needs F2 and F3, which a stream that ever held an event fails.
- A stream whose item exists gets `mode` and a hole in one write whenever its
  `ExactHandoff` differs between the old and new properties, in either direction, with or
  without an origin. A stream missing from one side counts as `off`.
- A rollback is another `Update` with the properties swapped: it writes `mode` back and
  appends another hole. Nothing undoes a hole. `Delete` writes nothing.

**3. The origin** is written by `dynaflux backfill -all` (and `backfill <view>` for its
stream) in region R for stream S when, in order:

1. the stream item's `mode` is `all` (**the correction**: from the moment a deploy that
   turns archiving off writes its hole, no origin can be written);
2. R's `catalog/current` has `ExactHandoff(R, S)`;
3. F2 and F3 hold, with the scan sent after items 1 and 2 were read;
4. when the stream item holds a mode hole, the scan starts at least 16 minutes (the longest
   invocation plus 60 s) after `current.registered_at` and after the last mode hole's `at`,
   so no invocation of a build that didn't archive is still running. A rollback writes its
   hole and registers no catalog, in an order CloudFormation doesn't document; with item 1,
   the wait from the hole also covers what item 2 guards when the registration comes within
   16 minutes of it. Comparing the CLI's clock with the setup function's is what the 60 s
   allow for.

The write is conditional on the `_v` read in item 1 and records `holes`, the list's length
then. An origin certifies the archive from then on; a hole counts only when it comes after
the origin in the list. Item 4 looks at mode holes only. An origin that §4 voids is
replaced the same way; the old life's `lag` and `errors` holes sit before the new index and
drop out.

**4. Gap evidence**, any of which makes the archive incomplete:

1. a gap Lambda wrote to `aws/lambda/` for `origin.arn`;
2. a `lag` hole after the origin. Before anything that can fail the batch, the view states
   among them (G3), a batch with an `INSERT` or a `MODIFY` and a record at least 23 h 50 min
   old reads its unit and appends the hole when such a record is above progress; a failed
   write fails the batch. So a projector that fails until DynamoDB trims the records after
   it still records the lag, and progress never passes a trimmed range unrecorded;
3. an hour with writes to the stream table (`ConsumedWriteCapacityUnits`, both sources) with
   no successful invocation (`Invocations` less `Errors`) of the stream's projector function
   in that hour or the next 22, checked from the origin's hour on: a bound on the query, not
   a filter that could drop evidence, since F2 and F3 prove that no record predates the
   origin. Lambda counts failed invocations, init failures among them, in `Invocations`;
4. no mapping on the stream that reads it from its checkpoint (`Enabled`, `Creating`,
   `Enabling` or `Updating`: a deploy that changes the mapping passes through them), or a
   stream ARN other than `origin.arn`;
5. an `errors` hole after the origin: a job found commit-failure or undecodable objects in
   the error prefix, which it reads first.

The origin is void when `arn` isn't the stream's ARN in the region: the table, its replica
there or its stream was re-created. `born` doesn't void it: removing the oldest replica's
region changes the earliest creation time, while the region's stream goes on. `backfill
-all` may then record a new one when §3 holds, and the archive's history starts over. A job
reads none of the archive's records whose `_arrival` is before the table's earliest replica
creation, to the second, and none of the error objects written before it: they are an older
table's, which the Iceberg table and the error prefix keep.

**5. One pure function, `backfill.Completeness(Evidence) Verdict`,** evaluates these rules
for the CLI's choice of a source, for the job, and for `status`. The job re-reads the stream
item and `catalog/current` and evaluates every rule at `select`, after the last archive
read, and the stream item and `current` again before `MakeLive`. A failed rule fails the
job, naming the hole or evidence; the version stays `activating` and needs a rebuild from
the table, since its split can't change.

### Rejected

- **Origin from `catalog/current` alone** (m5-plan §17 as written): the race above.
- **Comparing hole and origin times:** three hosts' clocks; see "Order, not clocks".
- **An origin written once and never replaced:** a re-created stream table would leave
  the archive incomplete for good.
- **Firehose-loss evidence (`DataFreshness` of 23 hours or more) and a re-created archive
  table (Glue `CreateTime`):** beyond the brief. The `DataFreshness` alarm pages after 15
  minutes, a day before Firehose drops data, and a re-created archive table needs someone
  to drop it by hand. Deferred.
- **Holes written by the catalog registration:** it runs after the functions changed, too
  late to stop an origin and too late to bound the interval.
- **Ignoring holes recorded after the cut:** sound, since records at or below a fence were
  archived before the cut, but one more case to prove for a rare deploy during a backfill.
- **Per-mapping `PolledEventCount` for item 3:** exact for `split: store`, but mapping
  UUIDs change on replacement and CloudWatch forgets metric names after two weeks. Items 2
  and 4 narrow the blind spot of one function serving several streams; one case stays
  open: a mapping disabled for over a day while other streams keep the function busy, then
  enabled again when its oldest records left are less than 23 h 50 min old.
- **Parent-shard evidence in the job's `fence` phase** (a parent shard unfinished while a
  child has progress): `DescribeStream` lists a closed shard for 24 hours only, so it would
  see only a parent closed within the last day; and progress stays below a parent's end
  whenever its last records are `REMOVE`s or `MODIFY`s, as TTL deletes make, which would fail
  complete archives.
- **Counting writes by `SuccessfulRequestLatency`**, which leaves out conditional writes that
  failed (duplicate appends, an import run again): it would end a false silent hour, but
  whether DynamoDB reports `TransactWriteItems` per table is undocumented, and a write it
  misses fails open. `ConsumedWriteCapacityUnits` errs toward a suspected gap.
- **Failing closed past CloudWatch's 455 days of hourly data:** a store that runs no
  archive backfill for 15 months would lose the archive source for good; `checked_through`
  and a `status` note ("not checked before <date>") keep it usable.
- **Writing the origin at deploy time:** needs `dynamodb:Scan` on stream tables for the
  setup role; `backfill -all` already runs right after a first deploy.

## Consequences

- The first deploy's `dynaflux backfill -all` records origins, and so does the one after a
  new stream. A stream younger than 23 hours without an origin gets a warning (DF7218), and
  `dynaflux import` refuses to seed one (DF7406): after its first event, no origin can be
  proven and every later version backfills from the table.
- `status` reports suspected gaps and incomplete archives (DF7204) with the affected
  versions; `backfill -source archive` names the failed rule (DF7503).
- The projector reads its unit and the stream item only on a batch with a record at least
  23 h 50 min old, and writes the stream item when one is above progress: two `GetItem`s and
  one conditional `PutItem` before anything else, with grants it has. A job that finds bad error objects writes an `errors` hole,
  so later `backfill` runs choose the table source before paying for an export.
- The simulation harness models archive modes per region switched by deploys, origins
  written by a CLI racing the deploys, a clock per actor with skew, projector stalls past
  24 hours, and negative controls for the hole check, the `mode` condition, a hole written
  after the functions change, an ignored `lag` hole, a lag recorded after `advance` or with
  its error ignored, and holes ordered by clock.

[fh-retry]: https://docs.aws.amazon.com/firehose/latest/dev/retry.html

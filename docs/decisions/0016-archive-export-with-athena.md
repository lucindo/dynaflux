<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0016: A backfill reads the archive with sliced Athena `UNLOAD`s, after a `DataFreshness` watermark, plus the error prefix

- Status: Accepted. Amends [ADR 0011](0011-backfill-coordinator.md), whose decision
  stands: the `backfill.Archive` seam is reshaped, the job item's `stage` gains the
  archive fields, and its `waits` goes, since no job waits for a later release.
- Date: 2026-10-27

## Context

The fence split (plan D25, S1b) hands a version's events at or below each delivery unit's
fence to the backfill, which reads them from the region's archive once the archive holds
everything accepted before the cut. M5 proved the split behind `backfill.Archive` in the
simulation harness and against DynamoDB Local, and on AWS used it only for fresh streams,
which need no read (ADR 0011 §5, ADR 0012 §3). M6 builds the read on AWS.

The archive is an Iceberg table per stream in the Glue catalog, partitioned by
`day(event_time)`, written by Firehose with `AppendOnly`; records Firehose can't deliver go
to `errors/firehose/<stream>/` in the bucket ([infra.md §7](../spec/infra.md#7-archive)).

What AWS documents:

- `UNLOAD` writes gzip JSON files, several in parallel, refuses a destination that holds
  data, and leaves its files when it fails. Its data manifest is written to the query
  result location in S3 ([UNLOAD][unload], [output files][athena-out]); managed query
  results are readable only through the API ([managed results][managed]).
- A DML query times out after 30 minutes by default (240 at most); running and queued
  queries count against an account-wide concurrency quota
  ([quotas][athena-quotas]).
- `StartQueryExecution` with the same `ClientRequestToken` returns the same query and
  creates no other ([StartQueryExecution][sqe]).
- `DeliveryToIceberg.DataFreshness` (namespace `AWS/Firehose`, dimension
  `DeliveryStreamName`) is "the age (from getting into Firehose to now) of the earliest
  record in Firehose. Any record earlier than this age has been delivered to Apache Iceberg
  Tables" ([Iceberg metrics][fh-ice-metrics]); a record Firehose can't deliver goes to the
  backup bucket after its retries ([CloudWatch metrics][fh-cw]). Datapoints are stamped with
  their period's start ([CloudWatch concepts][cw]). A wrong metric name, dimension or region
  returns no datapoints, not an error.
- After its retry window, a failed Iceberg commit sends data file keys, not records, to the
  error prefix ([Iceberg considerations][fh-ice-cons]).
- Athena doesn't document the JSON text `UNLOAD` writes for `timestamp`, `decimal` or
  `bigint`.

## Decision

1. **Phases.** The fence split keeps M5's phases. `watermark` polls
   `Archive.Complete(stream, cut)` every 60 s; `stage` reads the error prefix, then plans
   slices, exports them one at a time and stages each into sorted runs as it succeeds. One
   invariant carries M5's proof: **every archive read (the error-prefix listing, the
   partition listing, every slice) starts after the watermark passed the cut, and the
   fences are read after the last of them ended.** Their order among themselves doesn't
   matter to the proof. Each read then returns a superset of the records at
   or below the fences; the extras are above a fence or are copies that the merge drops by
   (`_shard`, `_seq`).
2. **Watermark.** Complete when some `AWS/Firehose` `DeliveryToIceberg.DataFreshness`
   (Maximum, dimension `DeliveryStreamName`, period 60 s; 300 s once the cut is more than 15
   days old) datapoint at period start t with value f has t − f > cut + 60 s. **Idle rule:**
   also complete when the job's clock is past cut + 2 h 30 min, Firehose is empty now (no
   datapoint for periods starting in [now − 20 min, now − 5 min]), and there is positive
   evidence that the metric is the right one: `firehose:DescribeDeliveryStream` reports the
   stream `ACTIVE`, and `cloudwatch:ListMetrics` lists the metric for it. Without that
   evidence the job keeps waiting and says why. Both rules rest on one assumption, which
   the end-to-end suite checks: the metric is emitted whenever Firehose holds a record.
3. **A backfill workgroup**, `{prefix}{store}-backfill`, in archive regions, with results
   under `athena/backfill/` in the region's bucket (SSE-S3, expected bucket owner,
   configuration enforced) and a lifecycle rule (30 days; noncurrent versions 1 day). The
   archive workgroup's managed results would keep the manifest out of the job's reach.
4. **Slices.** One `SELECT … FROM "<stream>$partitions"` after the watermark gives each
   day's record count and bytes. Consecutive days join a slice up to a byte target (25 GB of
   Parquet until the end-to-end suite measures throughput); a larger day is a slice of its
   own. A slice that times out or exhausts resources is halved, down to one hour. The plan
   is a staging object committed to the job item by name.
5. **One statement per slice:**
   `UNLOAD (SELECT <typed projection>, "_shard", "_seq", "_arrival" FROM "<db>"."<stream>" WHERE
   "event_time" >= <from> AND "event_time" < <to>) TO '<staging>unload/<plan>/<from>-<to>/<try>/'
   WITH (format = 'JSON')`. The projection casts `bigint`, `decimal`, `date`, `timestamp`
   and arrays of them to `varchar`, so one `archive.Decoder` reads the export and the
   error records (our own encoder's bytes). For a `DEDUP BY` stream the reader recomputes
   `event_id` from the decoded values and fails the job on a mismatch: a damaged time or key
   column can't pass silently. A record whose `_arrival` is before the stream table's
   creation is an older table's, and the job leaves it out
   ([ADR 0017](0017-archive-completeness.md) §4).
6. **Tries.** Each slice has a try counter in the job item, its own destination per try,
   and a token H(version, `created_at`, plan, from, to, try), plan being the committed plan
   object's name: a restage commits a new plan, so a try counter that starts over can't
   meet an old query or a non-empty destination. The partitions' listing's token counts the
   failed tries and the restages, so a restage lists them again. The query ID is saved before
   polling. Only the files the succeeded query's manifest lists are read. Throttling and
   concurrency limits wait 60 s with the same token; retryable failures and cancellations
   take a new try (three at most for other failures); permission failures fail the job
   naming the grant. Two checks fail a try or the job: a whole-day slice must yield at least
   the record count `$partitions` reported, and the first slice, when whole, must not scan
   more than twice its planned bytes plus 1 GB (partition pruning failed): a half's bytes are
   a share of its days', which it may scan whole.
7. **Error prefix**, read first: list recursively, the objects written since the stream
   table's creation, gunzip by magic number, decode
   `rawData`. A line with `_shard` and `_seq` is ours and goes to the runs; one without them
   isn't ours and is counted and skipped; ours that doesn't decode, data file keys, and
   anything else append an `errors` hole to the stream item
   ([ADR 0017](0017-archive-completeness.md)) and fail the job as an incomplete archive, so
   later jobs choose the table source before paying for an export.
8. **Resume within a file**: the job item records the slice, the manifest file and the rows
   staged from it; a run never spans a commit.
9. **The seam:**

```go
type Archive interface {
	Complete(ctx context.Context, stream string, cut time.Time) (bool, error)
	Start(ctx context.Context, q Query, token string) (id string, err error)
	Status(ctx context.Context, id string) (QueryStatus, error)
	Stop(ctx context.Context, id string) error
	Partitions(ctx context.Context, id string) ([]Partition, error)
	Files(ctx context.Context, id string) ([]string, error)
	ErrorObjects(ctx context.Context, stream, after string, since time.Time) ([]string, error)
	Open(ctx context.Context, name string) (io.ReadCloser, error)
}
```

The harness model implements it without SQL; the AWS adapter builds the SQL with
`archive.Unload` and `archive.PartitionsQuery`, next to the table DDL.

### Rejected

- **`SELECT *` and a decoder for whatever Athena writes:** a lossy timestamp or decimal
  would pass silently on a stream without `DEDUP BY`.
- **`UNLOAD` to Parquet:** a Parquet reader dependency for one consumer.
- **One query per table with the 240-minute timeout:** needs a quota increase, and a
  failure repeats the whole scan.
- **Sharing one export among jobs of one stream:** saves N − 1 scans, at $5 per TB, at the
  cost of a shared export item, reference counting and a restage story. Revisit when an
  archive passes several terabytes.
- **At most one export per stream and region:** it saves no scans (N jobs still scan N
  times) and doesn't bound Athena's concurrency across streams; the 60-second throttle wait
  with the same token already handles the account quota.
- **A workgroup-wide `BytesScannedCutoffPerQuery`:** it would fail a legitimate large day
  forever; the pruning check bounds a failure to one scan.
- **A Trino container in CI:** it doesn't run `UNLOAD`, and Athena isn't Trino release for
  release.

## Consequences

- At the plan's ceiling (a year at 1,000 events/s, about 5 TB of Parquet) a backfill reads
  about $25 of Athena per region, plus S3 and Lambda for staging; the view writes of
  `apply` stay the dominant cost.
- The backfill role gains Athena, Glue and S3 read grants on the archive, scoped to the
  backfill workgroup, the archive database and fixed prefixes, plus `cloudwatch:ListMetrics`
  and `firehose:DescribeDeliveryStream`; the operator gains `athena:StopQueryExecution` for
  `backfill -cancel`. Athena reads and writes with the caller's credentials, which no
  static grants test sees, so the end-to-end suite runs through the deployed backfill
  function, and a catalog that Lake Formation governs needs grants to the backfill role.
- `status` shows "archive delivered through <t − f>" and the bytes a job scanned.
- The adapters are tested against recorded responses and a minimal in-process fake of the
  Athena, S3 and CloudWatch subsets, which "executes" only the two statement shapes
  `archive` generates and proves the wiring with one failed try; fault coverage lives in
  unit tests over a scripted `Archive` and in the simulation harness. The fake encodes our
  reading of Athena; the end-to-end suite's experiments (the JSON forms, `$partitions`,
  token memory, idle `DataFreshness`, error object layout, cancellation, concurrency)
  confirm it before M6 calls the archive source done.
- Firehose commit failures make the archive incomplete for a backfill: the version needs a
  table source.

[unload]: https://docs.aws.amazon.com/athena/latest/ug/unload.html
[athena-out]: https://docs.aws.amazon.com/athena/latest/ug/querying-finding-output-files.html
[managed]: https://docs.aws.amazon.com/athena/latest/ug/managed-results.html
[athena-quotas]: https://docs.aws.amazon.com/athena/latest/ug/service-limits.html
[sqe]: https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html
[fh-ice-metrics]: https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-metric.html
[fh-cw]: https://docs.aws.amazon.com/firehose/latest/dev/monitoring-with-cloudwatch-metrics.html
[fh-ice-cons]: https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-considerations.html
[cw]: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html

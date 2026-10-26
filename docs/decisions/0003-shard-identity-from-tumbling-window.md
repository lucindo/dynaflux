<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0003: The projector learns its shard from a Lambda tumbling window

- Status: Accepted
- Date: 2026-10-26

## Context

The projector's correctness model keys its bookkeeping by DynamoDB stream shard: each view row
keeps a high-water mark per shard, and each shard has a progress item and per-view fences for
backfill handoff. So every batch must know which shard it came from.

A standard Lambda invocation from a DynamoDB stream doesn't say. The event has one top-level
field, `Records`, and no record field, context field or environment variable names the shard.
Sequence numbers are ordered only within a shard, and open shards have no end of range, so a
record can't be matched to a shard by range: in a test on AWS, that lookup was right for 16 of
41 records.

A tumbling window (`TumblingWindowInSeconds`) adds a top-level `shardId` to every invocation.
The maintainer ran a scripted test on AWS with a 60-second window and the production event
source mapping settings:

| Check | Result |
|-------|--------|
| `shardId` is the record's real shard | Yes, for all 93 records |
| Partial batch responses (`ReportBatchItemFailures`) | Work: the retry starts at the reported record |
| Batch bisection on error | Works |
| Order of first delivery within a shard | No violations |
| Response with `state` null or missing | Treated as a failed invocation |
| Final invocation at each window end | Carries no records |
| Extra latency over a mapping without a window | Median 43 ms, p95 1.0 s, max 1.3 s on clean writes; up to 6.8 s during retries |

AWS rejects a tumbling window on a mapping that has `FilterCriteria`
(`Tumbling window period should be zero when FilterCriteria is configured`).

## Decision

- The projector's event source mapping sets `TumblingWindowInSeconds = 900`, the maximum, which
  gives the fewest window boundaries. The window is used only to learn the shard.
- The mapping has no `FilterCriteria`. The projector skips `REMOVE` records in code.
- The handler always returns `"state": {}` together with `batchItemFailures`.
- An invocation with no records (the window's final invocation) returns at once.
- An invocation with an empty `shardId` fails the whole batch, so a change on the AWS side stops
  projection loudly instead of corrupting high-water marks.
- The AWS adapter passes `(delivery unit, records)` to the projector core. The core never sees
  shards or windows, so the simulation harness and the in-memory store use the same core.

## Consequences

- The other mapping settings stay as planned: `TRIM_HORIZON`, `ParallelizationFactor = 1`,
  bisection, `ReportBatchItemFailures`, unlimited retries within the stream's 24 hours, and an S3
  on-failure destination.
- `REMOVE` records now reach the function: about one per event per region once TTL deletes it.
  Stream reads are free, so the cost is a little Lambda time; the cost model should count it.
- View lag grows a little: usually under a second more, a few seconds during retries.
- The test used a 60-second window. The AWS end-to-end suite must run the 900-second production
  value, including a shard rollover.

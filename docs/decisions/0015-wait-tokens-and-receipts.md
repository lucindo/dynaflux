<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0015: Wait tokens poll receipts that the origin region's projector writes for appends that ask for them

- Status: Accepted
- Date: 2026-10-27
- Amended by [ADR 0019](0019-v1-compatibility-promise.md): in format 1, each event carries
  its `event_id` as 16 bytes and its `_it` in microseconds, in place of `sk` and `_it` as
  text. The first `_it` is a uvarint, and each next one a varint of the difference from the
  one before. A 1,000-event token shrinks from 97,358 characters to about 24,000.

## Context

Plan §9 offers read-your-writes without `SYNC`: `Append` returns a token, and the query
option `WaitFor(tok, timeout)` waits until the projector has applied the append, at the
cost of "one extra small write per event, only when wait is used". The token is
region-local ([sql.md §9.7](../spec/sql.md#97-mode)). Today a token names the stored events'
keys and the engine's region; `AppendOption` exists with no options
([ADR 0006](0006-generated-code-contract.md)).

The projector can't know at apply time whether anyone will wait, so the append must say so
on the event. What "applied" means must hold under the
projector's failure modes: a batch whose rows partly fail resumes from the lowest failed
sequence number, and records at or below a unit's `progress` are dropped on redelivery
without being looked at again.

## Decision

1. **Opt-in per append.** `dynaflux.Waitable()`, an `AppendOption`, sets `_rcpt` (BOOL
   true) on each event of the call. Generated code doesn't change: `fluxgen` starts reading
   the options it already receives.
2. **Receipt item** in the meta table:
   `pk = rcpt#<stream>#<event_id>#<_it>` (`_it` in µs, as stored), `sk = !`, `_ttl` = write
   time + 25 hours. One partition per receipt; written unconditionally. The `_it` ties a
   receipt to one stored record: a `DEDUP BY` event appended again after its item expired
   has the same `event_id`, and the old receipt, whose TTL deletion runs late, would
   otherwise answer for the new record.
3. **Only the projector of the event's origin region writes it**: for each `INSERT` with
   `_rcpt` and `_r` equal to its region.
4. **After `advance`.** In a batch, receipts are written once progress is raised past
   them, for records below the batch's `FailedSeq` (every record when none failed), and
   for the records at or below progress that the batch drops as already applied. Records
   that reach no version, fenced or `_sync`-named ones included, still get a receipt: it
   means "handled here". A failed receipt write lowers `FailedSeq` to its record; the
   redelivery finds it at or below progress and writes the receipt. Before `advance` a
   receipt could claim a record that the batch then fails: a version that the batch read as
   `pending_backfill` can be fenced below the record after the batch read its unit, and
   `advance` then fails with the fence, leaving the record to the redelivery. `advance` is
   conditional on the unit item, as the fence is, so a receipt written after it never
   precedes such a fence.
5. **Token contents.** Format 1, redefined before the first release: format byte, region,
   stream, then per stored event its `sk`, `_it` and a flag byte: bit 0 the stored item has
   `_rcpt`; bit 1 the stored item's `_r` isn't the engine's region. `_it` and the flags
   come from the stored item, so the token of an `ErrDuplicate` is right too. The token
   leaves out `pk`, which holds the event's `PRIMARY KEY` values and which a receipt doesn't
   need: tokens reach clients and logs.
   `dynaflux.ParseToken` reads a token's text back, so a token can cross processes. A token
   is unauthenticated text and grants nothing.
6. **`WaitFor`.** A zero token waits for nothing. Before any read, `WaitFor` fails at once
   when the token's region isn't the engine's, when it holds more than 1,000 events, when
   its stream isn't in the catalog, when an event lacks bit 0 ("append with
   `dynaflux.Waitable()`"), or has bit 1 ("stored by another region; its receipt is
   written there"). Otherwise it polls the missing receipts with consistent `BatchGetItem`
   (at once, then after 50, 100, 200 and 400 ms, then every 500 ms, ±20 % jitter) until
   all are present, the timeout passes (`ErrWaitTimeout`), or the context ends. Then the
   view states are read again, consistently, and the query runs with strongly consistent
   reads. The fresh read matters: the waited record, or another in its batch, can open a
   version's breaker, and the projector then drops the version's groups and still writes
   the receipt; the cache can be 30 seconds old. A query over a stream skips both: a
   consistent read in the append's region already sees the event.

### Rejected

- **A per-stream switch that makes every event carry `_rcpt`:** every event pays for a
  receipt whether anyone waits or not, against plan §9.
- **Receipts in every region:** R times the cost, and a wait in another region would
  still race replication. Tokens are region-local.
- **A receipt written before the rows, or between the rows and `advance`:** before the
  rows it can claim an event that isn't applied; between them, one that a fence created
  after the batch read its unit leaves to the redelivery (§4).
- **A short TTL with the append time in the token** (and a "token too old" error): 25
  hours outlives every stream record, so an absent receipt always means "not yet", and the
  `_it` in the key keeps a present one from answering for another record.
- **A receipt keyed by `event_id` alone:** on `DEDUP BY` streams an `event_id` can be stored
  again once its item expires, and the old receipt would end a wait before the projector
  applied the new record.
- **Waiting on the unit's `progress`:** the token would need the record's shard and
  sequence number, which the writer never learns.

## Consequences

- The cost is one write unit per waitable event in its origin region, plus about six
  consistent reads of one unit for a wait of about a second. Non-waitable appends pay
  nothing.
- The latency floor is the projector's: the batching window (1 s by default) plus the
  invocation. Read-your-writes in milliseconds needs `SYNC`.
- A token can be waited on for 24 hours; after 25 hours its receipt may be gone, and
  `WaitFor` times out rather than answering early.
- Applications that wait need `dynamodb:BatchGetItem` on the meta table, which the
  documented policy limits to `rcpt#` keys with `dynamodb:LeadingKeys`. Receipts and polls
  load the meta table; with provisioned meta capacity, size it for them.
- New public API before the first tag: `Waitable`, `WaitFor`, `ParseToken`,
  `ErrWaitTimeout`.
- The query handler has no wait in M6: a custom domain can route a request to another
  region than the append's. `engine.Wait` and `ParseToken` keep that door open.
- The simulation harness checks that a receipt lands only once its unit's progress
  covers its record, that every waitable `INSERT` gets one in its origin region and none
  elsewhere, and that a query after a wait sees the waited event or refuses, with negative
  controls for each placement rule and the `_it` key; an engine test is the control of the
  fresh state read, which the harness seldom reaches.

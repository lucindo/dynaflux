<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0014: `SYNC` views are applied in the append's transaction, and the origin region's projector skips them by `_sync`

- Status: Accepted. Amends [ADR 0004](0004-runtime-storage-boundary.md), whose decision
  stands: the boundary gains `Transact` and two error classes, `ErrConflict` and
  `ErrThrottled`.
- Date: 2026-10-27

## Context

A view declared `MODE SYNC` must show an append to a query in the same region at once
([sql.md §9.7](../spec/sql.md#97-mode), plan D8). Every other view is applied by the
projector from the region's change stream, about a second later. M6 has to decide:

1. **Which versions an append applies.** A view can have a current and retained versions,
   each in its own state per region ([lifecycle.md §3](../spec/lifecycle.md#3-view-version-states)).
   A version that is still `activating` is being filled by a backfill job, which splits the
   events with live projection by fences or by `ingest_time`. If an append applied such a
   version, the archive or the table the backfill reads would have to say which events the
   append already applied.
2. **How the projector avoids applying the event a second time.** The writer and the
   projector run different builds, read the view states at different moments, and a
   `MODE` change doesn't change a version's fingerprint, so the two can disagree about a
   version's mode and state.
3. **How a retried append stays exactly once.** The SDK retries, the engine retries after a
   lost condition, and a caller may resend.
4. **What the storage boundary offers.** ADR 0004 writes whole items with two conditions
   and has no transaction; it anticipated `Transact` for M6.

What DynamoDB does ([transactions][txhow], [TransactWriteItems][txapi]):

- At most 100 actions and 4 MB, in one account and region; ACID only in the region that
  writes, for a global table. Other regions receive the items through replication, as
  ordinary writes.
- A cancelled transaction returns one reason per action, in order, with `None` for actions
  that didn't fail, and the stored item when the action asked for
  `ReturnValuesOnConditionCheckFailure = ALL_OLD`.
- Two transactions on one item cancel with `TransactionConflict`; a single-item write that
  meets a transaction in progress fails with `TransactionConflictException`. Cancelled
  transactions are billed. The SDK doesn't retry a cancelled transaction.
- A resend with the same `ClientRequestToken` within 10 minutes succeeds without a second
  effect. The SDK fills a token once per call and reuses it on its own retries, which stop
  after its attempt budget. For `TransactionInProgressException` AWS advises retrying with
  the same token until at least 5 seconds after the first attempt; the SDK counts that
  exception as throttling (`aws/retry` v1.47.4).
- IAM has no transaction action: `PutItem` and `GetItem` govern the actions, and the
  `dynamodb:EnclosingOperation` condition key can restrict them to transactions
  ([IAM][txiam]).

An experiment on DynamoDB Local 3.3.2, run for this design, confirmed the reasons and
returned items, the token reuse after a dropped response, and that transactional puts
appear in the change stream as ordinary `INSERT` records. It also
showed two deviations: DynamoDB Local never raises `TransactionConflict`, and it accepts
two actions on one item, which AWS rejects ([usage notes][ddblocal]).

## Decision

**1. The transaction.** For each event of an append to a stream that has a `SYNC` version
in the caller's catalog, the engine writes one `TransactWriteItems` in its region:

| Action | Item | Condition |
|--------|------|-----------|
| 0 | the event, with `_sync` | `attribute_not_exists(pk)` |
| per applied version | the whole row: states plus the event's contribution, `_v + 1`, `_hwm` and the `B` mark as read, `_ttl` | `_v` = the value read, or absent |
| per `uniqExact` value whose marker was absent when read | the marker, owner `E` + `event_id`, the row's `_ttl` | absent |

Every action asks for the stored item on failure. The rows and markers are read first,
consistently, with `GetItem` and `BatchGetItem`. `SYNC` forbids `ARRAY JOIN` and `SPLIT`,
so an event reaches at most one row per version and one value per `uniqExact` aggregate,
and DF3305 bounds the action count at 100.

A value counts only when its marker was absent at the read and this transaction writes it.
A present marker never counts, whatever its owner: an earlier attempt's marker committed
with its event, and a present `E` marker with this `event_id` belongs to a `DEDUP BY` event
appended again after its item expired, whose value is already counted.

**2. Live versions only.** An append applies the `SYNC` versions of its catalog (current
and retained, none the catalog lists as retired) whose state the engine's view-state cache
reads as `live`, the cache that queries use (at most 30 seconds old), read for each event of
a batch, so a long call follows retire's 30 seconds. It writes their IDs, sorted, as
`_sync`, a list of strings, absent when empty. With no such version the event is the plain
conditional `Put` of today. `Import` never applies `SYNC`. A state read that fails fails the
events not yet written.

**3. The projector skips by `_sync`, in the origin region only.** For an `INSERT` whose
`_r` is the projector's own region, the projector skips every version that `_sync` names,
before its fence and time-split rules. It never decides by its own reading of a version's
mode or state. Other regions ignore `_sync`. A `_sync` that doesn't decode breaks every
active version of the stream that the record reaches, in the origin region, whatever mode
the projector's catalog gives them: mode isn't in the fingerprint, so only the writer knows
what it applied. `_sync` is reserved for Dynaflux appends. The projector learns its
region from `AWS_REGION` (the runtime), the engine's region (`dynaflux dev`) or `local`
(`fluxtest`).

**4. Outcomes and retries.** The event's own condition failing decides as a plain append
does today: our own earlier attempt (same `_r`, `_it` and payload) is success, any other
stored event is `ErrDuplicate`. A row or marker condition failing returns the stored items;
the engine folds them in and retries at once. A conflict or throttling backs off with full
jitter and re-reads; either may follow an earlier attempt of the same call that was
applied, and the re-read then finds the event. An evaluation error, or a transaction over
4 MB, fails the append with nothing written. After about ten attempts the append fails with
a transient error naming the version and row. Batch appends stay one transaction per event,
in input order.

**5. Idempotency** holds on three layers: the backend sets the transaction's token itself
and resends an in-doubt transaction (a lost response, a 5xx,
`TransactionInProgressException`) with the same token and input until the outcome is known,
sending at least once 5 seconds or more after the first send; a resend without the token
finds our own event, and atomicity means our rows went in with it; the engine builds the
event item, `_sync` included, once per call.

**6. `_hwm` is copied unchanged** by a transactional row write, and the backfill's `B` mark
stays. No high-water mark entry is needed: "the event exists" and "its `SYNC` contributions
are in" commit together. The next projector write of the row prunes as usual.

**7. Hot rows.** The store reports a conflict as `ErrConflict` (still transient). The
projector re-reads a row and retries in place when its put meets a transaction, a few
times with jitter, instead of failing the batch's row.

**8. The storage boundary** gains, for MREC tables only:

```go
type TxPut struct {
	Table string
	Item  Item
	Cond  Cond
}

// Transact writes every put or none: tables of the store's region, distinct items.
Transact(ctx context.Context, puts []TxPut) error

// TxError is a transaction that a failed condition canceled. Conds lines up with the
// puts: the stored item of each put whose condition failed; nil elsewhere.
type TxError struct{ Conds []*ConditionError } // matches ErrConditionFailed

var (
	// Transient. The last attempt wrote nothing; an earlier one may have been applied.
	ErrConflict  = errors.New("store: conflict with a concurrent write")
	ErrThrottled = errors.New("store: throttled")
)
```

`ErrThrottled` comes only from `ProvisionedThroughputExceededException`,
`ThrottlingException`, `RequestLimitExceeded`, and cancellations whose reasons are only
`ThrottlingError`, `ProvisionedThroughputExceeded` or `None`.
`TransactionInProgressException` is in doubt, never a conflict or throttling.

`CheckTransact` refuses more than 100 puts and two puts on one item before sending, and
returns `ErrTooLarge` past 4 MB. There is still no `UpdateItem`, `DeleteItem`,
`ConditionCheck` or free condition.

**9. Queries over a `SYNC` version read strongly consistently**, as sql.md §9.7 says.

### Rejected

- **Applying `activating` versions in the transaction:** the archive and the stream table
  would have to carry `_sync`, and every backfill would need a skip rule.
- **The projector deciding by its own state or mode:** a stale cache or an older build
  would apply an event twice or never. `_sync` records what the writer did.
- **Skipping `_sync` versions in every region:** other regions never ran the transaction.
- **A `ConditionCheck` on present markers:** a marker leaves only with its row, by TTL,
  and the row's `_v` condition covers that.
- **`TransactGetItems` for the reads:** twice the cost; the `_v` condition already makes
  plain consistent reads enough.
- **Pruning `_hwm` in the writer:** it would read the unit items on every append.
- **`UpdateItem ADD` for additive states:** breaks ADR 0004's whole-item rule and still
  conflicts.
- **A string set (`SS`) for `_sync`:** the store would need a set type; Dynaflux never
  writes sets.
- **Parallel transactions in a batch append:** they would conflict on shared rows.

## Consequences

- Each version ID in `_sync` is applied exactly once in the origin region: by the
  transaction, which commits with the event, and never by the projector. Every other
  version, in every region, is the projector's as before. The archive needs no `_sync`:
  a `SYNC`-applied event appeared after its version went live, so it is above every fence
  and after every table scan.
- For up to 30 seconds after a version goes live, a writer with an older cache leaves it to
  the projector: exact, but read-your-writes starts once the cache sees `live`.
- Cost in the origin region per event with one `SYNC` row: one consistent read and two
  transactional writes at twice the price, against one write plus the projector's batched
  write. Cancelled transactions are billed; `explain`'s DF3501 warning says so. Latency is
  about two round trips more than a plain append.
- Applications that append to a stream with `SYNC` views need `dynamodb:GetItem`,
  `BatchGetItem` and `PutItem` on those view tables, by a per-view pattern since each new
  version is a new table; the documented policy allows `PutItem` only inside transactions
  (`dynamodb:EnclosingOperation`). They need `dynamodb:Query` on the meta table's `view`
  partition even if they never query. infra.md §9's "no
  role can call the transaction actions" becomes "no generated role writes in a
  transaction".
- DynamoDB Local can't show conflicts or the distinct-item rule; fakes and the simulation
  harness inject conflicts, and `CheckTransact` enforces distinct items.
- A region-string mismatch between the writer and the projector double-applies `SYNC`
  versions. `_r` is the region the caller configured, and the SDK rewrites the FIPS
  pseudo-regions (`fips-us-east-1`, `us-east-1-fips`) to the region it signs for, so `Open`
  and the CLI refuse a region that isn't canonical or contains `fips` (the hint names
  `AWS_USE_FIPS_ENDPOINT`). With a canonical name, SigV4's credential scope makes `_r` the
  region that stored the item. `dynaflux dev` refuses an event whose origin region isn't
  its own.
- A runtime from before M6 doesn't read `_sync`, so `regional.yaml` deploys before an
  application with `SYNC` views (lifecycle.md §2's order already) and never rolls back past
  M6 while one runs.
- The harness gains a `Transact` gate with transient, ambiguous, token-less retry,
  in-progress, applied-then-throttled and conflict faults (and applied-then-error faults on
  single puts), scenarios with stale caches across activation, mode flips and
  cross-region duplicates, and negative controls for each rule above.

[txhow]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html
[txapi]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
[txiam]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis-iam.html
[ddblocal]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html

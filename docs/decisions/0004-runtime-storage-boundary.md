<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0004: Keep every protocol rule above a storage interface of DynamoDB primitives

- Status: Accepted
- Date: 2026-10-26

## Context

The projector's correctness rests on a protocol validated only in simulation: per-row
versions and high-water marks, owner markers for `uniqExact`, the per-view breaker, the
fenced backfill handoff and the pruning of high-water marks by progress. The same protocol
must run on two backends: an in-memory store (M2, used by `fluxtest` and the simulation
harness) and DynamoDB (M3). The simulation harness guards every projector change, so it has
to run the code that production runs, not a model of it.

The harness also needs control of time, faults and scheduling. A research prototype ran every
region of a seed inside one `testing/synctest` bubble, with each store call parked at a gate
and released one at a time by a seeded scheduler. It was exact at 300 seeds × 3 regions in
5.3 s and replayed failing seeds exactly. It needed nothing from the code under test except a
`context.Context` on every store call and time taken only from `time.Now` and the context
deadline, both of which `synctest` fakes ([testing/synctest][synctest]).

Two shapes of storage interface were possible:

- **Semantic operations** (`AppendEvent`, `PutRow`, `AdvanceProgress`, `EnsureFence`, …).
  Fault injection is easy, but each backend implements every protocol condition, and the
  harness tests only the in-memory copy.
- **DynamoDB primitives with a closed set of conditions.** The protocol is written once,
  above the interface. A backend only translates a few calls, which an M3 conformance suite
  can check call by call.

Since June 2023 a failed conditional write can return the stored item
(`ReturnValuesOnConditionCheckFailure = ALL_OLD`) ([announcement][rvoccf]), so a lost race
needs no extra read. `PutItem` and `UpdateItem` are billed alike, at the larger of the item's
sizes before and after the write ([read and write operations][rwops]), and every versioned
item is already read before it is written.

## Decision

**The storage interface (`internal/store`) mirrors DynamoDB operations.** M2 has four:

```go
type Backend interface {
	Get(ctx context.Context, table string, k Key, consistent bool) (Item, error)       // nil when absent
	BatchGet(ctx context.Context, table string, ks []Key, consistent bool) ([]Item, error) // ≤ 100 keys, may repeat; aligned with ks
	Query(ctx context.Context, table string, q Query) iter.Seq2[Item, error]           // pages hidden; Limit is a total, PageSize the first page
	Put(ctx context.Context, table string, it Item, c Cond) error                      // *ConditionError on a failed condition
}
func IfAbsent() Cond          // attribute_not_exists(pk)
func IfVersion(v int64) Cond  // _v = v
```

- Errors fall in four classes, decided the same way by every backend:
  - A failed condition returns `*ConditionError{Current Item}` (matched by
    `ErrConditionFailed`), holding the stored item or nil.
  - `ErrTooLarge` comes only from checks made before a write is sent: `store.ItemSize`, an
    upper bound on DynamoDB's item size, and key lengths. It is never parsed from a
    response, where DynamoDB reports it as one of many `ValidationException`s.
  - `ErrInvalidRequest` covers every other `ValidationException`,
    `ResourceNotFoundException`, and the caller bugs both backends check before the call
    (more than 100 keys, crossed bounds, an invalid number or string, an oversized lookup
    key). DynamoDB rejects them on every retry, so they are never retried and never treated
    as a fact about the data.
  - Everything else is transient: throttling, 5xx, `TransactionConflictException` and
    `ReplicatedWriteConflictException`.
- `BatchGet` accepts repeated keys. DynamoDB rejects them
  ([BatchGetItem][batchget]), so the DynamoDB backend sends each key once and copies
  results back into place; callers don't deduplicate.
- There is no `UpdateItem`, `DeleteItem` or free-form condition expression. Items are always
  written whole.
- `Transact` (for `SYNC` views) arrives in M6 and `Scan` (table-source backfill) in M5, each
  with the same condition set. `Transact` and `_ttl` work only on MREC global tables: MRSC
  global tables support neither transactions nor TTL ([global tables][gtworks]), so
  strongly consistent streams, planned for a later version on MRSC tables, can't use `SYNC`
  or `LOG RETENTION` as designed.

**Every protocol rule lives above the interface,** once:

| Package | Owns |
|---------|------|
| `internal/projector` | The compiled catalog, compiled once per process, and the row codec that reads and writes share; the batch core: record kinds, evaluation, high-water marks, markers, pruning, breaker, progress and fences, backfill apply. |
| `internal/meta` | Unit items (progress, fences) and view-state items, as versioned read-modify-writes; the view-state transitions and the wait before units are listed for fencing. |
| `internal/engine` | Append (with dedup), stream reads and named queries over the projector's compiled catalog, used by the public packages and later by the CLI and query handler. It imports the projector, as M6's `SYNC` appends need. |
| `internal/keys`, `internal/agg` | The bytes of [encoding.md](../spec/encoding.md). |
| `internal/expr` | The expression evaluator. |

**The projector core takes plain values and a context:**

```go
func (c *Core) Handle(ctx context.Context, b Batch) (Result, error) // Batch{Stream, Unit, Records}
```

- The deadline comes from `ctx.Deadline()`; a context without a deadline is refused, and
  nothing is sent after it. The M3 adapter sets it to its write cutoff, so the core has no
  cutoff setting. Time comes from `time.Now`. There is no clock interface.
- The core passes the invocation's context to every store call, keeps no package-level state,
  orders store calls deterministically, holds no mutex across a store call, and starts no
  goroutine that outlives `Handle`.
- The AWS adapter (M3) turns a Lambda tumbling-window event into a `Batch`
  ([ADR 0003](0003-shard-identity-from-tumbling-window.md)), so neither the SDK nor
  `aws-lambda-go` types reach the core.

**Backends.** `internal/store/mem` keeps items in memory with DynamoDB's semantics where
Dynaflux depends on them: byte order of string keys, conditions that return the stored item,
size limits, numbers stored in DynamoDB's form (no leading or trailing zeros), and the
requests DynamoDB rejects ([data types][types]). It has no change feed: `fluxtest` projects
through a small decorator over the backend, and the harness models change streams itself.
`internal/store/dynamo` (M3) makes one SDK call per operation. A shared conformance suite,
`internal/store/storetest`, runs against both.

**Faults stay out of production code.** The harness wraps `Backend` with its own scheduler,
fault injection and deadline kill. One fault has the shape of an SDK retry after a lost
response: the write is applied, then the call fails with a `*ConditionError` holding the
caller's own write ([PutItem][putitem], [retries][sdkretry]); `storetest.RetriedWrite`
offers it to unit tests. Negative controls that change code are applied at build time with
`go test -overlay`; production code has no build tags or test hooks for them.

**Public packages reach internal values through hooks.** `internal/access` holds function
variables that the root package sets in `init`, so `fluxgen`, `fluxtest` and, from M3, the
runtime package can build a `*dynaflux.Store` around an internal engine without exporting a
constructor that takes internal types. grpc-go uses this pattern
([grpc-go internal][grpcinternal]).

Rejected:

- Semantic storage methods: the protocol would be written twice and tested once.
- `UpdateItem` with condition expressions on `progress` and fences: DynamoDB would evaluate
  part of the protocol, which the in-memory store would have to re-implement exactly, for no
  saving.
- An injected clock: `synctest` already fakes `time.Now`, timers and context deadlines.
- Build-tag or exported test hooks for negative controls: they ship in production builds and
  drift from the mistakes they stand for.

## Consequences

- A backend is small: the in-memory prototype took about 250 lines. M3's DynamoDB backend
  maps each call to one SDK call and its errors to the four classes.
- The harness, `fluxtest` and production run the same projector, engine and encodings.
- Protocol conditions are checked in Go on the item a read or a failed write returned, then
  enforced by `_v`. DynamoDB itself evaluates only `attribute_not_exists(pk)` and `_v = :v`,
  which keeps the M3 emulator checks short.
- Every write sends the whole item. That costs what an update costs, but large items cost
  more to rewrite, which keeps item size (high-water marks, sketches) a design concern.
- Rules the harness found that this layout must keep: a broken version counts as covered in
  the fence check of a progress advance, or a version that breaks while it holds a fence
  blocks its stream forever; `broken` is final, so a backfill can't turn it into `live`;
  units are listed for fencing only after every projector has seen `activating`, which
  `meta` enforces; and `retire` (M5) must delete the retired version's fences from every
  unit item.
- A run-time `ErrTooLarge` on a row isn't a fact about the events: `_hwm` and the spread of
  a `SPLIT` row differ by region. So the projector never opens a breaker for it; `validate`
  bounds every other part of a row item
  ([encoding.md §7.8](../spec/encoding.md#78-size-bound)).
- `internal/engine`, `internal/expr`, `internal/meta`, `internal/access` and `fluxgen` are
  new packages; `internal/meta` arrives in M2 rather than M5, holding only what the
  projector and the backfill need.

[synctest]: https://pkg.go.dev/testing/synctest
[rvoccf]: https://aws.amazon.com/about-aws/whats-new/2023/06/amazon-dynamodb-cost-failed-conditional-writes/
[rwops]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html
[grpcinternal]: https://github.com/grpc/grpc-go/blob/master/internal/internal.go
[batchget]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
[gtworks]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html
[types]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
[putitem]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
[sdkretry]: https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-retries-timeouts.html

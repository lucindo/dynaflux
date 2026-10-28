<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0012: Only a backfill job activates a view version; guards stop older code, and a fresh stream goes live without a read

- Status: Accepted. Amends [ADR 0004](0004-runtime-storage-boundary.md), whose decision stands:
  `retire` no longer deletes fences.
- Date: 2026-10-26
- Amended by [ADR 0017](0017-archive-completeness.md): §4's stream origin is recorded in a
  stream item, with archive holes and gap evidence

## Context

Each view version has a state in each region: `pending_backfill` (no item), `activating`,
`live` or `broken` ([encoding.md §8.2](../spec/encoding.md#82-view-state-item)). The
projector applies an idempotent version to every event at once, and an additive one only
from `activating` on, above each delivery unit's fence. M2 built the states and the fenced
handoff; M5 adds the commands that move them. Four problems are open:

1. **Nothing moves a deployed version to `live`.** On a brand-new store the views have no
   history, yet an additive view gets no event until something activates it, and queries
   can't tell an empty view from a complete one. Something has to mark such versions
   `live`, and only when that is provably safe.
2. **Older code during activation.** The handoff's proof (spike S1b) assumes every
   projector knows the version. After a deploy, invocations of the previous build keep
   running for up to `W` − 60 s; a rollback, or an older build deployed later, runs without
   the version for good. The spike listed both as "not simulated". M2's design answered
   with a rule for M5: take the backfill's cut only after every container runs a catalog
   with the version, plus one invocation lifetime, about 16 minutes of waiting on every
   backfill.
3. **Idempotent versions have no fence**, so an older invocation can record `progress` past
   events it didn't apply to the version, and an archive cut taken right after activation
   misses them.
4. **Retiring a version.** ADR 0004 says `retire` must delete the version's fences from
   every unit item, or a projector whose catalog no longer has the version fails every
   batch on them. Deleting them can't be ordered safely: before the deploy that drops the
   version, projectors re-create the fences; after it, they are already blocked.

Facts the decision relies on:

| # | Fact | Source |
|---|------|--------|
| a | An event's `_it` is the writer's clock at append, so no event predates its table by more than clock skew. | `engine`; `CreationDateTime` ([TableDescription][td]) |
| b | `_ttl` ≥ `_it` + 1 day (`LOG RETENTION` ≥ 1 DAY, DF2109), and TTL deletes only items whose expiry has passed. | sql.md §8.6; [TTL][ttl] |
| c | Only Dynaflux writes a stream table, and it never deletes an event. | encoding.md §1 |
| d | When a record appears in a replica's stream, its item is already in that replica; every replica's stream holds every write to it, local or replicated. | spike S2; [global tables][gt] |
| e | The stream of a multi-region replica can't be disabled. | [global tables][gt] |
| f | A consistent `Scan` sees every write completed before it began. | [Scan][scan] |
| g | A replica added to a table starts with a copy of the existing items; whether that copy passes through its stream isn't documented. | [adding replicas][addreplica]; infra.md §16 |

DynamoDB Local returns `CreationDateTime`, an empty `Replicas` list and the `Scan` results
the checks below need.

## Decision

**1. States and who moves them.**

- A **backfill job** ([ADR 0011](0011-backfill-coordinator.md)) is the only path from
  `pending_backfill` to `activating` and from `activating` to `live`. The write that sets
  `activating` also records the version's split (`fence`, `time` or `none`), which no later
  job can change.
- The **projector** and a **backfill job** move any state but `retired` to `broken`; the
  first failure is kept, with a cause: `record` (an evaluation error) or `split` (an event
  that a time split can't place, [ADR 0011](0011-backfill-coordinator.md)).
- **`dynaflux retire`**, and a successful deploy of a catalog that lists the version as
  retired (§5 below), move any state to a new, final state, **`retired`**, stamping
  `retired_at`.
- Nothing moves back. Deploying templates changes no other state: the setup function stays
  out of activation, because the checks below read every replica region, and a deploy
  must not hang on an impaired region or start reading an archive as a side effect.

**2. Guards.**

- **G1, deployed code first (a pre-check).** A job activates a version only when the
  region's deployed catalog defines it. The deployed catalog is the `catalog`/`current`
  item that the setup function registers after every function of the stack was updated.
- **G2, fence every version that reads an archive.** With an archive source, the job fences
  every listed delivery unit for the version, idempotent versions too. Live projection still
  applies an idempotent version to every event; the backfill keeps archived records at or
  below the fence. An older invocation then can't record progress past the fence, because
  it can't account for the version.
- **G3, older code fails.** A projector fails the whole batch when the view states it read
  hold an `activating` or `live` version of the batch's stream that its catalog neither
  defines nor lists as retired. State items record their stream, and the projector reads
  the states for every batch with an `INSERT` or a `MODIFY`, cached for 30 seconds as
  today: a `MODIFY` can be the only evidence that a time split was straddled, and older
  code would consume it unchecked. Unknown `broken` and `retired` items are ignored.

G3 is the safety and G1 only spares the stream a block. Together with the 90-second listing
delay that `meta.FenceUnits` already enforces, they replace M2's wait:

- an invocation that started before the activation either recorded progress before the
  fences, so its events are at or below them and in the archive before the cut, or meets
  a fence it can't account for and fails, so its batch is retried by code that applies the
  version;
- every batch that starts after the listing delay read states that show the version, so it
  applies the version or fails under G3.

The cut is the time `FenceUnits` returns, for every version. A table-source job reads no
earlier than the listing delay after activation (idempotent) or after its split time's
bounds ([ADR 0011](0011-backfill-coordinator.md)).

**3. The fresh-stream rule.** A job with an archive source moves its version from
`activating` to `live` without reading any source when, after `FenceUnits` returned:

| # | Check |
|---|-------|
| F1 | Every fence of the version that `FenceUnits` wrote or found is the zero fence. |
| F2 | The stream table is new: the scan of F3 ends before `born` + 23 h, where `born` is the earliest `CreationDateTime` that `DescribeTable` returns for the table in any of its replica regions; every replica answers, and none reports a `RestoreSummary`. |
| F3 | A `Scan` of the region's replica with `ConsistentRead`, sent after F1, paged until it finds an item or ends, finds no item. |

Proof:

1. By F2, (a) and (b), no event had expired from any replica when the scan ended; by (c),
   nothing else removed one.
2. By F3 and (f), the region's replica held no event when the scan began, and by step 1 it
   never held one; by (d), no `INSERT` had appeared in the region's stream.
3. So every `INSERT` of the region's stream is delivered in a batch that started after the
   scan, more than the listing delay after activation. That batch's states show the version:
   if its code defines the version, it fences the unit before applying anything and applies
   the version above the fence; if not, G3 fails it.
4. `progress` rises only after a batch with an `INSERT`, so every fence is the zero fence
   (F1 for the listed units; a later unit's first batch fences it at zero). Every record is
   applied live, and the backfill's set is empty.

When a check fails or a call fails, the job continues with the normal backfill from the
same cut, which reads the archive (on AWS, from M6); the rule only skips steps. It refuses a
region added later (its replica holds the copied events), a re-created regional stack on a
used stream, and any store whose application already wrote. A table-source job needs no
rule: its scan of an empty table finds nothing.

**4. Stream origin.** The same two checks, F2 and F3, prove that the region's change stream
carries every event the stream will ever have. In M5 they are used only in the moment: the
`backfill` command chooses the archive source for a stream that passes them and that the
region archives with `events: all`, and the job's fresh rule repeats them after the fences.
Recording the fact, so that a later version can trust the region's archive, comes with the
archive read in M6, under two rules this design found: the record is valid only while the
region's registered catalog keeps the exact handoff it had then, and any change of that
handoff, recorded before the functions change, is a hole for good. Fact (g) is why a region
added later never gets it.

**5. Retire.** `retire` runs its checks and sets `retired` in every region of the one
environment it is given, on every run, and marks the version `retired` in `dynaflux.lock`.
The catalog gains the store's retired version IDs. The projector neither applies nor
fences a retired version, and counts a fence of a version as covered when the states call
it retired. A projector whose catalog lists a version as retired sets it `retired` when it
finds it `activating` or `live`, before it skips any of its events, so no region serves a
version as `live` that a build skipped ([ADR 0013](0013-deployed-catalog-registration.md)).
The setup function also sets `retired` for every version the registered catalog lists, in
the cleanup phase of a successful update (the catalog resource is replaced per catalog
hash, so its old instance's `Delete` comes only then), so an environment where `retire`
didn't run gets its tombstones from the deploy that drops the tables; that resource retires
nothing on a rolled-back update. No fence is deleted: fences of
a retired version stay on unit items written before, and expire with them. This replaces
ADR 0004's consequence that `retire` must delete the retired version's fences from every
unit item.

**6. Queries** read the state of the version they target through one cache per meta table,
the same type the projector uses (at most 30 seconds old, wall clock). Every state but
`live` returns `ErrViewNotReady`, naming the version, state and region; `AllowPartial()`
skips the read.

**Rejected:**

- **Setup marks versions `live` on a stack's first `Create`** (or a
  `Custom::DynafluxActivate` starting jobs): a cross-region read at deploy time, old code in
  flight on an update, and a second path to `live`. A store that was written to before its
  regional stacks existed (a region added later, a re-created stack) would be marked live
  wrongly unless the resource repeated every check here.
- **A first-deploy flag on `backfill`:** the operator would assert what F1–F3 prove.
- **G1 through a catalog fingerprint in each function's environment and
  `GetFunctionConfiguration`:** one more variable and grant; the registered catalog already
  answers it, and works in `dev`.
- **Heartbeat items from every container** as evidence for M2's wait: a meta write path on
  every container, for a guarantee G3 gives at no cost.
- **Waiting `activated_at` + 90 s + `W` before the cut** for idempotent versions: about
  16 minutes on every backfill, and still unsafe against a rollback without G3.
- **Deleting fences on retire** (ADR 0004 as written): can't be ordered with the deploy, and
  an environment someone forgets blocks every shard of the stream.
- **Serving broken versions by default, or a separate `ErrViewBroken`:** a broken view's
  rows are no longer exact; `AllowPartial` serves them when an application wants that.

## Consequences

- After the first deploy, and after adding a stream, operators run `dynaflux backfill -all`;
  on a store without events it finishes in about two minutes in regions that archive with
  `events: all`, and in about 30 minutes elsewhere. M4's e2e scenario 1 stops calling
  `meta.SetLive`, which remains for `fluxtest` and the harness; `dev` activates through the
  same job with short bounds.
- A rollback to a build without an active version blocks that version's stream until a
  build that defines it is deployed again; `retire` can't help, because the version is
  current in the newer build. The lag alarm fires, and `status` names the version, the
  build to deploy and the time records start to be lost. infra.md warns against rolling a
  regional stack back past an active version.
- G3 needs every projector build since M5. No earlier build was released.
- The harness gains cores running an older catalog that switch at a random time, with
  controls for G2 and G3 (one with `MODIFY`-only batches); a `fresh` scenario with
  controls that skip F3, run it before the listing delay, or accept a nonzero fence; and a
  `retire` scenario, with an older build redeployed afterwards, whose controls skip the
  projector's tombstone or ignore a retired state.
- encoding.md §8.2 gains `retired`, `stream`, `split`, `split_at`, `split_late`,
  `partial_since`, `retired_at` and the failure's `cause`. The catalog gains `retired`;
  slim retired entries in the lock answer ADR 0007's open question. The plan's "the
  projector applies live events to them immediately" becomes true only of idempotent
  versions, as the spec already says.

[td]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableDescription.html
[ttl]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html
[gt]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html
[scan]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
[addreplica]: https://aws.amazon.com/about-aws/whats-new/2019/11/you-can-now-enhance-availability-of-your-existing-amazon-dynamodb-tables-by-adding-global-replicas

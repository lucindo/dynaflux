<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0002: Number view versions with a committed lock file

- Status: Accepted
- Date: 2026-10-25
- Amended by [ADR 0007](0007-lock-file-format.md): lock format details

## Context

A deployed view version never changes. When a view's definition changes, the new definition
becomes the next version, for example `page_daily@v2`, with its own table. The change rolls
out gradually: the projector fills v2 while the deployed app still queries v1. v1 stays until
an operator runs `dynaflux retire page_daily@v1`.

`dynaflux generate` writes the Go code, the runtime catalog and the infrastructure templates.
For each view it needs:

1. **The number of the current version.** Table names are deterministic and include it:
   `{prefix}{store}-view-{name}-v{N}`, for example `prod-analytics-view-page_daily-v2`. The
   CLI finds every resource from the repository alone, with no lookup service.
2. **The definition of every older version that isn't retired.** The projector keeps
   maintaining v1 while v2 fills, so the catalog needs v1's definition. The templates must
   keep v1's table, or the next deploy deletes it while the app still reads it.

The SQL files can't supply either one. They hold only the current definition, with no
number. And `generate` is a build step, like sqlc: it reads only the repository, needs no
AWS credentials or network access, and gives the same output for the same inputs.

## Decision

`dynaflux generate` maintains `dynaflux.lock`, a generated file that users commit next to
`dynaflux.yaml`, as they do with `go.sum`. Users don't edit it by hand.

The lock has a `format` field that versions its layout. For each view, the lock lists its
versions. Each version has:

| Field | Meaning |
|---|---|
| `n` | The version number, starting at 1. Never reused, even after the version is retired. |
| fingerprint | SHA-256 over the canonical JSON of everything that determines the view's stored data: the source stream, `ARRAY JOIN`, `WHERE`, the keys and how many of them form the `PRIMARY KEY`, `SPLIT`, the aggregates and their state formats, `TTL`, and the encoding and storage format versions. It ignores `MODE`, capacity, comments, Go names, formatting and source positions. |
| state | `current` (the definition in the SQL files), `retained` (an older version that is still deployed) or `retired` (removed, and kept so its number is never reused). |
| definition | The version's definition as catalog `ViewVersion` JSON, including `MODE`, which can change without a new number. The catalog uses it to keep a retained version running after its SQL has changed. |

Rules:

- When the current definition's fingerprint isn't in the lock, it gets the highest number
  plus 1 and becomes `current`. The previous current version becomes `retained`.
- When it matches a `retained` version, that version becomes `current` again, with no new
  number and no backfill, and the previous current version becomes `retained`.
- When it matches only a `retired` version, it gets a new number, as in the first rule:
  numbers are never reused.
- The catalog contains every version that isn't retired. Queries bind to the current one.
- The lock is JSON with sorted keys and a fixed layout, so the same inputs always give the
  same bytes.

One view over its life:

| Step | `page_daily` in the lock | View tables after deploy |
|---|---|---|
| First `generate` | v1 current | `…-page_daily-v1` |
| Change the view, run `generate` | v1 retained, v2 current | `…-v1`, `…-v2` |
| `dynaflux retire page_daily@v1`, then `generate` | v1 retired, v2 current | `…-v2` |

### Merge conflicts are a feature

Two branches that each change `page_daily`, now at v2, both add v3 with different
fingerprints. They edit the same lines of the lock, so git reports a conflict. That is the
point: two definitions must never share one table name. To resolve it, keep the lock from
the branch that merged first, because its v3 may already be deployed, then run
`dynaflux generate` on the merged SQL. A definition that differs from v3 gets v4. Branches
that change different views edit different parts of the lock and merge cleanly.

### Alternatives considered

- **A `VERSION n` clause that users bump in the SQL.** Rejected:
  - A forgotten bump puts a changed definition under a number that is already deployed. To
    catch that, the compiler needs the old fingerprint, which is the lock again.
  - Retained versions still need their old definitions, so users would keep copies of old
    views in their SQL files until retirement.
  - Two branches that both change `VERSION 2` to `VERSION 3` make the same edit, which git
    merges without a conflict. If their other edits touch different lines, the merge
    silently produces a third definition under v3.
- **Read the deployed catalog from the meta table.** `generate` would ask AWS which versions
  exist. Rejected:
  - `generate` would need AWS credentials and network access, so no offline builds, and CI
    would need read access to production.
  - There is no single answer to read. Each region has its own meta table, and one
    repository deploys to several environments (prefixes such as `prod-` and `staging-`)
    whose deployed versions differ. The generated code is one build for all of them.
  - The output would depend on what is deployed, not on the commit, so builds wouldn't be
    reproducible.
  - On the first deploy there is no meta table to read.
- **Name tables by fingerprint instead of by number.** This needs no stored state for
  numbering, but table names become hashes that operators must type in `backfill` and
  `retire`, and retained versions still need their definitions from somewhere. Rejected.

## Consequences

- One more generated file to commit. A version change shows up in code review: a new
  `current` entry means a new table and a backfill.
- The lock is the repository's only record of version numbers. Deleting or reverting it
  renumbers views, and the next deploy would drop tables and create empty ones.
  `dynaflux diff` (M5) must report a lock that disagrees with the deployed catalog before
  such a deploy.
- **M1:** `validate` and `explain` don't read or write the lock. Every view is v1.
- **M2 (`generate`):** reads and writes the lock, computes fingerprints, assigns numbers and
  emits every non-retired version into the catalog and the templates. It must refuse a lock
  it can't parse, such as one with conflict markers, instead of starting over at v1.
- **M5 (`retire`):** checks in the meta table that no running catalog has queries that
  still target the version, then marks it `retired` in the lock. The user regenerates and
  deploys, and the infrastructure tool deletes the table. Every environment deploys from the
  same lock, but each has its own meta tables, so M5 must decide how `retire` checks every
  region and every environment.

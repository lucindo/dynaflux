<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0007: Format details of `dynaflux.lock`

- Status: Accepted. Amends [ADR 0002](0002-view-version-lock-file.md), whose decision stands.
- Date: 2026-10-26

## Context

ADR 0002 decided that `dynaflux generate` numbers view versions in a committed lock file. M2
implements it. A prototype of the lock (format, parser and numbering algorithm, with tests)
found details that ADR 0002 leaves open or states differently:

1. ADR 0002 names the number field `n`; the catalog's `ViewVersion` JSON, which the lock
   stores, names it `number`.
2. A clean git merge can leave two `current` versions of one view: branch A adds v3 while
   branch B reverts to v1. Both branches change v2's state line the same way, so git reports
   no conflict.
3. Retired entries appear only through `dynaflux retire`, which arrives in M5. Until then no
   lock can hold one.

## Decision

- **Layout:** indented JSON (two spaces), a `format` field (1), views sorted by name, and
  each view's versions **newest first**, so adding a version inserts lines at the top of its
  view and branches that change different views merge cleanly.
- **Entries:** every entry is its catalog `ViewVersion` JSON, including `number`, `state`,
  `fingerprint` and `mode`. In M2 a state is `current` or `retained`; `retired` is a DF6013
  error until M5 adds `retire`, which also decides the shape of a retired entry.
- **Reading is strict:** unknown fields, an unknown `format`, duplicate numbers, and a
  definition that doesn't match its fingerprint (a hand edit) are errors. Every lock error is
  DF6013 and stops `validate` and `generate`; there is never a fallback to v1.
- **Two `current` versions are repaired:** numbering first marks every `current` entry
  `retained`, then the SQL picks which version is current. Nothing is dropped, so the repair
  needs no question to the user.
- From M2, `validate`, `explain` and `generate` all read the lock, so they agree on numbers;
  only `generate` writes it. `validate` adds two checks that keep the projector from failing
  on a retained version: its stream is gone, or it reads a column the stream no longer has
  with that type. The hint is `dynaflux retire view@vN`.

## Open questions

- **Storage formats.** A new storage format changes the fingerprint of every view, which
  would renumber every view and force a backfill of each. One answer: a definition matches a
  locked version when their fingerprints are equal after computing the definition's
  fingerprint with that version's storage format. Nothing changes while format 1 is the
  only one, so this is decided, implemented and tested with the first format bump.
- **Retired entries** (M5): whether they keep the whole definition or only `number`,
  `state` and `fingerprint`, which is all numbering needs.

## Consequences

- About 50 lock lines per version that isn't retired.
- The one merge that still conflicts is the one ADR 0002 wants: two branches adding a version
  to the same view.
- [sql.md §9.9](../spec/sql.md#99-view-versions) states the lock rules, and
  [config.md](../spec/config.md#resource-names) checks the resource names of every version
  in the catalog and defines DF6013.

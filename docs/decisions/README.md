<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Architecture decision records

Each file records one decision that shapes Dynaflux, so readers can see what we chose and
why. Files are named `NNNN-title.md`, numbered in order. Each record starts with its status
and date, then has three sections: **Context** (the problem and the evidence), **Decision**
(what we chose, and the alternatives we rejected) and **Consequences** (what follows, good
and bad). To change a decision, write a new record that supersedes the old one, and set the
old one's status to `Superseded by NNNN`. A record that settles details of an older one,
whose decision stands, amends it instead: the older record gets the line
`Amended by NNNN`.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-sql-parser.md) | Hand-written parser for the SQL dialect | Accepted |
| [0002](0002-view-version-lock-file.md) | Number view versions with a committed lock file | Accepted, amended by 0007 |
| [0003](0003-shard-identity-from-tumbling-window.md) | The projector learns its shard from a Lambda tumbling window | Accepted |
| [0004](0004-runtime-storage-boundary.md) | Keep every protocol rule above a storage interface of DynamoDB primitives | Accepted, amended by 0010, 0012 and 0014 |
| [0005](0005-public-go-value-types.md) | Go types for SQL values in generated code | Accepted, amended by 0019 |
| [0006](0006-generated-code-contract.md) | Generated code calls six generic helpers in package `fluxgen` | Accepted, amended by 0019 |
| [0007](0007-lock-file-format.md) | Format details of `dynaflux.lock` | Accepted |
| [0008](0008-archive-schema-migration.md) | A custom resource served by the runtime binary migrates the archive schema | Accepted, amended by 0013 |
| [0009](0009-emulator-choice.md) | DynamoDB Local runs every integration test | Accepted |
| [0010](0010-runtime-entry-point.md) | `runtime.Run` starts every function, and the projector core stops sending before Lambda's deadline | Accepted, amended by 0019 |
| [0011](0011-backfill-coordinator.md) | Backfills run as a resumable step, driven by Step Functions, with their state in the meta table | Accepted, amended by 0016 |
| [0012](0012-view-version-activation.md) | Only a backfill job activates a view version; guards stop older code, and a fresh stream goes live without a read | Accepted, amended by 0017 |
| [0013](0013-deployed-catalog-registration.md) | The setup function registers the deployed catalog from its own binary | Accepted |
| [0014](0014-sync-views-in-the-append-transaction.md) | `SYNC` views are applied in the append's transaction, and the origin region's projector skips them by `_sync` | Accepted |
| [0015](0015-wait-tokens-and-receipts.md) | Wait tokens poll receipts that the origin region's projector writes for appends that ask for them | Accepted, amended by 0019 |
| [0016](0016-archive-export-with-athena.md) | A backfill reads the archive with sliced Athena `UNLOAD`s, after a `DataFreshness` watermark, plus the error prefix | Accepted |
| [0017](0017-archive-completeness.md) | A region's archive is trusted for a backfill only from a recorded origin, with no hole and no gap evidence since | Accepted |
| [0018](0018-query-handler-and-front-doors.md) | One query handler serves every front door, from a third template deployed with the application | Accepted |
| [0019](0019-v1-compatibility-promise.md) | v1.x keeps every shipped boundary compatible, not only the Go API | Accepted |

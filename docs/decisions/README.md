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
| [0004](0004-runtime-storage-boundary.md) | Keep every protocol rule above a storage interface of DynamoDB primitives | Accepted, amended by 0010 and 0012 |
| [0005](0005-public-go-value-types.md) | Go types for SQL values in generated code | Accepted |
| [0006](0006-generated-code-contract.md) | Generated code calls six generic helpers in package `fluxgen` | Accepted |
| [0007](0007-lock-file-format.md) | Format details of `dynaflux.lock` | Accepted |
| [0008](0008-archive-schema-migration.md) | A custom resource served by the runtime binary migrates the archive schema | Accepted, amended by 0013 |
| [0009](0009-emulator-choice.md) | DynamoDB Local runs every integration test | Accepted |
| [0010](0010-runtime-entry-point.md) | `runtime.Run` starts every function, and the projector core stops sending before Lambda's deadline | Accepted |
| [0011](0011-backfill-coordinator.md) | Backfills run as a resumable step, driven by Step Functions, with their state in the meta table | Accepted |
| [0012](0012-view-version-activation.md) | Only a backfill job activates a view version; guards stop older code, and a fresh stream goes live without a read | Accepted |
| [0013](0013-deployed-catalog-registration.md) | The setup function registers the deployed catalog from its own binary | Accepted |

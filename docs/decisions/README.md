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
| [0004](0004-runtime-storage-boundary.md) | Keep every protocol rule above a storage interface of DynamoDB primitives | Accepted |
| [0005](0005-public-go-value-types.md) | Go types for SQL values in generated code | Accepted |
| [0006](0006-generated-code-contract.md) | Generated code calls six generic helpers in package `fluxgen` | Accepted |
| [0007](0007-lock-file-format.md) | Format details of `dynaflux.lock` | Accepted |

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
old one's status to `Superseded by NNNN`.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-sql-parser.md) | Hand-written parser for the SQL dialect | Accepted |
| [0002](0002-view-version-lock-file.md) | Number view versions with a committed lock file | Accepted |

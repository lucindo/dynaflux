<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0001: Hand-written parser for the SQL dialect

- Status: Accepted
- Date: 2026-10-25

## Context

Dynaflux compiles a small, ClickHouse-flavored SQL dialect, specified in
[`docs/spec/sql.md`](../spec/sql.md). It has three parts:

- DDL that ClickHouse doesn't have: `CREATE STREAM` with `DEDUP BY`, `LOG RETENTION`,
  `ACCEPT`, `SHARDS` and `CONSISTENCY`, and `CREATE MATERIALIZED VIEW` with `MODE` and
  `SPLIT`.
- A `SELECT` subset: `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, `ARRAY JOIN`, a fixed list of
  functions, and `{name:Type}` parameters.
- sqlc-style annotation comments: `-- name:`, `-- expose:` and `-- go:type`.

Anything outside the subset must fail with `file:line:col`, an error code and a hint.

We tested `github.com/AfterShip/clickhouse-sql-parser` v0.5.6 (latest tag, 2026-08-11)
against a corpus with every construct in the dialect, 10 realistic syntax errors and a fuzz
target. We re-ran the key cases on master (`92df70a`, 2026-10-08).

| Criterion | Finding |
|---|---|
| Coverage of `SELECT` and expressions | Everything in our subset parses, with correct precedence in all 12 precedence cases we checked. The library also accepts a large superset: its AST has 196 node types. |
| Coverage of our DDL | None of it parses. `CREATE STREAM` fails at the second token. `CREATE MATERIALIZED VIEW` fails at `MODE`, because ClickHouse requires `TO` or `ENGINE` there. |
| Room for our own DDL | The only entry point parses a whole string (`ParseStmts`). No API parses an expression, a `SELECT` or a type starting at an offset. To delegate one clause, our own tokenizer must find where it ends; otherwise the parser reads the next clause keyword, such as `SPLIT` in `TTL day + INTERVAL 2 YEAR SPLIT 8`, as an implicit alias. |
| Comments | The lexer drops them, so annotations need our own scanner. |
| AST fit | String literals keep their source escapes (`'it''s'` comes back as `it''s`). `NULL` and `true` are identifiers. `DISTINCT`, `FINAL`, `GLOBAL` and `UNION` are flags on nodes we would allow. Rejecting everything outside the subset needs a deny-by-default walk over about 30 node types and about 30 fields. |
| Error messages | Parsing stops at the first error in a file. Of the 10 realistic mistakes, 4 messages were good or OK, 4 were vague and 2 were misleading: `FORM page_daily` reports an unexpected `page_daily`, because `FORM` was read as an alias. Messages can't carry our hints. |
| Robustness | Fuzzing found 2 nil-pointer panics, the first after about 20 seconds. Both are reachable from half-typed queries such as `SELECT a FROM t ORDER BY day AS`, and both are still on master. |
| Maintenance | Active: 112 commits, 12 releases and 21 authors in the last 12 months. But it is pre-1.0, and the AST still moves: in v0.5.6, lowercase `array join` parses to a different node shape than `ARRAY JOIN`, and an unreleased fix on master changes it. Any upgrade can change what our checks see. |
| License | MIT, compatible with Apache-2.0. The runtime package imports only the standard library. |

Estimated size for the exact subset:

| Option | Production lines of code |
|---|---|
| Hand-written lexer, parser, AST and diagnostics | 1,300–1,600 |
| The library for `SELECT` and expressions, plus our tokenizer, DDL grammar, AST, position remapping and deny-by-default converter | 1,200–1,550, plus the dependency |

The library would replace only our `SELECT` and expression grammar, about 400 lines. The
converter from its AST to ours costs about as much.

## Decision

Write our own lexer and recursive-descent parser in `internal/sql`, using precedence
climbing for expressions. It accepts exactly the grammar in `docs/spec/sql.md`. It:

- recognizes annotations in the lexer and attaches them to the next statement
- unescapes string literals once, in the lexer
- records a position on every AST node
- recovers at `;`, so one run reports every bad statement in a file
- recognizes common out-of-subset keywords (`JOIN`, `UNION`, `WITH`, `HAVING` and others)
  and reports them with a hint

Dynaflux takes no dependency on `AfterShip/clickhouse-sql-parser`.

### Alternatives considered

- **The library for `SELECT` and expressions, our own grammar for DDL.** Rejected. It isn't
  smaller (see the size table), every call needs a `recover()` because of the panics, and
  users would see two styles of error message.
- **Reshape our DDL into ClickHouse syntax**, for example `ENGINE = Dynaflux` and
  `SETTINGS mode = 'async', split = 8`. The library parses that, but clauses become strings,
  and `ACCEPT` can't be an expression because `SETTINGS` takes only literals. Rejected: the
  dialect is designed for users, not for a parser.

## Consequences

- We own about 1,500 lines of parser, plus tests, golden files and a fuzz target from the
  first commit.
- Syntax errors can teach, in one style across DDL and queries.
- The AST is shaped for `internal/catalog` and `internal/validate`, and no dependency upgrade
  can widen the accepted language.
- We must match ClickHouse precedence and lexical rules ourselves. ClickHouse documents no
  full precedence table, only that binary operators are left-associative, so a golden
  precedence table in our tests pins the behavior.
- Clause keywords are reserved only where a clause can start, so column names such as `day`,
  `mode`, `order` and `key` keep working.
- A possibility, not a commitment: a differential test that uses this library, or
  `clickhouse local`, as an oracle for expression parsing. It would live in a nested,
  test-only Go module, so the library never enters Dynaflux's `go.mod`.

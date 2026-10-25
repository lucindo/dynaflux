<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Dynaflux SQL dialect

This page is the normative specification of the SQL that Dynaflux compiles: streams,
materialized views and named queries. The parser (`internal/sql`), the compiler
(`internal/validate`) and their golden tests implement exactly what it says. Anything this
page doesn't allow is an error, never a silent fallback.

- Dialect version: 1 (Dynaflux v1).
- Reference ClickHouse version: 26.7.
- Related: [config.md](config.md) (`dynaflux.yaml`) and `docs/spec/encoding.md` (key and
  value encoding, written in M2).

## Contents

- [1. Overview](#1-overview)
- [2. Files and statements](#2-files-and-statements)
- [3. Lexical structure](#3-lexical-structure)
- [4. Types](#4-types)
- [5. Expressions](#5-expressions)
- [6. Scalar functions](#6-scalar-functions)
- [7. Aggregate functions](#7-aggregate-functions)
- [8. CREATE STREAM](#8-create-stream)
- [9. CREATE MATERIALIZED VIEW](#9-create-materialized-view)
- [10. Named queries](#10-named-queries)
- [11. Deviations from ClickHouse](#11-deviations-from-clickhouse)
- [12. Worked examples](#12-worked-examples)
- [Appendix A. Diagnostics](#appendix-a-diagnostics)

## 1. Overview

A Dynaflux project has three kinds of statement:

| Statement | Defines |
|-----------|---------|
| `CREATE STREAM` | An append-only stream of immutable events, stored in a DynamoDB Global Table. |
| `CREATE MATERIALIZED VIEW` | An aggregation over one stream, kept current in every region. |
| `SELECT` with a `-- name:` annotation | A named, parameterized query over one view or one stream. |

```sql
CREATE STREAM page_views (
    user_id     String,
    page        String,
    duration_ms UInt32
)
PRIMARY KEY (user_id);

CREATE MATERIALIZED VIEW page_daily
ORDER BY (page, day)
PRIMARY KEY (page)
AS SELECT
    page,
    toStartOfDay(event_time) AS day,
    count()                  AS views,
    uniqExact(user_id)       AS visitors
FROM page_views
GROUP BY page, day;

-- name: PageStats :many
SELECT day, views, visitors
FROM page_daily
WHERE page = {page:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;
```

### 1.1 The strict-subset rule

- Every type, expression, function and `SELECT` clause that Dynaflux accepts means the same
  as in ClickHouse 26.7 with `session_timezone = 'UTC'` and default settings, except for the
  numbered deviations in [§11](#11-deviations-from-clickhouse).
- Everything else is an error with a hint. Dynaflux never reinterprets SQL it doesn't
  support.
- "Means the same" is defined as follows:
  - A view row holds what ClickHouse returns for the view's `SELECT`, run over every event
    ever appended to the stream and restricted to that row's key.
  - A query returns what ClickHouse returns for the query, run over a table that holds the
    view's rows (one row per key, aggregates finalized) or the stream's events.
- The DDL clauses ClickHouse doesn't have (`STREAM`, `MODE`, `SPLIT`, `DEDUP BY`,
  `LOG RETENTION`, `ACCEPT`, `SHARDS`) belong to Dynaflux and are defined on this page.
- Because of this rule, `clickhouse local` can serve as a test oracle for expressions and
  aggregates.

### 1.2 Conventions

- Grammar is EBNF: `=` defines a production, `|` separates alternatives, `[ ]` is optional,
  `{ }` repeats zero or more times, `( )` groups, and `(* *)` is a comment. Keywords in
  double quotes, such as `"SELECT"`, match in any letter case. Other quoted text matches
  exactly. `letter`, `digit`, `hex`, `tab` and `line_end` have their usual ASCII meanings.
  Each production appears once on this page; the start symbol is `file`
  ([§2](#2-files-and-statements)).
- The grammar is wider than the language in places, such as `TTL expr`: the parser accepts
  the wider form, and the rules reject what they don't allow with their own codes. Input
  outside the grammar is a syntax error (DF1101), unless a rule names another code for it.
- A rule ends with the diagnostic code that enforces it, such as (DF2102).
  [Appendix A](#appendix-a-diagnostics) lists every code with its message and hint.
- A rule marked (Dev n) differs from ClickHouse on purpose.
  [§11](#11-deviations-from-clickhouse) lists each deviation and the reason for it.
- An *error* makes `dynaflux validate` exit with status 1. A *warning* is reported, and the
  exit status stays 0.
- A *run-time error* is a typed error that the generated code returns from an append or a
  query. It isn't a diagnostic.

## 2. Files and statements

```ebnf
file      = [ statement ] { ";" [ statement ] } ;
statement = create_stream | create_view | select ;
```

- The `sql` globs in `dynaflux.yaml` choose the files ([config.md](config.md#sql-file-patterns)).
- A file must be valid UTF-8 (DF1001). A byte-order mark at the start is ignored.
- Statements are separated by `;`. The `;` after the last statement is optional, and empty
  statements (`;;`) are ignored.
- A file can hold any number of statements of any kind. Each statement is classified by
  how it starts: `CREATE STREAM`, `CREATE MATERIALIZED VIEW`, or `SELECT`. A `SELECT` needs a
  `-- name:` annotation (DF1104). Directory names don't matter.
- Other statements are errors:
  - `WITH …`, `(SELECT …)`, and a `CREATE` with a modifier from
    [Table A.1](#table-a1-unsupported-sql-features-df1102), such as `CREATE OR REPLACE` or
    `IF NOT EXISTS`, are DF1102.
  - Anything else is DF1103. Its `<kind>` is the first word in uppercase, or the first two
    after `CREATE`: `INSERT`, `CREATE TABLE`, `CREATE VIEW`.
- Definitions belong to the whole store and can appear in any order and any file. A view
  can come before its stream.
- Streams and views share one namespace: both become DynamoDB tables and keys of `tables`
  in `dynaflux.yaml` ([config.md](config.md#tables)), and a query's `FROM` can name either.
  So a stream and a view can't have the same name. Query names have their own namespace.
  Names are case-sensitive ([§3.4](#34-identifiers-and-names)). A name defined twice in one
  namespace is an error (DF1303).

## 3. Lexical structure

### 3.1 Whitespace and comments

- Whitespace is space, tab, carriage return and line feed. A line ends at a line feed; a
  carriage return before it is ignored.
- `--` starts a comment that runs to the end of the line.
- `/* … */` is a block comment. Block comments nest: `/* a /* b */ c */` is one comment. An
  unclosed block comment is an error (DF1004).
- ClickHouse also accepts `#`, `#!` and `//` comments. Dynaflux rejects them (DF1005), so
  that one comment style carries annotations.

### 3.2 Annotations

Annotations are `--` comments that configure the statement after them.

```ebnf
annotation  = "--" [ blanks ] ( name_ann | expose_ann | gotype_ann ) [ blanks ] line_end ;
name_ann    = 'name:' blanks go_name blanks ( ':one' | ':many' ) ;
expose_ann  = 'expose:' blanks 'http' ;
gotype_ann  = 'go:type' blanks go_name ;
go_name     = word ;                     (* checked by DF1206 *)
word        = (* one or more characters other than blanks and line breaks *) ;
blanks      = ( ' ' | tab ) { ' ' | tab } ;
```

| Annotation | Applies to | Meaning |
|------------|------------|---------|
| `-- name: <Name> :one` or `:many` | `SELECT` | Required. Names the query; `<Name>` becomes the generated Go function name. `:one` and `:many` set the result shape ([§10.9](#109-one-and-many)). |
| `-- expose: http` | `SELECT` | Serves the query from the generated HTTP handler (see `http` in [config.md](config.md)). Off by default. |
| `-- go:type <Name>` | `CREATE STREAM` | Overrides the name of the generated Go event type. |

Rules:

- A `--` comment is an annotation when its text, after optional blanks, starts with `name:`,
  `expose:` or `go:`. Every other `--` comment is an ordinary comment. Keys and values are
  case-sensitive: `-- Name:` is an ordinary comment, and `:ONE` is an unknown value.
- An annotation is checked in this order, and the first failure is reported:
  1. The key must be `name:`, `expose:` or `go:type`. Others, such as `-- go:package`, are
     DF1202.
  2. The words after the key must match the grammar in number and layout (DF1201).
  3. Each value must be known. Others, such as `:exec` or `grpc`, are DF1202.
  4. `<Name>` must match `[A-Z][A-Za-z0-9]*` (DF1206).
- Annotations attach to the next statement. They must sit between the previous statement's
  `;` (or the start of the file) and the statement's first token. Ordinary comments and
  blank lines can be mixed in. An annotation inside a statement, after the last statement,
  or before an empty statement (`;`) is an error (DF1203).
- An annotation on the wrong kind of statement is an error (DF1204), and so is the same key
  twice on one statement (DF1205).
- Query names are unique in the store (DF1303).
- Text inside `/* */` is never an annotation.

Generated Go names must be unique in the store (DF1207). The check covers every generated
name: `New`, `Client`, each event type and its fields, `Append<Event>`,
`Append<Event>Batch`, `Read<Stream>`, `Import<Stream>`, `<Stream>Key`, and each query with
its `<Name>Params` and `<Name>Row` and their fields. An event type is the stream name in
singular CamelCase, and a field is the column name in CamelCase with Go initialisms
(`user_id` becomes `UserID`), as in sqlc. Code generation (M2) fixes the exact rules, and
`validate` uses the same ones. So streams `page_view` and `page_views` collide, and
`-- go:type` resolves it.

### 3.3 Keywords

- Keywords match in any letter case: `select`, `SELECT` and `Select` are the same.
- Keywords are contextual: a word is a keyword only where the grammar expects that keyword.
  Words such as `day`, `mode`, `order`, `key`, `ttl`, `split`, `shards`, `log` and `stream`
  work as names.
- These words are reserved inside expressions, so they can't be names (DF1302): `AND`,
  `BETWEEN`, `CASE`, `FALSE`, `IN`, `INF`, `INTERVAL`, `IS`, `NAN`, `NOT`, `NULL`, `OR`,
  `TRUE`. `INF` and `NAN` are on the list because ClickHouse reads them as float literals even
  where a column has that name.
- These words mark unsupported syntax in some positions, and are DF1102 there: `LIKE`,
  `ILIKE`, `REGEXP`, `DIV`, `MOD`, `GLOBAL` and `OVER` after an operand; `DISTINCT` and `TOP`
  right after `SELECT`; and `ALL` right after `ORDER BY`. Quoted, they are always names.

### 3.4 Identifiers and names

```ebnf
identifier = plain_ident | quoted_ident ;
plain_ident = ( letter | "_" ) { letter | digit | "_" } ;
quoted_ident = '"' qchar { qchar } '"' | '`' qchar { qchar } '`' ;
               (* qchar: any character except the quote itself, "\" and a line break *)
name       = identifier ;   (* a name you define or reference; see the rules below *)
```

- Identifiers are case-sensitive: `Page` and `page` are different.
- Quoting doesn't change an identifier: `"page"`, `` `page` `` and `page` are the same name.
  A quoted identifier can't be empty or contain its quote character, a backslash or a line
  break (DF1010).
- Names you define (streams, views, columns, aliases and parameters) must match
  `[a-z][a-z0-9_]*` (DF1301). They become DynamoDB attribute names and Iceberg column names,
  and the archive needs exact lowercase names.
- Reserved names (DF1302):
  - the expression keywords in [§3.3](#33-keywords)
  - for stream columns: `event_id`, `event_time`, `ingest_time` and `origin_region` (the
    implicit columns), plus `pk` and `sk`
  - for view output columns: `pk` and `sk`
- Stream and view names also appear in AWS resource names, which `validate` checks with
  the config ([config.md](config.md#resource-names)): their lengths (DF6110), and stream
  names that end with `_dedup`, which the archive's Athena views use (DF2101).
- A name, `.` and another name, such as `t.col` or `db.t`, is an error (DF5102). Any other
  `.` outside a number is DF1002.
- An unknown column is an error (DF5101). The message suggests a name visible at that
  point: one that differs only in letter case, or else the one with the smallest
  Levenshtein distance over code points, if that is at most 2. Ties go to the name that
  sorts first byte by byte. With no such name, there is no suggestion. DF3001, DF4001 and
  DF5201 suggest names the same way. The note that string literals use single quotes
  appears only for a double-quoted name.

### 3.5 Literals

```ebnf
literal        = number | string | "TRUE" | "FALSE" | "NULL" ;
signed_literal = [ "-" ] number | string | "TRUE" | "FALSE" | "NULL" ;
number         = integer | float ;
integer        = digits ;
float          = digits "." digits [ exponent ] | digits exponent ;
digits         = digit { [ "_" ] digit } ;
exponent       = ( "e" | "E" ) [ "+" | "-" ] digits ;
string         = "'" { schar | escape } "'" ;
               (* schar: any character except "'", "\" and a line break *)
escape         = "''" | "\'" | "\\" | "\n" | "\r" | "\t" | "\0" | "\x" hex hex ;
```

| Literal | Examples | Type |
|---------|----------|------|
| Integer | `0`, `42`, `1_000_000`, `08` | The smallest of `UInt8`, `UInt16`, `UInt32`, `UInt64` that holds the value. |
| Negative integer | `-1`, `-129` | The smallest of `Int8`, `Int16`, `Int32`, `Int64` that holds the value. |
| Float | `1.5`, `2.0e-3`, `1e6` | `Float64` |
| String | `'home'`, `'it''s'` | `String`, or `Date`, `DateTime`, `DateTime64` or `UUID` when compared with one ([§4.6](#46-literal-conversion)) |
| Boolean | `true`, `FALSE` | `Bool` |
| `NULL` | `NULL` | The `Nullable` type of the other values, only where [§5.5](#55-null) allows it |
| Array | `[]`, `['a', 'b']` | `Array(T)`. Only in `DEFAULT` ([§8.2](#82-columns)); elsewhere DF1102. |

Rules:

- A `-` in prefix position (not after an operand) directly before a number literal, with
  or without whitespace between them, makes a negative literal, as in ClickHouse: `-1` is
  the `Int8` value −1, and `-0` is the `UInt8` value 0. After an operand, `-` is
  subtraction: `5 - 3` and `5 -3` are `UInt8 - UInt8`, of type `Int16`. A parenthesized
  operand is negated instead: `-(1)` has type `Int16` ([§5.6](#56-arithmetic)).
- Underscores may appear only between two digits.
- Leading zeros don't mean octal: `08` is 8, as in ClickHouse.
- A number token runs over every following letter, digit, `_` and `.`, and a sign right
  after `e` or `E`. If the run isn't a valid number, it is an error (DF1008): `123abc`,
  `1e`, `1.5.2`, hexadecimal (`0x1F`), binary (`0b101`), a point without digits on both
  sides (`.5`, `5.`), and misplaced underscores (`1_`, `1__0`). So are `inf`, `nan`, and a
  float too large for `Float64` (`1e400`).
- An integer above 18446744073709551615 or below −9223372036854775808 is an error (DF1009).
  ClickHouse silently turns it into a `Float64`.
- String escapes:

  | Escape | Meaning |
  |--------|---------|
  | `''` or `\'` | `'` |
  | `\\` | `\` |
  | `\n`, `\r`, `\t` | line feed, carriage return, tab |
  | `\0` | the NUL character |
  | `\xHH` | the byte with hex value `HH` (exactly two hex digits) |

- Any other backslash sequence is an error (DF1006). ClickHouse also accepts
  `\a \b \e \f \v \" \/ \N`, and keeps the backslash in unknown sequences such as `\q`.
  There is no `\u` escape: write the character itself.
- A line break inside a string is an error (DF1003); write `\n`.
- After escapes are applied, the string must be valid UTF-8 (DF1007).

### 3.6 Parameters

```ebnf
param = "{" name ":" type "}" ;
```

Parameters such as `{page:String}` appear only in named queries (DF4403). Whitespace is
allowed inside the braces. [§10.3](#103-parameters) gives the rules.

### 3.7 Operators and punctuation

The tokens are `+ - * / % = == != <> < <= > >= ( ) [ ] { } , ; :`. `==` means `=`, and `<>`
means `!=`. Any other character outside strings, quoted identifiers and comments is an
error (DF1002).

`||`, `::`, `->` and `<=>` are each read as one token. They, `?`, and other ClickHouse
operators such as `DIV`, `MOD` and `LIKE` are errors with a hint (DF1102,
[Table A.1](#table-a1-unsupported-sql-features-df1102)). For `.`, see
[§3.4](#34-identifiers-and-names).

## 4. Types

### 4.1 Scalar types

```ebnf
type     = identifier [ "(" type_arg { "," type_arg } ")" ] ;
type_arg = type | integer | string ;
```

A type with the wrong number or kind of arguments, such as `String(10)`, `Array()`,
`Nullable(String, String)` or `Decimal(10, 2, 3)`, is an error (DF5009). Type errors are
reported before the parameter rules of [§10.3](#103-parameters).

| Type | Values | Literals |
|------|--------|----------|
| `String` | Valid UTF-8 text. Compared byte by byte. | `'text'` |
| `UInt8`, `UInt16`, `UInt32`, `UInt64` | 0 to 2^n − 1 | integers |
| `Int8`, `Int16`, `Int32`, `Int64` | −2^(n−1) to 2^(n−1) − 1 | integers |
| `Float64` | Finite IEEE 754 doubles ([§4.3](#43-value-restrictions)) | floats and integers |
| `Decimal(P, S)` | 1 ≤ P ≤ 38, 0 ≤ S ≤ P: at most P digits, S of them after the point. `Decimal(P)` means `Decimal(P, 0)`. | numbers, converted exactly |
| `Bool` | `true`, `false` | `true`, `false` |
| `UUID` | 128-bit values | `'61f0c404-5cb3-11e7-907b-a6006ad3dba0'` (hex digits in either case) |
| `Date` | Days from 1970-01-01 to 2106-02-07 | `'2024-05-31'` |
| `DateTime64(3)`, `DateTime64(6)` | Instants in UTC, with millisecond or microsecond precision, from 1970-01-01 00:00:00 up to but not including 2106-02-07 06:28:15 | `'2024-05-31'`, `'2024-05-31 13:45:00.250'` |
| `DateTime` | Whole seconds in UTC, same range. Only a result type: the bucket functions `toStartOfMinute`, `toStartOfFiveMinutes`, `toStartOfHour` and `toStartOfDay` return it. You can't declare a column or parameter of this type. | — |

Type names follow ClickHouse's letter-case rules:

- Case-sensitive: `String`, `UInt8`–`UInt64`, `Int8`–`Int64`, `Float64`, `UUID`, `Array`,
  `Nullable`, `LowCardinality`. A spelling such as `string` is an unknown type, and the
  message suggests `String` (DF5001).
- Any case: `Date`, `DateTime64`, `Decimal`, `Bool`.

Type rules:

- `DateTime64` needs an explicit precision of 3 or 6 (DF5003). ClickHouse defaults to 3.
  `DateTime64(9)` is rejected because Python, JSON and Iceberg/Athena consumers would
  silently truncate nanoseconds.
- `DateTime64(p, 'UTC')` is accepted and means `DateTime64(p)`. Any other time zone is an
  error (DF5004): every time in Dynaflux is UTC.
- `Decimal` outside 1 ≤ P ≤ 38, 0 ≤ S ≤ P, or written without a precision, is an error
  (DF5005).
- These ClickHouse types aren't in v1 (DF5002); the hint names the replacement:

  | Type | Use instead |
  |------|-------------|
  | `Float32` | `Float64` |
  | `DateTime` (as a declared type), `Date32` | `DateTime64(3)` or `DateTime64(6)`; `Date` |
  | `Int128`, `Int256`, `UInt128`, `UInt256` | `Int64`, `UInt64` or `Decimal(38, 0)` |
  | `Decimal32(S)` … `Decimal256(S)` | `Decimal(P, S)` |
  | `FixedString(N)` | `String` |
  | `Enum8`, `Enum16` | `LowCardinality(String)` |
  | `Map`, `Tuple`, `JSON`, `Variant`, `Dynamic`, `IPv4`, `IPv6` | Separate columns of supported types |
  | SQL aliases such as `INT`, `BIGINT`, `VARCHAR`, `TEXT`, `DOUBLE`, `BOOLEAN`, `TIMESTAMP` | The ClickHouse type name |

Payload columns are stored as native DynamoDB types (string, number, boolean and list), so
they stay readable in the console and in exports. DynamoDB numbers hold 38 significant
digits, magnitudes from 1e-130 to just under 1e126, and no NaN or infinity, which is where
the `Decimal` and `Float64` limits come from. Key encoding is specified in
`docs/spec/encoding.md`.

### 4.2 Nullable, Array and LowCardinality

- `Nullable(T)` adds `NULL` to a scalar type `T`. `T` can't be an `Array`, a `Nullable` or a
  `LowCardinality` (DF5006). As in ClickHouse, write `LowCardinality(Nullable(String))`, not
  `Nullable(LowCardinality(String))`.
- `Array(T)` holds a list of values. `T` must be a non-Nullable scalar type (DF5007), so
  arrays don't nest and can't hold `NULL`. An array itself can't be `NULL`; use `[]`.
- Arrays can be used in `ARRAY JOIN` ([§9.5](#95-array-join)), in `length`, as `DEFAULT`
  values, and as stream query outputs. They can't be keys, be compared, or be passed to
  aggregates (DF5203, DF5301).
- `LowCardinality(String)` and `LowCardinality(Nullable(String))` are accepted for
  familiarity. They mean exactly `String` and `Nullable(String)` everywhere, including
  inside `Array`. Any other `LowCardinality(T)` is an error (DF5008).

### 4.3 Value restrictions

Dynaflux rejects some values that ClickHouse accepts. Appends that break these rules fail
with a run-time error, and so do query parameter values (the HTTP handler answers 400).
An append checks only stream-level rules, such as these and the `ACCEPT` window
([§8.7](#87-accept-and-event-time)). It doesn't evaluate views, except `SYNC` views in its
transaction ([§9.7](#97-mode)).

| Type | Restriction | Why |
|------|-------------|-----|
| `String` | Valid UTF-8. | DynamoDB strings are UTF-8. |
| `Float64` | Finite, and 0 or of a magnitude from 1e-130 to 9.9999999999999999999999999999999999999e125: NaN, ±Inf, `1e300` and `1e-200` are rejected. −0.0 is stored, and computed, as 0.0. | DynamoDB numbers and JSON can't carry NaN or infinity, and DynamoDB numbers have that range. |
| `Decimal(P, S)` | At most P digits. | A stored value always fits its declared type. ClickHouse checks only the storage width. |
| `Date`, `DateTime64(p)` | Within the ranges in [§4.1](#41-scalar-types), also in `import` mode. | The bucket functions return `DateTime`, which ClickHouse silently wraps outside 1970–2106. The bound keeps every bucket exact. |
| `DateTime64(p)` | Finer precision is truncated to p fractional digits. | Callers may pass more precision, such as Go's nanoseconds. ClickHouse truncates too. |
| `Array(T)` | An appended array has at most 1,000 elements. | Bounds the rows one event adds to a view through `ARRAY JOIN` ([§9.5](#95-array-join)). |
| Key columns | The encoded partition key must fit 2048 bytes and the encoded sort key 1024 bytes, including any shard or `SPLIT` suffix. | DynamoDB key limits. A stream key that's too long fails the append; a view key that's too long is an evaluation error ([§5.8](#58-evaluation-errors)). |
| Event | The encoded event item, with attribute names and bookkeeping, must fit 400 KB. | DynamoDB item limit. |

### 4.4 Type groups

| Group | Types | Used by |
|-------|-------|---------|
| integer | `UInt8`–`UInt64`, `Int8`–`Int64` | `%`, `intDiv` |
| numeric | integer, `Float64`, `Decimal(P, S)` | arithmetic, `sum`, `avg` |
| temporal | `Date`, `DateTime`, `DateTime64(p)` | bucket functions, `TTL` |
| orderable | numeric, `String`, `Bool`, temporal | `<`, `<=`, `>`, `>=`, `BETWEEN`, `min`, `max`, the second argument of `argMax`/`argMin`, `ORDER BY` |
| scalar | every type except `Array` | aggregate arguments |
| key | non-Nullable `String`, integer, `Decimal`, `Bool`, `UUID`, temporal | stream `PRIMARY KEY` and `DEDUP BY`; view keys |

- `UUID` supports only `=`, `!=` and `IN` (DF5311, or DF4302 in a query's `ORDER BY`).
  ClickHouse orders UUIDs by their second half first, which no other system does.
- `Float64` isn't a key type (DF2105, DF3104): equality on computed floats is fragile.

### 4.5 Common type

`if`, `multiIf` and `coalesce` return the common type of their value arguments (not their
conditions). It is computed over all of them at once: remove `Nullable`, apply the rules
below to the inner types, then make the result `Nullable` if any argument was `Nullable` or
`NULL`. Rules, in order:

1. A type with itself gives that type. `LowCardinality(String)` counts as `String`.
2. Integers:
   - all unsigned: the widest one
   - all signed: the widest one
   - mixed: the smallest signed type that is at least as wide as the widest signed type and
     wider than the widest unsigned type. `UInt64` with any signed type has none (DF5308).
3. `Float64` with an integer of at most 32 bits gives `Float64`. With `Int64` or `UInt64` there
   is none (DF5308).
4. `Decimal(P1, S)` with `Decimal(P2, S)` gives `Decimal(C, S)`, where C is the larger
   precision class of the two. The class of P is 9 when P ≤ 9, 18 when P ≤ 18, and 38
   otherwise. Decimals with different scales have no common type (DF5308): ClickHouse fails
   on them at run time.
5. `Decimal(P, S)` with an integer type gives `Decimal(C, S)`, where C is the smallest of 9,
   18 and 38 that is at least max(P, S + D). D is the integer type's digit count: 3 for 8-bit,
   5 for 16-bit, 10 for 32-bit, 19 for `Int64` and 20 for `UInt64`. As in ClickHouse, when
   S = 0, D is 9 for `Int32` and 18 for `Int64`: `Decimal(5, 0)` with `Int32` gives
   `Decimal(9, 0)`. If no class fits, there is none (DF5308).
6. `Decimal` with `Float64` is an error (DF5309).
7. Temporal: `Date` with `DateTime` gives `DateTime`. `Date` or `DateTime` with
   `DateTime64(p)` gives `DateTime64(p)`. `DateTime64(3)` with `DateTime64(6)` gives
   `DateTime64(6)`.
8. `String`, `Bool` and `UUID` combine only with themselves.
9. Anything else has no common type (DF5308). ClickHouse 26 would build a `Variant`.

A value converted to a narrower decimal class can overflow, as in ClickHouse; that is an
evaluation error.

### 4.6 Literal conversion

A literal compared with a typed value (with a comparison operator, `IN` or `BETWEEN`) or
used as a `DEFAULT` converts as follows:

- An integer literal stands for its exact value. A float literal stands for the `Float64`
  nearest to its text (ties to even). Numbers then compare by exact value, as in
  ClickHouse: `f = 0.1` is true when `f` holds 0.1, and the `UInt64` 9007199254740993 isn't
  equal to `9007199254740992.0`. The one exception is a float literal compared with a
  `Decimal`: it stands for its exact decimal text (Dev 7).
- A number literal compared with a `Decimal` is an error when its exact value needs more
  than 38 digits, so that no `Decimal(38, S)` holds it (DF5406): `d < 1e40` and
  `d = 1e-40` are errors.
- A string literal compared with a temporal value or a `UUID` is parsed at compile time,
  strictly, and must be in the range of [§4.1](#41-scalar-types) (DF5401):

  | Other side | Accepted forms |
  |------------|----------------|
  | `Date` | `'YYYY-MM-DD'`, a real calendar date |
  | `DateTime` | `'YYYY-MM-DD'` (midnight UTC) or `'YYYY-MM-DD hh:mm:ss'`, with no fraction: ClickHouse would drop it |
  | `DateTime64(p)` | `'YYYY-MM-DD'` (midnight UTC), or `'YYYY-MM-DD hh:mm:ss'` with an optional `.` and 1 to p fractional digits |
  | `UUID` | 8-4-4-4-12 hex digits |

  ClickHouse parses these leniently, so `'2024-02-30'` becomes 2024-03-01 and
  `'2024-1-1'` is accepted. Dynaflux rejects both, and also rejects `T`, `Z` and offsets.
- A string literal never converts to a number or a `Bool` (DF5402).
- Dynaflux has no other implicit conversion between types.

A `DEFAULT` value must fit the column type exactly (DF2004). Numbers fit by exact value:
in range, whole for integer columns, and with at most P digits and S fractional digits for
`Decimal(P, S)`. A `Float64` column takes any number, rounded to the nearest `Float64`.
Temporal and `UUID` columns take strings in the forms above.

## 5. Expressions

### 5.1 Grammar

```ebnf
expr      = or_expr ;
or_expr   = and_expr { "OR" and_expr } ;
and_expr  = not_expr { "AND" not_expr } ;
not_expr  = "NOT" not_expr | predicate ;
predicate = sum { pred_tail } ;
pred_tail = cmp_op sum
          | [ "NOT" ] "IN" in_rhs
          | [ "NOT" ] "BETWEEN" sum "AND" sum
          | "IS" [ "NOT" ] "NULL" ;
cmp_op    = "=" | "==" | "!=" | "<>" | "<" | "<=" | ">" | ">=" ;
in_rhs    = "(" in_item { "," in_item } ")" | param ;
in_item   = signed_literal | param ;
sum       = product { ( "+" | "-" ) product } ;
product   = unary { ( "*" | "/" | "%" ) unary } ;
unary     = "-" unary | primary ;
primary   = literal | param | call | interval | array_literal | name | "(" expr ")" ;
call      = plain_ident "(" [ "*" | [ "DISTINCT" ] args ] ")" [ "(" [ args ] ")" ] ;
args      = expr { "," expr } ;
array_literal = "[" [ signed_literal { "," signed_literal } ] "]" ;
```

- A `call` with two argument lists is a parametric aggregate, such as `quantile(0.9)(x)`:
  the first list holds the parameters.
- A second `pred_tail` is a chained comparison (DF5304, [§5.2](#52-precedence-and-associativity)).
- `count(*)` is DF5208, and `*` in any other call is DF5203. `DISTINCT` inside a call is
  DF5207.
- `interval` ([§8.1](#81-grammar)) is allowed only in `TTL` and `ACCEPT` (DF5403).
- Array literals are allowed only in `DEFAULT` (DF1102).

### 5.2 Precedence and associativity

From lowest to highest:

| Level | Operators | Associativity |
|-------|-----------|---------------|
| 1 | `OR` | left |
| 2 | `AND` | left |
| 3 | `NOT` (prefix) | right |
| 4 | `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `[NOT] IN`, `[NOT] BETWEEN … AND …`, `IS [NOT] NULL` | none |
| 5 | `+`, `-` | left |
| 6 | `*`, `/`, `%` | left |
| 7 | `-` (prefix) | right |

- Level 4 doesn't chain (DF5304). `a = b = c`, `a = b IS NULL` and `x BETWEEN 1 AND 2 = 1` are
  errors. ClickHouse accepts them with surprising meanings: `2 = 2 = 2` is 0, and
  `a = b IS NULL` means `(a = b) IS NULL`.
- `NOT a = b` means `NOT (a = b)`, and `NOT a AND b` means `(NOT a) AND b`.
- `-a * b` means `(-a) * b`.
- In `x BETWEEN a AND b`, the `AND` belongs to `BETWEEN`: `x BETWEEN 1 AND 2 AND y` means
  `(x BETWEEN 1 AND 2) AND y`.

This table agrees with ClickHouse on every expression Dynaflux accepts. ClickHouse splits
level 4 into three levels; merging them and forbidding chains rejects exactly the
expressions where the split matters.

### 5.3 Names and aliases

- A column name refers to the source of the statement:
  - in a view and in a stream query: the stream's columns, its implicit columns
    ([§8.3](#83-implicit-columns)) and the `ARRAY JOIN` alias
  - in a view query: the view's output columns
- `AS` is required to name an expression. `SELECT page views` is a syntax error with the
  hint "aliases need AS" (DF1101).
- A `SELECT` item that isn't a plain column reference needs an alias (DF5106). A plain
  column keeps its name.
- An alias, including the `ARRAY JOIN` alias, can't equal any column name of the source
  (DF5103). ClickHouse lets an alias shadow a column, which silently changes other
  expressions that use the name. `page AS page` is an error too; write `page`.
- In `WHERE`, `GROUP BY` and `ORDER BY`, and in a view's `ORDER BY`, `PRIMARY KEY` and `TTL`
  clauses, a name resolves to a source column or to a `SELECT` alias, at any depth,
  including inside function arguments. Inside `SELECT` items, only source columns resolve
  (DF5105).
- An alias stands for its expression. So with `page AS p`, the term `p = 'home'` binds
  `page` ([§10.4](#104-key-binding-over-a-view)), and a key `user_id AS u` counts as
  `user_id` for DF3501.
- Output column names must be unique (DF5104).

### 5.4 Conditions

A *condition* is an expression of type `Bool`:

- a comparison, `IN`, `BETWEEN` or `IS [NOT] NULL` (a *predicate*)
- `AND`, `OR` or `NOT` applied to conditions (also a predicate)
- a `Bool` column, a `Bool` literal, or a function returning `Bool`

Rules:

- `WHERE`, the condition argument of an `-If` aggregate, the first argument of `if` and
  arguments 1, 3, …, 2n−1 (counting from 1) of `multiIf` must be conditions (DF5302).
  ClickHouse also accepts numbers there; Dynaflux doesn't.
- The operands of `AND`, `OR` and `NOT` must be conditions (DF5302).
- A predicate can't be used as a value: not as a `SELECT` item, a key, a comparison operand
  or another function's argument (DF5303). The hint is `if(<predicate>, 1, 0)`. ClickHouse
  types predicates as `UInt8`, not `Bool`; keeping them in condition positions keeps that
  difference invisible.

### 5.5 NULL

- Unless a rule below says otherwise, an operator or function returns `NULL` when any
  argument is `NULL`, and its result type is `Nullable` when any argument type is.
- Comparisons with `NULL` return `NULL`. A `NULL` literal as an operand of a comparison or
  `BETWEEN`, such as `x = NULL` or `x < NULL`, is an error (DF5305). For `=` and `!=` the
  hint is `x IS [NOT] NULL`.
- The `NULL` literal is allowed only as a `DEFAULT` and as a value argument of `if`,
  `multiIf` or `coalesce` that has at least one typed value argument. Elsewhere it has no
  type: DF5305 and DF5307 cover comparisons and `IN` lists, and anything else, such as
  `NULL + 1` or `SELECT NULL AS x`, is DF5314.
- `x IS [NOT] NULL` requires a `Nullable` operand (DF5306). On other types ClickHouse
  returns a constant.
- `AND` and `OR` follow three-valued logic: `NULL AND false` is `false`, `NULL OR true` is
  `true`, and otherwise a `NULL` operand gives `NULL`. `NOT NULL` is `NULL`.
- `WHERE`, `-If` conditions, `if` and `multiIf` treat a `NULL` condition as false. A
  `Nullable` condition doesn't make their result `Nullable`.
- `IN` with a `NULL` left side returns `NULL`, for `NOT IN` too. A `NULL` inside an `IN` list
  never matches, so it is an error (DF5307).
- Aggregates skip `NULL` arguments ([§7](#7-aggregate-functions)).
- A query's `GROUP BY` puts `NULL` values in their own group. View keys can't be `Nullable`
  (DF3103).

### 5.6 Arithmetic

Operands must be numeric (DF5312). Result types follow ClickHouse:

| Operator | Integer operands | With `Float64` |
|----------|------------------|----------------|
| `a + b`, `a * b` | One size wider than the wider operand (8 → 16 → 32 → 64 bits, at most 64). Signed if either operand is signed. | `Float64` |
| `a - b` | Same width rule, always signed. | `Float64` |
| `a / b` | `Float64` | `Float64` |
| `a % b` | Unsigned dividend: unsigned, as wide as the divisor. Signed dividend: signed, one size wider than the divisor (at most 64). | error (DF5312) |
| `intDiv(a, b)` | As wide as the dividend; signed if either operand is signed. | error (DF5312) |
| `-a` | Signed: the same type. Unsigned: signed and one size wider (`UInt64` gives `Int64`). | `Float64` |

Examples: `UInt8 + UInt8` is `UInt16`, `UInt32 - UInt32` is `Int64`, `Int8 % UInt8` is
`Int16`, and `UInt64 % Int8` is `UInt8`.

- Integer `+`, `-`, `*`, `%` and `intDiv` compute the exact result, then wrap it (two's
  complement) into the result type, as in ClickHouse. The largest `Int64` plus 1 is the
  smallest `Int64`; the largest `UInt64` plus the `Int8` 1 is the `Int64` 0;
  `intDiv(<largest UInt64>, -1)` is the `Int64` 1.
- `%` truncates toward zero and takes the sign of the dividend: `-7 % 2` is `-1`, and the
  `UInt8` 200 `%` the `Int8` −3 is 2.
- `intDiv` truncates toward zero: `intDiv(-7, 2)` is `-3`.
- Integers in `/` and in `Float64` arithmetic convert to the nearest `Float64` (ties to
  even). Each `Float64` operation rounds on its own, with no fused multiply-add, so results
  are the same on every CPU architecture.
- A divisor of `/`, `%` or `intDiv` that is a number literal with value 0, such as `0`, `-0`
  or `0.0`, is an error (DF5313).
- Any other divisor `d` gets a warning (DF5501), because a zero is an evaluation error
  ([§5.8](#58-evaluation-errors)). Parameters count as non-constant. There is no warning
  when a guard rules zero out. A guard is a term `d != 0`, `d <> 0` or `d > 0`, written
  either way round (`0 < d`), with `d` the same expression after aliases are resolved. It
  must be a top-level `AND` term of one of these:
  - the condition of the `if` or `multiIf` branch that holds the division, or `d = 0` for
    the branch taken when that condition is false
  - the condition of the `-If` aggregate whose arguments hold the division
  - `WHERE`
  - the left operand of an `AND` whose right operand holds the division

`Decimal` arithmetic (P1, S1 and P2, S2 are the operands' precision and scale; C is the
larger precision class of the two, as in [§4.5](#45-common-type)):

| Operation | Result |
|-----------|--------|
| `Decimal + Decimal`, `Decimal - Decimal` | `Decimal(C, max(S1, S2))` |
| `Decimal * Decimal` | `Decimal(C, S1 + S2)`. A scale above C is an error (DF5310). |
| `Decimal / Decimal` | `Decimal(C, S1)`. S1 < S2 is an error (DF5310). |
| `Decimal` with an integer (`+ - * /`, either order) | Exactly the `Decimal` operand's type: `Decimal(5, 2) + Int64` is `Decimal(5, 2)` |
| `-Decimal` | The same type |
| `Decimal` with `Float64`, including a float literal | Error (DF5309). Scale with integers instead: `price * 11 / 10`. |
| `%` or `intDiv` with a `Decimal` | Error (DF5312) |

Division computes the exact quotient and truncates it toward zero to the result's scale:
`1000 / d`, with `d` a `Decimal(5, 2)` holding 1.50, is 666.66.

A `Decimal` result must fit the storage of its class, not its P: a 32-bit integer for
C = 9, a 64-bit integer for C = 18, and 38 digits for C = 38. Overflow is an evaluation
error. ClickHouse checks `Decimal32` and `Decimal64` against their storage width, and
`Decimal128` `+` and `-` against the 128-bit range (about 1.7e38), but silently wraps
`Decimal128` `*` and `sum` (Dev 7).

Temporal values don't take part in arithmetic (DF5312). Use the bucket functions in
[§6](#6-scalar-functions).

### 5.7 Comparisons, IN and BETWEEN

- Allowed operand pairs, with the comparison operators, `IN` and `BETWEEN`:

  | Left | Right |
  |------|-------|
  | numeric | numeric, except `Decimal` with a `Float64` value that isn't a literal (DF5309) |
  | `String` | `String` |
  | `Bool` | `Bool` |
  | `UUID` | `UUID` (only `=`, `!=` and `IN`, DF5311) |
  | temporal | temporal |
  | any of the above | a literal that converts ([§4.6](#46-literal-conversion)) |

  Other pairs are errors (DF5301).
- A parameter has its declared type and never converts: it pairs like a column of that
  type. So `day = {d:String}` is DF5301 for a `Date` column `day`, and a `Decimal` column
  compared with a `Float64` parameter is DF5309.
- Numbers compare by exact value, after a float literal becomes its `Float64`
  ([§4.6](#46-literal-conversion)): the `UInt64` value 9007199254740993 isn't equal to the
  `Float64` 9007199254740992, although converting it to `Float64` would give that value.
- Strings compare byte by byte: `'Z' < 'a'`.
- Temporal values compare as instants. A `Date` is midnight UTC of that day.
- `x [NOT] IN (v1, …, vn)`: each `v` is a literal or a scalar parameter. The result is true
  when `x` equals some `v`; duplicates don't matter.
- `x [NOT] IN {p:Array(T)}`: the same test against every element; `T` pairs with `x` by
  the table above. An empty array matches nothing, so `IN` gives false and `NOT IN` gives
  true. A parameter right after `IN` must be an `Array`, and an `Array` parameter can't be
  an item of an `IN` list (DF4405).
- The left side of `IN` is a single value. Tuples such as `(a, b) IN (…)` are errors
  (DF1102).
- `x BETWEEN a AND b` means `x >= a AND x <= b`, including both ends. `x NOT BETWEEN a AND b`
  means `NOT (x BETWEEN a AND b)`. The operands must be orderable.

### 5.8 Evaluation errors

Expressions are pure and deterministic. These failures are *evaluation errors*:

| Failure | Example |
|---------|---------|
| Integer division or modulo by zero | `intDiv(a, b)` or `a % b` with `b = 0` |
| The minimum value of the dividend's type, divided by or taken modulo −1 | `intDiv(x, -1)` or `x % -1`, with `x` the minimum `Int8` … `Int64` of its own type, whatever the divisor's type. Go returns 0 for `%`; ClickHouse fails. |
| A non-finite `Float64` | `a / b` with `b = 0`; `1e308 * 10` |
| A `Float64` that a view would store outside DynamoDB's range ([§4.3](#43-value-restrictions)), other than a `sum` or `avg` total ([§7.2](#72-function-reference)) | `max(x * 1e200)` |
| `Decimal` overflow or division by zero | [§5.6](#56-arithmetic) |
| A view key longer than DynamoDB allows | [§4.3](#43-value-restrictions) |
| An integer or `Decimal` total outside its result type, when read | [§7.2](#72-function-reference) |

- In a view, an evaluation error opens that view's breaker: the view stops updating and its
  state becomes `broken`, while the stream and the other views continue. The fix is a
  corrected definition, which becomes a new view version, and a backfill.
- In a query, an evaluation error is a run-time error for that call. No partial result is
  returned.
- Order of evaluation:
  - `WHERE` comes first, then a view's keys, then each aggregate's `-If` condition, then
    its arguments, only on rows where the condition is true. So `WHERE b != 0` and
    `sumIf(a / b, b != 0)` never fail. ClickHouse evaluates aggregate arguments on every
    row, so it can fail where Dynaflux doesn't.
  - `if` and `multiIf` evaluate only the branch they return. `AND` and `OR` evaluate left to
    right and stop as soon as the result is known. So `if(b = 0, NULL, a / b)` never fails.

### 5.9 Where each construct may appear

| Construct | View `SELECT` and `WHERE` | Query | `DEFAULT` | `TTL` | `ACCEPT` |
|-----------|---------------------------|-------|-----------|-------|----------|
| Column references | yes | yes | no (DF2003) | one key column (DF3301) | `event_time` only (DF2110) |
| Parameters | no (DF4403) | yes | no (DF4403) | no | no |
| Aggregates | as whole `SELECT` items | `SELECT` items | no | no | no |
| `now()` | no (DF5405) | no (DF5405) | no | no | yes |
| `INTERVAL` | no (DF5403) | no (DF5403) | no | yes | yes |
| Array literals | no | no | yes | no | no |

## 6. Scalar functions

General rules:

- Function names follow ClickHouse's letter-case rules. These match in any case: `count`,
  `sum`, `min`, `max`, `avg`, `if`, `coalesce`, `lower`, `upper`, `concat`, `length`, `now`.
  Every other name, including every combinator form such as `countIf` or `avgMerge`, must be
  spelled exactly. A wrong spelling is an unknown function, and the message suggests the
  right one (DF5201).
- Arguments follow [§5.5](#55-null) for `NULL` unless the function says otherwise.
- Time zone arguments, accepted by several ClickHouse date functions, are errors (DF5210).
- A wrong argument count is DF5202, and a wrong argument type is DF5203.
- The operators are covered in [§5](#5-expressions). The functions are:

| Function | Arguments | Result | Notes |
|----------|-----------|--------|-------|
| `intDiv(a, b)` | integers | [§5.6](#56-arithmetic) | Truncates toward zero. Fails when `b = 0`, and when `a` is the minimum value of its type and `b` is −1. |
| `if(c, then, else)` | `c` a condition | common type of `then` and `else` ([§4.5](#45-common-type)) | A `NULL` condition selects `else`. Evaluates only the selected branch. |
| `multiIf(c1, v1, …, cn, vn, else)` | n ≥ 1, so an odd count of at least 3 | common type of the values | Returns the value after the first true condition, otherwise `else`. `NULL` conditions count as false. |
| `coalesce(x1, …, xn)` | n ≥ 2; every argument but the last must be `Nullable` (DF5212) | common type, `Nullable` unless `xn` isn't | The first non-`NULL` argument. `coalesce(referrer, '')` is a `String`. |
| `lower(s)`, `upper(s)` | `String` | `String` | ASCII letters only; other bytes are unchanged: `lower('ÀB')` is `'Àb'`. The Unicode-aware `lowerUTF8` and `upperUTF8` aren't in v1, because ClickHouse and Go use different case tables. |
| `concat(s1, …, sn)` | n ≥ 2, all `String` | `String` | Any `NULL` argument gives `NULL`. Numbers and dates aren't accepted: ClickHouse formats them as text in ways Go can't reproduce byte for byte. |
| `length(x)` | `String` or `Array(T)` | `UInt64` | Bytes for a string (`length('é')` is 2), elements for an array. |
| `toDate(x)` | `Date`, `DateTime`, `DateTime64(p)`, or a string literal | `Date` | The UTC date of the instant. A string literal must be `'YYYY-MM-DD'`; a parameter isn't accepted in its place (DF4404). Numbers and `String` columns aren't accepted: ClickHouse reads small numbers as days and large ones as seconds, and parses strings leniently. |
| `toStartOfMinute(t)` | `DateTime`, `DateTime64(p)` | `DateTime` | Start of the minute. A `Date` is an error, as in ClickHouse. |
| `toStartOfFiveMinutes(t)` | `DateTime`, `DateTime64(p)` | `DateTime` | Start of the five-minute interval (minute 0, 5, 10, …). The alias `toStartOfFiveMinute` is accepted and stored as `toStartOfFiveMinutes`. |
| `toStartOfHour(t)` | `DateTime`, `DateTime64(p)` | `DateTime` | Start of the hour. |
| `toStartOfDay(t)` | temporal | `DateTime` | Midnight UTC. |
| `toStartOfWeek(t[, mode])` | temporal; `mode` an integer literal from 0 to 9 (DF5211), default 0 | `Date` | Even modes start weeks on Sunday and odd modes on Monday; use 1 for ISO weeks. A result before 1970-01-01 becomes 1970-01-01, as in ClickHouse. |
| `toStartOfMonth(t)` | temporal | `Date` | First day of the month. |
| `toYYYYMM(t)` | temporal | `UInt32` | Year × 100 + month: `202405`. |
| `now()` | none | — | Only in `ACCEPT` (DF5405), where it is the writer's clock at append time, with microsecond precision (Dev 1). |

## 7. Aggregate functions

### 7.1 Summary

"Combine across rows" is what a query uses to aggregate a view column of this kind
([§7.4](#74-read-time-aggregation-and--merge)).

| Function | Arguments | Result | No input rows | State class | Combine across rows |
|----------|-----------|--------|---------------|-------------|---------------------|
| `count()` | none | `UInt64` | 0 | additive | `sum(c)` or `countMerge(c)` |
| `sum(x)` | numeric | `UInt64` from unsigned, `Int64` from signed, `Float64` from `Float64`, `Decimal(38, S)` from `Decimal(P, S)` | 0 | additive | `sum(c)` or `sumMerge(c)` |
| `min(x)`, `max(x)` | orderable | type of `x` | the type's default | idempotent | `min(c)`, `max(c)`, `minMerge(c)`, `maxMerge(c)` |
| `avg(x)` | numeric | `Nullable(Float64)` | `NULL` | additive | `avgMerge(c)` |
| `argMax(arg, val)`, `argMin(arg, val)` | `arg` scalar, `val` orderable | type of `arg` | `arg`'s default | idempotent | `argMaxMerge(c)`, `argMinMerge(c)` |
| `any(x)` | scalar | type of `x` | the type's default | idempotent | `anyMerge(c)` |
| `uniq(x)` (alias `uniqHLL12`) | scalar | `UInt64` | 0 | idempotent | `uniqMerge(c)` |
| `uniqExact(x)` | scalar | `UInt64` | 0 | idempotent | none: can't be combined |
| `quantile(level)(x)` | integer or `Float64` | `Nullable(Float64)` | `NULL` | additive | `quantileMerge(level)(c)` |
| `quantileDD(rel_acc, level)(x)` | integer or `Float64` | `Nullable(Float64)` | `NULL` | additive | `quantileDDMerge(rel_acc, level)(c)` |

Shared rules:

- "Scalar" is every type except `Array` (DF5203).
- Aggregates skip rows where an argument is `NULL`. For `sum`, `min`, `max`, `argMax`,
  `argMin` and `any`, a `Nullable` argument makes the result `Nullable`, and a group with no
  non-`NULL` values gives `NULL`. `count`, `uniq` and `uniqExact` stay `UInt64`. The
  condition of an `-If` aggregate doesn't count: `sumIf(u, r = 'a')` is `UInt64` for a
  `UInt32` `u`, even when `r` is `Nullable`.
- Type defaults: 0 for numbers, `''`, `false`, the all-zero UUID, 1970-01-01, and
  1970-01-01 00:00:00.
- `uniq` and `uniqExact` take exactly one argument (DF5209).
- An aggregate can't contain another aggregate, and can't appear in `WHERE`, `GROUP BY`,
  `DEFAULT`, `TTL` or `ACCEPT` (DF5204).
- Parametric aggregates take literal parameters (DF5205, DF4404). Other functions take no
  parameter list (DF5205).

### 7.2 Function reference

**`count()`** counts rows: events that pass `WHERE`, after `ARRAY JOIN`. `count(*)` and
`count(x)` are errors (DF5208). The hint is `count()`, or `countIf(x IS NOT NULL)` for a
`Nullable` `x`. `COUNT(DISTINCT x)` and `DISTINCT` inside any aggregate are errors with the
hint `uniqExact(x)` or `uniq(x)` (DF5207).

**`sum(x)`**

- `Bool` isn't accepted; use `countIf(flag)`.
- Integer and `Decimal` sums are exact: they never wrap. A total outside the result type
  (`UInt64`, `Int64`, or 38 digits for `Decimal(38, S)`) is a run-time error when read
  (Dev 6). ClickHouse wraps silently.
- A view stores `sum` and `avg` totals with no range limit, not even DynamoDB's 38 digits:
  a limit on the running total would make the result depend on the order of events. Only
  the final total is checked, when read.
- A `Float64` sum depends on the order of additions, so regions, and live projection versus
  backfill, can differ in the last bits. Views get a warning per such aggregate (DF3502).
  Use `Decimal` for money.

**`min(x)`, `max(x)`** return the smallest or largest non-`NULL` value. Strings compare
byte by byte. `UUID` isn't accepted (DF5311).

**`avg(x)`** is the sum divided by the count, in `Float64`. For integer and `Decimal` input,
the exact sum is rounded to the nearest `Float64` first. For `Float64` input, the sum is
the order-dependent `Float64` sum, and gets the same warning as `sum` (DF3502). `avg`
returns `NULL` when no rows contribute, and its type is always `Nullable(Float64)` (Dev 4).

**`argMax(arg, val)`, `argMin(arg, val)`**

- Return `arg` from the row with the largest (`argMax`) or smallest (`argMin`) `val`.
- Ties are broken by `(val, event_time, event_id, array_index)`: `argMax` takes the largest
  tuple, so the latest event wins, and `argMin` the smallest, so the earliest wins (Dev 8).
  `array_index` is the element's 1-based position in the `ARRAY JOIN` array, or 0 without
  one. For a non-`Nullable` `val`, the ClickHouse equivalent is
  `argMax(arg, (val, event_time, event_id))`. For a `Nullable` `val` there is no exact
  equivalent: a tuple is never `NULL`, so ClickHouse wouldn't skip those rows.
- Rows where `arg` or `val` is `NULL` are skipped, as in ClickHouse. So a latest-state view
  can't record that a value was cleared to `NULL`. A `Nullable` `arg` gets a warning
  (DF3503) whose hint is a sentinel: `argMax(coalesce(referrer, ''), event_time)`.
- The result is `Nullable` when `arg` or `val` is.

**`any(x)`** returns the non-`NULL` `x` with the smallest
`(event_time, event_id, array_index)`: the earliest event's value (Dev 8). ClickHouse
returns whichever value it saw first.

**`uniq(x)`**

- Estimates the number of distinct values with a HyperLogLog sketch of 2^12 registers. The
  standard error is about 1.6%. The estimate is the same in every region and doesn't depend
  on event order.
- `uniqHLL12` is an alias, in every combinator form.
- Estimates differ from ClickHouse's `uniq` and `uniqHLL12` (Dev 2).
- The hash and the sketch encoding are part of the storage format. Changing them requires a
  new view version.

**`uniqExact(x)`**

- Counts distinct values exactly.
- The view stores one marker item per distinct value per row, which costs about one extra
  write per new value.
- An `ARRAY JOIN` array that holds a value twice counts it once, as in ClickHouse.
- Its rows can't be combined at read time (DF4201): each row's markers belong to that row.

**`quantile(level)(x)`, `quantileDD(rel_acc, level)(x)`**

- Estimate the value at rank `level` with a deterministic, mergeable sketch of log-scale
  buckets. The returned value is within relative error `rel_acc` of a value at that rank.
  `quantile` uses `rel_acc` = 0.01.
- `level` is a number literal with 0 ≤ `level` ≤ 1. `rel_acc` is a float literal with
  0 < `rel_acc` < 1, as ClickHouse requires (DF5205).
- `level` may be left out, as in ClickHouse, and then is 0.5: `quantile(x)`,
  `quantileDD(0.01)(x)`, and likewise `quantileMerge(c)` and `quantileDDMerge(0.01)(c)`.
- `x` is an integer or `Float64`. `Decimal` isn't accepted (DF5203), as with ClickHouse's
  `quantileDD`.
- There is no interpolation between values.
- The number of buckets is capped by the storage format. When a sketch reaches the cap, its
  lowest buckets merge: high quantiles keep their accuracy, the lowest ones lose it.
- Values differ from ClickHouse, whose `quantile` samples randomly (Dev 3).
- These two may ship after v1.0. M2 decides.

### 7.3 The -If combinator

- `<fn>If(args…, cond)` aggregates only the rows where `cond` is true. `NULL` counts as
  false. `cond` must be a condition (DF5302). For parametric aggregates the parameters stay
  in front: `quantileIf(0.9)(x, cond)`.
- Available for every aggregate in [§7.1](#71-summary): `countIf(cond)`, `sumIf`, `minIf`,
  `maxIf`, `avgIf`, `argMaxIf`, `argMinIf`, `anyIf`, `uniqIf`, `uniqExactIf`, `quantileIf`,
  `quantileDDIf`.
- With no matching rows the result is the "No input rows" value in [§7.1](#71-summary):
  `minIf` gives 0, `anyIf` over a string gives `''`, and `avgIf` and `quantileIf` give
  `NULL`.
- An `-If` aggregate has the state class of its base function.
- Other ClickHouse combinators (`-OrNull`, `-OrDefault`, `-Array`, `-Distinct`, `-ForEach`,
  `-Map`, `-Resample`, `-SimpleState`) are errors (DF5206). `-State` and `-Merge` inside a
  view are errors too (DF3105): views store aggregate states themselves.

### 7.4 Read-time aggregation and -Merge

A view column is both a finalized value and a stored state (Dev 9). A query over a view can
aggregate it either way:

- A plain aggregate works on finalized values, as in ClickHouse: `sum(views)`,
  `max(last_view)`, and `count()`, which counts fetched rows.
- `<fn>Merge(c)` merges the stored states of column `c`. `c` must be a view column defined
  by `<fn>`, with the same parameters except `level` (DF4205). For a column defined by
  `<fn>If`, write `<fn>IfMerge(c)`, as in ClickHouse: `uniqIfMerge(c)` for a `uniqIf`
  column. A merge skips rows whose state holds no value, such as a `minIf` row where no
  event matched; `min(c)` would see that row's default, 0.

  | Function | Column defined by | Result |
  |----------|-------------------|--------|
  | `countMerge(c)` | `count` | `UInt64`; the same as `sum(c)` |
  | `sumMerge(c)` | `sum` | the same as `sum(c)` |
  | `minMerge(c)` | `min` | the smallest value across rows |
  | `maxMerge(c)` | `max` | the largest value across rows |
  | `avgMerge(c)` | `avg` | `Nullable(Float64)`: total sum over total count |
  | `argMaxMerge(c)` | `argMax` | `arg` of the largest `(val, event_time, event_id, array_index)` across rows |
  | `argMinMerge(c)` | `argMin` | `arg` of the smallest `(val, event_time, event_id, array_index)` across rows |
  | `anyMerge(c)` | `any` | the earliest event's value across rows |
  | `uniqMerge(c)` | `uniq` | `UInt64`: estimate of the union |
  | `quantileMerge(level)(c)` | `quantile` | `Nullable(Float64)`; `level` may differ from the view's |
  | `quantileDDMerge(rel_acc, level)(c)` | `quantileDD` with the same `rel_acc` | `Nullable(Float64)` |

- `-Merge` applies only to queries over views; a stream query has no states (DF4205). A
  conditional merge, `<fn>MergeIf(c, cond)`, isn't supported (DF4206); filter the rows with
  `WHERE` instead.
- These aggregates are errors because they're almost always mistakes; each hint points to
  the right form. They match with or without `-If` on either side, and apply even when each
  group holds one view row:

  | Query aggregate | Column defined by | Code |
  |-----------------|-------------------|------|
  | `uniqExact(c)`, `uniqExactMerge(c)` | `uniqExact`: exact distinct sets of different rows can't be combined | DF4201 |
  | `avg(c)` | `avg`: an average of averages ignores how many events each row holds | DF4202 |
  | `uniq(c)` | `uniq`: counts distinct estimates, not distinct values | DF4203 |
  | `quantile(level)(c)`, `quantileDD(…)(c)` | `quantile`, `quantileDD`: a quantile of quantiles | DF4204 |

- Other plain aggregates of finalized values are allowed. `sum(visitors)` over a
  `uniqExact` column adds the per-row counts.
- At read time, ties in `argMax`, `argMin` and `any` over finalized values are broken by the
  row key in the order of [§10.8](#108-order-by-and-limit): the view key, or
  `(event_time, event_id)` in a stream query. `argMax` takes the largest key, and `argMin`
  and `any` the smallest (Dev 8).
- A read-time aggregation processes rows in key order, so it is deterministic.

### 7.5 State classes

How a view applies each event depends on its aggregates:

| Class | Functions | How repeats are handled |
|-------|-----------|-------------------------|
| Idempotent | `min`, `max`, `argMax`, `argMin`, `any`, `uniq`, and `uniqExact` (through its markers) | Applying an event twice changes nothing. |
| Additive | `count`, `sum`, `avg`, `quantile`, `quantileDD` | Counts add, so an event applied twice would count twice. |

`-If` forms take the class of their base function.

Every stored state depends only on the multiset of events applied: not on their order, not
on how they are spread over rows, `SPLIT` items or regions, and not on the CPU
architecture. This holds for the sparse form of `uniq`, which stands for exactly the same
2^12 registers, and for `quantile` buckets merged at the cap. Sketches compute bucket
indexes and estimates without math library functions whose results vary by architecture.

- The *projector*, a Lambda function in each region that keeps views current, reads each
  stream's DynamoDB change stream. The change stream is split into *delivery units* (its
  shards, unrelated to the `SHARDS` clause), and each delivery unit is read in order.
- Every view row records, per delivery unit, the last event it applied (a high-water mark).
  The projector uses it to apply each event to each row exactly once, despite retries and
  duplicate deliveries.
- A new view version is filled by a backfill while live projection runs. Views with at
  least one additive aggregate use the *fenced handoff*: each delivery unit is split at a
  fence, the backfill applies events up to the fence, and live projection applies the rest.
  Views with only idempotent aggregates need no fence. `dynaflux explain` and
  `dynaflux status` show which one applies.

### 7.6 Unsupported aggregates

| Function | Why | Use instead | Code |
|----------|-----|-------------|------|
| `groupArray`, `groupUniqArray`, `groupBitmap` | Unbounded state | `uniq(x)` or `uniqExact(x)` to count; `argMax(x, event_time)` for the latest | DF5213 |
| `groupArray(N)(x)`, `groupUniqArray(N)(x)`, `groupArraySample`, `anyLast`, `first_value`, `last_value`, `anyHeavy`, `topK` | Depend on the order events arrive in | `argMax(x, event_time)`, `any(x)` | DF5214 |
| `median`, `quantiles`, `quantileExact`, `quantileTiming`, `quantileTDigest`, `quantileBFloat16`, `quantileDeterministic` and their variants | Not mergeable across regions, or random | `quantile(level)(x)` | DF5201 |
| `uniqCombined`, `uniqCombined64`, `uniqTheta`, `uniqUpTo` | Other sketches | `uniq(x)` | DF5201 |
| `sumWithOverflow`, `sumKahan`, `sumCount`, `avgWeighted` | — | `sum(x)`, `avg(x)` | DF5201 |
| `countDistinct`, `COUNT(DISTINCT x)` | — | `uniqExact(x)` or `uniq(x)` | DF5207 |
| Window functions (`OVER`) | — | — | DF1102 |

## 8. CREATE STREAM

### 8.1 Grammar

```ebnf
create_stream = "CREATE" "STREAM" name "(" column_def { "," column_def } ")"
                { stream_clause } ;
column_def    = name type { column_option } ;
column_option = "DEFAULT" expr
              | "PII" [ "(" string ")" ]
              | other_option ;               (* DF2002 *)
other_option  = (* NULL, NOT NULL, MATERIALIZED, ALIAS, EPHEMERAL, CODEC, TTL or COMMENT,
                  in ClickHouse's syntax *) ;
stream_clause = "PRIMARY" "KEY" name_list
              | "DEDUP" "BY" name_list
              | "LOG" "RETENTION" ( [ "-" ] number identifier | "FOREVER" )
              | "ACCEPT" expr
              | "SHARDS" integer
              | "CONSISTENCY" "=" string ;
interval      = "INTERVAL" [ "-" ] ( number | string ) identifier [ "TO" identifier ] ;
                (* valid only as "INTERVAL" integer unit; the rest is DF5404 *)
unit          = "SECOND" | "SECONDS" | "MINUTE" | "MINUTES" | "HOUR" | "HOURS"
              | "DAY" | "DAYS" | "WEEK" | "WEEKS" | "MONTH" | "MONTHS"
              | "QUARTER" | "QUARTERS" | "YEAR" | "YEARS" ;
name_list     = name | "(" name { "," name } ")" ;
```

- Clauses, and the options of a column, can come in any order. Each may appear once
  (DF1105). `PRIMARY KEY` is required (DF2102).
- A valid interval is `INTERVAL n unit`, with n a positive integer literal and the unit
  written in full. Anything else the grammar accepts is DF5404: ClickHouse abbreviations
  such as `M` (which means month), `MI`, `D` or `YY`, `INTERVAL -1 DAY`, `INTERVAL 1.5 DAY`,
  `INTERVAL '2 days'` and `INTERVAL … TO …`.
- An interval is at most 100 years: n ≤ 100 `YEAR`, 400 `QUARTER`, 1200 `MONTH`,
  5217 `WEEK`, 36525 `DAY`, 876600 `HOUR`, 52596000 `MINUTE` or 3155760000 `SECOND`
  (DF5404). [§8.6](#86-log-retention) gives the bounds of a `LOG RETENTION` window.
- `MONTH`, `QUARTER` and `YEAR` are calendar arithmetic in UTC that clamps to the end of
  the month, as in ClickHouse: 2024-03-31 minus 1 month is 2024-02-29.

### 8.2 Columns

- A stream has at least one column (the grammar requires it), and column names are unique
  (DF2001). The implicit column names are reserved (DF1302).
- A column takes a type, an optional `DEFAULT` and an optional reserved `PII` tag. Other
  ClickHouse column options (`NULL`, `NOT NULL`, `MATERIALIZED`, `ALIAS`, `EPHEMERAL`,
  `CODEC`, `TTL`, `COMMENT`) are errors (DF2002). For `NULL`, the hint is `Nullable(T)`.
- `DEFAULT` takes a constant expression: literals, operators and scalar functions, such as
  `0`, `''`, `1 + 2` or `toDate('2024-01-01')`. Column references are DF2003, and
  parameters, `now()` and aggregates have their own codes
  ([§5.9](#59-where-each-construct-may-appear)). The compiler computes the value, which
  must fit the column type exactly (DF2004, [§4.6](#46-literal-conversion)); a value that
  fails to compute is DF2004 too. Arrays take an array literal of at most 1,000 elements,
  such as `DEFAULT []` (DF2004, [§4.3](#43-value-restrictions)).
- The generated append functions take every column, so `DEFAULT` matters only for events
  stored before the column existed ([§8.9](#89-changing-a-stream)). Events carry no schema
  version in v1: readers and the projector read a column that an event lacks as its
  `DEFAULT`, or as `NULL` for a `Nullable` column. A later Dynaflux version that adds a
  schema version will treat events without one as version 1.
- A `Nullable` column's `DEFAULT` must be `NULL`, which means the same as no `DEFAULT`
  (DF2006). So events stored before the column existed read it as `NULL`.
- `PII` and `PII('category')` are reserved for personal-data tooling in v2. They parse and
  are rejected (DF2005).

### 8.3 Implicit columns

Every stream has these columns. Views and stream queries can read them, and they can't be
declared.

| Column | Type | Value |
|--------|------|-------|
| `event_id` | `String` | 26 characters of Crockford base32. Without `DEDUP BY`: a ULID made by the writer. With `DEDUP BY`: a 128-bit hash of the encoded `PRIMARY KEY` values, `event_time` and dedup values ([§8.5](#85-dedup-by)). |
| `event_time` | `DateTime64(6)` | When the fact happened: set by the caller, or the writer's clock when the caller leaves it unset. |
| `ingest_time` | `DateTime64(6)` | When Dynaflux accepted the event, from the writer's clock. |
| `origin_region` | `String` | The AWS region that accepted the event, such as `us-east-1`. |

Events are ordered by `(event_time, event_id)`, the same in every region. Events written in
different places within the clock-skew bound (a few milliseconds) may be ordered
differently from reality.

### 8.4 PRIMARY KEY and SHARDS

- `PRIMARY KEY` lists the columns that group events for reading, such as `(user_id)`. Every
  stream query binds them all ([§10.5](#105-key-binding-over-a-stream)).
- Key columns must be declared columns, not implicit ones (DF2103), listed once (DF2106),
  non-`Nullable` (DF2104) and of a key type (DF2105, [§4.4](#44-type-groups)).
- The stream's DynamoDB item has:
  - partition key: the encoded `PRIMARY KEY` values, plus a shard suffix with `SHARDS`
  - sort key: `event_time` in microseconds, then `event_id`
- `SHARDS n` spreads each partition key value over n DynamoDB partitions, for keys too hot
  for one. n is from 2 to 64 (DF2107); without the clause there is one shard.
  - An event goes to shard `hash(event_id) mod n`, so a retried dedup append reaches the
    same item.
  - Stream queries read all n shards and merge them by `(event_time, event_id)`, so reads
    cost n times more.
  - n can't change after deployment.

### 8.5 DEDUP BY

- `DEDUP BY (cols)` makes appends idempotent. The columns follow the same rules as
  `PRIMARY KEY` (DF2103, DF2104, DF2105, DF2106).
- The caller must supply `event_time`. An append without it fails with a run-time error.
- Two appends are duplicates when their `PRIMARY KEY` values, `event_time` and dedup values
  are equal. The second one is rejected with a duplicate error and changes nothing. A retry
  must therefore resend the same `event_time`.
- `event_id` becomes a 128-bit hash of the encoded values that identify a duplicate: the
  `PRIMARY KEY` values, `event_time` in microseconds and the dedup values. It is written as
  26 characters of Crockford base32 (the width of a ULID). So two distinct stored events
  never share an `event_id`.
- A dedup key must identify one fact with one payload. If the same key is appended in two
  regions at once with different payloads, regions may keep different payloads, and the
  projector reports the divergence in a metric.
- Such a cross-region duplicate has a different `ingest_time` and `origin_region` in each
  region, so a view on a `DEDUP BY` stream that reads either column gets a warning (DF3504):
  regions may disagree.

### 8.6 LOG RETENTION

- `LOG RETENTION n unit` keeps each event in DynamoDB for n units after its `ingest_time`.
  The unit is `HOUR`, `DAY`, `WEEK`, `MONTH` or `YEAR`, singular or plural. n is a positive
  integer (DF2109). `MONTH` and `YEAR` are calendar units, counted from each event's
  `ingest_time`.
- The window is from 1 day to 36525 days (100 years): n is 24 to 876600 `HOUR`, 1 to
  36525 `DAY`, 1 to 5217 `WEEK`, 1 to 1200 `MONTH` or 1 to 100 `YEAR` (DF2109). So
  `36 HOUR` is allowed and `12 HOUR` isn't. Windows under a day are refused because DynamoDB
  deletes expired items lazily, often days late, and a region cut off from the others may
  delete events before they replicate (see the last rule below).
- Each event's expiry is fixed when it is appended. A changed retention applies to new
  events only.
- `LOG RETENTION FOREVER` never expires events. It gets a warning (DF2501): the table keeps
  growing, and so does the cost of projecting it. `dynaflux explain` shows the numbers.
- Without the clause, the stream uses `defaults.log_retention` from `dynaflux.yaml`, which
  defaults to 2 days ([config.md](config.md)).
- After the window, DynamoDB deletes the event. The archive keeps every event, and Athena
  reads older events there.
- Stream queries skip expired events even before DynamoDB deletes them, so every region
  returns the same events (Dev 10). The rules of [§9.6](#96-ttl) for expired view rows
  apply.
- Views keep the aggregates of expired events: the projector applies each event when it
  arrives. A new view version can backfill events older than the window only from the
  archive.
- If the archive is disabled for a stream with a limited window, events older than the
  window are lost for good (warning DF6504 in [config.md](config.md)).
- A region cut off from the others for longer than the window may delete events before
  they replicate, and the other regions may then never see them. Keep the window longer
  than any outage you plan for.

### 8.7 ACCEPT and event time

- `ACCEPT` limits the `event_time` an append may carry, measured against the writer's clock
  when the append runs. It takes one of two shapes:
  - `ACCEPT event_time BETWEEN <lower> AND <upper>`
  - `ACCEPT event_time <= <upper>`, with no past bound
- The column must be `event_time` (DF2110). Each bound is `now()`, or `now()` plus or minus
  an interval. The lower bound is `now()` or earlier, and the upper bound is `now()` or
  later (DF2112). An upper bound is required, so `ACCEPT event_time >= …` is an error
  (DF2113). Any other shape or bound is DF2111.
- Without the clause, the window is `ACCEPT event_time <= now() + INTERVAL 5 MINUTE`: no past
  bound, and at most 5 minutes in the future.
- Both bounds are inclusive and compared in microseconds. `now()` here is the writer's clock
  at append time, in UTC (Dev 1).
- An append outside the window fails with a run-time error. `dynaflux import` and the
  generated `Import<Stream>` functions skip the window, but not the range in
  [§4.3](#43-value-restrictions).
- The window keeps bad clocks from poisoning `argMax` and creating far-future buckets.
  There's no past bound by default because retention counts from `ingest_time`, so late
  events still get the full log window.

### 8.8 Reserved clauses

- `CONSISTENCY = '<mode>'` is reserved for consistent streams in v3. It parses and is
  rejected, whatever the value (DF2108).
- `PII` column tags are reserved for v2 ([§8.2](#82-columns)).

### 8.9 Changing a stream

Deployed streams change only additively. `dynaflux diff` (M5) enforces this and gives
these checks their codes. `validate` can't check them, because it has no record of the
deployed stream.

- Allowed: new `Nullable` columns; new columns with a `DEFAULT` (an `Array` column needs
  one, such as `DEFAULT []`); changes to `LOG RETENTION`, `ACCEPT` and `-- go:type`; and
  switching between `LowCardinality(String)` and `String`, which are the same type.
- Rejected: removing, renaming or retyping a column; changing the `DEFAULT` of an existing
  column, which would change what older events read as; and changing `PRIMARY KEY`,
  `DEDUP BY` or `SHARDS`.

## 9. CREATE MATERIALIZED VIEW

### 9.1 Grammar

```ebnf
create_view = "CREATE" "MATERIALIZED" "VIEW" name { view_clause } "AS" select ;
view_clause = "MODE" ( "ASYNC" | "SYNC" )
            | "ORDER" "BY" name_list
            | "PRIMARY" "KEY" name_list
            | "TTL" expr
            | "SPLIT" integer ;
select      = "SELECT" select_item { "," select_item }
              "FROM" name
              { [ "LEFT" ] "ARRAY" "JOIN" array_item { "," array_item } }
              [ "WHERE" expr ]
              [ "GROUP" "BY" expr { "," expr } ]
              [ "ORDER" "BY" order_item { "," order_item } ]
              [ "LIMIT" ( integer | param ) ] ;
select_item = "*" | expr [ "AS" name ] ;      (* "*" is DF4002 *)
array_item  = expr [ "AS" name ] ;
order_item  = expr [ "ASC" | "DESC" ] [ "NULLS" ( "FIRST" | "LAST" ) ] ;
```

- `select` is shared with named queries; each statement kind restricts it.
- View clauses come before `AS`, in any order, each once (DF1105). `ORDER BY` is required
  (DF3007).
- In ClickHouse, `ORDER BY`, `PRIMARY KEY` and `TTL` belong to the target table's engine.
  Dynaflux manages the table, so they belong to the view.

### 9.2 What a view is

- A view groups the events of one stream by its keys and maintains aggregates per group
  ([§1.1](#11-the-strict-subset-rule) defines its rows exactly).
- Each region keeps its own copy in regional tables, fed by that region's stream. Regions
  converge: once every event has replicated, every region holds the same rows, except for:
  - `Float64` sums and averages, up to rounding (DF3502)
  - views that read `ingest_time` or `origin_region` of a `DEDUP BY` stream (DF3504)
  - a dedup key appended in two regions with different payloads ([§8.5](#85-dedup-by))
  - a version backfilled from a table scan instead of an archive, whose starting point is
    approximate (`dynaflux backfill`, M5)
- One view version is one DynamoDB table per region.

### 9.3 SELECT rules

- `FROM` names one stream. An unknown name is DF3001, and a view is DF3002: views over views
  aren't in v1. There are no joins, subqueries or unions (DF1102).
- `GROUP BY` is required (DF3003). Each item is a name (DF5107) that names a `SELECT` item
  (DF3005), once (DF5108). These are the view's keys.
- Every `SELECT` item is either a key or a single aggregate call (DF3004), such as
  `count() AS views`. Expressions over aggregates, such as `sum(a) / count()`, belong in
  queries.
- A key is a column, the `ARRAY JOIN` alias, or an expression over them with an alias:
  `toStartOfDay(event_time) AS day`. A constant key, such as `'all' AS scope`, puts every
  event in the same row. Keys contain no aggregates (DF5204). They must be
  non-`Nullable` (DF3103, hint `coalesce(x, '') AS x_key`) and of a key type (DF3104).
- Aggregate arguments and `WHERE` read the source row: the stream's columns, its implicit
  columns and the `ARRAY JOIN` alias. `WHERE` can also use key aliases.
- A view `SELECT` has no `ORDER BY` or `LIMIT` (DF3006); the view's own `ORDER BY` clause
  sets the key order.
- Parameters (DF4403) and `now()` (DF5405) aren't allowed.
- A view may have keys and no aggregates; its rows then record which keys occurred.

### 9.4 Keys and the DynamoDB layout

- `ORDER BY` lists the view's full key: exactly the `GROUP BY` keys, each once, in any
  order (DF3101). ClickHouse collapses rows by the sorting key, so a shorter `ORDER BY`
  would merge distinct groups.
- `PRIMARY KEY` is a prefix of `ORDER BY` (DF3102, with ClickHouse's message, whose
  position counts from 0). Without it, the primary key is the whole `ORDER BY`.
- The view table's DynamoDB key:
  - partition key: the encoded `PRIMARY KEY` columns
  - sort key: the encoded remaining `ORDER BY` columns, in order, or a constant when there
    are none
- Choose `PRIMARY KEY` from the columns every query binds with `=` or `IN`, and put the
  columns queries take ranges over (usually a time bucket) after it in `ORDER BY`. Without
  a `PRIMARY KEY`, queries must bind every key column, so the view serves point lookups
  only. That's the safe default.
- A `UUID` key must be in `PRIMARY KEY` (DF3106): the sort key orders values, and
  ClickHouse orders UUIDs differently from every other system.
- Sort-key values are ordered by their type's natural order: numbers by value, strings byte
  by byte, times chronologically, `false` before `true`. `docs/spec/encoding.md` defines the
  bytes.

### 9.5 ARRAY JOIN

- `ARRAY JOIN <expr> AS <alias>` turns each event into one source row per array element.
  The alias names the element, and the array column keeps its own name.
- Exactly one array, written once:
  - an alias is required (DF3201)
  - `LEFT ARRAY JOIN` isn't in v1 (DF3202)
  - several arrays or `ARRAY JOIN` clauses aren't in v1 (DF3203); ClickHouse fails at run
    time when zipped arrays differ in length
  - the operand must be an `Array` (DF3204)
- An empty array produces no rows, so the event doesn't touch the view.
- `WHERE` runs after `ARRAY JOIN`, as in ClickHouse, and can use the alias.
- `count()` counts elements. An element's 1-based position is its `array_index`, which
  breaks ties in `argMax`, `argMin` and `any`.
- `ARRAY JOIN` is only for views. In a query it's an error (DF4003).

### 9.6 TTL

- `TTL <key> [+ INTERVAL n unit]` sets when a row expires. Any other expression is DF3301.
  `<key>` is a view key of type `Date`, `DateTime` or `DateTime64` (DF3302). A row expires
  at the moment the expression gives.
- Without `TTL`, rows are kept forever.
- Queries never return expired rows, even before DynamoDB deletes them, so every region
  returns the same rows (Dev 10):
  - Expired rows are removed before `WHERE`, aggregation, `ORDER BY` and `LIMIT`.
  - A row is expired when the reader's UTC clock is at or past its expiry.
  - The expiry stored for DynamoDB is rounded up, with a margin that
    `docs/spec/encoding.md` sets, so DynamoDB never deletes a row that a reader with a
    slightly slow clock still counts as live.
- An event that arrives for a row that has already expired, by the clock of the process
  applying it, is skipped. It doesn't bring the row back.
- The TTL depends only on the key, so a row and its `uniqExact` markers expire together.
  The expiry is computed exactly, without the range limits of [§4.3](#43-value-restrictions)
  (Dev 10).

### 9.7 MODE

| Mode | How the view updates | Use when |
|------|----------------------|----------|
| `ASYNC` | The projector in each region applies events in batches from that region's stream. Typical lag is about a second. Batching absorbs hot rows. | Almost always. The cheapest mode. |
| `SYNC` | An append reads the affected rows and writes the event and the row updates in one DynamoDB transaction, in the writer's region. Other regions apply the event through their projector as usual. | A check must see its own write at once, such as "is this quota used up?", on rows that aren't very hot. |

- Without the clause, the view uses `defaults.view_mode` from `dynaflux.yaml`
  ([config.md](config.md)). The `SYNC` rules below apply however the mode is chosen.
- `SYNC` costs more: transactional writes cost twice as much, and concurrent appends
  conflict on hot rows.
- `SYNC` rules:
  - no `ARRAY JOIN`, because one event could touch any number of rows (DF3303)
  - no `SPLIT` in v1 (DF3304)
  - one append's transaction holds the event, one row per `SYNC` view version on the stream
    that isn't retired, and one marker per `uniqExact` or `uniqExactIf` aggregate in those
    versions. DynamoDB allows at most 100 items (DF3305). The versions are the current
    definitions plus the retained versions in `dynaflux.lock` ([§9.9](#99-view-versions)).
    M1 doesn't read the lock, so it counts only the current definitions.
  - a `SYNC` view whose `GROUP BY` doesn't include every `PRIMARY KEY` column of the stream
    gets a warning (DF3501): many partition keys share its rows, which makes them hot.
- How `SYNC` appends behave:
  - Each event is its own transaction, also in batch appends.
  - Only `live` versions are applied in the transaction. A version still being backfilled
    is not, and queries on it return not-ready.
  - An evaluation error in a `SYNC` view, or a transaction over DynamoDB's 4 MB limit,
    fails the append with a run-time error, and nothing is written.
  - Queries over a `SYNC` view use strongly consistent reads, which cost twice as much. A
    query sees the append at once only in the region that accepted it; other regions see
    it when their projector applies it.
- A query can also wait for an append without `SYNC`: the append returns a token, and the
  query option `WaitFor` waits until the projector in the append's region has applied it,
  then reads with strong consistency. In any other region, `WaitFor` fails at once with a
  run-time error.

### 9.8 SPLIT

- `SPLIT n` stores each view row as n items, with n from 2 to 16 (DF3306).
- The projector writes an event to the item chosen by a hash of the delivery unit that
  delivered it ([§7.5](#75-state-classes)), so each item is written by fewer delivery units
  and carries a smaller high-water mark. That keeps hot rows small: `dynaflux explain`
  suggests `SPLIT` when a row would grow past 1 KB.
- Queries read all n items of a row and combine them, so results are the same as without
  `SPLIT`. `uniqExact` counts stay exact. Reads cost n times more.

### 9.9 View versions

- A deployed view version is immutable. A view gets a new version exactly when its
  *fingerprint* changes to one that no retained version has (see `dynaflux.lock` below).
  The new version gets its own table and is filled by a backfill before queries use it.
- The fingerprint is a SHA-256 hash of what decides the stored data and its layout: the
  stream, `ARRAY JOIN`, `WHERE`, the keys in `ORDER BY` order, the `PRIMARY KEY` split,
  `SPLIT`, each aggregate column (name, function, parameters, arguments, condition and
  state format), `TTL`, and the encoding and storage format versions.
- It is computed after canonicalization, so these don't change it:
  - `MODE`, capacity, comments, Go names, formatting and source positions
  - the order of `SELECT` items and of `GROUP BY` items
  - equivalent spellings: letter case where case doesn't matter, quoting, `==` and `<>`,
    the aliases `uniqHLL12` and `toStartOfFiveMinute`, `LowCardinality(String)`,
    `DateTime64(p, 'UTC')` and `Decimal(P)`
  - an explicit `PRIMARY KEY` equal to the whole `ORDER BY`
  - additive stream changes ([§8.9](#89-changing-a-stream))
- `dynaflux.lock`, a generated file committed next to `dynaflux.yaml`, numbers the versions
  ([ADR 0002](../decisions/0002-view-version-lock-file.md)):
  - A fingerprint the lock doesn't hold gets one more than the view's highest number.
    Numbers are never reused.
  - A fingerprint of a retained version, an older version that is still deployed, makes
    that version current again, with no new number and no backfill.
  - A fingerprint of a retired version gets a new number.
- `dynaflux generate` reads and writes the lock (M2). In M1, `validate` and `explain` don't
  read it, and every view is version 1. A lock that can't be parsed is DF6013
  ([config.md](config.md#resource-names)).
- `MODE` can change in place.

## 10. Named queries

### 10.1 Grammar

A named query is a `select` ([§9.1](#91-grammar)) after a `-- name:` annotation
([§3.2](#32-annotations)).

- `SELECT *` isn't supported (DF4002); list the columns.
- `ARRAY JOIN` isn't allowed (DF4003).
- `FROM` names one view or one stream (DF4001). A view resolves to its current version.
- There is no pagination in v1: no `OFFSET` (DF1102), and `:many` returns every row of the
  bounded read, after `LIMIT`.

### 10.2 Output columns

- Over a view, the query sees the view's keys and aggregate columns. Aggregate columns
  hold finalized values ([§7.1](#71-summary)).
- Over a stream, it sees the stream's columns and implicit columns.
- Each `SELECT` item is an output column. A plain column keeps its name; other expressions
  need an alias (DF5106). Names are unique (DF5104).
- Output order is the `SELECT` order. Generated code uses it.

### 10.3 Parameters

- `{name:Type}` binds a value at call time. `Type` is one of `String`, `UInt8`–`UInt64`,
  `Int8`–`Int64`, `Float64`, `Decimal(P, S)`, `Bool`, `UUID`, `Date`, `DateTime64(3)` or
  `DateTime64(6)` (DF4401).
- `Array(T)` of those types is allowed only right after `IN` or `NOT IN`:
  `x IN {p:Array(T)}` (DF4405).
- `Nullable` and `LowCardinality` parameters aren't allowed (DF4401).
- A name used more than once in a query must have the same type every time (DF4402).
  Different queries are independent.
- Parameters can appear wherever a value can, and in `LIMIT`. They have their declared type
  and never convert ([§5.7](#57-comparisons-in-and-between)). Places that need a literal
  don't take them: aggregate parameters, the `toStartOfWeek` mode and the string argument
  of `toDate` (DF4404).
- Parameters are ordered by first appearance. Generated code uses that order.
- Values are checked at call time ([§4.3](#43-value-restrictions)). The Go API passes typed
  values. The HTTP handler parses strictly and answers 400 on a bad value; ClickHouse would
  wrap `'300'` for `UInt8` to 44.

### 10.4 Key binding over a view

The `WHERE` clause is split into its top-level `AND` terms. A term *binds* a key column
when it has one of these forms, with the column on either side of the operator:

| Form | Kind |
|------|------|
| `k = v` | equality |
| `k IN (v1, …)`, `k IN {p:Array(T)}` | set |
| `k > v`, `k >= v`, `k < v`, `k <= v`, `k BETWEEN v1 AND v2` | range |

Each `v` is an expression without column references or aggregates: a literal, a
parameter, or a function of them, such as `toStartOfMonth({at:DateTime64(6)})`. Other
forms, such as `NOT IN`, `!=`, `OR`, `NOT` or a bare `Bool` column, never bind.

Rules, with the view key `k1, …, kn` in `ORDER BY` order and `k1, …, kp` its
`PRIMARY KEY`:

1. Every partition key column `k1` … `kp` is bound by equality or a set (DF4101). The hint
   suggests a parameter of the key's type, with `DateTime64(6)` for a `DateTime` key.
2. The sort key columns are taken in order. A column bound by equality or a set moves on
   to the next one. A column bound by a range, or not bound, stops the walk. Terms on the
   columns after it are residual filters ([§10.6](#106-residual-filters)).
3. Two equality or set terms, or two lower or two upper bounds, on the same key column are
   an error (DF4102), wherever the column sits: `page = 'a' AND page = 'b'`. `BETWEEN` is
   one lower and one upper bound.
4. The number of key combinations, the product of the set sizes over bound columns, is at
   most 100. A value repeated in a list counts once. Literal lists are checked at compile
   time (DF4103). Array parameters are checked at call time and fail with a run-time error.
   `dynaflux explain` prints the bound.

The read follows from the binding:

- Every key column bound by equality or a set: one item per key combination.
- Otherwise: one range read per combination of the columns bound by equality or a set,
  covering that prefix and the range.
- With `SPLIT n`, each logical row is n items, read together.

The read returns exactly the rows the predicates select, including across types: a
`DateTime` key compared with a `Date` or `DateTime64` parameter is bounded correctly.

### 10.5 Key binding over a stream

1. Every stream `PRIMARY KEY` column is bound by equality or a set (DF4101).
2. `event_time` may be bound by a range, or by `=` (a range of one instant), and follows
   rule 3 of [§10.4](#104-key-binding-over-a-view) (DF4102). Its terms of other forms, such
   as `!=`, are residual filters.
3. The 100-combination cap applies as in [§10.4](#104-key-binding-over-a-view).
4. Each partition key value is read from every shard ([§8.4](#84-primary-key-and-shards)).
5. A stream query over a stream with a limited log window gets a warning (DF4501): it reads
   only events still in the window, and older events are only in the archive.

### 10.6 Residual filters

`WHERE` terms that don't bind a key are applied to the rows the key read returns. They can
use any column of the source, but no aggregate (DF5204). They don't reduce the read: cost is
set by the key binding and `LIMIT` ([§10.8](#108-order-by-and-limit)), and
`dynaflux explain` reports items read, not rows returned.

### 10.7 Read-time aggregation

A query may aggregate the rows it reads, one level deep:

- `GROUP BY` items are names (DF5107), each once (DF5108): source columns or `SELECT`
  aliases of expressions without aggregates.
- Each `SELECT` item is a `GROUP BY` key, an aggregate, or an expression over them,
  constants and parameters (DF4207), such as `if(sum(views) > 1000, 'hot', 'cold') AS heat`.
- Aggregates follow [§7.4](#74-read-time-aggregation-and--merge): plain aggregates over
  finalized values, `-Merge` over stored states, and the guardrails DF4201 to DF4206.
- Without `GROUP BY`, aggregates produce exactly one row, even over zero rows: `count()` is
  0, as in ClickHouse.
- A typical pattern counts entities by their latest state: a view grouped by
  `(…, entity)` with `argMax(…)`, and a query that uses `countIf(…)` over its rows
  ([§12.3](#123-activity-rollup)).

### 10.8 ORDER BY and LIMIT

- An `ORDER BY` item is an output column name. Without `GROUP BY`, it may also be a column
  of the source; with `GROUP BY`, a `GROUP BY` key. Other items are errors, with the hint to
  add the expression to `SELECT` with an alias (DF4301).
- `ASC` is the default. `NULLS LAST` is the default for both `ASC` and `DESC`, as in
  ClickHouse; `NULLS FIRST` and `NULLS LAST` are accepted.
- `UUID` and `Array` columns can't be ordered (DF4302).
- Ties are broken by the row key: the view key for a view query, `(event_time, event_id)`
  for a stream query, or the `GROUP BY` keys after aggregation, ascending. Without
  `ORDER BY`, rows come in that key order. So the result order is the same in every region
  and every run (Dev 11). In this order only, `UUID` values compare by their 16 bytes (the
  order of their hex text), and `NULL` groups come last.
- Without read-time aggregation or residual filters, and with an `ORDER BY` that follows
  the key order (or its reverse) after the bound columns, or no `ORDER BY`, the read stops
  after `LIMIT` rows per key combination. `dynaflux explain` prints that bound.
- `LIMIT n` takes a whole number of at least 1, or a parameter of an unsigned integer type
  (DF4303). A parameter value of 0 returns no rows.
- `OFFSET`, `LIMIT m, n`, `LIMIT … BY`, `WITH TIES`, `TOP`, `ORDER BY ALL` and positional
  `ORDER BY 1` are errors (DF1102).

### 10.9 :one and :many

- `:many` returns a list of rows, possibly empty.
- `:one` returns one row, or a not-found error when there is none. The compiler accepts
  `:one` only when the query can't return more than one row (DF4304):
  - it aggregates without `GROUP BY`, or
  - it reads a view and binds every key column, partition and sort, by `=` to a single
    value, or
  - every `GROUP BY` key has a top-level `WHERE` term `key = v`, whether or not it binds a
    view key, or
  - it ends with `LIMIT 1`.
- Queries against a view version that isn't live yet fail with a not-ready error, unless the
  caller asks for partial results.

## 11. Deviations from ClickHouse

These are the only places where SQL that Dynaflux accepts means something different from
ClickHouse ([§1.1](#11-the-strict-subset-rule)). Each one exists so that every region
computes the same result in any event order, or so the value fits DynamoDB.

| # | Dynaflux | ClickHouse | Why |
|---|----------|------------|-----|
| Dev 1 | `now()` is allowed only in `ACCEPT`, where it is the writer's clock at append time, in microseconds, UTC. | The query's start time, as `DateTime` in seconds. | `ACCEPT` checks each append; there is no query time. |
| Dev 2 | `uniq` and `uniqHLL12` are a HyperLogLog sketch with 2^12 registers (about 1.6% standard error), the same in every region. | `uniq` uses adaptive sampling with states up to about 250 KB; `uniqHLL12` uses another hash and estimator. Estimates differ. | The state must fit a DynamoDB item and merge idempotently. |
| Dev 3 | `quantile` and `quantileDD` use a deterministic log-bucket sketch; `quantile` has relative accuracy 0.01. No interpolation. Integer and `Float64` input only. | `quantile` samples randomly, so results vary between runs, interpolates, and keeps `Decimal` and date types. `quantileDD` uses different buckets. | Every region must reach the same state in any event order. |
| Dev 4 | `avg`, `quantile` and `quantileDD`, in their plain, `-If` and `-Merge` forms, return `NULL` when no rows (or no non-empty states) contribute, and their type is `Nullable(Float64)`. | `nan`, type `Float64`. | NaN can't be stored in DynamoDB or sent as JSON. |
| Dev 5 | `Float64` is finite. An expression that produces NaN or infinity, such as `x / 0`, is an evaluation error, and so is a value a view would store outside DynamoDB's range. −0.0 becomes 0.0. | Returns `inf` or `nan`, and stores any double. Keeps −0.0 apart from 0.0 in `GROUP BY` and `uniqExact`. | DynamoDB numbers are finite and limited in range; folding −0.0 makes `min`, `max` and `uniq` independent of order. |
| Dev 6 | Integer `sum` and `avg` accumulate exactly. A total outside the result type is a run-time error when read. | Wraps silently, even in `avg`. | A wrapped total is wrong; an exact total is right in every region, and its error is deterministic. |
| Dev 7 | `Decimal` precision is at most 38. Results beyond 38 digits fail. Declared P is enforced on append. A float literal compared with a `Decimal` stands for its exact decimal text. | Precision up to 76; 128-bit `*` and `sum` wrap silently; only the storage width is checked. Comparisons with float literals go through `Float64`, so `d = 0.3` is also true for `d = 0.30000000000000001`. In `IN` lists, and in chains of three or more `=` joined by `OR`, which ClickHouse rewrites to `IN`, the float is first truncated to the `Decimal`'s scale, so `price IN (0.29, 1.1)` is false when `price` is 0.29. | DynamoDB numbers hold 38 digits, and money needs exact comparisons. |
| Dev 8 | `argMax` and `argMin` break ties by `(event_time, event_id, array_index)`. `any` returns the earliest event's value. In queries over view rows, their ties are broken by the row key ([§7.4](#74-read-time-aggregation-and--merge)). | The value seen first, which depends on arrival order. | Regions see events in different orders. ClickHouse could return the same value, so a ported query still agrees with it. |
| Dev 9 | A view column is both a finalized value and a state: plain aggregates read the value, `-Merge` merges the state. View rows are always fully merged. | A target column holds either finalized values or `-State` states, and rows with the same key stay apart until a background merge. | Dynaflux manages the table and merges on write. |
| Dev 10 | Expired view rows and expired stream events are never returned. `TTL` expiry is computed exactly, also past 2106. | Expired rows stay visible until a merge removes them. `DateTime` arithmetic in `TTL` wraps after 2106-02-07 06:28:15. | Regions delete expired items independently; filtering on read keeps them in agreement. |
| Dev 11 | Result order is fully determined: ties and unordered results follow the row key. | Unspecified. | Same results in every region and every run. |

Dynaflux also rejects a number of things ClickHouse accepts. These are restrictions, not
deviations, because Dynaflux never gives them another meaning:

- times other than UTC ([§4.1](#41-scalar-types)) and dates or times outside 1970–2106
  ([§4.3](#43-value-restrictions))
- lenient literal parsing, string-to-number conversion and `Variant` common types
  ([§4.5](#45-common-type), [§4.6](#46-literal-conversion))
- numbers as conditions, and predicates as values ([§5.4](#54-conditions))
- chained comparisons ([§5.2](#52-precedence-and-associativity))
- `x = NULL`, `IS NULL` on non-`Nullable` values, and `NULL` in `IN` lists
  ([§5.5](#55-null))
- `concat` over non-strings and Unicode case mapping ([§6](#6-scalar-functions))
- `uniq` and `uniqExact` with several arguments, and aggregates over arrays
  ([§7](#7-aggregate-functions))

## 12. Worked examples

The examples use three generic domains. Each block is one SQL file.

### 12.1 Page views

`dynaflux/streams/page_views.sql`:

```sql
CREATE STREAM page_views (
    user_id     String,
    page        String,
    referrer    Nullable(String),
    tags        Array(String),
    duration_ms UInt32,
    country     LowCardinality(String) DEFAULT ''
)
PRIMARY KEY (user_id)
LOG RETENTION 2 DAY
ACCEPT event_time BETWEEN now() - INTERVAL 30 DAY AND now() + INTERVAL 5 MINUTE;
```

`dynaflux/views/page_daily.sql`: daily totals per page. `day` is a `DateTime`, so the `TTL`
uses a key column of a temporal type.

```sql
CREATE MATERIALIZED VIEW page_daily
MODE ASYNC
ORDER BY (page, day)
PRIMARY KEY (page)
TTL day + INTERVAL 2 YEAR
AS SELECT
    page,
    toStartOfDay(event_time) AS day,
    count()                  AS views,
    uniqExact(user_id)       AS visitors,
    sum(duration_ms)         AS total_ms,
    max(event_time)          AS last_view
FROM page_views
WHERE page != ''
GROUP BY page, day;
```

`dynaflux/views/tag_hourly.sql`: one row per tag and hour, from the `tags` array.

```sql
CREATE MATERIALIZED VIEW tag_hourly
ORDER BY (tag, hour)
PRIMARY KEY (tag)
TTL hour + INTERVAL 30 DAY
AS SELECT
    tag,
    toStartOfHour(event_time) AS hour,
    count()                   AS views,
    uniq(user_id)             AS users
FROM page_views
ARRAY JOIN tags AS tag
GROUP BY tag, hour;
```

`dynaflux/queries/pages.sql`:

```sql
-- name: PageStats :many
SELECT day, views, visitors
FROM page_daily
WHERE page = {page:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;

-- name: TagTotals :many
-- expose: http
SELECT tag, sum(views) AS total_views, uniqMerge(users) AS total_users
FROM tag_hourly
WHERE tag IN {tags:Array(String)}
  AND hour >= {since:DateTime64(6)}
GROUP BY tag
ORDER BY total_views DESC
LIMIT 10;

-- name: RecentPageViews :many
SELECT event_time, page, duration_ms
FROM page_views
WHERE user_id = {user:String}
  AND event_time >= {since:DateTime64(6)}
ORDER BY event_time DESC
LIMIT {n:UInt32};
```

- `PageStats` binds the partition key `page` and takes a range over the sort key `day`.
- `TagTotals` reads up to 100 tags. `total_views` can't be called `views`, because `views`
  is already a column of `tag_hourly` (DF5103). `uniqMerge` merges the hourly sketches into
  one distinct count per tag.
- `RecentPageViews` reads the stream, so `validate` warns:

```
dynaflux/queries/pages.sql:20:6: warning DF4501: RecentPageViews: reads stream page_views, which keeps events for 2 DAY after ingestion (LOG RETENTION). Older events are only in the archive.
  hint: query older events with Athena, or aggregate them in a view
```

### 12.2 Quota-style counter

`dynaflux/quota.sql`: a stream with idempotent appends, a `SYNC` view that sees its own
writes, and a `:one` check.

```sql
CREATE STREAM api_calls (
    account_id String,
    endpoint   LowCardinality(String),
    units      UInt32,
    request_id UUID
)
PRIMARY KEY (account_id)
DEDUP BY (request_id);

CREATE MATERIALIZED VIEW quota_usage
MODE SYNC
ORDER BY (account_id, month)
PRIMARY KEY (account_id)
TTL month + INTERVAL 13 MONTH
AS SELECT
    account_id,
    toStartOfMonth(event_time) AS month,
    sum(units)                 AS used_units,
    count()                    AS calls
FROM api_calls
GROUP BY account_id, month;

-- name: QuotaUsed :one
SELECT used_units, calls
FROM quota_usage
WHERE account_id = {account_id:String}
  AND month = toStartOfMonth({at:DateTime64(6)});
```

- Callers set `request_id` and `event_time` and resend both on retry, so a retried append is
  rejected as a duplicate and counted once.
- `quota_usage` groups by the stream's partition key, so its rows aren't shared and `SYNC`
  gets no warning. Each append writes two items in one transaction: the event and one row.
- `QuotaUsed` binds every key column with `=`, so `:one` is allowed. It returns a not-found
  error before the account's first call of the month.

### 12.3 Activity rollup

`dynaflux/activity.sql`: per-member latest state, a daily rollup, and queries that combine
rows.

```sql
CREATE STREAM member_events (
    workspace_id String,
    member_id    String,
    action       LowCardinality(String),
    duration_ms  UInt32
)
PRIMARY KEY (workspace_id, member_id);

CREATE MATERIALIZED VIEW member_latest
ORDER BY (workspace_id, member_id)
PRIMARY KEY (workspace_id)
AS SELECT
    workspace_id,
    member_id,
    argMax(action, event_time) AS last_action,
    max(event_time)            AS last_seen,
    count()                    AS actions
FROM member_events
GROUP BY workspace_id, member_id;

CREATE MATERIALIZED VIEW workspace_daily
ORDER BY (workspace_id, day)
PRIMARY KEY (workspace_id)
TTL day + INTERVAL 400 DAY
AS SELECT
    workspace_id,
    toDate(event_time)        AS day,
    uniq(member_id)           AS active_members,
    countIf(action = 'login') AS logins,
    avg(duration_ms)          AS avg_duration_ms
FROM member_events
GROUP BY workspace_id, day;

-- name: WorkspaceSummary :one
SELECT
    count()                                     AS members,
    countIf(last_seen >= {since:DateTime64(6)}) AS recently_active,
    countIf(last_action = 'logout')             AS logged_out
FROM member_latest
WHERE workspace_id = {workspace_id:String};

-- name: WorkspaceActiveMembers :one
SELECT uniqMerge(active_members) AS members
FROM workspace_daily
WHERE workspace_id = {workspace_id:String}
  AND day BETWEEN {from:Date} AND {to:Date};
```

- `last_action` can't be called `action`, because `action` is a column of the stream
  (DF5103).
- `WorkspaceSummary` reads one partition of `member_latest`, one row per member, and counts
  members by their latest state. It aggregates without `GROUP BY`, so `:one` is allowed.
- `WorkspaceActiveMembers` merges the daily sketches to count distinct members over a date
  range. With `uniqExact(member_id)` in the view, the same query would be DF4201: exact
  distinct sets of different days can't be combined.

### 12.4 Diagnostics

These files contain three common mistakes.

`dynaflux/queries/mistakes.sql`:

```sql
-- name: PageStatsAnyPage :many
SELECT day, views
FROM page_daily
WHERE day >= {from:Date};

-- name: TopPagesByVisitors :many
SELECT page, uniqExact(visitors) AS unique_visitors
FROM page_daily
WHERE page IN {pages:Array(String)}
GROUP BY page;
```

`dynaflux/views/user_pages.sql`:

```sql
CREATE MATERIALIZED VIEW user_pages
ORDER BY (user_id)
AS SELECT user_id, groupArray(page) AS pages
FROM page_views
GROUP BY user_id;
```

`dynaflux validate` prints:

```
dynaflux/queries/mistakes.sql:4:1: error DF4101: PageStatsAnyPage: view page_daily is partitioned by (page), but WHERE does not bind "page" with = or IN. Every query must fix the partition key.
  hint: add page = {page:String} to WHERE, or declare a view whose PRIMARY KEY matches this query
dynaflux/queries/mistakes.sql:7:14: error DF4201: TopPagesByVisitors: uniqExact(visitors) cannot be combined across rows (the query groups by (page) but page_daily rows are (page, day)).
  hint: declare a view grouped by (page), or make visitors a uniq() column and use uniqMerge(visitors) for an approximate count
dynaflux/views/user_pages.sql:3:20: error DF5213: user_pages: groupArray(page) has unbounded state.
  hint: use uniq(page) or uniqExact(page) to count values, or argMax(page, event_time) for the latest one
```

## Appendix A. Diagnostics

### A.1 Output format

`dynaflux validate` prints each diagnostic as:

```
<path>:<line>:<col>: <severity> <code>: [<statement>: ]<message>
  hint: <hint>
```

- `<path>` is the file's path relative to the project root, with `/` separators, as the
  `sql` globs matched it.
- `<line>` and `<col>` start at 1. Columns count Unicode code points, and a tab counts as
  one.
- `<severity>` is `error` or `warning`.
- `<statement>` is the stream, view or query name, when the statement got far enough to
  have one.
- A diagnostic has at most one hint, on one `hint:` line.
- Diagnostics point at the first token of the construct they name. When the construct is
  missing, they point at the clause that should hold it (for a missing key binding, the
  `WHERE` keyword, or the name after `FROM` when there is no `WHERE`), or at the statement's
  first token when that clause is missing too. Where that leaves a choice:
  - DF1303 and DF4402 point at the later occurrence, by path, line and column, and the
    message names the earlier one.
  - DF5304 points at the second comparison operator.
  - DF3305 points at the stream's name.
  - DF3303, DF3304 and DF3501 point at `MODE`, or at the view's name when the mode comes
    from `dynaflux.yaml`.
  - DF4304 points at the `-- name:` annotation.
  - DF1001 points at line 1, column 1; its `<offset>` counts bytes from 0.
- `<expr>`, `<item>` and similar placeholders show the source text, without comments and
  with each run of whitespace written as one space. `<retention>` is written as `n UNIT`,
  such as `2 DAY`, also when it comes from `dynaflux.yaml`. The golden tests fix the exact
  text of free-form placeholders such as `<reason>` and `<expected>`.
- Output is sorted by path, line, column, code and message, and is the same on every run.
- Exit status: 0 when there are no errors (warnings allowed), 1 when there is an error, 2 for
  usage and I/O problems. `validate` checks `dynaflux.yaml` first ([config.md](config.md)).

Recovery:

- Lexical errors (DF1001 to DF1010) and syntax errors (DF1101 to DF1103) end the
  statement. The parser reports the first one, skips to the next `;` outside strings and
  comments, and continues; an unclosed string ends at its line end for this skip. So a
  statement reports at most one of them.
- DF1001 is the only diagnostic for its file.
- Every other code is a check. Checks run on every statement without a lexical or syntax
  error, including one with annotation errors, and report everything they find.
- A name that a skipped statement defines still counts as defined. Statements that use it
  skip the checks that need its contents, so they add no errors.

Codes:

- `DF1xxx`: files, lexical structure, syntax, annotations and names
- `DF2xxx`: streams
- `DF3xxx`: views
- `DF4xxx`: queries and parameters
- `DF5xxx`: types, expressions and functions
- `DF6xxx`: `dynaflux.yaml`, `dynaflux.lock` and finding the SQL files, in
  [config.md](config.md#codes)

From `DF1xxx` to `DF5xxx`, the codes `DFn5xx` are warnings and all others are errors. Codes
are stable: a code keeps its meaning and is never reused. Every code has at least one golden
test.

### A.2 Codes

Placeholders are in angle brackets. Messages after the statement name are shown without it.

#### Files, lexical structure and syntax

| Code | Severity | Message | Hint |
|------|----------|---------|------|
| DF1001 | error | file is not valid UTF-8 (byte <offset>) | save the file as UTF-8 |
| DF1002 | error | unexpected character <char> | |
| DF1003 | error | string literal is not closed on this line | close it with ', and write a line break as \n |
| DF1004 | error | block comment is not closed | add */; block comments nest, so every /* needs its own */ |
| DF1005 | error | <#, #! or //> comments are not supported | use -- or /* */ |
| DF1006 | error | invalid escape \<char> in string literal | the escapes are '' \' \\ \n \r \t \0 \xHH |
| DF1007 | error | string literal is not valid UTF-8 | check the \xHH escapes |
| DF1008 | error | unsupported number <text> | write decimal digits with digits on both sides of the point, such as 0.5; inf and nan aren't allowed |
| DF1009 | error | integer <text> is out of range | integers are between -9223372036854775808 and 18446744073709551615 |
| DF1010 | error | invalid quoted identifier | a quoted name can't be empty or contain its quote character, \ or a line break |
| DF1101 | error | expected <expected>, found <found> | depends on the context, such as "aliases need AS: count() AS views" |
| DF1102 | error | <feature> is not supported | see [Table A.1](#table-a1-unsupported-sql-features-df1102) |
| DF1103 | error | <kind> statements are not supported | use CREATE STREAM, CREATE MATERIALIZED VIEW or a named SELECT; for CREATE TABLE, use CREATE STREAM |
| DF1104 | error | SELECT has no -- name: annotation | add -- name: <Name> :many above the SELECT |
| DF1105 | error | <clause> appears twice | |

#### Annotations and names

| Code | Severity | Message | Hint |
|------|----------|---------|------|
| DF1201 | error | malformed annotation: <text> | the forms are -- name: <Name> :one, -- name: <Name> :many, -- expose: http, -- go:type <Name> |
| DF1202 | error | unknown annotation <text> | the keys are name, expose and go:type; the values are :one, :many and http |
| DF1203 | error | annotation must come right before a statement | |
| DF1204 | error | -- <key> doesn't apply to <statement kind> | name and expose apply to SELECT; go:type applies to CREATE STREAM |
| DF1205 | error | duplicate -- <key> annotation | |
| DF1206 | error | <Name> is not a valid Go name | use CamelCase that starts with an uppercase letter, such as PageStats |
| DF1207 | error | generated Go name <name> for <thing> collides with <other thing> at <path>:<line> | rename one of them, or set -- go:type on the stream |
| DF1301 | error | invalid name <name> | names use lowercase letters, digits and _, and start with a letter |
| DF1302 | error | <name> is reserved | choose another name |
| DF1303 | error | <name> is already defined at <path>:<line> | streams and views share one set of names, and queries have their own; rename one |

#### Streams

| Code | Severity | Message | Hint |
|------|----------|---------|------|
| DF2001 | error | duplicate column <column> | |
| DF2002 | error | column option <option> is not supported | columns take only DEFAULT; for NULL, use Nullable(<type>) |
| DF2003 | error | DEFAULT must be a constant expression | write the value, such as DEFAULT '' or DEFAULT 0 |
| DF2004 | error | DEFAULT <value> is not a valid <type> | |
| DF2005 | error | PII column tags are reserved for v2 | remove the tag |
| DF2006 | error | Nullable column <column> can't have DEFAULT <value> | remove the DEFAULT: events stored before the column existed read it as NULL |
| DF2101 | error | stream name <name> ends with _dedup | the archive uses <stream>_dedup for its deduplicated Athena view |
| DF2102 | error | stream has no PRIMARY KEY | add PRIMARY KEY (<column>): the columns that group events for reading |
| DF2103 | error | <clause> column <column> is not a declared column of the stream | implicit columns can't be keys |
| DF2104 | error | key column <column> is Nullable | declare it without Nullable |
| DF2105 | error | key column <column> has type <type>, which can't be part of a key | keys use String, integers, Decimal, Bool, UUID, Date or DateTime64 |
| DF2106 | error | <column> appears twice in <clause> | |
| DF2107 | error | SHARDS must be between 2 and 64 | omit SHARDS for one shard |
| DF2108 | error | CONSISTENCY is reserved for consistent streams (v3) | remove it; streams are eventually consistent across regions |
| DF2109 | error | LOG RETENTION <value> is not allowed | use a whole number of HOUR, DAY, WEEK, MONTH or YEAR, from 1 DAY (24 HOUR) to 36525 DAY, or FOREVER |
| DF2110 | error | ACCEPT must constrain event_time | ACCEPT event_time BETWEEN now() - INTERVAL 30 DAY AND now() + INTERVAL 5 MINUTE |
| DF2111 | error | ACCEPT must be event_time BETWEEN <bound> AND <bound>, or event_time <= <bound>, where a bound is now() or now() plus or minus an INTERVAL | |
| DF2112 | error | ACCEPT <lower or upper> bound is on the wrong side of now() | the lower bound is now() - INTERVAL …, the upper bound now() + INTERVAL … |
| DF2113 | error | ACCEPT needs an upper bound on event_time | ACCEPT event_time <= now() + INTERVAL 5 MINUTE is the default |
| DF2501 | warning | LOG RETENTION FOREVER keeps every event in DynamoDB; the table and the cost of projecting it keep growing | keep a short log (the default is 2 DAY) and use the archive as the permanent record; dynaflux explain shows the cost |

#### Views

| Code | Severity | Message | Hint |
|------|----------|---------|------|
| DF3001 | error | <name> is not a stream | did you mean <stream>? |
| DF3002 | error | <name> is a view; views over views are not supported | read the stream <stream> that <name> is built from |
| DF3003 | error | view SELECT has no GROUP BY | add GROUP BY with the view's keys |
| DF3004 | error | <item> must be a GROUP BY key or a single aggregate | compute expressions over aggregates in a query |
| DF3005 | error | GROUP BY <name> does not name a SELECT item | add <name> to SELECT |
| DF3006 | error | a view SELECT can't have <ORDER BY or LIMIT> | set the key order with the view's ORDER BY clause, before AS |
| DF3007 | error | view has no ORDER BY | add ORDER BY (<GROUP BY keys>) before AS |
| DF3101 | error | ORDER BY must list exactly the GROUP BY keys: <missing or extra> | |
| DF3102 | error | Primary key must be a prefix of the sorting key, but the column in the position <i> is <ORDER BY column>, not <PRIMARY KEY column> | |
| DF3103 | error | key <key> is Nullable | group by coalesce(<expr>, <default>) AS <key> |
| DF3104 | error | key <key> has type <type>, which can't be a view key | |
| DF3105 | error | <aggregate> is not allowed in a view | write the plain aggregate; views store aggregate states themselves |
| DF3106 | error | UUID key <key> must be in PRIMARY KEY | ClickHouse orders UUIDs differently from other systems, so they can't be in the sort key |
| DF3201 | error | ARRAY JOIN needs an alias | ARRAY JOIN <array> AS <element> |
| DF3202 | error | LEFT ARRAY JOIN is not supported in v1 | use ARRAY JOIN, which skips events with empty arrays |
| DF3203 | error | ARRAY JOIN takes one array in v1 | use one view per array |
| DF3204 | error | ARRAY JOIN <expr> is not an Array | |
| DF3301 | error | TTL must be <key> or <key> + INTERVAL <n> <unit> | |
| DF3302 | error | TTL column <name> is not a Date, DateTime or DateTime64 key of the view | |
| DF3303 | error | MODE SYNC can't be used with ARRAY JOIN | use MODE ASYNC |
| DF3304 | error | MODE SYNC can't be used with SPLIT in v1 | |
| DF3305 | error | an append to <stream> writes <n> items in one transaction; DynamoDB allows 100 | make some views ASYNC, use fewer uniqExact aggregates in SYNC views, or retire old versions |
| DF3306 | error | SPLIT must be between 2 and 16 | |
| DF3501 | warning | MODE SYNC on a view whose rows many partition keys of <stream> share; concurrent appends will conflict | add the stream's PRIMARY KEY columns to GROUP BY, or use MODE ASYNC |
| DF3502 | warning | <aggregate> over Float64 converges only up to rounding; regions may differ in the last bits | use Decimal(P, S) for exact totals |
| DF3503 | warning | <aggregate> skips rows where <arg> is NULL, so NULL never replaces a value | use a sentinel: <fn>(coalesce(<arg>, <default>), <val>) |
| DF3504 | warning | view reads <column> from DEDUP stream <stream>; a duplicate accepted in two regions has a different <column> in each | group by event_time instead of ingest_time; avoid origin_region |

#### Queries and parameters

| Code | Severity | Message | Hint |
|------|----------|---------|------|
| DF4001 | error | <name> is not a view or a stream | did you mean <name>? |
| DF4002 | error | SELECT * is not supported | list the columns |
| DF4003 | error | ARRAY JOIN is not allowed in queries | put the ARRAY JOIN in a view |
| DF4101 | error | <view or stream> <source> is partitioned by (<columns>), but WHERE does not bind "<column>" with = or IN. Every query must fix the partition key. | add <column> = {<column>:<type>} to WHERE, or declare a view whose PRIMARY KEY matches this query |
| DF4102 | error | <column> is bound more than once | combine the conditions: one = or IN, or one lower and one upper bound |
| DF4103 | error | the key binding reads <n> key combinations; the limit is 100 | split the call, or declare a view with a coarser key |
| DF4201 | error | <aggregate>(<column>) cannot be combined across rows (the query groups by (<keys>) but <view> rows are (<keys>)). Without GROUP BY, the parenthesis reads (the query combines all rows of <view>). | declare a view grouped by (<keys>), or make <column> a uniq() column and use uniqMerge(<column>) for an approximate count |
| DF4202 | error | avg(<column>) would average the averages of rows with different counts | use avgMerge(<column>) |
| DF4203 | error | uniq(<column>) would count distinct estimates, not distinct values | use uniqMerge(<column>) |
| DF4204 | error | <aggregate>(<column>) would take a quantile of quantiles | use quantileMerge(<level>)(<column>), or quantileDDMerge for a quantileDD column |
| DF4205 | error | <fn>Merge(<column>) needs a <fn> column of a view; <column> is <what> | for a <fn>If column, use <fn>IfMerge(<column>) |
| DF4206 | error | <fn>MergeIf is not supported | filter the rows with WHERE |
| DF4207 | error | <expr> must be a GROUP BY key or inside an aggregate | |
| DF4301 | error | ORDER BY <item> is not an output column | add it to SELECT with an alias and order by the alias |
| DF4302 | error | can't order by <column> of type <type> | |
| DF4303 | error | LIMIT must be a whole number of at least 1, or an unsigned integer parameter | |
| DF4304 | error | :one query can return more than one row | use :many, bind every view key with =, or add LIMIT 1 |
| DF4401 | error | parameter type <type> is not supported | |
| DF4402 | error | parameter <name> is <type> here but <type> at <line>:<col> | use one type per parameter |
| DF4403 | error | parameters are only allowed in named queries | |
| DF4404 | error | <what> needs a literal, not a parameter | |
| DF4405 | error | parameter <name> is misplaced: an Array parameter goes only right after IN or NOT IN, and a parameter there must be an Array | write x IN {<name>:Array(<type>)} |
| DF4501 | warning | reads stream <stream>, which keeps events for <retention> after ingestion (LOG RETENTION). Older events are only in the archive. | query older events with Athena, or aggregate them in a view |

#### Types, expressions and functions

| Code | Severity | Message | Hint |
|------|----------|---------|------|
| DF5001 | error | unknown type <type> | did you mean <type>? |
| DF5002 | error | type <type> is not supported in v1 | the replacement from [§4.1](#41-scalar-types) |
| DF5003 | error | DateTime64 needs precision 3 or 6 | for DateTime64(9): Python, JSON and Athena truncate nanoseconds; use DateTime64(6) |
| DF5004 | error | time zone <zone> is not supported | all times are UTC; omit the zone |
| DF5005 | error | <type> is out of range | Decimal(P, S) needs 1 ≤ P ≤ 38 and 0 ≤ S ≤ P |
| DF5006 | error | <type> can't be inside Nullable | write LowCardinality(Nullable(String)); for arrays, use [] instead of NULL |
| DF5007 | error | Array element type <type> is not supported | elements are non-Nullable scalars |
| DF5008 | error | LowCardinality(<type>) is not supported | LowCardinality works only with String |
| DF5009 | error | invalid type arguments for <type> | write them as in Decimal(10, 2), Array(String) or DateTime64(6) |
| DF5101 | error | unknown column <name> | did you mean <name>?; string literals use single quotes |
| DF5102 | error | qualified name <name> is not supported | use the column name alone |
| DF5103 | error | alias <name> is also a column of <source> | choose another alias |
| DF5104 | error | duplicate output column <name> | |
| DF5105 | error | alias <name> can't be used here | aliases work in WHERE, GROUP BY and ORDER BY; repeat the expression |
| DF5106 | error | <expr> needs a name | add AS <name> |
| DF5107 | error | GROUP BY item must be a name | give the expression an alias in SELECT and group by the alias |
| DF5108 | error | <name> appears twice in GROUP BY | |
| DF5201 | error | unknown function <name> | did you mean <name>?, or the replacement from [Table A.2](#table-a2-replacements-for-unknown-functions-df5201) |
| DF5202 | error | <function> takes <n> arguments, not <m> | |
| DF5203 | error | <function> doesn't accept <type> as argument <i> | |
| DF5204 | error | aggregate <function> is not allowed in <place> | |
| DF5205 | error | invalid parameters for <function>: <reason> | |
| DF5206 | error | combinator -<name> is not supported | use -If, or -Merge in a query over a view |
| DF5207 | error | DISTINCT inside <function> is not supported | use uniqExact(x), or uniq(x) for an estimate |
| DF5208 | error | count(<arg>) is not supported | use count(); for a Nullable <arg>, countIf(<arg> IS NOT NULL) counts its values |
| DF5209 | error | <function> takes one argument in v1 | |
| DF5210 | error | time zone arguments are not supported | all times are UTC; remove the argument |
| DF5211 | error | toStartOfWeek mode must be a whole number from 0 to 9 | 0 starts weeks on Sunday, 1 on Monday |
| DF5212 | error | coalesce argument <i> is not Nullable, so the arguments after it are never used | |
| DF5213 | error | <function>(<arg>) has unbounded state. | use uniq(<arg>) or uniqExact(<arg>) to count values, or argMax(<arg>, event_time) for the latest one |
| DF5214 | error | <function> depends on the order events arrive in | use argMax(<arg>, event_time) for the latest value, or any(<arg>) for the earliest |
| DF5301 | error | can't apply <operator> to <type> and <type> | |
| DF5302 | error | <expr> is not a condition | use a comparison, IN, IS NULL, AND, OR, NOT or a Bool value |
| DF5303 | error | condition <expr> can't be used as a value | if(<expr>, 1, 0) |
| DF5304 | error | comparisons can't be chained | combine them with AND, or add parentheses |
| DF5305 | error | a comparison with NULL is never true | for = and !=, use <expr> IS [NOT] NULL |
| DF5306 | error | <expr> is not Nullable, so IS NULL is always false | |
| DF5307 | error | NULL in an IN list never matches | |
| DF5308 | error | <type> and <type> have no common type | for Decimals, use one scale |
| DF5309 | error | can't combine Decimal and Float64 | use Decimal values, and scale with integers: x * 11 / 10 |
| DF5310 | error | Decimal scale <scale> is out of range for <operation> | |
| DF5311 | error | UUID values can't be ordered | compare UUIDs with = or IN |
| DF5312 | error | <operator> needs <integer or numeric> operands, not <type> | |
| DF5313 | error | division by zero | |
| DF5314 | error | NULL has no type here | use a typed value, or IS NULL |
| DF5401 | error | '<text>' is not a valid <type> | the forms are 'YYYY-MM-DD', 'YYYY-MM-DD hh:mm:ss[.ffffff]' and 8-4-4-4-12 hex digits for UUID |
| DF5402 | error | can't compare a string literal with <type> | |
| DF5403 | error | INTERVAL is only allowed in TTL and ACCEPT | |
| DF5404 | error | invalid INTERVAL: <reason> | write a positive whole number and a full unit name, such as INTERVAL 5 MINUTE |
| DF5405 | error | now() is only allowed in ACCEPT | pass the time as a parameter |
| DF5406 | error | <literal> needs more than 38 digits, so it can't be compared with <type> | Decimal values have at most 38 digits; use a literal that fits Decimal(38, S) |
| DF5501 | warning | division by <expr> fails when it is 0 | if(<expr> = 0, NULL, <a> / <expr>), or if(<expr> = 0, 0, intDiv(<a>, <expr>)) |

#### Table A.1. Unsupported SQL features (DF1102)

| Feature | Hint |
|---------|------|
| `JOIN`, subqueries, `WITH`, `UNION`, `INTERSECT`, `EXCEPT` | A statement reads one stream or view. Put the data you need into the stream, and split the work into views and queries. |
| `SELECT DISTINCT` | Use `GROUP BY`. |
| `HAVING` | Not in v1. Filter the rows in the caller. |
| `PREWHERE`, `FINAL`, `SAMPLE`, `SETTINGS`, `FORMAT` | Not needed: view rows are always merged. |
| `OFFSET`, `LIMIT m, n`, `LIMIT … BY`, `WITH TIES`, `TOP` | No pagination in v1. Narrow the key range instead. |
| `ORDER BY ALL`, positional `ORDER BY 1` | Name the column. |
| Window functions (`OVER`) | Not in v1. |
| `CASE` | `multiIf(cond1, value1, cond2, value2, default)` |
| `CAST`, `::` | No type conversions in v1. |
| `LIKE`, `ILIKE`, `NOT LIKE`, `REGEXP` | Compare with `=` or `IN`. |
| `\|\|` | `concat(a, b)` |
| `? :` | `if(cond, a, b)` |
| `DIV`, `MOD` | `intDiv(a, b)`, `a % b` |
| `IS [NOT] DISTINCT FROM`, `<=>` | `IS NULL` and `=` |
| `GLOBAL IN`, tuples, `[ ]` subscripts, array literals outside `DEFAULT`, `->` lambdas | Not in v1. |
| `IF NOT EXISTS`, `OR REPLACE`, `ON CLUSTER`, `ENGINE`, `TO`, `POPULATE` | Dynaflux creates and manages the tables. |

#### Table A.2. Replacements for unknown functions (DF5201)

| Function | Hint |
|----------|------|
| `toStartOfInterval`, `toStartOfQuarter`, `toStartOfYear`, `toMonday` | `toStartOfMinute` … `toStartOfMonth`; `toStartOfWeek(t, 1)` for Monday |
| `lowerUTF8`, `upperUTF8` | `lower`, `upper` (ASCII only) |
| `lengthUTF8` | `length` (bytes) |
| `isNull(x)`, `isNotNull(x)` | `x IS NULL`, `x IS NOT NULL` |
| `ifNull`, `nullIf` | `coalesce`, `if` |
| `toString`, `toInt*`, `toUInt*`, `toFloat64`, `toDecimal*`, `toDateTime`, `toDateTime64`, `parseDateTimeBestEffort` | No conversions in v1. Compare with a literal or a parameter. |
| `plus`, `minus`, `multiply`, `divide`, `modulo`, `equals`, `and`, `or`, `not` | The operator |
| `arrayJoin` | `ARRAY JOIN <array> AS <element>` in a view |
| The aggregates in [§7.6](#76-unsupported-aggregates) | The replacement listed there |

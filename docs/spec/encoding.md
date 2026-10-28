<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Dynaflux storage encoding

This page is the normative specification of how Dynaflux stores data in DynamoDB: item keys,
attribute values, aggregate states and the bookkeeping in the meta table. `internal/keys`
and `internal/agg` implement exactly what it says, and their tests check it.

- Storage format: 1 (`catalog.StorageFormatVersion`).
- Related: [sql.md](sql.md) (what the values mean) and [config.md](config.md)
  ([`runtime.prune_window_seconds`](config.md#runtime)).

## Contents

1. [Scope and versions](#1-scope-and-versions)
2. [Tables and items](#2-tables-and-items)
3. [Keys](#3-keys)
4. [Hashes and identifiers](#4-hashes-and-identifiers)
5. [Attribute values](#5-attribute-values)
6. [Stream items](#6-stream-items)
7. [View items](#7-view-items)
8. [Meta items](#8-meta-items)
9. [Determinism rules](#9-determinism-rules)

## 1. Scope and versions

- A stream and a view version each keep their storage format for life. Any change to a byte
  layout, a hash, a constant or a derivation on this page needs a new storage format, and so
  a new view version.
- Dynaflux writes only what this page describes. A reader that finds anything else in a
  view or meta item, such as a DynamoDB `NULL` value, reports a decode error naming the
  table and key: a foreign writer has touched the item.
- Stream items are the exception, because an app may run a newer catalog than the
  projector. Readers ignore attributes that are neither declared columns nor reserved
  names, so adding a column works in either deploy order. A declared or reserved attribute
  that doesn't decode is an evaluation error for each view version that reads it
  ([sql.md §5.8](sql.md#58-evaluation-errors)), never a failure of the whole batch.
- Terms:
  - **Delivery unit:** one shard of a stream table's DynamoDB change stream, named by its
    `ShardId` ([ADR 0003](../decisions/0003-shard-identity-from-tumbling-window.md)). It is
    unrelated to the `SHARDS` clause.
  - **Sequence number:** a record's position in its delivery unit, as DynamoDB delivers it:
    21 to 40 decimal digits ([StreamRecord][streamrecord]). Anything else is rejected.
  - **Logical row:** one view row, which `SPLIT n` stores as n items.

## 2. Tables and items

Every table has a string partition key `pk` and a string sort key `sk`
(`catalog.KeyAttrType = "S"`). DynamoDB compares strings by their UTF-8 bytes
([data types][types], [Query][query]), so the encodings below are built for byte order.

| Table | Items | `pk` | `sk` |
|-------|-------|------|------|
| Stream (Global Table) | events | `PRIMARY KEY` fields (§3.2), plus a shard suffix with `SHARDS` | `event_time` and `event_id` |
| View version (regional) | rows | `PRIMARY KEY` fields | the remaining `ORDER BY` fields, plus a `SPLIT` suffix; `!` when there are none |
| | `uniqExact` markers | `!` and a hash (§7.4) | `!` |
| Meta (regional, one per store) | units, view states, backfill jobs, the deployed catalog, stream items and wait receipts (§8) | §8 | §8 |

Attribute names:

- A stream's payload columns and a view's key and aggregate columns are stored under their
  own names. `validate` reserves `pk` and `sk` for them.
- Every other attribute name starts with `_`. Names used now: `_r`, `_it`, `_ttl`, `_v`,
  `_hwm`, `_o`, `_sync` and `_rcpt`. Reserved for later: `_ver` (consistent streams).
- Requests always pass attribute names through `ExpressionAttributeNames`, so a column named
  after a DynamoDB reserved word works.

## 3. Keys

### 3.1 Alphabet

A key is valid UTF-8. Inside a field every byte is `%` (0x25) or above, which reserves four
bytes:

| Byte | Role |
|------|------|
| `#` (0x23) | Separates fields. It sorts below every byte a field can contain. |
| `$` (0x24) | Never stored. Range bounds use it (§3.5): `X$` sorts after every key that continues `X` with `#`, and before every key that continues the last field of `X`. |
| `!` (0x21) | Never starts a row key. Non-row items (markers) and the constant sort key use it. |
| `%` (0x25) | Starts an escape inside a string field. |

### 3.2 Fields

Each value has one form, used in partition keys, sort keys and as hash input. Its byte order
is the type's order.

| Type | Field |
|------|-------|
| `String` | Each byte from 0x00 to 0x25 becomes `%` and two uppercase hex digits; every other byte is kept. The empty string is `%`. `New York` → `New%20York`; `50% #1` → `50%25%20%231`. |
| `UInt8`–`UInt64` | Zero-padded to 3, 5, 10 or 20 digits: `00000000000000001234`. |
| `Int8`–`Int64` | v ≥ 0: zero-padded to 3, 5, 10 or 19 digits. v < 0: `-`, then the nines' complement of \|v\| zero-padded to the same width. `Int64` 42 → `0000000000000000042`, −42 → `-9999999999999999957`, the minimum → `-0776627963145224191`. |
| `Decimal(P, S)` | The unscaled value zero-padded to P digits, with `.` before the last S digits when S > 0. A negative value is `-` and the nines' complement of those digits, with `.` in place. `Decimal(10, 2)`: 12.34 → `00000012.34`, −12.34 → `-99999987.65`. |
| `Bool` | `false`, `true` |
| `UUID` | Lowercase 8-4-4-4-12 hex: `61f0c404-5cb3-11e7-907b-a6006ad3dba0`. `PRIMARY KEY` only ([sql.md §9.4](sql.md#94-keys-and-the-dynamodb-layout)). |
| `Date` | `2024-05-31` |
| `DateTime` | `2024-05-31T13:00:00Z` |
| `DateTime64(3)` | `2024-05-31T13:45:00.250Z` |
| `DateTime64(6)` | `2024-05-31T13:45:00.250123Z` |

- Years are always 1970 to 2106 ([sql.md §4.3](sql.md#43-value-restrictions)), so time
  text has a fixed width per type and sorts in time order.
- `Float64`, `Nullable` and `Array` values are never key fields
  ([sql.md §4.4](sql.md#44-type-groups)).
- Each field preserves its type's order on its own: escapes sort below every kept byte,
  uppercase hex keeps their relative order, `%` (the empty string) is a prefix of every
  other escape, and `-` sorts below every digit.

### 3.3 Composite keys

- A key is its fields joined by `#`: `enc(f1)#enc(f2)#…#enc(fn)`.
- For keys of one schema, byte order equals the order of the value tuples: at the first field
  that differs, either the encodings differ at a byte, or the smaller one is a prefix of the
  larger one, and then the smaller key continues with `#` or ends while the larger continues
  with a byte of `%` or above.
- No field is ever empty, so no key is empty.
- Suffixes are fields with fixed formats, appended last:
  - `sNN` (`s00`–`s63`): the shard of a stream event, with `SHARDS n` for n ≥ 2 (§4.3).
  - `pNN` (`p00`–`p15`): the item of a logical row, with `SPLIT n` for n ≥ 2 (§4.3).
- A view with no sort fields has the sort key `!`, or `pNN` with `SPLIT`.
- Examples: stream pk `u-123`, or `u-123#s05` with `SHARDS`; view pk
  `acme#00000000000000001234` for `(tenant String, user_id UInt64)`; view sk
  `2024-05-31#p02`.

### 3.4 Keys are never decoded

Every item stores its key columns as attributes (§6, §7.1), and readers take key values from
them. The only key text a reader parses is a stream `sk`, whose two fields have fixed widths.
So keys need an encoder and no decoder.

### 3.5 Range bounds

A view query binds leading sort fields by `=` or `IN` (prefix `P`, joined by `#`) and may
bound the next field. Let `B` be `P#`, or empty when `P` is empty. A Query has one sort-key
condition: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN` (inclusive at both ends) or `begins_with`
([Query][query]). Each typed bound becomes a string bound:

| Typed bound | Sort-key bound |
|-------------|----------------|
| field ≥ lo | sk ≥ `B` + enc(lo) |
| field > lo | sk ≥ `B` + enc(lo) + `$` |
| field ≤ hi | sk ≤ `B` + enc(hi) + `$` |
| field < hi | With `P` empty and no lower bound: sk < enc(hi), which is exact. Otherwise sk ≤ `B` + enc(hi); when the bounded field is the last sort field and the view has no `SPLIT`, the item whose sort key equals the bound may be read, and the typed re-check drops it. |
| no lower | `B`, or none when `P` is empty |
| no upper | `P$`, or none when `P` is empty |

- Two bounds become `BETWEEN`, one becomes `>=`, `<=` or `<`, and none is a partition read.
  A lower bound never uses `>`: `sk > enc(lo)` would include the keys `enc(lo)#…`.
- When the lower bound sorts above the upper bound, the range is empty and no request is
  sent (DynamoDB rejects such a `BETWEEN`).
- For `LIMIT n` with a possibly dropped item, read n + 1 items.
- A stream read of `[since, until)` uses sk ≥ enc(since) and sk ≤ enc(until). `event_id`
  always follows `event_time`, so every event at `until` sorts above the bound and nothing
  is dropped.
- The read path re-checks the typed predicate on the key attributes, so the bounds only
  limit what is read. A bound longer than 1024 bytes is left out, and the range is loose
  (`Loose` in `keys.SKRange`): the Query may return any number of items past that bound, on
  either side, and the re-check drops them. With `LIMIT n`, a loose read counts only the
  items that pass the re-check, so it can't stop after n + 1 items.
- Bound values of another type than the key field (a `Date` parameter on a `DateTime` key)
  are converted to the key type first, moving inward (`catalog.KeyPlan`). That conversion
  belongs to the query engine; this page encodes values of the field's own type only.
- A read with every key field bound is a point read: `GetItem` for one item, `BatchGetItem`
  for up to 100. The n items of a `SPLIT` logical row are one Query,
  `sk BETWEEN lsk#p00 AND lsk#p<n−1>`, or the whole partition when the view has no sort
  fields. The range is exact because `#` never occurs inside a field, and a Query rounds
  its total size up to 4 KB once, where `BatchGetItem` rounds each item ([read and write
  operations][rwops]).

### 3.6 Size limits

- DynamoDB keys are 1 to 2048 bytes (partition) and 1 to 1024 bytes (sort)
  ([quotas][quotas]). Sizes count encoded bytes, suffixes included.
- A stream sort key is always 54 bytes. A stream partition key past 2048 bytes fails the
  append. A view key past its limit is an evaluation error, which opens the view's breaker
  ([sql.md §5.8](sql.md#58-evaluation-errors)). A lookup key built from query parameters
  past its limit matches nothing, so the read returns no rows without a request.
- Keys made only of fixed-width types have a size known at compile time. Only `String`
  fields need the run-time check.

## 4. Hashes and identifiers

`H(x)` is SHA-256 of the bytes x. `BE64(b)` reads 8 bytes as a big-endian unsigned integer.
`base32(b)` is Crockford base32 (`0123456789ABCDEFGHJKMNPQRSTVWXYZ`) of b read as one
big-endian number, left-padded with zero bits to a multiple of 5: 16 bytes give 26
characters and 8 bytes give 13. The alphabet is in ASCII order, so the text sorts like the
bytes. `uvarint` and `varint` are the unsigned and zig-zag signed varints of Go's
`encoding/binary`.

### 4.1 `event_id`

- **Without `DEDUP BY`:** a [ULID][ulid] made by the writer: the writer's clock in Unix
  milliseconds (48 bits), then 80 random bits, as `base32` (26 characters, first character
  `0`–`7`). A writer makes them monotonic, so one writer's appends keep their order: when
  its clock reads the previous ID's millisecond or an earlier one, as after the clock steps
  back, the new ID is the previous ID plus 1. A carry out of the random bits moves into the
  time bits, where the ULID spec would fail instead.
- **With `DEDUP BY`:** `base32(H(k)[0:16])`, where `k` is the key (§3.3) of the
  `PRIMARY KEY` values, `event_time` as `DateTime64(6)`, and the `DEDUP BY` values, in that
  order. Two appends are duplicates exactly when these values are equal
  ([sql.md §8.5](sql.md#85-dedup-by)).

### 4.2 Unit key

`unitkey(u) = base32(H(u)[0:8])`, 13 characters, where `u` is the delivery unit's ID. The
unit key names a unit in high-water marks (§7.6), marker owners (§7.4) and the unit's meta
item (§8.1), so a view row can find the meta item of every unit it has an entry for.

### 4.3 Buckets

`bucket(x, n) = BE64(H(x)[0:8]) mod n`.

- **Shard:** an event of a stream with `SHARDS n` goes to shard `bucket(event_id, n)`. A
  retried `DEDUP BY` append reaches the same item.
- **`SPLIT` item:** the projector writes a record's contribution to item `bucket(u, n)` of
  the logical row, where `u` is the record's delivery unit. The backfill uses the unit the
  record was first delivered by (the archive's `_shard`). A record that a backfill reads
  from the stream table has no unit: it goes to item `bucket(event_id, n)`, so every worker
  of the backfill puts it in the same item.

### 4.4 Canonical value bytes

Value identity for `uniq`, `uniqExact` and the per-record deduplication of values uses these
bytes. An aggregate's argument type is fixed, so no type tag is needed.

| Type | Bytes |
|------|-------|
| `String` | Its UTF-8 bytes. |
| Integers | 8 bytes big-endian: `uint64` for unsigned types, two's-complement `int64` for signed types. |
| `Float64` | 8 bytes big-endian of the IEEE 754 bits. −0.0 never exists ([sql.md §4.3](sql.md#43-value-restrictions)). |
| `Decimal(P, S)` | The unscaled integer, 16 bytes big-endian two's complement. |
| `Bool` | 1 byte, 0 or 1. |
| `UUID` | Its 16 bytes. |
| `Date`, `DateTime`, `DateTime64(p)` | 8 bytes big-endian two's complement of microseconds since 1970-01-01 00:00:00 UTC. |

`Nullable(T)` uses `T`'s bytes; `NULL` never reaches an aggregate. `LowCardinality(String)`
is `String`.

### 4.5 Other hashes

| Use | Value |
|-----|-------|
| `uniq` register input | `BE64(H(canon)[0:8])` |
| `uniqExact` marker identity | §7.4 |

## 5. Attribute values

### 5.1 Payload values

| Type | DynamoDB type | Written as | Read |
|------|---------------|------------|------|
| `String` | S | The text. The empty string is allowed outside keys. | as is |
| Integers | N | Decimal text. | Exact parse, then a range check. |
| `Float64` | N | The shortest text that reads back as the same `float64`, in plain decimal without an exponent (Go: `strconv.FormatFloat(x, 'f', -1, 64)`). | Parse to the nearest `float64`. |
| `Decimal(P, S)` | N | The canonical literal text of the catalog: optional `-`, the integer digits (at least one), then, when S > 0, `.` and exactly S digits: `12.50`, `-0.05`, `7`. Zero is never negative. DynamoDB drops trailing zeros. | Exact parse, rescale to S, check P. |
| `Bool` | BOOL | | |
| `UUID` | S | Lowercase 8-4-4-4-12 hex. | strict |
| `Date` | S | `YYYY-MM-DD` | strict |
| `DateTime`, `DateTime64(p)` | N | Integer microseconds since the epoch. | Exact integer; a multiple of 1,000 for `DateTime64(3)` and of 1,000,000 for `DateTime`. |
| `Nullable(T)`, `NULL` | — | The attribute is absent. The DynamoDB `NULL` type is never written. | Absent: `NULL`. |
| `Array(T)` | L | One element per value, each written as `T`, in order. `[]` is an empty list, never an absent attribute. | |

- A declared column that is missing from an event, for example one added after the event
  was written, reads as its `DEFAULT`, or `NULL` for a `Nullable` column
  ([sql.md §8.2](sql.md#82-columns)).
- Readers parse every N with one exact decimal parser: sign, digits, an optional fraction
  and an optional exponent. A value that the column type can't hold exactly is a decode
  error. So DynamoDB's normalization of numbers never changes a value, and payload
  comparisons use decoded values, never the stored text.

### 5.2 Exact numbers

Counters and sums store an integer with no bound
([sql.md §7.2](sql.md#72-function-reference)), written as decimal text at the column's
scale, such as `-1234.56` for scale 2:

- It is N when the unscaled value has at most 38 digits, the precision of a DynamoDB number
  ([data types][types]), and S otherwise. The choice depends only on the value, so equal
  totals are stored the same way in every region.
- Readers rescale an N, since DynamoDB drops trailing zeros (`12.3` at scale 2 is 1230). An
  S must be the text as written. A value stored with the other type is a decode error.
- Only a finalized value is range-checked, when it is read
  ([sql.md Dev 6](sql.md#11-deviations-from-clickhouse)).

### 5.3 Sequence numbers

- In meta items (§8) and marker owners (§7.4), a sequence number is a 40-character string of
  decimal digits, zero-padded (`catalog.SequenceWidth`), so string order is numeric order.
- In `_hwm` values (§7.6), it is the unsigned big-endian integer without leading zero bytes.
  Sequence numbers are at least 1, so it is never empty.

### 5.4 `_ttl`

`_ttl` is an N in Unix seconds: the expiry rounded up to a whole second, plus **3600**. Rows
are filtered out by their exact expiry when read
([sql.md Dev 10](sql.md#11-deviations-from-clickhouse)). Events are filtered out from
`_ttl` − 3600, their expiry rounded up to a whole second: it was fixed when they were
appended, and `LOG RETENTION` may have changed since
([sql.md §8.6](sql.md#86-log-retention)). So the margin only makes sure DynamoDB deletes
an item after every reader whose clock is up to an hour slow already treats it as expired
([TTL][ttl]). Nothing Dynaflux writes has already expired, so
DynamoDB's rule about TTL values more than five years in the past never applies.

## 6. Stream items

| Attribute | Value |
|-----------|-------|
| `pk`, `sk` | §2 and §3: `sk` is `event_time` (`DateTime64(6)`, 27 bytes), `#`, then `event_id` (26 bytes). |
| one per declared column | The column's value (§5.1), key columns included. |
| `_r` | S: `origin_region`, the region of the append that wrote the event, by its canonical name, such as `us-east-1`; never a FIPS pseudo-region such as `fips-us-east-1` ([ADR 0014](../decisions/0014-sync-views-in-the-append-transaction.md)). |
| `_it` | N: `ingest_time` in microseconds. |
| `_ttl` | `ingest_time` plus `LOG RETENTION` (calendar units clamp to the end of the month), as §5.4. None with `LOG RETENTION FOREVER`. |
| `_sync` | L of S: the IDs of the `SYNC` view versions that the append's transaction applied with the event, sorted ([sql.md §9.7](sql.md#97-mode)). Absent when there are none. |
| `_rcpt` | BOOL, true: the append asked for a receipt with `dynaflux.Waitable()` (§8.6). Absent otherwise. |

- `event_time` and `event_id` are read from `sk`.
- A reader hides an event from `_ttl` − 3600 seconds on (§5.4), and never hides one
  without `_ttl`.
- Attributes that are neither declared columns nor reserved names are ignored (§1).
- The payload of an event is its declared columns. A `MODIFY` record whose old and new
  payloads decode to different values is a divergent duplicate
  ([sql.md §8.5](sql.md#85-dedup-by)); `_r`, `_it`, `_ttl`, `_sync` and `_rcpt` are not
  compared.
- Only the projector of region `_r` reads `_sync` and `_rcpt`. For an `INSERT` it skips each
  version that `_sync` names, before its fence and time-split checks, and ignores IDs it
  doesn't know; it writes the receipt for `_rcpt` (§8.6). Projectors of other regions ignore
  both: they never ran the transaction. A `_sync` that isn't an L of S breaks every active
  version of the stream that the record reaches, in region `_r` only, since only the writer
  knows which versions it applied (§1).
- `_sync` is reserved for Dynaflux appends. An item that another writer puts with it, such
  as a copy tool or items put again from an export, makes the projector of region `_r` skip
  those versions for that event for good: a change record doesn't show whether a
  transaction wrote the item.
- The archive carries neither `_sync` nor `_rcpt`
  ([infra.md §7.3](infra.md#73-tables-and-columns)).
- The whole item, with attribute names, must fit DynamoDB's 400 KB item limit
  ([item size][itemsize]).

## 7. View items

### 7.1 Rows

| Attribute | Value |
|-----------|-------|
| `pk`, `sk` | §2 and §3. |
| one per key column | Its value (§5.1), under the column's name, as in stream items. Readers take key values from here. |
| one per aggregate column | Its state (§7.2). An absent attribute is the empty state: no input reached the column. |
| `_v` | N: the row version. 1 when the item is created, one more on each write. Every write is conditional: the item is absent, or `_v` still has the value read. |
| `_hwm` | M: high-water marks (§7.6). |
| `_ttl` | With `TTL`: the row's expiry ([sql.md §9.6](sql.md#96-ttl)), as §5.4. Without `TTL`: none. |

- A row exists once a source row has reached it after `ARRAY JOIN` and `WHERE`, as a
  ClickHouse `GROUP BY` group does ([sql.md §1.1](sql.md#11-the-strict-subset-rule)), even
  when no aggregate column had input: a keys-only view
  ([sql.md §9.3](sql.md#93-select-rules)), or a row whose `-If` conditions never held. The
  projector then writes the item with its key columns, `_v`, `_hwm` and `_ttl`.
- An absent aggregate attribute is the empty state: its finalized value is the "No input
  rows" value of [sql.md §7.1](sql.md#71-summary), so `countIf` of a condition that never
  held reads 0. `-Merge` skips absent states
  ([sql.md §7.4](sql.md#74-read-time-aggregation-and--merge)).
- With `SPLIT n`, a logical row exists when any of its items exists, and it is the merge of
  its items, each column merged with its function's merge (§7.2). Regions may spread a row
  over its items differently, because their delivery units differ; merged logical rows are
  identical.

### 7.2 Aggregate states

An `-If` aggregate stores what its base function stores; the condition only filters inputs.

| Function | Type | Content | Add one input | Merge |
|----------|------|---------|---------------|-------|
| `count` | N or S | Exact count (§5.2). | +1 | add |
| `sum` of integers or `Decimal` | N or S | Exact sum at the argument's scale (§5.2). | add | add |
| `sum` of `Float64` | B | Exact sum (§7.3). | add | add |
| `min`, `max` | as the value (§5.1) | The smallest or largest value. Strings compare byte by byte, numbers by exact value, `false` < `true`. | keep the smaller or larger | same |
| `avg` | L | `[sum, count]`, each in its `sum` and `count` form above. | add both | add both |
| `argMax`, `argMin` | L | `[arg, val, event_time, event_id, array_index]`: the two values (§5.1), `event_time` as N microseconds, `event_id` as S, `array_index` as N (0 without `ARRAY JOIN`). | keep the larger (`argMax`) or smaller (`argMin`) `(val, event_time, event_id, array_index)` | same |
| `any` | L | `[x, event_time, event_id, array_index]` | keep the smallest `(event_time, event_id, array_index)` | same |
| `uniq` | B | HyperLogLog sketch (§7.5). | register max | register max |
| `uniqExact` | N or S | Exact counter, plus marker items (§7.4). | +1 for each value whose marker this record owns | add, for the items of one logical row only (DF4201, [sql.md §7.4](sql.md#74-read-time-aggregation-and--merge)) |
| `quantile`, `quantileDD` | B | DDSketch (§7.5). | +1 in a bucket | add counts, then cap |

- A `Float64` value that a view would store outside DynamoDB's number range is an evaluation
  error ([sql.md Dev 5](sql.md#11-deviations-from-clickhouse)), so every stored `Float64`
  fits an N.
- `event_id` compares byte by byte. Each input row has its own `(event_id, array_index)`,
  so the kept tuple depends only on the set of inputs.
- Finalized values: `count`, `sum`, `uniqExact` are the stored number (range-checked);
  `avg` is the exact sum rounded to the nearest `float64`, divided by the count as a
  `float64`; `uniq` is the estimate of §7.5; quantiles are §7.5's read rule.
- Read-time aggregates of finalized values (`sum(c)`, `countIf(…)`, `uniq(key)`) use the
  same states in memory. A read-time `uniqExact` is a set in memory.

### 7.3 Exact `Float64` sums

A `Float64` sum, and the sum inside a `Float64` `avg`, is kept exactly as M·2^E with M odd,
or M = 0 and E = 0, so each value has one form:

- **Add x:** split x into an integer mantissa and an exponent with `math.Frexp` (exact), align
  the exponents, add as big integers, then remove trailing zero bits.
- **Bytes:** a signed varint of E, a sign byte (0 positive, 1 negative), then |M| big-endian
  without leading zero bytes. The exact zero is an empty B, which is not the same as an
  absent attribute.
- **Finalized value:** the `float64` nearest to M·2^E (ties to even).
- Sizes: 9 to 20 bytes for typical data, at most about 270 bytes for any finite inputs.

### 7.4 `uniqExact` markers

For each distinct value of a `uniqExact` column in a logical row, one marker item records
which record introduced it.

- **Identity:** `id = H(uvarint(len(pk)) ‖ pk ‖ uvarint(len(lsk)) ‖ lsk ‖ uvarint(len(col)) ‖ col ‖ canon)[0:16]`:
  - `pk`: the row's partition key
  - `lsk`: the row's sort fields joined by `#`, without a `SPLIT` suffix; empty when the view
    has none
  - `col`: the aggregate column's name
  - `canon`: the value's canonical bytes (§4.4)
- **Key:** `pk` = `!` + `base32(id)` (27 bytes), `sk` = `!`. Row keys never start with `!`.
- **Attributes:** `_o`, the owner (S); `_ttl`, the row's `_ttl` when the view has a `TTL`,
  so a row and its markers expire together.
- **Owner:** one string per record and applier, so live projection and a backfill never both
  count one value. Fixed widths, no separators:
  - live projection: `L` + unit key (13) + sequence number (40)
  - backfill: `B` + unit key (13) + sequence number (40)
  - `SYNC` appends: `E` + `event_id` (26)
- **Counting:** a record's values are first deduplicated per (record, row, value). Missing
  markers are written only if absent. Live projection and a backfill count a value when its
  marker's owner is the record's owner string, including a marker that the same record
  wrote in an earlier attempt.
- **Counting in a `SYNC` append:** a value counts only when its marker was absent when the
  append read it, and the append's transaction writes the marker. A present marker never
  counts, whatever its owner. An earlier attempt's marker committed with its event, which
  the event's own condition then finds. A present `E` marker with the append's `event_id`
  belongs to a `DEDUP BY` event appended again after its item expired, and its value is
  already counted.

### 7.5 Sketches

**`uniq`: HyperLogLog with 2^12 registers.**

- Input: h = `BE64(H(canon)[0:8])`. Register `idx = h >> 52`. Rank
  `= min(LeadingZeros64(h << 12) + 1, 31)`, so a register holds 1 to 31 (0 is empty) in
  5 bits. Merge is the register-wise maximum.
- **Sparse bytes:** `0x01`, then for each non-empty register in index order,
  `uvarint(v − prev)` with `v = idx << 5 | rank` and `prev` starting at 0.
- **Dense bytes:** `0x02`, then 4,096 registers × 5 bits, least significant bit first:
  register i holds bits 5i to 5i + 4 of the bit stream, where bit j is bit `j mod 8` of byte
  `j / 8`. 2,561 bytes in all.
- The sparse form is used exactly when it is shorter than the dense form. Both stand for
  the same registers, so equal registers give equal bytes and estimates don't jump at the
  switch. The decoder rejects anything else: zero deltas, rank 0, indexes past 4,095,
  trailing bytes, or a dense sketch that would be shorter sparse.
- **Estimate:** Ertl's improved raw estimator ([arXiv:1702.01284][ertl], Algorithm 6) with
  m = 4,096 and q = 30, computed with `+ − × ÷` and `math.Sqrt` only. An empty sketch
  estimates 0. Estimates are never stored.

**`quantile`, `quantileDD`: DDSketch with log-spaced buckets** ([DDSketch][ddsketch]).

- **Buckets per octave:** for relative accuracy α (0.01 for `quantile`), M is the smallest
  M ≥ 1 with 2^(1/M) ≤ γ = (1 + α)/(1 − α). It is computed with `math/big` floats of 256 bits,
  rounding to nearest even. α = 0.01 gives M = 35.
- **Tables,** built once per α: g = 2^(1/M) by Newton's method in `math/big`; T[j] = g^j
  rounded to `float64`, with T[0] = 1 and T[M] = 2; Rep[j] = 2·T[j]·T[j+1]/(T[j] + T[j+1])
  computed in `math/big` and rounded once.
- **Index of x > 0:** with `f, e := math.Frexp(x)` and u = 2f in [1, 2), the index is
  (e − 1)·M + j, where j counts the T[1] … T[M−1] that are ≤ u. **Value of index i:**
  `math.Ldexp(Rep[i mod M], ⌊i / M⌋)`, with floor division.
- **State:** a zero count, a positive store (indexes of x) and a negative store (indexes of
  |x|), all counts `uint64`.
- **Cap:** in each store, every count with an index below `top − 64·M + 1`, where `top` is
  the store's largest index, is added to the count at that index. Values more than 2^64
  times smaller in magnitude than the largest of their sign merge into one bucket. The cap
  only rises, so the state depends only on the inputs, not on their order or how they were
  merged.
- **Bytes:** `0x01`, `uvarint(zero count)`, then the positive store and the negative store,
  each as `uvarint(number of buckets)`, then for the first bucket `varint(index)` and
  `uvarint(count)`, and for each later one `uvarint(index − previous index)` and
  `uvarint(count)`. Only non-zero counts appear.
- **Quantile at `level`:** with n the total count, rank = level × (n − 1). Walk the negative
  buckets from the largest |x| down, then the zero count, then the positive buckets up, and
  return the first bucket whose running count is greater than rank: −value, 0 or value.
  n = 0 gives `NULL`.
- A bucket's value is within α·|x| of each x in it, except that for a subnormal x
  (|x| < 2^−1022) `Ldexp` rounds the value to a multiple of 2^−1074, so the bound is
  α·|x| + 2^−1075.
- `rel_acc` must be from 0.002 up to but not including 1
  ([sql.md §7.2](sql.md#72-function-reference), DF5205). At 0.001 (M = 347) one sketch's
  worst case reaches 400 KB (§7.8), and smaller values grow without bound (α = 10⁻⁶ gives
  M = 346,574).

### 7.6 High-water marks

`_hwm` is a map:

| Key | Value (B) |
|-----|-----------|
| a unit key (§4.2) | The last sequence number of that unit applied to this item, as §5.3. |
| `B` | The backfill's mark: the 8 bytes `H(u)[0:8]` of the last applied record's unit, then that record's sequence number as §5.3. The backfill applies records in order of (unit key, sequence number), and the mark compares in that order. |

- A backfill that reads the stream table
  ([lifecycle.md §5.2](lifecycle.md#52-sources)) gets items without a unit or a sequence
  number. In place of both, in the `B` value and in its marker owners (§7.4), it uses a
  record's *time mark*: the 8 bytes are `event_time` in microseconds, big-endian two's
  complement with the sign bit flipped, and the sequence number is the 128-bit number that
  `event_id` encodes (§4.1), stored as §5.3 stores sequence numbers. Time marks compare by
  `event_time`, then `event_id`. A version keeps its split for life (§8.2), so its rows
  never mix the two kinds of mark.
- When the projector writes an item, it removes each live entry whose unit's `progress`
  (§8.1) is at or above the entry and whose `progress_at` is more than
  [`runtime.prune_window_seconds`](config.md#runtime) in the past. The `B` entry is never
  removed; it is one entry.
- A `SYNC` append's transaction writes a row with `_hwm` as it read it, `B` included, and
  adds no entry: the event and its rows commit together, so the event's presence records
  that they hold it. The next projector write of the row prunes as usual.
- An entry costs 13 bytes of name plus about 10 bytes of value and 1 byte of map overhead
  ([item size][itemsize]).

### 7.7 Expiry

- A row's expiry depends only on its key, so its items and markers share one `_ttl`.
- The projector skips a contribution to a row that has expired by its own clock, so an
  expired row never comes back. A `SYNC` append does the same by its writer's clock, and
  still names the version in `_sync` (§6).

### 7.8 Size bound

A row item's size must not depend on the region or on the order of events, or a write could
pass DynamoDB's 400 KB limit in one region only. So every part of an item except `_hwm` has
a largest size known from the definition, assuming at most 2^64 inputs per row, and
`validate` rejects a view whose largest item (key, key columns, every state, attribute names
included) passes **400 KB minus a 32 KB reserve** for `_hwm` (about 1,300 entries), `_v`,
`_ttl` and slack (DF3307, [sql.md §7.2](sql.md#72-function-reference)). `agg.MaxSize` gives
the state sizes:

| State | Largest value |
|-------|---------------|
| `count`, `uniqExact` | 20 digits |
| integer `sum` | 40 characters (an S past 38 digits, §5.2) |
| `Decimal` `sum` | 60 characters |
| `Float64` `sum` | about 280 bytes (§7.3) |
| `avg` | its sum and count, plus 4 bytes of list overhead |
| `min`, `max`, `any` | the value: a `String` is at most 4 KB (4,096 bytes), because the 4 KB cap applies to every value that reaches the aggregate ([sql.md §7.2](sql.md#72-function-reference)) |
| `argMax`, `argMin` | both values, plus about 60 bytes for `event_time`, `event_id` and `array_index` |
| `uniq` | 2,561 bytes (dense) |
| `quantile`, `quantileDD` | 1 + 8 + 2 × (u + v + 8 + (64·M − 1) × 9) + 256 bytes, with u the length of `uvarint(64·M)` and v the length of `varint(−1074·M)`, the lowest index: the version byte, the zero count, and two full stores of 64·M buckets, each bucket after the first a 1-byte index step and a count. A count below 2^56 takes at most 8 bytes; with fewer than 2^64 inputs, at most 255 counts reach 2^56 and take a ninth byte, and one reaches 2^63 and takes a tenth, hence the 256. That is 40,593 bytes at α = 0.01 (M = 35) and 200,721 at α = 0.002 (M = 174) |

`String` key fields count at their DynamoDB limits; other fields have fixed widths. A row put
that still passes 400 KB carries an `_hwm` past the reserve. The projector fails that row
with an alarm, never the view's breaker, and pruning (§7.6) heals it.

## 8. Meta items

These are the items of the projector, the backfill, the operator commands and wait tokens.
Meta items are read-modify-written whole, conditional on `_v` (§7.1); a catalog body (§8.4)
is written once and never changes, and a receipt (§8.6) is written without a condition.
Each kind of item has partitions of its own, so the projector's query of the `view`
partition never meets another kind. Times are N values in Unix microseconds, rounded up to
the microsecond, so a stored deadline or activation time is never earlier than the real
one, and a wait measured from it is never short. `_ttl` is the exception: it is in seconds
(§5.4).

### 8.1 Unit item

One per delivery unit of each stream, in each region:

| Attribute | Value |
|-----------|-------|
| `pk` | `unit#`, the stream name, `#`, the unit key (§4.2): `unit#page_views#0F3KZ8M1QW7RD`. Each unit has its own partition. |
| `sk` | `!` |
| `id` | S: the unit's full ID, for diagnostics. |
| `progress` | S, 40 digits: the last sequence number fully applied and archived. Absent before the first advance. "Archived" means in this region's archive only when the region archives the stream with `archive.events: all` (below). |
| `progress_at` | N: the deadline, in microseconds, of the invocation that last raised `progress`: the one in Lambda's context, not the earlier cutoff at which the projector stops sending. |
| `fences` | M: view version ID (`page_daily@v2`) → S, 40 digits: the fence. Absent when there are none. |
| `_v` | N, as §7.1. |
| `_ttl` | 400 days after the last write, as §5.4. The item must outlive every record of its unit that can still be delivered, which needs at least 25 hours. |

In a region that archives the stream, the projector raises `progress` only after Firehose
accepted the archive records of the batch's `INSERT` records. With `archive.events: all`,
the region archives every `INSERT`, so its archive holds every event that the unit
delivered at or below `progress`, under the unit's ID and the record's sequence number: a
backfill can hand off at `progress` from that archive alone. With `archive.events: local`,
the region archives only the events it accepted. A replicated event at or below `progress`
is in the archive of the region that accepted it, under that region's unit and sequence
number, so a backfill can't hand off from this region's archive and reads the stream table
instead.

### 8.2 View-state item

One per view version, in each region:

| Attribute | Value |
|-----------|-------|
| `pk` | `view` |
| `sk` | The version ID, such as `page_daily@v2`. |
| `state` | S: `pending_backfill`, `activating`, `live`, `broken` or `retired`. A version without an item is `pending_backfill`, and no write sets that state. The moves are in [lifecycle.md §3](lifecycle.md#3-view-version-states): from `pending_backfill` to `activating` to `live`, from any state but `retired` to `broken`, and from any state to `retired`, which is final. |
| `stream` | S: the version's stream. A projector reads it to find the versions of its batch's stream that its catalog doesn't know ([lifecycle.md §4.1](lifecycle.md#41-guards)). |
| `activated_at` | N: when the state became `activating`. Present from `activating` on. Units are listed for fencing only after `activated_at` + 30 s (the longest a projector uses a view state it read) + 60 s. |
| `split` | S, with `activated_at`: `fence`, `time` or `none`, how the backfill and live projection split the version's events ([lifecycle.md §5.1](lifecycle.md#51-how-events-are-split)). It fixes the source of every job of the version: `fence` reads the archive, `time` and `none` the stream table. Written by the write that sets `activating`, and never changed. |
| `split_at` | N, only when `split` is `time`: the split time `B`, `activated_at` plus the job's listing delay and writer clock bound ε (90 s and 5 minutes). Live projection applies the version to an `INSERT` exactly when its `_it` is above `split_at`. |
| `split_late` | N, with `split_at`: `split_at` + ε + L, where L is the job's landing bound (15 minutes). The projector breaks the version, with cause `split`, on an `INSERT` with `_it` ≤ `split_at` in a batch it started at or after `split_late` by its own clock, and on a `MODIFY` whose old and new images have `_it` on different sides of `split_at`. |
| `partial_since` | N, set when a version filled from the stream table goes `live` without proof that the table held every event: the earliest `ingest_time` from which the version is complete: the later of the job's `read_after` (§8.3), the scan's earliest start, minus `LOG RETENTION`, and the time the job makes the version `live`, after the scan's end, minus `LOG RETENTION` minus 3,600 seconds, since DynamoDB may delete an item once its `_ttl` (§5.4) passed, also while the scan runs. Absent when the version is complete: the stream uses `LOG RETENTION FOREVER`, or its table's earliest replica was created more than the writer clock bound ε (5 minutes) after that time, so no event before it existed. |
| `broken` | M, once the version broke: `cause` (S: `record` for an evaluation error, `split` for an event the time split can't place), `unit` (S, the full unit ID), `seq` (S, 40 digits), `event_id` (S), `error` (S, cut to at most 1,024 bytes), `at` (N). A record that a backfill read from the stream table has no unit or sequence number: its `unit` and `seq` are empty strings, and `event_id` identifies it. |
| `retired_at` | N, only when `state` is `retired`: when the version was retired. |
| `_v` | N, as §7.1. |

A move to `broken` or `retired` keeps every attribute the item had, so a retired version
that broke first keeps its `broken` map.

### 8.3 Job item

One per view version that a backfill job was started for, in each region
([lifecycle.md §5.4](lifecycle.md#54-jobs),
[ADR 0011](../decisions/0011-backfill-coordinator.md)). The job's steps read and write it;
it stays after the job ends.

| Attribute | Value |
|-----------|-------|
| `pk` | `job` |
| `sk` | The version ID. |
| `stream` | S: the version's stream. |
| `source` | S: `archive` or `table`, which follows from `split`. |
| `split` | S: the view-state item's `split` (§8.2): `fence` with the archive source; `time` (additive versions) or `none` (idempotent versions) with the table source. |
| `state` | S: `running`, `cancelled`, `done` or `broken`. |
| `phase` | S: `activate`, `fence`, `fresh`, `watermark`, `wait`, `stage`, `apply` or `live`. |
| `attempt` | N, at least 1: the attempt that owns the job. Resuming the job claims it for the next attempt. |
| `name` | S: the name of the job's executions without the `-a<attempt>` and `-c<k>` suffixes. |
| `execution` | S: the ARN of the execution that ran the last step, or `local` when `dynaflux dev` or a test runs the job. |
| `created_at`, `updated_at` | N. |
| `cut` | N, with the archive source: the time the fences were taken. |
| `read_after` | N, with the table source: the earliest time the scan may start. With the time split it is `split_late` plus the clock margin (60 s), otherwise `activated_at` plus the listing delay (90 s). |
| `scanned_at` | N, with the table source: when the scan ended. The version holds every event from that time less the stream's `LOG RETENTION` and an hour ([lifecycle.md §5.2](lifecycle.md#52-sources)). |
| `fresh` | BOOL, once `done`: whether the fresh-stream rule made the version live ([lifecycle.md §4.3](lifecycle.md#43-fresh-streams)). |
| `staging` | S: the staging prefix in the region's bucket, `backfill/<version>/<created_at>/`. With the archive source it also holds the slice plan, `plan-<attempt>-<random>`, and each try of a slice's export, `unload/<plan>/<from>-<to>/<try>/` ([ADR 0016](../decisions/0016-archive-export-with-athena.md)). |
| `stage` | M: where staging resumes. With the table source: `segments` (N, the scan's segment count), `next` (M, a segment number → M with the `pk` and `sk` of the last item staged from it), `finished` (L of N, the segments read to their end). With the archive source, where a field that is zero, empty or false is absent: `plan_query` (S, the ID of the Athena query that lists the table's partitions, saved before the job waits for it), `plan` (S, the name of the committed plan object), `export` (M, the current try of the slice being staged: `from` and `to`, N in microseconds, `try` N, `failed` N, the slice's failed tries that count against its three, and `id` S, the query ID, saved before the job waits for it and absent until the try starts), `slice` (N, the index in the plan of the slice being staged; the slices before it are staged), `query` (S, that slice's succeeded query, whose manifest lists its files), `file` and `skip` (N, the manifest index of the file being staged and the rows already staged from it), `rows` (N, the rows staged from the slice, for its integrity check), `errors` (M: `after` S, the last object of the error prefix staged; `done` BOOL; `foreign` N, the lines skipped because Dynaflux didn't write them), `scanned` (N, the bytes Athena scanned for the job) and `tries` (N, the tries that failed and the restages, in all, which the token of the partitions' listing counts). |
| `runs` | L of M: the committed runs, each with `name` (S), `records` (N), and `first` and `last` (B, the marks of its first and last records, as §7.6's `B` value). A run in the staging prefix that this list doesn't name is garbage. A run never spans a commit of `stage`. |
| `merge` | M, during a merge pass: `inputs` (L of S, the run names), `outputs` (L, as `runs`), `mark` (B, the last mark written). |
| `staged` | N: records staged. |
| `mark` | B, as §7.6's `B` value: the cursor, the mark of the last record of the last completed chunk. Absent before the first chunk completes. |
| `applied`, `skipped`, `chunks` | N: the records applied and skipped, and the chunks completed. |
| `rate` | N: the most records per second to apply, 0 for no limit. |
| `paused` | BOOL, true while apply waits for the projector's lag to fall under 2.5 minutes ([lifecycle.md §5.4](lifecycle.md#54-jobs)). Absent otherwise. |
| `error` | M, the last failure: `phase` (S), `message` (S, cut to at most 1,024 bytes), `at` (N). |
| `_v` | N, as §7.1. |
| `_ttl` | 400 days after the job became `done` or `broken`, as §5.4. None while `running` or `cancelled`. |

### 8.4 Deployed catalog

The catalog that the region's functions run, registered by the setup function at each
deploy ([lifecycle.md §9](lifecycle.md#9-the-deployed-catalog)): one body item per catalog,
which never changes, and one item that names the current catalog.

| `pk` | `sk` | Attributes |
|------|------|------------|
| `catalog#<hash>` | `body` | `body` (B): the gzip of the bytes that `catalog.Marshal` returns. Written only if absent; a failed condition whose stored item is equal counts as success. No `_ttl`. |
| `catalog` | `current` | `hash` (S): the lowercase hex SHA-256 of the catalog's bytes, the `<hash>` of its body item; `format` (N): the catalog format; `generator` (S): the version of the Dynaflux module that generated the catalog; `registered_at` (N); `_v` (N, as §7.1). |

A reader gets `current`, then the body, with consistent `GetItem`s, decompresses at most
16 MiB, checks the hash and decodes the catalog. `generate` refuses a catalog whose gzip
passes 350,000 bytes (DF6019 in [config.md](config.md#codes)), so a body fits DynamoDB's
400 KB item limit.

### 8.5 Stream item

One per stream, in each region: the region's archive mode for the stream, and the history
that decides whether a backfill may trust the region's archive of it
([lifecycle.md §5.5](lifecycle.md#55-when-the-archive-is-complete),
[ADR 0017](../decisions/0017-archive-completeness.md)).

| Attribute | Value |
|-----------|-------|
| `pk` | `stream#`, then the stream name: `stream#page_views`. |
| `sk` | `!` |
| `mode` | S: `all`, `local` or `off`, the region's archive mode for the stream as the last deploy set it ([infra.md §6.6](infra.md#66-archive-mode)). `all` exactly when the region archives every `INSERT` of the stream (§8.1). |
| `mode_at` | N: when `mode` was last written. |
| `origin` | M, once recorded: `at` (N), `hash` (S, the `hash` of `catalog/current` then, §8.4), `registered_at` (N, that catalog's), `born` (N, the earliest `CreationDateTime` of the stream table's replicas then), `arn` (S, the stream's ARN in this region) and `holes` (N, the length of `holes` when the origin was written). |
| `holes` | L of M, in the order written, never removed. Each has `at` (N, the writer's clock, for display only) and `cause` (S): `update` or `create` when a deploy changed the mode to or from `all`, with `from` and `to` (S, the modes); `lag` when a projector handled a record above its unit's `progress` that arrived at least 23 hours 50 minutes before; `errors` when a backfill job found an object in the error prefix that isn't readable Dynaflux records, named in `object` (S). |
| `checked_through` | N, once set: how far the last passing check of projector activity reached. |
| `unchecked_before` | N, once set: the hours since the origin before it went unchecked, since CloudWatch kept no hourly data that old when a passing check started. |
| `_v` | N, as §7.1. |

- There is no `_ttl`. The item is written whole, conditional on `_v`, so the order of
  `holes` is the order of the writes.
- An origin counts only the holes at an index at or after its own `holes`. No rule compares
  `origin.at` with a hole's `at`: they come from the clocks of different hosts.
- A deploy writes `mode` and its hole in one write, before any function changes. A stream
  without a stream item gets one with its mode and no hole. A projector or a job adds a
  `lag` or `errors` hole only to an item that exists, and only when none of its cause
  counts against the origin yet: without an item, there is no origin for it to count
  against.
- An origin is written when there is none, or when the one there is void: its `arn` is no
  longer the stream's ARN in this region, as when the table, its replica here or its stream
  was re-created. A change of `born` alone, as when the oldest replica's region leaves,
  doesn't void it. The holes of a void origin stay, at indexes before the new one's.

### 8.6 Receipt

One per event appended with `dynaflux.Waitable()`, in the append's region, written by that
region's projector once it has handled the event's record
([sql.md §9.7](sql.md#97-mode), [ADR 0015](../decisions/0015-wait-tokens-and-receipts.md)).

| `pk` | `sk` | Attributes |
|------|------|------------|
| `rcpt#<stream>#<event_id>#<_it>` | `!` | `_ttl` (N): the write time plus 25 hours, in Unix seconds. |

- `<_it>` is the stored event's `_it`, in microseconds. It ties the receipt to one stored
  record: a `DEDUP BY` event appended again after its item expired has the same
  `event_id`, and the older receipt, which TTL may delete late, would otherwise answer for
  the new record.
- The projector writes it for each `INSERT` with `_rcpt` whose `_r` is its own region,
  once it has raised `progress` past the record: for records below the batch's lowest
  failed record, and for those at or below `progress` that a redelivery holds. A record
  that reaches no version, a fenced one or one whose versions `_sync` names, gets one
  too: a receipt means the record was handled. A receipt that fails to write fails its
  record, and the redelivery writes it.
- Its 25 hours outlive every change record, which DynamoDB keeps for 24 hours, so a
  missing receipt always means "not yet".
- `WaitFor` reads receipts with consistent `BatchGetItem`s. The projector never reads them.

## 9. Determinism rules

Every stored state depends only on the multiset of inputs applied, not on order, item,
region or CPU ([sql.md §7.5](sql.md#75-state-classes)). Code that computes states or
estimates follows these rules:

1. No math-library functions whose results may differ by CPU: no `math.Log`, `Exp`, `Pow`,
   `Log2`, `Cbrt` or the like. Use `+ − × ÷` and `math.Sqrt`, which IEEE 754 rounds
   correctly, and the exact `Frexp`, `Ldexp`, comparisons and `Round`.
2. Every product or quotient whose result feeds another operation is converted with an
   explicit `float64(…)`. The Go spec allows `x*y + z` to be fused, even across statements;
   an explicit conversion forbids it ([Go spec][gofp]).
3. Tables are computed with `math/big`, whose rounding is fully specified.

[types]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
[query]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
[quotas]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html
[itemsize]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
[ttl]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html
[rwops]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html
[streamrecord]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamRecord.html
[ulid]: https://github.com/ulid/spec
[ertl]: https://arxiv.org/abs/1702.01284
[ddsketch]: https://arxiv.org/abs/1908.10693
[gofp]: https://go.dev/ref/spec#Floating_point_operators

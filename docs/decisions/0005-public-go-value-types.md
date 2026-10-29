<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0005: Go types for SQL values in generated code

- Status: Accepted
- Date: 2026-10-26
- Amended by [ADR 0019](0019-v1-compatibility-promise.md): `UUID` stays a defined type for
  all of v1 and never becomes an alias of `uuid.UUID`; on Go 1.27, callers convert, as in
  `dynaflux.UUID(uuid.NewV7())`

## Context

`dynaflux generate` writes Go structs for events, query parameters and query rows. Each SQL
type needs one Go type, used both for values the caller passes (appends, parameters) and for
values Dynaflux returns (stream reads, query rows). Most types map to Go builtins. Four don't
have an obvious answer:

- `Decimal(P, S)`: exact, up to 38 digits, which needs 128 bits. The standard library has no
  decimal type.
- `UUID`: Go 1.27 added a standard `uuid` package with `type UUID [16]byte`
  ([pkg.go.dev/uuid][uuid]), but Dynaflux's minimum Go version stays 1.26 until Go 1.28
  ships.
- `Nullable(T)`.
- `Date`: a calendar day. Every time in Dynaflux is UTC
  ([sql.md §4.1](../spec/sql.md#41-scalar-types)), and a Go `time.Time` is an instant with a
  location.

The project's dependency rule asks for the standard library first, and anything in this list
is imported by every user's generated code.

## Decision

| SQL type | Go type |
|----------|---------|
| `String`, `LowCardinality(String)` | `string` |
| `UInt8` … `UInt64`, `Int8` … `Int64` | `uint8` … `uint64`, `int8` … `int64` |
| `Float64` | `float64` |
| `Bool` | `bool` |
| `Decimal(P, S)` | `dynaflux.Decimal` |
| `UUID` | `dynaflux.UUID` |
| `Date` | `time.Time` |
| `DateTime64(3)`, `DateTime64(6)`, `DateTime` | `time.Time` |
| `Nullable(T)` | `*T` |
| `Array(T)` | `[]T` |

### `Decimal`

```go
// Decimal is an exact decimal number of at most 38 digits that keeps its scale.
type Decimal struct{ /* 128-bit unscaled value and a scale; comparable, zero value is 0 */ }

func ParseDecimal(s string) (Decimal, error)
func DecimalFromBig(unscaled *big.Int, scale int) (Decimal, error)
func (d Decimal) Big() (unscaled *big.Int, scale int)
func (d Decimal) Compare(e Decimal) int // by value, across scales
func (d Decimal) Equal(e Decimal) bool  // by value: 12.5 equals 12.50
func (d Decimal) String() string        // "12.50"
func (d Decimal) MarshalText() ([]byte, error)
func (d *Decimal) UnmarshalText(b []byte) error
```

- `==` compares the representation, scale included, as `time.Time`'s `==` compares the
  location: `ParseDecimal("12.5")` and a `Decimal(10, 2)` read of `12.50` differ under `==`
  and are `Equal`. Values read from one `Decimal(P, S)` column always have scale S, so `==`
  works within a column. `Equal` makes go-cmp compare by value.
- `Compare` and `Equal` follow the names of `time.Time`, `netip.Addr` and `uuid.UUID`.
- On append and as a parameter, a value whose exact value has more than S fractional
  digits is rejected with `ErrInvalidValue`, never rounded: `1.234` into `Decimal(10, 2)`
  fails, `1.230` and `1.2` are accepted as `1.23` and `1.20`. More than P digits at scale S
  is rejected too ([sql.md §4.3](../spec/sql.md#43-value-restrictions)).
- Arithmetic is not offered: views compute inside Dynaflux, and `Big` bridges to other
  libraries.

### `UUID`

```go
type UUID [16]byte

func ParseUUID(s string) (UUID, error)          // the four forms of Go 1.27's uuid.Parse
func (u UUID) String() string                   // lowercase 8-4-4-4-12
func (u UUID) Compare(v UUID) int
func (u UUID) AppendText(b []byte) ([]byte, error)
func (u UUID) MarshalText() ([]byte, error)
func (u *UUID) UnmarshalText(b []byte) error    // same forms as ParseUUID
```

- The methods are a subset of Go 1.27's `uuid.UUID`, with the same behavior. `ParseUUID` and
  `UnmarshalText` accept exactly what Go 1.27's `uuid.Parse` accepts: `xxxxxxxx-xxxx-…`,
  `{…}`, `urn:uuid:…` and 32 bare hex digits, either case, tested against the same inputs.
  So both types decode the same text, and callers on Go 1.27 convert for free with
  `uuid.UUID(u)` and `dynaflux.UUID(id)`. `UUID` stays a defined type for all of v1
  ([ADR 0019](0019-v1-compatibility-promise.md)).
- SQL literals and HTTP parameters keep their strict 8-4-4-4-12 parsers in their own code.
- Making one, as `UUID`'s doc comment says: on Go 1.26, `rand.Read(id[:])` from
  `crypto/rand`; on Go 1.27, `dynaflux.UUID(uuid.NewV7())`.

### `Nullable(T)`

`*T`, and nil is `NULL`. With Go 1.26, `new(expr)` takes a pointer to any value:
`Referrer: new("https://example.com")`.

### Times and dates

- `DateTime64(p)` and `DateTime`: values Dynaflux returns are in UTC, carry no monotonic clock
  reading, and have at most p fractional digits, so `==` works on them. Values passed in are
  truncated to p fractional digits ([sql.md §4.3](../spec/sql.md#43-value-restrictions)),
  whatever their location.
- `Date`: a value Dynaflux returns is midnight UTC of the day. A value passed in stands for
  **the UTC date of its instant**, `t.UTC()`'s year, month and day, which is what
  `toDate(t)` gives in Dynaflux and in ClickHouse with `session_timezone = 'UTC'`. So
  `time.Date(2024, 5, 31, 0, 0, 0, 0, tokyo)` is 2024-05-30.

### Arrays

`[]T`. On input, nil and empty both mean `[]`. On output an empty array is **nil**, Go's
usual empty slice, so a test's want literal that leaves the field out equals what was read
(`reflect.DeepEqual`, go-cmp). Arrays are never `NULL`, so nil is unambiguous.

### No JSON contract

Generated types have no struct tags and promise nothing about `encoding/json`. The SQL names
in the catalog are the contract across languages: the M6 HTTP handler encodes its JSON forms
(`Decimal` and 64-bit integers as strings, dates as `YYYY-MM-DD`) from catalog types, and
each language's generator derives its own field names from the SQL names (`from_` in Python
for a parameter named `from`), so DF1207 doesn't grow per language.

### Input checks

Values that break [sql.md §4.3](../spec/sql.md#43-value-restrictions) fail with a
`*ValueError`:

```go
type ValueError struct {
	Name string // the column or parameter, as in SQL
	Err  error  // ErrInvalidValue, or a more specific sentinel
}
func (e *ValueError) Error() string
func (e *ValueError) Unwrap() error
```

Causes: invalid UTF-8, a non-finite or out-of-range `Float64`, a `Decimal` outside its type,
a time outside 1970–2106, more than 1,000 array elements. `errors.Is(err, ErrInvalidValue)`
holds, and `errors.As` gives the name the M6 handler and later clients need without parsing
text. A wrong Go type is an error, never a panic.

## Alternatives

| Type | Option | Why not chosen |
|------|--------|----------------|
| `Decimal` | `string`, validated | Not type-safe; `"1.5"` and `"1.50"` differ; every caller parses. |
| | `github.com/shopspring/decimal` | A dependency in every user's generated code; arbitrary precision and float constructors invite inexact values. |
| | `*big.Rat`, `github.com/cockroachdb/apd` | `Rat` loses the scale and is a pointer; `apd` is large for parse, format and compare. |
| | Round extra fractional digits on input (half to even) | Silent change to the caller's value; sql.md keeps values exact elsewhere. |
| `UUID` | `uuid.UUID` from the standard library now | Needs Go 1.27 as the minimum, while Dynaflux supports Go 1.26. |
| | `github.com/google/uuid` | A dependency for a 16-byte value; the standard type replaces it. |
| | `string`, `[16]byte` | Letter case makes equal strings unequal; a bare array prints as bytes. |
| | Strict 8-4-4-4-12 text decoding | Text that `uuid.Parse` accepts would fail here, so the two types would decode different text. |
| `Nullable` | `sql.Null[T]` (Go 1.22) | Brings `database/sql` into generated code; `Null[T]{V: x, Valid: true}` is wordier than `new(x)`. |
| | A `dynaflux.Null[T]` type | One more public type for what `*T` already says. |
| `Date` | `time.Time`, calendar date in the value's own location (`t.Date()`) | Matches local midnights, but disagrees with `toDate(event_time)` and with the UTC day buckets of views: `time.Now()` in Tokyo at 08:00 would name the next UTC day. |
| | A civil `dynaflux.Date{Year, Month, Day}` | No ambiguity, but every caller converts to and from `time.Time`. |
| | Reject inputs that aren't midnight UTC | Unambiguous, but `time.Now()` can't be passed directly. |
| Arrays, JSON | Non-nil empty arrays and `json:"<sql_name>"` tags (`,string` on 64-bit integers) | A JSON contract on Go types that still differs from the HTTP JSON forms for `Date` and times, and want literals must spell out every empty array. |

## Consequences

- The root package gains `Decimal`, `UUID` and `ValueError`, about 350 lines with tests, and
  no dependency.
- Generated code imports only `context`, `iter`, `time`, `github.com/lucindo/dynaflux` and
  `github.com/lucindo/dynaflux/fluxgen` ([ADR 0006](0006-generated-code-contract.md)).
- Callers in time zones east or west of UTC must build `Date` values in UTC, such as
  `time.Date(y, m, d, 0, 0, 0, 0, time.UTC)`. Generated doc comments on `Date` fields say so,
  and [sql.md §4.3](../spec/sql.md#43-value-restrictions) states the rule.
- Rows compare with `==` field by field, except slices and pointers, and with
  `reflect.DeepEqual` or go-cmp as a whole.
- A query's `Params` type has the same fields, in the same order, as its HTTP body; it
  doesn't share the body's JSON form.

[uuid]: https://pkg.go.dev/uuid

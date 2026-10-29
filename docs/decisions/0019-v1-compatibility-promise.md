<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0019: v1.x keeps every shipped boundary compatible, not only the Go API

- Status: Accepted. Amends [ADR 0005](0005-public-go-value-types.md),
  [ADR 0006](0006-generated-code-contract.md) and [ADR 0010](0010-runtime-entry-point.md),
  whose decisions stand: `UUID` stays a defined type, `dynaflux.AsImport()` replaces
  `Import<Stream>`, the runtime package is `fluxruntime`, the rules for
  `GeneratedCodeVersionN` are spelled out, and later options on `Run` come as a new
  function.
- Date: 2026-10-28

## Context

M7 prepares the first tagged release. Nothing is tagged yet, so every shape below can still
change at no cost. The first tag, even a prerelease, fixes the module path. After `v1.0.0`,
a break needs a new one, `github.com/lucindo/dynaflux/v2`.

The project counts every shipped boundary as public: the library, the CLI, and the wire
and storage formats. A review of the Go API alone misses the changes that break users
without a compile error:

- **View fingerprints.** If a release computes a different fingerprint for an unchanged
  view, `generate` numbers a new version, and every user faces a backfill.
- **Token text** is a wire format: `decodeToken` rejects any first byte other than
  `tokenFormat = 1`. During a rolling deploy, an older process must read the tokens of a
  newer one.
- **Generated names.** DF1207 checks package-level types and `Client` methods in one
  namespace. A release that generates a new name for a construct that already exists can
  collide with a user's query name, and a project that validated stops validating.
- **Logical IDs.** A changed logical ID makes CloudFormation replace the resource, a table
  included.

Three earlier decisions need a rule they don't have:

- [ADR 0006](0006-generated-code-contract.md) raises `GeneratedCodeVersionN` for a catalog
  format bump or an incompatible change to `fluxgen`, but not for an addition. Code from a
  newer generator that calls a new `fluxgen` function under the same N then fails on an
  older library with `undefined: fluxgen.X`, not at the guard. That mismatch is realistic,
  since `go install …@latest` doesn't pin the CLI to the library version in `go.mod`. The
  comment in `internal/catalog/json.go` also says that a format bump "replaces"
  `GeneratedCodeVersion1`, where ADR 0006 keeps every N.
- [ADR 0005](0005-public-go-value-types.md) plans `type UUID = uuid.UUID` once the minimum
  Go version is 1.27, which plan D18 ties to the release of Go 1.28, inside v1.x. That
  switch breaks Go 1 compatibility: a type switch that lists both types stops compiling,
  and `gorelease` reports it.
- [ADR 0010](0010-runtime-entry-point.md) plans to add `...Option` to `Run` later. Adding a
  parameter, variadic included, changes a function's type, so a function value or an
  interface built over it stops compiling.

The plan also calls its later scopes "v2" and "v3", and a view version is written
`page_daily@v2`. Next to a Go module, a bare "v2" reads as a breaking release.

## Decision

### What v1.x keeps

| Surface | Promise | Pinned by |
|---------|---------|-----------|
| Root package, `fluxtest`, `fluxruntime` | <ul><li>Go 1 compatibility: names and signatures are only ever added.</li><li>A function never gains a parameter, variadic included. Later options on `fluxruntime.Run` or `fluxtest.NewStore` come as new functions, such as `RunWith`.</li></ul> | `gorelease` against the previous tag, with no accepted exception |
| `fluxgen` and generated code | The version-guard rules below | the release checklist's version-guard step; `TestExampleIsGenerated` |
| Generated names | The generated-name rule below | codegen goldens; [sql.md §3.2](../spec/sql.md#32-annotations) |
| Projects (SQL, `dynaflux.yaml`) | <ul><li>A project that validates keeps validating, and generates the same API.</li><li>New warnings may appear.</li><li>A new error appears only to stop data from being lost or miscounted, and the changelog says so.</li></ul> | validate goldens; the tests of sql.md and config.md |
| View versions | <ul><li>An unchanged definition keeps its fingerprint, so no release starts a backfill by itself.</li><li>Defaults whose change would lose data, change results or change the billing model stay: retention, archive, TTL and capacity mode.</li><li>The batching window may change in a minor release, after a measurement, with a changelog entry and `WaitFor`'s timing note updated.</li></ul> | the locks in the goldens and the example |
| Storage, resource names, logical IDs | Every v1.x release reads what any v1.x release stored. Names and logical IDs never change. | the `internal/keys` and `internal/agg` tests; infra goldens |
| Catalog JSON, embedded and deployed | `MinFormatVersion` stays 1. | a fixture |
| Token text | The token rule below | a fixture per format |
| `dynaflux.lock` | Format 1 is read throughout v1. | lock goldens |
| CLI | <ul><li>Command names, flags, exit codes and `-json` lines (`"format": 1`) stay.</li><li>Human-readable text may change.</li><li>A diagnostic code keeps its meaning.</li></ul> | the flag lint; CLI goldens |
| HTTP ([http.md](../spec/http.md)) | Routes, bodies and value forms stay; fields are only added. | `internal/queryhttp` tests |
| Templates | Parameters, outputs, alarm names, and metric names and dimensions stay. Resources may be added. | infra goldens |
| Not promised | `explain`'s numbers and text, diagnostic message text, `internal/…`, and template details beyond the rows above | — |

The promise has no exception.

### Rules

1. **Version guard.**
   - Codegen writes `GeneratedCodeVersion{N+1}` whenever new generated code needs something
     the previous release lacks: a catalog `FormatVersion` bump, a new or changed `fluxgen`
     function, a new root identifier that generated code uses, or a changed meaning. Code
     from a newer generator then fails at the guard line, not with `undefined: …`.
   - Every v1.x release keeps every constant it defined. `fluxgen` only grows: a changed
     helper becomes a new function next to the old one.
   - Removing a constant needs `…/dynaflux/v2`.
2. **Generated names.** A later release generates new names only for SQL constructs that
   v1.0.0 doesn't have, such as methods for `CONSISTENCY = 'strong'` streams, so DF1207
   never rejects a project that validated. `Client` never gains a fixed-name method such as
   `Close`, since a query name can be any `[A-Z][A-Za-z0-9]*`.
3. **Tokens.** Each release reads token formats 1 to N. A release starts writing format
   N+1 only after a release that reads it has shipped, so a rolling deploy from the
   previous release never meets a token it can't read.
4. **`UUID`** stays a defined type, `[16]byte`, for all of v1. On Go 1.27 and later, the
   conversion is free both ways: `dynaflux.UUID(uuid.NewV7())` and `uuid.UUID(u)`.
5. **"v2".** The plan's later scopes ship as v1.x minor releases, unless one needs a break,
   which would be module `github.com/lucindo/dynaflux/v2`. The guides say "later releases"
   for the roadmap, and always write a view version in full, `page_daily@v2`, never a bare
   "v2".

### Before the tag

The promise freezes these shapes, so they change before `v1.0.0`, while no user has them:

- **`dynaflux.AsImport()`** replaces the generated `Import<Stream>` and
  `fluxgen.ImportEvents`. The method ignored its options and returned no `Token`; as an
  append option, an import honors `Waitable` and returns one.
- **`Token`** gains `MarshalText` and `UnmarshalText`, since a token in a JSON struct
  encodes as `{}` today. Format 1 shrinks from about 97 characters an event to about 25,
  before a fixture freezes it.
- **`ArchiveSizeError`** goes. The error still matches `ErrInvalidValue` and names the
  stream and the size.
- **`fluxtest.NewStore(tb, cat, more ...*dynaflux.Catalog)`** serves every catalog given,
  as one `Store` serves every generated package in production, and refuses a call through
  any other catalog before it writes.
- **`AppendOption`, `QueryOption` and `ReadOption`** become interfaces with an unexported
  method, so one constructor can serve several kinds of call.
- **Package `runtime` becomes `fluxruntime`.** Mains that users write for custom handlers
  would otherwise import it next to the standard `runtime`. The name matches `fluxgen`,
  `fluxtest` and `cmd/dynaflux-runtime`, and DF6301 reserves it in place of `runtime`.
- **DF6301 reserves `fluxgen` and `fluxtest` too.** Generated code imports `fluxgen`, and
  tests import `fluxtest` next to the generated package. After `v1.0.0`, reserving them
  would reject projects that validate.

### Rejected

- **A promise for the Go API alone:** fingerprints, tokens, names and formats would break
  users with no compile error to warn them.
- **`UUID` as an alias in a v1.x release,** as ADR 0005 planned: it breaks Go 1
  compatibility. **Go 1.27 as the minimum for `v1.0.0`, with the alias now:** users on Go
  1.26 couldn't adopt v1.0, although Go 1.26 keeps upstream support until about February
  2027.
- **A minor release that drops a `GeneratedCodeVersionN`,** with "regenerate" in the
  release notes: upgrading the library would break every build whose generated code is
  older.
- **Variadic options added later, with a documented `gorelease` exception:** new functions
  keep the check strict.
- **Today's token format, frozen as it is:** a 1,000-event token is 97,358 characters.
  **No token promise:** waits would break during rolling deploys.
- **`Import<Stream>` returning `(dynaflux.Token, error)` and honoring `Waitable`:** it keeps
  a generated name per stream and a `fluxgen` helper for good.
- **Keeping the name `runtime`, as ADR 0006 and ADR 0010 did:** every user-written main
  with custom handlers would need an import alias.

## Consequences

- From `v1.0.1` on, the release checklist runs `gorelease` against the previous tag, and
  its version-guard step on every release.
- Compatibility fixtures under `testdata/compat/v1/`, for the catalog and the tokens, are
  frozen at `v1.0.0`. A new format adds a fixture next to them.
- `GeneratedCodeVersion1`'s doc, `fluxgen`'s doc and the comment in
  `internal/catalog/json.go` state the version-guard rules.
- A feature that would want a new generated name on every stream can't have one in v1.x.
- A new validation error, or a new batching window, needs a changelog entry.
- Everything under "Not promised" may change in any release.

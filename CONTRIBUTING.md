<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Contributing

This page explains how to build and test Dynaflux, and how to keep the outputs in its docs
current. None of it needs an AWS account.

## Set up

You need Go 1.26 or later, and Docker for the integration tests. Once per clone, point Git
at the repository's hooks, which `make verify` checks for:

```sh
make setup-git-hooks
```

## Build and test

- `make help` lists the development targets. `make verify` runs the formatters,
  `go mod tidy`, a check that no module needs a Go newer than 1.26, the linters, the
  vulnerability check, the tests, each benchmark once and a `linux/arm64` build of the
  example's Lambda binary. If the linter reports an unknown linter, your `golangci-lint` is
  older than `.golangci.yml`: run `make tools-update`.
- `Float64` results must be the same on every CPU, so `make cross-arch` runs the
  cross-architecture tests as amd64: natively on an amd64 machine, and through Rosetta on
  Apple silicon. It isn't part of `make verify`.
- A test fails when `dynaflux generate` would change the files generated in
  [examples/analytics](examples/analytics): its Go code, CloudFormation templates and lock.
  After changing what generate writes, run `go run ../../cmd/dynaflux generate` in that
  directory.
- `make test` runs a smoke tier of the simulation harness. `make sim` and `make sim-controls`
  run its full tier and its negative controls, as
  [docs/runbooks/simulation.md](docs/runbooks/simulation.md) describes.
- `make integration` runs the packages that have `*_integration_test.go` files against
  DynamoDB Local in Docker. Each run recreates the container, which takes 1–2 s, so it
  starts from an empty emulator: DynamoDB Local keeps about 200 MB from every run even after
  the tests delete their tables, and its requests time out once it holds a few GB. Run one
  at a time on a machine, because a second run would recreate the emulator under the first.
  The container keeps running afterwards; `make emulators-down` stops it, and
  `make emulators-up` starts it without recreating a running one, for tests you run by hand.
  These tests never call AWS: they reach DynamoDB Local at the loopback address in
  `DYNAFLUX_DYNAMODB_ENDPOINT`, which the target sets, and fail without it. CI runs
  `make verify` and `make integration` on every push and pull request. The lane takes about
  50 s on an Apple M2 Max; it must stay under 3 minutes.
  `fluxruntime`'s tests also check every DynamoDB and Firehose call the projector made against
  the projector's role in the templates: a call the role doesn't allow fails the run and,
  after a run of all the tests, so does an action the role allows that no call used.
- `make e2e-emulator` runs the end-to-end suite on DynamoDB Local, as
  [docs/runbooks/e2e.md](docs/runbooks/e2e.md) describes.
- `make bench` runs the Go benchmarks, and `make bench-compare` compares them with the
  recorded baseline, as [docs/runbooks/benchmarks.md](docs/runbooks/benchmarks.md)
  describes.
- `make shellcheck` checks the shell scripts under `scripts/`, and the `sh` blocks of the
  README and the guides. CI's `infra-lint` job runs it.

## Docs

Tests in `internal/cli` keep the docs true:

- `TestDocs` runs the commands of the README and of the guides in `docs/guide`, and
  compares each output with the lines that its `console` block shows.
- `TestGettingStarted`, in `make integration`, follows
  [getting started](docs/guide/getting-started.md) on DynamoDB Local, in a new Go module.
  Only that guide may have `integration` blocks. `TestDocGo`, also in `make integration`,
  compiles the Go blocks marked `go`.
- Other tests check that, in every Markdown file of the repository, each `dynaflux` command
  parses, with flags of one dash, and each relative link and anchor exists. In the README
  and the guides, each diagnostic code must be in a spec, each alarm table must match the
  templates, names and severities, each dollar amount in the prose must be in a tested
  output, and the blocks marked `same` must be equal.

HTML comments before a block tell these tests how to run it, such as
`<!-- doctest: exit 1 -->` for a command that must fail, or `<!-- doctest: skip -->` for one
that calls AWS. The comment on `docBlock` in
[internal/cli/docs_test.go](internal/cli/docs_test.go) lists them. A block that would reach
AWS must be marked `skip`, and the unit lane fails on a `sam` or `aws` command that isn't.

When a change alters what a command prints, rewrite the outputs in the docs from the
actual output, then read the diff:

```sh
go test ./internal/cli -run TestDocs -update
```

For the outputs that only the integration lane runs, start DynamoDB Local first:

```sh
make emulators-up
DYNAFLUX_DYNAMODB_ENDPOINT=http://127.0.0.1:8000 go test -tags integration ./internal/cli -run TestGettingStarted -update
```

## Changelog

A change that users notice adds a line under `## [Unreleased]` in
[CHANGELOG.md](CHANGELOG.md). Say what users must do about it, such as run
`dynaflux generate` or backfill a view.

## License

Dynaflux is under the [Apache License 2.0](LICENSE). New files start with the license
header; copy it from a file of the same kind.

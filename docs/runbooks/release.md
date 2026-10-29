<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Releases

A release is a tag on `main`. The Go module proxy serves it to `go get -tool` and
`go install`, the main ways to get Dynaflux. The tag also starts a workflow that builds the
CLI's archives and leaves them in a draft GitHub release. Only the maintainer tags, pushes,
publishes a release and runs the AWS gates. AI assistants never do, and CI never publishes.

| Path | What it is |
|------|------------|
| `CHANGELOG.md` | What each release changes, and what users must do about it. A tag's section is its release notes. |
| `scripts/release-notes.sh` | Prints the section of `CHANGELOG.md` for a tag. Fails when the section is missing or empty. |
| `.goreleaser.yaml` | The archives: the CLI for `darwin` and `linux` on `amd64` and `arm64`, with `LICENSE`, `README.md` and `THIRD_PARTY_NOTICES`. Also an SPDX SBOM per archive, and `checksums.txt`. |
| `.github/workflows/release.yml` | Runs on a pushed version tag, such as `v1.0.0-rc.1`. Checks that the tag is on `main` and that its release isn't published, and runs govulncheck under `go.mod`'s toolchain. Then runs GoReleaser, checks the version that the CLI reports, attests where the archives were built, and leaves a draft release. |
| `THIRD_PARTY_NOTICES` | The licenses and NOTICE files of Go and of the modules built into the CLI. `make notices` writes it, and `make verify` runs `notices`. |
| The `release-config` job of `ci.yml` | Runs GoReleaser on a commit that changes one of the files above, without publishing ([section 7](#7-check-the-release-config)). |

## 1. Versions

- The first tag is `v1.0.0-rc.1`, after the [dry run](#dry-run). Each fix that an
  acceptance run needs makes a new release candidate, `v1.0.0-rc.2` and up. `v1.0.0`
  follows the accepted one.
- A release candidate's GitHub release is marked as a prerelease. `@latest` resolves to the
  newest release, or to the newest prerelease while there is no release, so `v1.0.0-rc.1`
  is `@latest` until `v1.0.0`.
- **Never move or delete a pushed tag.** The proxy and the checksum database keep it for
  good. A bad release gets a patch release, whose `go.mod` retracts it:
  `retract vX.Y.Z // the reason`.

## 2. The release-prep commit

Every gate runs on the exact commit that gets tagged, so this commit comes first.

1. Run `make bench` and `make bench-compare`, then record a new baseline
   ([benchmarks.md](benchmarks.md#record-the-baseline)).
2. In `CHANGELOG.md`, `## [Unreleased]` becomes `## [X.Y.Z] - YYYY-MM-DD`, or
   `## [X.Y.Z-rc.N] - YYYY-MM-DD` for a release candidate. Say what users must do: run
   `dynaflux generate`, redeploy the templates in order, and backfill which views. Print the
   notes that the workflow will publish:

   ```sh
   scripts/release-notes.sh vX.Y.Z
   ```

   It prints the section from below its heading to the next release's heading, or to the
   link definitions at the end of the file. It fails when the section is missing or holds
   only blank lines.
3. In `README.md`, update the Install commands and the Status section: what this release
   is, and what has run on AWS.
4. `THIRD_PARTY_NOTICES` is current. `make verify` rewrites it, so CI's `check-clean` fails
   when the committed file differs.
5. Raise `go.mod`'s `toolchain` line to the newest patch release of its `go` line, listed in
   the [Go release history](https://go.dev/doc/devel/release), such as
   `go mod edit -toolchain=go1.26.10`. `release.yml` builds the archives with that
   toolchain, and its govulncheck step fails when the toolchain's standard library has a
   known vulnerability. `make check-go-bound` allows any patch release of the `go` line.

## 3. Gates

Each gate runs on the release-prep commit's SHA.

6. `main` is green: `verify` (with `bench-smoke`), `integration` (with `TestGettingStarted`
   and `bench-emulator`), `infra-lint` (with `make shellcheck`), and `release-config`.
   `release-config` runs because `CHANGELOG.md` changed, and checks the notes of its
   newest release.
7. `make sim` and `make sim-controls` pass ([simulation.md](simulation.md)).
8. `make e2e-aws` passes, logged in [e2e.md](e2e.md#10-results).
9. For `v1.0.0`, and for any release that changes the projector or `explain`:
   `make bench-aws` ([benchmarks.md](benchmarks.md)). Run it on a different UTC day from
   `e2e-aws`: Cost Explorer reports the account by UTC day.
10. The version guard, from the second tag on. Compare with the previous tag:

   ```sh
   previous=v1.0.0
   git diff "$previous" -- internal/catalog/json.go fluxgen/ internal/codegen/templates/
   git diff --exit-code "$previous" -- testdata/compat/
   ```

   If a catalog format or a `fluxgen` signature changed, a new `GeneratedCodeVersionN`
   exists and the example's generated code names it
   ([ADR 0019](../decisions/0019-v1-compatibility-promise.md#rules)). The compatibility
   fixtures in `testdata/compat/` don't change. From `v1.0.1` on, gorelease reports no
   incompatible change, with no exception: its report has no `## incompatible changes`
   section.

   ```sh
   go run golang.org/x/exp/cmd/gorelease@latest -base="$previous"
   ```

11. `make check-go-bound` passes. List the dependencies at pre-release versions in the
    results:

    ```sh
    go list -m -f '{{if not .Main}}{{.Path}} {{.Version}}{{end}}' all | grep -E ' v[0-9]+\.[0-9]+\.[0-9]+-'
    ```

    Today it prints `go.yaml.in/yaml/v4 v4.0.0-rc.6`.

A result carries over to a later commit only when `git diff --name-only <gated SHA>..HEAD`
lists Markdown files alone. For the final release, the commit on top of the accepted
release candidate may change only `CHANGELOG.md` and `README.md`. The acceptance run read
the guides, so a change to a guide needs a new release candidate and a new acceptance run.

## 4. Tag

12. On the gated commit of `main`, with the [GitHub settings](#8-github-settings) in place:

    ```sh
    git tag -s vX.Y.Z -m "Dynaflux vX.Y.Z"
    git push origin vX.Y.Z
    ```

13. The push starts `release.yml`. It stops before the build when the tag isn't on `main`,
    its release is published, govulncheck finds a vulnerability, or `CHANGELOG.md` has no
    notes for it. After a failure, find the run with `gh run list --workflow release.yml`,
    and rerun its failed jobs:

    ```sh
    gh run rerun <run ID> --failed
    ```

    The rerun takes every file from the tag, `release.yml` included, so a fix to any file
    needs a new tag. GitHub reruns a run for 30 days after it started. Rerun only a tag
    whose release is still a draft: the rerun replaces the draft's files. The workflow
    refuses a tag whose release is published. Nothing else starts `release.yml`, so every
    attestation it signs names a release tag, which the check in step 16 pins.

    Once a tag is pushed, the provenance of its archives is public and permanent: the
    workflow records it in GitHub's attestation store and in the public Sigstore log
    before you review the draft. Deleting the draft doesn't remove it.

## 5. After the tag

14. Fetch the version into the proxy:

    ```sh
    GOPROXY=https://proxy.golang.org go list -m github.com/lucindo/dynaflux@vX.Y.Z
    ```

    Then check that `https://pkg.go.dev/github.com/lucindo/dynaflux@vX.Y.Z` shows it.
15. The smoke test, from a new empty directory outside any Go module:

    ```sh
    go mod init example.com/smoke
    go get -tool github.com/lucindo/dynaflux/cmd/dynaflux@vX.Y.Z
    go tool dynaflux version
    go tool dynaflux init -store smoke -region us-east-1 -region us-west-2
    go tool dynaflux generate
    go mod tidy
    go build ./...
    go test ./...
    go install github.com/lucindo/dynaflux/cmd/dynaflux@vX.Y.Z
    dynaflux version
    ```

    Both `version` commands print `cli vX.Y.Z` and `library vX.Y.Z`.
16. Check the draft release on GitHub:
    - four archives, `dynaflux_X.Y.Z_<os>_<arch>.tar.gz`, each with its `.sbom.json`, and
      `checksums.txt`
    - the notes: the `CHANGELOG.md` section, then the Install footer
    - a release candidate is marked as a prerelease

    Download one archive for your machine and `checksums.txt`, then:

    ```sh
    sha256sum --ignore-missing -c checksums.txt
    gh attestation verify dynaflux_X.Y.Z_darwin_arm64.tar.gz --repo lucindo/dynaflux \
      --cert-identity https://github.com/lucindo/dynaflux/.github/workflows/release.yml@refs/tags/vX.Y.Z
    tar -xzf dynaflux_X.Y.Z_darwin_arm64.tar.gz dynaflux
    ./dynaflux version
    ```

    `version` prints `cli vX.Y.Z`. Then publish the draft:

    ```sh
    gh release edit vX.Y.Z --draft=false
    ```

17. Open a new `## [Unreleased]` section at the top of `CHANGELOG.md`.

## 6. The acceptance run

### Cold read

Before the first tag, a reader new to Dynaflux gets only the README and `docs/guide/`, plus
Go, Docker and DynamoDB Local. They follow
[getting started](../guide/getting-started.md) steps 3 to 10 word for word, with no AWS,
and log every point where they needed something the guide didn't say. Each entry is fixed
or answered in the change that adds the log, and the log counts toward the acceptance.

### Dry run

Before `v1.0.0-rc.1`, once `e2e-aws` is green, the maintainer follows the guide on AWS with
the CLI of a pushed commit, a pseudo-version:
`go get -tool github.com/lucindo/dynaflux/cmd/dynaflux@<commit>`. Steps 11 to 13 of the
guide have never run, and each deviation found after a tag would spend a release
candidate. Fix what it finds, then tag `v1.0.0-rc.1`.

The repository is public by then: `go get` through the proxy, pkg.go.dev and
`gh attestation verify` need it.

### On each release candidate

- Use a new, empty `GOMODCACHE`, a sandbox account, and a UTC day with no other run. Open
  only the README, `docs/guide/` and the pages they link to.
- Follow getting-started.md word for word, from `go mod init` to the teardown, with its
  `@latest` as written. `go tool dynaflux version` must print the release candidate under
  test.
- If possible, a second person runs it. If not, record "not a fresh reader" in the results,
  and the cold-read log stands for the fresh reader.
- Record in the results:
  - each step's time
  - every deviation: a missing step, a wrong output, an unclear sentence
  - the trial's cost, the next day
  - what the teardown left behind, by the checks of the guide's step 13
- Fix a deviation in the docs, or in the CLI with a doc test, then tag a new release
  candidate. When the fix changes Markdown files alone, the last `e2e-aws` result carries
  over ([section 3](#3-gates)).
- `v1.0.0` follows a release candidate whose run has no deviations, with `e2e-aws` green on
  its commit, or on one that differs from it in Markdown files alone.

## 7. Check the release config

Anyone can check the config without publishing. [GoReleaser](https://goreleaser.com)
v2.18.3 is the version that `release.yml` runs, and
[syft](https://github.com/anchore/syft) on `PATH` writes the SBOMs.

```sh
goreleaser check
```

To build the archives, work in a copy with no git remote: one push of a scratch tag to the
real repository would spend that version for good.

```sh
copy="$(mktemp -d)"
git archive HEAD | tar -x -C "$copy"
cd "$copy"
git init --quiet --initial-branch=main
git add --all
git commit --quiet --message=scratch
git tag v0.0.0-scratch.1
git config branch.main.remote .
git config branch.main.merge refs/heads/main
goreleaser release --snapshot --clean
```

- The tag is never a real version, and is below v2: Go ignores a tag of v2 or later on a
  module path without a `/v2` suffix, and would stamp a pseudo-version.
- GoReleaser reads the tag and the commit time from git, and takes a branch that tracks
  itself in place of a remote. Without one, it names the snapshot `0.0.0` and its commit
  `none`.
- `dist/` then holds the four archives, `dynaflux_0.0.0-scratch.1-SNAPSHOT-<commit>_<os>_<arch>.tar.gz`,
  their SBOMs and `checksums.txt`. The `dynaflux` in an archive prints
  `cli v0.0.0-scratch.1`.

Two builds of one commit give the same archives, on macOS and on Linux. The SBOMs differ,
since each holds its creation time and a random ID. To rebuild a release's archives, run
`goreleaser release --snapshot --clean` in a clone at its tag, not in a copy: the archives
take their times from the tagged commit. Set `GOTOOLCHAIN` to `go.mod`'s toolchain line, as
`release.yml` does. A snapshot names the archives
`dynaflux_X.Y.Z-SNAPSHOT-<commit>_<os>_<arch>.tar.gz`, so compare their hashes, not their
names, with the release's `checksums.txt`, downloaded next to `dist/`:

```sh
shasum -a 256 dist/*.tar.gz | cut -d ' ' -f 1 | sort >rebuilt.txt
grep '\.tar\.gz$' checksums.txt | cut -d ' ' -f 1 | sort | diff - rebuilt.txt
```

No output means that the archives match. The SBOM lines never match.

The `release-config` job of `ci.yml` runs when a push or pull request changes
`.goreleaser.yaml`, `release.yml`, `ci.yml`, `CHANGELOG.md` or `scripts/release-notes.sh`:

- `goreleaser check`
- the notes of the newest release in `CHANGELOG.md`, through `scripts/release-notes.sh`
- the copy above, but a run without `--snapshot`, as `release.yml` runs it, with a config
  that disables the release, so it publishes nothing. The log must show that GoReleaser
  loaded the notes (`loading file`).

## 8. GitHub settings

Set both before the first tag:

- A tag ruleset on `v*`: only the maintainer creates these tags, and nobody updates or
  deletes them. The provenance check trusts `release.yml` at a `v*` tag, so this ruleset
  is what limits it to the maintainer's releases.
- Immutable releases: once published, a release's tag and files can't change. A draft
  stays editable, so a rerun of `release.yml` can replace it. `release.yml` also refuses a
  published release, but only this setting protects one from every other change.

## 9. Results

One block per release candidate:

```markdown
### vX.Y.Z-rc.N

- Date and commit:
- What ran, with links to the logs:
- Dependencies at pre-release versions:
- Acceptance run: reader, each step's time, deviations:
- Cost, the next day:
- Left after the teardown:
```

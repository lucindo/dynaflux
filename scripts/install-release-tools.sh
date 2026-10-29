#!/usr/bin/env bash
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Installs GoReleaser and syft for the release workflow and CI's release-config job: their
# Linux x86-64 release archives, checked against the SHA-256 pinned here, so a replaced
# release asset fails the job instead of running in it. Each hash is the one in the
# release's checksums file, so a version bump updates its hash too
# (docs/runbooks/release.md). The tools go in $RUNNER_TEMP/bin, which joins $GITHUB_PATH.
set -euo pipefail

goreleaser=v2.18.3
goreleaser_sha256=b8dee4728bca1aba0d71a832a17cd0a3799f75372878e7ffa36aef2343bce0bc
syft=v1.54.0
syft_sha256=54a87372498168b2d033e876fd41fa4e8035b872699e525a57046e1f2f09c860

bin="$RUNNER_TEMP/bin"
mkdir -p "$bin"
curl -fsSL -o "$RUNNER_TEMP/goreleaser.tar.gz" \
	"https://github.com/goreleaser/goreleaser/releases/download/$goreleaser/goreleaser_Linux_x86_64.tar.gz"
curl -fsSL -o "$RUNNER_TEMP/syft.tar.gz" \
	"https://github.com/anchore/syft/releases/download/$syft/syft_${syft#v}_linux_amd64.tar.gz"
printf '%s  %s\n' "$goreleaser_sha256" "$RUNNER_TEMP/goreleaser.tar.gz" "$syft_sha256" "$RUNNER_TEMP/syft.tar.gz" |
	sha256sum -c -
tar -xzf "$RUNNER_TEMP/goreleaser.tar.gz" -C "$bin" goreleaser
tar -xzf "$RUNNER_TEMP/syft.tar.gz" -C "$bin" syft
echo "$bin" >>"$GITHUB_PATH"

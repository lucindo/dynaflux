#!/usr/bin/env bash
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Prints the section of ./CHANGELOG.md for a tag: v1.0.0-rc.1 prints the section headed
# "## [1.0.0-rc.1]". The release workflow gives it to GoReleaser as the release notes
# (docs/runbooks/release.md). Fails when the section is missing or empty.
set -euo pipefail

if [[ $# -ne 1 || ! "$1" =~ ^v[0-9] ]]; then
	echo "usage: $0 <tag>, such as $0 v1.0.0" >&2
	exit 2
fi
version="${1#v}"

# The section ends at the next release's heading or at the link definitions that close the
# file. Leading blank lines are skipped, and $(...) drops the trailing ones.
notes="$(awk -v head="## [$version]" '
	index($0, head) == 1 { on = 1; next }
	on && (/^## / || /^\[[^]]+\]: /) { exit }
	on && (NF || started) { started = 1; print }
' CHANGELOG.md)"
if [[ -z "$notes" ]]; then
	echo "CHANGELOG.md has no notes under a heading \"## [$version]\"." >&2
	exit 1
fi
printf '%s\n' "$notes"

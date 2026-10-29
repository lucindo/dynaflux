#!/usr/bin/env bash
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Writes THIRD_PARTY_NOTICES: Go's license, and the license and NOTICE file of each module
# built into the release binaries of cmd/dynaflux, which the release archives must carry.
# make verify runs it, so CI fails when the committed file is stale. Not go-licenses
# (v2.0.1): it fails on a toolchain that go.mod's toolchain line downloaded.
set -euo pipefail

# The same order on every machine.
export LC_ALL=C

cd "$(dirname "$0")/.."

# Prints the path of the first of the named files that the directory holds.
first_file() {
	local dir="$1" name
	shift
	for name in "$@"; do
		if [[ -f "$dir/$name" ]]; then
			echo "$dir/$name"
			return
		fi
	done
}

goroot="$(go env GOROOT)"

# Every release target, since a dependency may build for some of them only. Packages of the
# standard library print an empty line.
deps="$(for target in darwin/amd64 darwin/arm64 linux/amd64 linux/arm64; do
	GOOS="${target%/*}" GOARCH="${target#*/}" go list -deps \
		-f '{{with .Module}}{{if not .Main}}{{.Path}}{{"\t"}}{{.Dir}}{{end}}{{end}}' ./cmd/dynaflux || exit 1
done | sort -u | sed '/^$/d')"

# One line for Go, then one per module: the name, the license file and the NOTICE file, if any.
files="$(while IFS=$'\t' read -r name dir; do
	license="$(first_file "$dir" LICENSE LICENSE.txt LICENSE.md COPYING)"
	if [[ -z "$license" ]]; then
		echo "$name has no license file in $dir." >&2
		exit 1
	fi
	printf '%s\t%s\t%s\n' "$name" "$license" "$(first_file "$dir" NOTICE NOTICE.txt NOTICE.md)"
done <<<"Go"$'\t'"$goroot"$'\n'"$deps")"

# Modules with the same license text share one copy of it.
notices="$(awk -F '\t' '
	function slurp(file,  line, text) {
		while ((getline line <file) > 0) text = text line "\n"
		close(file)
		return text
	}
	{
		text = slurp($2)
		if (!(text in group)) { group[text] = ++groups; license[groups] = text }
		users[group[text]] = users[group[text]] "- " $1 "\n"
		if ($3 != "") notice[++notices] = "The NOTICE file of " $1 ":\n\n" slurp($3)
	}
	END {
		rule = "================================================================================"
		print "The dynaflux binary holds code from Go and from the modules below. Their licenses"
		print "and NOTICE files follow. scripts/third-party-notices.sh writes this file."
		for (i = 1; i <= groups; i++) printf "\n%s\nThe license of:\n%s\n%s", rule, users[i], license[i]
		for (i = 1; i <= notices; i++) printf "\n%s\n%s", rule, notice[i]
	}
' <<<"$files")"
printf '%s\n' "$notices" >THIRD_PARTY_NOTICES

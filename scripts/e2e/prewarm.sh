#!/usr/bin/env bash
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Raises the warm throughput of tables of the e2e store of run E2E_RUN before a throughput run
# (docs/runbooks/benchmarks.md), then waits until every region reports it. Each argument is a
# table, named as after the store's name, and the write units per second it must take at once:
#
#   scripts/e2e/prewarm.sh stream-page_views=20000 view-tag_hourly-v1=8250
#
# A stream table is a global table, so it is raised in the home region and every replica
# follows; a view table is raised in each region. Warm throughput can't be lowered, and each
# unit raised is billed once. Maintainer only.
set -euo pipefail
# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "$0")/common.sh"

if (($# == 0)); then
	echo "Usage: scripts/e2e/prewarm.sh <table>=<write units per second>..., such as stream-page_views=20000" >&2
	exit 1
fi
pattern='^((stream|view)-[a-z0-9_-]+)=([1-9][0-9]*)$'
for arg in "$@"; do
	if [[ ! "$arg" =~ $pattern ]]; then
		echo "$arg: want <table>=<units>, the table named as after $base-, such as view-tag_hourly-v1=8250." >&2
		exit 1
	fi
done

# warm prints a table's warm write units per second and their status in a region. It fails
# when describe-table does, or shows no warm throughput: the CLI prints None for a missing one.
# Call it in an assignment, so that set -e stops the script then.
warm() { # table region
	local out
	out="$(aws dynamodb describe-table --table-name "$1" --region "$2" \
		--query '[Table.WarmThroughput.WriteUnitsPerSecond, Table.WarmThroughput.Status]' --output text)"
	if [[ ! "$out" =~ ^[0-9]+[[:space:]]+[A-Z_]+$ ]]; then
		echo "$1 in $2: unexpected warm throughput: $out" >&2
		return 1
	fi
	echo "$out"
}

for arg in "$@"; do
	[[ "$arg" =~ $pattern ]]
	table="$base-${BASH_REMATCH[1]}"
	units="${BASH_REMATCH[3]}"
	where=("${regions[@]}")
	if [[ "${BASH_REMATCH[2]}" == stream ]]; then
		where=("$home")
	fi
	for region in "${where[@]}"; do
		out="$(warm "$table" "$region")"
		read -r current _ <<<"$out"
		if ((current >= units)); then
			echo "$table in $region already takes $current write units per second."
			continue
		fi
		echo "Raising $table in $region from $current to $units write units per second."
		aws dynamodb update-table --table-name "$table" --region "$region" \
			--warm-throughput "WriteUnitsPerSecond=$units" >/dev/null
	done
done

# Pre-warming takes minutes, longer for a larger table.
deadline=$((SECONDS + 3600))
for arg in "$@"; do
	[[ "$arg" =~ $pattern ]]
	table="$base-${BASH_REMATCH[1]}"
	units="${BASH_REMATCH[3]}"
	for region in "${regions[@]}"; do
		while true; do
			out="$(warm "$table" "$region")"
			read -r current status <<<"$out"
			if [[ "$status" == ACTIVE ]] && ((current >= units)); then
				break
			fi
			if ((SECONDS > deadline)); then
				echo "$table in $region is at $current write units per second ($status) after an hour; check it in the console." >&2
				exit 1
			fi
			sleep 15
		done
		echo "$table in $region takes $current write units per second."
	done
done

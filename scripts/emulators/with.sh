#!/usr/bin/env bash
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Runs a command against DynamoDB Local: starts it as up.sh does, runs the command, then
# stops the container if this run started it, even when the command fails. A container that
# was already running, such as one started for `dynaflux dev`, is left alone unless --fresh
# recreated it. KEEP_EMULATOR=1 leaves the container running in every case.
#   with.sh [--fresh] [--] command [args...]
set -euo pipefail

here="$(dirname "$0")"
compose=(docker compose --file "$here/compose.yaml")

fresh=false
if [[ "${1:-}" == --fresh ]]; then
	fresh=true
	shift
fi
if [[ "${1:-}" == -- ]]; then
	shift
fi
if [[ $# -eq 0 ]]; then
	echo "usage: $0 [--fresh] [--] command [args...]" >&2
	exit 2
fi

stop() {
	"${compose[@]}" down >/dev/null 2>&1 ||
		echo "Couldn't stop DynamoDB Local: run make emulators-down." >&2
}

if [[ "${KEEP_EMULATOR:-}" != 1 ]] &&
	{ $fresh || [[ -z "$("${compose[@]}" ps --status running --quiet dynamodb)" ]]; }; then
	trap stop EXIT
fi

if $fresh; then
	"$here/up.sh" --fresh
else
	"$here/up.sh"
fi
"$@"

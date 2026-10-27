#!/usr/bin/env bash
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Starts DynamoDB Local unless it is running, then waits until it answers. A running
# container is never restarted, because test runs in progress may share it. With --fresh,
# the container is recreated, empty, whether it runs or not: DynamoDB Local keeps memory from
# every run, even after its tables are deleted.
set -euo pipefail

compose=(docker compose --file "$(dirname "$0")/compose.yaml")
endpoint=http://127.0.0.1:8000

case "${1:-}" in
--fresh)
	"${compose[@]}" up --detach --force-recreate dynamodb
	;;
"")
	if [[ -z "$("${compose[@]}" ps --status running --quiet dynamodb)" ]]; then
		"${compose[@]}" up --detach dynamodb
	fi
	;;
*)
	echo "usage: $0 [--fresh]" >&2
	exit 2
	;;
esac

# Any HTTP response means the server is up; curl fails only without one.
for _ in $(seq 120); do
	if curl --silent --max-time 2 --output /dev/null "$endpoint"; then
		exit 0
	fi
	sleep 0.25
done

echo "DynamoDB Local did not answer at $endpoint within 30 s. Its logs:" >&2
"${compose[@]}" logs dynamodb >&2
exit 1

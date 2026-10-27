#!/usr/bin/env bash
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Starts DynamoDB Local unless it is running, then waits until it answers. A running
# container is never restarted, because test runs in progress may share it.
set -euo pipefail

compose=(docker compose --file "$(dirname "$0")/compose.yaml")
endpoint=http://127.0.0.1:8000

if [[ -z "$("${compose[@]}" ps --status running --quiet dynamodb)" ]]; then
	"${compose[@]}" up --detach dynamodb
fi

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

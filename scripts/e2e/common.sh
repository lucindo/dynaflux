# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Names that deploy.sh and teardown.sh share (docs/runbooks/e2e.md), and the check that the
# credentials are for the sandbox account. Sourced, not run.
# shellcheck shell=bash disable=SC2034 # the scripts that source this file use the names

run="${E2E_RUN:-}"
if [[ ! "$run" =~ ^[a-z0-9]{1,8}$ ]]; then
	echo "Set E2E_RUN to 1 to 8 lowercase letters or digits, new for each run, such as E2E_RUN=a1." >&2
	exit 1
fi
if [[ ! "${E2E_ACCOUNT:-}" =~ ^[0-9]{12}$ ]]; then
	echo "Set E2E_ACCOUNT to the ID of the sandbox account (docs/runbooks/e2e.md, Prerequisites)." >&2
	exit 1
fi
# The regions of examples/analytics/dynaflux.yaml, which deploy.sh checks, or those of
# E2E_REGIONS, a list separated by spaces, which deploy.sh writes into the store it deploys.
# global.yaml goes in the first, the home region, and fis.yaml in the second. A bad name would
# otherwise fail the deploy after global.yaml is up, and sed in deploy.sh can't take / or &.
read -ra regions <<<"${E2E_REGIONS:-us-east-1 us-west-2}"
if ((${#regions[@]} < 2)); then
	echo "Set E2E_REGIONS to two regions or more, separated by spaces, such as E2E_REGIONS=\"us-east-1 us-west-2 eu-west-1\"." >&2
	exit 1
fi
for r in "${regions[@]}"; do
	if [[ ! "$r" =~ ^[a-z]{2}(-[a-z]+)+-[0-9]+$ ]]; then
		echo "E2E_REGIONS: $r isn't a region name, such as us-east-1." >&2
		exit 1
	fi
done
duplicates="$(printf '%s\n' "${regions[@]}" | sort | uniq -d)"
if [[ -n "$duplicates" ]]; then
	echo "E2E_REGIONS names $duplicates more than once." >&2
	exit 1
fi
# The default credential chain can point at any account, and teardown deletes by name.
account="$(aws sts get-caller-identity --query Account --output text)"
if [[ "$account" != "$E2E_ACCOUNT" ]]; then
	echo "The AWS credentials are for account $account, not E2E_ACCOUNT=$E2E_ACCOUNT." >&2
	exit 1
fi

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
prefix="e2e-${run}-"
base="${prefix}analytics"
home="${regions[0]}"
fis_region="${regions[1]}"
global_stack="${base}-global"
regional_stack="${base}-regional"
fis_stack="${base}-fis"

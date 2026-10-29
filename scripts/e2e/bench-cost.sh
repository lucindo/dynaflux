#!/usr/bin/env bash
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Prints what the sandbox account E2E_ACCOUNT cost from one UTC day up to another, by usage
# type, from Cost Explorer (docs/runbooks/benchmarks.md, Cost). The end day is left out, as
# Cost Explorer does:
#
#   scripts/e2e/bench-cost.sh 2026-11-02 2026-11-03
#
# Cost Explorer counts the whole account by UTC day and lags by up to a day, so run it the day
# after a run, and keep other runs on other days. Maintainer only.
set -euo pipefail
# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "$0")/common.sh"

day='^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
if (($# != 2)) || [[ ! "$1" =~ $day || ! "$2" =~ $day || ! "$1" < "$2" ]]; then
	echo "Usage: scripts/e2e/bench-cost.sh <first day> <day after the last>, such as 2026-11-02 2026-11-03 (UTC)" >&2
	exit 1
fi

# Cost Explorer's endpoint is in us-east-1. A range that spans months comes back by month,
# so the first awk adds up each usage type's lines.
aws ce get-cost-and-usage --region us-east-1 \
	--time-period "Start=$1,End=$2" --granularity MONTHLY \
	--metrics UnblendedCost UsageQuantity --group-by Type=DIMENSION,Key=USAGE_TYPE \
	--query 'ResultsByTime[].Groups[].[Keys[0], Metrics.UsageQuantity.Amount, Metrics.UsageQuantity.Unit, Metrics.UnblendedCost.Amount]' \
	--output text |
	awk -F'\t' '{ quantity[$1] += $2; unit[$1] = $3; cost[$1] += $4 }
		END { for (k in cost) printf "%.2f\t%s\t%.2f\t%s\n", cost[k], k, quantity[k], unit[k] }' |
	sort -t$'\t' -k1,1gr |
	awk -F'\t' -v from="$1" -v to="$2" '
		BEGIN {
			printf "Account cost from %s up to %s (UTC), by usage type\n", from, to
			printf "%10s  %-48s %16s  %s\n", "USD", "usage type", "quantity", "unit"
		}
		{ printf "%10s  %-48s %16s  %s\n", $1, $2, $3, $4; total += $1 }
		END { printf "%10.2f  total\n", total }'

#!/usr/bin/env bash
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Deploys the end-to-end store of run E2E_RUN to account E2E_ACCOUNT with your AWS
# credentials (docs/runbooks/e2e.md): the example with examples/analytics/e2e/testdata/e2e.sql
# added, generated in a Go module of its own; global.yaml in the home region, regional.yaml in
# every region, a backfill of every view version, and the replication-pause experiment in the
# second region. With E2E_REGIONS set, the store is in those regions instead of the example's.
# With E2E_WORK set, the project stays in that directory. Maintainer only.
set -euo pipefail

for cmd in go sam aws; do
	if ! command -v "$cmd" >/dev/null; then
		echo "deploy.sh needs $cmd on PATH (docs/runbooks/e2e.md, Prerequisites)." >&2
		exit 1
	fi
done

# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "$0")/common.sh"

example="$repo/examples/analytics"
printf -v regions_line 'regions: [%s]' "$(printf '%s, ' "${regions[@]}" | sed 's/, $//')"
if [[ -z "${E2E_REGIONS:-}" ]] && ! grep -qxF "$regions_line" "$example/dynaflux.yaml"; then
	echo "The regions in common.sh differ from those of $example/dynaflux.yaml." >&2
	exit 1
fi

# E2E_WORK=<dir> keeps the project for the manual experiments. It must be new or empty, so
# that no file of an earlier project or experiment mixes into this one.
if [[ -n "${E2E_WORK:-}" ]]; then
	mkdir -p "$E2E_WORK"
	if [[ -n "$(ls -A "$E2E_WORK")" ]]; then
		echo "E2E_WORK=$E2E_WORK isn't empty: set it to a new or empty directory." >&2
		exit 1
	fi
	work="$(cd "$E2E_WORK" && pwd)"
else
	work="$(mktemp -d)"
	trap 'rm -rf "$work"' EXIT
fi

# The project builds against this checkout of Dynaflux, as the example does.
cp -R "$example/dynaflux.yaml" "$example/dynaflux.lock" "$example/dynaflux" "$work/"
cp "$example/e2e/testdata/e2e.sql" "$work/dynaflux/"
# The suite compiles the store with the same line when E2E_REGIONS is set.
if [[ -n "${E2E_REGIONS:-}" ]]; then
	sed "s/^regions: .*/$regions_line/" "$example/dynaflux.yaml" >"$work/dynaflux.yaml"
fi
cat >"$work/go.mod" <<MOD
module e2e.dynaflux.test/analytics

go $(awk '$1 == "go" { print $2; exit }' "$repo/go.mod")

require github.com/lucindo/dynaflux v0.0.0

replace github.com/lucindo/dynaflux => $repo
MOD
cp "$repo/go.sum" "$work/"
(cd "$repo" && go build -o "$work/bin/dynaflux" ./cmd/dynaflux)

cd "$work"
bin/dynaflux generate
go mod tidy
CGO_ENABLED=0 GOFLAGS=-tags=lambda.norpc sam build -t infra/dynaflux/regional.yaml

# No stack policy on the global stack, so that teardown.sh can delete it.
sam deploy -t infra/dynaflux/global.yaml --stack-name "$global_stack" --region "$home" \
	--resolve-s3 --parameter-overrides "Prefix=$prefix" --no-fail-on-empty-changeset
for region in "${regions[@]}"; do
	sam deploy --stack-name "$regional_stack" --region "$region" \
		--capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --resolve-s3 \
		--parameter-overrides "Prefix=$prefix" --no-fail-on-empty-changeset
done
# Before the first event, as on a user's first deploy: each version goes live without a read,
# and each region records where its archive of each stream starts (docs/spec/lifecycle.md §4.3,
# §5.5). -watch fails the deploy unless every version goes live.
bin/dynaflux backfill -all -yes -watch -prefix "$prefix"
aws cloudformation deploy --template-file "$example/e2e/fis.yaml" --stack-name "$fis_stack" \
	--region "$fis_region" --capabilities CAPABILITY_IAM \
	--parameter-overrides "Prefix=$prefix" "Run=$run" --no-fail-on-empty-changeset

echo "Deployed run $run. Test it with: E2E_RUN=$run E2E_ACCOUNT=$E2E_ACCOUNT ${E2E_REGIONS:+E2E_REGIONS=\"$E2E_REGIONS\" }go test -tags e2e -timeout 120m -v ./examples/analytics/e2e -target=aws"
if [[ -n "${E2E_WORK:-}" ]]; then
	echo "The generated project is in $work."
fi

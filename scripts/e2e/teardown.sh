#!/usr/bin/env bash
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Deletes what deploy.sh made for run E2E_RUN in account E2E_ACCOUNT, the resources that the
# stacks retain included (docs/runbooks/e2e.md, Teardown). make e2e-aws runs it after every
# run; run it by hand after an interrupted one. What is already gone is skipped, so it can run
# again after a failure.
set -euo pipefail
# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "$0")/common.sh"

# exists runs a read-only AWS command and reports whether what it reads exists. Any failure
# other than "not found" stops the script.
exists() {
	local err
	if err="$("$@" 2>&1 >/dev/null)"; then
		return 0
	fi
	case "$err" in
	*ResourceNotFoundException* | *EntityNotFoundException* | *NoSuchEntity* | *"does not exist"* | *"Not Found"*)
		return 1
		;;
	esac
	echo "$err" >&2
	exit 1
}

# words runs an AWS command with --output text and prints its words on one line: with
# --query, the AWS CLI prints a line per page (AWS CLI user guide, "Filtering output").
words() {
	local out
	out="$("$@")" || return
	echo "${out//[$'\t\n']/ }"
}

# describe prints a table's status and whether deletion protection is on, or GONE.
describe() { # table region
	if exists aws dynamodb describe-table --table-name "$1" --region "$2"; then
		aws dynamodb describe-table --table-name "$1" --region "$2" \
			--query '[Table.TableStatus, Table.DeletionProtectionEnabled]' --output text
	else
		echo GONE
	fi
}

# unprotect turns off deletion protection on a table in a region, unless the table is gone
# or going.
unprotect() { # table region
	local state
	state="$(describe "$1" "$2")"
	case "$state" in
	GONE) ;;
	DELETING*) aws dynamodb wait table-not-exists --table-name "$1" --region "$2" ;;
	*True)
		aws dynamodb wait table-exists --table-name "$1" --region "$2"
		aws dynamodb update-table --table-name "$1" --region "$2" --no-deletion-protection-enabled >/dev/null
		;;
	esac
}

# replicas prints the regions that hold a replica of a stream table. Without one, the text
# output of null would be None.
replicas() { # table
	words aws dynamodb describe-table --table-name "$1" --region "$home" \
		--query "Table.Replicas[].RegionName || \`[]\`" --output text
}

# delete_table turns off deletion protection on every replica of a stream table, removes the
# replicas outside the home region, then deletes the table.
delete_table() { # table
	local list region state others
	echo "Deleting table $1"
	list="$(replicas "$1")"
	read -ra others <<<"$list"
	for region in "$home" ${others[@]+"${others[@]}"}; do
		unprotect "$1" "$region"
	done
	for region in ${others[@]+"${others[@]}"}; do
		state="$(describe "$1" "$region")"
		if [[ "$region" != "$home" && "$state" != GONE ]]; then
			aws dynamodb wait table-exists --table-name "$1" --region "$home"
			aws dynamodb update-table --table-name "$1" --region "$home" \
				--replica-updates "Delete={RegionName=$region}" >/dev/null
			aws dynamodb wait table-not-exists --table-name "$1" --region "$region"
		fi
	done
	state="$(describe "$1" "$home")"
	if [[ "$state" != GONE ]]; then
		aws dynamodb wait table-exists --table-name "$1" --region "$home"
		aws dynamodb delete-table --table-name "$1" --region "$home" >/dev/null
		aws dynamodb wait table-not-exists --table-name "$1" --region "$home"
	fi
}

delete_stack() { # stack region
	if exists aws cloudformation describe-stacks --stack-name "$1" --region "$2"; then
		echo "Deleting stack $1 in $2"
		aws cloudformation delete-stack --stack-name "$1" --region "$2"
		aws cloudformation wait stack-delete-complete --stack-name "$1" --region "$2"
	fi
}

# delete_database deletes the table optimizers of a Glue database, then the database: the
# setup function creates the optimizers outside CloudFormation, and AWS doesn't document
# that deleting a table deletes them.
delete_database() { # database region
	local list table type tables optimizer
	echo "Deleting Glue database $1 in $2"
	list="$(words aws glue get-tables --database-name "$1" --region "$2" \
		--query "TableList[?TableType=='EXTERNAL_TABLE'].Name" --output text)"
	read -ra tables <<<"$list"
	for table in ${tables[@]+"${tables[@]}"}; do
		for type in compaction retention; do
			optimizer=(--catalog-id "$account" --database-name "$1" --table-name "$table" --type "$type" --region "$2")
			if exists aws glue get-table-optimizer "${optimizer[@]}"; then
				aws glue delete-table-optimizer "${optimizer[@]}"
			fi
		done
	done
	aws glue delete-database --name "$1" --region "$2"
}

# empty_bucket deletes every object version and delete marker of a versioned bucket, 1,000
# at a time.
empty_bucket() { # bucket region
	local versions=(aws s3api list-object-versions --bucket "$1" --region "$2" --no-paginate)
	local kind count batch objects errors batches=0
	for kind in Versions DeleteMarkers; do
		count="length(${kind} || \`[]\`)"
		batch="{Objects: ${kind}[].{Key: Key, VersionId: VersionId}, Quiet: \`true\`}"
		while [[ "$("${versions[@]}" --query "$count" --output text)" != 0 ]]; do
			if ((++batches > 1000)); then
				echo "Bucket $1 still has objects after 1,000 deletes of 1,000." >&2
				exit 1
			fi
			objects="$("${versions[@]}" --query "$batch" --output json)"
			# A key that fails to delete comes back in Errors, with exit status 0.
			errors="$(aws s3api delete-objects --bucket "$1" --region "$2" --delete "$objects" --query Errors --output json)"
			if [[ "$errors" != null && "$errors" != "[]" ]]; then
				echo "Bucket $1: $errors" >&2
				exit 1
			fi
		done
	done
}

list="$(words aws dynamodb list-tables --region "$home" \
	--query "TableNames[?starts_with(@, '${base}-stream-')]" --output text)"
read -ra tables <<<"$list"
# Experiment 17 adds a region: take in every region that holds a replica.
for table in ${tables[@]+"${tables[@]}"}; do
	list="$(replicas "$table")"
	read -ra others <<<"$list"
	for region in ${others[@]+"${others[@]}"}; do
		if [[ " ${regions[*]} " != *" $region "* ]]; then
			regions+=("$region")
		fi
	done
done

# Scenario 23 adds this policy to the projector role and removes it; an interrupted run may
# leave it, and CloudFormation deletes only the policies it made.
projector="${base}-projector"
if exists aws lambda get-function --function-name "$projector" --region "$home"; then
	role="$(aws lambda get-function-configuration --function-name "$projector" --region "$home" --query Role --output text)"
	role="${role##*/}"
	if exists aws iam get-role-policy --role-name "$role" --policy-name dynaflux-e2e-deny; then
		aws iam delete-role-policy --role-name "$role" --policy-name dynaflux-e2e-deny
	fi
fi

# An interrupted scenario 3 may leave its replication pause on. While it lasts, FIS denies
# replication on the tables, so it must end before a stack or a replica is deleted.
if exists aws cloudformation describe-stacks --stack-name "$fis_stack" --region "$fis_region"; then
	template="$(aws cloudformation describe-stacks --stack-name "$fis_stack" --region "$fis_region" \
		--query "Stacks[0].Outputs[?OutputKey=='ExperimentTemplateId'].OutputValue" --output text)"
	unfinished="state.status!='completed' && state.status!='stopped' && state.status!='failed' && state.status!='cancelled'"
	list="$(words aws fis list-experiments --region "$fis_region" \
		--query "experiments[?experimentTemplateId=='$template' && $unfinished].id" --output text)"
	read -ra active <<<"$list"
	for id in ${active[@]+"${active[@]}"}; do
		status="$(aws fis get-experiment --id "$id" --region "$fis_region" --query experiment.state.status --output text)"
		if [[ "$status" =~ ^(pending|initiating|running)$ ]]; then
			echo "Stopping experiment $id"
			aws fis stop-experiment --id "$id" --region "$fis_region" >/dev/null
		fi
		tries=0
		until [[ "$status" =~ ^(completed|stopped|failed|cancelled)$ ]]; do
			if ((++tries > 40)); then
				echo "Experiment $id is still $status after 10 minutes: stop it in the AWS FIS console, then run teardown.sh again." >&2
				exit 1
			fi
			sleep 15
			status="$(aws fis get-experiment --id "$id" --region "$fis_region" --query experiment.state.status --output text)"
		done
	done
fi

for region in "${regions[@]}"; do
	delete_stack "$regional_stack" "$region"
done
delete_stack "$global_stack" "$home"
delete_stack "$fis_stack" "$fis_region"

for table in ${tables[@]+"${tables[@]}"}; do
	delete_table "$table"
done

# The database goes first, so that no optimizer writes to the bucket while it empties.
database="${base//-/_}"
for region in "${regions[@]}"; do
	if exists aws glue get-database --name "$database" --region "$region"; then
		delete_database "$database" "$region"
	fi
	bucket="${base}-${account}-${region}-an"
	if exists aws s3api head-bucket --bucket "$bucket" --region "$region"; then
		echo "Deleting bucket $bucket"
		empty_bucket "$bucket" "$region"
		aws s3api delete-bucket --bucket "$bucket" --region "$region"
	fi
done

# The scratch table of the one-shot checks (docs/runbooks/e2e.md §9).
scratch="${prefix}scratch"
state="$(describe "$scratch" "$home")"
if [[ "$state" != GONE ]]; then
	echo "Deleting table $scratch"
	aws dynamodb delete-table --table-name "$scratch" --region "$home" >/dev/null
	aws dynamodb wait table-not-exists --table-name "$scratch" --region "$home"
fi

# A table that the steps above missed, such as a replica left by a failed run, shows here.
for region in "${regions[@]}"; do
	list="$(words aws dynamodb list-tables --region "$region" \
		--query "TableNames[?starts_with(@, '$prefix')]" --output text)"
	read -ra left <<<"$list"
	if ((${#left[@]} > 0)); then
		echo "Tables of run $run are left in $region: ${left[*]}" >&2
		exit 1
	fi
done

echo "Run $run is torn down. Left alone: the Glue optimizer log groups and SAM's deployment bucket, which other stacks share."

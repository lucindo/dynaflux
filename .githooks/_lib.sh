# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

# Shared helpers sourced by individual git hooks. Do not execute directly.
# Usage from a hook:
#   #!/usr/bin/env bash
#   # shellcheck source=_lib.sh
#   source "$(dirname "$0")/_lib.sh"

set -Eeuo pipefail

# Better diagnostics on failure.
trap 'rc=$?; echo "ERROR: ${HOOK_NAME:-hook} failed at line $LINENO (exit $rc)" >&2' ERR

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

NULL_SHA="0000000000000000000000000000000000000000"

# Capture the current dirty-state (tracked-modified + untracked) of the
# working tree. Output goes to stdout; callers redirect to a temp file.
hook_snapshot() {
    {
        git diff --name-only
        git ls-files --others --exclude-standard
    } | sort -u
}

# Compare current dirty-state against a snapshot file written earlier.
# Fails (exit 1) only on files the hook ITSELF introduced; pre-existing
# unstaged noise is ignored.
#
# Args:
#   $1 - path to the snapshot file
#   $2 - command the user should run to bypass the hook (e.g. "git commit --no-verify")
hook_check_changes() {
    local before_file="$1"
    local bypass_hint="${2:-git <cmd> --no-verify}"
    local after new
    after="$(hook_snapshot)"
    new="$(comm -13 "$before_file" <(printf '%s\n' "$after"))"
    if [ -n "$new" ]; then
        echo
        echo "${HOOK_NAME:-Hook} modified files. Review and re-stage them:"
        echo "$new" | sed 's/^/  /'
        echo
        echo "Then: git add <files> and re-run."
        echo "(To bypass once: $bypass_hint)"
        exit 1
    fi
}

# Run a command, prefixing with "==> <name>" and printing wall-clock time.
step() {
    local name="$1"; shift
    local start end
    start=$(date +%s)
    echo "==> $name"
    "$@"
    end=$(date +%s)
    echo "    ($name took $((end - start))s)"
}

# Sets GITLEAKS to the copy `make tools` installs, not whatever is first on PATH.
require_gitleaks() {
    GITLEAKS="$(go env GOPATH)/bin/gitleaks"
    if [[ ! -x "$GITLEAKS" ]]; then
        echo "${HOOK_NAME:-hook}: gitleaks is not installed, so secrets cannot be checked." >&2
        echo "Run: make tools" >&2
        exit 1
    fi
}

# Warn + `go mod download` IF go.mod or go.sum changed between two refs.
# Cheap no-op otherwise.
maybe_mod_download() {
    local base="$1" tip="$2"
    if git diff --name-only "$base" "$tip" -- go.mod go.sum 2>/dev/null | grep -q .; then
        echo
        echo "WARNING: go.mod / go.sum changed between $base..$tip"
        echo "         running 'go mod download'…"
        go mod download
    fi
}

# Create the snapshot tmpfile, register cleanup, export path as $SNAPSHOT.
hook_begin() {
    # Portable across GNU and BSD mktemp: pass a full template path.
    SNAPSHOT="$(mktemp "${TMPDIR:-/tmp}/dynaflux-hook.XXXXXX")"
    trap 'rm -f "$SNAPSHOT"' EXIT
    hook_snapshot > "$SNAPSHOT"
    echo "Running ${HOOK_NAME:-hook} checks (root $ROOT)"
}

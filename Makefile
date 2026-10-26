# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

# Flags for `go test`. -count=1 bypasses the test cache so verify results
# never depend on per-machine cache state. Set GO_TEST_FLAGS= to re-enable it.
GO_TEST_FLAGS ?= -count=1

# Where `go install` drops binaries. Used to invoke tools by absolute
# path so PATH ordering (brew, asdf, system) can't shadow what we just installed.
GOBIN_DIR := $(shell go env GOPATH)/bin

# Go tools are not pinned: `make tools` installs the latest release of any
# that is missing, and `make tools-update` upgrades them all.

# Absolute paths to the tool binaries installed into GOBIN_DIR.
GOLANGCI_LINT := $(GOBIN_DIR)/golangci-lint
GITLEAKS      := $(GOBIN_DIR)/gitleaks
GOVULNCHECK   := $(GOBIN_DIR)/govulncheck
OSV_SCANNER   := $(GOBIN_DIR)/osv-scanner

# Install $(2) into GOBIN_DIR if the binary at $(1) is missing. Checked by
# absolute path, not PATH, because callers run that exact binary.
define ensure_gobin_tool
	@[ -x "$(1)" ] || { \
		echo "Installing missing Go tool: $(notdir $(1)) ($(2))"; \
		GOBIN='$(GOBIN_DIR)' go install $(2); \
	}
endef

# Fail with an install hint when a non-Go tool is not on PATH.
define require_cmd
	@command -v $(1) >/dev/null 2>&1 || { \
		echo "This target needs $(1) on PATH. Install it with: $(2)"; \
		exit 1; \
	}
endef

# Dockerfiles anywhere in the repo, tracked or new, minus .gitignore'd ones.
DOCKERFILES = $(shell git ls-files --cached --others --exclude-standard -- '*Dockerfile*')

.PHONY: help check-git-hooks warn-git-hooks setup-git-hooks tools tools-update \
        test bench cross-arch fmt fix lint vuln osv gomod mod-download verify check-clean \
        sim sim-controls sim-seed hadolint checkov infra-lint trivy semgrep

.DEFAULT_GOAL := help

help: ## Show this help — auto-generated from inline annotations on each target.
	@echo ""
	@echo "Targets (grouped by section in this Makefile):"
	@echo ""
	@awk ' \
	  BEGIN { FS = ":[^#]*## " } \
	  /^# === / { \
	    section = substr($$0, 7); \
	    printed_header = 0; \
	    section_lines = ""; \
	    next; \
	  } \
	  /^[a-zA-Z][a-zA-Z0-9_-]*:.*## / { \
	    line = sprintf("    \033[36m%-26s\033[0m %s\n", $$1, $$2); \
	    if (section != "" && !printed_header) { \
	      printf "\n  \033[1m%s\033[0m\n", section; \
	      printed_header = 1; \
	    } \
	    printf "%s", line; \
	  } \
	' $(MAKEFILE_LIST)
	@echo ""

# === Go tools

tools: ## Install missing Go tools at their latest release.
	$(call ensure_gobin_tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest)
	$(call ensure_gobin_tool,$(GITLEAKS),github.com/zricethezav/gitleaks/v8@latest)
	$(call ensure_gobin_tool,$(GOVULNCHECK),golang.org/x/vuln/cmd/govulncheck@latest)
	$(call ensure_gobin_tool,$(OSV_SCANNER),github.com/google/osv-scanner/v2/cmd/osv-scanner@latest)

tools-update: ## Reinstall all Go tools at their latest release.
	GOBIN='$(GOBIN_DIR)' go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	GOBIN='$(GOBIN_DIR)' go install github.com/zricethezav/gitleaks/v8@latest
	GOBIN='$(GOBIN_DIR)' go install golang.org/x/vuln/cmd/govulncheck@latest
	GOBIN='$(GOBIN_DIR)' go install github.com/google/osv-scanner/v2/cmd/osv-scanner@latest

# === Code

test: ## Run `go test -race -shuffle=on ./...`.
	go test -race -shuffle=on ./... $(GO_TEST_FLAGS)

bench: ## Run all benchmarks with memory stats (not part of verify).
	go test -run='^$$' -bench=. -benchmem ./...

# Float64 results must not depend on the CPU. Apple silicon runs the amd64 build through
# Rosetta; an amd64 machine runs it natively.
cross-arch: ## Run the cross-architecture Float64 tests as amd64 (not part of verify).
	GOARCH=amd64 go test -run 'CrossArchitecture|NoFusedMultiplyAdd' ./internal/agg

fmt: tools ## Format Go code with the formatters in .golangci.yml.
	$(GOLANGCI_LINT) fmt

fix: ## Run `go fix ./...`.
	go fix ./...

lint: tools ## Run `golangci-lint run ./...`.
	$(GOLANGCI_LINT) run ./...

vuln: tools ## Check dependencies for known vulnerabilities (needs network).
	$(GOVULNCHECK) ./...

osv: tools ## Scan every lockfile in the repo with OSV-Scanner (needs network; not part of verify).
	$(OSV_SCANNER) scan source --recursive --all-vulns .

gomod: ## Run `go mod tidy`.
	go mod tidy

mod-download: ## Run `go mod download`.
	go mod download

verify: check-git-hooks tools fmt gomod lint vuln test ## Run fmt, gomod, lint, vuln, and test.

check-clean: ## Fail if the working tree is dirty (CI guard: verify must not rewrite files).
	@status="$$(git status --porcelain)"; \
	if [ -n "$$status" ]; then \
		echo; \
		echo "Working tree is dirty — verify rewrote files that should be committed:"; \
		echo "$$status" | sed 's/^/  /'; \
		echo; \
		exit 1; \
	fi

# === Simulation

# `make test` runs the harness's smoke tier with -race. These targets leave -race off unless
# SIM_RACE=-race: the scheduler runs one actor at a time, so -race sees only the core's own
# goroutines, and the smoke tier covers those.
SIM_RACE ?=

sim: ## Run every simulation scenario at its full tier of seeds (SIM_RACE=-race adds -race).
	go test $(SIM_RACE) -count=1 -timeout=60m ./internal/simulation -sim.full

sim-controls: ## Run every negative control of the simulation harness; each must be caught.
	go run ./internal/simulation/mutants

sim-seed: ## Rerun one simulation seed verbosely: make sim-seed SCENARIO=faults SEED=17.
	@[ -n "$(SCENARIO)" ] && [ -n "$(SEED)" ] || { echo "Usage: make sim-seed SCENARIO=<name> SEED=<n>"; exit 1; }
	go test $(SIM_RACE) -count=1 -v -run '^TestSim$$/^$(SCENARIO)$$' ./internal/simulation -sim.seed=$(SEED)

# === External scanners

# These tools are not Go modules, so `make tools` can't install them and
# they stay out of verify.

hadolint: ## Lint every Dockerfile with hadolint (needs hadolint on PATH).
	$(call require_cmd,hadolint,brew install hadolint)
	@files="$(DOCKERFILES)"; \
	if [ -z "$$files" ]; then echo "No Dockerfiles to lint."; else hadolint $$files; fi

# SAM templates are CloudFormation to checkov, so one framework covers both.
checkov: ## Scan CloudFormation/SAM templates with checkov (needs checkov on PATH).
	$(call require_cmd,checkov,brew install checkov)
	checkov --directory . --framework cloudformation --compact --quiet

# Version 1.58.0 is the one the templates were designed against.
infra-lint: ## Lint the golden CloudFormation templates with cfn-lint in every region (needs cfn-lint on PATH).
	$(call require_cmd,cfn-lint,uv tool install cfn-lint==1.58.0)
	go test -count=1 -run '^TestCfnLint$$' -v ./internal/infra/cloudformation

trivy: ## Scan the repo for vulnerabilities, secrets, and misconfigurations with trivy (needs trivy on PATH).
	$(call require_cmd,trivy,brew install trivy)
	trivy fs --scanners vuln,secret,misconfig --exit-code 1 --disable-telemetry --skip-version-check .

# Registry rulesets instead of `--config auto`, which needs metrics on.
semgrep: ## Run semgrep's Go and Trail of Bits rulesets (needs semgrep on PATH).
	$(call require_cmd,semgrep,brew install semgrep)
	semgrep scan --config p/golang --config p/trailofbits --error --metrics=off .

# === Git configuration

check-git-hooks: ## Check if local repo has git hooks properly configured (hard fail). No-op in CI.
	@if [ -n "$${CI:-}" ]; then exit 0; fi; \
	expected=".githooks"; \
	current="$$(git config --get core.hooksPath || true)"; \
	if [ "$$current" != "$$expected" ]; then \
		echo; \
		echo "Git hooks are not configured."; \
		echo; \
		echo "Run:"; \
		echo "  make setup-git-hooks"; \
		echo; \
		echo "Current core.hooksPath: '$$current'"; \
		echo "Expected: '$$expected'"; \
		echo; \
		exit 1; \
	fi

warn-git-hooks: ## Warn (log only) if local repo does not have git hooks configured
	@expected=".githooks"; \
	current="$$(git config --get core.hooksPath || true)"; \
	if [ "$$current" != "$$expected" ]; then \
		echo; \
		echo "WARNING: git hooks are not configured"; \
		echo "Run: make setup-git-hooks"; \
		echo; \
	fi

setup-git-hooks: ## Setup local Git hooks, need to run this only once
	git config core.hooksPath .githooks
	find .githooks -type f -not -name '_*' -exec chmod +x {} +

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

# golangci-lint is pinned in tools/go.mod and runs through `go tool`, because other
# projects pin their own copy in GOBIN_DIR and .golangci.yml needs this version.
# govulncheck is pinned to the version release.yml runs, because verify installs it on
# every CI run. The other Go tools are not pinned: `make tools` installs the latest release
# of any that is missing, and `make tools-update` upgrades them all and bumps the pin.
GOLANGCI_LINT ?= go tool -modfile=tools/go.mod golangci-lint
GOVULNCHECK_VERSION := v1.9.0

# Absolute paths to the tool binaries installed into GOBIN_DIR.
GITLEAKS      := $(GOBIN_DIR)/gitleaks
GOVULNCHECK   := $(GOBIN_DIR)/govulncheck
OSV_SCANNER   := $(GOBIN_DIR)/osv-scanner
BENCHSTAT     := $(GOBIN_DIR)/benchstat

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

# The shell scripts under scripts/, found the same way.
SHELL_SCRIPTS = $(shell git ls-files --cached --others --exclude-standard -- 'scripts/*.sh')

.PHONY: help check-git-hooks warn-git-hooks setup-git-hooks tools tools-update \
        test bench bench-compare bench-smoke cross-arch lambda-build fmt fix lint vuln osv gomod notices check-go-bound mod-download verify \
        check-clean clean sim sim-controls sim-seed emulators-up emulators-down integration \
        e2e-emulator e2e-aws bench-emulator bench-aws hadolint checkov infra-lint shellcheck trivy semgrep

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

tools: ## Install missing Go tools: govulncheck at its pin, the others at their latest release.
	$(call ensure_gobin_tool,$(GITLEAKS),github.com/zricethezav/gitleaks/v8@latest)
	$(call ensure_gobin_tool,$(GOVULNCHECK),golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION))
	$(call ensure_gobin_tool,$(OSV_SCANNER),github.com/google/osv-scanner/v2/cmd/osv-scanner@latest)

tools-update: ## Reinstall all Go tools at their latest release and bump the golangci-lint pin.
	go -C tools get -tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	go -C tools mod tidy
	GOBIN='$(GOBIN_DIR)' go install github.com/zricethezav/gitleaks/v8@latest
	GOBIN='$(GOBIN_DIR)' go install golang.org/x/vuln/cmd/govulncheck@latest
	GOBIN='$(GOBIN_DIR)' go install github.com/google/osv-scanner/v2/cmd/osv-scanner@latest
	GOBIN='$(GOBIN_DIR)' go install golang.org/x/perf/cmd/benchstat@latest

# === Code

test: ## Run `go test -race -shuffle=on ./...`.
	go test -race -shuffle=on ./... $(GO_TEST_FLAGS)

# Literal paths, not BENCH_OUT: an override can point outside the repo.
clean: ## Remove build outputs from the repo root (a stray mutants binary, bench.txt, dist/) and stop DynamoDB Local.
	rm -rf mutants bench.txt dist
	@if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then \
		docker compose --file $(EMULATORS_COMPOSE) down; fi

# docs/runbooks/benchmarks.md. The baseline is recorded on the maintainer's idle machine.
BENCH_COUNT    ?= 6
BENCH_OUT      ?= bench.txt
BENCH_BASELINE := docs/runbooks/benchmarks-baseline.txt

# One package at a time: packages benchmarked in parallel slow each other down. benchstat reads
# the go line as configuration, since allocs/op changes with the toolchain. The recipe sets
# pipefail itself because make 3.81, the one macOS ships, ignores .SHELLFLAGS.
bench: ## Run every benchmark BENCH_COUNT times, one package at a time, into BENCH_OUT (not part of verify).
	@set -euo pipefail; \
	{ echo "go: $$(go env GOVERSION)"; go test -p 1 -run='^$$' -bench=. -benchmem -count=$(BENCH_COUNT) ./...; } | tee $(BENCH_OUT)

# benchstat exits 0 when it can't open a file.
bench-compare: ## Compare BENCH_OUT with the recorded baseline; times compare only on the machine that recorded it.
	$(call ensure_gobin_tool,$(BENCHSTAT),golang.org/x/perf/cmd/benchstat@latest)
	@[ -f '$(BENCH_BASELINE)' ] || { echo "No baseline at $(BENCH_BASELINE): docs/runbooks/benchmarks.md says how to record one." >&2; exit 1; }
	@[ -f '$(BENCH_OUT)' ] || { echo "No $(BENCH_OUT): run make bench first." >&2; exit 1; }
	$(BENCHSTAT) $(BENCH_BASELINE) $(BENCH_OUT)

# The benchmarks check their results, so one iteration of each catches a broken one.
bench-smoke: ## Run every benchmark once.
	go test -run='^$$' -bench=. -benchtime=1x ./... $(GO_TEST_FLAGS)

# Float64 results must not depend on the CPU. Apple silicon runs the amd64 build through
# Rosetta; an amd64 machine runs it natively.
cross-arch: ## Run the cross-architecture Float64 tests as amd64 (not part of verify).
	GOARCH=amd64 go test -run 'CrossArchitecture|NoFusedMultiplyAdd' ./internal/agg

# The example's generated main.go, built as sam build builds it for the templates' default
# architecture, so a change to codegen or the runtime that breaks the Lambda binary fails.
lambda-build: ## Build the example's Lambda binary for linux/arm64.
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /dev/null ./examples/analytics/cmd/dynaflux-runtime

fmt: ## Format Go code with the formatters in .golangci.yml.
	$(GOLANGCI_LINT) fmt

fix: ## Run `go fix ./...`.
	go fix ./...

# The second run covers the files behind the integration build tag; the first still covers
# the !integration ones. The third covers the end-to-end suite, which nothing else builds.
lint: ## Run `golangci-lint run ./...`, then again with the integration and e2e build tags.
	$(GOLANGCI_LINT) run ./...
	$(GOLANGCI_LINT) run --build-tags integration ./...
	$(GOLANGCI_LINT) run --build-tags e2e $(E2E_PKG)/...

vuln: ## Check dependencies for known vulnerabilities (needs network).
	$(call ensure_gobin_tool,$(GOVULNCHECK),golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION))
	$(GOVULNCHECK) ./...

osv: ## Scan every lockfile in the repo with OSV-Scanner (needs network; not part of verify).
	$(call ensure_gobin_tool,$(OSV_SCANNER),github.com/google/osv-scanner/v2/cmd/osv-scanner@latest)
	$(OSV_SCANNER) scan source --recursive --all-vulns .

gomod: ## Run `go mod tidy` on the library and the tools module.
	go mod tidy
	go -C tools mod tidy

# The release archives carry THIRD_PARTY_NOTICES (docs/runbooks/release.md). verify rewrites
# it, so CI's check-clean fails when the committed file is stale.
notices: ## Write THIRD_PARTY_NOTICES: the licenses and NOTICE files of the code in the dynaflux binary.
	scripts/third-party-notices.sh

# Dynaflux supports Go 1.26 and later, so no module may need a newer Go. The bound is fixed
# here: `go get` and `go mod tidy` raise go.mod's own go line to the highest of any
# dependency, so comparing with that line could never fail.
GO_BOUND := 1.26

# The recipe sets pipefail itself because make 3.81, the one macOS ships, ignores .SHELLFLAGS.
check-go-bound: ## Fail if go.mod's go or toolchain line, or any module's go line, is above GO_BOUND.
	@set -euo pipefail; \
	{ go mod edit -json | awk -F'"' '$$2 == "Toolchain" { print "go.mod", "toolchain", $$4 }'; \
	  go list -m -f '{{.Path}} go {{.GoVersion}}' all; } | \
	awk -v bound='$(GO_BOUND)' ' \
	  function above(v,  p, b) { \
	    sub(/^go/, "", v); split(v, p, "."); split(bound, b, "."); \
	    return p[1] + 0 > b[1] + 0 || (p[1] + 0 == b[1] + 0 && p[2] + 0 > b[2] + 0); \
	  } \
	  NF == 3 && above($$3) { found = found "\n  " $$0 } \
	  END { \
	    if (found != "") { \
	      printf "Dynaflux supports Go %s, but these need a newer Go:%s\n", bound, found > "/dev/stderr"; \
	      print "Use module versions that support it, and keep go.mod'\''s go and toolchain lines within it." > "/dev/stderr"; \
	      exit 1; \
	    } \
	  }'

mod-download: ## Run `go mod download`.
	go mod download

verify: check-git-hooks fmt gomod notices check-go-bound lint vuln test bench-smoke lambda-build ## Run fmt, gomod, notices, the Go version bound, lint, vuln, test, the benchmarks once and the Lambda build.

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

# === Integration

# DynamoDB Local in Docker (docs/decisions/0009-emulator-choice.md). emulators-up leaves a
# running container alone, so tests run by hand can share it. integration recreates it: DynamoDB
# Local keeps about 200 MB from each run, even after the tests delete their tables, and its
# requests time out once it holds a few GB. No target stops it but emulators-down.
EMULATORS_COMPOSE := scripts/emulators/compose.yaml
DYNAMODB_ENDPOINT := http://127.0.0.1:8000

emulators-up: ## Start DynamoDB Local unless it runs, and wait until it answers (needs docker).
	$(call require_cmd,docker,brew install --cask docker)
	scripts/emulators/up.sh

emulators-down: ## Stop DynamoDB Local and drop its tables.
	$(call require_cmd,docker,brew install --cask docker)
	docker compose --file $(EMULATORS_COMPOSE) down

# Packages are found by their *_integration_test.go files, so a new one needs no edit here.
integration: ## Run the tests of every package with a *_integration_test.go file against a fresh DynamoDB Local, then stop it.
	$(call require_cmd,docker,brew install --cask docker)
	@set -euo pipefail; \
	pkgs="$$(go list -tags integration -f '{{.ImportPath}} {{.TestGoFiles}} {{.XTestGoFiles}}' ./... | \
		awk '/_integration_test\.go/ { print $$1 }')"; \
	if [ -z "$$pkgs" ]; then echo "No *_integration_test.go files found."; exit 0; fi; \
	echo "go test -race -shuffle=on -tags integration" $$pkgs; \
	DYNAFLUX_DYNAMODB_ENDPOINT='$(DYNAMODB_ENDPOINT)' scripts/emulators/with.sh --fresh \
		go test -race -shuffle=on -tags integration $$pkgs $(GO_TEST_FLAGS)

# === End to end

# docs/runbooks/e2e.md. The suite is the example's, behind the e2e build tag.
E2E_PKG := ./examples/analytics/e2e

e2e-emulator: ## Run the end-to-end suite on DynamoDB Local (needs docker).
	$(call require_cmd,docker,brew install --cask docker)
	DYNAFLUX_DYNAMODB_ENDPOINT='$(DYNAMODB_ENDPOINT)' scripts/emulators/with.sh \
		go test -race -tags e2e $(E2E_PKG) -run '^TestE2E$$' -target=emulator $(GO_TEST_FLAGS)

# Maintainer only: it deploys to AWS with your credentials. common.sh checks the variables and
# the account before the trap, which tears the run down even after a later failure;
# scripts/e2e/teardown.sh does it by hand after an interrupted run. deploy.sh keeps the project
# in E2E_WORK, which scenarios 24 and 36 change; E2E_BACKFILL=1 runs them, for about 3 hours more.
e2e-aws: ## Deploy the e2e store, run the suite on AWS, and tear it down: make e2e-aws E2E_RUN=<id> E2E_ACCOUNT=<sandbox account ID> [E2E_BACKFILL=1].
	set -euo pipefail; export E2E_RUN='$(E2E_RUN)' E2E_ACCOUNT='$(E2E_ACCOUNT)' E2E_REGIONS='$(E2E_REGIONS)'; \
	bash -c 'source scripts/e2e/common.sh'; \
	if [ -n '$(E2E_BACKFILL)' ] && [[ "$${E2E_OPERATOR_ROLE:-}" != "arn:aws:iam::$$E2E_ACCOUNT:role/"* ]]; then \
		echo "E2E_BACKFILL needs E2E_OPERATOR_ROLE, the ARN of a role in E2E_ACCOUNT (docs/runbooks/e2e.md §1)." >&2; exit 1; fi; \
	export E2E_WORK="$$(mktemp -d)"; \
	trap 'rm -rf "$$E2E_WORK"; scripts/e2e/teardown.sh || { echo "Teardown failed: run E2E_RUN=$$E2E_RUN E2E_ACCOUNT=$$E2E_ACCOUNT E2E_REGIONS=\"$$E2E_REGIONS\" scripts/e2e/teardown.sh again." >&2; exit 1; }' EXIT; \
	scripts/e2e/deploy.sh; \
	go test -tags e2e -timeout $(if $(E2E_BACKFILL),360m,120m) -v $(E2E_PKG) -run '^TestE2E$$' -target=aws $(if $(E2E_BACKFILL),-backfill) $(GO_TEST_FLAGS)

# docs/runbooks/benchmarks.md. A low rate on DynamoDB Local checks the load, the probes and the
# oracle; the report reads CloudWatch, so only bench-aws prints one. The result goes in a new
# directory, so that two runs, or a link someone put in a shared /tmp, can't take its place.
bench-emulator: ## Run the throughput benchmark for 30 s at 50 events/s on DynamoDB Local (needs docker).
	$(call require_cmd,docker,brew install --cask docker)
	@set -eu; out="$$(mktemp -d)/bench-emulator.json"; echo "The result goes to $$out."; \
	DYNAFLUX_DYNAMODB_ENDPOINT='$(DYNAMODB_ENDPOINT)' scripts/emulators/with.sh \
		go test -race -tags e2e $(E2E_PKG) -run '^TestThroughput$$' -target=emulator \
		-throughput.rate=50 -throughput.warmup=5s -throughput.duration=30s -throughput.out="$$out" \
		$(GO_TEST_FLAGS)

# Maintainer only: it deploys to AWS with your credentials. It checks THROUGHPUT_OUT and
# compiles the suite before anything is deployed. THROUGHPUT_OUT must be a new file, so that
# the report never reads an earlier run's result. The report runs even after a failed load,
# and the trap tears the run down after both. A manual teardown needs the same E2E_REGIONS.
bench-aws: ## Deploy the e2e store, load it, report and tear it down: make bench-aws E2E_RUN=<id> E2E_ACCOUNT=<id> THROUGHPUT_OUT=<file outside the repo> [E2E_REGIONS=…] [BENCH_PREWARM=…] [BENCH_FLAGS=…].
	@set -euo pipefail; out='$(THROUGHPUT_OUT)'; \
	if [ -z "$$out" ]; then echo "Set THROUGHPUT_OUT to the run's result file, outside the repo, such as THROUGHPUT_OUT=$$HOME/bench/r1.json." >&2; exit 1; fi; \
	dir="$$(cd "$$(dirname "$$out")" 2>/dev/null && pwd -P)" || { echo "THROUGHPUT_OUT=$$out: its directory doesn't exist." >&2; exit 1; }; \
	case "$$dir/" in "$$(pwd -P)/"*) echo "THROUGHPUT_OUT=$$out is inside the repo: put the result outside it." >&2; exit 1;; esac; \
	out="$$dir/$$(basename "$$out")"; \
	[ ! -e "$$out" ] || { echo "THROUGHPUT_OUT=$$out exists: name a new file." >&2; exit 1; }; \
	bin="$$(mktemp -d)/e2e.test"; trap 'rm -r "$$(dirname "$$bin")"' EXIT; \
	go test -c -tags e2e -o "$$bin" $(E2E_PKG); \
	export E2E_RUN='$(E2E_RUN)' E2E_ACCOUNT='$(E2E_ACCOUNT)' E2E_REGIONS='$(E2E_REGIONS)'; \
	bash -c 'source scripts/e2e/common.sh'; \
	trap 'rm -r "$$(dirname "$$bin")"; scripts/e2e/teardown.sh || { echo "Teardown failed: run E2E_RUN=$$E2E_RUN E2E_ACCOUNT=$$E2E_ACCOUNT E2E_REGIONS=\"$$E2E_REGIONS\" scripts/e2e/teardown.sh again." >&2; exit 1; }' EXIT; \
	scripts/e2e/deploy.sh; \
	if [ -n '$(BENCH_PREWARM)' ]; then scripts/e2e/prewarm.sh $(BENCH_PREWARM); fi; \
	status=0; \
	(cd $(E2E_PKG) && "$$bin" -test.v -test.timeout 180m -test.run '^TestThroughput$$' -target=aws -throughput.out="$$out" $(BENCH_FLAGS)) || status=$$?; \
	(cd $(E2E_PKG) && "$$bin" -test.v -test.timeout 30m -test.run '^TestThroughputReport$$' -target=aws -throughput.out="$$out") || \
		{ s=$$?; [ "$$status" -ne 0 ] || status=$$s; }; \
	exit "$$status"

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

# Version 1.58.0 is the one the templates were designed against. The e2e FIS template is
# linted only in the example's regions, where deploy.sh puts it: AWS FIS isn't in every
# region. As in TestCfnLint, informational findings don't fail.
infra-lint: ## Lint the golden CloudFormation templates and the example's with cfn-lint in every region, and the e2e FIS template in the example's (needs cfn-lint on PATH).
	$(call require_cmd,cfn-lint,uv tool install cfn-lint==1.58.0)
	go test -count=1 -run '^TestCfnLint$$' -v ./internal/infra/cloudformation
	cfn-lint --regions us-east-1 us-west-2 --non-zero-exit-code warning -- $(E2E_PKG)/fis.yaml

# -x follows the files that scripts source. TestDocShell checks the sh blocks of the README and
# the guides, and runs only with -shellcheck.
shellcheck: ## Lint the shell scripts under scripts/, and the sh blocks of the README and the guides, with shellcheck (needs shellcheck on PATH).
	$(call require_cmd,shellcheck,brew install shellcheck)
	shellcheck -x $(SHELL_SCRIPTS)
	go test -count=1 -run '^TestDocShell$$' ./internal/cli -shellcheck

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

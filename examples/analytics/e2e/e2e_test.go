// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

// Package e2e_test is the end-to-end suite of docs/runbooks/e2e.md. It runs the example
// store, with testdata/e2e.sql added, in one of two targets:
//
//   - -target=emulator: DynamoDB Local, each region a database of its own, with the
//     projector run as the Lambda binary in a child process fed by esmlocal;
//   - -target=aws: the stacks that scripts/e2e/deploy.sh deployed for E2E_RUN, with the
//     maintainer's credentials. AI assistants and CI never run it.
package e2e_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/runtime"
)

//nolint:gochecknoglobals // a test flag
var target = flag.String("target", "", "where the suite runs: emulator or aws")

const (
	targetEmulator = "emulator"
	targetAWS      = "aws"
)

// The test binary is also the Lambda binary: with childVar set, TestMain serves the
// catalog in catalogVar as the generated main does, instead of running tests.
const (
	childVar   = "DYNAFLUX_E2E_CHILD"
	catalogVar = "DYNAFLUX_E2E_CATALOG"
)

func TestMain(m *testing.M) {
	if os.Getenv(childVar) != "" {
		if err := runtime.Run(fluxgen.LoadCatalog(os.Getenv(catalogVar))); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	os.Exit(m.Run())
}

// scenarios run in this order. On AWS, 7 reads the logs of the scenarios before it, and 23
// denies the projector writes on purpose, so it comes last.
//
//nolint:gochecknoglobals // the suite's table
var scenarios = []struct {
	name    string
	targets []string
	run     func(*testing.T, *harness)
}{
	{"6_mappings", []string{targetAWS}, testMappings},
	{"18_lake_formation", []string{targetAWS}, testLakeFormation},
	{"1_appends", []string{targetEmulator, targetAWS}, testAppends},
	{"4_remove", []string{targetEmulator, targetAWS}, testRemove},
	{"5_poison", []string{targetEmulator, targetAWS}, testPoison},
	{"2_dedup", []string{targetAWS}, testDedup},
	{"8_archive", []string{targetEmulator, targetAWS}, testArchive},
	{"7_least_privilege", []string{targetAWS}, testLeastPrivilege},
	{"3_replication_pause", []string{targetAWS}, testReplicationPause},
	{"23_partial_failures", []string{targetAWS}, testPartialFailures},
}

// TestE2E runs the scenarios of the target on one store, one after the other.
//
//nolint:paralleltest // the scenarios share one store, and some disturb it on purpose
func TestE2E(t *testing.T) {
	p := loadProject(t)
	var h *harness
	switch *target {
	case targetEmulator:
		h = newEmulator(t, p)
	case targetAWS:
		h = newAWS(t, p)
	default:
		t.Fatalf("-target is %q; want %s or %s (docs/runbooks/e2e.md)", *target, targetEmulator, targetAWS)
	}
	for _, s := range scenarios {
		if slices.Contains(s.targets, *target) {
			t.Run(s.name, func(t *testing.T) { s.run(t, h) })
		}
	}
}

// harness is the e2e store in one target.
type harness struct {
	p       *project
	regions []*region // in the catalog's order; the first is the home region
	// replicated is set when an append reaches every region, not only its own.
	replicated bool
	// patience is how long a region may take to apply what it was sent, and
	// archivePatience how long its archive may take.
	patience, archivePatience time.Duration
	aws                       *awsTarget // nil on the emulator
}

// region is one region of the e2e store.
type region struct {
	name    string
	naming  catalog.Naming
	store   *dynaflux.Store
	client  *flux.Client // the example's client: it doesn't know e2e_ratio
	ddb     *dynamodb.Client
	streams *dynamodbstreams.Client
	meta    *meta.Meta
	// settle applies every stream record written so far: the emulator drains the streams
	// through the projector, and on AWS the event source mappings do it.
	settle func(context.Context) error
	// archive returns what the region archived of a stream's events whose column key
	// starts with prefix.
	archive func(ctx context.Context, stream, key, prefix string) (archived, error)
	// logs returns the projector's output so far on the emulator; it is nil on AWS.
	logs func() string
}

// pollInterval is how often await checks again on AWS.
const pollInterval = 15 * time.Second

// await settles every region and calls check until it returns nil, for at most patience,
// and then fails the test with what check last returned.
func (h *harness) await(t *testing.T, patience time.Duration, what string, check func(context.Context) error) {
	t.Helper()
	deadline := time.Now().Add(patience)
	for {
		err := h.settle(t.Context())
		if err == nil {
			if err = check(t.Context()); err == nil {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %v", what, err)
		}
		select {
		case <-t.Context().Done():
			t.Fatalf("%s: %v", what, t.Context().Err())
		case <-time.After(pollInterval):
		}
	}
}

func (h *harness) settle(ctx context.Context) error {
	for _, r := range h.regions {
		if err := r.settle(ctx); err != nil {
			return fmt.Errorf("%s: %w", r.name, err)
		}
	}
	return nil
}

// setLive marks every view version live on a fresh store, before its first append: until
// M5's backfill, nothing else does (m3 decision 10). A store with view states is an earlier
// run's, and keeps them.
func setLive(ctx context.Context, r *region, cat *catalog.Catalog) error {
	states, err := r.meta.ViewStates(ctx)
	if err != nil || len(states) > 0 {
		return err
	}
	return r.meta.SetLive(ctx, cat)
}

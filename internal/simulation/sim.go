// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package simulation is the deterministic harness for the projector: regions, change
// streams, Lambda's event source mapping, faults and an oracle, all in one testing/synctest
// bubble per seed, so a failing seed replays exactly.
//
// The code under test sees only a [store.Backend], an [Archiver] and a context that
// carries its invocation's deadline. Every request parks at a gate, and a seeded scheduler
// releases one at a time once every other goroutine is blocked; the fake clock supplies
// latency, deadlines and delays. Only tests import this package.
//
// A run checks these properties:
//   - P0: every applied append is in each region's change stream exactly once.
//   - P1: each region's views equal the oracle over that region's records.
//   - P2: regions hold the same rows.
//   - P3: a view is broken exactly when a record fails its evaluation; a live view, by
//     the time a Handle of that record returns without error.
//   - P4: a row holds high-water marks only for units that can still deliver.
//   - P5: row versions rise by one; marker owners, progress and fences never go back.
//   - P6: the run reaches quiescence within its budget.
//   - P7: no request comes from an invocation whose Handle returned.
//   - P8: no core panics.
//   - P9: an empty batch makes no request.
//
// A run also fails when one invocation has two identical requests in flight (D3): the
// seed can't order them, so the run wouldn't replay.
package simulation

import (
	"context"
	"math/rand/v2"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// Core is the projector instance of one warm container. Its only clock limit is the
// deadline of the context Handle gets.
type Core interface {
	Handle(ctx context.Context, b projector.Batch) (projector.Result, error)
}

// Archiver takes the records a core archives.
type Archiver interface {
	Archive(ctx context.Context, stream, unit string, recs []projector.Record) error
}

// Config is what the harness needs of a system: the catalog whose streams the writers
// append to, through [engine.Engine.Append], and the names of the store's tables. Every
// view version is live when a run starts.
type Config struct {
	Catalog *engine.Catalog
	Naming  catalog.Naming
}

// System is the code under test with its reference semantics.
type System interface {
	Config() Config
	// Event returns the values of the n-th event to append, as Append takes them, and its
	// stream. A poison event fails the evaluation of a view.
	Event(r *rand.Rand, n int, poison bool) (stream string, vals []expr.Value)
	// NewCore returns a new container's core; b and a carry the region's faults.
	NewCore(b store.Backend, a Archiver) Core
	// Expect feeds an oracle one INSERT record of a region. A region's oracle gets each
	// record once, whatever the deliveries were; P3 also feeds the records of a batch to
	// an oracle of their own, to learn which views they fail.
	Expect(o *Oracle, unit string, rec projector.Record) error
	// Read reads every view of a region through the read path, without faults.
	Read(ctx context.Context, b store.Backend) (State, error)
}

// Scenario configures one kind of run.
type Scenario struct {
	Name     string
	Regions  int
	Events   int
	Writers  int
	Poison   float64 // the share of poison events
	Faults   Faults
	Topology Topology
	ESM      ESM
	Archive  Archive
	// Window, when set, replaces the catalog's W in P4, for a core that prunes with another.
	Window time.Duration
	// Budget is the fake time a seed has to reach quiescence (P6).
	Budget time.Duration
	Seeds  struct{ Smoke, Full int }
}

// Faults are the per-request fault rates and the response times of the store and the
// archive.
type Faults struct {
	Transient float64 // fails without effect
	Ambiguous float64 // a write applies, but its caller sees a timeout
	Retried   float64 // a conditional write applies, then fails its condition on the SDK's retry
	Crash     float64 // the invocation dies
	Spike     float64 // the response takes up to SpikeMax longer
	Late      float64 // a core's write lands up to LateMax after its deadline; the core dies
	Latency   time.Duration
	SpikeMax  time.Duration
	LateMax   time.Duration
}

// Topology shapes the change streams.
type Topology struct {
	Partitions  int     // root delivery units per stream
	UnitRecords int     // the most records a unit takes before it closes
	Split       float64 // a closing unit has two children instead of one
	MaxDepth    int     // splits along one lineage
}

// ESM models Lambda's event source mapping.
type ESM struct {
	Timeout  time.Duration // invocation lifetime
	Window   time.Duration // batching window
	MaxBatch int
	PF       int     // ParallelizationFactor; production uses 1
	Replay   bool    // a second mapping replays every unit from the trim horizon
	Dup      float64 // a batch that succeeded is delivered again
	Stale    float64 // a stale invocation of a batch runs alongside it
	Empty    float64 // an empty invocation arrives, as at a tumbling window's end
	Slow     float64 // one response of an invocation stalls until near its deadline
}

// Archive models Firehose delivering into Iceberg.
type Archive struct {
	MinDelay, MaxDelay time.Duration // from an acknowledged put to delivery
	Dup                float64       // a record is delivered twice
	Error              float64       // a record goes to the error prefix instead
	Lie                bool          // the watermark reports complete too early
}

// Outcome is one seed's run.
type Outcome struct {
	Steps      int64
	Trace      uint64        // digest of the release order; equal runs have equal traces
	Time       time.Duration // fake time until every actor exited
	Stats      Stats
	Violations []string // sorted; each starts with its property, such as "P1:"
}

// Stats counts what a run exercised. Killed counts the requests refused because their
// invocation had died or reached its deadline; Slow counts stalled responses; Poison
// counts poison events.
type Stats struct {
	Requests, Transient, Ambiguous, Retried, Spikes, Crashes, Killed, Late, Slow int64
	Invocations, Timeouts, Stale, Dups, Empty, Containers, Units, Poison         int64
}

// Run runs seed of sc against sys in its own synctest bubble and checks the properties.
// Call t.Parallel before it, if at all: a bubble forbids it.
func Run(t *testing.T, sc Scenario, sys System, seed uint64) Outcome {
	t.Helper()
	if err := sc.check(); err != nil {
		t.Fatalf("scenario %s: %v", sc.Name, err)
	}
	var out Outcome
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		r, err := newRun(t.Context(), sc, sys, seed)
		if err != nil {
			t.Fatalf("scenario %s seed %d: %v", sc.Name, seed, err)
		}
		r.start()
		r.sched.run()
		took := time.Since(start)
		// Time stops when this goroutine returns, so goroutines that outlived their Handle
		// get the budget to reach a gate, where P7 reports them. One still blocked after it
		// fails the test through synctest's deadlock panic.
		time.Sleep(sc.Budget)
		out = r.outcome(t.Context(), took)
	})
	return out
}

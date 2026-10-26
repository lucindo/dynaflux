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
//   - P0: every applied append is in each region's change stream exactly once as an INSERT,
//     and once as a later REMOVE when some region deleted it; there is a MODIFY of it only
//     when other regions appended it too.
//   - P1: each region's views equal the oracle over that region's records.
//   - P2: regions hold the same logical rows: each aggregate's state, merged across a row's
//     SPLIT items, byte for byte. A view that keys rows by a DEDUP BY stream's ingest_time
//     or origin_region is compared with its rows merged across those keys, and rows that a
//     divergent duplicate reached are left out.
//   - P3: a view is broken exactly when a record fails its evaluation; a live view, by
//     the time a Handle that evaluated the record returns without error.
//   - P4: a row item holds high-water marks only for the units of its view's stream that
//     are still delivering or finished within W plus two lifetimes (and the lateness of a
//     late write), and the backfill's.
//   - P5: row versions rise by one; marker owners, progress and fences never go back.
//   - P6: the run reaches quiescence within its budget, every backfilled version live or
//     broken.
//   - P7: no request comes from an invocation whose Handle returned, and every request of
//     a core carries a context that ends by its invocation's deadline: in Lambda, only the
//     context stops a request.
//   - P8: no core panics.
//   - P9: an empty batch makes no request.
//
// A run also fails when one invocation has two identical requests in flight (D3): the
// seed can't order them, so the run wouldn't replay. It fails, too, when an append returns
// ErrDuplicate though its write found no other call's event, or success though it did
// ("events:"), and when a Handle reports more divergent MODIFY records than its batch
// holds ("Stats:").
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
// deadline of the context Handle or ApplyBackfill gets.
type Core interface {
	Handle(ctx context.Context, b projector.Batch) (projector.Result, error)
	ApplyBackfill(ctx context.Context, version string, chunk []projector.BackfillRecord) error
}

// Archiver takes the records a core archives.
type Archiver interface {
	Archive(ctx context.Context, stream, unit string, recs []projector.Record) error
}

// Config is what the harness needs of a system: the catalog whose streams the writers
// append to, through [engine.Engine.Append], and the names of the store's tables.
type Config struct {
	Catalog *engine.Catalog
	Naming  catalog.Naming
	// Backfill names the view versions that a scenario with a backfill starts pending their
	// backfill, then backfills. Every other version is live when a run starts.
	Backfill []string
}

// System is the code under test with its reference semantics.
type System interface {
	Config() Config
	// Event returns the n-th event to append. A poison event fails the evaluation of a view.
	Event(r *rand.Rand, n int, poison bool) Event
	// NewCore returns a new container's core; b and a carry the region's faults.
	NewCore(b store.Backend, a Archiver) Core
	// Expect feeds an oracle one INSERT record of a region's stream. A region's oracle gets
	// each record once, whatever the deliveries were; P3 also feeds the records of a batch
	// to an oracle of their own, to learn which views they fail.
	Expect(o *Oracle, stream, unit string, rec projector.Record) error
	// Read reads every view of a region through the read path, without faults.
	Read(ctx context.Context, b store.Backend) (State, error)
}

// Event is an event to append.
type Event struct {
	Stream string
	Vals   []expr.Value // as Append takes them
	// Divergent, when set, is what a writer appends in a second region instead of Vals: the
	// same DEDUP BY identity with other values, which regions may keep apart (sql.md §8.5).
	Divergent []expr.Value
}

// Scenario configures one kind of run.
type Scenario struct {
	Name     string
	Regions  int
	Events   int
	Writers  int
	WriteGap time.Duration // the most time between a writer's appends
	Poison   float64       // the share of poison events
	// Duplicate is the share of a DEDUP BY stream's events that a writer appends in a
	// second region too, as a client that fails over does.
	Duplicate float64
	Faults    Faults
	Topology  Topology
	Isolation Isolation
	Sweep     Sweep
	ESM       ESM
	Archive   Archive
	Backfill  Backfill
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
	// Idle is the chance that an open unit goes quiet after a batch, as when no write
	// reaches its keys for a while: no mapping delivers it for up to IdleFor.
	Idle    float64
	IdleFor time.Duration
}

// Isolation cuts regions off: replication to and from a region, or only one way, pauses,
// and its backlog lands when it resumes. The region keeps taking appends meanwhile.
type Isolation struct {
	Share  float64       // the chance that a region is cut off, once
	Within time.Duration // the latest a cut starts
	Max    time.Duration // the longest a cut lasts
}

// Sweep models DynamoDB's TTL deletes: each region deletes a stream item or a unit item
// whose _ttl has passed up to Delay later. A stream item's delete emits a REMOVE and
// replicates.
type Sweep struct {
	Delay time.Duration // 0 for no deletes
}

// ESM models Lambda's event source mapping.
type ESM struct {
	Timeout  time.Duration // invocation lifetime
	Window   time.Duration // batching window
	MaxBatch int
	PF       int // ParallelizationFactor; production uses 1
	// Replay, when set, is the latest a second mapping starts replaying every unit from the
	// trim horizon. Records are never trimmed, so it stays within the streams' retention.
	Replay time.Duration
	Dup    float64 // a batch that succeeded is delivered again
	Stale  float64 // a stale invocation of a batch runs alongside it
	Empty  float64 // an empty invocation arrives, as at a tumbling window's end
	Slow   float64 // one response of an invocation stalls until near its deadline
}

// Archive models Firehose delivering into Iceberg.
type Archive struct {
	MinDelay, MaxDelay time.Duration // from an acknowledged put to delivery
	Dup                float64       // a record is delivered twice
	Error              float64       // a record goes to the error prefix instead
	Lie                bool          // the watermark reports complete too early
}

// Backfill configures the backfills of the versions that [Config] names: one coordinator
// per version and region.
type Backfill struct {
	Within time.Duration // each coordinator starts at most this long after the run; 0 for no backfill
	Chunk  int           // the most records in a chunk
}

// Outcome is one seed's run.
type Outcome struct {
	Steps      int64
	Trace      uint64        // digest of the release order; equal runs have equal traces
	Time       time.Duration // fake time until every actor exited
	Stats      Stats
	Violations []string // sorted; each starts with its property, such as "P1:"
	// MaxHWM is the most high-water marks, the backfill's included, that a row write left.
	MaxHWM int
}

// Stats counts what a run exercised. Killed counts the requests refused because their
// invocation had died or reached its deadline; Slow counts stalled responses; Poison
// counts poison events. Duplicates counts the appends of an event in a second region, and
// Modifies the MODIFY records they caused, Divergent those of events appended with two
// payloads, and Reported the divergent records that Handles which returned counted. Cuts
// counts the isolations, and Held the replicated writes they held back. Removes counts
// REMOVE records. Backfills counts the versions a backfill made live, Chunks the worker
// invocations that applied a chunk, and Unlisted the units created after a backfill listed
// their stream's units. Idle counts the times a unit went quiet. Pruned counts the
// high-water marks that row writes dropped, and PrunedOpen those of units still delivering.
type Stats struct {
	Requests, Transient, Ambiguous, Retried, Spikes, Crashes, Killed, Late, Slow int64
	Invocations, Timeouts, Stale, Dups, Empty, Containers, Units, Poison         int64
	Duplicates, Modifies, Divergent, Reported, Cuts, Held, Removes               int64
	Backfills, Chunks, Unlisted, Idle, Pruned, PrunedOpen                        int64
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

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
//     when other regions appended it too. An event appended again after its REMOVE starts
//     the item's next life, which is checked the same way. An append acknowledged as stored,
//     not as a duplicate, wrote its event: an item with its key, origin_region and
//     ingest_time.
//   - P1: each region's views equal the oracle over that region's records. A version retired
//     in the region is left out, and once its tombstone could be cached no invocation that
//     starts writes it.
//   - P2: regions hold the same logical rows: each aggregate's state, merged across a row's
//     SPLIT items, byte for byte. A view that keys rows by a DEDUP BY stream's ingest_time
//     or origin_region is compared with its rows merged across those keys, and rows that a
//     divergent duplicate reached are left out.
//   - P3: a view is broken exactly when a record fails its evaluation; a live view, by
//     the time a Handle that evaluated the record returns without error. A version split by
//     time may break by its split instead, which P10 checks.
//   - P4: a row item that a single-item write stores holds high-water marks only for the
//     units of its view's stream that are still delivering or finished within W plus two
//     lifetimes (and the lateness of a late write), and the backfill's. A transaction's row
//     writes are P11c's.
//   - P5: row versions rise by one; marker owners, progress and fences never go back.
//   - P6: the run reaches quiescence within its budget, every backfilled version live or
//     broken.
//   - P7: no request comes from an invocation whose Handle returned, and every request of
//     a core carries a context that ends by its invocation's deadline: in Lambda, only the
//     context stops a request.
//   - P8: no core panics.
//   - P9: a batch with neither INSERT nor MODIFY, such as an empty one or one of TTL deletes,
//     makes no request.
//   - P10: once the projector has caught up, a version split by time is exact or broken by
//     its split, and only by an INSERT at or before its split time that a batch started at
//     or after its late bound, or by a MODIFY whose images straddle the split time; a version
//     whose region holds such a MODIFY is broken.
//   - P11: an event whose _sync names a view version V lands only in a transaction of its
//     writer's region (a) that writes, of the SYNC versions, the rows of the versions _sync
//     names, every row of V that the event reaches by the oracle, or alone when it reaches
//     no row of them; (b) V was live in that region's meta at some moment from 30 s before
//     the append's call to the landing; and (c) the transaction's row writes keep _hwm, the
//     backfill's mark included, as stored.
//   - P12: a receipt lands in a region only for an INSERT of its change stream that an append
//     there asked one for, and only once the progress of the record's unit in the region's
//     meta covers the record: the projector handled it, and no fence can make it the
//     projector's again.
//   - P13: at quiescence each region holds the receipt of every INSERT that an append there
//     asked one for, and no other region holds it.
//   - P14: a region's archive of a stream backs a job split by fences only while complete
//     (ADR 0017): (a) an origin lands only while the region's functions archive the stream;
//     when the functions stop or start archiving it, and when a unit's progress passes a
//     record that entered the stream a day before, the stream item already holds a hole
//     after its origin; (b) a job moves from stage to apply only after its step read the
//     stream item while, by the model, an origin had landed under a registered catalog that
//     archives every event of the stream, and neither change happened since; (c) a job that
//     ends broken leaves its version broken, and one that fails names an archive rule and
//     leaves its version short of live; (d) a job marks its error prefix read only when no
//     object of a commit that failed had landed when its step last listed the prefix.
//   - P15: a view query that waited for an append's event and returned rows read a version
//     that was live in the region at some moment after the event's receipt first landed, and
//     whose rows that the event reaches by the oracle hold it, by their high-water marks or by
//     the unit's progress.
//
// A run also fails when one invocation has two identical requests in flight (D3): the
// seed can't order them, so the run wouldn't replay. It fails, too, when an append returns
// ErrDuplicate though its write found no other call's event, or success though it did, or
// another error than ErrEvaluation for a poison event ("events:"), and when a Handle reports
// more divergent MODIFY records than its batch holds ("Stats:").
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
	// Retire names a version, live from the start and additive, that a scenario with
	// Builds.Retire retires.
	Retire string
	// Variants are catalogs that writers append with instead of Catalog, as Scenario.Variant
	// says: the same versions in other modes, such as with every view ASYNC.
	Variants []*engine.Catalog
	// Params holds, by name, the value of each named query's parameter that reads every row
	// of the query's view. Readers query with them.
	Params map[string]expr.Value
}

// System is the code under test with its reference semantics.
type System interface {
	Config() Config
	// Event returns the n-th event to append. A poison event fails the evaluation of a view.
	Event(r *rand.Rand, n int, poison bool) Event
	// NewCore returns a new container's core of region, running cat, or the system's catalog
	// when cat is nil; b and a carry the region's faults.
	NewCore(region string, b store.Backend, a Archiver, cat *projector.Compiled) Core
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
	// Poison is set by the run for a poison event; a SYNC append of it may fail with
	// ErrEvaluation.
	Poison bool
}

// Scenario configures one kind of run.
type Scenario struct {
	Name     string
	Regions  int
	Events   int
	Writers  int
	WriteGap time.Duration // the most time between a writer's appends
	// WriteStart is the latest the writers start appending, all at one time.
	WriteStart time.Duration
	Poison     float64 // the share of poison events
	// Duplicate is the share of a DEDUP BY stream's events that a writer appends in a
	// second region too, as a client that fails over does.
	Duplicate float64
	// Variant is the share of appends that a writer sends with one of Config.Variants.
	Variant float64
	// Reappend is the share of a DEDUP BY stream's events that a writer appends again once
	// the run expired their items in every region, as TTL deletes do.
	Reappend float64
	// Waitable is the share of appends that ask for a receipt. A reader waits for each such
	// append that stored its event, and appends again ask as the first did.
	Waitable  float64
	Faults    Faults
	Topology  Topology
	Isolation Isolation
	Sweep     Sweep
	ESM       ESM
	Archive   Archive
	Backfill  Backfill
	Builds    Builds
	TimeSplit TimeSplit
	History   History
	// Budget is the fake time a seed has to reach quiescence (P6).
	Budget time.Duration
	Seeds  struct{ Smoke, Full int }
}

// Faults are the per-request fault rates and the response times of the store and the
// archive. A transaction gets the faults of a conditional write but Late and
// AppliedConflict, and InProgress of its own.
type Faults struct {
	Transient float64 // fails without effect
	Ambiguous float64 // a write applies, but its caller sees a timeout
	// Retried: a conditional write applies, then fails its condition on the SDK's retry; a
	// transaction's resend without its token fails every condition over its own items.
	Retried float64
	Crash   float64 // the invocation dies
	Spike   float64 // the response takes up to SpikeMax longer
	Late    float64 // a core's write lands up to LateMax after its deadline; the core dies
	// Conflict: a store write fails without effect with store.ErrConflict, as one that meets
	// a transaction.
	Conflict float64
	// InProgress: a transaction holds its items, so other writes to them conflict, and
	// commits up to HoldMax later; its caller gets the commit's outcome.
	InProgress float64
	// AppliedThrottled: a store write applies, then its caller sees store.ErrThrottled, as
	// when the SDK's retries after a lost response were throttled.
	AppliedThrottled float64
	// AppliedConflict: a single-item write applies, then its caller sees store.ErrConflict,
	// as when the SDK's retry after a lost response met a transaction.
	AppliedConflict float64
	Latency         time.Duration
	SpikeMax        time.Duration
	LateMax         time.Duration
	HoldMax         time.Duration
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
	// CloseOnNext closes a full unit only when the next record for its keys arrives, which
	// goes to its child, as a shard that rolls over by time can close long after its last
	// record was delivered.
	CloseOnNext bool
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

// Archive models Firehose delivering into Iceberg, and Athena reading it.
type Archive struct {
	MinDelay, MaxDelay time.Duration // from an acknowledged put to delivery
	Dup                float64       // a record is delivered twice
	Error              float64       // a record goes to the error prefix instead
	Lie                bool          // the watermark reports complete too early
	// Commit is the chance that the records of a put fail to commit: no table gets them, and an
	// error object names their data files instead. Foreign is the chance that an object of the
	// error prefix also holds a line that Dynaflux didn't write.
	Commit, Foreign float64
	// Freshness, when set, makes the watermark CloudWatch's: Firehose samples DataFreshness that
	// often while it holds a record, and CloudWatch stamps the samples by a clock up to Skew off.
	// Otherwise the watermark knows when every record arrived.
	Freshness, Skew time.Duration
	// RecordBytes is the Parquet bytes each record stands for in the table's partitions, which
	// size the slices of an export; 0 for 1.
	RecordBytes int64
	// Timeout is the chance that an export of a slice two hours or longer times out, and Fail
	// that the first try of a slice's export fails; either leaves some of its files.
	Timeout, Fail float64
}

// Backfill configures the backfills of the versions that [Config] names: one job per version
// and region, driven as the state machine drives it (lifecycle.md §5.4).
type Backfill struct {
	Within time.Duration // each job starts at most this long after the run; 0 for no backfill
	Chunk  int           // the most records in a chunk
	// Together starts a region's jobs at one time, as dynaflux backfill -all does.
	Together bool
	// Step is a step's time, from its start to its deadline: more than the minute a step
	// keeps in reserve.
	Step time.Duration
	// Crash is the chance that a step dies at a random request, after which its job is claimed
	// for a new attempt, as a resumed backfill does.
	Crash float64
	// Twin is the chance that a step gets a duplicate worker, which steps the same attempt for
	// a while with a deadline of its own.
	Twin float64
	// Lose is the chance that a job loses its staged runs once it applies them.
	Lose float64
	// Table makes every job read the region's stream table: an additive version split by
	// time, an idempotent one not split. Otherwise jobs read the archive, split at fences.
	Table bool
	// Rate is the records a second each job stages and applies; 0 for no limit.
	Rate int
}

// TimeSplit shapes what the time split's bounds rest on (lifecycle.md §5.1): writer clocks,
// and when replicated writes land.
type TimeSplit struct {
	// Skew is the most a writer's clock is off, either way: the gated Put shifts the
	// ingest_time of each of its appends by its offset.
	Skew time.Duration
	// Landing is the landing bound L. With Spread, a replicated write lands at a time drawn up
	// to L after its ingest_time; with chance Late, it lands up to LateMax after L instead.
	Landing time.Duration
	Spread  bool
	Late    float64
	LateMax time.Duration
	// Failover makes a writer append an event in a second region from another client, whose
	// clock is up to Skew off too, so that the two ingest_times can fall on both sides of a
	// split time.
	Failover bool
}

// Builds models deploys of the projector's code (lifecycle.md §4.1). A region's new
// containers run one build at a time; an invocation keeps the build it started with, and a
// backfill worker always runs the full one. The full build is the system's catalog; the old
// one lacks the versions Config.Backfill names; the retiring one lacks Config.Retire and
// lists it as retired.
type Builds struct {
	// Switch, when set, runs the old build in each region until a time up to Switch, then the
	// full one. Backfills start regardless, as if G1 were skipped.
	Switch time.Duration
	// Rollback, when set, returns each region to the old build up to Rollback after its
	// backfills ended, for up to Rollback, then to the full build.
	Rollback time.Duration
	// Flap, when set, returns each region to the old build at a time up to Flap, for up to
	// Flap, then to the full build: older code meets the versions its backfills activate.
	Flap time.Duration
	// Retire, when set, moves each region to the retiring build at a time up to Retire, then
	// back to the full build, an older one now, up to Retire later: for up to Retire, or for
	// good. Most regions also get a tombstone of Config.Retire at a time up to Retire, as
	// dynaflux retire or a deploy writes it; in the others only the retiring build writes it.
	Retire time.Duration
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
// REMOVE records. Backfills counts the versions a backfill made live, Fresh those the
// fresh-stream rule made live, Chunks the chunks applied, and Unlisted the units created after
// a backfill listed their stream's units; Resumed counts the attempts claimed after a step
// died, Twins the duplicate workers, and Lost the times staged runs were lost. Idle counts the
// times a unit went quiet. Pruned counts the
// high-water marks that row writes dropped, and PrunedOpen those of units still delivering.
// Deploys counts the builds regions moved to, Guarded the batches that failed under G3, and
// Tombstones the regions that retired Config.Retire. Skewed counts the appends a writer's
// clock shifted, LateLandings the replicated writes that landed past TimeSplit.Landing,
// TimeSplits the versions split by time that ended live, and SplitBreaks those that its split
// broke. Transactions counts the transactions sent, Conflicts the writes that failed with
// store.ErrConflict without effect, injected or held off by a transaction in progress,
// InProgress the transactions that held their items, and AppliedThrottled and
// AppliedConflicts the writes that applied before their caller saw an error. Synced counts
// the events that landed in a transaction with their SYNC rows, SyncedAlone those whose
// _sync named versions that they reach no row of, put alone, Variants the appends sent
// with a variant catalog, Refused the poison events a SYNC append refused, and Reappends
// the events appended again after they expired. Receipts counts the receipts that landed,
// and of the readers' waits, Waits those that returned rows, WaitTimeouts those that timed
// out, and WaitsNotReady those whose version wasn't live. Toggles counts the deploys that
// stopped or started archiving a stream, Origins the origins the CLI recorded, Stalls the
// regions whose mappings stalled, Lags the progress writes that passed a record a day old,
// and Incomplete the jobs that failed on an archive that wasn't complete. Exports counts
// the exports Athena ran, Halved those that timed out, FailedTries those that failed, and
// CommitFailures the puts whose records failed to commit.
type Stats struct {
	Requests, Transient, Ambiguous, Retried, Spikes, Crashes, Killed, Late, Slow int64
	Invocations, Timeouts, Stale, Dups, Empty, Containers, Units, Poison         int64
	Duplicates, Modifies, Divergent, Reported, Cuts, Held, Removes               int64
	Backfills, Chunks, Unlisted, Idle, Pruned, PrunedOpen                        int64
	Fresh, Resumed, Twins, Lost                                                  int64
	Deploys, Guarded, Tombstones                                                 int64
	Skewed, LateLandings, TimeSplits, SplitBreaks                                int64
	Transactions, Conflicts, InProgress, AppliedThrottled, AppliedConflicts      int64
	Synced, SyncedAlone, Variants, Refused, Reappends                            int64
	Receipts, Waits, WaitTimeouts, WaitsNotReady                                 int64
	Toggles, Origins, Stalls, Lags, Incomplete                                   int64
	Exports, Halved, FailedTries, CommitFailures                                 int64
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

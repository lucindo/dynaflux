// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

const writeRetry = 50 * time.Millisecond // a writer's wait before it retries an append

// run is one seed: every region, actor and record of it.
type run struct {
	sc      Scenario
	sys     System
	cfg     Config
	meta    string        // the meta table: P3 reads its view states, P5 checks its unit items
	window  time.Duration // W in P4
	seed    uint64
	started time.Time // when the stream tables were created
	// catalogData is the catalog every region registers as deployed, and catalogHash its hash.
	catalogData []byte
	catalogHash string
	sched       *sched
	faults      *rand.Rand // drawn only right after a gate
	topo        *rand.Rand // drawn only right after a gate
	arch        *rand.Rand // drawn only right after a gate
	regions     []*region
	builds      []*projector.Compiled // by build; nil for the system's catalog
	events      []Event
	counts      map[string]int    // events by stream
	dedup       map[string]bool   // the streams with DEDUP BY
	fenced      map[string]bool   // the view versions that need the fenced handoff
	sources     map[string]string // the stream table of each view version's table, for P4
	views       map[string]string // the view version of each view table, for P2
	// writeStart is when the writers start appending, after the run's start.
	writeStart time.Duration
	stop       chan struct{} // closed at quiescence, or when the budget runs out
	quiet      chan struct{} // closed when nothing is left to deliver

	mu         sync.Mutex
	stats      Stats
	violations []string
	// pending counts writers, shipments, lane records, deletes and backfills not yet done.
	pending   int
	applied   map[tableKey]int  // the regions whose own append wrote each stream item
	expired   map[tableKey]bool // the stream items a region deleted, for P0
	divergent map[tableKey]bool // the events appended with two payloads, for P2
	maxHWM    int
}

type tableKey struct {
	table string
	key   store.Key
}

func newRun(ctx context.Context, sc Scenario, sys System, seed uint64) (*run, error) {
	cfg := sys.Config()
	if cfg.Catalog == nil || cfg.Naming.Store != cfg.Catalog.Catalog().Store {
		return nil, fmt.Errorf("the system needs a catalog, and tables named for its store: got %+v", cfg.Naming)
	}
	data, merr := catalog.Marshal(cfg.Catalog.Catalog())
	if merr != nil {
		return nil, merr
	}
	r := &run{
		sc:          sc,
		sys:         sys,
		cfg:         cfg,
		meta:        cfg.Naming.MetaTable(),
		window:      time.Duration(cfg.Catalog.Catalog().PruneWindowSeconds) * time.Second,
		seed:        seed,
		started:     time.Now(),
		catalogData: data,
		catalogHash: catalog.Hash(data),
		faults:      newRand(seed, streamFaults),
		topo:        newRand(seed, streamTopology),
		arch:        newRand(seed, streamArchive),
		counts:      map[string]int{},
		dedup:       map[string]bool{},
		fenced:      map[string]bool{},
		sources:     map[string]string{},
		views:       map[string]string{},
		stop:        make(chan struct{}),
		quiet:       make(chan struct{}),
		applied:     map[tableKey]int{},
		expired:     map[tableKey]bool{},
		divergent:   map[tableKey]bool{},
	}
	for _, st := range cfg.Catalog.Catalog().Streams {
		r.dedup[st.Name] = len(st.DedupBy) > 0
	}
	for _, v := range cfg.Catalog.Catalog().Views {
		for i := range v.Versions {
			vv := &v.Versions[i]
			id, table := catalog.VersionID(v.Name, vv.Number), cfg.Naming.ViewTable(v.Name, vv.Number)
			r.fenced[id] = vv.FencedHandoff()
			r.sources[table] = cfg.Naming.StreamTable(vv.Stream)
			r.views[table] = id
		}
	}
	r.sched = newSched(seed, r.violatef)
	r.writeStart = between(r.actorRNG("writers"), 0, sc.WriteStart)
	if err := r.compileBuilds(); err != nil {
		return nil, err
	}
	if err := r.generate(); err != nil {
		return nil, err
	}
	for i := range sc.Regions {
		rg, err := newRegion(ctx, r, i)
		if err != nil {
			return nil, err
		}
		r.regions = append(r.regions, rg)
	}
	for _, from := range r.regions {
		for _, to := range r.regions {
			if from != to {
				from.links = append(from.links, newLink(from, to))
			}
		}
	}
	r.isolate()
	return r, nil
}

// The links a cut pauses.
const (
	cutBoth = iota
	cutOut
	cutIn
	cutWays
)

// isolate draws the cuts: each region, with chance Share, loses its links both ways, only
// its outgoing ones, or only its incoming ones, for a while.
func (r *run) isolate() {
	iso := r.sc.Isolation
	for _, rg := range r.regions {
		if !chance(r.topo, iso.Share) {
			continue
		}
		r.stats.Cuts++
		from := time.Now().Add(between(r.topo, 0, iso.Within))
		c := cut{from: from, to: from.Add(between(r.topo, 1, iso.Max))}
		way := r.topo.IntN(cutWays)
		for _, other := range r.regions {
			for _, l := range other.links {
				if l.to == rg && way != cutOut || l.from == rg && way != cutIn {
					l.cuts = append(l.cuts, c)
				}
			}
		}
	}
}

func (r *run) generate() error {
	rng := newRand(r.seed, streamEvents)
	for n := range r.sc.Events {
		poison := chance(rng, r.sc.Poison)
		if poison {
			r.count(&r.stats.Poison)
		}
		ev := r.sys.Event(rng, n, poison)
		if _, ok := r.cfg.Catalog.Catalog().Stream(ev.Stream); !ok {
			return fmt.Errorf("event %d is on stream %q, which the catalog doesn't have", n, ev.Stream)
		}
		r.events = append(r.events, ev)
		r.counts[ev.Stream]++
	}
	return nil
}

// start opens every region's units and starts the writers, the backfills, the deploys and
// the actor that stops the run.
func (r *run) start() {
	r.pending = r.sc.Writers
	for _, rg := range r.regions {
		rg.open()
		for _, l := range rg.links {
			r.sched.goActor(l.run)
		}
		if r.sc.Sweep != (Sweep{}) {
			r.sched.goActor(rg.sweeper.run)
		}
		for _, d := range rg.drivers() {
			r.pending++
			rg.backfilling++
			r.sched.goActor(d.run)
		}
		if r.sc.Builds != (Builds{}) {
			r.pending++
			r.sched.goActor(rg.deploy)
		}
		if r.sc.Builds.Retire > 0 {
			r.pending++
			r.sched.goActor(rg.retire)
		}
	}
	for w := range r.sc.Writers {
		r.sched.goActor(func() { r.write(w) })
	}
	r.sched.goActor(r.coordinate)
}

// write appends every Writers-th event, each to a random home region, retrying until
// the append is acknowledged. A retry gets a new event_id, so an append that applied
// before its caller saw a timeout stays as an event of its own; on a DEDUP BY stream, the
// retry finds the event stored. An event of a DEDUP BY stream may be appended in a second
// region too, with its divergent payload if it has one, and with TimeSplit.Failover from
// another client. The writer's clock is up to TimeSplit.Skew off.
func (r *run) write(w int) {
	id := "w" + strconv.Itoa(w)
	rng, clocks := r.actorRNG(id), r.actorRNG(id+" clock")
	ctx := withInvocation(context.Background(), &invocation{id: id, stallAt: -1, skew: r.clockSkew(clocks)})
	// Entropy of its own keeps the writer's event_ids apart from the others'.
	entropy := rand.NewChaCha8(sha256.Sum256(fmt.Appendf(nil, "%d %s", r.seed, id)))
	engines := make([]*engine.Engine, len(r.regions))
	for i, rg := range r.regions {
		engines[i] = engine.New(rg.store, engine.Options{Region: rg.name, Prefix: r.cfg.Naming.Prefix, Entropy: entropy})
	}
	time.Sleep(r.writeStart)
	for n := w; n < len(r.events); n += r.sc.Writers {
		time.Sleep(between(rng, 0, r.sc.WriteGap))
		ev, home := r.events[n], rng.IntN(len(engines))
		k := r.append(ctx, engines[home], ev.Stream, ev.Vals)
		if r.dedup[ev.Stream] && len(engines) > 1 && chance(rng, r.sc.Duplicate) {
			r.count(&r.stats.Duplicates)
			vals := ev.Vals
			if ev.Divergent != nil {
				vals = ev.Divergent
				r.mu.Lock()
				r.divergent[tableKey{r.cfg.Naming.StreamTable(ev.Stream), k}] = true
				r.mu.Unlock()
			}
			other, dup := engines[(home+1+rng.IntN(len(engines)-1))%len(engines)], ctx
			if r.sc.TimeSplit.Failover {
				dup = withInvocation(context.Background(), &invocation{id: id + " failover", stallAt: -1, skew: r.clockSkew(clocks)})
			}
			r.append(dup, other, ev.Stream, vals)
		}
	}
	r.sched.gate(id, "done")
	r.settle(1)
}

// clockSkew draws how far a client's clock is off, up to TimeSplit.Skew either way.
func (r *run) clockSkew(rng *rand.Rand) time.Duration {
	if s := r.sc.TimeSplit.Skew; s > 0 {
		return between(rng, -s, s)
	}
	return 0
}

// append appends an event until it is acknowledged, and returns its key. On a DEDUP BY
// stream, a duplicate is acknowledged: the retry of an attempt that applied, or the
// event appended in another region. An append is a duplicate exactly when its write found
// another call's event: the SDK's retry of the call's own write succeeds.
func (r *run) append(ctx context.Context, e *engine.Engine, stream string, vals []expr.Value) store.Key {
	inv := invocationOf(ctx)
	for !r.stopped() {
		inv.taken = false
		res, err := e.Append(ctx, r.cfg.Catalog, stream, [][]expr.Value{vals}, engine.AppendMode{})
		if err == nil {
			err = res[0].Err
		}
		dup := errors.Is(err, engine.ErrDuplicate)
		switch {
		case err == nil && inv.taken:
			r.violatef("events: append to %s succeeded, but its write found another call's event", stream)
		case dup && !inv.taken:
			r.violatef("events: append to %s: %v, but its write found no other call's event", stream, err)
		}
		switch {
		case err == nil, r.dedup[stream] && dup:
			return res[0].Key
		case !errors.Is(err, errTransient) && !errors.Is(err, errAmbiguous):
			r.violatef("events: append to %s: %v", stream, err)
			return store.Key{}
		}
		time.Sleep(writeRetry)
	}
	return store.Key{}
}

// coordinate stops the run at quiescence, or when the budget runs out (P6).
func (r *run) coordinate() {
	budget := time.NewTimer(r.sc.Budget)
	defer budget.Stop()
	select {
	case <-r.quiet:
	case <-budget.C:
	}
	r.sched.gate("coordinator", "stop")
	if !isClosed(r.quiet) {
		r.violatef("P6: no quiescence within %v: %d deliveries, writers or shipments pending", r.sc.Budget, r.pending)
	}
	close(r.stop)
}

// settle marks n pending things done. Only gated code calls it.
func (r *run) settle(n int) {
	if r.pending -= n; r.pending == 0 && !isClosed(r.quiet) {
		close(r.quiet)
	}
}

func (r *run) stopped() bool { return isClosed(r.stop) }

// isDivergent reports whether an event was appended with two payloads.
func (r *run) isDivergent(k tableKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.divergent[k]
}

func (r *run) count(field *int64) {
	r.mu.Lock()
	*field++
	r.mu.Unlock()
}

func (r *run) violatef(format string, args ...any) {
	r.mu.Lock()
	r.violations = append(r.violations, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}

// draw reports whether an event of probability p happens, from the shared fault stream.
func (r *run) draw(p float64) bool { return chance(r.faults, p) }

func chance(rng *rand.Rand, p float64) bool { return p > 0 && rng.Float64() < p }

// between draws a duration in [lo, hi].
func between(rng *rand.Rand, lo, hi time.Duration) time.Duration {
	return lo + time.Duration(rng.Int64N(int64(hi-lo)+1))
}

func (r *run) actorRNG(id string) *rand.Rand { return newRand(r.seed, fnv(fnvOffset, id)) }

func (r *run) outcome(ctx context.Context, took time.Duration) Outcome {
	r.check(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	slices.Sort(r.violations)
	return Outcome{
		Steps:      r.sched.steps,
		Trace:      r.sched.trace,
		Time:       took,
		Stats:      r.stats,
		Violations: r.violations,
		MaxHWM:     r.maxHWM,
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

const (
	writeGap   = 200 * time.Millisecond // most time between a writer's appends
	writeRetry = 50 * time.Millisecond  // a writer's wait before it retries an append
)

// run is one seed: every region, actor and record of it.
type run struct {
	sc      Scenario
	sys     System
	cfg     Config
	meta    string        // the meta table: P3 reads its view states, P5 checks its unit items
	window  time.Duration // W in P4
	seed    uint64
	sched   *sched
	faults  *rand.Rand // drawn only right after a gate
	topo    *rand.Rand // drawn only right after a gate
	arch    *rand.Rand // drawn only right after a gate
	regions []*region
	events  []event
	counts  map[string]int // events by stream
	stop    chan struct{}  // closed at quiescence, or when the budget runs out
	quiet   chan struct{}  // closed when nothing is left to deliver

	mu         sync.Mutex
	stats      Stats
	violations []string
	pending    int // writers, shipments and lane records not yet acknowledged
	applied    map[tableKey]bool
}

type event struct {
	stream string
	vals   []expr.Value
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
	r := &run{
		sc:      sc,
		sys:     sys,
		cfg:     cfg,
		meta:    cfg.Naming.MetaTable(),
		window:  cmp.Or(sc.Window, time.Duration(cfg.Catalog.Catalog().PruneWindowSeconds)*time.Second),
		seed:    seed,
		faults:  newRand(seed, streamFaults),
		topo:    newRand(seed, streamTopology),
		arch:    newRand(seed, streamArchive),
		counts:  map[string]int{},
		stop:    make(chan struct{}),
		quiet:   make(chan struct{}),
		applied: map[tableKey]bool{},
	}
	if r.window <= 0 {
		return nil, fmt.Errorf("the prune window is %v", r.window)
	}
	r.sched = newSched(seed, r.violatef)
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
	return r, nil
}

func (r *run) generate() error {
	rng := newRand(r.seed, streamEvents)
	for n := range r.sc.Events {
		poison := chance(rng, r.sc.Poison)
		if poison {
			r.count(&r.stats.Poison)
		}
		name, vals := r.sys.Event(rng, n, poison)
		if _, ok := r.cfg.Catalog.Catalog().Stream(name); !ok {
			return fmt.Errorf("event %d is on stream %q, which the catalog doesn't have", n, name)
		}
		r.events = append(r.events, event{name, vals})
		r.counts[name]++
	}
	return nil
}

// start opens every region's units and starts the writers and the coordinator.
func (r *run) start() {
	for _, rg := range r.regions {
		rg.open()
		for _, l := range rg.links {
			r.sched.goActor(l.run)
		}
	}
	r.pending = r.sc.Writers
	for w := range r.sc.Writers {
		r.sched.goActor(func() { r.write(w) })
	}
	r.sched.goActor(r.coordinate)
}

// write appends every Writers-th event, each to a random home region, retrying until
// the append is acknowledged. A retry gets a new event_id, so an append that applied
// before its caller saw a timeout stays as an event of its own.
func (r *run) write(w int) {
	id := "w" + strconv.Itoa(w)
	rng := r.actorRNG(id)
	ctx := withInvocation(context.Background(), &invocation{id: id, stallAt: -1})
	// Entropy of its own keeps the writer's event_ids apart from the others'.
	entropy := rand.NewChaCha8(sha256.Sum256(fmt.Appendf(nil, "%d %s", r.seed, id)))
	engines := make([]*engine.Engine, len(r.regions))
	for i, rg := range r.regions {
		engines[i] = engine.New(rg.store, engine.Options{Region: rg.name, Prefix: r.cfg.Naming.Prefix, Entropy: entropy})
	}
	for n := w; n < len(r.events); n += r.sc.Writers {
		time.Sleep(time.Duration(rng.Int64N(int64(writeGap))))
		r.append(ctx, engines[rng.IntN(len(engines))], r.events[n])
	}
	r.sched.gate(id, "done")
	r.settle(1)
}

func (r *run) append(ctx context.Context, e *engine.Engine, ev event) {
	for !r.stopped() {
		res, err := e.Append(ctx, r.cfg.Catalog, ev.stream, [][]expr.Value{ev.vals}, engine.AppendMode{})
		if err == nil {
			err = res[0].Err
		}
		switch {
		case err == nil:
			return
		case !errors.Is(err, errTransient) && !errors.Is(err, errAmbiguous):
			r.violatef("events: append to %s: %v", ev.stream, err)
			return
		}
		time.Sleep(writeRetry)
	}
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
	}
}

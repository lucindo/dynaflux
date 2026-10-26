// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"errors"
	"iter"
	"maps"
	"math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// The unit items of the meta table (encoding.md §8.1), whose history the store checks (P5).
const (
	unitPrefix   = "unit#"
	attrProgress = "progress"
	attrFences   = "fences"
)

// Injected errors. Neither is a condition failure, so the code under test treats them as
// transient.
var (
	errTransient = errors.New("simulation: injected throttling, not applied")
	errAmbiguous = errors.New("simulation: injected timeout after the write applied")
)

// invocation is one caller at the gates: a core invocation, or a harness actor such as a
// writer, which has no deadline and never crashes.
type invocation struct {
	id       string
	deadline time.Time     // zero for harness actors
	empty    bool          // a core invocation without records
	stallAt  int           // the request whose response stalls; -1 for none
	stall    time.Duration // how long before the deadline the stalled response returns
	// taken is set when a writer's write of an event found another call's event at its key.
	// Only the writer touches it.
	taken bool

	mu       sync.Mutex
	requests int
	dead     bool
	returned bool
}

type invocationKey struct{}

func withInvocation(ctx context.Context, inv *invocation) context.Context {
	return context.WithValue(ctx, invocationKey{}, inv)
}

// invocationOf panics on a context without an invocation: the code under test must pass
// its invocation's context to every request.
func invocationOf(ctx context.Context) *invocation {
	inv, ok := ctx.Value(invocationKey{}).(*invocation)
	if !ok {
		panic("simulation: a request without its invocation's context")
	}
	return inv
}

// admit counts a request the scheduler released. It returns the request's index, whether
// the invocation may still send it, and whether its Handle had returned. Nothing is sent
// at or after the deadline: Lambda has stopped the invocation by then.
func (inv *invocation) admit(now time.Time) (int, bool, bool) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.dead || !inv.deadline.IsZero() && !now.Before(inv.deadline) {
		inv.dead = true
		return 0, false, inv.returned
	}
	inv.requests++
	return inv.requests - 1, true, inv.returned
}

func (inv *invocation) isDead() bool {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.dead
}

func (inv *invocation) kill() {
	inv.mu.Lock()
	inv.dead = true
	inv.mu.Unlock()
}

func (inv *invocation) finish() {
	inv.mu.Lock()
	inv.returned = true
	inv.mu.Unlock()
}

// region is one replica of the store with its change streams, mappings and containers.
type region struct {
	run      *run
	name     string
	mem      *mem.Mem
	store    store.Backend // mem behind the gates and faults: what the code under test gets
	archiver Archiver
	archive  *archive
	sweeper  *sweeper
	streams  map[string]*stream // by table
	order    []*stream          // in catalog order
	mappings []*mapping
	links    []*link
	pool     []Core // idle warm containers
	unitIDs  map[string]bool
	units    map[string]*unit // by unit key, as high-water marks name them
	// written holds the keys of the items written to each view table, for P2.
	written map[string]map[store.Key]bool
}

func newRegion(ctx context.Context, r *run, i int) (*region, error) {
	rg := &region{
		run:     r,
		name:    "r" + strconv.Itoa(i),
		mem:     mem.New(),
		archive: &archive{cfg: r.sc.Archive},
		streams: map[string]*stream{},
		unitIDs: map[string]bool{},
		units:   map[string]*unit{},
		written: map[string]map[store.Key]bool{},
	}
	rg.store = gated{rg}
	rg.archiver = archiver{rg}
	rg.sweeper = &sweeper{rg: rg, id: rg.name + " sweeper", queue: newDueQueue[sweep]()}
	rg.mappings = []*mapping{{rg: rg, name: "m0", start: time.Now()}}
	if r.sc.ESM.Replay > 0 {
		start := time.Now().Add(between(r.topo, 0, r.sc.ESM.Replay))
		rg.mappings = append(rg.mappings, &mapping{rg: rg, name: "m1", start: start})
	}
	cat := r.cfg.Catalog.Catalog()
	for _, s := range cat.Streams {
		st := newStream(rg, s.Name, r.cfg.Naming.StreamTable(s.Name))
		rg.streams[st.table] = st
		rg.order = append(rg.order, st)
	}
	if err := rg.setLive(ctx); err != nil {
		return nil, err
	}
	return rg, nil
}

// setLive makes every view version live but those a backfill fills.
func (rg *region) setLive(ctx context.Context) error {
	r := rg.run
	m := meta.New(rg.mem, r.meta)
	for _, v := range r.cfg.Catalog.Catalog().Views {
		for _, vv := range v.Versions {
			id := catalog.VersionID(v.Name, vv.Number)
			if r.backfills(id) {
				continue
			}
			for _, s := range []meta.State{meta.Activating, meta.Live} {
				if err := m.SetViewState(ctx, id, s); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// backfills reports whether the run backfills version.
func (r *run) backfills(version string) bool {
	return r.sc.Backfill.Within > 0 && slices.Contains(r.cfg.Backfill, version)
}

// open creates every stream's root units, which starts their pollers.
func (rg *region) open() {
	for _, s := range rg.order {
		for root := range rg.run.sc.Topology.Partitions {
			s.newUnit(uint64(root), 0, 0, nil)
		}
	}
}

type kind uint8

const (
	kindRead kind = iota
	kindWrite
	kindCondWrite
	kindArchive
)

type fault uint8

const (
	faultNone fault = iota
	faultTransient
	faultAmbiguous
	faultRetried
	faultLate
)

// enter is a request's arrival: the gate, the kill check, then what happens to it. It
// returns the response's latency and the request's fault.
func (rg *region) enter(ctx context.Context, k kind, op string) (time.Duration, fault) {
	r := rg.run
	inv := invocationOf(ctx)
	over := !r.sched.gate(inv.id, op)
	n, ok, returned := inv.admit(time.Now())
	if returned {
		// Lambda would have frozen the sandbox and sent this later, whatever the deadline.
		r.violatef("P7: %s: %s after its Handle returned", inv.id, op)
	}
	if over {
		runtime.Goexit()
	}
	if !ok {
		r.count(&r.stats.Killed)
		runtime.Goexit()
	}
	r.count(&r.stats.Requests)
	if inv.empty {
		r.violatef("P9: %s: %s for an empty batch", inv.id, op)
	}
	core := !inv.deadline.IsZero()
	if d, has := ctx.Deadline(); core && (!has || d.After(inv.deadline)) {
		r.violatef("P7: %s: %s with a context that outlives the invocation's deadline", inv.id, op)
	}
	if core && r.draw(r.sc.Faults.Crash) {
		r.count(&r.stats.Crashes)
		inv.kill()
		runtime.Goexit()
	}
	lat := r.latency()
	if core && n == inv.stallAt {
		r.count(&r.stats.Slow)
		lat = max(lat, time.Until(inv.deadline.Add(-inv.stall)))
	}
	return lat, r.fault(k, core)
}

func (r *run) latency() time.Duration {
	f := r.sc.Faults
	var lat time.Duration
	if f.Latency > 0 {
		lat = between(r.faults, 1, f.Latency)
	}
	if r.draw(f.Spike) {
		r.count(&r.stats.Spikes)
		lat += between(r.faults, 1, f.SpikeMax)
	}
	return lat
}

func (r *run) fault(k kind, core bool) fault {
	f := r.sc.Faults
	switch {
	case r.draw(f.Transient):
		r.count(&r.stats.Transient)
		return faultTransient
	case k == kindRead:
		return faultNone
	case core && k != kindArchive && r.draw(f.Late):
		r.count(&r.stats.Late)
		return faultLate
	case r.draw(f.Ambiguous):
		r.count(&r.stats.Ambiguous)
		return faultAmbiguous
	case k == kindCondWrite && r.draw(f.Retried):
		r.count(&r.stats.Retried)
		return faultRetried
	}
	return faultNone
}

// gated is a region's store as the code under test sees it.
type gated struct{ rg *region }

func (g gated) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	lat, f := g.rg.enter(ctx, kindRead, "get "+table+" "+k.PK+" "+k.SK)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	return g.rg.mem.Get(ctx, table, k, consistent)
}

func (g gated) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	op := []string{"batchget", table}
	for _, k := range ks {
		op = append(op, k.PK, k.SK)
	}
	lat, f := g.rg.enter(ctx, kindRead, strings.Join(op, " "))
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	return g.rg.mem.BatchGet(ctx, table, ks, consistent)
}

// Query sends a request per page, as DynamoDB's paginator does: mem reads a page when
// its request is released, and the caller gets it after the request's latency.
func (g gated) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	return func(yield func(store.Item, error) bool) {
		op := "query " + table + " " + q.PK
		lat, f := g.rg.enter(ctx, kindRead, op)
		if f == faultTransient {
			time.Sleep(lat)
			yield(nil, errTransient)
			return
		}
		p := pager{size: q.PageSize, left: q.Limit, limited: q.Limit > 0}
		for it, err := range g.rg.mem.Query(ctx, table, q) {
			time.Sleep(lat)
			lat = 0
			if !yield(it, err) || err != nil {
				return
			}
			if !p.ends(it) {
				continue
			}
			if lat, f = g.rg.enter(ctx, kindRead, op); f == faultTransient {
				time.Sleep(lat)
				yield(nil, errTransient)
				return
			}
		}
		time.Sleep(lat)
	}
}

// pageBytes is DynamoDB's cap on what one Query request reads.
const pageBytes = 1 << 20

// pager finds a Query's page ends by the rules of [store.Query], which mem follows: a
// page ends at its item limit or at pageBytes, and another follows unless Limit is reached.
type pager struct {
	size, left int // the page's item limit, 0 for none; the items left under Limit
	limited    bool
	n, bytes   int // read of the page
}

// ends counts an item and reports whether it ends its page with another to follow.
func (p *pager) ends(it store.Item) bool {
	p.n++
	p.bytes += store.ItemSize(it)
	limit := p.size
	if p.limited && (limit == 0 || p.left < limit) {
		limit = p.left
	}
	if p.n != limit && p.bytes < pageBytes {
		return false
	}
	p.left -= p.n
	p.n, p.bytes = 0, 0
	p.size *= 2
	return !p.limited || p.left > 0
}

func (g gated) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	k := kindWrite
	if c != (store.Cond{}) {
		k = kindCondWrite
	}
	lat, f := g.rg.enter(ctx, k, "put "+table+" "+keyText(it))
	switch f {
	case faultTransient:
		time.Sleep(lat)
		return errTransient
	case faultLate:
		g.rg.landLate(ctx, table, it, c)
	case faultNone, faultAmbiguous, faultRetried:
	}
	err := g.rg.apply(ctx, table, it, c)
	if g.rg.streams[table] != nil {
		var ce *store.ConditionError
		invocationOf(ctx).taken = errors.As(err, &ce)
	}
	if err == nil && f == faultRetried {
		// The SDK's retry finds the first attempt's write and fails its condition.
		var cur store.Item
		if cur, err = g.rg.mem.Get(ctx, table, keyOf(it), true); err == nil {
			err = &store.ConditionError{Current: cur}
		}
	}
	time.Sleep(lat)
	if err == nil && f == faultAmbiguous {
		err = errAmbiguous
	}
	return err
}

// landLate applies a write up to LateMax after its invocation's deadline. The invocation
// waits for the response until it times out, so landLate doesn't return.
func (rg *region) landLate(ctx context.Context, table string, it store.Item, c store.Cond) {
	r := rg.run
	inv := invocationOf(ctx)
	at := inv.deadline.Add(between(r.faults, 1, r.sc.Faults.LateMax))
	it = cloneItem(it)
	lctx := context.WithoutCancel(ctx)
	r.sched.goActor(func() {
		time.Sleep(time.Until(at))
		r.sched.gate(inv.id+" late", "put "+table+" "+keyText(it))
		_ = rg.apply(lctx, table, it, c) //nolint:errcheck // the invocation died at its deadline: nobody reads the response
	})
	time.Sleep(time.Until(inv.deadline) + time.Nanosecond)
	runtime.Goexit()
}

// apply writes an item, then checks the store's invariants against what it replaced.
// Only gated code calls it.
func (rg *region) apply(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if s := rg.streams[table]; s != nil {
		return s.append(ctx, it, c)
	}
	cur, getErr := rg.mem.Get(ctx, table, keyOf(it), true)
	if err := rg.mem.Put(ctx, table, it, c); err != nil {
		return err
	}
	if getErr == nil {
		rg.checkWrite(table, cur, it)
	}
	switch {
	case rg.run.views[table] != "":
		if rg.written[table] == nil {
			rg.written[table] = map[store.Key]bool{}
		}
		rg.written[table][keyOf(it)] = true
	case table == rg.run.meta:
		rg.sweeper.expire(table, it)
	}
	return nil
}

// checkWrite checks a write that replaced cur with next: the row version rises by
// exactly one, and a marker's owner, a unit's progress and its fences never go back
// (P5); a row's high-water marks stay within the units that can still deliver (P4).
func (rg *region) checkWrite(table string, cur, next store.Item) {
	r := rg.run
	where := rg.name + " " + table + " " + keyText(next)
	if v, ok := next[catalog.AttrRowVersion]; ok {
		want := int64(1)
		if old, found := cur[catalog.AttrRowVersion]; found {
			want = integer(old) + 1
		}
		if got := integer(v); got != want {
			r.violatef("P5: %s: _v %d, want %d", where, got, want)
		}
	}
	if o, ok := cur[catalog.AttrMarkerOwner]; ok && next[catalog.AttrMarkerOwner].Str != o.Str {
		r.violatef("P5: %s: marker owner %q changed to %q", where, o.Str, next[catalog.AttrMarkerOwner].Str)
	}
	if table == r.meta && strings.HasPrefix(keyOf(next).PK, unitPrefix) {
		if p, ok := cur[attrProgress]; ok && next[attrProgress].Str < p.Str {
			r.violatef("P5: %s: progress %s went back to %q", where, p.Str, next[attrProgress].Str)
		}
		fences, stored := cur[attrFences].Map, next[attrFences].Map
		for _, version := range slices.Sorted(maps.Keys(fences)) {
			if stored[version].Str != fences[version].Str {
				r.violatef("P5: %s: fence of %s changed from %s to %q", where, version, fences[version].Str, stored[version].Str)
			}
		}
	}
	if hwm, ok := next[catalog.AttrHighWaterMarks]; ok {
		rg.checkMarks(where, table, cur[catalog.AttrHighWaterMarks].Map, hwm.Map)
	}
}

// checkMarks checks P4 on a write to table, a view's, that replaced the high-water marks old
// with next: no entry of a unit that finished by prunableSince, and at most one per unit
// of the view's stream that may still need its own, plus the backfill's. It counts the
// entries the write dropped.
func (rg *region) checkMarks(where, table string, old, next map[string]store.Value) {
	r := rg.run
	since := rg.prunableSince(time.Now())
	n, bound := len(next), rg.streams[r.sources[table]].liveUnits(since)+1
	if n > bound {
		r.violatef("P4: %s: %d high-water marks, over the bound %d", where, n, bound)
	}
	for name := range next {
		if u := rg.units[name]; u != nil && !u.finished.IsZero() && !u.finished.After(since) {
			r.violatef("P4: %s: the high-water mark of %s, which finished %v ago", where, u.id, time.Since(u.finished))
		}
	}
	var pruned, open int64
	for name := range old {
		if _, kept := next[name]; kept {
			continue
		}
		pruned++
		if u := rg.units[name]; u != nil && u.finished.IsZero() {
			open++
		}
	}
	r.mu.Lock()
	r.stats.Pruned += pruned
	r.stats.PrunedOpen += open
	r.maxHWM = max(r.maxHWM, n)
	r.mu.Unlock()
}

// prunableSince returns the time at or before which a unit must have finished for a row
// write that lands at now to drop its high-water mark. A unit finished at f had its progress stamped by f + timeout at
// the latest, and a write lands at most a timeout after it read progress, plus LateMax when
// writes land late; from f + W + 2·timeout (+ LateMax) on, every write prunes the unit's
// entry.
func (rg *region) prunableSince(now time.Time) time.Time {
	sc := rg.run.sc
	since := now.Add(-rg.run.window - 2*sc.ESM.Timeout)
	if sc.Faults.Late > 0 {
		since = since.Add(-sc.Faults.LateMax)
	}
	return since
}

// liveUnits returns the units of s still delivering, or finished after since.
func (s *stream) liveUnits(since time.Time) int {
	n := 0
	for _, u := range s.units {
		if u.finished.IsZero() || u.finished.After(since) {
			n++
		}
	}
	return n
}

// acquire takes a warm container, or starts a new one.
func (rg *region) acquire(actor string, rng *rand.Rand) Core {
	rg.run.sched.gate(actor, "acquire")
	if len(rg.pool) == 0 {
		rg.run.count(&rg.run.stats.Containers)
		return rg.run.sys.NewCore(rg.store, rg.archiver)
	}
	i := rng.IntN(len(rg.pool))
	c := rg.pool[i]
	rg.pool = slices.Delete(rg.pool, i, i+1)
	return c
}

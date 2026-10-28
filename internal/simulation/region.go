// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
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

// Injected errors. None is a condition failure, so the code under test treats them as
// transient.
var (
	errTransient        = fmt.Errorf("simulation: injected throttling, not applied: %w", store.ErrThrottled)
	errAmbiguous        = errors.New("simulation: injected timeout after the write applied")
	errConflict         = fmt.Errorf("simulation: injected conflict, not applied: %w", store.ErrConflict)
	errHeld             = fmt.Errorf("simulation: a transaction in progress holds the item, not applied: %w", store.ErrConflict)
	errAppliedConflict  = fmt.Errorf("simulation: injected conflict after the write applied: %w", store.ErrConflict)
	errAppliedThrottled = fmt.Errorf("simulation: injected throttling after the write applied: %w", store.ErrThrottled)
)

// injected reports whether err is one the store's faults explain.
func injected(err error) bool {
	for _, class := range []error{errTransient, errAmbiguous, store.ErrConflict, store.ErrThrottled} {
		if errors.Is(err, class) {
			return true
		}
	}
	return false
}

// invocation is one caller at the gates: a core invocation, or a harness actor such as a
// writer, which has no deadline and never crashes.
type invocation struct {
	id       string
	start    time.Time
	deadline time.Time     // zero for harness actors
	idle     bool          // a core invocation whose batch holds neither INSERT nor MODIFY
	stallAt  int           // the request whose response stalls; -1 for none
	crashAt  int           // the request at which a core invocation dies; -1 for none
	stall    time.Duration // how long before the deadline the stalled response returns
	skew     time.Duration // a writer's clock offset, which its appends' ingest_time carries
	// taken is set when a writer's write of an event found another call's event at its key.
	// skewed is the last item whose ingest_time skew shifted, so a resend of it isn't
	// shifted again. called is when the writer's append call started. Only the writer
	// touches them.
	taken  bool
	skewed store.Item
	called time.Time

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

// lifetime is how long a core invocation lives: 0 for a harness actor.
func (inv *invocation) lifetime() time.Duration {
	if inv.deadline.IsZero() {
		return 0
	}
	return inv.deadline.Sub(inv.start)
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
	runs     *backfill.MemRuns // the backfills' staged objects
	sweeper  *sweeper
	streams  map[string]*stream // by table
	order    []*stream          // in catalog order
	mappings []*mapping
	links    []*link
	pool     []container // idle warm containers
	build    int         // the build that new containers run
	// backfillStart is when the region's backfills start with Backfill.Together, after the
	// run's start; backfilling counts those that haven't ended. retiredAt is when the region's
	// tombstone of Config.Retire was first stored, zero before.
	backfillStart time.Duration
	backfilling   int
	retiredAt     time.Time
	unitIDs       map[string]bool
	units         map[string]*unit // by unit key, as high-water marks name them
	// written holds the keys of the items written to each view table, for P2.
	written map[string]map[store.Key]bool
	// delivered holds when the last invocation that delivered each record started, for P10.
	delivered map[RecordID]time.Time
	// held holds the items of the transactions in progress.
	held map[tableKey]bool
	// live holds the versions live in the region's meta, and liveUntil when each version
	// last stopped being live, for P11b and P15.
	live      map[string]bool
	liveUntil map[string]time.Time
	// waitable holds the INSERTs that appends here asked receipts for, by their receipts'
	// partition keys, and landed when each receipt first landed, for P12, P13 and P15.
	waitable map[string]waited
	landed   map[string]time.Time
	// off holds the streams that the region's functions don't archive, and gen counts its
	// changes, which new containers take. It is replaced, never changed. The mappings stall
	// from stallFrom to stallTo.
	off                map[string]bool
	gen                int
	stallFrom, stallTo time.Time
	// For P14: when each stream's origin landed; when the functions changed archiving it, and
	// when progress passed a record of it a day old; the catalogs registered, in order; and
	// when each actor last read each stream item, by actor and stream.
	originAt   map[string]time.Time
	changes    map[string][]time.Time
	lagged     map[string][]time.Time
	registered []registration
	reads      map[string]time.Time
	// listed holds when each actor last listed each stream's error prefix, by actor and stream.
	listed map[string]time.Time
}

func newRegion(ctx context.Context, r *run, i int) (*region, error) {
	rg := &region{
		run:       r,
		name:      r.regionNames[i],
		mem:       mem.New(),
		archive:   newArchive(r.sc.Archive, r.actorRNG(r.regionNames[i]+" archive")),
		runs:      backfill.NewMemRuns(),
		streams:   map[string]*stream{},
		unitIDs:   map[string]bool{},
		units:     map[string]*unit{},
		written:   map[string]map[store.Key]bool{},
		delivered: map[RecordID]time.Time{},
		held:      map[tableKey]bool{},
		live:      map[string]bool{},
		liveUntil: map[string]time.Time{},
		waitable:  map[string]waited{},
		landed:    map[string]time.Time{},
		off:       map[string]bool{},
		originAt:  map[string]time.Time{},
		changes:   map[string][]time.Time{},
		lagged:    map[string][]time.Time{},
		reads:     map[string]time.Time{},
		listed:    map[string]time.Time{},
	}
	rg.store = gated{rg}
	rg.archiver = archiver{rg}
	rg.sweeper = &sweeper{rg: rg, id: rg.name + " sweeper", queue: newDueQueue[sweep]()}
	if r.sc.Builds.Switch > 0 {
		rg.build = buildOld
	}
	if r.sc.Backfill.Together {
		rg.backfillStart = r.sc.History.Deployed + between(r.actorRNG(rg.name+" backfill"), 0, r.sc.Backfill.Within)
	}
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
	if _, err := meta.New(rg.mem, r.meta).RegisterCatalog(ctx, r.catalogData, "sim"); err != nil {
		return nil, err
	}
	if h, rng := r.sc.History, r.actorRNG(rg.name+" stall"); chance(rng, h.Stall) {
		rg.stallFrom = time.Now().Add(between(rng, 0, h.StallWithin))
		rg.stallTo = rg.stallFrom.Add(h.StallFor + between(rng, 0, time.Hour))
		r.count(&r.stats.Stalls)
	}
	return rg, rg.openHistory(ctx)
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
			if _, err := m.Activate(ctx, id, vv.Stream, meta.SplitFence, meta.DefaultBounds); err != nil {
				return err
			}
			if err := m.MakeLive(ctx, id, time.Time{}); err != nil {
				return err
			}
			rg.noteState(id, meta.Live)
		}
	}
	return nil
}

// noteState notes that version's state in the region's meta is now state, for P11b.
func (rg *region) noteState(version string, state meta.State) {
	switch {
	case state == meta.Live:
		rg.live[version] = true
	case rg.live[version]:
		rg.live[version] = false
		rg.liveUntil[version] = time.Now()
	}
}

// wasLive reports whether version was live in the region's meta at some moment from since
// on.
func (rg *region) wasLive(version string, since time.Time) bool {
	return rg.live[version] || !rg.liveUntil[version].Before(since)
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
	kindTransact
)

type fault uint8

const (
	faultNone fault = iota
	faultTransient
	faultAmbiguous
	faultRetried
	faultLate
	faultConflict
	faultInProgress
	faultAppliedThrottled
	faultAppliedConflict
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
	if inv.idle {
		r.violatef("P9: %s: %s for a batch without an INSERT or a MODIFY", inv.id, op)
	}
	core := !inv.deadline.IsZero()
	if d, has := ctx.Deadline(); core && (!has || d.After(inv.deadline)) {
		r.violatef("P7: %s: %s with a context that outlives the invocation's deadline", inv.id, op)
	}
	if core && (r.draw(r.sc.Faults.Crash) || n == inv.crashAt) {
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

// fault draws a request's fault. A rate of 0 draws nothing, so a fault added here leaves
// the draws of scenarios without it as they were.
func (r *run) fault(k kind, core bool) fault {
	f := r.sc.Faults
	single := k == kindWrite || k == kindCondWrite
	switch {
	case r.draw(f.Transient):
		r.count(&r.stats.Transient)
		return faultTransient
	case k == kindRead:
		return faultNone
	case core && single && r.draw(f.Late):
		r.count(&r.stats.Late)
		return faultLate
	case k != kindArchive && r.draw(f.Conflict):
		r.count(&r.stats.Conflicts)
		return faultConflict
	case r.draw(f.Ambiguous):
		r.count(&r.stats.Ambiguous)
		return faultAmbiguous
	case (k == kindCondWrite || k == kindTransact) && r.draw(f.Retried):
		r.count(&r.stats.Retried)
		return faultRetried
	}
	return r.appliedFault(k, single)
}

// appliedFault draws the faults that apply a write and fail its caller, after Retried.
func (r *run) appliedFault(k kind, single bool) fault {
	f := r.sc.Faults
	switch {
	case k == kindTransact && r.draw(f.InProgress):
		return faultInProgress
	case k != kindArchive && r.draw(f.AppliedThrottled):
		r.count(&r.stats.AppliedThrottled)
		return faultAppliedThrottled
	case single && r.draw(f.AppliedConflict):
		r.count(&r.stats.AppliedConflicts)
		return faultAppliedConflict
	}
	return faultNone
}

// appliedError is what the caller of a write that applied sees under f.
func appliedError(f fault) error {
	switch f {
	case faultAmbiguous:
		return errAmbiguous
	case faultAppliedThrottled:
		return errAppliedThrottled
	case faultAppliedConflict:
		return errAppliedConflict
	case faultNone, faultTransient, faultRetried, faultLate, faultConflict, faultInProgress:
	}
	return nil
}

// gated is a region's store as the code under test sees it.
type gated struct{ rg *region }

func (g gated) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	lat, f := g.rg.enter(ctx, kindRead, "get "+table+" "+k.PK+" "+k.SK)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	if stream, ok := strings.CutPrefix(k.PK, streamPrefix); ok && table == g.rg.run.meta {
		g.rg.reads[invocationOf(ctx).id+" "+stream] = time.Now()
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

// scanPage is the most items a page of a scan holds that asks DynamoDB for 1 MB pages: that
// limit, scaled to the harness's small items.
const scanPage = 8

// ScanPage reads mem when its request is released, and the caller gets the page after the
// request's latency.
func (g gated) ScanPage(ctx context.Context, table string, s store.Scan, start *store.Key) ([]store.Item, *store.Key, error) {
	if s.PageSize == 0 {
		s.PageSize = scanPage
	}
	op := "scan " + table + " " + strconv.Itoa(s.Segment) + "/" + strconv.Itoa(s.Segments)
	if start != nil {
		op += " " + start.PK + " " + start.SK
	}
	lat, f := g.rg.enter(ctx, kindRead, op)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, nil, errTransient
	}
	return g.rg.mem.ScanPage(ctx, table, s, start)
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
	case faultConflict:
		time.Sleep(lat)
		return errConflict
	case faultLate:
		g.rg.landLate(ctx, table, it, c)
	case faultNone, faultAmbiguous, faultRetried, faultInProgress, faultAppliedThrottled, faultAppliedConflict:
	}
	if g.rg.held[tableKey{table, store.KeyOf(it)}] {
		g.rg.run.count(&g.rg.run.stats.Conflicts)
		time.Sleep(lat)
		return errHeld
	}
	if g.rg.streams[table] != nil {
		g.rg.skew(ctx, it)
	}
	err := g.rg.apply(ctx, table, it, c)
	if g.rg.streams[table] != nil {
		var ce *store.ConditionError
		invocationOf(ctx).taken = errors.As(err, &ce)
		if _, ok := it[catalog.AttrSync]; ok && err == nil {
			g.rg.checkSync(ctx, []store.TxPut{{Table: table, Item: it, Cond: c}})
		}
	}
	if err == nil && f == faultRetried {
		// The SDK's retry finds the first attempt's write and fails its condition.
		var cur store.Item
		if cur, err = g.rg.mem.Get(ctx, table, store.KeyOf(it), true); err == nil {
			err = &store.ConditionError{Current: cur}
		}
	}
	time.Sleep(lat)
	if err == nil {
		err = appliedError(f)
	}
	return err
}

// skew shifts an append's ingest_time by its writer's clock offset, once per item. It
// changes the caller's item, as the writer's clock would have written it, so the append
// still knows its event.
func (rg *region) skew(ctx context.Context, it store.Item) {
	inv := invocationOf(ctx)
	if inv.skew == 0 || inv.skewed != nil && reflect.ValueOf(inv.skewed).UnsafePointer() == reflect.ValueOf(it).UnsafePointer() {
		return
	}
	inv.skewed = it
	at := integer(it[catalog.AttrIngestTime]) + inv.skew.Microseconds()
	it[catalog.AttrIngestTime] = store.Value{Type: store.N, Str: strconv.FormatInt(at, 10)}
	rg.run.count(&rg.run.stats.Skewed)
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
	return rg.write(ctx, table, it, c, false)
}

// write is apply, of a transaction's put when tx is set.
func (rg *region) write(ctx context.Context, table string, it store.Item, c store.Cond, tx bool) error {
	if s := rg.streams[table]; s != nil {
		return s.append(ctx, it, c)
	}
	cur, getErr := rg.mem.Get(ctx, table, store.KeyOf(it), true)
	if err := rg.mem.Put(ctx, table, it, c); err != nil {
		return err
	}
	if getErr == nil {
		rg.checkWrite(table, cur, it, invocationOf(ctx).lifetime(), tx)
	}
	switch {
	case rg.run.views[table] != "":
		rg.checkRetired(ctx, table)
		if rg.written[table] == nil {
			rg.written[table] = map[store.Key]bool{}
		}
		rg.written[table][store.KeyOf(it)] = true
	case table == rg.run.meta:
		rg.sweeper.expire(table, it)
		rg.noteTombstone(it)
		rg.noteHistory(ctx, cur, it)
		if k := store.KeyOf(it); k.PK == "view" {
			rg.noteState(k.SK, meta.State(it["state"].Str))
		} else if strings.HasPrefix(k.PK, receiptPrefix) {
			rg.noteReceipt(ctx, it)
		}
	}
	return nil
}

// checkWrite checks a write that replaced cur with next, sent by an invocation that lives
// lifetime, in a transaction when tx is set: the row version rises by exactly one, and a
// marker's owner, a unit's progress and its fences never go back (P5); a row's high-water
// marks stay within the units that can still deliver (P4), or, written by a transaction,
// stay as they were (P11c).
func (rg *region) checkWrite(table string, cur, next store.Item, lifetime time.Duration, tx bool) {
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
	if table == r.meta && strings.HasPrefix(store.KeyOf(next).PK, unitPrefix) {
		rg.checkUnit(where, cur, next)
	}
	hwm, ok := next[catalog.AttrHighWaterMarks]
	switch {
	case tx && marks(next) != marks(cur):
		r.violatef("P11c: %s: a transaction wrote high-water marks {%s} over {%s}", where, marks(next), marks(cur))
	case ok && !tx:
		rg.checkMarks(where, table, cur[catalog.AttrHighWaterMarks].Map, hwm.Map, lifetime)
	}
}

// checkUnit checks a write that replaced cur, a unit item, with next: its progress and
// fences never go back (P5), and when its progress passed a record a day old, a lag was
// recorded (P14).
func (rg *region) checkUnit(where string, cur, next store.Item) {
	r := rg.run
	from, to := cur[attrProgress].Str, next[attrProgress].Str
	if _, ok := cur[attrProgress]; ok && to < from {
		r.violatef("P5: %s: progress %s went back to %q", where, from, to)
	}
	if to > from {
		rg.checkLag(next[catalog.AttrPK].Str, from, to)
	}
	fences, stored := cur[attrFences].Map, next[attrFences].Map
	for _, version := range slices.Sorted(maps.Keys(fences)) {
		if stored[version].Str != fences[version].Str {
			r.violatef("P5: %s: fence of %s changed from %s to %q", where, version, fences[version].Str, stored[version].Str)
		}
	}
}

// marks returns a row item's high-water marks as text, "" for none.
func marks(it store.Item) string {
	m := it[catalog.AttrHighWaterMarks].Map
	parts := make([]string, 0, len(m))
	for _, unit := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, unit+"="+valueText(m[unit]))
	}
	return strings.Join(parts, " ")
}

// checkMarks checks P4 on a write to table, a view's, that replaced the high-water marks old
// with next: no entry of a unit that finished by prunableSince, and at most one per unit
// of the view's stream that may still need its own, plus the backfill's. It counts the
// entries the write dropped.
func (rg *region) checkMarks(where, table string, old, next map[string]store.Value, lifetime time.Duration) {
	r := rg.run
	since := rg.prunableSince(time.Now(), lifetime)
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
// write that lands at now, from an invocation that lives lifetime, to drop its high-water
// mark. A unit finished at f had its progress stamped by f + timeout at the latest, and a
// write lands at most its invocation's lifetime after it read progress, plus LateMax when
// writes land late; from f + W + timeout + lifetime (+ LateMax) on, every write prunes the
// unit's entry. A backfill step may live longer than a projector's timeout.
func (rg *region) prunableSince(now time.Time, lifetime time.Duration) time.Time {
	sc := rg.run.sc
	since := now.Add(-rg.run.window - sc.ESM.Timeout - lifetime)
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

// container is a warm container: its core, the build the core runs, and the generation of
// the region's archiving it took.
type container struct {
	core  Core
	build int
	gen   int
}

// acquire takes a warm container of the region's build, or of the full build for a backfill
// worker, or starts a new one.
func (rg *region) acquire(actor string, rng *rand.Rand, worker bool) container {
	r := rg.run
	r.sched.gate(actor, "acquire")
	build := rg.build
	if worker {
		build = buildFull
	}
	var idle []int
	for i, c := range rg.pool {
		if c.build == build && c.gen == rg.gen {
			idle = append(idle, i)
		}
	}
	if len(idle) == 0 {
		r.count(&r.stats.Containers)
		return container{core: r.sys.NewCore(rg.name, rg.store, archiving{rg.archiver, rg.off}, r.builds[build]), build: build, gen: rg.gen}
	}
	i := idle[rng.IntN(len(idle))]
	c := rg.pool[i]
	rg.pool = slices.Delete(rg.pool, i, i+1)
	return c
}

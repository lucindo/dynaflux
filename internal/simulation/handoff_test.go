// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/projector/projectortest"
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// The scripted handoffs below place one race each, which random seeds reach too rarely,
// and backfill site_totals, whose views column counts the records applied to s0.

// archiveDelay is how long every archived record takes to arrive.
const archiveDelay = time.Minute

// handoff is one region of the fixture's store with every version but site_totals live,
// and an archive that delivers each record archiveDelay after its put.
type handoff struct {
	sys     *pageViews
	mem     *mem.Mem
	meta    *meta.Meta
	engine  *engine.Engine
	archive *fixedArchive
}

func newHandoff(t *testing.T) *handoff {
	t.Helper()
	h := &handoff{
		sys:     newPageViews(t),
		mem:     mem.New(),
		archive: &fixedArchive{},
	}
	h.meta = meta.New(h.mem, metaTable())
	h.engine = engine.New(h.mem, engine.Options{Region: "r0", Entropy: rand.NewChaCha8([32]byte{})})
	for _, v := range h.sys.cat.Catalog().Views {
		for _, vv := range v.Versions {
			if id := catalog.VersionID(v.Name, vv.Number); id != siteTotals {
				h.activate(t, id, vv.Stream)
				h.makeLive(t, id)
			}
		}
	}
	return h
}

// activate makes a version of stream activating with the fence split, as a backfill does.
func (h *handoff) activate(t *testing.T, version, stream string) {
	t.Helper()
	if _, err := h.meta.Activate(t.Context(), version, stream, meta.SplitFence, meta.DefaultBounds); err != nil {
		t.Fatal(err)
	}
}

func (h *handoff) makeLive(t *testing.T, version string) {
	t.Helper()
	if err := h.meta.MakeLive(t.Context(), version, time.Time{}); err != nil {
		t.Fatal(err)
	}
}

// core returns a container's core, writing through b.
func (h *handoff) core(b store.Backend) *projector.Core {
	return projector.New(h.sys.cat.Projector(), b, h.archive, projector.Config{Naming: h.sys.Config().Naming, Region: "r0"})
}

// batch appends a page view of s0 per sequence number and returns them as one batch of
// unit.
func (h *handoff) batch(t *testing.T, unit string, seqs ...int) projector.Batch {
	t.Helper()
	b := projector.Batch{Stream: "page_views", Unit: unit}
	for _, n := range seqs {
		ev := []expr.Value{value("s", 0), value("p", 0), value("u", n), expr.Uint(100), expr.Decimal(big.NewInt(100)),
			expr.Float(1), value("l", 0), expr.Array(nil), expr.Null()}
		res, err := h.engine.Append(t.Context(), h.sys.cat, "page_views", [][]expr.Value{ev}, engine.AppendMode{})
		if err == nil {
			err = res[0].Err
		}
		it, gerr := h.mem.Get(t.Context(), h.sys.Config().Naming.StreamTable("page_views"), res[0].Key, true)
		if err != nil || gerr != nil {
			t.Fatal(err, gerr)
		}
		b.Records = append(b.Records, projector.Record{Op: projector.Insert, Seq: fmt.Sprintf("1%020d", n), New: it})
	}
	return b
}

// deliver delivers b to core in one invocation, and fails unless every row was written.
func deliver(ctx context.Context, core *projector.Core, b projector.Batch) error {
	return deliverBy(ctx, core, b, time.Now().Add(2*time.Minute))
}

// deliverBy is deliver in an invocation whose deadline is deadline.
func deliverBy(ctx context.Context, core *projector.Core, b projector.Batch, deadline time.Time) error {
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	res, err := core.Handle(ctx, b)
	if err == nil && res.FailedSeq != "" {
		err = fmt.Errorf("rows failed from %s: %w", res.FailedSeq, res.RowErr)
	}
	return err
}

// handle delivers a new batch of unit to core.
func (h *handoff) handle(t *testing.T, core *projector.Core, unit string, seqs ...int) {
	t.Helper()
	if err := deliver(t.Context(), core, h.batch(t, unit, seqs...)); err != nil {
		t.Errorf("Handle of %s %v: %v", unit, seqs, err)
	}
}

// backfill fills site_totals as a backfill job does with the cut FenceUnits returned: it
// waits until every record archived by then arrived, keeps the arrived records at or below
// their unit's fence, applies them and makes the version live.
func (h *handoff) backfill(t *testing.T, core *projector.Core, cut time.Time) {
	t.Helper()
	time.Sleep(h.archive.completeAfter(cut))
	recs := h.archive.arrived(time.Now())
	var unitKeys []string
	for _, r := range recs {
		unitKeys = append(unitKeys, keys.UnitKey(r.Unit))
	}
	slices.Sort(unitKeys)
	units, err := h.meta.Units(t.Context(), "page_views", slices.Compact(unitKeys), true)
	if err != nil {
		t.Fatal(err)
	}
	fences := map[string]string{}
	for _, u := range units {
		if f, ok := u.Fences[siteTotals]; ok {
			fences[u.ID] = f
		}
	}
	sel, err := projectortest.SelectBackfill(recs, fences)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err = core.ApplyBackfill(ctx, siteTotals, sel); err != nil {
		t.Fatal(err)
	}
	h.makeLive(t, siteTotals)
}

// views returns site_totals' views of s0.
func (h *handoff) views(t *testing.T) string {
	t.Helper()
	st, err := h.sys.Read(t.Context(), h.mem)
	if err != nil {
		t.Fatal(err)
	}
	return st.Rows[simulation.RowID{View: siteTotals, Key: "s0"}]["views"]
}

// TestUnitsAreListedOnceEveryCoreSawTheActivation: a core whose cached view states
// predate site_totals' activation delivers the first records of a unit created after an
// early listing: it doesn't fence the unit, archives the records after the cut and advances
// past them, and the next batch fences the unit there. Records at or below a fence then
// arrive in the archive after the backfill read it. Listing only after every core saw the
// activation (meta.DefaultBounds.Listing) fences such a unit before it archives anything.
func TestUnitsAreListedOnceEveryCoreSawTheActivation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newHandoff(t)
		cached, fresh := h.core(h.mem), h.core(h.mem)
		h.handle(t, cached, "shardId-u0", 1) // caches site_totals pending
		h.activate(t, siteTotals, "page_views")
		var mu sync.Mutex
		units := []string{"shardId-u0"}
		list := func(context.Context) ([]string, error) {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(units), nil
		}
		var cut time.Time
		var err error
		fenced := make(chan struct{})
		go func() {
			defer close(fenced)
			cut, _, err = h.meta.FenceUnits(t.Context(), "page_views", siteTotals, meta.DefaultBounds, list)
		}()
		time.Sleep(time.Second)
		mu.Lock()
		units = append(units, "shardId-u1")
		mu.Unlock()
		time.Sleep(time.Second)
		h.handle(t, cached, "shardId-u1", 2, 3)
		h.handle(t, fresh, "shardId-u1", 4)
		<-fenced
		if err != nil {
			t.Fatal(err)
		}
		h.backfill(t, h.core(h.mem), cut)
		if got := h.views(t); got != "4" {
			t.Errorf("site_totals s0 counts %s views, want 4", got)
		}
	})
}

// TestRecordArchivedWhileFencingReachesTheBackfill: a batch whose states predate
// site_totals' activation, and which ran long enough to outlive the listing delay, archives
// its record and advances the unit between the job's read of the unit and its fence. The
// batch didn't apply site_totals, so the record is the backfill's: the fence must cover the
// progress it replaces, and the cut must follow the fence.
func TestRecordArchivedWhileFencingReachesTheBackfill(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newHandoff(t)
		cached := h.core(h.mem)
		h.handle(t, cached, "shardId-u0", 1) // caches site_totals pending
		h.activate(t, siteTotals, "page_views")
		b := h.batch(t, "shardId-u0", 2)
		h.archive.held = make(chan struct{})
		batch := make(chan error)
		go func() { batch <- deliver(t.Context(), cached, b) }()
		var batchErr error
		job := meta.New(&afterUnitRead{Backend: h.mem, do: func() {
			time.Sleep(time.Second) // so the record is archived after the listing
			close(h.archive.held)
			batchErr = <-batch
		}}, metaTable())
		list := func(context.Context) ([]string, error) { return []string{"shardId-u0"}, nil }
		cut, _, err := job.FenceUnits(t.Context(), "page_views", siteTotals, meta.DefaultBounds, list)
		if err != nil || batchErr != nil {
			t.Fatal(err, batchErr)
		}
		h.backfill(t, h.core(h.mem), cut)
		if got := h.views(t); got != "2" {
			t.Errorf("site_totals s0 counts %s views, want 2", got)
		}
	})
}

var errUnitWrite = errors.New("unit write failed")

// TestUnitFencesItselfBeforeItArchives: the first batch of a unit created after the listing
// fails to fence the unit, and the backfill reads the archive before the batch is delivered
// again. Had the batch archived its record before the fence, the backfill would apply the
// record of a unit without one, and the retry would apply it live too.
func TestUnitFencesItselfBeforeItArchives(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newHandoff(t)
		h.activate(t, siteTotals, "page_views")
		none := func(context.Context) ([]string, error) { return nil, nil }
		cut, _, err := h.meta.FenceUnits(t.Context(), "page_views", siteTotals, meta.DefaultBounds, none)
		if err != nil {
			t.Fatal(err)
		}
		core, b := h.core(&failsFirstUnitWrite{Backend: h.mem}), h.batch(t, "shardId-u1", 1)
		if err = deliver(t.Context(), core, b); !errors.Is(err, errUnitWrite) {
			t.Fatalf("first delivery: %v, want the failed unit write", err)
		}
		time.Sleep(archiveDelay) // whatever the batch archived has arrived
		h.backfill(t, h.core(h.mem), cut)
		if err = deliver(t.Context(), core, b); err != nil {
			t.Fatal(err)
		}
		if got := h.views(t); got != "1" {
			t.Errorf("site_totals s0 counts %s views, want 1", got)
		}
	})
}

// fixedArchive is an archive whose records arrive archiveDelay after their put. A put waits
// for held to close, when it is set.
type fixedArchive struct {
	held chan struct{} // set before any put that it holds

	mu   sync.Mutex
	recs []projector.BackfillRecord
	puts []time.Time
}

func (a *fixedArchive) Archive(_ context.Context, _, unit string, recs []projector.Record) error {
	if a.held != nil {
		<-a.held
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, r := range recs {
		a.recs = append(a.recs, projector.BackfillRecord{Unit: unit, Seq: r.Seq, Item: r.New})
		a.puts = append(a.puts, time.Now())
	}
	return nil
}

// completeAfter returns how long until every record put by cut has arrived.
func (a *fixedArchive) completeAfter(cut time.Time) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	var wait time.Duration
	for _, put := range a.puts {
		if !put.After(cut) {
			wait = max(wait, time.Until(put.Add(archiveDelay)))
		}
	}
	return wait
}

func (a *fixedArchive) arrived(now time.Time) []projector.BackfillRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []projector.BackfillRecord
	for i, put := range a.puts {
		if !put.Add(archiveDelay).After(now) {
			out = append(out, a.recs[i])
		}
	}
	return out
}

// failsFirstUnitWrite fails the first write of a unit item, without effect.
type failsFirstUnitWrite struct {
	store.Backend

	once sync.Once
}

func (f *failsFirstUnitWrite) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	fail := false
	if table == metaTable() && strings.HasPrefix(it[catalog.AttrPK].Str, "unit#") {
		f.once.Do(func() { fail = true })
	}
	if fail {
		return errUnitWrite
	}
	return f.Backend.Put(ctx, table, it, c)
}

// afterUnitRead runs do once, after the first read of a unit item.
type afterUnitRead struct {
	store.Backend

	do   func()
	once sync.Once
}

func (a *afterUnitRead) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	it, err := a.Backend.Get(ctx, table, k, consistent)
	if strings.HasPrefix(k.PK, "unit#") {
		a.once.Do(a.do)
	}
	return it, err
}

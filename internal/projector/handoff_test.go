// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/projector/projectortest"
	"github.com/lucindo/dynaflux/internal/store"
)

const pageTotals = "page_totals@v1"

// archiveLog is an archive that keeps what it is given, or fails with err.
type archiveLog struct {
	mu       sync.Mutex
	recs     []projector.BackfillRecord
	arrivals []time.Time // of recs
	err      error
}

func (a *archiveLog) Archive(_ context.Context, _, unit string, recs []projector.Record) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	for _, r := range recs {
		a.recs = append(a.recs, projector.BackfillRecord{Unit: unit, Seq: r.Seq, Item: r.New})
		a.arrivals = append(a.arrivals, r.Arrival)
	}
	return nil
}

// seqs returns the archived records as their sequence numbers and arrivals.
func (a *archiveLog) seqs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.recs))
	for i, r := range a.recs {
		out[i] = r.Seq + " at " + a.arrivals[i].Format(time.RFC3339)
	}
	return out
}

func (f *fixture) unit(t *testing.T) meta.Unit {
	t.Helper()
	u, err := f.meta.Unit(t.Context(), "clicks", unit)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func (f *fixture) activate(t *testing.T, version string) {
	t.Helper()
	if _, err := f.meta.Activate(t.Context(), version, "clicks", meta.SplitFence, meta.DefaultBounds); err != nil {
		t.Fatal(err)
	}
}

func TestHandleArchivesOnlyInserts(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		a, b := f.click(t, "u1", "/a", 100), f.click(t, "u2", "/a", 200)
		arrival := start.Add(-time.Second)
		res := f.handle(t,
			projector.Record{Op: projector.Insert, Seq: seq(1), Arrival: arrival, New: a},
			projector.Record{Op: projector.Modify, Seq: seq(2), Arrival: arrival, Old: a, New: b},
			projector.Record{Op: projector.Remove, Seq: seq(3), Arrival: arrival, Old: a},
		)
		want := []string{seq(1) + " at 1999-12-31T23:59:59Z"}
		if got := f.archive.seqs(); res.Stats.Divergent != 1 || !slices.Equal(got, want) {
			t.Errorf("archived %v with %d divergent, want %v and 1", got, res.Stats.Divergent, want)
		}
	})
}

func TestHandleAdvancesProgressToTheDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		fail := &failOnce{table: pageTotalsTable, pk: "/b"}
		f := newFixture(t, func(b store.Backend) store.Backend { fail.Backend = b; return fail })
		// No click has tags, so no record reaches user_tags, which every batch still applies.
		res := f.handle(t, insert(1, f.click(t, "u1", "/a", 100)), insert(2, f.click(t, "u1", "/b", 100)), insert(3, f.click(t, "u1", "/c", 100)))
		u := f.unit(t)
		if res.FailedSeq != seq(2) || u.Progress != padSeq(t, seq(1)) || !u.ProgressAt.Equal(start.Add(time.Minute)) {
			t.Errorf("FailedSeq %q, progress %s at %s; want %s, %s at %s",
				res.FailedSeq, u.Progress, u.ProgressAt, seq(2), padSeq(t, seq(1)), start.Add(time.Minute))
		}
		if fence := u.Fences["user_tags@v1"]; fence != strings.Repeat("0", catalog.SequenceWidth) {
			t.Errorf("user_tags fence %q, want the zero fence of a unit without progress", fence)
		}
	})
}

func TestHandleFailsWhenTheArchiveFails(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		f.archive.err = errTransient
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		_, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{insert(1, f.click(t, "u1", "/a", 100))}})
		if !errors.Is(err, errTransient) || f.unit(t).Progress != "" || f.row(t, pageTotals, page("/a")) != "" {
			t.Errorf("Handle = %v with progress %q, want the archive's error before any write", err, f.unit(t).Progress)
		}
	})
}

// A version activated after the unit's progress reached 2 takes records above 2 live; 1 and
// 2 are its backfill's.
func TestHandleAppliesFencedVersionAboveItsFence(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		c1, c2, c3 := f.click(t, "u1", "/a", 100), f.click(t, "u2", "/a", 200), f.click(t, "u3", "/a", 400)
		f.handle(t, insert(1, c1), insert(2, c2))
		f.activate(t, pageTotals)
		time.Sleep(meta.MaxViewStateAge)
		f.handle(t, insert(2, c2), insert(3, c3))
		if fence := f.unit(t).Fences[pageTotals]; fence != padSeq(t, seq(2)) {
			t.Errorf("fence %q, want %s", fence, padSeq(t, seq(2)))
		}
		if got, want := f.row(t, pageTotals, page("/a")), "/a 400 4.00 400 400 1 1"; got != want {
			t.Errorf("page_totals /a is %q, want %q", got, want)
		}
	})
}

// A batch whose cached states predate an activation finds the version's fence on the unit
// and reads the states again, instead of failing on the fence.
func TestHandleRefreshesStatesThatPredateAFence(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		f.handle(t, insert(1, f.click(t, "u1", "/a", 100)))
		f.activate(t, pageTotals)
		if _, err := f.meta.EnsureFence(t.Context(), f.unit(t), pageTotals); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		_, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{insert(2, f.click(t, "u2", "/a", 200))}})
		if got, want := f.row(t, pageTotals, page("/a")), "/a 200 2.00 200 200 1 1"; err != nil || got != want {
			t.Errorf("Handle = %v, page_totals /a %q; want no error, %q", err, got, want)
		}
	})
}

// A fence that appears while a batch runs without its version stops the batch's progress,
// and the next batch reads the states again and applies the version.
func TestHandleFailsOnAFenceItDidNotApply(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var f *fixture
		var fenceErr error
		hook := &afterUnitRead{do: func(ctx context.Context) {
			if _, fenceErr = f.meta.Activate(ctx, pageTotals, "clicks", meta.SplitFence, meta.DefaultBounds); fenceErr == nil {
				_, fenceErr = f.meta.EnsureFence(ctx, f.unit(t), pageTotals)
			}
		}}
		f = newPendingFixture(t, func(b store.Backend) store.Backend { hook.Backend = b; return hook })
		c1 := f.click(t, "u1", "/a", 100)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		_, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{insert(1, c1)}})
		if fenceErr != nil {
			t.Fatal(fenceErr)
		}
		if !errors.Is(err, meta.ErrFenced) || f.unit(t).Progress != "" {
			t.Fatalf("Handle = %v with progress %q, want ErrFenced and none", err, f.unit(t).Progress)
		}
		f.handle(t, insert(1, c1))
		if got, want := f.row(t, pageTotals, page("/a")), "/a 100 1.00 100 100 1 1"; got != want || f.unit(t).Progress != padSeq(t, seq(1)) {
			t.Errorf("after the retry page_totals /a is %q with progress %q, want %q and %s", got, f.unit(t).Progress, want, padSeq(t, seq(1)))
		}
	})
}

// A broken version's fence doesn't stop progress: no backfill or batch applies the version
// any more.
func TestBrokenVersionCountsAsCovered(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		f.activate(t, "page_rates@v1")
		f.handle(t, insert(1, f.click(t, "u1", "/a", 100)))
		f.handle(t, insert(2, f.click(t, "u1", "/a", 0))) // page_rates divides by ms
		time.Sleep(meta.MaxViewStateAge)                  // until the core sees page_rates broken
		res := f.handle(t, insert(3, f.click(t, "u1", "/a", 100)))
		u := f.unit(t)
		if _, fenced := u.Fences["page_rates@v1"]; !fenced || u.Progress != padSeq(t, seq(3)) || res.FailedSeq != "" {
			t.Errorf("page_rates fenced %v, progress %s, FailedSeq %q; want a fence and progress %s",
				fenced, u.Progress, res.FailedSeq, padSeq(t, seq(3)))
		}
	})
}

func (f *fixture) applyBackfill(t *testing.T, version string, chunk []projector.BackfillRecord) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	return f.core.ApplyBackfill(ctx, version, chunk)
}

// The handoff end to end: records up to the fence reach the version once through the
// backfill, the rest live, however often each is delivered or archived.
func TestBackfillAndLiveCountEachRecordOnce(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		c := []store.Item{f.click(t, "u1", "/a", 100), f.click(t, "u2", "/a", 200), f.click(t, "u1", "/a", 400), f.click(t, "u3", "/a", 800)}
		f.handle(t, insert(1, c[0]), insert(2, c[1]))
		f.handle(t, insert(1, c[0]), insert(2, c[1])) // archived twice
		f.activate(t, pageTotals)
		list := func(context.Context) ([]string, error) { return []string{unit}, nil }
		if _, _, err := f.meta.FenceUnits(t.Context(), "clicks", pageTotals, meta.DefaultBounds, list); err != nil {
			t.Fatal(err)
		}
		f.handle(t, insert(3, c[2]), insert(4, c[3]))
		sel, err := projectortest.SelectBackfill(f.archive.recs, map[string]string{unit: f.unit(t).Fences[pageTotals]})
		if err != nil {
			t.Fatal(err)
		}
		// The second chunk is applied twice, as after a worker timed out.
		for _, chunk := range [][]projector.BackfillRecord{sel[:1], sel[1:], sel[1:]} {
			if err = f.applyBackfill(t, pageTotals, chunk); err != nil {
				t.Fatal(err)
			}
		}
		if got, want := f.row(t, pageTotals, page("/a")), "/a 800 15.00 100 1500 3 4"; len(sel) != 2 || got != want {
			t.Errorf("backfill of %d records, page_totals /a is %q; want 2, %q", len(sel), got, want)
		}
	})
}

// A pending version with uniqExact takes no record live, so its backfill writes every marker:
// a value counts only for the record whose marker holds it, and a live attempt that wrote one
// and was never retried would leave a value the backfill never counts.
func TestBackfillAloneMarksAPendingVersion(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		c := f.click(t, "u1", "/a", 100)
		f.handle(t, insert(1, c))
		items, _, err := f.mem.ScanPage(t.Context(), "app-view-page_users-v1", store.Scan{Segments: 1}, nil)
		if err != nil || len(items) > 0 {
			t.Fatalf("page_users holds %d items after a live batch, %v; want none", len(items), err)
		}
		if err = f.applyBackfill(t, "page_users@v1", []projector.BackfillRecord{{Unit: unit, Seq: seq(1), Item: c}}); err != nil {
			t.Fatal(err)
		}
		if got, want := f.row(t, "page_users@v1", page("/a")), "/a 1"; got != want {
			t.Errorf("page_users /a is %q, want %q", got, want)
		}
	})
}

func TestApplyBackfillOpensTheBreaker(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		f.activate(t, "page_rates@v1")
		chunk := []projector.BackfillRecord{
			{Unit: unit, Seq: seq(1), Item: f.click(t, "u1", "/a", 100)},
			{Unit: unit, Seq: seq(2), Item: f.click(t, "u1", "/a", 0)}, // page_rates divides by ms
		}
		err := f.applyBackfill(t, "page_rates@v1", chunk)
		if got := f.failedAt(t); !errors.Is(err, projector.ErrBroken) || !slices.Equal(got, []string{"page_rates@v1"}) {
			t.Errorf("ApplyBackfill = %v, failed at record 2: %v; want ErrBroken for page_rates", err, got)
		}
		if got := f.row(t, "page_rates@v1", page("/a")); got != "" {
			t.Errorf("page_rates /a is %q, want no row from a chunk that broke the version", got)
		}
	})
}

func TestApplyBackfillRejectsRecordsOutOfOrder(t *testing.T) {
	t.Parallel()
	f := newPendingFixture(t, nil)
	c := f.click(t, "u1", "/a", 100)
	chunk := []projector.BackfillRecord{{Unit: unit, Seq: seq(2), Item: c}, {Unit: unit, Seq: seq(1), Item: c}}
	if err := f.applyBackfill(t, pageTotals, chunk); err == nil {
		t.Error("ApplyBackfill took records out of order")
	}
}

// afterUnitRead runs do once, after the first read of a unit item.
type afterUnitRead struct {
	store.Backend

	do   func(context.Context)
	once sync.Once
}

func (a *afterUnitRead) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	it, err := a.Backend.Get(ctx, table, k, consistent)
	if strings.HasPrefix(k.PK, "unit#") {
		a.once.Do(func() { a.do(ctx) })
	}
	return it, err
}

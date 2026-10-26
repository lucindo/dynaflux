// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// Boundaries of D23a that random seeds can't pin, on site_totals' row of s0, which every
// unit writes.

const (
	margin = catalog.LifetimeMarginSeconds * time.Second
	// lifetime is the longest invocation a core with W = window accepts.
	lifetime = window - margin
)

// liveHandoff is newHandoff with site_totals live too.
func liveHandoff(t *testing.T) *handoff {
	t.Helper()
	h := newHandoff(t)
	h.setState(t, siteTotals, meta.Activating)
	h.setState(t, siteTotals, meta.Live)
	return h
}

// TestPruneBoundary: a row write keeps a unit's mark while the unit's progress rose at most W
// ago, and drops it a microsecond later. The comparison is strict, the window is W, and a
// progress advance is timed by the deadline of the invocation that made it.
func TestPruneBoundary(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := liveHandoff(t)
		marks := &lastMarks{Backend: h.mem, table: catalog.Naming{Store: storeName}.ViewTable("site_totals", 1)}
		deadline := time.Now().Add(lifetime)
		if err := deliverBy(t.Context(), h.core(marks), h.batch(t, "shardId-S", 1), deadline); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(deadline.Add(window)))
		if err := deliverBy(t.Context(), h.core(marks), h.batch(t, "shardId-T", 2), time.Now().Add(lifetime)); err != nil {
			t.Fatal(err)
		}
		atW := marks.has("shardId-S")
		time.Sleep(time.Microsecond)
		if err := deliverBy(t.Context(), h.core(marks), h.batch(t, "shardId-U", 3), time.Now().Add(lifetime)); err != nil {
			t.Fatal(err)
		}
		if afterW := marks.has("shardId-S"); !atW || afterW {
			t.Errorf("S's mark kept by a write W after S's progress rose: %v, and 1 µs later: %v; want true, false", atW, afterW)
		}
		if got := h.views(t); got != "3" {
			t.Errorf("site_totals s0 counts %s views, want 3", got)
		}
	})
}

// TestLateProgress: J applies S's record to the row, but its progress write is in flight
// when J times out, and lands late. I, a redelivery of J's batch, starts just before it
// lands, reads the old progress, and writes the row at the end of its life. T writes the row
// at the first instant W lets it drop S's mark. I writes first as long as it lives at most
// W − 60 s and the late write lands at most 60 s late: W ≥ lifetime + lateness.
func TestLateProgress(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		late     time.Duration // after J's deadline, when J's progress write lands
		lifetime time.Duration // of I
		err      error         // I's
		twice    bool          // S's record counts twice: the margin is exact
	}{
		{"the write lands at the margin", margin, lifetime, nil, false},
		{"I lives longer than W allows", margin, lifetime + time.Second, projector.ErrLifetime, false},
		{"the write lands after the margin", margin + time.Second, lifetime, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				h := liveHandoff(t)
				if err := lateProgressStory(t, h, tc.late, tc.lifetime); !errors.Is(err, tc.err) {
					t.Errorf("I: %v, want %v", err, tc.err)
				}
				want := "2"
				if tc.twice {
					want = "3"
				}
				if got := h.views(t); got != want {
					t.Errorf("site_totals s0 counts %s views, want %s", got, want)
				}
			})
		})
	}
}

// TestWarmContainerReadsProgressAgain: container K applies S's record and advances S's
// progress, stamped with a deadline a second away, so T's write drops S's mark W later. K
// then gets S's record again, within one lifetime of its first read of S's progress, and would
// write the row after T: only a progress read by this invocation lets K skip the record.
func TestWarmContainerReadsProgressAgain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := liveHandoff(t)
		start := time.Now()
		k, b := h.core(lastInstant{h.mem}), h.batch(t, "shardId-S", 1)
		if err := deliverBy(t.Context(), k, b, start.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(start.Add(lifetime - time.Microsecond)))
		again := make(chan error, 1)
		go func() { again <- deliverBy(t.Context(), k, b, time.Now().Add(lifetime)) }()
		time.Sleep(time.Until(start.Add(time.Second + window + time.Microsecond)))
		marks := &lastMarks{Backend: h.mem, table: catalog.Naming{Store: storeName}.ViewTable("site_totals", 1)}
		if err := deliverBy(t.Context(), h.core(marks), h.batch(t, "shardId-T", 2), time.Now().Add(lifetime)); err != nil {
			t.Fatal(err)
		}
		if err := <-again; err != nil {
			t.Fatal(err)
		}
		if got, s, tm := h.views(t), marks.has("shardId-S"), marks.has("shardId-T"); got != "2" || s || !tm {
			t.Errorf("site_totals s0 counts %s views; T's write kept S's mark: %v, its own: %v; want 2, false, true", got, s, tm)
		}
	})
}

// lateProgressStory runs TestLateProgress's invocations, J's progress write landing late
// after J's deadline and I living lifetime, and returns I's error.
func lateProgressStory(t *testing.T, h *handoff, late, lifetimeI time.Duration) error {
	t.Helper()
	b := h.batch(t, "shardId-S", 1)
	deadlineJ := time.Now().Add(lifetime)
	j := &lateProgress{Backend: h.mem, land: deadlineJ.Add(late), landed: make(chan error, 1)}
	if err := deliverBy(t.Context(), h.core(j), b, deadlineJ); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("J: %v, want a timeout", err)
	}
	time.Sleep(time.Until(j.land.Add(-time.Microsecond)))
	i := make(chan error, 1)
	go func() { i <- deliverBy(t.Context(), h.core(lastInstant{h.mem}), b, time.Now().Add(lifetimeI)) }()
	time.Sleep(time.Until(deadlineJ.Add(window + time.Microsecond)))
	if err := deliverBy(t.Context(), h.core(h.mem), h.batch(t, "shardId-T", 2), time.Now().Add(lifetime)); err != nil {
		t.Fatal(err)
	}
	if err := <-j.landed; err != nil {
		t.Fatalf("J's progress write: %v", err)
	}
	return <-i
}

// lastMarks records the high-water marks of the last row item put to table.
type lastMarks struct {
	store.Backend

	table string
	mu    sync.Mutex
	marks map[string]store.Value
}

func (l *lastMarks) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	err := l.Backend.Put(ctx, table, it, c)
	if hwm, ok := it[catalog.AttrHighWaterMarks]; ok && err == nil && table == l.table {
		l.mu.Lock()
		l.marks = hwm.Map
		l.mu.Unlock()
	}
	return err
}

// has reports whether the last row item holds unit's mark.
func (l *lastMarks) has(unit string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.marks[keys.UnitKey(unit)]
	return ok
}

// lateProgress lands a unit's progress write at land, after its caller timed out.
type lateProgress struct {
	store.Backend

	land   time.Time
	landed chan error // the late write's result
}

func (l *lateProgress) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if _, ok := it["progress"]; !ok || table != metaTable() {
		return l.Backend.Put(ctx, table, it, c)
	}
	go func() {
		time.Sleep(time.Until(l.land))
		l.landed <- l.Backend.Put(context.WithoutCancel(ctx), table, it, c)
	}()
	<-ctx.Done()
	return ctx.Err()
}

// lastInstant sends each request to a view table a microsecond before its caller's
// deadline: an invocation that stalled, then wrote at the last instant it could.
type lastInstant struct{ store.Backend }

func (l lastInstant) wait(ctx context.Context, table string) {
	if table != metaTable() {
		deadline, _ := ctx.Deadline()
		time.Sleep(time.Until(deadline.Add(-time.Microsecond)))
	}
}

func (l lastInstant) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	l.wait(ctx, table)
	return l.Backend.Get(ctx, table, k, consistent)
}

func (l lastInstant) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	l.wait(ctx, table)
	return l.Backend.BatchGet(ctx, table, ks, consistent)
}

func (l lastInstant) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	l.wait(ctx, table)
	return l.Backend.Put(ctx, table, it, c)
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// The test store has the default prune window, 960 s, so an invocation may live 900 s.
const (
	window   = 960 * time.Second
	lifetime = 900 * time.Second
)

// TestHandleLifetime: an invocation may live W − 60 s; one a microsecond longer is refused
// before any request.
func TestHandleLifetime(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		left time.Duration // until the deadline
		want error
		row  string
	}{
		{"W − 60 s", lifetime, nil, "/a 100 1.00 100 100 1 1"},
		{"a microsecond more", lifetime + time.Microsecond, projector.ErrLifetime, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				calls := &callCount{}
				f := newFixture(t, func(b store.Backend) store.Backend { calls.Backend = b; return calls })
				click := f.click(t, "u1", "/a", 100)
				ctx, cancel := context.WithTimeout(t.Context(), tt.left)
				defer cancel()
				_, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{insert(1, click)}})
				if !errors.Is(err, tt.want) {
					t.Errorf("Handle: %v, want %v", err, tt.want)
				}
				if n := calls.n.Load(); err != nil && n != 0 {
					t.Errorf("a refused batch made %d store calls, want 0", n)
				}
				if got := f.row(t, "page_totals@v1", page("/a")); got != tt.row {
					t.Errorf("page_totals /a is %q, want %q", got, tt.row)
				}
			})
		})
	}
}

// TestHandleProgressAt: progress is stamped with the invocation's deadline, rounded up to the
// microsecond, not with the earlier cutoff of the core's sends.
func TestHandleProgressAt(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		click := f.click(t, "u1", "/a", 100)
		ctx, cancel := context.WithDeadline(t.Context(), start.Add(time.Minute+time.Nanosecond))
		defer cancel()
		if _, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{insert(1, click)}}); err != nil {
			t.Fatal(err)
		}
		if got, want := f.unit(t).ProgressAt, start.Add(time.Minute+time.Microsecond); !got.Equal(want) {
			t.Errorf("progress_at %v, want %v", got, want)
		}
	})
}

// TestHandleStopsSendingAtTheCutoff: the core sends nothing after the deadline less a reserve
// of min(2 s, a tenth of the time left). A row that never answers ends half a reserve before
// that cutoff, and the progress write still checkpoints the row written below it, so Handle
// replies with FailedSeq with the reserve to spare.
func TestHandleStopsSendingAtTheCutoff(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name               string
		left, rows, cutoff time.Duration
	}{
		{"5 s", 5 * time.Second, 4250 * time.Millisecond, 4500 * time.Millisecond},
		{"a minute", time.Minute, 57 * time.Second, 58 * time.Second},
		{"W − 60 s", lifetime, 897 * time.Second, 898 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				hung := &hungPut{table: "app-view-page_keys-v1", pk: "/b"}
				f := newFixture(t, func(b store.Backend) store.Backend { hung.Backend = b; return hung })
				a, b := f.click(t, "u1", "/a", 100), f.click(t, "u2", "/b", 100)
				ctx, cancel := context.WithTimeout(t.Context(), tt.left)
				defer cancel()
				res, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{insert(1, a), insert(2, b)}})
				if took := time.Since(start); took != tt.rows || err != nil || res.FailedSeq != seq(2) {
					t.Errorf("Handle = %+v, %v after %v; want FailedSeq %q after %v", res, err, took, seq(2), tt.rows)
				}
				if last := hung.last(); !last.Before(start.Add(tt.cutoff)) {
					t.Errorf("a request was sent %v after the start, at or after the cutoff", last.Sub(start))
				}
				if got, want := f.unit(t).Progress, padSeq(t, seq(1)); got != want {
					t.Errorf("progress %q, want %q", got, want)
				}
			})
		})
	}
}

// TestHandleSkipsWhatProgressCovers: once unit A's progress rose more than W ago, B's write
// drops A's mark from the row, and a late redelivery of A's record counts nothing.
func TestHandleSkipsWhatProgressCovers(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		a, b := f.click(t, "u1", "/a", 100), f.click(t, "u2", "/a", 200)
		f.handleUnit(t, "shardId-A", insert(1, a))
		time.Sleep(time.Minute + window + time.Microsecond) // past A's deadline, then W
		f.handleUnit(t, "shardId-B", insert(2, b))
		if got, want := f.marks(t, "/a"), []string{keys.UnitKey("shardId-B")}; !slices.Equal(got, want) {
			t.Errorf("page_totals /a holds marks %v, want %v", got, want)
		}
		f.handleUnit(t, "shardId-A", insert(1, a))
		if got, want := f.row(t, "page_totals@v1", page("/a")), "/a 200 3.00 100 300 2 2"; got != want {
			t.Errorf("page_totals /a is %q, want %q", got, want)
		}
	})
}

// handleUnit delivers recs as one batch of unit u in an invocation of a minute.
func (f *fixture) handleUnit(t *testing.T, u string, recs ...projector.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if res, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: u, Records: recs}); err != nil || res.FailedSeq != "" {
		t.Fatalf("Handle of %s = %+v, %v", u, res, err)
	}
}

// marks returns the unit keys of the high-water marks of page_totals' row for page p.
func (f *fixture) marks(t *testing.T, p string) []string {
	t.Helper()
	it, err := f.mem.Get(t.Context(), pageTotalsTable, store.Key{PK: p, SK: keys.NoSortKey}, true)
	if err != nil {
		t.Fatal(err)
	}
	h, err := keys.DecodeHWM(it[catalog.AttrHighWaterMarks], true)
	if err != nil {
		t.Fatal(err)
	}
	return slices.Sorted(maps.Keys(h.Units))
}

// hungPut never answers a write of the row of table whose pk is pk: the call ends with its
// context, as the SDK's does on a hung server. It keeps when it was last sent a request, one
// with a live context.
type hungPut struct {
	store.Backend

	table, pk string
	mu        sync.Mutex
	lastSent  time.Time
}

func (h *hungPut) send(ctx context.Context) {
	if ctx.Err() == nil {
		h.mu.Lock()
		h.lastSent = time.Now()
		h.mu.Unlock()
	}
}

func (h *hungPut) last() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastSent
}

func (h *hungPut) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	h.send(ctx)
	return h.Backend.Get(ctx, table, k, consistent)
}

func (h *hungPut) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	h.send(ctx)
	return h.Backend.BatchGet(ctx, table, ks, consistent)
}

func (h *hungPut) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	h.send(ctx)
	return h.Backend.Query(ctx, table, q)
}

func (h *hungPut) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	h.send(ctx)
	if table == h.table && it[catalog.AttrPK].Str == h.pk {
		<-ctx.Done()
		return ctx.Err()
	}
	return h.Backend.Put(ctx, table, it, c)
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"context"
	"errors"
	"maps"
	"slices"
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

func TestHandleRefusesInvocationThatOutlivesWindow(t *testing.T) {
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

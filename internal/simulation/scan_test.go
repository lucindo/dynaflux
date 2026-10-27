// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
)

// TestScanPages: each ScanPage is a request of its own, refused once the invocation's
// deadline has passed.
func TestScanPages(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		slow     bool
		want     []string
		requests int64 // the append, three puts and the pages
		killed   int64
	}{
		{"three pages of one item, then an empty one", false, []string{"a", "b", "c"}, 1 + 3 + 4, 0},
		{"the second page after the deadline", true, []string{"a"}, 1 + 3 + 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := &scanReader{slow: tc.slow}
			out := simulation.Run(t, oneEvent(simulation.Faults{}), newScripted(t, r.script), 1)
			if len(out.Violations) > 0 {
				t.Fatalf("violations: %v", out.Violations)
			}
			if !reflect.DeepEqual(r.got, tc.want) || r.err != nil {
				t.Errorf("read %v, %v; want %v, no error", r.got, r.err, tc.want)
			}
			if out.Stats.Requests != tc.requests || out.Stats.Killed != tc.killed {
				t.Errorf("%d requests, %d refused; want %d, %d", out.Stats.Requests, out.Stats.Killed, tc.requests, tc.killed)
			}
		})
	}
}

// scanReader's first invocation writes three items to one partition, then scans the
// table a page at a time; when slow, it waits for its deadline after each page.
type scanReader struct {
	slow bool
	got  []string
	err  error
}

func (r *scanReader) script(ctx context.Context, b store.Backend, n int64) error {
	if n > 1 {
		return nil
	}
	for _, sk := range []string{"a", "b", "c"} {
		if err := b.Put(ctx, viewTable, store.Item{catalog.AttrPK: str("q"), catalog.AttrSK: str(sk)}, store.Cond{}); err != nil {
			return err
		}
	}
	s := store.Scan{Segments: 1, Consistent: true, PageSize: 1}
	var start *store.Key
	for {
		var page []store.Item
		if page, start, r.err = b.ScanPage(ctx, viewTable, s, start); r.err != nil {
			return r.err
		}
		for _, it := range page {
			r.got = append(r.got, it[catalog.AttrSK].Str)
		}
		if start == nil {
			return nil
		}
		if deadline, _ := ctx.Deadline(); r.slow {
			time.Sleep(time.Until(deadline))
		}
	}
}

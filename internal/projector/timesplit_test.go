// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// activateByTime activates version with the time split: split_at is 6.5 minutes later,
// split_late 26.5.
func (f *fixture) activateByTime(t *testing.T, version string) meta.ViewState {
	t.Helper()
	vs, err := f.meta.Activate(t.Context(), version, "clicks", meta.SplitTime, meta.DefaultBounds)
	if err != nil {
		t.Fatal(err)
	}
	return vs
}

// withIngest returns a copy of it with ingest_time at.
func withIngest(it store.Item, at time.Time) store.Item {
	out := maps.Clone(it)
	out[catalog.AttrIngestTime] = store.Value{Type: store.N, Str: strconv.FormatInt(at.UnixMicro(), 10)}
	return out
}

func eventID(t *testing.T, it store.Item) string {
	t.Helper()
	_, id, err := keys.ParseStreamSK(it[catalog.AttrSK].Str)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// viewState returns version's stored state.
func (f *fixture) viewState(t *testing.T, version string) meta.ViewState {
	t.Helper()
	states, err := f.meta.ViewStates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return states[version]
}

// Live projection takes an event whose ingest_time is after the split time, and leaves the
// rest to the backfill. It fences nothing for the version.
func TestHandleAppliesATimeSplitVersionAfterTheSplitTime(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		vs := f.activateByTime(t, pageTotals)
		before := f.click(t, "u1", "/a", 100)
		time.Sleep(time.Until(vs.SplitAt))
		at := f.click(t, "u2", "/a", 200)
		time.Sleep(time.Microsecond)
		after := f.click(t, "u3", "/a", 400)
		f.handle(t, insert(1, before), insert(2, at), insert(3, after))
		if got, want := f.row(t, pageTotals, page("/a")), "/a 400 4.00 400 400 1 1"; got != want {
			t.Errorf("page_totals /a is %q, want %q: only the event after the split time", got, want)
		}
		u := f.unit(t)
		if _, fenced := u.Fences[pageTotals]; fenced || u.Progress != padSeq(t, seq(3)) {
			t.Errorf("progress %s, fences %v; want %s and none of %s", u.Progress, u.Fences, padSeq(t, seq(3)), pageTotals)
		}
	})
}

// An event at or before the split time in a batch that started at or after the late bound
// may have landed after the backfill's read: it opens the breaker with cause split. A
// microsecond earlier, it is the backfill's.
func TestHandleBreaksATimeSplitVersionOnALateEvent(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		vs := f.activateByTime(t, pageTotals)
		early, late := f.click(t, "u1", "/a", 100), f.click(t, "u2", "/a", 200)
		time.Sleep(time.Until(vs.SplitLate) - time.Microsecond)
		if res := f.handle(t, insert(1, early)); len(res.Stats.Broken) > 0 {
			t.Fatalf("a batch a microsecond before the late bound broke %v", res.Stats.Broken)
		}
		time.Sleep(time.Microsecond)
		res := f.handle(t, insert(2, late))
		got := f.viewState(t, pageTotals)
		want := meta.Failure{
			Cause: meta.CauseSplit, Unit: unit, Seq: padSeq(t, seq(2)), EventID: eventID(t, late),
			Error: "ingest_time 2000-01-01T00:00:00.000001Z is at or before the split time 2000-01-01T00:06:30Z, " +
				"but the batch started at 2000-01-01T00:26:30Z, at or after the late bound 2000-01-01T00:26:30Z",
			At: vs.SplitLate,
		}
		if !slices.Equal(res.Stats.Broken, []string{pageTotals}) || got.State != meta.Broken || got.Failure == nil || *got.Failure != want {
			t.Errorf("broke %v; %s is %s with %+v, want broken with %+v", res.Stats.Broken, pageTotals, got.State, got.Failure, want)
		}
		if got := f.row(t, pageTotals, page("/a")); got != "" {
			t.Errorf("page_totals /a is %q, want no row", got)
		}
	})
}

// A MODIFY whose images straddle the split time opens the breaker with cause split, in a
// batch of its own or after an INSERT; one on either side of the split time doesn't.
func TestHandleBreaksATimeSplitVersionOnAStraddlingModify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		from, to time.Duration // the images' ingest_time, from the split time
		insert   bool
		broken   bool
	}{
		{"alone, across the split time", 0, time.Microsecond, false, true},
		{"after an INSERT, across the split time", 0, time.Microsecond, true, true},
		{"at or before the split time", -time.Minute, 0, false, false},
		{"after the split time", time.Microsecond, time.Minute, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newPendingFixture(t, nil)
				vs := f.activateByTime(t, pageTotals)
				old := f.click(t, "u1", "/a", 100)
				recs := []projector.Record{{Op: projector.Modify, Seq: seq(2), Old: withIngest(old, vs.SplitAt.Add(tt.from)), New: withIngest(old, vs.SplitAt.Add(tt.to))}}
				if tt.insert {
					recs = append([]projector.Record{insert(1, f.click(t, "u2", "/b", 100))}, recs...)
				}
				res := f.handle(t, recs...)
				got := f.viewState(t, pageTotals)
				if !tt.broken {
					if got.State != meta.Activating || len(res.Stats.Broken) > 0 {
						t.Errorf("%s is %s, broke %v; want activating", pageTotals, got.State, res.Stats.Broken)
					}
					return
				}
				want := fmt.Sprintf("%s %s %s %s", meta.CauseSplit, unit, padSeq(t, seq(2)), eventID(t, old))
				if got.State != meta.Broken || got.Failure == nil ||
					fmt.Sprintf("%s %s %s %s", got.Failure.Cause, got.Failure.Unit, got.Failure.Seq, got.Failure.EventID) != want {
					t.Errorf("%s is %s with %+v, want broken by %s", pageTotals, got.State, got.Failure, want)
				}
			})
		})
	}
}

// tableChunk returns items as a backfill reads them from the stream table: by time mark, with
// neither unit nor sequence number.
func tableChunk(t *testing.T, items ...store.Item) []projector.BackfillRecord {
	t.Helper()
	var chunk []projector.BackfillRecord
	for _, it := range items {
		et, id, err := keys.ParseStreamSK(it[catalog.AttrSK].Str)
		if err != nil {
			t.Fatal(err)
		}
		m, err := keys.TimeMark(et, id)
		if err != nil {
			t.Fatal(err)
		}
		chunk = append(chunk, projector.BackfillRecord{Item: it, Mark: m})
	}
	slices.SortFunc(chunk, func(a, b projector.BackfillRecord) int { return a.Mark.Compare(b.Mark) })
	return chunk
}

// A table item has no unit: its time mark orders it and keeps a repeated chunk from counting
// it again, and its SPLIT item is bucket(event_id, n), wherever a worker writes it.
func TestApplyBackfillTakesTableItems(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		f.activateByTime(t, "page_days@v1")
		var items []store.Item
		for i := range 4 {
			items = append(items, f.click(t, "u"+strconv.Itoa(i), "/a", 100))
		}
		chunk := tableChunk(t, items...)
		for _, c := range [][]projector.BackfillRecord{chunk[:3], chunk[1:], chunk} {
			if err := f.applyBackfill(t, "page_days@v1", c); err != nil {
				t.Fatal(err)
			}
		}
		if got, want := f.row(t, "page_days@v1", page("/a"), expr.Micros(start.UnixMicro())), "/a 2000-01-01 4 4"; got != want {
			t.Errorf("page_days /a is %q, want %q", got, want)
		}
		var want []string
		for _, it := range items {
			want = append(want, fmt.Sprintf("p%02d", keys.Bucket(eventID(t, it), 2)))
		}
		slices.Sort(want)
		if got := f.splitItems(t, "app-view-page_days-v1"); !slices.Equal(got, slices.Compact(want)) {
			t.Errorf("SPLIT items %q, want %q: bucket(event_id, 2) of each item", got, slices.Compact(want))
		}
	})
}

// inFlight is a backend that counts the view rows read at once, each read taking a
// millisecond of the bubble's clock.
type inFlight struct {
	store.Backend

	mu        sync.Mutex
	now, most int
}

func (b *inFlight) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	if strings.Contains(table, "-view-") {
		b.mu.Lock()
		b.now++
		b.most = max(b.most, b.now)
		b.mu.Unlock()
		time.Sleep(time.Millisecond)
		b.mu.Lock()
		b.now--
		b.mu.Unlock()
	}
	return b.Backend.Get(ctx, table, k, consistent)
}

// A backfill chunk writes Config.BackfillRows rows at once, 16 by default.
func TestApplyBackfillRows(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ rows, want int }{{0, 16}, {64, 64}} {
		synctest.Test(t, func(t *testing.T) {
			f := newPendingFixture(t, nil)
			f.activateByTime(t, "page_days@v1")
			var items []store.Item
			for i := range 100 {
				items = append(items, f.click(t, "u1", "/p"+strconv.Itoa(i), 100))
			}
			b := &inFlight{Backend: f.mem}
			core := projector.New(f.cat.Projector(), b, nil, projector.Config{Naming: catalog.Naming{Store: "app"}, BackfillRows: tt.rows})
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			if err := core.ApplyBackfill(ctx, "page_days@v1", tableChunk(t, items...)); err != nil {
				t.Fatal(err)
			}
			if b.most != tt.want {
				t.Errorf("BackfillRows %d: %d rows read at once, want %d", tt.rows, b.most, tt.want)
			}
		})
	}
}

// splitItems returns the SPLIT suffixes of the row items of a view table, sorted.
func (f *fixture) splitItems(t *testing.T, table string) []string {
	t.Helper()
	items, next, err := f.mem.ScanPage(t.Context(), table, store.Scan{Segments: 1}, nil)
	if err != nil || next != nil {
		t.Fatalf("scan %s: %v, more: %v", table, err, next != nil)
	}
	var out []string
	for _, it := range items {
		if _, row := it[catalog.AttrHighWaterMarks]; row {
			sk := it[catalog.AttrSK].Str
			out = append(out, sk[len(sk)-3:])
		}
	}
	slices.Sort(out)
	return out
}

// A table item that fails its evaluation breaks the version, which names it by its event_id
// alone.
func TestApplyBackfillBreaksOnATableItem(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		f.activateByTime(t, "page_rates@v1")
		poison := f.click(t, "u1", "/a", 0) // page_rates divides by ms
		err := f.applyBackfill(t, "page_rates@v1", tableChunk(t, poison))
		got := f.viewState(t, "page_rates@v1")
		wantErr := "projector: backfill of page_rates@v1: event " + eventID(t, poison) + ": " + projector.ErrBroken.Error()
		if err == nil || err.Error() != wantErr || !errors.Is(err, projector.ErrBroken) {
			t.Errorf("ApplyBackfill = %v, want %q", err, wantErr)
		}
		if f := got.Failure; got.State != meta.Broken || f == nil || f.Cause != meta.CauseRecord || f.Unit != "" || f.Seq != "" || f.EventID != eventID(t, poison) {
			t.Errorf("page_rates is %s with %+v, want broken by event %s alone", got.State, f, eventID(t, poison))
		}
	})
}

func TestApplyBackfillRejectsATableItemWithAUnit(t *testing.T) {
	t.Parallel()
	f := newPendingFixture(t, nil)
	chunk := tableChunk(t, f.click(t, "u1", "/a", 100))
	chunk[0].Unit = unit
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err := f.core.ApplyBackfill(ctx, pageTotals, chunk); err == nil {
		t.Error("ApplyBackfill took a table item with a unit")
	}
}

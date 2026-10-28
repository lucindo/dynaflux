// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// syncViews is the store of the SYNC tests: the clicks stream of views, with SYNC views.
const syncViews = `
CREATE STREAM clicks (
    user_id String,
    page    String,
    ms      UInt32,
    price   Decimal(10, 2),
    tags    Array(String)
)
PRIMARY KEY (user_id)
ACCEPT event_time BETWEEN now() - INTERVAL 30 DAY AND now() + INTERVAL 5 MINUTE;

CREATE MATERIALIZED VIEW page_totals
MODE SYNC
ORDER BY (page)
AS SELECT page, count() AS views, sum(ms) AS total_ms, uniqExact(user_id) AS users,
    uniqExactIf(user_id, ms > 150) AS slow_users, max(ms) AS longest, min(ms) AS shortest,
    sum(price) AS revenue, avg(ms) AS mean_ms, argMax(user_id, ms) AS slowest,
    any(user_id) AS first_user, uniq(user_id) AS visitors, quantile(0.5)(ms) AS median_ms
FROM clicks
GROUP BY page;

CREATE MATERIALIZED VIEW page_users
MODE SYNC
ORDER BY (page)
AS SELECT page, uniqExact(user_id) AS users
FROM clicks
GROUP BY page;

CREATE MATERIALIZED VIEW page_days
MODE SYNC
ORDER BY (page, day)
PRIMARY KEY (page)
TTL day + INTERVAL 1 DAY
AS SELECT page, toStartOfDay(event_time) AS day, count() AS views
FROM clicks
WHERE ms > 0
GROUP BY page, day;

CREATE MATERIALIZED VIEW page_rates
MODE SYNC
ORDER BY (page)
AS SELECT page, sum(intDiv(1000, ms)) AS rate
FROM clicks
GROUP BY page;

CREATE MATERIALIZED VIEW page_keys
ORDER BY (page)
AS SELECT page
FROM clicks
GROUP BY page;
`

// syncVersions are the SYNC versions of syncViews.
var syncVersions = []string{"page_days@v1", "page_rates@v1", "page_totals@v1", "page_users@v1"} //nolint:gochecknoglobals // a constant list

// withSync returns a copy of it, an event, whose _sync names versions and whose _r is r.
func withSync(it store.Item, r string, versions ...string) store.Item {
	out := maps.Clone(it)
	out[catalog.AttrOriginRegion] = store.Value{Type: store.S, Str: r}
	list := store.Value{Type: store.L, List: []store.Value{}}
	for _, v := range versions {
		list.List = append(list.List, store.Value{Type: store.S, Str: v})
	}
	out[catalog.AttrSync] = list
	return out
}

// TestHandleSkipsTheVersionsItsAppendApplied: only a record of the core's region has the
// versions its _sync names skipped; unknown IDs match nothing.
func TestHandleSkipsTheVersionsItsAppendApplied(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		region   string
		versions []string
		want     []string // page_totals and page_keys
	}{
		{"this region's", region, []string{"page_totals@v1", "page_totals@v9", "gone@v1"}, []string{"", "/a"}},
		{"another region's", "eu-west-1", []string{"page_totals@v1"}, []string{"/a 100 1.00 100 100 1 1", "/a"}},
		{"an empty list", region, nil, []string{"/a 100 1.00 100 100 1 1", "/a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, nil)
				res := f.handle(t, insert(1, withSync(f.click(t, "u1", "/a", 100), tt.region, tt.versions...)))
				got := []string{f.row(t, pageTotals, page("/a")), f.row(t, "page_keys@v1", page("/a"))}
				if !slices.Equal(got, tt.want) || len(res.Stats.Broken) > 0 || f.unit(t).Progress != padSeq(t, seq(1)) {
					t.Errorf("rows %q, broken %v, progress %s; want %q, none broken, progress at the record",
						got, res.Stats.Broken, f.unit(t).Progress, tt.want)
				}
			})
		})
	}
}

// An event that its append applied to a version split by time doesn't open the version's
// breaker when it is at or before the split time and comes after the late bound: a writer
// clock off by more than ε doesn't matter to it.
func TestHandleSkipsAnAppliedVersionBeforeItsTimeSplit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		vs := f.activateByTime(t, pageTotals)
		late := withSync(f.click(t, "u1", "/a", 100), region, pageTotals)
		time.Sleep(time.Until(vs.SplitLate))
		res := f.handle(t, insert(1, late))
		if state := f.viewState(t, pageTotals).State; len(res.Stats.Broken) > 0 || state != meta.Activating {
			t.Errorf("broke %v, %s is %s; want nothing broken and activating", res.Stats.Broken, pageTotals, state)
		}
	})
}

// A malformed _sync opens the breaker of every version the record reaches, ASYNC ones
// included, since only the writer knows what it applied; in another region it is ignored.
func TestHandleBreaksOnAMalformedSync(t *testing.T) {
	t.Parallel()
	every := []string{"page_days@v1", "page_keys@v1", "page_rates@v1", "page_totals@v1", "page_users@v1",
		"page_visitors@v1", "slow_pages@v1", "user_tags@v1"}
	tests := []struct {
		name   string
		region string
		sync   store.Value
		broken []string
	}{
		{"a string", region, store.Value{Type: store.S, Str: pageTotals}, every},
		{"a list with a number", region, store.Value{Type: store.L, List: []store.Value{{Type: store.N, Str: "1"}}}, every},
		{"another region's", "eu-west-1", store.Value{Type: store.S, Str: pageTotals}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, nil)
				good, bad := f.click(t, "u1", "/a", 100), withSync(f.click(t, "u2", "/a", 100), tt.region)
				bad[catalog.AttrSync] = tt.sync
				res := f.handle(t, insert(1, good), insert(2, bad))
				if got := f.failedAt(t); !slices.Equal(res.Stats.Broken, tt.broken) || !slices.Equal(got, tt.broken) {
					t.Errorf("broke %v, failed at record 2: %v; want %v", res.Stats.Broken, got, tt.broken)
				}
				if tt.broken != nil {
					want := meta.Failure{Cause: meta.CauseRecord, Unit: unit, Seq: padSeq(t, seq(2)), EventID: eventID(t, bad),
						Error: "_sync is not a list of strings", At: start}
					if got := f.viewState(t, pageTotals).Failure; got == nil || *got != want {
						t.Errorf("%s failed with %+v, want %+v", pageTotals, got, want)
					}
				}
			})
		})
	}
}

func TestHandleNeedsARegion(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		core := projector.New(f.cat.Projector(), f.mem, nil, projector.Config{Naming: catalog.Naming{Store: "app"}})
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		_, err := core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{insert(1, f.click(t, "u1", "/a", 100))}})
		want := "projector: a batch needs a delivery unit, a context with a deadline and a core with a region"
		if err == nil || err.Error() != want {
			t.Errorf("Handle without a region: %v, want %q", err, want)
		}
	})
}

// conflicts fails the first n writes to table whose partition key is or isn't a marker's
// with store.ErrConflict, after applying them when applied is set. It records when each
// write to that table came.
type conflicts struct {
	store.Backend

	table   string
	marker  bool
	n       int
	applied bool

	mu    sync.Mutex
	times []time.Time
}

func (c *conflicts) Put(ctx context.Context, table string, it store.Item, cond store.Cond) error {
	if table != c.table || (it[catalog.AttrPK].Str[0] == '!') != c.marker {
		return c.Backend.Put(ctx, table, it, cond)
	}
	c.mu.Lock()
	c.times = append(c.times, time.Now())
	hit := len(c.times) <= c.n
	c.mu.Unlock()
	if !hit {
		return c.Backend.Put(ctx, table, it, cond)
	}
	if c.applied {
		if err := c.Backend.Put(ctx, table, it, cond); err != nil {
			return err
		}
	}
	return store.ErrConflict
}

// A row write that meets a transaction is retried in place, after reading the row again:
// up to 4 times, each 5 to 50 ms after the last. A write that landed before its caller saw
// the conflict counts once.
func TestHandleRetriesARowThatMeetsATransaction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		marker  bool
		n       int
		applied bool
		want    string // page_totals /a
		failed  bool
	}{
		{"4 conflicts", false, 4, false, "/a 200 3.00 100 300 2 2", false},
		{"5 conflicts", false, 5, false, "", true},
		{"a conflict after the write", false, 1, true, "/a 200 3.00 100 300 2 2", false},
		{"a marker's conflict", true, 2, false, "/a 200 3.00 100 300 2 2", false},
		{"a marker's conflict after the write", true, 2, true, "/a 200 3.00 100 300 2 2", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := &conflicts{table: pageTotalsTable, marker: tt.marker, n: tt.n, applied: tt.applied}
				f := newFixture(t, func(b store.Backend) store.Backend { c.Backend = b; return c })
				res := f.handle(t, insert(1, f.click(t, "u1", "/a", 100)), insert(2, f.click(t, "u2", "/a", 200)))
				got := f.row(t, pageTotals, page("/a"))
				if got != tt.want || (res.FailedSeq != "") != tt.failed || tt.failed && !errors.Is(res.RowErr, store.ErrConflict) {
					t.Errorf("page_totals /a is %q with FailedSeq %q (%v), want %q, failed %v", got, res.FailedSeq, res.RowErr, tt.want, tt.failed)
				}
				for i := 1; i < min(len(c.times), tt.n+1); i++ {
					if wait := c.times[i].Sub(c.times[i-1]); wait < 5*time.Millisecond || wait > 50*time.Millisecond {
						t.Errorf("write %d came %v after the one before, want 5 to 50 ms", i+1, wait)
					}
				}
			})
		})
	}
}

// newSyncFixture is newFixture of syncViews.
func newSyncFixture(t *testing.T) *fixture {
	t.Helper()
	f := newStoreFixture(t, syncViews, nil)
	if err := f.meta.SetLive(t.Context(), f.cat.Catalog()); err != nil {
		t.Fatal(err)
	}
	return f
}

// plan returns the SYNC plan of it, an event of clicks, for versions at now.
func (f *fixture) plan(t *testing.T, it store.Item, now time.Time, versions ...string) *projector.SyncPlan {
	t.Helper()
	p, err := f.cat.Projector().PlanSync(catalog.Naming{Store: "app"}, "clicks", it, versions, now)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// transact writes it, an event of clicks, as a SYNC append that applies versions does at
// now: in one transaction, with the puts of its plan from what the store holds. It returns
// the plan's puts.
func (f *fixture) transact(t *testing.T, it store.Item, now time.Time, versions ...string) []store.TxPut {
	t.Helper()
	ctx := t.Context()
	p := f.plan(t, it, now, versions...)
	read := map[projector.TableKey]store.Item{}
	for _, tk := range p.Reads() {
		items, err := f.mem.BatchGet(ctx, tk.Table, tk.Keys, true)
		if err != nil {
			t.Fatal(err)
		}
		for i, k := range tk.Keys {
			if items[i] != nil {
				read[projector.TableKey{Table: tk.Table, Key: k}] = items[i]
			}
		}
	}
	puts, err := p.Puts(read)
	if err != nil {
		t.Fatal(err)
	}
	event := store.TxPut{Table: clicksTable, Item: withSync(it, region, versions...), Cond: store.IfAbsent()}
	if err = f.mem.Transact(ctx, append([]store.TxPut{event}, puts...)); err != nil {
		t.Fatal(err)
	}
	return puts
}

// A SYNC append's puts leave the rows that live projection leaves, event after event.
func TestSyncPlanWritesTheRowsHandleWrites(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		live, applied := newSyncFixture(t), newSyncFixture(t)
		events := []store.Item{live.click(t, "u1", "/a", 100), live.click(t, "u2", "/a", 300), live.click(t, "u1", "/a", 200)}
		live.handle(t, insert(1, events[0]), insert(2, events[1]), insert(3, events[2]))
		for _, it := range events {
			applied.transact(t, it, start, syncVersions...)
		}
		day := expr.Micros(start.UnixMicro())
		want := []string{
			"/a u1 300 200 199.85175265620114 6.00 100 2 u2 600 2 3 2",
			"/a 2000-01-01 3",
			"/a 18",
			"/a 2",
		}
		for i, f := range []*fixture{live, applied} {
			got := []string{
				f.row(t, "page_totals@v1", page("/a")),
				f.row(t, "page_days@v1", page("/a"), day),
				f.row(t, "page_rates@v1", page("/a")),
				f.row(t, "page_users@v1", page("/a")),
			}
			if !slices.Equal(got, want) {
				t.Errorf("rows by %s: %q, want %q", []string{"Handle", "SyncPlan"}[i], got, want)
			}
		}
	})
}

// The row put keeps _hwm, the backfill's mark included, raises _v, and holds when _v is
// still the one read; the marker put is owned by the event and holds when it is absent.
func TestSyncPlanKeepsHighWaterMarks(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f, src := newSyncFixture(t), newSyncFixture(t)
		backfilled, delivered, appended := src.click(t, "u1", "/a", 100), src.click(t, "u2", "/a", 100), src.click(t, "u3", "/a", 100)
		if err := f.applyBackfill(t, "page_users@v1", []projector.BackfillRecord{{Unit: "shardId-0", Seq: seq(1), Item: backfilled}}); err != nil {
			t.Fatal(err)
		}
		f.handle(t, insert(2, delivered))
		row := f.plan(t, appended, start, "page_users@v1").Reads()[0]
		before, err := f.mem.Get(t.Context(), row.Table, row.Keys[0], true)
		if err != nil {
			t.Fatal(err)
		}
		puts := f.transact(t, appended, start, "page_users@v1")
		after, err := f.mem.Get(t.Context(), row.Table, row.Keys[0], true)
		if err != nil {
			t.Fatal(err)
		}
		if len(before[catalog.AttrHighWaterMarks].Map) != 2 || !reflect.DeepEqual(after[catalog.AttrHighWaterMarks], before[catalog.AttrHighWaterMarks]) {
			t.Errorf("_hwm %+v, then %+v; want a unit's entry and B, kept", before[catalog.AttrHighWaterMarks], after[catalog.AttrHighWaterMarks])
		}
		if v := after[catalog.AttrRowVersion].Str; before[catalog.AttrRowVersion].Str != "2" || v != "3" {
			t.Errorf("_v %s, then %s; want 2, then 3", before[catalog.AttrRowVersion].Str, v)
		}
		if got := f.row(t, "page_users@v1", page("/a")); got != "/a 3" {
			t.Errorf("page_users /a is %q, want %q", got, "/a 3")
		}
		conds := []store.Cond{store.IfVersion(2), store.IfAbsent()}
		owner := "E" + eventID(t, appended)
		if len(puts) != 2 || puts[0].Cond != conds[0] || puts[1].Cond != conds[1] || puts[1].Item[catalog.AttrMarkerOwner].Str != owner {
			t.Errorf("puts %+v, want the row's if _v is 2 and the marker's if absent, owned by %s", puts, owner)
		}
	})
}

// A value counts only when its marker is absent: a present marker never counts, whoever
// owns it, the event itself included, as when a DEDUP BY event is appended again after its
// item expired.
func TestSyncPlanCountsOnlyAbsentMarkers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		owner func(event string) string // "" for no marker
		want  string
	}{
		{"absent", func(string) string { return "" }, "/a 1"},
		{"live projection's", func(string) string { return "L" + "0000000000000" + seq(1) }, "/a 0"},
		{"another event's", func(string) string { return "E01ARZ3NDEKTSV4RRFFQ69G5FAV" }, "/a 0"},
		{"this event's", func(event string) string { return "E" + event }, "/a 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f, src := newSyncFixture(t), newSyncFixture(t)
				it := src.click(t, "u1", "/a", 100)
				if o := tt.owner(eventID(t, it)); o != "" {
					tk := f.plan(t, it, start, "page_users@v1").Reads()[0]
					marker := store.Item{
						catalog.AttrPK:          {Type: store.S, Str: tk.Keys[1].PK},
						catalog.AttrSK:          {Type: store.S, Str: tk.Keys[1].SK},
						catalog.AttrMarkerOwner: {Type: store.S, Str: o},
					}
					if err := f.mem.Put(t.Context(), tk.Table, marker, store.IfAbsent()); err != nil {
						t.Fatal(err)
					}
				}
				f.transact(t, it, start, "page_users@v1")
				if got := f.row(t, "page_users@v1", page("/a")); got != tt.want {
					t.Errorf("page_users /a is %q, want %q", got, tt.want)
				}
			})
		})
	}
}

// A version whose WHERE drops the event, or whose row has expired by the writer's clock,
// gets nothing to read or write.
func TestSyncPlanSkipsDroppedAndExpiredRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ms   uint64
		now  time.Duration // after the event
		want []string      // the tables read
		puts int
	}{
		{"kept", 100, 0, []string{"app-view-page_days-v1", "app-view-page_users-v1"}, 3},
		{"dropped by WHERE", 0, 0, []string{"app-view-page_users-v1"}, 2},
		{"a microsecond before the row expires", 100, 24*time.Hour - time.Microsecond, []string{"app-view-page_days-v1", "app-view-page_users-v1"}, 3},
		{"expired", 100, 24 * time.Hour, []string{"app-view-page_users-v1"}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newSyncFixture(t)
				p := f.plan(t, f.click(t, "u1", "/a", tt.ms), start.Add(tt.now), "page_days@v1", "page_users@v1")
				var tables []string
				for _, tk := range p.Reads() {
					tables = append(tables, tk.Table)
				}
				puts, err := p.Puts(map[projector.TableKey]store.Item{})
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(tables, tt.want) || len(puts) != tt.puts {
					t.Errorf("reads %v and %d puts, want reads %v and %d puts", tables, len(puts), tt.want, tt.puts)
				}
			})
		})
	}
}

func TestPlanSyncRefuses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		ms       uint64
		versions []string
		want     string
		eval     bool
	}{
		{"an evaluation error", 0, []string{"page_users@v1", "page_rates@v1"}, "projector: view page_rates@v1: evaluation error in intDiv: division by zero", true},
		{"an ASYNC version", 100, []string{"page_keys@v1"}, "projector: page_keys@v1 is not a SYNC view version of stream clicks", false},
		{"an unknown version", 100, []string{"page_users@v9"}, "projector: page_users@v9 is not a SYNC view version of stream clicks", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newSyncFixture(t)
				_, err := f.cat.Projector().PlanSync(catalog.Naming{Store: "app"}, "clicks", f.click(t, "u1", "/a", tt.ms), tt.versions, start)
				if err == nil || err.Error() != tt.want || errors.Is(err, expr.ErrEval) != tt.eval {
					t.Errorf("PlanSync: %v, want %q, matching expr.ErrEval: %v", err, tt.want, tt.eval)
				}
			})
		})
	}
}

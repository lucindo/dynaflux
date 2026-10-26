// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// queries is the test store of named queries: a stream, a view read by ranges, one read
// by points, and two with SPLIT.
const queries = `
CREATE STREAM visits (
    site     String,
    user_id  String,
    ms       UInt32,
    referrer Nullable(String)
)
PRIMARY KEY (site)
ACCEPT event_time BETWEEN now() - INTERVAL 30 DAY AND now() + INTERVAL 5 MINUTE;

CREATE MATERIALIZED VIEW site_hours
ORDER BY (site, hour)
PRIMARY KEY (site)
TTL hour + INTERVAL 2 DAY
AS SELECT site, toStartOfHour(event_time) AS hour, count() AS views, max(ms) AS longest
FROM visits
GROUP BY site, hour;

CREATE MATERIALIZED VIEW site_users
ORDER BY (site, user_id)
AS SELECT site, user_id, count() AS views
FROM visits
GROUP BY site, user_id;

CREATE MATERIALIZED VIEW split_hours
ORDER BY (site, hour)
PRIMARY KEY (site)
SPLIT 4
AS SELECT site, toStartOfHour(event_time) AS hour, count() AS views
FROM visits
GROUP BY site, hour;

-- No sort fields: each item's sort key is its SPLIT suffix alone.
CREATE MATERIALIZED VIEW split_sites
ORDER BY (site)
SPLIT 4
AS SELECT site, count() AS views
FROM visits
GROUP BY site;

-- Per user: states that queries merge, and values that they aggregate. No visit of u2 is
-- over 35 ms, so its minIf state is empty.
CREATE MATERIALIZED VIEW user_stats
ORDER BY (site, user_id)
PRIMARY KEY (site)
AS SELECT site, user_id,
    count()            AS views,
    sumIf(ms, ms > 35) AS slow_ms,
    minIf(ms, ms > 35) AS fastest_slow,
    uniq(ms)           AS distinct_ms,
    avg(ms)            AS avg_ms,
    quantile(0.9)(ms)  AS p90_ms
FROM visits
GROUP BY site, user_id;

-- name: Hours :many
SELECT hour, views FROM site_hours
WHERE site = {site:String} AND hour BETWEEN {from:Date} AND {to:DateTime64(6)}
ORDER BY hour;

-- name: HoursBefore :many
SELECT hour, views FROM site_hours
WHERE site = {site:String} AND hour < {to:DateTime64(6)}
ORDER BY hour;

-- name: LatestHours :many
SELECT site, hour, views FROM site_hours
WHERE site IN {sites:Array(String)}
ORDER BY hour DESC
LIMIT {n:UInt32};

-- name: BusyHours :many
SELECT hour, views FROM site_hours
WHERE site = {site:String} AND views >= {min:UInt64}
ORDER BY views DESC, hour;

-- name: PerView :many
SELECT intDiv(longest, {d:UInt32}) AS per FROM site_hours
WHERE site = {site:String};

-- name: UserViews :one
SELECT views FROM site_users
WHERE site = {site:String} AND user_id = {user:String};

-- name: Users :many
SELECT site, user_id, views FROM site_users
WHERE site IN {sites:Array(String)} AND user_id IN ('u1', 'u2');

-- name: Pairs :many
SELECT site, user_id, views FROM site_users
WHERE site IN {sites:Array(String)} AND user_id IN {users:Array(String)};

-- name: Referrers :many
SELECT referrer, ms FROM visits
WHERE site = {site:String}
ORDER BY referrer DESC;

-- name: ReferrersNullsFirst :many
SELECT referrer, ms FROM visits
WHERE site = {site:String}
ORDER BY referrer NULLS FIRST;

-- name: SplitLatest :many
SELECT hour, views FROM split_hours
WHERE site = {site:String}
ORDER BY hour DESC
LIMIT {n:UInt32};

-- name: SiteViews :one
SELECT views FROM split_sites
WHERE site = {site:String};

-- name: SiteStats :many
SELECT site, sum(views) AS total, sumIfMerge(slow_ms) AS slow, minIfMerge(fastest_slow) AS fastest,
    min(fastest_slow) AS fastest_or_zero, uniqMerge(distinct_ms) AS distinct, avgMerge(avg_ms) AS mean,
    quantileMerge(0.5)(p90_ms) AS median, argMax(user_id, views) AS busiest,
    argMax(user_id, site) AS last_user, any(user_id) AS first_user
FROM user_stats
WHERE site IN {sites:Array(String)}
GROUP BY site
ORDER BY site;

-- name: SiteSummary :one
SELECT count() AS users, sum(views) AS total, avgMerge(avg_ms) AS mean
FROM user_stats
WHERE site = {site:String};

-- name: ReferrerCounts :many
SELECT referrer, count() AS n, max(ms) AS longest
FROM visits
WHERE site = {site:String}
GROUP BY referrer
ORDER BY n DESC;

-- name: Recent :many
SELECT ms FROM visits
WHERE site = {site:String} AND event_time >= {since:DateTime64(6)}
ORDER BY event_time DESC
LIMIT {n:UInt32};
`

// queryStore appends visits and projects each before the append returns, from each unit
// in turn.
type queryStore struct {
	cat   *engine.Catalog
	mem   *mem.Mem
	e     *engine.Engine
	core  *projector.Core
	reads *readLog
	units []string
	seq   int
}

// splitUnits returns n delivery units whose contributions go to the n different items of a
// SPLIT n row.
func splitUnits(n int) []string {
	units := make([]string, n)
	for i, found := 0, 0; found < n; i++ {
		u := "shardId-" + strconv.Itoa(i)
		if b := keys.Bucket(u, n); units[b] == "" {
			units[b] = u
			found++
		}
	}
	return units
}

func newQueryStore(t *testing.T, sql string) *queryStore {
	t.Helper()
	cat := compile(t, sql)
	m := mem.New()
	naming := catalog.Naming{Store: "app"}
	if err := meta.New(m, naming.MetaTable()).SetLive(t.Context(), cat.Catalog()); err != nil {
		t.Fatal(err)
	}
	reads := &readLog{Backend: m}
	return &queryStore{
		cat:   cat,
		mem:   m,
		e:     engine.New(reads, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})}),
		core:  projector.New(cat.Projector(), m, nil, projector.Config{Naming: naming}),
		reads: reads,
		units: splitUnits(4),
	}
}

// visit appends a visit at the time at and projects it.
func (s *queryStore) visit(t *testing.T, site, user string, ms uint64, referrer expr.Value, at time.Time) {
	t.Helper()
	s.project(t, "visits", []expr.Value{expr.String(site), expr.String(user), expr.Uint(ms), referrer, expr.Micros(at.UnixMicro())})
}

// project appends the event ev to stream and projects it.
func (s *queryStore) project(t *testing.T, stream string, ev []expr.Value) {
	t.Helper()
	res, err := s.e.Append(t.Context(), s.cat, stream, [][]expr.Value{ev}, engine.AppendMode{})
	if err == nil {
		err = res[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	it, err := s.mem.Get(t.Context(), "app-stream-"+stream, res[0].Key, true)
	if err != nil {
		t.Fatal(err)
	}
	s.seq++
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	r, err := s.core.Handle(ctx, projector.Batch{Stream: stream, Unit: s.units[s.seq%len(s.units)], Records: []projector.Record{
		{Op: projector.Insert, Seq: fmt.Sprintf("1%020d", s.seq), New: it},
	}})
	if err != nil || r.FailedSeq != "" || len(r.Stats.Broken) > 0 {
		t.Fatalf("project: %v %+v", err, r)
	}
}

// query runs a query and returns its rows as text, or the error.
func (s *queryStore) query(t *testing.T, name string, params ...expr.Value) ([]string, error) {
	t.Helper()
	rows, err := s.e.Query(t.Context(), s.cat, name, params)
	var out []string
	for _, row := range rows {
		parts := make([]string, len(row))
		for i, v := range row {
			parts[i] = text(v)
		}
		out = append(out, strings.Join(parts, " "))
	}
	return out, err
}

func text(v expr.Value) string {
	switch v.Kind() { //nolint:exhaustive // the kinds of the test queries' columns
	case expr.KindNull:
		return "NULL"
	case expr.KindString:
		return v.AsString()
	case expr.KindUint:
		return strconv.FormatUint(v.AsUint(), 10)
	case expr.KindInt:
		return strconv.FormatInt(v.AsInt(), 10)
	case expr.KindMicros:
		return time.UnixMicro(v.AsMicros()).UTC().Format("01-02T15:04")
	case expr.KindFloat:
		return strconv.FormatFloat(v.AsFloat(), 'g', -1, 64)
	}
	return fmt.Sprintf("kind %d", v.Kind())
}

func strs(ss ...string) expr.Value {
	vs := make([]expr.Value, len(ss))
	for i, s := range ss {
		vs[i] = expr.String(s)
	}
	return expr.Array(vs)
}

// readLog counts the reads sent and the items they returned.
type readLog struct {
	store.Backend

	requests, items atomic.Int64
}

func (l *readLog) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	l.requests.Add(1)
	return l.Backend.Get(ctx, table, k, consistent)
}

func (l *readLog) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	l.requests.Add(1)
	return l.Backend.BatchGet(ctx, table, ks, consistent)
}

func (l *readLog) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	l.requests.Add(1)
	return func(yield func(store.Item, error) bool) {
		for it, err := range l.Backend.Query(ctx, table, q) {
			l.items.Add(1)
			if !yield(it, err) {
				return
			}
		}
	}
}

func (l *readLog) reset() { l.requests.Store(0); l.items.Store(0) }

func TestQueryRows(t *testing.T) {
	t.Parallel()
	day := start.Add(-24 * time.Hour)
	at := func(d time.Duration) expr.Value { return expr.Micros(day.Add(d).UnixMicro()) }
	tests := []struct {
		name   string
		query  string
		params []expr.Value
		want   []string
	}{
		{
			// A Date lower bound is midnight; a DateTime64 upper bound moves down to a whole
			// hour of the DateTime key.
			"cross-type range", "Hours",
			[]expr.Value{expr.String("a"), at(0), at(2*time.Hour + 30*time.Minute + 500*time.Millisecond)},
			[]string{"12-31T00:00 1", "12-31T01:00 2", "12-31T02:00 2"},
		},
		{
			// An exclusive bound between two key values becomes an inclusive one.
			"cross-type exclusive bound", "HoursBefore",
			[]expr.Value{expr.String("a"), at(2*time.Hour + 500*time.Millisecond)},
			[]string{"12-31T00:00 1", "12-31T01:00 2", "12-31T02:00 2"},
		},
		{
			// Ties on hour go by the rest of the key, in the direction of the last ORDER BY item.
			"IN fan-out, DESC and LIMIT", "LatestHours",
			[]expr.Value{strs("a", "b", "a"), expr.Uint(3)},
			[]string{"b 12-31T03:00 2", "b 12-31T02:00 1", "a 12-31T02:00 2"},
		},
		{
			"residual filter and sort by a non-key column", "BusyHours",
			[]expr.Value{expr.String("a"), expr.Uint(2)},
			[]string{"12-31T01:00 2", "12-31T02:00 2"},
		},
		{
			"point reads of every key combination", "Users",
			[]expr.Value{strs("b", "a", "c")},
			[]string{"a u1 3", "a u2 2", "b u1 2"},
		},
		{
			"NULLs last with DESC", "Referrers",
			[]expr.Value{expr.String("b")},
			[]string{"y 200", "x 100", "NULL 300"},
		},
		{
			"NULLS FIRST", "ReferrersNullsFirst",
			[]expr.Value{expr.String("b")},
			[]string{"NULL 300", "x 100", "y 200"},
		},
		{
			// Merges skip u2's empty minIf state, which min of the finalized values reads as
			// 0. The medians are sketch values within 1% of 25 and 200. Ties in argMax and
			// any go by the view key: argMax takes the last user.
			"read-time aggregation and merges", "SiteStats",
			[]expr.Value{strs("b", "a")},
			[]string{
				"a 5 40 40 0 5 25 24.981469082025143 u1 u2 u1",
				"b 3 600 100 100 3 200 199.85175265620114 u1 u3 u1",
			},
		},
		{
			// Without GROUP BY, one row also over no rows.
			"aggregation over no rows", "SiteSummary",
			[]expr.Value{expr.String("c")},
			[]string{"0 0 NULL"},
		},
		{
			// A NULL group comes last; ties on n go by the GROUP BY key in n's direction.
			"aggregation of a stream", "ReferrerCounts",
			[]expr.Value{expr.String("b")},
			[]string{"y 1 200", "x 1 100", "NULL 1 300"},
		},
		{
			"stream read backwards with LIMIT", "Recent",
			[]expr.Value{expr.String("a"), at(time.Hour), expr.Uint(2)},
			[]string{"40", "30"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newQueryStore(t, queries)
				null := expr.Null()
				s.visit(t, "a", "u1", 10, null, day.Add(10*time.Minute))
				s.visit(t, "a", "u1", 20, null, day.Add(70*time.Minute))
				s.visit(t, "a", "u2", 25, null, day.Add(80*time.Minute))
				s.visit(t, "a", "u2", 30, null, day.Add(130*time.Minute))
				s.visit(t, "a", "u1", 40, null, day.Add(140*time.Minute))
				s.visit(t, "b", "u1", 100, expr.String("x"), day.Add(130*time.Minute))
				s.visit(t, "b", "u1", 200, expr.String("y"), day.Add(190*time.Minute))
				s.visit(t, "b", "u3", 300, null, day.Add(195*time.Minute))
				got, err := s.query(t, tt.query, tt.params...)
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(got, tt.want) {
					t.Errorf("%s returned %q, want %q", tt.query, got, tt.want)
				}
			})
		})
	}
}

func TestQueryLimitStopsTheRead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newQueryStore(t, queries)
		for h := range 10 {
			s.visit(t, "a", "u1", 10, expr.Null(), start.Add(-time.Duration(h+1)*time.Hour))
		}
		s.reads.reset()
		got, err := s.query(t, "LatestHours", strs("a"), expr.Uint(1))
		if err != nil {
			t.Fatal(err)
		}
		// One more item than the rows tells where the last row ends.
		if n := s.reads.items.Load(); n > 2 || !slices.Equal(got, []string{"a 12-31T23:00 1"}) {
			t.Errorf("read %d items for %q, want at most 2 for the newest hour", n, got)
		}
	})
}

func sites(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "site" + strconv.Itoa(i)
	}
	return out
}

func TestQuerySplitView(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newQueryStore(t, queries)
		for h := range 6 {
			for range 2 { // from two units: two items of the row
				s.visit(t, "a", "u1", 10, expr.Null(), start.Add(-time.Duration(h+1)*time.Hour))
			}
		}
		s.reads.reset()
		latest, err := s.query(t, "SplitLatest", expr.String("a"), expr.Uint(2))
		if err != nil {
			t.Fatal(err)
		}
		// Two rows of two items, then the item that ends the second row.
		if n := s.reads.items.Load(); n > 5 || !slices.Equal(latest, []string{"12-31T23:00 2", "12-31T22:00 2"}) {
			t.Errorf("SplitLatest read %d items for %q, want at most 5 for the two newest hours", n, latest)
		}
		total, err := s.query(t, "SiteViews", expr.String("a"))
		if err != nil || !slices.Equal(total, []string{"12"}) {
			t.Errorf("SiteViews = %q, %v; want 12", total, err)
		}
	})
}

func TestQueryWithoutRequest(t *testing.T) {
	t.Parallel()
	day := start.Add(-24 * time.Hour)
	tests := []struct {
		name   string
		query  string
		params []expr.Value
	}{
		{"crossed bounds", "Hours", []expr.Value{expr.String("a"), expr.Micros(day.UnixMicro()), expr.Micros(day.Add(-time.Hour).UnixMicro())}},
		{"lookup key over 2048 bytes", "UserViews", []expr.Value{expr.String(strings.Repeat("s", 2049)), expr.String("u1")}},
		{"LIMIT 0", "LatestHours", []expr.Value{strs("a"), expr.Uint(0)}},
		{"empty IN array", "LatestHours", []expr.Value{strs(), expr.Uint(5)}},
		// No key combination, whatever the order of the sets.
		{"empty IN array after 101 values", "Pairs", []expr.Value{strs(sites(101)...), strs()}},
		{"101 values after an empty IN array", "Pairs", []expr.Value{strs(), strs(sites(101)...)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newQueryStore(t, queries)
				s.visit(t, "a", "u1", 10, expr.Null(), day)
				s.reads.reset()
				got, err := s.query(t, tt.query, tt.params...)
				if err != nil && !errors.Is(err, engine.ErrNotFound) {
					t.Fatal(err)
				}
				if n := s.reads.requests.Load(); n != 0 || len(got) != 0 {
					t.Errorf("sent %d requests and returned %q, want none", n, got)
				}
			})
		})
	}
}

func TestQueryErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		query  string
		params []expr.Value
		want   error
		msg    string
	}{
		{"101 key combinations", "LatestHours", []expr.Value{strs(sites(101)...), expr.Uint(1)}, engine.ErrTooManyKeys,
			"dynaflux: query LatestHours: too many key combinations: more than 100"},
		{":one without a row", "UserViews", []expr.Value{expr.String("a"), expr.String("u9")}, engine.ErrNotFound,
			"dynaflux: no row found"},
		{"division by zero", "PerView", []expr.Value{expr.Uint(0), expr.String("a")}, engine.ErrEvaluation,
			"dynaflux: query PerView: evaluation error in intDiv: division by zero"},
		{"invalid parameter", "UserViews", []expr.Value{expr.String("a"), expr.String("\xff")}, engine.ErrInvalidValue,
			"dynaflux: user: invalid value: not valid UTF-8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newQueryStore(t, queries)
				s.visit(t, "a", "u1", 10, expr.Null(), start.Add(-time.Hour))
				if _, err := s.query(t, tt.query, tt.params...); !errors.Is(err, tt.want) || err.Error() != tt.msg {
					t.Errorf("got %v, want %q matching %v", err, tt.msg, tt.want)
				}
			})
		})
	}
}

func TestQueryCountsRepeatedKeyValueOnce(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newQueryStore(t, queries)
		s.visit(t, "a", "u1", 10, expr.Null(), start.Add(-time.Hour))
		// 100 distinct sites, one of them twice: within the cap.
		names := append([]string{"a"}, sites(99)...)
		got, err := s.query(t, "LatestHours", strs(append(names, "a")...), expr.Uint(10))
		if err != nil || !slices.Equal(got, []string{"a 12-31T23:00 1"}) {
			t.Errorf("got %q, %v, want the one row of site a", got, err)
		}
	})
}

func TestQueryHidesExpiredRows(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newQueryStore(t, queries)
		s.visit(t, "a", "u1", 10, expr.Null(), start.Add(-time.Hour))
		// site_hours rows expire 2 days after their hour.
		time.Sleep(47*time.Hour - time.Microsecond)
		before, err := s.query(t, "LatestHours", strs("a"), expr.Uint(10))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Microsecond)
		after, err := s.query(t, "LatestHours", strs("a"), expr.Uint(10))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(before, []string{"a 12-31T23:00 1"}) || len(after) != 0 {
			t.Errorf("rows %q 1 µs before the expiry and %q at it, want one and then none", before, after)
		}
	})
}

func TestReadRowChecksKeyValues(t *testing.T) {
	t.Parallel()
	s := newQueryStore(t, queries)
	_, ok, err := s.e.ReadRow(t.Context(), s.cat, "site_hours@v1", []expr.Value{expr.String("a"), expr.String("not a time")})
	ve, isVE := errors.AsType[*engine.ValueError](err)
	if ok || !isVE || ve.Name != "hour" || !errors.Is(err, engine.ErrInvalidValue) {
		t.Errorf("ReadRow with a String hour = %v, %v; want an invalid value of hour", ok, err)
	}
}

// totals is a store whose sums leave Int64 in one row and come back across rows.
const totals = `
CREATE STREAM deltas (
    account String,
    k       String,
    n       Int64
)
PRIMARY KEY (account);

CREATE MATERIALIZED VIEW sums
ORDER BY (account, k)
PRIMARY KEY (account)
AS SELECT account, k, count() AS events, sum(n) AS total
FROM deltas
GROUP BY account, k;

-- name: Events :many
SELECT k, events FROM sums
WHERE account = {account:String};

-- name: KeyEvents :one
SELECT events FROM sums
WHERE account = {account:String} AND k = {k:String};

-- name: Net :one
SELECT sumMerge(total) AS net FROM sums
WHERE account = {account:String};

-- name: Totals :many
SELECT k, total FROM sums
WHERE account = {account:String};
`

// TestQueryChecksOnlyTotalsItReads checks that a total outside its type fails only the reads
// of its value, and that a merge checks only the merged total (sql.md §7.2).
func TestQueryChecksOnlyTotalsItReads(t *testing.T) {
	t.Parallel()
	a := expr.String("a")
	tests := []struct {
		name   string
		query  string
		params []expr.Value
		want   []string
		err    error
	}{
		{"other columns", "Events", []expr.Value{a}, []string{"r1 2", "r2 1"}, nil},
		{"other columns of one row", "KeyEvents", []expr.Value{a, expr.String("r1")}, []string{"2"}, nil},
		{"a merge whose total fits", "Net", []expr.Value{a}, []string{"4611686018427387904"}, nil},
		{"the total", "Totals", []expr.Value{a}, nil, engine.ErrEvaluation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newQueryStore(t, totals)
				at := expr.Micros(start.Add(-time.Hour).UnixMicro())
				for _, d := range []struct {
					k string
					n int64
				}{{"r1", 1 << 62}, {"r1", 1 << 62}, {"r2", -1 << 62}} {
					s.project(t, "deltas", []expr.Value{a, expr.String(d.k), expr.Int(d.n), at})
				}
				got, err := s.query(t, tt.query, tt.params...)
				if !errors.Is(err, tt.err) || !slices.Equal(got, tt.want) {
					t.Errorf("%s = %q, %v; want %q, %v", tt.query, got, err, tt.want, tt.err)
				}
				// A row read whole finalizes every column.
				if _, _, err = s.e.ReadRow(t.Context(), s.cat, "sums@v1", []expr.Value{a, expr.String("r1")}); !errors.Is(err, engine.ErrEvaluation) {
					t.Errorf("ReadRow of r1: %v, want an evaluation error", err)
				}
			})
		})
	}
}

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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
)

// pageViewsSQL is the store the real core projects: every aggregate, -If, ARRAY JOIN,
// TTL and SPLIT. A run ends long before any TTL, so no row expires. Each named query reads
// every row of one view, its keys first.
const pageViewsSQL = `
CREATE STREAM page_views (
    site    String,
    page    String,
    user_id String,
    ms      UInt32,
    price   Decimal(10, 2),
    score   Float64,
    label   String,
    tags    Array(String)
)
PRIMARY KEY (page);

-- Every additive aggregate. Its rows outlive a run.
CREATE MATERIALIZED VIEW page_hourly
ORDER BY (page, hour)
PRIMARY KEY (page)
TTL hour + INTERVAL 7 DAY
AS SELECT page, toStartOfHour(event_time) AS hour, count() AS views, sum(ms) AS total_ms,
    sum(price) AS revenue, sum(score) AS total_score, avg(ms) AS avg_ms, min(ms) AS fastest,
    max(ms) AS slowest, uniqExact(user_id) AS users, quantile(0.9)(ms) AS p90_ms,
    quantileDD(0.01, 0.99)(ms) AS p99_ms, sumIf(ms, ms > 2500) AS slow_ms
FROM page_views
GROUP BY page, hour;

CREATE MATERIALIZED VIEW page_users
ORDER BY (page, user_id)
PRIMARY KEY (page)
AS SELECT page, user_id
FROM page_views
GROUP BY page, user_id;

-- No event is that slow, so each row holds a count of 0.
CREATE MATERIALIZED VIEW slow_pages
ORDER BY (page)
AS SELECT page, countIf(ms > 60000) AS slow
FROM page_views
GROUP BY page;

-- Few sites: every unit writes each row.
CREATE MATERIALIZED VIEW site_totals
ORDER BY (site)
AS SELECT site, count() AS views, uniqExact(user_id) AS users
FROM page_views
GROUP BY site;

-- The same hot rows, each stored as 4 items.
CREATE MATERIALIZED VIEW site_daily
ORDER BY (site, day)
PRIMARY KEY (site)
SPLIT 4
AS SELECT site, toDate(event_time) AS day, count() AS views, uniqExact(user_id) AS users,
    sum(price) AS revenue
FROM page_views
GROUP BY site, day;

-- Ties in ms go by event. A poison event's label, over 4 KB, fails the evaluation though it
-- is never the largest.
CREATE MATERIALIZED VIEW user_latest
ORDER BY (user_id)
AS SELECT user_id, argMax(page, ms) AS slowest_page, argMin(page, ms) AS fastest_page,
    any(page) AS first_page, max(label) AS top_label
FROM page_views
GROUP BY user_id;

-- An event's tags repeat: uniqExact counts a tag once per record and row.
CREATE MATERIALIZED VIEW page_tags
ORDER BY (page)
AS SELECT page, count() AS tag_rows, uniqExact(tag) AS distinct_tags, uniq(tag) AS approx_tags
FROM page_views
ARRAY JOIN tags AS tag
GROUP BY page;

-- A poison event, whose ms is 0, fails the evaluation.
CREATE MATERIALIZED VIEW page_rates
ORDER BY (page)
AS SELECT page, sum(intDiv(1000, ms)) AS rate
FROM page_views
GROUP BY page;

-- name: PageHourly :many
SELECT page, hour, views, total_ms, revenue, total_score, avg_ms, fastest, slowest, users, p90_ms,
    p99_ms, slow_ms
FROM page_hourly
WHERE page IN {pages:Array(String)};

-- name: PageUsers :many
SELECT page, user_id FROM page_users
WHERE page IN {pages:Array(String)};

-- name: SlowPages :many
SELECT page, slow FROM slow_pages
WHERE page IN {pages:Array(String)};

-- name: SiteTotals :many
SELECT site, views, users FROM site_totals
WHERE site IN {sites:Array(String)};

-- name: SiteDaily :many
SELECT site, day, views, users, revenue FROM site_daily
WHERE site IN {sites:Array(String)};

-- name: UserLatest :many
SELECT user_id, slowest_page, fastest_page, first_page, top_label FROM user_latest
WHERE user_id IN {users:Array(String)};

-- name: PageTags :many
SELECT page, tag_rows, distinct_tags, approx_tags FROM page_tags
WHERE page IN {pages:Array(String)};

-- name: PageRates :many
SELECT page, rate FROM page_rates
WHERE page IN {pages:Array(String)};
`

const (
	storeName = "sim"
	sites     = 2
	pages     = 5
	users     = 12
	labels    = 10
	tags      = 3 // distinct tags; an event has up to maxTags, so they repeat
	maxTags   = 3
	maxMS     = 5000
	slowSumMS = 2500 // page_hourly's slow_ms sums the views slower than this
	slowMS    = 60000
	maxCents  = 10000
	maxScore  = 100
	spread    = 3 * time.Hour // how far back event times go
)

func metaTable() string { return catalog.Naming{Store: storeName}.MetaTable() }

// The view versions of pageViewsSQL.
const (
	pageHourly = "page_hourly@v1"
	pageUsers  = "page_users@v1"
	slowPages  = "slow_pages@v1"
	siteTotals = "site_totals@v1"
	siteDaily  = "site_daily@v1"
	userLatest = "user_latest@v1"
	pageTags   = "page_tags@v1"
	pageRates  = "page_rates@v1"
)

// compile returns the catalog of a store whose one SQL file holds sql.
func compile(t *testing.T, sql string) *engine.Catalog {
	t.Helper()
	cat, err := engine.Compile(catalogtest.Marshal(t, storeName, sql))
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

// pageViews is pageViewsSQL's store, projected by the real core. Seeds may share it.
type pageViews struct {
	cat    *engine.Catalog
	stream *projector.Stream
	// p90, p99 and uniq make the oracle's sketches of page_hourly's p90_ms and p99_ms and
	// page_tags's approx_tags.
	p90, p99, uniq *agg.Func
}

func newPageViews(t *testing.T) *pageViews {
	t.Helper()
	s := &pageViews{cat: compile(t, pageViewsSQL)}
	s.stream, _ = s.cat.Projector().Stream("page_views")
	s.p90 = aggregateFunc(t, s.cat.Catalog(), "page_hourly", "p90_ms")
	s.p99 = aggregateFunc(t, s.cat.Catalog(), "page_hourly", "p99_ms")
	s.uniq = aggregateFunc(t, s.cat.Catalog(), "page_tags", "approx_tags")
	return s
}

// aggregateFunc returns the Func of the aggregate column col of view.
func aggregateFunc(t *testing.T, cat *catalog.Catalog, view, col string) *agg.Func {
	t.Helper()
	v, _ := cat.View(view)
	vv, _ := v.Current()
	i := slices.IndexFunc(vv.Aggregates, func(a catalog.ViewAggregate) bool { return a.Name == col })
	spec := agg.Spec{Agg: vv.Aggregates[i].Aggregate}
	for _, arg := range vv.Aggregates[i].Args {
		spec.ArgTypes = append(spec.ArgTypes, arg.Type)
	}
	f, err := agg.Compile(spec)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (s *pageViews) Config() simulation.Config {
	return simulation.Config{Catalog: s.cat, Naming: catalog.Naming{Store: storeName}}
}

// value returns the i-th value of a String column whose values start with prefix.
func value(prefix string, i int) expr.Value { return expr.String(prefix + strconv.Itoa(i)) }

// every returns the first n values of such a column, as an array.
func every(prefix string, n int) expr.Value {
	vals := make([]expr.Value, n)
	for i := range vals {
		vals[i] = value(prefix, i)
	}
	return expr.Array(vals)
}

// Event returns a page view. A poison event fails page_rates or user_latest.
func (*pageViews) Event(r *rand.Rand, _ int, poison bool) (string, []expr.Value) {
	ms, label := 1+r.Uint64N(maxMS), value("l", r.IntN(labels))
	if poison && r.IntN(2) == 0 {
		ms = 0
	} else if poison {
		// Below every other label, so a max never keeps it.
		label = expr.String(strings.Repeat("a", agg.MaxStringBytes+1))
	}
	tagVals := make([]expr.Value, r.IntN(maxTags+1))
	for i := range tagVals {
		tagVals[i] = value("t", r.IntN(tags))
	}
	// A run draws its events when it starts, so event times go back from then.
	at := time.Now().Add(-time.Duration(r.Int64N(int64(spread))))
	return "page_views", []expr.Value{
		value("s", r.IntN(sites)),
		value("p", r.IntN(pages)),
		value("u", r.IntN(users)),
		expr.Uint(ms),
		expr.Decimal(big.NewInt(1 + r.Int64N(maxCents))),
		expr.Float(r.Float64() * maxScore),
		label,
		expr.Array(tagVals),
		expr.Micros(at.UnixMicro()),
	}
}

func (s *pageViews) NewCore(b store.Backend, _ simulation.Archiver) simulation.Core {
	return projector.New(s.cat.Projector(), b, projector.Config{Naming: s.Config().Naming})
}

// pageView is a decoded event of page_views.
type pageView struct {
	site, page, user, label string
	ms, price               *big.Rat
	score                   float64
	tags                    []string
	tie                     simulation.Tie
}

func (s *pageViews) decode(rec projector.Record) (pageView, error) {
	vals, bad := s.stream.Decode(rec.New)
	if err := errors.Join(bad...); err != nil {
		return pageView{}, fmt.Errorf("record %s: %w", rec.Seq, err)
	}
	n := len(s.stream.Columns)
	ev := pageView{
		site:  vals[0].AsString(),
		page:  vals[1].AsString(),
		user:  vals[2].AsString(),
		ms:    new(big.Rat).SetUint64(vals[3].AsUint()),
		price: new(big.Rat).SetFrac(vals[4].AsDecimal(), big.NewInt(100)),
		score: vals[5].AsFloat(),
		label: vals[6].AsString(),
		// event_id, then event_time, follow the declared columns.
		tie: simulation.Tie{ID: vals[n].AsString(), Time: vals[n+1].AsMicros()},
	}
	for _, tag := range vals[7].AsArray() {
		ev.tags = append(ev.tags, tag.AsString())
	}
	return ev, nil
}

func (s *pageViews) Expect(o *simulation.Oracle, unit string, rec projector.Record) error {
	ev, err := s.decode(rec)
	if err != nil {
		return err
	}
	id := simulation.RecordID{Unit: unit, Seq: rec.Seq}
	at := time.UnixMicro(ev.tie.Time).UTC()
	hour := simulation.RowID{View: pageHourly, Key: ev.page + " " + rfc3339(at.Truncate(time.Hour))}
	o.Count(hour, "views", 1)
	o.Sum(hour, "total_ms", ev.ms)
	o.Sum(hour, "revenue", ev.price)
	o.SumFloat(hour, "total_score", ev.score)
	o.Avg(hour, "avg_ms", ev.ms)
	o.Min(hour, "fastest", ev.ms)
	o.Max(hour, "slowest", ev.ms)
	o.UniqExact(hour, "users", ev.user)
	ms := expr.Uint(ev.ms.Num().Uint64())
	if err = o.Sketch(hour, "p90_ms", s.p90, ms); err != nil {
		return err
	}
	if err = o.Sketch(hour, "p99_ms", s.p99, ms); err != nil {
		return err
	}
	slowSum := new(big.Rat)
	if ev.ms.Cmp(big.NewRat(slowSumMS, 1)) > 0 {
		slowSum = ev.ms
	}
	o.Sum(hour, "slow_ms", slowSum)
	o.Reach(simulation.RowID{View: pageUsers, Key: ev.page + " " + ev.user})
	slow := int64(0)
	if ev.ms.Cmp(big.NewRat(slowMS, 1)) > 0 {
		slow = 1
	}
	o.Count(simulation.RowID{View: slowPages, Key: ev.page}, "slow", slow)
	expectSites(o, ev, at)
	expectUser(o, ev, id)
	if err = s.expectTags(o, ev); err != nil {
		return err
	}
	if ev.ms.Sign() == 0 {
		o.Poison(pageRates, id)
		return nil
	}
	rate := new(big.Int).Quo(big.NewInt(1000), ev.ms.Num())
	o.Sum(simulation.RowID{View: pageRates, Key: ev.page}, "rate", new(big.Rat).SetInt(rate))
	return nil
}

func expectSites(o *simulation.Oracle, ev pageView, at time.Time) {
	total := simulation.RowID{View: siteTotals, Key: ev.site}
	o.Count(total, "views", 1)
	o.UniqExact(total, "users", ev.user)
	day := simulation.RowID{View: siteDaily, Key: ev.site + " " + rfc3339(at.Truncate(24*time.Hour))}
	o.Count(day, "views", 1)
	o.UniqExact(day, "users", ev.user)
	o.Sum(day, "revenue", ev.price)
}

func expectUser(o *simulation.Oracle, ev pageView, id simulation.RecordID) {
	row := simulation.RowID{View: userLatest, Key: ev.user}
	if len(ev.label) > agg.MaxStringBytes {
		o.Poison(userLatest, id)
		return
	}
	o.Pick(row, "slowest_page", catalog.AggArgMax, ev.page, ev.ms, ev.tie)
	o.Pick(row, "fastest_page", catalog.AggArgMin, ev.page, ev.ms, ev.tie)
	o.Pick(row, "first_page", catalog.AggAny, ev.page, nil, ev.tie)
	o.MaxString(row, "top_label", ev.label)
}

// expectTags adds one source row per element of the event's tags.
func (s *pageViews) expectTags(o *simulation.Oracle, ev pageView) error {
	row := simulation.RowID{View: pageTags, Key: ev.page}
	for _, tag := range ev.tags {
		o.Count(row, "tag_rows", 1)
		o.UniqExact(row, "distinct_tags", tag)
		if err := o.Sketch(row, "approx_tags", s.uniq, expr.String(tag)); err != nil {
			return err
		}
	}
	return nil
}

// Read reads every view through its named query, and the breakers from the meta table.
func (s *pageViews) Read(ctx context.Context, b store.Backend) (simulation.State, error) {
	st := simulation.State{Rows: simulation.Rows{}, Broken: map[string]simulation.RecordID{}}
	states, err := meta.New(b, metaTable()).ViewStates(ctx)
	if err != nil {
		return st, err
	}
	for version, vs := range states {
		if vs.State == meta.Broken {
			// The meta table pads sequence numbers; the oracle has them as delivered.
			st.Broken[version] = simulation.RecordID{Unit: vs.Failure.Unit, Seq: strings.TrimLeft(vs.Failure.Seq, "0")}
		}
	}
	e := engine.New(b, engine.Options{})
	all := map[string]expr.Value{"sites": every("s", sites), "pages": every("p", pages), "users": every("u", users)}
	for i := range s.cat.Catalog().Queries {
		if err = s.readView(ctx, e, &s.cat.Catalog().Queries[i], all, st.Rows); err != nil {
			return st, err
		}
	}
	return st, nil
}

// readView adds the rows that q reads to rows. q's one parameter, from all, holds every
// value of a partition key column.
func (s *pageViews) readView(ctx context.Context, e *engine.Engine, q *catalog.Query, all map[string]expr.Value, rows simulation.Rows) error {
	v, _ := s.cat.Catalog().View(q.Source.Name)
	vv, _ := v.Current()
	got, err := e.Query(ctx, s.cat, q.Name, []expr.Value{all[q.Params[0].Name]})
	if err != nil {
		return fmt.Errorf("%s: %w", q.Name, err)
	}
	for _, vals := range got {
		key := make([]string, len(vv.Keys))
		row := simulation.Row{}
		for i, val := range vals {
			if i < len(key) {
				key[i] = text(val, q.Columns[i].Expr.Type)
			} else {
				row[q.Columns[i].Name] = text(val, q.Columns[i].Expr.Type)
			}
		}
		rows[simulation.RowID{View: catalog.VersionID(v.Name, vv.Number), Key: strings.Join(key, " ")}] = row
	}
	return nil
}

// text formats a value of type t as the oracle does.
func text(v expr.Value, t catalog.Type) string {
	switch v.Kind() { //nolint:exhaustive // the kinds of pageViewsSQL's columns
	case expr.KindUint:
		return strconv.FormatUint(v.AsUint(), 10)
	case expr.KindDecimal:
		return new(big.Rat).SetFrac(v.AsDecimal(), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(t.Scale)), nil)).RatString()
	case expr.KindMicros:
		return rfc3339(time.UnixMicro(v.AsMicros()))
	case expr.KindString:
		return v.AsString()
	case expr.KindFloat:
		return simulation.FormatFloat(v.AsFloat())
	case expr.KindNull:
		return "NULL"
	}
	return fmt.Sprintf("a value of kind %d", v.Kind())
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func str(s string) store.Value { return store.Value{Type: store.S, Str: s} }

func num(n int64) store.Value { return store.Value{Type: store.N, Str: strconv.FormatInt(n, 10)} }

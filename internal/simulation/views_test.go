// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
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

// fixture is the store the real core projects: testdata/sim, which every scenario runs.
//
//go:embed testdata/sim
var fixture embed.FS

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
	accounts  = 3
	maxUnits  = 100
	customers = 16
	quotaEach = 4             // every quotaEach-th event is a quota request or a charge, in turn
	spread    = 3 * time.Hour // how far back event times go
)

func metaTable() string { return catalog.Naming{Store: storeName}.MetaTable() }

// The view versions of the fixture.
const (
	pageHourlyV1   = "page_hourly@v1" // retained, by dynaflux.lock
	pageHourly     = "page_hourly@v2"
	pageUsers      = "page_users@v1"
	slowPages      = "slow_pages@v1"
	siteTotals     = "site_totals@v1"
	siteDaily      = "site_daily@v1"
	userLatest     = "user_latest@v1"
	pageTags       = "page_tags@v1"
	pageRates      = "page_rates@v1"
	quotaByAccount = "quota_by_account@v1"
	quotaByOrigin  = "quota_by_origin@v1"
	chargeTotals   = "charge_totals@v1"
)

// compile returns the catalog of a store whose one SQL file holds sql.
func compile(t *testing.T, sql string) *engine.Catalog {
	t.Helper()
	return compileData(t, catalogtest.Marshal(t, storeName, sql), 0)
}

// window is pageViews' prune window W: the least that the scenarios' longest timeout, 2
// minutes, allows.
const window = 3 * time.Minute

// compileWindow returns compile's catalog with prune window w, as the harness's config
// controls edit it.
func compileWindow(t *testing.T, sql string, w time.Duration) *engine.Catalog {
	t.Helper()
	return compileData(t, catalogtest.Marshal(t, storeName, sql), w)
}

// fixtureWindow returns the fixture's catalog with prune window w.
func fixtureWindow(t *testing.T, w time.Duration) *engine.Catalog {
	t.Helper()
	root, err := fs.Sub(fixture, "testdata/sim")
	if err != nil {
		t.Fatal(err)
	}
	return compileData(t, catalogtest.MarshalFS(t, root), w)
}

// compileData compiles a marshaled catalog, with prune window w unless w is 0.
func compileData(t *testing.T, data []byte, w time.Duration) *engine.Catalog {
	t.Helper()
	if w != 0 {
		def, err := catalog.Unmarshal(data)
		if err != nil {
			t.Fatal(err)
		}
		def.PruneWindowSeconds = int(w / time.Second)
		if data, err = catalog.Marshal(def); err != nil {
			t.Fatal(err)
		}
	}
	cat, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

// pageViews is the fixture's store, projected by the real core. Seeds may share it.
type pageViews struct {
	cat *engine.Catalog
	// core is the catalog the cores run: cat, or for a config control, cat with another W.
	core                   *engine.Catalog
	stream, quota, charges *projector.Stream
	// p90, p99 and uniq make the oracle's sketches of page_hourly's p90_ms and p99_ms and
	// page_tags's approx_tags.
	p90, p99, uniq *agg.Func
}

func newPageViews(t *testing.T) *pageViews {
	t.Helper()
	cat := fixtureWindow(t, window)
	s := &pageViews{cat: cat, core: cat}
	s.stream, _ = s.cat.Projector().Stream("page_views")
	s.quota, _ = s.cat.Projector().Stream("quota_events")
	s.charges, _ = s.cat.Projector().Stream("charges")
	s.p90 = aggregateFunc(t, s.cat.Catalog(), "page_hourly", "p90_ms")
	s.p99 = aggregateFunc(t, s.cat.Catalog(), "page_hourly", "p99_ms")
	s.uniq = aggregateFunc(t, s.cat.Catalog(), "page_tags", "approx_tags")
	return s
}

// withCoreWindow returns s with cores that prune with w, while the harness checks P4
// against the catalog's W.
func (s *pageViews) withCoreWindow(t *testing.T, w time.Duration) *pageViews {
	t.Helper()
	c := *s
	c.core = fixtureWindow(t, w)
	return &c
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

// Config backfills idempotent-only versions and fenced ones: page_hourly's version 2 while
// version 1 runs, page_rates and user_latest, which break on poison events, site_daily,
// which has SPLIT, and the views of cross-region duplicates. The others are live from the
// start.
func (s *pageViews) Config() simulation.Config {
	return simulation.Config{
		Catalog: s.cat,
		Naming:  catalog.Naming{Store: storeName},
		Backfill: []string{pageHourly, pageUsers, siteDaily, userLatest, pageTags, pageRates, quotaByAccount,
			chargeTotals},
	}
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

// Event returns a page view, or every quotaEach-th a quota request or a charge, in turn.
// Half the charges are retried with another amount. A poison event is a page view that fails
// page_rates or user_latest.
func (*pageViews) Event(r *rand.Rand, n int, poison bool) simulation.Event {
	// A run draws its events when it starts, so event times go back from then.
	at := time.Now().Add(-time.Duration(r.Int64N(int64(spread))))
	switch {
	case poison || n%quotaEach != quotaEach-1:
	case n/quotaEach%2 == 0:
		return simulation.Event{Stream: "quota_events", Vals: []expr.Value{
			value("a", r.IntN(accounts)),
			value("q", n),
			expr.Uint(1 + r.Uint64N(maxUnits)),
			expr.Micros(at.UnixMicro()),
		}}
	default:
		ev := simulation.Event{Stream: "charges", Vals: []expr.Value{
			value("c", r.IntN(customers)),
			value("k", n),
			expr.Uint(1 + r.Uint64N(maxCents)),
			expr.Micros(at.UnixMicro()),
		}}
		if r.IntN(2) == 0 {
			ev.Divergent = slices.Clone(ev.Vals)
			ev.Divergent[2] = expr.Uint(ev.Vals[2].AsUint() + 1 + r.Uint64N(maxCents))
		}
		return ev
	}
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
	return simulation.Event{Stream: "page_views", Vals: []expr.Value{
		value("s", r.IntN(sites)),
		value("p", r.IntN(pages)),
		value("u", r.IntN(users)),
		expr.Uint(ms),
		expr.Decimal(big.NewInt(1 + r.Int64N(maxCents))),
		expr.Float(r.Float64() * maxScore),
		label,
		expr.Array(tagVals),
		expr.Micros(at.UnixMicro()),
	}}
}

func (s *pageViews) NewCore(b store.Backend, a simulation.Archiver) simulation.Core {
	return projector.New(s.core.Projector(), b, a, projector.Config{Naming: s.Config().Naming})
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

func (s *pageViews) Expect(o *simulation.Oracle, stream, unit string, rec projector.Record) error {
	switch stream {
	case "quota_events":
		return s.expectQuota(o, rec)
	case "charges":
		return s.expectCharge(o, rec)
	}
	ev, err := s.decode(rec)
	if err != nil {
		return err
	}
	id := simulation.RecordID{Unit: unit, Seq: rec.Seq}
	at := time.UnixMicro(ev.tie.Time).UTC()
	for _, version := range []string{pageHourlyV1, pageHourly} {
		if err = s.expectHour(o, version, ev, at); err != nil {
			return err
		}
	}
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

// expectHour adds a page view to its row of a version of page_hourly. Version 1 has no
// slow_ms.
func (s *pageViews) expectHour(o *simulation.Oracle, version string, ev pageView, at time.Time) error {
	hour := simulation.RowID{View: version, Key: ev.page + " " + rfc3339(at.Truncate(time.Hour))}
	o.Count(hour, "views", 1)
	o.Sum(hour, "total_ms", ev.ms)
	o.Sum(hour, "revenue", ev.price)
	o.SumFloat(hour, "total_score", ev.score)
	o.Avg(hour, "avg_ms", ev.ms)
	o.Min(hour, "fastest", ev.ms)
	o.Max(hour, "slowest", ev.ms)
	o.UniqExact(hour, "users", ev.user)
	ms := expr.Uint(ev.ms.Num().Uint64())
	if err := o.Sketch(hour, "p90_ms", s.p90, ms); err != nil {
		return err
	}
	if err := o.Sketch(hour, "p99_ms", s.p99, ms); err != nil {
		return err
	}
	if version == pageHourly {
		slowSum := new(big.Rat)
		if ev.ms.Cmp(big.NewRat(slowSumMS, 1)) > 0 {
			slowSum = ev.ms
		}
		o.Sum(hour, "slow_ms", slowSum)
	}
	return nil
}

// originRegion is where origin_region is in a decoded event: the fourth of the implicit
// columns that follow the declared ones.
const originRegion = 3

func (s *pageViews) expectQuota(o *simulation.Oracle, rec projector.Record) error {
	vals, bad := s.quota.Decode(rec.New)
	if err := errors.Join(bad...); err != nil {
		return fmt.Errorf("record %s: %w", rec.Seq, err)
	}
	account, origin := vals[0].AsString(), vals[len(s.quota.Columns)+originRegion].AsString()
	for _, row := range []simulation.RowID{{View: quotaByAccount, Key: account}, {View: quotaByOrigin, Key: account + " " + origin}} {
		o.Count(row, "requests", 1)
		o.Sum(row, "used", new(big.Rat).SetUint64(vals[2].AsUint()))
	}
	return nil
}

func (s *pageViews) expectCharge(o *simulation.Oracle, rec projector.Record) error {
	vals, bad := s.charges.Decode(rec.New)
	if err := errors.Join(bad...); err != nil {
		return fmt.Errorf("record %s: %w", rec.Seq, err)
	}
	row := simulation.RowID{View: chargeTotals, Key: vals[0].AsString()}
	o.Count(row, "charges", 1)
	o.Sum(row, "total_cents", new(big.Rat).SetUint64(vals[2].AsUint()))
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
	o.UniqExact(row, "pages", ev.page)
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
	all := map[string]expr.Value{
		"sites":     every("s", sites),
		"pages":     every("p", pages),
		"users":     every("u", users),
		"accounts":  every("a", accounts),
		"customers": every("c", customers),
	}
	for i := range s.cat.Catalog().Queries {
		if err = s.readView(ctx, e, &s.cat.Catalog().Queries[i], all, st.Rows); err != nil {
			return st, err
		}
	}
	return st, nil
}

// readView adds the rows that q reads to rows, and those of the view's retained versions at
// the same keys, which a query can't read. q's one parameter, from all, holds every value
// of a partition key column.
func (s *pageViews) readView(ctx context.Context, e *engine.Engine, q *catalog.Query, all map[string]expr.Value, rows simulation.Rows) error {
	v, _ := s.cat.Catalog().View(q.Source.Name)
	cur, _ := v.Current()
	got, err := e.Query(ctx, s.cat, q.Name, []expr.Value{all[q.Params[0].Name]})
	if err != nil {
		return fmt.Errorf("%s: %w", q.Name, err)
	}
	for _, vals := range got {
		addRow(rows, catalog.VersionID(v.Name, cur.Number), len(cur.Keys), q.Columns, vals)
		for i := range v.Versions {
			vv := &v.Versions[i]
			if vv.Number == cur.Number {
				continue
			}
			id := catalog.VersionID(v.Name, vv.Number)
			old, ok, rerr := e.ReadRow(ctx, s.cat, id, vals[:len(cur.Keys)])
			if rerr != nil {
				return fmt.Errorf("%s: %w", id, rerr)
			}
			if ok {
				addRow(rows, id, len(vv.Keys), versionColumns(vv), old)
			}
		}
	}
	return nil
}

// versionColumns returns a view version's columns as ReadRow returns them: the keys, then
// the aggregates.
func versionColumns(vv *catalog.ViewVersion) []catalog.NamedExpr {
	cols := slices.Clone(vv.Keys)
	for _, a := range vv.Aggregates {
		cols = append(cols, catalog.NamedExpr{Name: a.Name, Expr: catalog.Expr{Type: a.Type}})
	}
	return cols
}

// addRow adds a row of view version id to rows: vals, the values of cols, whose first n are
// its keys.
func addRow(rows simulation.Rows, id string, n int, cols []catalog.NamedExpr, vals []expr.Value) {
	key := make([]string, n)
	row := simulation.Row{}
	for i, val := range vals {
		if i < n {
			key[i] = simulation.FormatValue(val, cols[i].Expr.Type)
		} else {
			row[cols[i].Name] = simulation.FormatValue(val, cols[i].Expr.Type)
		}
	}
	rows[simulation.RowID{View: id, Key: strings.Join(key, " ")}] = row
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func str(s string) store.Value { return store.Value{Type: store.S, Str: s} }

func num(n int64) store.Value { return store.Value{Type: store.N, Str: strconv.FormatInt(n, 10)} }

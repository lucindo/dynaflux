// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
)

// query is a compiled named query.
type query struct {
	def    *catalog.Query
	view   *catalog.ViewVersion // nil for a stream query
	viewID string
	// funcs make the view's states; finals are the view aggregates whose values the query
	// reads, and merges reports a -Merge, which reads states.
	funcs  []*agg.Func
	finals []int
	merges bool
	stream *stream          // the source of a stream query
	cols   []catalog.Column // the source row's columns
	bound  []binding        // the partition key columns, then the sort prefix
	rng    *keyRange
	filter *expr.Program
	agg    *aggregation // nil without read-time aggregation
	out    []*expr.Program
	order  []orderKey
	limit  *expr.Program
	// keyOrder reports an ORDER BY that follows the order in which each key combination is
	// read, so the read of a combination can stop after LIMIT rows. desc reads backwards.
	keyOrder, desc bool
}

type orderKey struct {
	p                *expr.Program
	t                catalog.Type
	desc, nullsFirst bool
}

func (c *Catalog) compileQuery(def *catalog.Query) (*query, error) {
	q := &query{def: def}
	keyCols, err := c.source(q)
	if err != nil {
		return nil, err
	}
	if def.Aggregation != nil {
		q.agg = &aggregation{rowKey: q.rowKey(keyCols)}
	}
	names := make([]string, len(def.Params))
	for i, p := range def.Params {
		names[i] = p.Name
	}
	if err = q.compileKeys(expr.Scope{Params: names}); err != nil {
		return nil, err
	}
	scope := expr.Scope{Params: names}
	for _, col := range q.cols {
		scope.Columns = append(scope.Columns, col.Name)
	}
	if err = q.compileRows(scope); err != nil {
		return nil, err
	}
	if q.view != nil {
		q.finals = q.finalized()
	}
	if def.Limit != nil {
		if q.limit, err = expr.Compile(def.Limit, expr.Scope{Params: names}); err != nil {
			return nil, fmt.Errorf("LIMIT: %w", err)
		}
	}
	// An aggregation reads every row, and its ORDER BY sorts the groups.
	if q.agg == nil {
		q.keyOrder, q.desc = q.followsKey(keyCols[len(q.bound):])
	}
	return q, nil
}

// source sets the query's source and returns its key columns in key order.
func (c *Catalog) source(q *query) ([]string, error) {
	name := q.def.Source.Name
	if q.def.Source.Kind == catalog.SourceStream {
		s, err := c.stream(name)
		if err != nil {
			return nil, err
		}
		q.stream = s
		q.cols = slices.Concat(s.Def.Columns, catalog.ImplicitColumns())
		return append(slices.Clone(s.Def.PrimaryKey), catalog.ColumnEventTime, catalog.ColumnEventID), nil
	}
	v, ok := c.def.View(name)
	if !ok {
		return nil, fmt.Errorf("no view %s", name)
	}
	vv, ok := v.Current()
	if !ok {
		return nil, fmt.Errorf("view %s has no current version", name)
	}
	q.view, q.viewID = vv, catalog.VersionID(name, vv.Number)
	q.funcs = c.p.Funcs(q.viewID)
	keyCols := make([]string, len(vv.Keys))
	for i, k := range vv.Keys {
		keyCols[i] = k.Name
		q.cols = append(q.cols, catalog.Column{Name: k.Name, Type: k.Expr.Type})
	}
	for _, a := range vv.Aggregates {
		q.cols = append(q.cols, catalog.Column{Name: a.Name, Type: a.Type})
	}
	return keyCols, nil
}

func (q *query) column(name string) (int, error) {
	i := slices.IndexFunc(q.cols, func(c catalog.Column) bool { return c.Name == name })
	if i < 0 {
		return 0, fmt.Errorf("no key column %s in %s %s", name, q.def.Source.Kind, q.def.Source.Name)
	}
	return i, nil
}

func (q *query) compileKeys(params expr.Scope) error {
	compile := func(e *catalog.Expr) (operand, error) {
		p, err := expr.Compile(e, params)
		return operand{p: p, t: e.Type}, err
	}
	for _, kb := range slices.Concat(q.def.Keys.Partition, q.def.Keys.SortPrefix) {
		col, err := q.column(kb.Column)
		if err != nil {
			return err
		}
		b := binding{col: col, typ: q.cols[col].Type}
		for i := range kb.Values {
			o, cerr := compile(&kb.Values[i])
			if cerr != nil {
				return fmt.Errorf("key %s: %w", kb.Column, cerr)
			}
			b.values = append(b.values, o)
		}
		q.bound = append(q.bound, b)
	}
	r := q.def.Keys.Range
	if r == nil {
		return nil
	}
	col, err := q.column(r.Column)
	if err != nil {
		return err
	}
	q.rng = &keyRange{col: col, typ: q.cols[col].Type}
	for _, rb := range []struct {
		src *catalog.RangeBound
		dst **bound
	}{{r.Lower, &q.rng.lower}, {r.Upper, &q.rng.upper}} {
		if rb.src == nil {
			continue
		}
		o, cerr := compile(&rb.src.Value)
		if cerr != nil {
			return fmt.Errorf("range of %s: %w", r.Column, cerr)
		}
		*rb.dst = &bound{operand: o, inclusive: rb.src.Inclusive}
	}
	return nil
}

// compileRows compiles what runs on the rows read: the filter, the aggregation, the output
// columns and the sort keys.
func (q *query) compileRows(scope expr.Scope) error {
	var err error
	if q.def.Filter != nil {
		if q.filter, err = expr.Compile(q.def.Filter, scope); err != nil {
			return fmt.Errorf("WHERE: %w", err)
		}
	}
	if q.agg != nil {
		if err = q.compileAggregation(scope); err != nil {
			return err
		}
		// Columns and ORDER BY see the GROUP BY keys and the aggregates.
		scope = expr.Scope{Params: scope.Params, Aggs: len(q.agg.aggs)}
		for _, g := range q.def.Aggregation.GroupBy {
			scope.Columns = append(scope.Columns, g.Name)
		}
	}
	for _, c := range q.def.Columns {
		p, cerr := expr.Compile(&c.Expr, scope)
		if cerr != nil {
			return fmt.Errorf("column %s: %w", c.Name, cerr)
		}
		q.out = append(q.out, p)
	}
	for _, o := range q.def.OrderBy {
		p, cerr := expr.Compile(&o.Expr, scope)
		if cerr != nil {
			return fmt.Errorf("ORDER BY: %w", cerr)
		}
		q.order = append(q.order, orderKey{p: p, t: o.Expr.Type, desc: o.Desc, nullsFirst: o.NullsFirst})
	}
	return nil
}

// finalized returns the view aggregates whose values the query reads. A -Merge reads
// states, so a total outside its type fails only the queries that read it (sql.md §7.2).
func (q *query) finalized() []int {
	names := map[string]bool{}
	read := func(e *catalog.Expr) {
		if e != nil {
			e.ColumnNames(names)
		}
	}
	d := q.def
	read(d.Filter)
	for i := range d.Columns {
		read(&d.Columns[i].Expr)
	}
	for i := range d.OrderBy {
		read(&d.OrderBy[i].Expr)
	}
	if a := d.Aggregation; a != nil {
		for i := range a.GroupBy {
			read(&a.GroupBy[i].Expr)
		}
		for i := range a.Aggregates {
			qa := &a.Aggregates[i]
			read(qa.Cond)
			if qa.Merge {
				continue
			}
			for j := range qa.Args {
				read(&qa.Args[j])
			}
		}
	}
	var idx []int
	for i, va := range q.view.Aggregates {
		if names[va.Name] {
			idx = append(idx, i)
		}
	}
	return idx
}

// followsKey reports whether OrderBy, without the bound columns, is exactly the unbound key
// columns in one direction (catalog OrderBy already ends with the row key), and whether that
// direction is descending.
func (q *query) followsKey(unbound []string) (bool, bool) {
	bound := map[string]bool{}
	for _, b := range q.bound {
		bound[q.cols[b.col].Name] = true
	}
	var names []string
	desc := false
	for _, o := range q.def.OrderBy {
		switch {
		case o.Expr.Op != catalog.OpColumn:
			return false, false
		case bound[o.Expr.Name]:
			continue
		case names == nil:
			desc = o.Desc
		case o.Desc != desc:
			return false, false
		}
		names = append(names, o.Expr.Name)
	}
	if !slices.Equal(names, unbound) {
		return false, false
	}
	return true, desc
}

// QueryOptions change how Query runs.
type QueryOptions struct {
	// AllowPartial reads a view whose version isn't live, without reading its state.
	AllowPartial bool
	// Wait makes the query wait for the events of appends first, one token after another.
	Wait []Wait
}

// Query runs the named query with params, in the order of its parameters, and returns its
// rows, each with the output columns in SELECT order. A :one query that finds no row
// returns ErrNotFound. An evaluation error matches ErrEvaluation.
//
// A query over a view fails with ErrViewNotReady unless the view's current version is live
// in the engine's region, in states read from the meta table at most
// [meta.MaxViewStateAge] ago, or o.AllowPartial is set. A query over a SYNC version reads
// strongly consistently, so it sees every append in the region that returned before it.
//
// With o.Wait, a query over a view first waits for each token's receipts ([Engine.Wait]),
// then reads the version's state again rather than from the cache: a waited record may have
// opened its breaker. A query over a stream only checks the tokens. Either reads strongly
// consistently.
func (e *Engine) Query(ctx context.Context, cat *Catalog, name string, params []expr.Value, o QueryOptions) ([][]expr.Value, error) {
	q, ok := cat.queries[name]
	if !ok {
		return nil, fmt.Errorf("dynaflux: no query %s in the catalog", name)
	}
	if len(params) != len(q.def.Params) {
		return nil, fmt.Errorf("dynaflux: query %s takes %d parameters, not %d", name, len(q.def.Params), len(params))
	}
	for i, p := range q.def.Params {
		if _, _, err := encode(p.Name, p.Type, params[i]); err != nil {
			return nil, err
		}
	}
	waiting := false
	for _, w := range o.Wait {
		if w.Token == "" {
			continue
		}
		waiting = true
		if err := e.waitFor(ctx, cat, q, w); err != nil {
			return nil, fmt.Errorf("dynaflux: query %s: %w", name, err)
		}
	}
	if q.view != nil && !o.AllowPartial {
		if err := e.ready(ctx, cat, q.viewID, waiting); err != nil {
			return nil, fmt.Errorf("dynaflux: query %s: %w", name, err)
		}
	}
	rows, err := e.run(ctx, cat, q, params, waiting)
	if err != nil {
		return nil, fmt.Errorf("dynaflux: query %s: %w", name, err)
	}
	if len(rows) == 0 && q.def.Cardinality == catalog.CardinalityOne {
		return nil, ErrNotFound
	}
	return rows, nil
}

// ready returns nil when view version id is live in the engine's region, by the cached
// states or, when fresh is set, states read now, and otherwise an error that matches
// ErrViewNotReady and names the state, with a broken version's failure.
func (e *Engine) ready(ctx context.Context, cat *Catalog, id string, fresh bool) error {
	states, err := e.stateCache(e.naming(cat).MetaTable()).States(ctx, fresh)
	if err != nil {
		return err
	}
	vs := states[id]
	if vs.State == meta.Live {
		return nil
	}
	if vs.State == meta.Broken {
		return fmt.Errorf("%w: %s is broken in %s: event %s: %s", ErrViewNotReady, id, e.region, vs.Failure.EventID, vs.Failure.Error)
	}
	// A version without a state item is pending.
	return fmt.Errorf("%w: %s is %s in %s", ErrViewNotReady, id, cmp.Or(vs.State, meta.PendingBackfill), e.region)
}

// stateCache returns the cache of the view states in meta table.
func (e *Engine) stateCache(table string) *meta.StateCache {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, ok := e.states[table]
	if !ok {
		c = meta.NewStateCache(meta.New(e.b, table))
		e.states[table] = c
	}
	return c
}

// run reads q's rows, strongly consistently when strong is set or q reads a SYNC version.
func (e *Engine) run(ctx context.Context, cat *Catalog, q *query, params []expr.Value, strong bool) ([][]expr.Value, error) {
	limit, err := q.limitValue(params)
	if err != nil || limit == 0 {
		return nil, err
	}
	r, err := q.planRead(params)
	if err != nil {
		return nil, err
	}
	r.consistent = strong || q.view != nil && consistent(q.view)
	perCombo := 0
	if limit > 0 && q.keyOrder {
		perCombo = limit
	}
	var src []source
	keep := func(row source) (bool, error) {
		ok, kerr := q.keep(&r, row.vals, params)
		if ok {
			src = append(src, row)
		}
		return ok, kerr
	}
	if !r.empty {
		if err = e.readRows(ctx, cat, q, &r, perCombo, keep); err != nil {
			return nil, err
		}
	}
	var frames []expr.Frame
	if q.agg != nil {
		frames, err = q.agg.groups(src, params)
	} else {
		frames = make([]expr.Frame, len(src))
		for i, row := range src {
			frames[i] = expr.Frame{Cols: row.vals, Params: params}
		}
	}
	if err != nil {
		return nil, err
	}
	return q.output(frames, limit)
}

// limitValue returns the LIMIT, or -1 without one.
func (q *query) limitValue(params []expr.Value) (int, error) {
	if q.limit == nil {
		return -1, nil
	}
	v, err := q.limit.Eval(&expr.Frame{Params: params})
	if err != nil {
		return 0, err
	}
	return int(min(v.AsUint(), math.MaxInt)), nil
}

// keep reports whether a row read passes the range as given and the residual filter.
func (q *query) keep(r *read, row, params []expr.Value) (bool, error) {
	if q.rng != nil && !r.inRange(row[q.rng.col], q.rng.typ) {
		return false, nil
	}
	if q.filter == nil {
		return true, nil
	}
	v, err := q.filter.Eval(&expr.Frame{Cols: row, Params: params})
	return v.IsTrue(), err
}

// output computes the output columns and sort keys of every row, sorts the rows and keeps
// the first limit, or all of them for a negative limit.
func (q *query) output(frames []expr.Frame, limit int) ([][]expr.Value, error) {
	type sorted struct{ out, keys []expr.Value }
	rows := make([]sorted, len(frames))
	for i := range frames {
		f := &frames[i]
		rows[i] = sorted{out: make([]expr.Value, len(q.out)), keys: make([]expr.Value, len(q.order))}
		for j, p := range q.out {
			var err error
			if rows[i].out[j], err = p.Eval(f); err != nil {
				return nil, err
			}
		}
		for j, o := range q.order {
			var err error
			if rows[i].keys[j], err = o.p.Eval(f); err != nil {
				return nil, err
			}
		}
	}
	slices.SortStableFunc(rows, func(a, b sorted) int {
		for j, o := range q.order {
			if c := o.compare(a.keys[j], b.keys[j]); c != 0 {
				return c
			}
		}
		return 0
	})
	if limit >= 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([][]expr.Value, len(rows))
	for i, r := range rows {
		out[i] = r.out
	}
	return out, nil
}

// compare orders two sort-key values. NULLs go last unless NULLS FIRST, also with DESC, so
// they are placed here rather than by negating expr.Compare, which puts NULL last.
func (o orderKey) compare(a, b expr.Value) int {
	switch {
	case a.IsNull() && b.IsNull():
		return 0
	case a.IsNull() != b.IsNull():
		if a.IsNull() == o.nullsFirst {
			return -1
		}
		return 1
	}
	c := expr.Compare(a, o.t, b, o.t)
	if o.desc {
		return -c
	}
	return c
}

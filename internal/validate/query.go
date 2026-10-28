// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"maps"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// queryInfo is a compiled named query.
type queryInfo struct {
	query catalog.Query
	// params are the first use of each parameter, and outs the SELECT items: they become
	// the fields of the generated <Name>Params and <Name>Row types.
	params []*sql.Param
	outs   []*output
}

// querySource is the view or stream that a query reads.
type querySource struct {
	kind   catalog.SourceKind
	name   string
	view   *viewInfo   // nil for a stream
	stream *streamInfo // nil for a view
	// columns are the columns a query sees, with their types: a view's keys and finalized
	// aggregates, or a stream's declared and implicit columns.
	columns map[string]catalog.Type
	// bad are the columns with an invalid definition: what reads them is reported already.
	bad map[string]bool
	// keys are the key columns in key order, and the first partition of them the partition
	// key: a view's ORDER BY and PRIMARY KEY, or a stream's PRIMARY KEY.
	keys      []string
	partition int
	// keysOK is false when the definition's errors leave its key unknown; the key binding
	// is then not checked.
	keysOK bool
}

// queryCompiler is the state of compiling one named query.
type queryCompiler struct {
	// scope holds the source's columns. Its bad columns include the ARRAY JOIN aliases,
	// which DF4003 reports.
	scope

	d   *decl
	st  *stmt
	sel *sql.Select
	// src is nil when FROM names no compiled view or stream, and then nothing that needs
	// its columns is checked.
	src    *querySource
	params paramTable
	// outputs are the checked output columns by name, as Columns and ORDER BY see them.
	outputs map[string]operand
	terms   []whereTerm
	// byColumn are the WHERE terms that bind each key column.
	byColumn map[string]*columnTerms
	// whereBad is set when a WHERE term has an error, which may hide a key binding.
	whereBad bool
	// aggregating is set for a read-time aggregation (sql.md §10.7).
	aggregating bool
	groupNames  []string
	// groupKeys are the GROUP BY keys by name, over the rows read.
	groupKeys map[string]operand
	// groupOK is false when a GROUP BY item has an error.
	groupOK bool
	aggs    []catalog.QueryAggregate
	// allEq is set when = binds every key column of a view.
	allEq bool
	q     catalog.Query
}

// compileQuery checks a named query (sql.md §10) and compiles it.
func (p *project) compileQuery(d *decl, s *sql.Query) {
	q := &queryCompiler{
		d: d, st: d.st, sel: s.Select,
		outputs: map[string]operand{}, groupKeys: map[string]operand{}, groupOK: true,
	}
	q.param = q.resolveParam
	q.src = p.querySource(d.st, s.Select.From)
	q.bad = map[string]bool{}
	if q.src != nil {
		q.columns = q.src.columns
		maps.Copy(q.bad, q.src.bad)
	}
	q.params = collectParams(d.st, s.Select)
	q.outs, q.byName = selectOutputs(q.st, q.sel.Items, q.sel.From.Name, q.isColumn)
	q.arrayJoins()
	q.where()
	q.aggregating = q.isAggregating()
	q.groupBy()
	q.selectItems()
	q.bindKeys()
	q.q.OrderBy = q.orderBy()
	q.q.Limit = q.limit()
	q.checkCardinality()
	q.warnRetention()
	if d.defines && d.earlier == nil {
		d.query = &queryInfo{query: q.catalogQuery(), params: q.params.first, outs: q.outs}
	}
}

// querySource resolves FROM (sql.md §10.1). It returns nil for a qualified name (DF5102),
// an unknown one (DF4001), or a statement that didn't compile.
func (p *project) querySource(st *stmt, from sql.Ident) *querySource {
	if qualified(st, from) {
		return nil
	}
	d, ok := p.tables[from.Name]
	switch {
	case !ok:
		st.errorf(from.Pos, "DF4001", didYouMean(from.Name, p.tableNames(declStream, declView)),
			"%s is not a view or a stream", from.Name)
	case d.view != nil:
		return viewSource(d.view)
	case d.stream != nil:
		return streamSource(d.stream)
	}
	return nil
}

func viewSource(v *viewInfo) *querySource {
	vv := &v.version
	src := &querySource{
		kind: catalog.SourceView, name: v.decl.name, view: v, columns: map[string]catalog.Type{},
		bad: v.badColumns, partition: vv.PartitionKeyLen, keysOK: v.keysOK,
	}
	for _, k := range vv.Keys {
		src.columns[k.Name] = k.Expr.Type
		src.keys = append(src.keys, k.Name)
	}
	for _, a := range vv.Aggregates {
		src.columns[a.Name] = a.Type
	}
	return src
}

func streamSource(s *streamInfo) *querySource {
	src := &querySource{
		kind: catalog.SourceStream, name: s.stream.Name, stream: s, columns: streamColumnTypes(&s.stream),
		bad: s.badColumns, keys: s.stream.PrimaryKey, partition: len(s.stream.PrimaryKey), keysOK: true,
	}
	for _, k := range src.keys {
		if _, ok := src.columns[k]; !ok || src.bad[k] {
			src.keysOK = false
		}
	}
	return src
}

// aggregate returns the view aggregate column named name, or nil.
func (s *querySource) aggregate(name string) *catalog.ViewAggregate {
	if s.view == nil {
		return nil
	}
	aggs := s.view.version.Aggregates
	if i := slices.IndexFunc(aggs, func(a catalog.ViewAggregate) bool { return a.Name == name }); i >= 0 {
		return &aggs[i]
	}
	return nil
}

// arrayJoins reports ARRAY JOIN, which only views take (DF4003).
func (q *queryCompiler) arrayJoins() {
	for _, aj := range q.sel.ArrayJoins {
		q.st.errorf(aj.Pos, "DF4003", "put the ARRAY JOIN in a view", "ARRAY JOIN is not allowed in queries")
		for _, item := range aj.Items {
			if item.Alias != nil {
				q.bad[item.Alias.Name] = true
			}
		}
	}
}

// warnRetention warns about a stream query that misses the events past the log window
// (DF4501).
func (q *queryCompiler) warnRetention() {
	if q.src == nil || q.src.stream == nil {
		return
	}
	s := q.src.stream.stream
	period, limited := s.LogRetention.Period()
	if !limited {
		return
	}
	q.st.warnf(q.sel.From.Pos, "DF4501", "query older events with Athena, or aggregate them in a view",
		"reads stream %s, which keeps events for %s after ingestion (LOG RETENTION). Older events are only in the archive.",
		s.Name, period)
}

// catalogQuery fills in the parts of the compiled query that need no more checks.
func (q *queryCompiler) catalogQuery() catalog.Query {
	cq := q.q
	cq.Name = q.d.name
	cq.Cardinality = catalog.CardinalityMany
	if a, ok := nameAnnotation(q.d.node); ok && !a.Many {
		cq.Cardinality = catalog.CardinalityOne
	}
	cq.ExposeHTTP = q.d.exposed()
	cq.Params = q.params.valid
	if q.src != nil {
		cq.Source = catalog.Source{Kind: q.src.kind, Name: q.src.name}
	}
	if q.aggregating {
		agg := &catalog.Aggregation{Aggregates: q.aggs}
		for _, name := range q.groupNames {
			agg.GroupBy = append(agg.GroupBy, catalog.NamedExpr{Name: name, Expr: q.groupKeys[name].ir})
		}
		cq.Aggregation = agg
	}
	return cq
}

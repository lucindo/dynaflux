// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"maps"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// viewInfo is a compiled view.
type viewInfo struct {
	decl *decl
	// version is version 1: M1 doesn't read dynaflux.lock.
	version catalog.ViewVersion
	// stream is the stream in FROM; nil when FROM names no compiled stream.
	stream *streamInfo
	// badColumns are the output columns with an invalid definition: what reads them is
	// reported already.
	badColumns map[string]bool
	// keysOK is set when the version's keys and partition key are complete, so queries
	// can be checked against them.
	keysOK bool
}

// viewCompiler is the state of compiling one CREATE MATERIALIZED VIEW.
type viewCompiler struct {
	p    *project
	st   *stmt
	s    *sql.CreateView
	info *viewInfo
	outs []*output
	// byName holds the first output of each name.
	byName map[string]*output
	// elements are the ARRAY JOIN aliases, bad when their array is.
	elements map[string]operand
	// groupNames are the GROUP BY names in order; groupOK is false when an item isn't a
	// name, so they can't be compared with ORDER BY.
	groupNames []string
	groupOK    bool
	// keys are the checked keys by name. quietKeys are the same, checked without
	// reporting, for WHERE, which runs before the keys.
	keys      map[string]operand
	quietKeys map[string]operand
	// guards are the divisors that WHERE rules out zero for (DF5501).
	guards []catalog.Expr
}

// compileView checks a CREATE MATERIALIZED VIEW (sql.md §9) and compiles it.
func (p *project) compileView(d *decl, s *sql.CreateView) {
	v := &viewCompiler{
		p: p, st: d.st, s: s,
		info:     &viewInfo{decl: d, badColumns: map[string]bool{}},
		byName:   map[string]*output{},
		elements: map[string]operand{},
		keys:     map[string]operand{}, quietKeys: map[string]operand{},
	}
	v.info.stream = p.viewSource(d.st, s.Select.From)
	vv := &v.info.version
	vv.Number, vv.State, vv.StorageFormat = 1, catalog.StateCurrent, catalog.StorageFormatVersion
	vv.Stream = s.Select.From.Name
	v.nameOutputs()
	vv.ArrayJoin = v.arrayJoins()
	v.groupBy()
	vv.Where = v.where()
	v.checkKeys()
	vv.Aggregates = v.aggregates()
	v.selectOrder()
	v.layout()
	vv.TTL = v.ttl()
	vv.Split = split(d.st, s.Split)
	vv.Mode = v.mode()
	// Fingerprint fails only on a type that doesn't marshal, which the checks never
	// produce. The fingerprint would then stay empty, and catalog.Marshal reject it.
	if fp, err := catalog.Fingerprint(vv); err == nil {
		vv.Fingerprint = fp
	}
	if d.earlier == nil {
		p.views[d.name] = v.info
	}
}

// viewSource resolves a view's FROM (sql.md §9.3). It returns nil, and nothing that
// needs the stream's columns is checked, when the name isn't a compiled stream: an unknown
// name (DF3001), a view (DF3002), or a stream whose statement is broken.
func (p *project) viewSource(st *stmt, from sql.Ident) *streamInfo {
	if qualified(st, from) {
		return nil
	}
	if s, ok := p.streams[from.Name]; ok {
		return s
	}
	d, ok := p.tables[from.Name]
	switch {
	case !ok:
		st.errorf(from.Pos, "DF3001", didYouMean(from.Name, p.tableNames(declStream)), "%s is not a stream", from.Name)
	case d.kind == declView:
		hint := ""
		if v, isView := d.node.(*sql.CreateView); isView && v.Select != nil {
			hint = "read the stream " + v.Select.From.Name + " that " + from.Name + " is built from"
		}
		st.errorf(from.Pos, "DF3002", hint, "%s is a view; views over views are not supported", from.Name)
	}
	return nil
}

// compiledViews returns the compiled views in source order.
func (p *project) compiledViews() []*viewInfo {
	var out []*viewInfo
	for _, d := range p.decls {
		if v, ok := p.views[d.name]; ok && v.decl == d {
			out = append(out, v)
		}
	}
	return out
}

// nameOutputs names the SELECT items. View output columns can't be pk or sk (DF1302).
func (v *viewCompiler) nameOutputs() {
	var isColumn func(string) bool
	if s := v.info.stream; s != nil {
		isColumn = func(name string) bool {
			_, ok := s.stream.Column(name)
			return ok || s.badColumns[name] || v.isElement(name)
		}
	}
	v.outs = selectOutputs(v.st, v.s.Select.Items, v.s.Select.From.Name, isColumn, catalog.AttrPK, catalog.AttrSK)
	for _, out := range v.outs {
		if out.name != "" && v.byName[out.name] == nil {
			v.byName[out.name] = out
		}
	}
}

// isElement reports whether name is an ARRAY JOIN alias.
func (v *viewCompiler) isElement(name string) bool {
	for _, aj := range v.s.Select.ArrayJoins {
		for _, item := range aj.Items {
			if item.Alias != nil && item.Alias.Name == name {
				return true
			}
		}
	}
	return false
}

// checker returns a checker for an expression of the view in place, whose names resolve
// with column.
func (v *viewCompiler) checker(st *stmt, place string, column func(*stmt, *sql.ColumnRef) operand) *checker {
	return &checker{st: st, guards: v.guards, env: env{
		place:  place,
		column: func(ref *sql.ColumnRef) operand { return column(st, ref) },
	}}
}

// sourceColumn resolves a column of the source row: a stream column, an implicit column
// or an ARRAY JOIN alias. found is false when ref names none of them.
func (v *viewCompiler) sourceColumn(st *stmt, ref *sql.ColumnRef) (operand, bool) {
	s := v.info.stream
	if s == nil || s.badColumns[ref.Name] {
		return badOperand(), true
	}
	if col, ok := s.stream.Column(ref.Name); ok {
		v.warnDedup(st, ref)
		return operand{ir: catalog.Expr{Op: catalog.OpColumn, Type: col.Type, Name: col.Name}, node: ref}, true
	}
	if e, ok := v.elements[ref.Name]; ok {
		e.node = ref
		return e, true
	}
	return operand{}, false
}

// warnDedup warns about a column that differs between the copies of a duplicate accepted
// in two regions (DF3504).
func (v *viewCompiler) warnDedup(st *stmt, ref *sql.ColumnRef) {
	s := v.info.stream.stream
	if len(s.DedupBy) == 0 || ref.Name != catalog.ColumnIngestTime && ref.Name != catalog.ColumnOriginRegion {
		return
	}
	st.warnf(ref.Pos, "DF3504", "group by event_time instead of ingest_time; avoid origin_region",
		"view reads %s from DEDUP stream %s; a duplicate accepted in two regions has a different %s in each",
		ref.Name, s.Name, ref.Name)
}

// selectColumn resolves a name inside a SELECT item, where only source columns resolve
// (DF5105).
func (v *viewCompiler) selectColumn(st *stmt, ref *sql.ColumnRef) operand {
	if o, ok := v.sourceColumn(st, ref); ok {
		return o
	}
	if isAlias(v.byName[ref.Name]) {
		return aliasInSelect(st, ref)
	}
	return unknownColumn(st, ref, v.visible(false))
}

// isAlias reports whether out names its item with AS; a plain column keeps its name.
func isAlias(out *output) bool {
	return out != nil && out.item.Alias != nil
}

// aliasInSelect reports an alias used inside a SELECT item (DF5105).
func aliasInSelect(st *stmt, ref *sql.ColumnRef) operand {
	st.errorf(ref.Pos, "DF5105", "aliases work in WHERE, GROUP BY and ORDER BY; repeat the expression",
		"alias %s can't be used here", ref.Name)
	return badOperand()
}

// whereColumn resolves a name in WHERE: a source column, or a key alias, which stands for
// its expression.
func (v *viewCompiler) whereColumn(st *stmt, ref *sql.ColumnRef) operand {
	if o, ok := v.sourceColumn(st, ref); ok {
		return o
	}
	out := v.byName[ref.Name]
	switch {
	case out == nil:
		return unknownColumn(st, ref, v.visible(true))
	case out.key:
		o := v.quietKey(out)
		o.node = ref
		return o
	}
	if call, ok := unparen(out.item.X).(*sql.Call); ok && resolveCall(st, call).kind == fnAggregate {
		st.errorf(ref.Pos, "DF5204", "", "aggregate %s is not allowed in WHERE", call.Name.Name)
	}
	return badOperand()
}

// visible returns the names that an unknown column's suggestion comes from: the source
// columns, and the SELECT aliases when they resolve.
func (v *viewCompiler) visible(aliases bool) []string {
	var names []string
	for _, c := range slices.Concat(v.info.stream.stream.Columns, catalog.ImplicitColumns()) {
		names = append(names, c.Name)
	}
	names = slices.AppendSeq(names, maps.Keys(v.elements))
	if aliases {
		names = slices.AppendSeq(names, maps.Keys(v.byName))
	}
	return names
}

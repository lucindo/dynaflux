// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"maps"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// viewInfo is a compiled view.
type viewInfo struct {
	decl *decl
	// version is the definition; numberViews gives it its number and state.
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
	// scope holds the stream's columns, then the ARRAY JOIN aliases.
	scope

	p    *project
	st   *stmt
	s    *sql.CreateView
	info *viewInfo
	// groupNames are the GROUP BY names in order; groupOK is false when an item isn't a
	// name, so they can't be compared with ORDER BY.
	groupNames []string
	groupOK    bool
	// keys are the checked keys by name. quietKeys are the same, checked without
	// reporting, for WHERE, which runs before the keys.
	keys      map[string]operand
	quietKeys map[string]operand
}

// compileView checks a CREATE MATERIALIZED VIEW (sql.md §9) and compiles it.
func (p *project) compileView(d *decl, s *sql.CreateView) {
	v := &viewCompiler{
		p: p, st: d.st, s: s,
		info: &viewInfo{decl: d, badColumns: map[string]bool{}},
		keys: map[string]operand{}, quietKeys: map[string]operand{},
	}
	v.info.stream = p.viewStream(d.st, s.Select.From)
	if src := v.info.stream; src != nil {
		v.columns, v.bad = streamColumnTypes(&src.stream), maps.Clone(src.badColumns)
		if len(src.stream.DedupBy) > 0 {
			v.dedup = &src.stream
		}
	}
	vv := &v.info.version
	vv.StorageFormat = catalog.StorageFormatVersion
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
		d.view = v.info
	}
}

// viewStream resolves a view's FROM (sql.md §9.3). It returns nil, and nothing that
// needs the stream's columns is checked, when the name isn't a compiled stream: an unknown
// name (DF3001), a view (DF3002), or a stream whose statement is broken.
func (p *project) viewStream(st *stmt, from sql.Ident) *streamInfo {
	if qualified(st, from) {
		return nil
	}
	d, ok := p.tables[from.Name]
	switch {
	case ok && d.stream != nil:
		return d.stream
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

// nameOutputs names the SELECT items. View output columns can't be pk or sk (DF1302), nor
// an ARRAY JOIN alias, which binds later (DF5103).
func (v *viewCompiler) nameOutputs() {
	isColumn := func(name string) bool { return v.isColumn(name) || v.columns != nil && v.isElement(name) }
	v.outs, v.byName = selectOutputs(v.st, v.s.Select.Items, v.s.Select.From.Name, isColumn, catalog.AttrPK, catalog.AttrSK)
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

// whereColumn resolves a name in WHERE: a source column, or a key alias, which stands for
// its expression.
func (v *viewCompiler) whereColumn(c *checker, ref *sql.ColumnRef) operand {
	st := c.st
	if o, ok := v.source(st, ref); ok {
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

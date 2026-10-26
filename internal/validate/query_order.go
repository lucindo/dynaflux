// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"reflect"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// orderBy checks ORDER BY (sql.md §10.8) and returns the full sort order: its items, then
// the row key columns they don't sort by, which break ties. The tie-breakers take the
// direction of the last item, so ORDER BY event_time DESC reads the key in reverse and
// LIMIT can stop the read.
func (q *queryCompiler) orderBy() []catalog.OrderItem {
	var items []catalog.OrderItem
	if ob := q.sel.OrderBy; ob != nil {
		for _, it := range ob.Items {
			if e, ok := q.orderExpr(it); ok {
				items = append(items, catalog.OrderItem{Expr: e, Desc: it.Desc, NullsFirst: it.NullsFirst})
			}
		}
	}
	desc := len(items) > 0 && items[len(items)-1].Desc
	for _, k := range q.rowKey() {
		if !slices.ContainsFunc(items, func(it catalog.OrderItem) bool { return reflect.DeepEqual(it.Expr, k) }) {
			items = append(items, catalog.OrderItem{Expr: k, Desc: desc})
		}
	}
	return items
}

// orderExpr checks an ORDER BY item: a name that can be ordered (DF4302).
func (q *queryCompiler) orderExpr(it *sql.OrderItem) (catalog.Expr, bool) {
	ref, ok := unparen(it.X).(*sql.ColumnRef)
	if !ok {
		q.notOutput(it.X)
		return catalog.Expr{}, false
	}
	if qualified(q.st, ref.Ident) {
		return catalog.Expr{}, false
	}
	o := q.orderColumn(ref)
	if o.bad {
		return catalog.Expr{}, false
	}
	if t := o.typ(); t.Array || t.Kind == catalog.KindUUID {
		q.st.errorf(it.X.Extent().Pos, "DF4302", "", "can't order by %s of type %s", q.st.text(it.X), t)
		return catalog.Expr{}, false
	}
	return o.ir, true
}

// orderColumn resolves an ORDER BY name: an output column, a source column when the query
// doesn't aggregate, or a GROUP BY key when it does (DF4301).
func (q *queryCompiler) orderColumn(ref *sql.ColumnRef) operand {
	if q.byName[ref.Name] != nil {
		return q.outputs[ref.Name]
	}
	o, found := q.source(q.st, ref)
	switch {
	case !found:
		return unknownColumn(q.st, ref, q.visible(true))
	case o.bad || !q.aggregating:
		return o
	case q.isGroupKey(ref.Name):
		return q.groupedKey(ref.Name)
	}
	q.notOutput(ref)
	return badOperand()
}

func (q *queryCompiler) notOutput(x sql.Expr) {
	q.st.errorf(x.Extent().Pos, "DF4301", "add it to SELECT with an alias and order by the alias",
		"ORDER BY %s is not an output column", q.st.text(x))
}

// rowKey returns the row key that breaks ties: the GROUP BY keys after aggregation, the
// view key, or (event_time, event_id) for a stream.
func (q *queryCompiler) rowKey() []catalog.Expr {
	var key []catalog.Expr
	switch {
	case q.aggregating:
		for _, name := range q.groupNames {
			if k := q.groupedKey(name); !k.bad {
				key = append(key, k.ir)
			}
		}
	case q.src == nil:
	case q.src.view != nil:
		for _, k := range q.src.view.version.Keys {
			key = append(key, catalog.Expr{Op: catalog.OpColumn, Type: k.Expr.Type, Name: k.Name})
		}
	default:
		for _, name := range []string{catalog.ColumnEventTime, catalog.ColumnEventID} {
			key = append(key, catalog.Expr{Op: catalog.OpColumn, Type: q.src.columns[name], Name: name})
		}
	}
	return key
}

// limit checks LIMIT: a whole number of at least 1, or an unsigned integer parameter
// (DF4303).
func (q *queryCompiler) limit() *catalog.Expr {
	l := q.sel.Limit
	if l == nil {
		return nil
	}
	if n, ok := l.X.(*sql.NumberLit); ok && !n.IsFloat && !n.Neg && n.Int >= 1 {
		lit := numberLiteral(n)
		return &lit
	}
	if p, ok := l.X.(*sql.Param); ok {
		o := q.checker(q.st, "LIMIT", nil).param(p, paramValue)
		if o.bad {
			return nil
		}
		if isInteger(o.typ()) && !isSigned(o.typ().Kind) {
			return &o.ir
		}
	}
	q.st.errorf(l.Pos, "DF4303", "", "LIMIT must be a whole number of at least 1, or an unsigned integer parameter")
	return nil
}

// checkCardinality accepts :one only for a query that can't return more than one row
// (DF4304, sql.md §10.9).
func (q *queryCompiler) checkCardinality() {
	a, ok := nameAnnotation(q.d.node)
	if !ok || a.Bad || a.Many || q.src == nil || !q.src.keysOK || q.whereBad || !q.groupOK {
		return
	}
	limitOne := q.q.Limit != nil && q.q.Limit.Op == catalog.OpLiteral && q.q.Limit.Value == "1"
	one := q.aggregating && q.sel.GroupBy == nil || q.allEq || q.sel.GroupBy != nil && q.groupsFixed() || limitOne
	if !one {
		q.st.errorf(a.Pos, "DF4304", "use :many, bind every view key with =, or add LIMIT 1",
			":one query can return more than one row")
	}
}

// groupsFixed reports whether a top-level WHERE term key = v fixes every GROUP BY key.
func (q *queryCompiler) groupsFixed() bool {
	for _, name := range q.groupNames {
		key := q.groupKeys[name].ir
		if !slices.ContainsFunc(q.terms, func(t whereTerm) bool { return fixes(t.o.ir, key) }) {
			return false
		}
	}
	return true
}

// fixes reports whether term is key = v or v = key, with v constant.
func fixes(term, key catalog.Expr) bool {
	if term.Op != catalog.OpEquals {
		return false
	}
	l, r := term.Args[0], term.Args[1]
	return reflect.DeepEqual(l, key) && isConstant(r) || reflect.DeepEqual(r, key) && isConstant(l)
}

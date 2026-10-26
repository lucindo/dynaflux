// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"maps"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// whereTerm is one top-level AND term of WHERE.
type whereTerm struct {
	x sql.Expr
	o operand
}

// nameOutputs names the SELECT items (sql.md §10.2).
func (q *queryCompiler) nameOutputs() {
	var isColumn func(string) bool
	if q.src != nil {
		isColumn = func(name string) bool {
			_, ok := q.src.columns[name]
			return ok || q.src.bad[name]
		}
	}
	q.outs = selectOutputs(q.st, q.sel.Items, q.sel.From.Name, isColumn)
	for _, out := range q.outs {
		if out.name != "" && q.byName[out.name] == nil {
			q.byName[out.name] = out
		}
	}
}

// newChecker returns a checker for an expression of the query in place, whose names
// resolve with column.
func (q *queryCompiler) newChecker(st *stmt, place string, column func(*stmt, *sql.ColumnRef) operand) *checker {
	return &checker{st: st, guards: q.guards, env: env{
		place:  place,
		column: func(ref *sql.ColumnRef) operand { return column(st, ref) },
		param:  func(p *sql.Param, role paramRole) operand { return q.param(st, p, role) },
	}}
}

// sourceColumn resolves a column of the rows the query reads. found is false when ref
// names none.
func (q *queryCompiler) sourceColumn(ref *sql.ColumnRef) (operand, bool) {
	if q.src == nil || q.src.bad[ref.Name] || q.elements[ref.Name] {
		return badOperand(), true
	}
	t, ok := q.src.columns[ref.Name]
	if !ok {
		return operand{}, false
	}
	return operand{ir: catalog.Expr{Op: catalog.OpColumn, Type: t, Name: ref.Name}, node: ref}, true
}

// selectColumn resolves a name inside a SELECT item, where only source columns resolve
// (DF5105).
func (q *queryCompiler) selectColumn(st *stmt, ref *sql.ColumnRef) operand {
	if o, ok := q.sourceColumn(ref); ok {
		return o
	}
	if isAlias(q.byName[ref.Name]) {
		return aliasInSelect(st, ref)
	}
	return unknownColumn(st, ref, q.visible(false))
}

// whereColumn resolves a name in WHERE: a source column, or an alias, which stands for its
// expression (sql.md §5.3).
func (q *queryCompiler) whereColumn(st *stmt, ref *sql.ColumnRef) operand {
	if o, ok := q.sourceColumn(ref); ok {
		return o
	}
	out := q.byName[ref.Name]
	if out == nil {
		return unknownColumn(st, ref, q.visible(true))
	}
	return q.aliasValue(st, ref, out, "WHERE")
}

// aliasValue returns the expression of an alias used in place, which can't hold an
// aggregate (DF5204). The SELECT item reports its own errors, so it is checked quietly.
func (q *queryCompiler) aliasValue(st *stmt, ref *sql.ColumnRef, out *output, place string) operand {
	if call := firstAggregate(st, out.item.X); call != nil {
		st.errorf(ref.Pos, "DF5204", "", "aggregate %s is not allowed in %s", call.Name.Name, place)
		return badOperand()
	}
	quiet := &stmt{d: &diags{}, f: st.f, name: st.name}
	o := q.newChecker(quiet, place, q.selectColumn).value(out.item.X)
	o.node = ref
	return o
}

// groupedColumn resolves a name inside a SELECT item of an aggregating query, outside
// aggregates: only GROUP BY keys are known per row (DF4207).
func (q *queryCompiler) groupedColumn(st *stmt, ref *sql.ColumnRef) operand {
	o, found := q.sourceColumn(ref)
	switch {
	case !found:
		return q.selectColumn(st, ref)
	case o.bad:
		return o
	case q.isGroupKey(ref.Name):
		return q.groupedKey(ref.Name)
	}
	st.errorf(ref.Pos, "DF4207", "", "%s must be a GROUP BY key or inside an aggregate", st.text(ref))
	return badOperand()
}

func (q *queryCompiler) isGroupKey(name string) bool {
	_, ok := q.groupKeys[name]
	return ok
}

// groupedKey returns a GROUP BY key as the aggregated rows hold it.
func (q *queryCompiler) groupedKey(name string) operand {
	k := q.groupKeys[name]
	if k.bad {
		return k
	}
	return operand{ir: catalog.Expr{Op: catalog.OpColumn, Type: k.typ(), Name: name}}
}

// visible returns the names that an unknown column's suggestion comes from: the source
// columns, and the SELECT aliases when they resolve.
func (q *queryCompiler) visible(aliases bool) []string {
	var names []string
	if q.src != nil {
		names = slices.AppendSeq(names, maps.Keys(q.src.columns))
	}
	if aliases {
		names = slices.AppendSeq(names, maps.Keys(q.byName))
	}
	return names
}

// where checks WHERE term by term, each guarding the divisors of the terms after it, and
// keeps the divisors it guards for the rest of the query.
func (q *queryCompiler) where() {
	w := q.sel.Where
	if w == nil {
		return
	}
	c := q.newChecker(q.st, "WHERE", q.whereColumn)
	for _, x := range andTerms(w.X) {
		o := c.condition(x)
		q.terms = append(q.terms, whereTerm{x: x, o: o})
		if o.bad {
			q.whereBad = true
			continue
		}
		c.guards = append(c.guards, nonZeroIfTrue(o.ir)...)
	}
	q.guards = c.guards
}

// andTerms splits a condition into its top-level AND terms.
func andTerms(x sql.Expr) []sql.Expr {
	if b, ok := unparen(x).(*sql.BinaryExpr); ok && b.Op == sql.OpAnd {
		return append(andTerms(b.X), andTerms(b.Y)...)
	}
	return []sql.Expr{x}
}

// isAggregating reports a read-time aggregation: GROUP BY, or an aggregate in SELECT.
func (q *queryCompiler) isAggregating() bool {
	if q.sel.GroupBy != nil {
		return true
	}
	return slices.ContainsFunc(q.outs, func(out *output) bool { return firstAggregate(q.st, out.item.X) != nil })
}

// groupBy resolves the GROUP BY names (sql.md §10.7): source columns, or aliases of SELECT
// items without aggregates, which are then checked here as keys.
func (q *queryCompiler) groupBy() {
	g := q.sel.GroupBy
	if g == nil {
		return
	}
	refs, ok := groupByNames(q.st, g)
	q.groupOK = ok
	for _, ref := range refs {
		k := q.groupKey(ref)
		q.groupNames = append(q.groupNames, ref.Name)
		q.groupKeys[ref.Name] = k
		q.groupOK = q.groupOK && !k.bad
	}
}

func (q *queryCompiler) groupKey(ref *sql.ColumnRef) operand {
	if o, ok := q.sourceColumn(ref); ok {
		return o
	}
	out := q.byName[ref.Name]
	switch {
	case out == nil:
		return unknownColumn(q.st, ref, q.visible(true))
	case firstAggregate(q.st, out.item.X) != nil:
		return q.aliasValue(q.st, ref, out, "GROUP BY")
	}
	out.key = true
	return q.newChecker(q.st, "SELECT", q.selectColumn).value(out.item.X)
}

// selectItems checks the SELECT items and sets the output columns.
func (q *queryCompiler) selectItems() {
	for _, out := range q.outs {
		o := q.item(out)
		if out.name == "" {
			continue
		}
		if _, seen := q.columns[out.name]; !seen {
			q.columns[out.name] = o
		}
		if !o.bad {
			q.q.Columns = append(q.q.Columns, catalog.NamedExpr{Name: out.name, Expr: o.ir})
		}
	}
}

func (q *queryCompiler) item(out *output) operand {
	if out.key {
		return q.groupedKey(out.name)
	}
	if !q.aggregating {
		return q.newChecker(q.st, "SELECT", q.selectColumn).value(out.item.X)
	}
	c := q.newChecker(q.st, "SELECT", q.groupedColumn)
	c.env.aggregate = q.aggregate
	return c.value(out.item.X)
}

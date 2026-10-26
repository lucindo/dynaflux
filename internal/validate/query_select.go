// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// whereTerm is one top-level AND term of WHERE.
type whereTerm struct {
	x sql.Expr
	o operand
}

// whereColumn resolves a name in WHERE: a source column, or an alias, which stands for its
// expression (sql.md §5.3).
func (q *queryCompiler) whereColumn(c *checker, ref *sql.ColumnRef) operand {
	if o, ok := q.source(c.st, ref); ok {
		return o
	}
	out := q.byName[ref.Name]
	if out == nil {
		return unknownColumn(c.st, ref, q.visible(true))
	}
	return q.aliasValue(c.st, ref, out, "WHERE")
}

// aliasValue returns the expression of an alias used in place, which can't hold an
// aggregate (DF5204). The SELECT item reports its own errors, so it is checked quietly.
func (q *queryCompiler) aliasValue(st *stmt, ref *sql.ColumnRef, out *output, place string) operand {
	if call := firstAggregate(st, out.item.X); call != nil {
		st.errorf(ref.Pos, "DF5204", "", "aggregate %s is not allowed in %s", call.Name.Name, place)
		return badOperand()
	}
	o := q.checker(st.quiet(), place, q.selectColumn).value(out.item.X)
	o.node = ref
	return o
}

// groupedColumn resolves a name inside a SELECT item of an aggregating query, outside
// aggregates: only GROUP BY keys are known per row (DF4207).
func (q *queryCompiler) groupedColumn(c *checker, ref *sql.ColumnRef) operand {
	st := c.st
	o, found := q.source(st, ref)
	switch {
	case !found:
		return q.selectColumn(c, ref)
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

// where checks WHERE term by term, each guarding the divisors of the terms after it, and
// keeps the divisors it guards for the rest of the query.
func (q *queryCompiler) where() {
	w := q.sel.Where
	if w == nil {
		return
	}
	c := q.checker(q.st, "WHERE", q.whereColumn)
	q.byColumn = map[string]*columnTerms{}
	for i, x := range andTerms(w.X) {
		t := whereTerm{x: x, o: c.condition(x)}
		q.terms = append(q.terms, t)
		if t.o.bad {
			q.whereBad = true
			continue
		}
		c.guards = append(c.guards, nonZeroIfTrue(t.o.ir)...)
		if q.src != nil {
			q.fileKeyTerm(i, t)
		}
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
		if t := k.typ(); !k.bad && t.Array {
			q.st.errorf(ref.Pos, "DF4208", "to group by its elements, ARRAY JOIN it in a view",
				"can't group by %s of type %s", ref.Name, t)
			k = badOperand()
		}
		q.groupNames = append(q.groupNames, ref.Name)
		q.groupKeys[ref.Name] = k
		q.groupOK = q.groupOK && !k.bad
	}
}

func (q *queryCompiler) groupKey(ref *sql.ColumnRef) operand {
	if o, ok := q.source(q.st, ref); ok {
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
	return q.checker(q.st, "SELECT", q.selectColumn).value(out.item.X)
}

// selectItems checks the SELECT items and sets the output columns.
func (q *queryCompiler) selectItems() {
	for _, out := range q.outs {
		o := q.item(out)
		if out.name == "" {
			continue
		}
		if _, seen := q.outputs[out.name]; !seen {
			q.outputs[out.name] = o
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
		return q.checker(q.st, "SELECT", q.selectColumn).value(out.item.X)
	}
	c := q.checker(q.st, "SELECT", q.groupedColumn)
	c.env.aggregate = q.aggregate
	return c.value(out.item.X)
}

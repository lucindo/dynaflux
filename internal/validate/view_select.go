// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/goname"
	"github.com/lucindo/dynaflux/internal/sql"
)

// arrayJoins checks the ARRAY JOIN clauses (sql.md §9.5): one inner ARRAY JOIN of one
// array. Every alias is bound, even in a clause with an error, so that the rest of the
// view isn't reported for it.
func (v *viewCompiler) arrayJoins() *catalog.ArrayJoin {
	var first *catalog.ArrayJoin
	for i, aj := range v.s.Select.ArrayJoins {
		if aj.Left {
			v.st.errorf(aj.Pos, "DF3202", "use ARRAY JOIN, which skips events with empty arrays",
				"LEFT ARRAY JOIN is not supported in v1")
		}
		if i > 0 || len(aj.Items) > 1 {
			v.st.errorf(aj.Pos, "DF3203", "use one view per array", "ARRAY JOIN takes one array in v1")
		}
		for _, item := range aj.Items {
			if ir := v.arrayItem(aj, item); first == nil {
				first = ir
			}
		}
	}
	return first
}

// arrayItem checks one array of an ARRAY JOIN and binds its alias to the element type.
func (v *viewCompiler) arrayItem(aj *sql.ArrayJoin, item *sql.ArrayItem) *catalog.ArrayJoin {
	o := v.checker(v.st, "ARRAY JOIN", v.selectColumn).value(item.X)
	if !o.bad && !o.typ().Array {
		v.st.errorf(item.X.Extent().Pos, "DF3204", "", "ARRAY JOIN %s is not an Array", v.st.text(item.X))
		o = badOperand()
	}
	alias := item.Alias
	if alias == nil {
		element := "element"
		if ref, ok := item.X.(*sql.ColumnRef); ok && goname.Singular(ref.Name) != ref.Name {
			element = goname.Singular(ref.Name)
		}
		v.st.errorf(aj.Pos, "DF3201", "ARRAY JOIN "+v.st.text(item.X)+" AS "+element, "ARRAY JOIN needs an alias")
		return nil
	}
	checkName(v.st, *alias, catalog.AttrPK, catalog.AttrSK)
	element := o.typ()
	element.Array = false
	if s := v.info.stream; s != nil {
		// A stream column wins over an alias of the same name.
		if _, ok := s.stream.Column(alias.Name); ok || s.badColumns[alias.Name] {
			aliasIsColumn(v.st, *alias, s.stream.Name)
		} else {
			v.columns[alias.Name], v.bad[alias.Name] = element, o.bad
		}
	}
	if o.bad {
		return nil
	}
	return &catalog.ArrayJoin{Array: o.ir, Alias: alias.Name}
}

// groupBy resolves the GROUP BY items to the SELECT items they name: the view's keys
// (sql.md §9.3).
func (v *viewCompiler) groupBy() {
	g := v.s.Select.GroupBy
	if g == nil {
		v.st.errorf(v.s.Pos, "DF3003", "add GROUP BY with the view's keys", "view SELECT has no GROUP BY")
		return
	}
	refs, ok := groupByNames(v.st, g)
	v.groupOK = ok
	for _, ref := range refs {
		v.groupNames = append(v.groupNames, ref.Name)
		if out := v.byName[ref.Name]; out != nil {
			out.key = true
			continue
		}
		v.st.errorf(ref.Pos, "DF3005", "add "+ref.Name+" to SELECT", "GROUP BY %s does not name a SELECT item", ref.Name)
	}
}

// where checks WHERE, which can use key aliases, and keeps the divisors it guards.
func (v *viewCompiler) where() *catalog.Expr {
	w := v.s.Select.Where
	if w == nil {
		return nil
	}
	o := v.checker(v.st, "WHERE", v.whereColumn).condition(w.X)
	if o.bad {
		return nil
	}
	v.guards = nonZeroIfTrue(o.ir)
	return &o.ir
}

// checkKeys checks each key: an expression over source columns, without aggregates
// (DF5204), non-Nullable (DF3103) and of a key type (DF3104).
func (v *viewCompiler) checkKeys() {
	for _, name := range v.groupNames {
		out := v.byName[name]
		if out == nil {
			continue
		}
		o := v.key(v.st, out)
		v.keys[name] = o
		if o.bad {
			v.info.badColumns[name] = true
		}
	}
}

func (v *viewCompiler) key(st *stmt, out *output) operand {
	x := out.item.X
	o := v.checker(st, "GROUP BY", v.selectColumn).value(x)
	switch {
	case o.bad:
	case o.typ().Nullable:
		st.errorf(x.Extent().Pos, "DF3103", nullableKeyHint(st, out, o.typ()), "key %s is Nullable", out.name)
	case !isKeyType(o.typ()):
		st.errorf(x.Extent().Pos, "DF3104", "", "key %s has type %s, which can't be a view key", out.name, o.typ())
	default:
		return o
	}
	return badOperand()
}

// nullableKeyHint suggests a coalesce for a Nullable key of type t, named as the key, or
// <key>_key when the key is a column without an alias.
func nullableKeyHint(st *stmt, out *output, t catalog.Type) string {
	def, ok := typeDefault(t)
	if !ok {
		return "use a non-Nullable UUID: coalesce can't turn a string into a UUID"
	}
	name := out.name
	if out.item.Alias == nil {
		name += "_key" // an alias can't be the column's own name (DF5103)
	}
	return "group by coalesce(" + st.text(out.item.X) + ", " + def + ") AS " + name
}

// quietKey returns a key for WHERE, checked without reporting: its own check reports.
func (v *viewCompiler) quietKey(out *output) operand {
	if o, ok := v.quietKeys[out.name]; ok {
		return o
	}
	o := v.key(v.st.quiet(), out)
	v.quietKeys[out.name] = o
	return o
}

// aggregates checks the SELECT items that aren't keys: each is a single aggregate call
// (sql.md §9.3).
func (v *viewCompiler) aggregates() []catalog.ViewAggregate {
	var aggs []catalog.ViewAggregate
	for _, out := range v.outs {
		if out.key {
			continue
		}
		agg, ok := v.aggregateItem(out)
		switch {
		case out.name == "":
		case !ok:
			v.info.badColumns[out.name] = true
		default:
			aggs = append(aggs, catalog.ViewAggregate{Aggregate: agg, Name: out.name})
		}
	}
	return aggs
}

func (v *viewCompiler) aggregateItem(out *output) (catalog.Aggregate, bool) {
	x := out.item.X
	c := v.checker(v.st, "SELECT", v.selectColumn)
	if call, ok := unparen(x).(*sql.Call); ok {
		fn := resolveCall(v.st, call)
		switch {
		case fn.kind == fnAggregate && !call.Distinct:
			return v.aggregate(c, call, fn.agg)
		case fn.kind == fnAggregate, fn.kind == fnUnknown:
			c.expr(call) // reports why the call is invalid
			return catalog.Aggregate{}, false
		}
	}
	v.st.errorf(x.Extent().Pos, "DF3004", "compute expressions over aggregates in a query",
		"%s must be a GROUP BY key or a single aggregate", v.st.text(x))
	return catalog.Aggregate{}, false
}

// aggregate checks an aggregate column (sql.md §7). It warns about argMax or argMin of a
// Nullable argument, which never records a NULL (DF3503).
func (v *viewCompiler) aggregate(c *checker, x *sql.Call, fn aggregateFunc) (catalog.Aggregate, bool) {
	if fn.state || fn.merge {
		v.st.errorf(x.Pos, "DF3105", "write the plain aggregate; views store aggregate states themselves",
			"%s is not allowed in a view", x.Name.Name)
		return catalog.Aggregate{}, false
	}
	agg, ok := c.aggregateCall(x, fn)
	if !ok {
		return agg, false
	}
	latest := agg.Fn == catalog.AggArgMax || agg.Fn == catalog.AggArgMin
	if latest && agg.Args[0].Type.Nullable {
		v.st.warnf(x.Pos, "DF3503", sentinelHint(v.st, x, agg.Args[0].Type),
			"%s skips rows where %s is NULL, so NULL never replaces a value", x.Name.Name, v.st.text(x.Args[0]))
	}
	return agg, true
}

// sentinelHint suggests coalescing the Nullable first argument of argMax or argMin x, of
// type t, with its type's default, or returns "" when the type has none.
func sentinelHint(st *stmt, x *sql.Call, t catalog.Type) string {
	def, ok := typeDefault(t)
	if !ok {
		return ""
	}
	args := []string{"coalesce(" + st.text(x.Args[0]) + ", " + def + ")"}
	for _, a := range x.Args[1:] {
		args = append(args, st.text(a))
	}
	return "use a sentinel: " + x.Name.Name + "(" + strings.Join(args, ", ") + ")"
}

// selectOrder reports ORDER BY and LIMIT in a view's SELECT (DF3006).
func (v *viewCompiler) selectOrder() {
	const hint = "set the key order with the view's ORDER BY clause, before AS"
	sel := v.s.Select
	if sel.OrderBy != nil {
		v.st.errorf(sel.OrderBy.Pos, "DF3006", hint, "a view SELECT can't have ORDER BY")
	}
	if sel.Limit != nil {
		v.st.errorf(sel.Limit.Pos, "DF3006", hint, "a view SELECT can't have LIMIT")
	}
}

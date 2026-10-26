// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"cmp"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
)

// paramTable holds the parameters of a query (sql.md §10.3), resolved in source order
// before anything else is checked, so the first use of a name is the one in the text.
type paramTable struct {
	ops map[*sql.Param]operand
	// first is the first use of each name, in source order.
	first []*sql.Param
	// valid are the parameters with a valid name and type, in source order.
	valid []catalog.Param
}

// paramName is what the uses of one parameter name so far establish.
type paramName struct {
	nameOK bool
	typed  bool
	typ    catalog.Type
	at     diag.Pos // the use that set typ
}

// collectParams resolves every parameter of a query. It reports an invalid name (DF1301,
// DF1302), a type that parameters can't have (DF4401), and a name used with two types
// (DF4402).
func collectParams(st *stmt, sel *sql.Select) paramTable {
	var uses []*sql.Param
	for _, x := range selectExprs(sel) {
		inspect(x, func(e sql.Expr) {
			if p, ok := e.(*sql.Param); ok {
				uses = append(uses, p)
			}
		})
	}
	slices.SortFunc(uses, func(a, b *sql.Param) int { return cmp.Compare(a.Start, b.Start) })
	t := paramTable{ops: map[*sql.Param]operand{}}
	names := map[string]*paramName{}
	for _, p := range uses {
		n := names[p.Name.Name]
		if n == nil {
			n = &paramName{nameOK: checkName(st, p.Name)}
			names[p.Name.Name] = n
			t.first = append(t.first, p)
		}
		t.ops[p] = n.use(st, p)
	}
	for _, p := range t.first {
		if n := names[p.Name.Name]; n.nameOK && n.typed {
			t.valid = append(t.valid, catalog.Param{Name: p.Name.Name, Type: n.typ})
		}
	}
	return t
}

func (n *paramName) use(st *stmt, p *sql.Param) operand {
	typ, ok := paramType(st, p.Type)
	switch {
	case !ok || !n.nameOK:
		return badOperand()
	case n.typed && typ != n.typ:
		st.errorf(p.Pos, "DF4402", "use one type per parameter", "parameter %s is %s here but %s at %d:%d",
			p.Name.Name, typ, n.typ, n.at.Line, n.at.Col)
		return badOperand()
	case !n.typed:
		n.typed, n.typ, n.at = true, typ, p.Pos
	}
	return operand{ir: catalog.Expr{Op: catalog.OpParam, Type: typ, Name: p.Name.Name}, node: p}
}

// paramType resolves the type of a parameter: a scalar type, or an Array of one, but no
// Nullable or LowCardinality (DF4401).
func paramType(st *stmt, t *sql.Type) (catalog.Type, bool) {
	inner := t
	if t.Name.Name == typeArray && len(t.Args) == 1 {
		if elem, ok := t.Args[0].(*sql.Type); ok {
			inner = elem
		}
	}
	if inner.Name.Name == typeNullable || inner.Name.Name == typeLowCardinality || inner != t && inner.Name.Name == typeArray {
		st.errorf(t.Pos, "DF4401", "", "parameter type %s is not supported", st.text(t))
		return catalog.Type{}, false
	}
	return resolveType(st, t)
}

// param resolves a parameter where it appears: an Array parameter goes right after IN,
// and only there (DF4405).
func (q *queryCompiler) param(st *stmt, p *sql.Param, role paramRole) operand {
	o := q.params.ops[p]
	if o.bad {
		return o
	}
	if o.typ().Array != (role == paramAfterIn) {
		elem := o.typ()
		elem.Array = false
		st.errorf(p.Pos, "DF4405", "write x IN {"+p.Name.Name+":Array("+elem.String()+")}",
			"parameter %s is misplaced: an Array parameter goes only right after IN or NOT IN, and a parameter there must be an Array",
			p.Name.Name)
		return badOperand()
	}
	return o
}

// selectExprs returns the expressions of a SELECT, clause by clause.
func selectExprs(sel *sql.Select) []sql.Expr {
	var xs []sql.Expr
	for _, item := range sel.Items {
		if !item.Star {
			xs = append(xs, item.X)
		}
	}
	for _, aj := range sel.ArrayJoins {
		for _, item := range aj.Items {
			xs = append(xs, item.X)
		}
	}
	if sel.Where != nil {
		xs = append(xs, sel.Where.X)
	}
	if sel.GroupBy != nil {
		xs = append(xs, sel.GroupBy.Items...)
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			xs = append(xs, item.X)
		}
	}
	if sel.Limit != nil {
		xs = append(xs, sel.Limit.X)
	}
	return xs
}

// inspect calls f for x and every expression inside it.
func inspect(x sql.Expr, f func(sql.Expr)) {
	f(x)
	for _, c := range children(x) {
		inspect(c, f)
	}
}

func children(x sql.Expr) []sql.Expr {
	switch x := x.(type) {
	case *sql.Call:
		return slices.Concat(x.Params, x.Args)
	case *sql.ParenExpr:
		return []sql.Expr{x.X}
	case *sql.UnaryExpr:
		return []sql.Expr{x.X}
	case *sql.BinaryExpr:
		return []sql.Expr{x.X, x.Y}
	case *sql.InExpr:
		if x.Param != nil {
			return []sql.Expr{x.X, x.Param}
		}
		return append([]sql.Expr{x.X}, x.List...)
	case *sql.BetweenExpr:
		return []sql.Expr{x.X, x.Lo, x.Hi}
	case *sql.IsNullExpr:
		return []sql.Expr{x.X}
	case *sql.ArrayLit:
		return x.Elems
	case *sql.NumberLit, *sql.StringLit, *sql.BoolLit, *sql.NullLit, *sql.Param, *sql.ColumnRef, *sql.Interval:
	}
	return nil
}

// firstAggregate returns the first aggregate call in x, or nil.
func firstAggregate(st *stmt, x sql.Expr) *sql.Call {
	var found *sql.Call
	inspect(x, func(e sql.Expr) {
		if c, ok := e.(*sql.Call); ok && found == nil && resolveCall(st, c).kind == fnAggregate {
			found = c
		}
	})
	return found
}

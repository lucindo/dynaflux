// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// columnDefault checks a column's DEFAULT and computes its value (sql.md §8.2): a literal
// of the column's type, an array of them, or nil. typeOK is false when the column's type
// didn't resolve, so only the expression is checked.
func columnDefault(st *stmt, col *sql.Column, t catalog.Type, typeOK bool) *catalog.Expr {
	clause := col.Default
	if clause == nil {
		return nil
	}
	x := unparen(clause.X)
	_, isNull := x.(*sql.NullLit)
	if typeOK && t.Nullable && !isNull {
		st.errorf(col.Pos, "DF2006", "remove the DEFAULT: events stored before the column existed read it as NULL",
			"Nullable column %s can't have DEFAULT %s", col.Name.Name, st.text(clause.X))
		return nil
	}
	if arr, ok := x.(*sql.ArrayLit); ok {
		return arrayDefault(st, clause, arr, t, typeOK)
	}
	o := defaultChecker(st, clause).valueOrNull(clause.X)
	if o.bad || !typeOK || isNull && t.Nullable {
		return nil
	}
	v, vt, err := defaultValue(o)
	if err == nil {
		if lit, ok := fitValue(v, vt, t); ok {
			return &lit
		}
	}
	invalidDefault(st, clause, t)
	return nil
}

// defaultChecker checks a DEFAULT, which is a constant expression: column references are
// DF2003, reported once at the clause.
func defaultChecker(st *stmt, clause *sql.ExprClause) *checker {
	reported := false
	return &checker{st: st, env: env{
		place: "DEFAULT",
		column: func(*sql.ColumnRef) operand {
			if !reported {
				st.errorf(clause.Pos, "DF2003", "write the value, such as DEFAULT '' or DEFAULT 0",
					"DEFAULT must be a constant expression")
				reported = true
			}
			return badOperand()
		},
	}}
}

// defaultValue computes a checked DEFAULT. A number literal stands for its exact text, so
// that DEFAULT 999.99 fits Decimal(5, 2).
func defaultValue(o operand) (cval, catalog.Type, error) {
	if o.null {
		return cval{null: true}, catalog.Type{}, nil
	}
	if n, ok := o.node.(*sql.NumberLit); ok {
		return cval{num: exactValue(n)}, exactNumber(), nil
	}
	v, err := eval(o.ir)
	return v, o.typ(), err
}

// exactNumber is the type that marks a cval holding an exact number.
func exactNumber() catalog.Type {
	return catalog.Type{Kind: catalog.KindDecimal}
}

func invalidDefault(st *stmt, clause *sql.ExprClause, t catalog.Type) {
	st.errorf(clause.Pos, "DF2004", "", "DEFAULT %s is not a valid %s", st.text(clause.X), t)
}

func arrayDefault(st *stmt, clause *sql.ExprClause, arr *sql.ArrayLit, t catalog.Type, typeOK bool) *catalog.Expr {
	if !typeOK {
		return nil
	}
	if !t.Array || len(arr.Elems) > catalog.MaxArrayLen {
		invalidDefault(st, clause, t)
		return nil
	}
	elem := t
	elem.Array = false
	out := &catalog.Expr{Op: catalog.OpArray, Type: t}
	for _, e := range arr.Elems {
		lit, ok := literalDefault(e, elem)
		if !ok {
			invalidDefault(st, clause, t)
			return nil
		}
		out.Args = append(out.Args, lit)
	}
	return out
}

// literalDefault converts an element of an array literal to the element type.
func literalDefault(x sql.Expr, t catalog.Type) (catalog.Expr, bool) {
	if n, ok := x.(*sql.NumberLit); ok {
		return fitValue(cval{num: exactValue(n)}, exactNumber(), t)
	}
	if s, ok := x.(*sql.StringLit); ok {
		return fitValue(cval{str: s.Value}, catalog.Type{Kind: catalog.KindString}, t)
	}
	if b, ok := x.(*sql.BoolLit); ok {
		return fitValue(cval{b: b.Value}, catalog.Type{Kind: catalog.KindBool}, t)
	}
	return catalog.Expr{}, false
}

// fitValue returns v, of type vt, as a literal of the scalar type t when it fits exactly
// (sql.md §4.6): numbers by exact value, and temporal and UUID values also from strings.
func fitValue(v cval, vt, t catalog.Type) (catalog.Expr, bool) {
	t = scalarOf(t)
	vt = scalarOf(vt)
	switch {
	case v.null || vt.Array || t.Array:
		return catalog.Expr{}, false
	case isNumeric(t):
		if !isNumeric(vt) {
			return catalog.Expr{}, false
		}
		return fitNumber(num(v, vt), t)
	case isTemporal(t):
		return fitTime(v, vt, t)
	case t.Kind == catalog.KindUUID:
		id, ok := v.str, vt.Kind == catalog.KindUUID
		if vt.Kind == catalog.KindString {
			id, ok = parseUUID(v.str)
		}
		return literal(t, id), ok
	case t.Kind == catalog.KindBool:
		return boolLiteral(v.b), vt.Kind == catalog.KindBool
	}
	return literal(t, v.str), vt.Kind == catalog.KindString
}

func fitNumber(r *big.Rat, t catalog.Type) (catalog.Expr, bool) {
	switch {
	case isFloat(t):
		f, _ := r.Float64()
		return literal(t, formatFloat(f)), floatInRange(f)
	case isInteger(t):
		lo, hi := intRange(t)
		return literal(t, r.Num().String()), r.IsInt() && r.Num().Cmp(lo) >= 0 && r.Num().Cmp(hi) <= 0
	}
	m := new(big.Rat).Mul(r, new(big.Rat).SetInt(pow10(t.Scale)))
	if !m.IsInt() || new(big.Int).Abs(m.Num()).Cmp(pow10(t.Precision)) >= 0 {
		return catalog.Expr{}, false
	}
	return literal(t, formatDecimal(m.Num(), t.Scale)), true
}

// floatInRange reports whether DynamoDB can store f (sql.md §4.3): 0, or a magnitude
// from 1e-130 to just under 1e126.
func floatInRange(f float64) bool {
	const smallest, largest = 1e-130, 9.9999999999999999999999999999999999999e125
	a := math.Abs(f)
	return a == 0 || a >= smallest && a <= largest
}

// fitTime fits a temporal value, or a string in the forms of sql.md §4.6, to the temporal
// type t: it must be in range and lose no precision.
func fitTime(v cval, vt, t catalog.Type) (catalog.Expr, bool) {
	tm, ok := v.tm, isTemporal(vt)
	if vt.Kind == catalog.KindString {
		tm, ok = parseTime(v.str, t)
	}
	const day = 24 * time.Hour
	step := time.Second
	if t.Kind == catalog.KindDate {
		step = day
	}
	if t.Kind == catalog.KindDateTime64 {
		step = time.Duration(math.Pow10(nanoDigits - t.Precision))
	}
	ok = ok && tm.Equal(tm.Truncate(step)) && inRange(tm, t)
	return literal(t, formatTime(tm, t)), ok
}

// retentionText writes an interval as diagnostics show it: n UNIT, such as 2 DAY.
func retentionText(i catalog.Interval) string {
	return strconv.Itoa(i.N) + " " + string(i.Unit)
}

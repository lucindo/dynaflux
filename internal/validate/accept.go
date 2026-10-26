// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"math"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// defaultFutureMinutes is the upper bound of the default ACCEPT window.
const defaultFutureMinutes = 5

// acceptWindow checks ACCEPT (sql.md §8.7) and compiles it.
func acceptWindow(st *stmt, clause *sql.ExprClause) catalog.Accept {
	window := catalog.Accept{Future: catalog.Interval{N: defaultFutureMinutes, Unit: catalog.UnitMinute}}
	if clause == nil {
		return window
	}
	col, lower, upper, ok := acceptShape(clause.X)
	ref, isRef := col.(*sql.ColumnRef)
	switch {
	case isRef && qualified(st, ref.Ident):
		return window
	case isRef && ref.Name != catalog.ColumnEventTime:
		st.errorf(clause.Pos, "DF2110", "ACCEPT event_time BETWEEN now() - INTERVAL 30 DAY AND now() + INTERVAL 5 MINUTE",
			"ACCEPT must constrain event_time")
		return window
	case ok && isRef && upper == nil:
		st.errorf(clause.Pos, "DF2113", "ACCEPT event_time <= now() + INTERVAL 5 MINUTE is the default",
			"ACCEPT needs an upper bound on event_time")
		return window
	case !ok || !isRef:
		acceptShapeError(st, clause)
		return window
	}
	future, upperShaped, upperOK := acceptBound(st, upper, 1, "upper")
	past, lowerShaped, lowerOK := catalog.Interval{}, true, true
	if lower != nil {
		past, lowerShaped, lowerOK = acceptBound(st, lower, -1, "lower")
	}
	if !upperShaped || !lowerShaped {
		acceptShapeError(st, clause)
	}
	if !upperOK || !lowerOK {
		return window
	}
	window.Future = future
	if lower != nil {
		window.Past = &past
	}
	return window
}

// acceptShape splits ACCEPT col BETWEEN lower AND upper, col <= upper, and col >= lower
// or col > lower, which lack an upper bound.
func acceptShape(x sql.Expr) (sql.Expr, sql.Expr, sql.Expr, bool) {
	if b, ok := unparen(x).(*sql.BetweenExpr); ok {
		return b.X, b.Lo, b.Hi, !b.Not
	}
	b, ok := unparen(x).(*sql.BinaryExpr)
	if !ok {
		return nil, nil, nil, false
	}
	switch b.Op {
	case sql.OpLe:
		return b.X, nil, b.Y, true
	case sql.OpGe, sql.OpGt:
		return b.X, b.Y, nil, true
	case sql.OpOr, sql.OpAnd, sql.OpEq, sql.OpNe, sql.OpLt, sql.OpAdd, sql.OpSub, sql.OpMul, sql.OpDiv, sql.OpMod:
	}
	return b.X, nil, nil, false
}

func acceptShapeError(st *stmt, clause *sql.ExprClause) {
	st.errorf(clause.Pos, "DF2111", "",
		"ACCEPT must be event_time BETWEEN <bound> AND <bound>, or event_time <= <bound>, where a bound is now() or now() plus or minus an INTERVAL")
}

// acceptBound compiles a bound now() or now() ± INTERVAL, whose sign must be 0 or the
// given direction (DF2112). It returns the interval, whether the bound has that shape, and
// whether it is valid. The caller reports another shape, once for both bounds (DF2111).
func acceptBound(st *stmt, x sql.Expr, direction int, which string) (catalog.Interval, bool, bool) {
	x = unparen(x)
	if isNow(x) {
		return catalog.Interval{N: 0, Unit: catalog.UnitSecond}, true, true
	}
	b, ok := x.(*sql.BinaryExpr)
	if !ok || b.Op != sql.OpAdd && b.Op != sql.OpSub || !isNow(unparen(b.X)) {
		return catalog.Interval{}, false, false
	}
	iv, ok := unparen(b.Y).(*sql.Interval)
	if !ok {
		return catalog.Interval{}, false, false
	}
	interval, ok := checkInterval(st, iv)
	if !ok {
		return catalog.Interval{}, true, false
	}
	sign := 1
	if b.Op == sql.OpSub {
		sign = -1
	}
	if sign != direction {
		st.errorf(b.Pos, "DF2112", "the lower bound is now() - INTERVAL …, the upper bound now() + INTERVAL …",
			"ACCEPT %s bound is on the wrong side of now()", which)
		return catalog.Interval{}, true, false
	}
	return interval, true, true
}

func isNow(x sql.Expr) bool {
	c, ok := x.(*sql.Call)
	return ok && strings.EqualFold(c.Name.Name, "now") && len(c.Args) == 0 && !c.Star && !c.Parametric
}

// checkInterval checks INTERVAL n unit (sql.md §8.1, DF5404).
func checkInterval(st *stmt, iv *sql.Interval) (catalog.Interval, bool) {
	n, isNumber := iv.Value.(*sql.NumberLit)
	unit, unitOK := catalog.ParseUnit(iv.Unit.Name)
	var reason string
	switch {
	case iv.To != nil:
		reason = "INTERVAL … TO … is not supported"
	case !isNumber:
		reason = "the value must be a number, not a string"
	case iv.Neg:
		reason = "the value -" + n.Text + " is negative"
	case n.IsFloat:
		reason = n.Text + " is not a whole number"
	case n.Int == 0:
		reason = "the value must be at least 1"
	case !unitOK:
		reason = iv.Unit.Name + " is not a unit; write a unit in full, such as MINUTE"
	case n.Int > unit.Max():
		reason = n.Text + " " + string(unit) + " is more than 100 years"
	default:
		return catalog.Interval{N: int(min(n.Int, math.MaxInt)), Unit: unit}, true
	}
	st.errorf(iv.Pos, "DF5404", "write a positive whole number and a full unit name, such as INTERVAL 5 MINUTE",
		"invalid INTERVAL: %s", reason)
	return catalog.Interval{}, false
}

// logRetention checks LOG RETENTION (sql.md §8.6) and compiles it; without the clause,
// the stream inherits def.
func logRetention(st *stmt, r *sql.LogRetention, def catalog.Retention) catalog.Retention {
	if r == nil {
		return def
	}
	if r.Forever {
		st.warnf(r.Pos, "DF2501",
			"keep a short log (the default is 2 DAY) and use the archive as the permanent record; dynaflux explain shows the cost",
			"LOG RETENTION FOREVER keeps every event in DynamoDB; the table and the cost of projecting it keep growing")
		return catalog.RetainForever()
	}
	unit, ok := catalog.ParseUnit(r.Unit.Name)
	ret := catalog.RetainFor(catalog.Interval{N: int(min(r.Value.Int, math.MaxInt32)), Unit: unit})
	if _, err := ret.MarshalText(); ok && err == nil && !r.Neg && !r.Value.IsFloat {
		return ret
	}
	value := r.Value.Text + " " + r.Unit.Name
	if r.Neg {
		value = "-" + value
	}
	st.errorf(r.Pos, "DF2109", "use a whole number of HOUR, DAY, WEEK, MONTH or YEAR, from 1 DAY (24 HOUR) to 36525 DAY, or FOREVER",
		"LOG RETENTION %s is not allowed", value)
	return def
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"cmp"
	"math/big"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// evalLogic evaluates AND and OR left to right, stopping once the result is known, with
// three-valued logic (sql.md §5.5).
func evalLogic(e catalog.Expr) (cval, error) {
	decisive := e.Op == catalog.OpOr // the value that decides the result
	null := false
	for _, a := range e.Args {
		v, err := eval(a)
		if err != nil {
			return cval{}, err
		}
		switch {
		case v.null:
			null = true
		case v.b == decisive:
			return cval{b: decisive}, nil
		}
	}
	return cval{null: null, b: !decisive}, nil
}

// evalChoice evaluates if and multiIf, which evaluate only the value they return, and
// converts it to the result type.
func evalChoice(e catalog.Expr) (cval, error) {
	i, err := chosen(e)
	if err != nil {
		return cval{}, err
	}
	v, err := eval(e.Args[i])
	if err != nil || v.null {
		return v, err
	}
	return convert(v, e.Args[i].Type, e.Type), nil
}

// coalesce returns the first of the evaluated args that isn't NULL, converted to the
// result type.
func coalesce(e catalog.Expr, args []cval) cval {
	for i, v := range args {
		if !v.null {
			return convert(v, e.Args[i].Type, e.Type)
		}
	}
	return cval{null: true}
}

// chosen returns the index of the argument that a conditional returns.
func chosen(e catalog.Expr) (int, error) {
	last := len(e.Args) - 1
	for i := 0; i < last; i += 2 {
		cond, err := eval(e.Args[i])
		if err != nil {
			return 0, err
		}
		if !cond.null && cond.b {
			return i + 1, nil
		}
	}
	return last, nil
}

func evalIn(e catalog.Expr, args []cval) cval {
	for i := 1; i < len(args); i++ {
		if compareValues(args[0], e.Args[0].Type, args[i], e.Args[i].Type) == 0 {
			return cval{b: true}
		}
	}
	return cval{b: false}
}

// compareValues compares two values of comparable types (sql.md §5.7): numbers by exact
// value, times as instants, strings byte by byte, and false before true.
func compareValues(a cval, at catalog.Type, b cval, bt catalog.Type) int {
	switch {
	case isNumeric(at):
		return num(a, at).Cmp(num(b, bt))
	case isTemporal(at):
		return a.tm.Compare(b.tm)
	case isBool(at):
		return cmp.Compare(boolRank(a.b), boolRank(b.b))
	}
	return strings.Compare(a.str, b.str)
}

func boolRank(b bool) int {
	if b {
		return 1
	}
	return 0
}

func compareResult(symbol string, c int) bool {
	switch symbol {
	case "=":
		return c == 0
	case "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	}
	return c >= 0
}

type scalarEvaluator struct {
	op   catalog.Op
	eval func(args []cval) (cval, error)
}

// scalarEvaluators evaluate the scalar functions other than the conditionals.
func scalarEvaluators() []scalarEvaluator {
	const (
		fiveMinuteStep = 5 * time.Minute
		monthsShift    = 100
	)
	day := func(tm time.Time) time.Time { return time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, time.UTC) }
	truncated := func(step time.Duration) func([]cval) (cval, error) {
		return func(args []cval) (cval, error) { return cval{tm: args[0].tm.Truncate(step)}, nil }
	}
	return []scalarEvaluator{
		{catalog.OpLower, func(args []cval) (cval, error) { return cval{str: expr.LowerASCII(args[0].str)}, nil }},
		{catalog.OpUpper, func(args []cval) (cval, error) { return cval{str: expr.UpperASCII(args[0].str)}, nil }},
		{catalog.OpConcat, func(args []cval) (cval, error) {
			var b strings.Builder
			for _, v := range args {
				b.WriteString(v.str)
			}
			return cval{str: b.String()}, nil
		}},
		{catalog.OpLength, func(args []cval) (cval, error) {
			return cval{num: new(big.Rat).SetInt64(int64(len(args[0].str)))}, nil
		}},
		{catalog.OpToYYYYMM, func(args []cval) (cval, error) {
			tm := args[0].tm
			return cval{num: new(big.Rat).SetInt64(int64(tm.Year()*monthsShift + int(tm.Month())))}, nil
		}},
		{catalog.OpToDate, func(args []cval) (cval, error) { return cval{tm: day(args[0].tm)}, nil }},
		{catalog.OpToStartOfDay, func(args []cval) (cval, error) { return cval{tm: day(args[0].tm)}, nil }},
		{catalog.OpToStartOfMinute, truncated(time.Minute)},
		{catalog.OpToStartOfFiveMinutes, truncated(fiveMinuteStep)},
		{catalog.OpToStartOfHour, truncated(time.Hour)},
		{catalog.OpToStartOfMonth, func(args []cval) (cval, error) {
			tm := args[0].tm
			return cval{tm: time.Date(tm.Year(), tm.Month(), 1, 0, 0, 0, 0, time.UTC)}, nil
		}},
		{catalog.OpToStartOfWeek, func(args []cval) (cval, error) {
			return cval{tm: startOfWeek(day(args[0].tm), args[1].num.Num().Bit(0) == 1)}, nil
		}},
	}
}

// startOfWeek returns the Sunday, or the Monday, on or before day. A week that starts
// before 1970 starts on 1970-01-01, as in ClickHouse.
func startOfWeek(day time.Time, monday bool) time.Time {
	const daysPerWeek, mondayShift = 7, 6
	back := int(day.Weekday())
	if monday {
		back = (back + mondayShift) % daysPerWeek
	}
	start := day.AddDate(0, 0, -back)
	if epoch := time.Unix(0, 0).UTC(); start.Before(epoch) {
		return epoch
	}
	return start
}

// convert converts the value a conditional returns to the conditional's type (sql.md
// §4.5). Only a conversion to Float64 changes how the value is held.
func convert(v cval, from, to catalog.Type) cval {
	if isFloat(scalarOf(to)) {
		return cval{f: toFloat(v, from)}
	}
	return v
}

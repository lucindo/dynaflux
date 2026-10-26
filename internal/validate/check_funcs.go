// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"
	"strconv"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

const (
	maxWeekMode     = 9
	unlimited       = -1
	argsOfIf        = 3
	argsOfMultiIf   = 3 // at least, and an odd count
	argsOfConcat    = 2 // at least
	argsOfCoalesce  = 2 // at least
	argsOfBinaryFun = 2
)

func (c *checker) call(x *sql.Call) operand {
	fn := resolveCall(c.st, x)
	switch {
	case fn.kind == fnUnknown:
		c.st.errorf(x.Pos, fn.code, fn.hint, "%s", fn.msg)
	case x.Distinct:
		c.st.errorf(x.DistinctPos, "DF5207", "use uniqExact(x), or uniq(x) for an estimate",
			"DISTINCT inside %s is not supported", x.Name.Name)
	case fn.kind == fnNow:
		c.st.errorf(x.Pos, "DF5405", "pass the time as a parameter", "now() is only allowed in ACCEPT")
	case fn.kind == fnAggregate && c.env.aggregate == nil:
		c.st.errorf(x.Pos, "DF5204", "", "aggregate %s is not allowed in %s", x.Name.Name, c.env.place)
	case fn.kind == fnAggregate:
		return c.env.aggregate(c, x, fn.agg)
	case x.Star:
		c.starArg(x)
	case x.Parametric:
		c.paramsError(x, "it takes no parameter list")
	default:
		return fn.check(c, x)
	}
	return badOperand()
}

// resolveCall resolves the function that a call names; hints quote its first argument.
func resolveCall(st *stmt, x *sql.Call) funcRef {
	arg := "x"
	if len(x.Args) > 0 {
		arg = st.text(x.Args[0])
	}
	return resolveFunc(x, arg)
}

func unparen(x sql.Expr) sql.Expr {
	for {
		p, ok := x.(*sql.ParenExpr)
		if !ok {
			return x
		}
		x = p.X
	}
}

// arity checks the argument count of a call; most is unlimited or the most it takes. A
// date function's extra string argument is a time zone (DF5210).
func (c *checker) arity(x *sql.Call, fewest, most int, zone bool) bool {
	n := len(x.Args)
	if zone && n == most+1 {
		if _, ok := unparen(x.Args[most]).(*sql.StringLit); ok {
			c.st.errorf(x.Args[most].Extent().Pos, "DF5210", "all times are UTC; remove the argument",
				"time zone arguments are not supported")
			return false
		}
	}
	if n >= fewest && (most == unlimited || n <= most) {
		return true
	}
	var want string
	switch {
	case most == unlimited:
		want = "at least " + strconv.Itoa(fewest) + " arguments"
	case fewest == 1 && most == 1:
		want = "1 argument"
	case fewest == most:
		want = strconv.Itoa(fewest) + " arguments"
	default:
		want = strconv.Itoa(fewest) + " or " + strconv.Itoa(most) + " arguments"
	}
	c.st.errorf(x.Pos, "DF5202", "", "%s takes %s, not %d", x.Name.Name, want, n)
	return false
}

func (c *checker) argType(x *sql.Call, i int, t catalog.Type) operand {
	c.st.errorf(x.Pos, "DF5203", "", "%s doesn't accept %s as argument %d", x.Name.Name, t, i+1)
	return badOperand()
}

func (c *checker) starArg(x *sql.Call) {
	c.st.errorf(x.Pos, "DF5203", "", "%s doesn't accept * as argument 1", x.Name.Name)
}

// unaryFunc returns the checker of a one-argument function. rule returns the result type
// for an argument type, and whether the function accepts that type. zone is set for the
// date functions, which ClickHouse lets take a time zone.
func unaryFunc(op catalog.Op, zone bool, rule func(catalog.Type) (catalog.Type, bool)) func(*checker, *sql.Call) operand {
	return func(c *checker, x *sql.Call) operand {
		if !c.arity(x, 1, 1, zone) {
			return badOperand()
		}
		a := c.value(x.Args[0])
		if a.bad {
			return a
		}
		t, ok := rule(scalarOf(a.typ()))
		if !ok {
			return c.argType(x, 0, a.typ())
		}
		t.Nullable = a.typ().Nullable
		return operand{ir: catalog.Expr{Op: op, Type: t, Args: []catalog.Expr{a.ir}}, node: x}
	}
}

func stringArg(t catalog.Type) (catalog.Type, bool) {
	return t, t == catalog.Type{Kind: catalog.KindString}
}

func lengthArg(t catalog.Type) (catalog.Type, bool) {
	return catalog.Type{Kind: catalog.KindUInt64}, t.Array || t.Kind == catalog.KindString
}

// timeArg is the rule of toStartOfMinute, toStartOfFiveMinutes and toStartOfHour, which
// reject a Date, as in ClickHouse.
func timeArg(t catalog.Type) (catalog.Type, bool) {
	return catalog.Type{Kind: catalog.KindDateTime}, isTemporal(t) && t.Kind != catalog.KindDate
}

// temporalArg is the rule of a function of any temporal value that returns result.
func temporalArg(result catalog.Kind) func(catalog.Type) (catalog.Type, bool) {
	return func(t catalog.Type) (catalog.Type, bool) {
		return catalog.Type{Kind: result}, isTemporal(t)
	}
}

func (c *checker) ifCall(x *sql.Call) operand {
	if !c.arity(x, argsOfIf, argsOfIf, false) {
		return badOperand()
	}
	cond := c.condition(x.Args[0])
	var whenTrue, whenFalse []catalog.Expr
	if !cond.bad {
		whenTrue, whenFalse = nonZeroIfTrue(cond.ir), nonZeroIfFalse(cond.ir)
	}
	then := c.guarded(whenTrue, func() operand { return c.valueOrNull(x.Args[1]) })
	els := c.guarded(whenFalse, func() operand { return c.valueOrNull(x.Args[2]) })
	if cond.bad || then.bad || els.bad {
		return badOperand()
	}
	t, values, ok := c.choose(x, []operand{then, els})
	if !ok {
		return badOperand()
	}
	return operand{ir: catalog.Expr{Op: catalog.OpIf, Type: t, Args: append([]catalog.Expr{cond.ir}, values...)}, node: x}
}

func (c *checker) multiIf(x *sql.Call) operand {
	n := len(x.Args)
	if n < argsOfMultiIf || n%2 == 0 {
		c.st.errorf(x.Pos, "DF5202", "", "%s takes an odd number of arguments, at least 3, not %d", x.Name.Name, n)
		return badOperand()
	}
	var conds, values []operand
	var whenFalse []catalog.Expr // divisors that the conditions so far rule out when false
	ok := true
	for i := 0; i+1 < n; i += 2 {
		cond := c.guarded(whenFalse, func() operand { return c.condition(x.Args[i]) })
		guards := whenFalse
		if !cond.bad {
			guards = append(slices.Clip(whenFalse), nonZeroIfTrue(cond.ir)...)
			whenFalse = append(slices.Clip(whenFalse), nonZeroIfFalse(cond.ir)...)
		}
		v := c.guarded(guards, func() operand { return c.valueOrNull(x.Args[i+1]) })
		conds, values = append(conds, cond), append(values, v)
		ok = ok && !cond.bad && !v.bad
	}
	els := c.guarded(whenFalse, func() operand { return c.valueOrNull(x.Args[n-1]) })
	if !ok || els.bad {
		return badOperand()
	}
	t, irs, ok := c.choose(x, append(values, els))
	if !ok {
		return badOperand()
	}
	args := make([]catalog.Expr, 0, n)
	for i, cond := range conds {
		args = append(args, cond.ir, irs[i])
	}
	return operand{ir: catalog.Expr{Op: catalog.OpMultiIf, Type: t, Args: append(args, irs[n/2])}, node: x}
}

func (c *checker) coalesce(x *sql.Call) operand {
	if !c.arity(x, argsOfCoalesce, unlimited, false) {
		return badOperand()
	}
	values := make([]operand, len(x.Args))
	for i, a := range x.Args {
		values[i] = c.valueOrNull(a)
		if values[i].bad {
			return badOperand()
		}
	}
	for i, v := range values[:len(values)-1] {
		if !v.null && !v.typ().Nullable {
			c.st.errorf(x.Args[i].Extent().Pos, "DF5212", "",
				"coalesce argument %d is not Nullable, so the arguments after it are never used", i+1)
			return badOperand()
		}
	}
	t, irs, ok := c.choose(x, values)
	if !ok {
		return badOperand()
	}
	last := values[len(values)-1]
	t.Nullable = last.null || last.typ().Nullable
	return operand{ir: catalog.Expr{Op: catalog.OpCoalesce, Type: t, Args: irs}, node: x}
}

// choose types the values of if, multiIf or coalesce (sql.md §4.5, §5.5): their common
// type, Nullable when one is, with each NULL literal typed as the result.
func (c *checker) choose(x *sql.Call, values []operand) (catalog.Type, []catalog.Expr, bool) {
	var typed []catalog.Type
	var firstNull sql.Node
	for _, v := range values {
		switch {
		case !v.null:
			typed = append(typed, v.typ())
		case firstNull == nil:
			firstNull = v.node
		}
	}
	if len(typed) == 0 {
		c.untypedNull(firstNull)
		return catalog.Type{}, nil, false
	}
	t, err := commonType(typed, firstNull != nil)
	if err != nil {
		c.typeError(x, err)
		return catalog.Type{}, nil, false
	}
	irs := make([]catalog.Expr, len(values))
	for i, v := range values {
		irs[i] = v.ir
		if v.null {
			irs[i] = catalog.Expr{Op: catalog.OpNull, Type: t}
		}
	}
	return t, irs, true
}

func (c *checker) intDiv(x *sql.Call) operand {
	if !c.arity(x, argsOfBinaryFun, argsOfBinaryFun, false) {
		return badOperand()
	}
	a, b := c.value(x.Args[0]), c.value(x.Args[1])
	if a.bad || b.bad {
		return badOperand()
	}
	t, err := arithType(opIntDiv, a.typ(), b.typ())
	if err != nil {
		return c.typeError(x, err)
	}
	if !c.divisor(x, x.Args[0], x.Args[1], b) {
		return badOperand()
	}
	return operand{ir: catalog.Expr{Op: catalog.OpIntDiv, Type: t, Args: []catalog.Expr{a.ir, b.ir}}, node: x}
}

func (c *checker) concat(x *sql.Call) operand {
	if !c.arity(x, argsOfConcat, unlimited, false) {
		return badOperand()
	}
	t := catalog.Type{Kind: catalog.KindString}
	args := make([]catalog.Expr, len(x.Args))
	for i, a := range x.Args {
		v := c.value(a)
		if v.bad {
			return v
		}
		if scalarOf(v.typ()) != (catalog.Type{Kind: catalog.KindString}) {
			return c.argType(x, i, v.typ())
		}
		t.Nullable = t.Nullable || v.typ().Nullable
		args[i] = v.ir
	}
	return operand{ir: catalog.Expr{Op: catalog.OpConcat, Type: t, Args: args}, node: x}
}

// toDate takes a temporal value or a 'YYYY-MM-DD' literal, which becomes a Date literal.
func (c *checker) toDate(x *sql.Call) operand {
	if !c.arity(x, 1, 1, true) {
		return badOperand()
	}
	date := catalog.Type{Kind: catalog.KindDate}
	if s, ok := unparen(x.Args[0]).(*sql.StringLit); ok {
		tm, valid := parseTime(s.Value, date)
		if !valid {
			c.st.errorf(s.Pos, "DF5401", hintTimeForms, "'%s' is not a valid Date", s.Value)
			return badOperand()
		}
		arg := literal(date, formatTime(tm, date))
		return operand{ir: catalog.Expr{Op: catalog.OpToDate, Type: date, Args: []catalog.Expr{arg}}, node: x}
	}
	a := c.value(x.Args[0])
	if a.bad {
		return a
	}
	if p, isParam := unparen(x.Args[0]).(*sql.Param); isParam && a.typ().Kind == catalog.KindString {
		c.st.errorf(p.Pos, "DF4404", "", "the string argument of toDate needs a literal, not a parameter")
		return badOperand()
	}
	if !isTemporal(a.typ()) {
		return c.argType(x, 0, a.typ())
	}
	date.Nullable = a.typ().Nullable
	return operand{ir: catalog.Expr{Op: catalog.OpToDate, Type: date, Args: []catalog.Expr{a.ir}}, node: x}
}

func (c *checker) toStartOfWeek(x *sql.Call) operand {
	if !c.arity(x, 1, argsOfBinaryFun, true) {
		return badOperand()
	}
	t := c.value(x.Args[0])
	if t.bad {
		return t
	}
	if !isTemporal(t.typ()) {
		return c.argType(x, 0, t.typ())
	}
	mode := literal(catalog.Type{Kind: catalog.KindUInt8}, "0")
	if len(x.Args) > 1 {
		var ok bool
		if mode, ok = c.weekMode(x.Args[1]); !ok {
			return badOperand()
		}
	}
	date := catalog.Type{Kind: catalog.KindDate, Nullable: t.typ().Nullable}
	return operand{ir: catalog.Expr{Op: catalog.OpToStartOfWeek, Type: date, Args: []catalog.Expr{t.ir, mode}}, node: x}
}

func (c *checker) weekMode(x sql.Expr) (catalog.Expr, bool) {
	if c.rejectParam(x, "the toStartOfWeek mode") {
		return catalog.Expr{}, false
	}
	if m, ok := unparen(x).(*sql.NumberLit); ok && !m.IsFloat && (!m.Neg || m.Int == 0) && m.Int <= maxWeekMode {
		return literal(catalog.Type{Kind: catalog.KindUInt8}, strconv.FormatUint(m.Int, 10)), true
	}
	c.st.errorf(x.Extent().Pos, "DF5211", "0 starts weeks on Sunday, 1 on Monday",
		"toStartOfWeek mode must be a whole number from 0 to 9")
	return catalog.Expr{}, false
}

// rejectParam reports x when it is a parameter, where what needs a literal (DF4404), and
// whether it is one. A parameter with its own error isn't reported again.
func (c *checker) rejectParam(x sql.Expr, what string) bool {
	p, ok := unparen(x).(*sql.Param)
	if ok && !c.param(p, paramValue).bad {
		c.st.errorf(p.Pos, "DF4404", "", "%s needs a literal, not a parameter", what)
	}
	return ok
}

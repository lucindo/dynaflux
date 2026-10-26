// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

const (
	// placeAggregate is where DF5204 says a nested aggregate is.
	placeAggregate = "another aggregate"
	// defaultLevel is the level of quantile and quantileDD when SQL leaves it out.
	defaultLevel = 0.5
)

// aggregateCall checks an aggregate call (sql.md §7.1 to §7.3): its parameters, its
// arguments and its -If condition, and types it. Combinators other than If are the
// caller's to check.
func (c *checker) aggregateCall(x *sql.Call, fn aggregateFunc) (catalog.Aggregate, bool) {
	inner := &checker{st: c.st, env: c.env, guards: c.guards}
	inner.env.place, inner.env.aggregate = placeAggregate, nil
	params, paramsOK := inner.aggregateParams(x, fn.fn)
	hasIf := slices.Contains(fn.combinators, combIf)
	if !inner.aggregateArity(x, fn.fn, hasIf) {
		return catalog.Aggregate{}, false
	}
	args, cond, argsOK := inner.aggregateArgs(x, hasIf)
	if !paramsOK || !argsOK {
		return catalog.Aggregate{}, false
	}
	types := make([]catalog.Type, len(args))
	irs := make([]catalog.Expr, len(args))
	for i, a := range args {
		if !c.aggregateArgType(x, fn.fn, i, a.typ()) {
			return catalog.Aggregate{}, false
		}
		types[i], irs[i] = a.typ(), a.ir
	}
	return catalog.Aggregate{Fn: fn.fn, Params: params, Args: irs, Cond: cond, Type: aggregateResult(fn.fn, types)}, true
}

// aggregateArity checks the argument count of an aggregate; hasIf adds the condition.
func (c *checker) aggregateArity(x *sql.Call, fn catalog.AggFunc, hasIf bool) bool {
	n := 1
	switch fn {
	case catalog.AggCount:
		n = 0
	case catalog.AggArgMax, catalog.AggArgMin:
		n = argsOfBinaryFun
	case catalog.AggSum, catalog.AggMin, catalog.AggMax, catalog.AggAvg, catalog.AggAny, catalog.AggUniq,
		catalog.AggUniqExact, catalog.AggQuantile, catalog.AggQuantileDD:
	}
	if hasIf {
		n++
	}
	isUniq := fn == catalog.AggUniq || fn == catalog.AggUniqExact
	switch {
	case fn == catalog.AggCount && !hasIf && (x.Star || len(x.Args) > 0):
		c.countArgument(x)
	case x.Star:
		c.st.errorf(x.Pos, "DF5203", "", "%s doesn't accept * as argument 1", x.Name.Name)
	case isUniq && len(x.Args) > n:
		c.st.errorf(x.Pos, "DF5209", "", "%s takes one argument in v1", x.Name.Name)
	default:
		return c.arity(x, n, n, false)
	}
	return false
}

// countArgument reports count(*) and count(x) (DF5208).
func (c *checker) countArgument(x *sql.Call) {
	if x.Star {
		c.st.errorf(x.Pos, "DF5208", "use count()", "%s(*) is not supported", x.Name.Name)
		return
	}
	texts := make([]string, len(x.Args))
	for i, a := range x.Args {
		texts[i] = c.st.text(a)
	}
	arg := strings.Join(texts, ", ")
	c.st.errorf(x.Pos, "DF5208", "use count(); for a Nullable "+arg+", countIf("+arg+" IS NOT NULL) counts its values",
		"%s(%s) is not supported", x.Name.Name, arg)
}

// aggregateArgs checks the arguments of an aggregate after its -If condition, which runs
// first and guards their divisors (sql.md §5.6, §5.8).
func (c *checker) aggregateArgs(x *sql.Call, hasIf bool) ([]operand, *catalog.Expr, bool) {
	values := x.Args
	var cond *catalog.Expr
	var guards []catalog.Expr
	ok := true
	if hasIf {
		values = x.Args[:len(x.Args)-1]
		o := c.condition(x.Args[len(x.Args)-1])
		if o.bad {
			ok = false
		} else {
			cond, guards = &o.ir, nonZeroIfTrue(o.ir)
		}
	}
	args := make([]operand, len(values))
	for i, v := range values {
		args[i] = c.guarded(guards, func() operand { return c.value(v) })
		ok = ok && !args[i].bad
	}
	return args, cond, ok
}

// aggregateParams checks the parameter list of an aggregate: quantile takes an optional
// level, quantileDD a rel_acc and an optional level, and the others none (DF5205).
func (c *checker) aggregateParams(x *sql.Call, fn catalog.AggFunc) ([]float64, bool) {
	switch {
	case fn == catalog.AggQuantile && len(x.Params) > 1:
		return nil, c.paramsError(x, "it takes one parameter, level")
	case fn == catalog.AggQuantile:
		level, ok := c.level(x, x.Params)
		return []float64{level}, ok
	case fn == catalog.AggQuantileDD && (len(x.Params) == 0 || len(x.Params) > argsOfBinaryFun):
		return nil, c.paramsError(x, "it takes rel_acc and an optional level")
	case fn == catalog.AggQuantileDD:
		relAcc, relOK := c.paramValue(x, x.Params[0], "rel_acc")
		if relOK && (relAcc <= 0 || relAcc >= 1) {
			relOK = c.paramsError(x, "rel_acc "+c.st.text(x.Params[0])+" is not between 0 and 1, exclusive")
		}
		level, levelOK := c.level(x, x.Params[1:])
		return []float64{relAcc, level}, relOK && levelOK
	case x.Parametric:
		return nil, c.paramsError(x, "it takes no parameter list")
	}
	return nil, true
}

// level returns the level among params, 0.5 when there is none.
func (c *checker) level(x *sql.Call, params []sql.Expr) (float64, bool) {
	if len(params) == 0 {
		return defaultLevel, true
	}
	level, ok := c.paramValue(x, params[0], "level")
	if ok && (level < 0 || level > 1) {
		return 0, c.paramsError(x, "level "+c.st.text(params[0])+" is not between 0 and 1")
	}
	return level, ok
}

// paramValue returns the value of an aggregate parameter, which must be a number literal.
func (c *checker) paramValue(x *sql.Call, p sql.Expr, what string) (float64, bool) {
	if param, ok := unparen(p).(*sql.Param); ok {
		if !c.param(param, paramValue).bad {
			c.st.errorf(param.Pos, "DF4404", "", "the %s of %s needs a literal, not a parameter", what, x.Name.Name)
		}
		return 0, false
	}
	n, ok := unparen(p).(*sql.NumberLit)
	if !ok {
		return 0, c.paramsError(x, what+" must be a number literal")
	}
	v := n.Float
	if !n.IsFloat {
		v = float64(n.Int)
	}
	if n.Neg {
		v = -v
	}
	return v, true
}

// paramsError reports invalid aggregate parameters (DF5205) and returns false.
func (c *checker) paramsError(x *sql.Call, reason string) bool {
	c.st.errorf(x.Pos, "DF5205", "", "invalid parameters for %s: %s", x.Name.Name, reason)
	return false
}

// aggregateArgType checks the type of argument i of an aggregate (sql.md §7.1).
func (c *checker) aggregateArgType(x *sql.Call, fn catalog.AggFunc, i int, t catalog.Type) bool {
	t = scalarOf(t)
	orderable := fn == catalog.AggMin || fn == catalog.AggMax ||
		i == 1 && (fn == catalog.AggArgMax || fn == catalog.AggArgMin)
	numeric := fn == catalog.AggSum || fn == catalog.AggAvg
	quantile := fn == catalog.AggQuantile || fn == catalog.AggQuantileDD
	switch {
	case orderable && t.Kind == catalog.KindUUID && !t.Array:
		c.st.errorf(x.Pos, "DF5311", "compare UUIDs with = or IN", "UUID values can't be ordered")
		return false
	case t.Array, orderable && !isOrderable(t), numeric && !isNumeric(t), quantile && !isInteger(t) && !isFloat(t):
		c.argType(x, i, t)
		return false
	}
	return true
}

// aggregateResult returns the finalized type of an aggregate (sql.md §7.1). The -If
// condition doesn't count.
func aggregateResult(fn catalog.AggFunc, args []catalog.Type) catalog.Type {
	switch fn {
	case catalog.AggCount, catalog.AggUniq, catalog.AggUniqExact:
		return catalog.Type{Kind: catalog.KindUInt64}
	case catalog.AggAvg, catalog.AggQuantile, catalog.AggQuantileDD:
		return catalog.Type{Kind: catalog.KindFloat64, Nullable: true}
	case catalog.AggSum:
		return sumType(args[0])
	case catalog.AggArgMax, catalog.AggArgMin:
		t := args[0]
		t.Nullable = t.Nullable || args[1].Nullable
		return t
	case catalog.AggMin, catalog.AggMax, catalog.AggAny:
	}
	return args[0]
}

// sumType is the type of sum(x): exact totals in the widest type of x's group.
func sumType(t catalog.Type) catalog.Type {
	switch {
	case isDecimal(t):
		return catalog.Type{Kind: catalog.KindDecimal, Precision: catalog.MaxDecimalPrecision, Scale: t.Scale, Nullable: t.Nullable}
	case isFloat(t):
		return t
	case isSigned(t.Kind):
		return catalog.Type{Kind: catalog.KindInt64, Nullable: t.Nullable}
	}
	return catalog.Type{Kind: catalog.KindUInt64, Nullable: t.Nullable}
}

// typeDefault returns the text of a value of t that hints can offer as a sentinel for
// NULL: the type's default (sql.md §7.1).
func typeDefault(t catalog.Type) string {
	switch {
	case t.Kind == catalog.KindString:
		return "''"
	case isNumeric(scalarOf(t)):
		return "0"
	case isBool(t):
		return "false"
	case isTemporal(scalarOf(t)):
		return "toDate('1970-01-01')"
	}
	return "'00000000-0000-0000-0000-000000000000'"
}

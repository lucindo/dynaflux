// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

type funcKind uint8

const (
	fnUnknown funcKind = iota // not supported: code, hint and msg say why
	fnScalar
	fnNow
	fnAggregate
)

// funcRef is what a function name resolves to.
type funcRef struct {
	kind funcKind
	// check checks a call of a scalar function.
	check func(c *checker, x *sql.Call) operand
	agg   aggregateFunc
	// code, hint and msg report a function that isn't supported.
	code, hint, msg string
}

// aggregateFunc is an aggregate function with its combinators, such as sumIf.
type aggregateFunc struct {
	fn catalog.AggFunc
	// combinators are the suffixes in the order written, such as If and Merge for
	// sumIfMerge. Only If, Merge and State are known to the rules; the others are DF5206.
	combinators []string
}

// Combinators that the rules handle.
const (
	combIf    = "If"
	combMerge = "Merge"
	combState = "State"
)

// resolveFunc resolves the name of a call with ClickHouse's letter-case rules (sql.md §6).
// arg is the text of its first argument, which hints quote.
func resolveFunc(x *sql.Call, arg string) funcRef {
	name := x.Name.Name
	if check, ok := scalarFunc(name); ok {
		return funcRef{kind: fnScalar, check: check}
	}
	if strings.EqualFold(name, "now") {
		return funcRef{kind: fnNow}
	}
	if ref, ok := unsupportedAggregate(x, arg); ok {
		return ref
	}
	if agg, ok := parseAggregate(name); ok {
		for _, comb := range agg.combinators {
			if comb != combIf && comb != combMerge && comb != combState {
				return funcRef{code: "DF5206", hint: hintCombinator, msg: "combinator -" + comb + " is not supported"}
			}
		}
		return funcRef{kind: fnAggregate, agg: agg}
	}
	hint, ok := replacement(name)
	if !ok {
		hint = didYouMean(name, knownFunctions())
	}
	return funcRef{code: "DF5201", hint: hint, msg: "unknown function " + name}
}

// scalarFunc returns the checker of a scalar function (sql.md §6).
func scalarFunc(name string) (func(*checker, *sql.Call) operand, bool) {
	switch strings.ToLower(name) {
	case "if":
		return (*checker).ifCall, true
	case "coalesce":
		return (*checker).coalesce, true
	case "lower":
		return unaryFunc(catalog.OpLower, false, stringArg), true
	case "upper":
		return unaryFunc(catalog.OpUpper, false, stringArg), true
	case "concat":
		return (*checker).concat, true
	case "length":
		return unaryFunc(catalog.OpLength, false, lengthArg), true
	}
	switch name {
	case "multiIf":
		return (*checker).multiIf, true
	case "intDiv":
		return (*checker).intDiv, true
	case "toDate":
		return (*checker).toDate, true
	case "toStartOfMinute":
		return unaryFunc(catalog.OpToStartOfMinute, true, timeArg), true
	case "toStartOfFiveMinutes", "toStartOfFiveMinute":
		return unaryFunc(catalog.OpToStartOfFiveMinutes, true, timeArg), true
	case "toStartOfHour":
		return unaryFunc(catalog.OpToStartOfHour, true, timeArg), true
	case "toStartOfDay":
		return unaryFunc(catalog.OpToStartOfDay, true, temporalArg(catalog.KindDateTime)), true
	case "toStartOfWeek":
		return (*checker).toStartOfWeek, true
	case "toStartOfMonth":
		return unaryFunc(catalog.OpToStartOfMonth, true, temporalArg(catalog.KindDate)), true
	case "toYYYYMM":
		return unaryFunc(catalog.OpToYYYYMM, true, temporalArg(catalog.KindUInt32)), true
	}
	return nil, false
}

type aggName struct {
	name string
	fn   catalog.AggFunc
}

// aggregateNames lists the aggregate names, longest first where one is a prefix of
// another, so that uniqExact isn't read as uniq with a suffix.
func aggregateNames() []aggName {
	return []aggName{
		{"uniqExact", catalog.AggUniqExact}, {"uniqHLL12", catalog.AggUniq}, {"uniq", catalog.AggUniq},
		{"quantileDD", catalog.AggQuantileDD}, {"quantile", catalog.AggQuantile},
		{"argMax", catalog.AggArgMax}, {"argMin", catalog.AggArgMin},
		{"count", catalog.AggCount}, {"sum", catalog.AggSum}, {"min", catalog.AggMin}, {"max", catalog.AggMax},
		{"avg", catalog.AggAvg}, {"any", catalog.AggAny},
	}
}

func parseAggregate(name string) (aggregateFunc, bool) {
	for _, b := range aggregateNames() {
		if strings.EqualFold(name, b.name) && isCaseFreeAggregate(b.fn) {
			return aggregateFunc{fn: b.fn}, true
		}
		rest, found := strings.CutPrefix(name, b.name)
		if !found {
			continue
		}
		if combs, ok := parseCombinators(rest); ok {
			return aggregateFunc{fn: b.fn, combinators: combs}, true
		}
	}
	return aggregateFunc{}, false
}

// isCaseFreeAggregate reports whether the plain name of f matches in any letter case.
func isCaseFreeAggregate(f catalog.AggFunc) bool {
	switch f {
	case catalog.AggCount, catalog.AggSum, catalog.AggMin, catalog.AggMax, catalog.AggAvg:
		return true
	case catalog.AggArgMax, catalog.AggArgMin, catalog.AggAny, catalog.AggUniq, catalog.AggUniqExact,
		catalog.AggQuantile, catalog.AggQuantileDD:
	}
	return false
}

// parseCombinators splits a suffix such as IfMerge into ClickHouse combinators.
func parseCombinators(s string) ([]string, bool) {
	known := []string{
		"SimpleState", "OrDefault", "OrNull", "Resample", "Distinct", "ForEach", "ArgMin", "ArgMax",
		combMerge, combState, "Array", combIf, "Map",
	}
	var out []string
	for s != "" {
		found := false
		for _, k := range known {
			if rest, ok := strings.CutPrefix(s, k); ok {
				out, s, found = append(out, k), rest, true
				break
			}
		}
		if !found {
			return nil, false
		}
	}
	return out, true
}

// unsupportedAggregate resolves the ClickHouse aggregates of sql.md §7.6, in any
// combinator form.
func unsupportedAggregate(x *sql.Call, arg string) (funcRef, bool) {
	name := x.Name.Name
	unbounded := funcRef{code: "DF5213", msg: name + "(" + arg + ") has unbounded state.",
		hint: "use uniq(" + arg + ") or uniqExact(" + arg + ") to count values, or argMax(" + arg + ", event_time) for the latest one"}
	orderDependent := funcRef{code: "DF5214", msg: name + " depends on the order events arrive in",
		hint: "use argMax(" + arg + ", event_time) for the latest value, or any(" + arg + ") for the earliest"}
	switch base := unsupportedBase(name, []string{
		"groupArraySample", "groupArray", "groupUniqArray", "groupBitmap", "anyLast", "anyHeavy", "first_value",
		"last_value", "topK", "countDistinct",
	}); base {
	case "groupArray", "groupUniqArray":
		if x.Parametric {
			return orderDependent, true
		}
		return unbounded, true
	case "groupBitmap":
		return unbounded, true
	case "groupArraySample", "anyLast", "anyHeavy", "first_value", "last_value", "topK":
		return orderDependent, true
	case "countDistinct":
		return funcRef{code: "DF5207", hint: "use uniqExact(x), or uniq(x) for an estimate",
			msg: "DISTINCT inside " + name + " is not supported"}, true
	}
	return funcRef{}, false
}

// unsupportedBase returns the base among bases that name is, with any combinators.
func unsupportedBase(name string, bases []string) string {
	for _, b := range bases {
		if rest, ok := strings.CutPrefix(name, b); ok {
			if _, isCombinators := parseCombinators(rest); isCombinators {
				return b
			}
		}
	}
	return ""
}

// replacement returns the hint for a ClickHouse function that Dynaflux replaces
// (sql.md Table A.2 and §7.6).
func replacement(name string) (string, bool) {
	prefixes := []struct{ prefix, hint string }{
		{"median", hintQuantile}, {"quantiles", hintQuantile}, {"quantileExact", hintQuantile},
		{"quantileTiming", hintQuantile}, {"quantileTDigest", hintQuantile}, {"quantileBFloat16", hintQuantile},
		{"quantileDeterministic", hintQuantile},
		{"uniqCombined", hintUniq}, {"uniqTheta", hintUniq}, {"uniqUpTo", hintUniq},
		{"sumWithOverflow", "use sum(x)"}, {"sumKahan", "use sum(x)"}, {"sumCount", "use sum(x) and count()"},
		{"avgWeighted", "use avg(x)"},
		{"toInt", hintNoConversions}, {"toUInt", hintNoConversions}, {"toFloat", hintNoConversions},
		{"toDecimal", hintNoConversions}, {"toDateTime", hintNoConversions}, {"parseDateTime", hintNoConversions},
	}
	for _, p := range prefixes {
		if strings.HasPrefix(name, p.prefix) {
			return p.hint, true
		}
	}
	switch name {
	case "toStartOfInterval", "toStartOfQuarter", "toStartOfYear", "toMonday":
		return "use toStartOfMinute … toStartOfMonth; toStartOfWeek(t, 1) for Monday", true
	case "lowerUTF8", "upperUTF8":
		return "use lower or upper (ASCII only)", true
	case "lengthUTF8":
		return "use length (bytes)", true
	case "isNull", "isNotNull":
		return "use x IS NULL or x IS NOT NULL", true
	case "ifNull", "nullIf":
		return "use coalesce or if", true
	case "toString":
		return hintNoConversions, true
	case "arrayJoin":
		return "use ARRAY JOIN <array> AS <element> in a view", true
	}
	if op, ok := operatorFunc(strings.ToLower(name)); ok {
		return "use the operator " + op, true
	}
	return "", false
}

const (
	hintCombinator    = "use -If, or -Merge in a query over a view"
	hintNoConversions = "no conversions in v1; compare with a literal or a parameter"
	hintQuantile      = "use quantile(level)(x)"
	hintUniq          = "use uniq(x)"
)

func operatorFunc(name string) (string, bool) {
	switch name {
	case "plus":
		return opPlus, true
	case "minus":
		return opMinus, true
	case "multiply":
		return opTimes, true
	case "divide":
		return opDivide, true
	case "modulo":
		return opModulo, true
	case "equals":
		return "=", true
	case "and":
		return "AND", true
	case "or":
		return "OR", true
	}
	return "", false
}

// knownFunctions lists the names that an unknown function's suggestion comes from.
func knownFunctions() []string {
	names := []string{
		"if", "multiIf", "coalesce", "lower", "upper", "concat", "length", "now", "intDiv", "toDate",
		"toStartOfMinute", "toStartOfFiveMinutes", "toStartOfHour", "toStartOfDay", "toStartOfWeek",
		"toStartOfMonth", "toYYYYMM",
	}
	for _, b := range aggregateNames() {
		names = append(names, b.name, b.name+combIf, b.name+combMerge, b.name+combIf+combMerge)
	}
	return names
}

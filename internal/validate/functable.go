// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"
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

// aggregateFunc is an aggregate function with the combinators the rules know: -If,
// -Merge and -State, each at most once. mergeIf marks -If written after -Merge, as in
// sumMergeIf, which filters merged states.
type aggregateFunc struct {
	fn                            catalog.AggFunc
	withIf, merge, state, mergeIf bool
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
	if check, ok := scalarCheck(name); ok {
		return funcRef{kind: fnScalar, check: check}
	}
	if strings.EqualFold(name, "now") {
		return funcRef{kind: fnNow}
	}
	if ref, ok := unsupportedAggregate(x, arg); ok {
		return ref
	}
	if fn, combs, ok := parseAggregate(name); ok {
		agg := aggregateFunc{fn: fn}
		for i, comb := range combs {
			if slices.Contains(combs[:i], comb) {
				return funcRef{code: "DF5206", hint: hintCombinator, msg: "combinator -" + comb + " is not supported twice"}
			}
			switch comb {
			case combIf:
				agg.withIf, agg.mergeIf = true, agg.merge
			case combMerge:
				agg.merge = true
			case combState:
				agg.state = true
			default:
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

// scalarFunc is a scalar function (sql.md §6).
type scalarFunc struct {
	name string
	// caseFree marks a name that matches in any letter case, as in ClickHouse.
	caseFree bool
	// alias marks a second spelling, which suggestions leave out.
	alias bool
	check func(*checker, *sql.Call) operand
}

func scalarFuncs() []scalarFunc {
	return []scalarFunc{
		{name: "if", caseFree: true, check: (*checker).ifCall},
		{name: "coalesce", caseFree: true, check: (*checker).coalesce},
		{name: "lower", caseFree: true, check: unaryFunc(catalog.OpLower, false, stringArg)},
		{name: "upper", caseFree: true, check: unaryFunc(catalog.OpUpper, false, stringArg)},
		{name: "concat", caseFree: true, check: (*checker).concat},
		{name: "length", caseFree: true, check: unaryFunc(catalog.OpLength, false, lengthArg)},
		{name: "multiIf", check: (*checker).multiIf},
		{name: "intDiv", check: (*checker).intDiv},
		{name: "toDate", check: (*checker).toDate},
		{name: "toStartOfMinute", check: unaryFunc(catalog.OpToStartOfMinute, true, timeArg)},
		{name: "toStartOfFiveMinutes", check: unaryFunc(catalog.OpToStartOfFiveMinutes, true, timeArg)},
		{name: "toStartOfFiveMinute", alias: true, check: unaryFunc(catalog.OpToStartOfFiveMinutes, true, timeArg)},
		{name: "toStartOfHour", check: unaryFunc(catalog.OpToStartOfHour, true, timeArg)},
		{name: "toStartOfDay", check: unaryFunc(catalog.OpToStartOfDay, true, temporalArg(catalog.KindDateTime))},
		{name: "toStartOfWeek", check: (*checker).toStartOfWeek},
		{name: "toStartOfMonth", check: unaryFunc(catalog.OpToStartOfMonth, true, temporalArg(catalog.KindDate))},
		{name: "toYYYYMM", check: unaryFunc(catalog.OpToYYYYMM, true, temporalArg(catalog.KindUInt32))},
	}
}

// scalarCheck returns the checker of the scalar function name names.
func scalarCheck(name string) (func(*checker, *sql.Call) operand, bool) {
	for _, f := range scalarFuncs() {
		if name == f.name || f.caseFree && strings.ToLower(name) == f.name {
			return f.check, true
		}
	}
	return nil, false
}

type aggName struct {
	name     string
	fn       catalog.AggFunc
	caseFree bool // the plain name matches in any letter case
}

// aggregateNames lists the aggregate names, longest first where one is a prefix of
// another, so that uniqExact isn't read as uniq with a suffix.
func aggregateNames() []aggName {
	return []aggName{
		{"uniqExact", catalog.AggUniqExact, false}, {"uniqHLL12", catalog.AggUniq, false}, {"uniq", catalog.AggUniq, false},
		{"quantileDD", catalog.AggQuantileDD, false}, {"quantile", catalog.AggQuantile, false},
		{"argMax", catalog.AggArgMax, false}, {"argMin", catalog.AggArgMin, false},
		{"count", catalog.AggCount, true}, {"sum", catalog.AggSum, true}, {"min", catalog.AggMin, true},
		{"max", catalog.AggMax, true}, {"avg", catalog.AggAvg, true}, {"any", catalog.AggAny, false},
	}
}

// parseAggregate splits name into an aggregate function and its combinators, in the
// order written.
func parseAggregate(name string) (catalog.AggFunc, []string, bool) {
	for _, b := range aggregateNames() {
		if b.caseFree && strings.EqualFold(name, b.name) {
			return b.fn, nil, true
		}
		rest, found := strings.CutPrefix(name, b.name)
		if !found {
			continue
		}
		if combs, ok := parseCombinators(rest); ok {
			return b.fn, combs, true
		}
	}
	return "", nil, false
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

const (
	hintCombinator    = "use -If, or -Merge in a query over a view"
	hintNoConversions = "no conversions in v1; compare with a literal or a parameter"
	hintQuantile      = "use quantile(level)(x)"
	hintUniq          = "use uniq(x)"
	hintStartOf       = "use toStartOfMinute … toStartOfMonth; toStartOfWeek(t, 1) for Monday"
)

// nameMatch is how a replaced function's name matches.
type nameMatch uint8

const (
	matchPrefix  nameMatch = iota + 1 // the start of the name, with any suffix
	matchExact                        // the whole name
	matchAnyCase                      // the whole name, in any letter case
)

// replacement returns the hint for a ClickHouse function that Dynaflux replaces
// (sql.md Table A.2 and §7.6).
func replacement(name string) (string, bool) {
	for _, r := range []struct {
		name  string
		match nameMatch
		hint  string
	}{
		{"median", matchPrefix, hintQuantile}, {"quantiles", matchPrefix, hintQuantile},
		{"quantileExact", matchPrefix, hintQuantile}, {"quantileTiming", matchPrefix, hintQuantile},
		{"quantileTDigest", matchPrefix, hintQuantile}, {"quantileBFloat16", matchPrefix, hintQuantile},
		{"quantileDeterministic", matchPrefix, hintQuantile},
		{"uniqCombined", matchPrefix, hintUniq}, {"uniqTheta", matchPrefix, hintUniq}, {"uniqUpTo", matchPrefix, hintUniq},
		{"sumWithOverflow", matchPrefix, "use sum(x)"}, {"sumKahan", matchPrefix, "use sum(x)"},
		{"sumCount", matchPrefix, "use sum(x) and count()"}, {"avgWeighted", matchPrefix, "use avg(x)"},
		{"toInt", matchPrefix, hintNoConversions}, {"toUInt", matchPrefix, hintNoConversions},
		{"toFloat", matchPrefix, hintNoConversions}, {"toDecimal", matchPrefix, hintNoConversions},
		{"toDateTime", matchPrefix, hintNoConversions}, {"parseDateTime", matchPrefix, hintNoConversions},
		{"toStartOfInterval", matchExact, hintStartOf}, {"toStartOfQuarter", matchExact, hintStartOf},
		{"toStartOfYear", matchExact, hintStartOf}, {"toMonday", matchExact, hintStartOf},
		{"lowerUTF8", matchExact, "use lower or upper (ASCII only)"}, {"upperUTF8", matchExact, "use lower or upper (ASCII only)"},
		{"lengthUTF8", matchExact, "use length (bytes)"},
		{"isNull", matchExact, "use x IS NULL or x IS NOT NULL"}, {"isNotNull", matchExact, "use x IS NULL or x IS NOT NULL"},
		{"ifNull", matchExact, "use coalesce or if"}, {"nullIf", matchExact, "use coalesce or if"},
		{"toString", matchExact, hintNoConversions},
		{"arrayJoin", matchExact, "use ARRAY JOIN <array> AS <element> in a view"},
		{"plus", matchAnyCase, "use the operator " + opPlus}, {"minus", matchAnyCase, "use the operator " + opMinus},
		{"multiply", matchAnyCase, "use the operator " + opTimes}, {"divide", matchAnyCase, "use the operator " + opDivide},
		{"modulo", matchAnyCase, "use the operator " + opModulo}, {"equals", matchAnyCase, "use the operator ="},
		{"and", matchAnyCase, "use the operator AND"}, {"or", matchAnyCase, "use the operator OR"},
	} {
		if r.match == matchPrefix && strings.HasPrefix(name, r.name) || r.match == matchExact && name == r.name ||
			r.match == matchAnyCase && strings.ToLower(name) == r.name {
			return r.hint, true
		}
	}
	return "", false
}

// knownFunctions lists the names that an unknown function's suggestion comes from.
func knownFunctions() []string {
	names := []string{"now"}
	for _, f := range scalarFuncs() {
		if !f.alias {
			names = append(names, f.name)
		}
	}
	for _, b := range aggregateNames() {
		names = append(names, b.name, b.name+combIf, b.name+combMerge, b.name+combIf+combMerge)
	}
	return names
}

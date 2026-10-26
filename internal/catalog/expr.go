// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"fmt"
	"unicode/utf8"
)

// Expr is a node of the typed expression IR. Every node carries its resolved type, so an
// evaluator never infers one. Which other fields a node uses depends on its Op.
//
// An OpColumn names a column of the row the expression runs on:
//   - in a view version: a stream column, an implicit column or the ARRAY JOIN alias;
//   - in a query: a column of the source (a view's keys and aggregates, or a stream's
//     columns), except in the Columns and OrderBy of an aggregating query, which see the
//     GROUP BY names and reach aggregates through OpAggregate.
//
// The IR holds no conversion nodes. An evaluator applies the type rules of sql.md, which
// are normative for the IR: if, multiIf and coalesce convert the value they return to the
// node's Type (§4.5), arithmetic follows §5.6 with the node's Type as its result, and
// comparisons, IN and BETWEEN compare their operands by exact value without converting
// them (§5.7).
//
// SELECT aliases are already replaced by their expressions, and the IR holds no source
// positions, so formatting never changes it.
type Expr struct {
	Op   Op   `json:"op"`
	Type Type `json:"type"`
	// Value is an OpLiteral constant in canonical text. The compiler writes only this form,
	// and an evaluator rejects any other:
	//   - String: the text itself, valid UTF-8
	//   - integers: base 10 without "+" or leading zeros; zero is "0", never "-0"
	//   - Float64: [strconv.FormatFloat] with format 'g' and precision -1; −0.0 is "0"
	//   - Decimal(P, S): an optional "-", the integer part without leading zeros ("0" when
	//     it is zero), then, when S > 0, a point and exactly S digits, such as "12.50" or
	//     "-0.05"; zero is never negative
	//   - Bool: "true" or "false"
	//   - UUID: lowercase 8-4-4-4-12 hex digits
	//   - Date: "YYYY-MM-DD"; DateTime: "YYYY-MM-DD hh:mm:ss"
	//   - DateTime64(p): "YYYY-MM-DD hh:mm:ss.f" with exactly p fractional digits
	//
	// A Float64 literal stands for the float64 its text parses to. A number literal
	// compared with a Decimal, by a comparison, IN, BETWEEN or a key binding, is instead a
	// Decimal(P, S) literal that holds its exact value (sql.md Dev 7). Written in decimal
	// without trailing zeros after the point, the value has S digits after the point and
	// P - S before it, not counting a lone 0; P is from 1 to 38. So 0.1 is a Decimal(1, 1)
	// "0.1", 1.50 a Decimal(2, 1) "1.5", and 0.30000000000000001 a Decimal(17, 17).
	Value string `json:"value,omitzero"`
	// Name is the column of an OpColumn or the parameter of an OpParam.
	Name string `json:"name,omitzero"`
	// Index is the position of an OpAggregate in [Aggregation].Aggregates.
	Index int `json:"index,omitzero"`
	// Args are the operands, in SQL order.
	Args []Expr `json:"args,omitempty"`
}

// ColumnNames adds the name of every column that e reads to names.
func (e *Expr) ColumnNames(names map[string]bool) {
	if e.Op == OpColumn {
		names[e.Name] = true
	}
	for i := range e.Args {
		e.Args[i].ColumnNames(names)
	}
}

// NamedExpr is an expression and the name of the column it defines.
type NamedExpr struct {
	Name string `json:"name"`
	Expr Expr   `json:"expr"`
}

// Op is the operation of an [Expr]. Function ops use ClickHouse's own function names, so
// an expression prints as ClickHouse SQL.
type Op string

// Leaf ops.
const (
	OpLiteral   Op = "literal"   // A constant: Value.
	OpNull      Op = "null"      // NULL; Type is the Nullable type it takes from its context.
	OpArray     Op = "array"     // An array literal of at most [MaxArrayLen] Args. Only in a column DEFAULT.
	OpColumn    Op = "column"    // A column: Name.
	OpParam     Op = "param"     // A query parameter: Name.
	OpAggregate Op = "aggregate" // A read-time aggregate of a query: Index.
)

// Logic and comparison ops. not and isNull take one Arg, and the comparisons two. The
// compiler writes x BETWEEN a AND b as and(greaterOrEquals(x, a), lessOrEquals(x, b)),
// x NOT IN (…) as not(in(x, …)), and x IS NOT NULL as not(isNull(x)).
const (
	OpAnd             Op = "and" // Two or more Args, evaluated left to right until the result is known.
	OpOr              Op = "or"  // Two or more Args, evaluated left to right until the result is known.
	OpNot             Op = "not"
	OpEquals          Op = "equals"
	OpNotEquals       Op = "notEquals"
	OpLess            Op = "less"
	OpLessOrEquals    Op = "lessOrEquals"
	OpGreater         Op = "greater"
	OpGreaterOrEquals Op = "greaterOrEquals"
	OpIn              Op = "in" // Args[0] in Args[1:]: scalar literals and parameters, or one Array parameter.
	OpIsNull          Op = "isNull"
)

// Arithmetic ops. negate takes one Arg, and the others two.
const (
	OpPlus     Op = "plus"
	OpMinus    Op = "minus"
	OpMultiply Op = "multiply"
	OpDivide   Op = "divide"
	OpModulo   Op = "modulo"
	OpNegate   Op = "negate"
	OpIntDiv   Op = "intDiv"
)

// Conditional ops. Each evaluates only the branch it returns.
const (
	OpIf       Op = "if"       // Three Args.
	OpMultiIf  Op = "multiIf"  // An odd number of Args, at least three.
	OpCoalesce Op = "coalesce" // Two or more Args.
)

// String ops. concat takes two or more Args, and the others one.
const (
	OpLower  Op = "lower"
	OpUpper  Op = "upper"
	OpConcat Op = "concat"
	OpLength Op = "length"
)

// Date and time ops. All times are UTC. Each takes one Arg, except toStartOfWeek.
const (
	OpToDate               Op = "toDate" // The compiler turns a string literal argument into a Date literal.
	OpToStartOfMinute      Op = "toStartOfMinute"
	OpToStartOfFiveMinutes Op = "toStartOfFiveMinutes"
	OpToStartOfHour        Op = "toStartOfHour"
	OpToStartOfDay         Op = "toStartOfDay"
	OpToStartOfWeek        Op = "toStartOfWeek" // Args[1] is always the mode, a UInt8 literal (0 when omitted in SQL).
	OpToStartOfMonth       Op = "toStartOfMonth"
	OpToYYYYMM             Op = "toYYYYMM"
)

// Argument counts of the ops; unlimited means no upper bound.
const (
	oneArg    = 1
	twoArgs   = 2
	threeArgs = 3
	unlimited = -1
)

// argCounts returns the fewest and the most Args that o takes, and false for an unknown
// op. multiIf also needs an odd count.
func (o Op) argCounts() (int, int, bool) {
	switch o {
	case OpLiteral, OpNull, OpColumn, OpParam, OpAggregate:
		return 0, 0, true
	case OpArray:
		return 0, MaxArrayLen, true
	case OpNot, OpIsNull, OpNegate, OpLower, OpUpper, OpLength, OpToDate, OpToStartOfMinute,
		OpToStartOfFiveMinutes, OpToStartOfHour, OpToStartOfDay, OpToStartOfMonth, OpToYYYYMM:
		return oneArg, oneArg, true
	case OpEquals, OpNotEquals, OpLess, OpLessOrEquals, OpGreater, OpGreaterOrEquals,
		OpPlus, OpMinus, OpMultiply, OpDivide, OpModulo, OpIntDiv, OpToStartOfWeek:
		return twoArgs, twoArgs, true
	case OpAnd, OpOr, OpIn, OpCoalesce, OpConcat:
		return twoArgs, unlimited, true
	case OpIf:
		return threeArgs, threeArgs, true
	case OpMultiIf:
		return threeArgs, unlimited, true
	}
	return 0, 0, false
}

// check reports a node whose fields don't fit its Op, so an evaluator can index Args by
// the counts the Op comments give. OpAggregate may refer to nAggs aggregates.
func (e *Expr) check(nAggs int) error {
	if !utf8.ValidString(e.Value) {
		return fmt.Errorf("%s value is not valid UTF-8", e.Op)
	}
	fewest, most, known := e.Op.argCounts()
	if !known {
		return fmt.Errorf("unknown expression op %q", e.Op)
	}
	n := len(e.Args)
	countFits := n >= fewest && (most == unlimited || n <= most) && (e.Op != OpMultiIf || n%2 == 1)
	if !countFits || !e.fieldsFit(nAggs) {
		return fmt.Errorf("invalid %s expression with %d arguments", e.Op, n)
	}
	for i := range e.Args {
		if err := e.Args[i].check(nAggs); err != nil {
			return err
		}
	}
	return nil
}

// fieldsFit reports whether e sets exactly the fields its Op uses. It needs a valid
// argument count.
func (e *Expr) fieldsFit(nAggs int) bool {
	switch {
	case e.Op == OpLiteral:
		return e.Name == "" && e.Index == 0
	case e.Op == OpColumn || e.Op == OpParam:
		return e.Name != "" && e.Value == "" && e.Index == 0
	case e.Op == OpAggregate:
		return e.Name == "" && e.Value == "" && e.Index >= 0 && e.Index < nAggs
	case e.Op == OpToStartOfWeek && e.Args[1].Op != OpLiteral:
		return false
	}
	return e.Name == "" && e.Value == "" && e.Index == 0
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

type evalCase struct {
	name string
	e    catalog.Expr
	// want is the result in canonical text, "NULL", or "error <op>" for an evaluation
	// error of that op.
	want string
}

func testScope() expr.Scope {
	return expr.Scope{
		Columns: []string{"null_int", "null_bool", "null_str", "null_time", "some_int", "u8", "tags", "no_tags", "null"},
		Params:  []string{"tags_p", "empty_p", "null_p"},
		Aggs:    2,
	}
}

func testFrame() *expr.Frame {
	abc := []expr.Value{expr.String("a"), expr.String("b"), expr.String("c")}
	return &expr.Frame{
		Cols: []expr.Value{
			expr.Null(), expr.Null(), expr.Null(), expr.Null(), expr.Int(7), expr.Uint(200),
			expr.Array(abc), expr.Array(nil), expr.Null(),
		},
		Params: []expr.Value{expr.Array(abc), expr.Array(nil), expr.Null()},
		Aggs:   []expr.Value{expr.Uint(41), expr.Uint(42)},
	}
}

// nullOf is a NULL column of type Nullable(typ).
func nullOf(typ string) catalog.Expr { return col("null", "Nullable("+typ+")") }

// fails is an operand whose evaluation fails, to show which operands are skipped.
func fails() catalog.Expr {
	return call(catalog.OpIntDiv, "UInt8", lit("UInt8", "1"), lit("UInt8", "0"))
}

func runEval(t *testing.T, cases []evalCase) {
	t.Helper()
	scope, frame := testScope(), testFrame()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			p, err := expr.Compile(&c.e, scope)
			if err != nil {
				t.Fatal(err)
			}
			// A second run sees any operand the first one changed.
			for range 2 {
				if got := result(c.e.Type, p, frame); got != c.want {
					t.Fatalf("got %s, want %s", got, c.want)
				}
			}
		})
	}
}

func result(t catalog.Type, p *expr.Program, f *expr.Frame) string {
	v, err := p.Eval(f)
	var evalErr *expr.Error
	switch {
	case errors.As(err, &evalErr) && errors.Is(err, expr.ErrEval):
		return "error " + string(evalErr.Op)
	case err != nil:
		return "unexpected error: " + err.Error()
	}
	return render(t, v)
}

func TestIntegerArithmetic(t *testing.T) {
	t.Parallel()
	const u64Max, i64Max, i64Min = "18446744073709551615", "9223372036854775807", "-9223372036854775808"
	runEval(t, []evalCase{
		{"UInt8 + UInt8 widens", call(catalog.OpPlus, "UInt16", lit("UInt8", "200"), lit("UInt8", "100")), "300"},
		{"Int64 max + 1 wraps to min", call(catalog.OpPlus, "Int64", lit("Int64", i64Max), lit("UInt8", "1")), i64Min},
		{"UInt64 max + Int8 1 is Int64 0", call(catalog.OpPlus, "Int64", lit("UInt64", u64Max), lit("Int8", "1")), "0"},
		{"UInt32 - UInt32 is signed", call(catalog.OpMinus, "Int64", lit("UInt32", "1"), lit("UInt32", "2")), "-1"},
		{"Int8 * Int8 widens", call(catalog.OpMultiply, "Int16", lit("Int8", "-128"), lit("Int8", "-1")), "128"},
		{"UInt64 product wraps", call(catalog.OpMultiply, "UInt64", lit("UInt64", u64Max), lit("UInt8", "2")), "18446744073709551614"},
		{"Int64 product wraps", call(catalog.OpMultiply, "Int64", lit("Int64", "3037000500"), lit("Int64", "3037000500")), "-9223372036709301616"},
		{"remainder takes the dividend's sign", call(catalog.OpModulo, "Int16", lit("Int8", "-7"), lit("UInt8", "2")), "-1"},
		{"UInt8 % negative Int8", call(catalog.OpModulo, "UInt8", lit("UInt8", "200"), lit("Int8", "-3")), "2"},
		{"UInt64 max % Int8", call(catalog.OpModulo, "UInt8", lit("UInt64", u64Max), lit("Int8", "-10")), "5"},
		{"intDiv truncates toward zero", call(catalog.OpIntDiv, "Int8", lit("Int8", "-7"), lit("UInt8", "2")), "-3"},
		{"intDiv of UInt64 max by -1 wraps", call(catalog.OpIntDiv, "Int64", lit("UInt64", u64Max), lit("Int8", "-1")), "1"},
		{"intDiv by zero", fails(), "error intDiv"},
		{"modulo by zero", call(catalog.OpModulo, "UInt8", lit("UInt8", "7"), lit("UInt8", "0")), "error modulo"},
		{"Int8 min intDiv -1", call(catalog.OpIntDiv, "Int8", lit("Int8", "-128"), lit("Int8", "-1")), "error intDiv"},
		{"Int64 min % -1", call(catalog.OpModulo, "Int16", lit("Int64", i64Min), lit("Int8", "-1")), "error modulo"},
		{"the dividend's type decides min / -1", call(catalog.OpIntDiv, "Int8", lit("Int8", "-128"), lit("Int16", "-1")), "error intDiv"},
		{"-128 is not the minimum Int16", call(catalog.OpIntDiv, "Int16", lit("Int16", "-128"), lit("Int8", "-1")), "128"},
		{"negate UInt8", call(catalog.OpNegate, "Int16", lit("UInt8", "5")), "-5"},
		{"negate UInt64 max", call(catalog.OpNegate, "Int64", lit("UInt64", u64Max)), "1"},
		{"negate Int8 min wraps", call(catalog.OpNegate, "Int8", lit("Int8", "-128")), "-128"},
		{"NULL operand", call(catalog.OpPlus, "Nullable(Int64)", col("null_int", "Nullable(Int64)"), lit("UInt8", "1")), "NULL"},
	})
}

func TestFloatArithmetic(t *testing.T) {
	t.Parallel()
	runEval(t, []evalCase{
		{"integer / is Float64", call(catalog.OpDivide, "Float64", lit("UInt8", "1"), lit("UInt8", "4")), "0.25"},
		{"x / 0", call(catalog.OpDivide, "Float64", lit("UInt8", "1"), lit("UInt8", "0")), "error divide"},
		{"0 / 0", call(catalog.OpDivide, "Float64", lit("Float64", "0"), lit("UInt8", "0")), "error divide"},
		{"overflow to infinity", call(catalog.OpMultiply, "Float64", lit("Float64", "1e+308"), lit("UInt8", "10")), "error multiply"},
		{"each operation rounds", call(catalog.OpPlus, "Float64", lit("Float64", "0.1"), lit("Float64", "0.2")), "0.30000000000000004"},
		{"integers convert to the nearest Float64, ties to even", call(catalog.OpDivide, "Float64", lit("UInt64", "9007199254740993"), lit("UInt8", "1")), "9.007199254740992e+15"},
		{"-0 is 0", call(catalog.OpMultiply, "Float64", lit("Float64", "-1.5"), lit("UInt8", "0")), "0"},
		{"negate 0", call(catalog.OpNegate, "Float64", lit("Float64", "0")), "0"},
		{"negate", call(catalog.OpNegate, "Float64", lit("Float64", "1.5")), "-1.5"},
		{"Float64 - Int8", call(catalog.OpMinus, "Float64", lit("Float64", "0.5"), lit("Int8", "-1")), "1.5"},
	})
}

func TestDecimalArithmetic(t *testing.T) {
	t.Parallel()
	nines38 := strings.Repeat("9", 38)
	int32Min := call(catalog.OpMinus, "Decimal(9, 0)",
		call(catalog.OpMinus, "Decimal(9, 0)", lit("Decimal(9, 0)", "-999999999"), lit("Decimal(9, 0)", "999999999")),
		lit("Decimal(9, 0)", "147483650"))
	// A Decimal plus an Int64 has the Decimal's type, so the sum can pass P.
	plus9 := func(d, i string) catalog.Expr {
		return call(catalog.OpPlus, "Decimal(9, 0)", lit("Decimal(9, 0)", d), lit("Int64", i))
	}
	plus18 := func(d, i string) catalog.Expr {
		return call(catalog.OpPlus, "Decimal(18, 0)", lit("Decimal(18, 0)", d), lit("Int64", i))
	}
	runEval(t, []evalCase{
		{"Decimal + Decimal", call(catalog.OpPlus, "Decimal(9, 2)", lit("Decimal(5, 2)", "1.50"), lit("Decimal(5, 1)", "1.5")), "3.00"},
		{"Decimal + Int64 keeps the Decimal type", call(catalog.OpPlus, "Decimal(5, 2)", lit("Decimal(5, 2)", "1.50"), lit("Int64", "2")), "3.50"},
		{"Int8 - Decimal", call(catalog.OpMinus, "Decimal(5, 2)", lit("Int8", "1"), lit("Decimal(5, 2)", "1.50")), "-0.50"},
		{"Decimal * Decimal adds the scales", call(catalog.OpMultiply, "Decimal(9, 3)", lit("Decimal(5, 2)", "1.50"), lit("Decimal(5, 1)", "2.5")), "3.750"},
		{"Decimal * UInt8", call(catalog.OpMultiply, "Decimal(5, 2)", lit("Decimal(5, 2)", "1.50"), lit("UInt8", "3")), "4.50"},
		{"integer / Decimal truncates", call(catalog.OpDivide, "Decimal(5, 2)", lit("UInt16", "1000"), lit("Decimal(5, 2)", "1.50")), "666.66"},
		{"Decimal / Decimal", call(catalog.OpDivide, "Decimal(9, 2)", lit("Decimal(9, 2)", "1.00"), lit("Decimal(9, 1)", "3.0")), "0.33"},
		{"a negative quotient truncates toward zero", call(catalog.OpDivide, "Decimal(9, 2)", lit("Decimal(9, 2)", "-1.00"), lit("Decimal(9, 1)", "3.0")), "-0.33"},
		{"Decimal / Int8", call(catalog.OpDivide, "Decimal(9, 2)", lit("Decimal(9, 2)", "1.00"), lit("Int8", "-3")), "-0.33"},
		{"Decimal / 0", call(catalog.OpDivide, "Decimal(5, 2)", lit("Decimal(5, 2)", "1.50"), lit("Decimal(5, 0)", "0")), "error divide"},
		{"a result may pass P within its storage", call(catalog.OpPlus, "Decimal(9, 0)", lit("Decimal(9, 0)", "999999999"), lit("Decimal(9, 0)", "999999999")), "1999999998"},
		{"32-bit storage holds its minimum", int32Min, "-2147483648"},
		{"32-bit storage overflows", call(catalog.OpNegate, "Decimal(9, 0)", int32Min), "error negate"},
		{"32-bit storage holds its maximum", plus9("999999999", "1147483648"), "2147483647"},
		{"32-bit storage overflows above", plus9("999999999", "1147483649"), "error plus"},
		{"32-bit storage overflows below", plus9("-999999999", "-1147483650"), "error plus"},
		{"64-bit storage holds its maximum", plus18("999999999999999999", "8223372036854775808"), "9223372036854775807"},
		{"64-bit storage overflows above", plus18("999999999999999999", "8223372036854775809"), "error plus"},
		{"64-bit storage holds its minimum", plus18("-999999999999999999", "-8223372036854775809"), "-9223372036854775808"},
		{"64-bit storage overflows below", plus18("-999999999999999999", "-8223372036854775810"), "error plus"},
		{"38 digits hold", call(catalog.OpPlus, "Decimal(38, 0)", lit("Decimal(38, 0)", nines38), lit("Int8", "-1")), strings.Repeat("9", 37) + "8"},
		{"38 digits overflow", call(catalog.OpPlus, "Decimal(38, 0)", lit("Decimal(38, 0)", nines38), lit("UInt8", "1")), "error plus"},
		{"negate", call(catalog.OpNegate, "Decimal(9, 2)", lit("Decimal(9, 2)", "1.50")), "-1.50"},
	})
}

func TestComparisons(t *testing.T) {
	t.Parallel()
	const two53Plus1, two53 = "9007199254740993", "9.007199254740992e+15"
	const id1, id2 = "61f0c404-5cb3-11e7-907b-a6006ad3dba0", "61f0c404-5cb3-11e7-907b-a6006ad3dba1"
	cmp := func(op catalog.Op, a, b catalog.Expr) catalog.Expr { return call(op, "Bool", a, b) }
	runEval(t, []evalCase{
		{"UInt64 = Float64 by exact value", cmp(catalog.OpEquals, lit("UInt64", two53Plus1), lit("Float64", two53)), "false"},
		{"UInt64 > Float64 by exact value", cmp(catalog.OpGreater, lit("UInt64", two53Plus1), lit("Float64", two53)), "true"},
		{"Int8 -1 = UInt64 max", cmp(catalog.OpEquals, lit("Int8", "-1"), lit("UInt64", "18446744073709551615")), "false"},
		{"Int8 -1 < UInt64 max", cmp(catalog.OpLess, lit("Int8", "-1"), lit("UInt64", "18446744073709551615")), "true"},
		{"2^63 as Float64 > Int64 max", cmp(catalog.OpGreater, lit("Float64", "9.223372036854776e+18"), lit("Int64", "9223372036854775807")), "true"},
		{"-2^63 as Float64 = Int64 min", cmp(catalog.OpEquals, lit("Float64", "-9.223372036854776e+18"), lit("Int64", "-9223372036854775808")), "true"},
		{"a fraction below a negative integer", cmp(catalog.OpLess, lit("Float64", "-2.5"), lit("Int8", "-2")), "true"},
		{"Decimals of different scales", cmp(catalog.OpEquals, lit("Decimal(3, 2)", "1.50"), lit("Decimal(2, 1)", "1.5")), "true"},
		{"Decimal > integer", cmp(catalog.OpGreater, lit("Decimal(3, 2)", "1.50"), lit("UInt8", "1")), "true"},
		{"Float64 0.1 = Decimal 0.1", cmp(catalog.OpEquals, lit("Float64", "0.1"), lit("Decimal(1, 1)", "0.1")), "false"},
		{"Float64 0.5 = Decimal 0.5", cmp(catalog.OpEquals, lit("Float64", "0.5"), lit("Decimal(1, 1)", "0.5")), "true"},
		{"<= at equality", cmp(catalog.OpLessOrEquals, lit("UInt8", "3"), lit("Int64", "3")), "true"},
		{">= at equality", cmp(catalog.OpGreaterOrEquals, lit("UInt8", "3"), lit("Int64", "3")), "true"},
		{"!= at equality", cmp(catalog.OpNotEquals, lit("UInt8", "3"), lit("Int64", "3")), "false"},
		{"strings compare by bytes", cmp(catalog.OpLess, lit("String", "Z"), lit("String", "a")), "true"},
		{"UTF-8 after ASCII", cmp(catalog.OpGreater, lit("String", "é"), lit("String", "z")), "true"},
		{"a Date is midnight", cmp(catalog.OpEquals, lit("Date", "2024-05-31"), lit("DateTime64(3)", "2024-05-31 00:00:00.000")), "true"},
		{"Date < DateTime", cmp(catalog.OpLess, lit("Date", "2024-05-31"), lit("DateTime", "2024-05-31 00:00:01")), "true"},
		{"false < true", cmp(catalog.OpLess, boolean(false), boolean(true)), "true"},
		{"UUID =", cmp(catalog.OpEquals, lit("UUID", id1), lit("UUID", id1)), "true"},
		{"UUID !=", cmp(catalog.OpNotEquals, lit("UUID", id1), lit("UUID", id2)), "true"},
		{"NULL operand", cmp(catalog.OpEquals, col("null_int", "Nullable(Int64)"), lit("Int64", "1")), "NULL"},
	})
}

func TestIn(t *testing.T) {
	t.Parallel()
	in := func(args ...catalog.Expr) catalog.Expr { return call(catalog.OpIn, "Bool", args...) }
	nullInt := col("null_int", "Nullable(Int64)")
	runEval(t, []evalCase{
		{"in the list", in(lit("UInt8", "2"), lit("UInt8", "1"), lit("UInt8", "2")), "true"},
		{"not in the list", in(lit("UInt8", "2"), lit("UInt8", "1"), lit("UInt8", "3")), "false"},
		{"items compare across types", in(lit("Float64", "1"), lit("UInt8", "1")), "true"},
		{"items compare exactly", in(lit("UInt64", "9007199254740993"), lit("Float64", "9.007199254740992e+15")), "false"},
		{"NULL left side", in(nullInt, lit("Int64", "1")), "NULL"},
		{"NOT IN with a NULL left side", call(catalog.OpNot, "Nullable(Bool)", in(nullInt, lit("Int64", "1"))), "NULL"},
		{"in an Array parameter", in(lit("String", "b"), param("tags_p", "Array(String)")), "true"},
		{"not in an Array parameter", in(lit("String", "z"), param("tags_p", "Array(String)")), "false"},
		{"an empty Array matches nothing", in(lit("String", "a"), param("empty_p", "Array(String)")), "false"},
		{"NOT IN an empty Array", call(catalog.OpNot, "Bool", in(lit("String", "a"), param("empty_p", "Array(String)"))), "true"},
		{"a NULL item never matches", in(lit("Int64", "1"), param("null_p", "Nullable(Int64)")), "false"},
	})
}

func TestLogic(t *testing.T) {
	t.Parallel()
	and := func(args ...catalog.Expr) catalog.Expr { return call(catalog.OpAnd, "Nullable(Bool)", args...) }
	or := func(args ...catalog.Expr) catalog.Expr { return call(catalog.OpOr, "Nullable(Bool)", args...) }
	nullBool := col("null_bool", "Nullable(Bool)")
	failing := call(catalog.OpEquals, "Bool", fails(), lit("UInt8", "1"))
	runEval(t, []evalCase{
		{"true AND NULL", and(boolean(true), nullBool), "NULL"},
		{"NULL AND false", and(nullBool, boolean(false)), "false"},
		{"false AND NULL", and(boolean(false), nullBool), "false"},
		{"true AND true AND true", and(boolean(true), boolean(true), boolean(true)), "true"},
		{"NULL OR true", or(nullBool, boolean(true)), "true"},
		{"false OR NULL", or(boolean(false), nullBool), "NULL"},
		{"false OR false", or(boolean(false), boolean(false)), "false"},
		{"NOT NULL", call(catalog.OpNot, "Nullable(Bool)", nullBool), "NULL"},
		{"NOT true", call(catalog.OpNot, "Bool", boolean(true)), "false"},
		{"AND stops at false", and(boolean(true), boolean(false), failing), "false"},
		{"OR stops at true", or(boolean(true), failing), "true"},
		{"AND goes on after NULL", and(nullBool, failing), "error intDiv"},
		{"IS NULL", call(catalog.OpIsNull, "Bool", col("null_int", "Nullable(Int64)")), "true"},
		{"IS NULL of a value", call(catalog.OpIsNull, "Bool", col("some_int", "Nullable(Int64)")), "false"},
		{"IS NOT NULL", call(catalog.OpNot, "Bool", call(catalog.OpIsNull, "Bool", col("some_int", "Nullable(Int64)"))), "true"},
	})
}

func TestConditionals(t *testing.T) {
	t.Parallel()
	nullInt, someInt := col("null_int", "Nullable(Int64)"), col("some_int", "Nullable(Int64)")
	runEval(t, []evalCase{
		{"if converts to the common type", call(catalog.OpIf, "Int16", boolean(true), lit("UInt8", "1"), lit("Int8", "-1")), "1"},
		{"if takes else", call(catalog.OpIf, "Int16", boolean(false), lit("UInt8", "1"), lit("Int8", "-1")), "-1"},
		{"a NULL condition takes else", call(catalog.OpIf, "UInt8", col("null_bool", "Nullable(Bool)"), lit("UInt8", "1"), lit("UInt8", "2")), "2"},
		{
			"if evaluates only the branch it returns",
			call(catalog.OpIf, "Nullable(Float64)",
				call(catalog.OpEquals, "Bool", lit("UInt8", "0"), lit("UInt8", "0")),
				null("Nullable(Float64)"),
				call(catalog.OpDivide, "Float64", lit("UInt8", "1"), lit("UInt8", "0"))),
			"NULL",
		},
		{"integer to Float64", call(catalog.OpIf, "Float64", boolean(true), lit("UInt8", "1"), lit("Float64", "0.5")), "1"},
		{"integer to Decimal", call(catalog.OpIf, "Decimal(18, 2)", boolean(true), lit("Int32", "7"), lit("Decimal(5, 2)", "1.50")), "7.00"},
		{"Decimal to a wider class", call(catalog.OpIf, "Decimal(18, 2)", boolean(false), lit("Int32", "7"), lit("Decimal(5, 2)", "1.50")), "1.50"},
		{
			"multiIf takes the first true condition",
			call(catalog.OpMultiIf, "UInt8", boolean(false), lit("UInt8", "1"), col("null_bool", "Nullable(Bool)"), lit("UInt8", "2"),
				boolean(true), lit("UInt8", "3"), lit("UInt8", "4")),
			"3",
		},
		{"multiIf converts else", call(catalog.OpMultiIf, "UInt16", boolean(false), lit("UInt8", "1"), lit("UInt16", "300")), "300"},
		{"multiIf skips the values it doesn't return", call(catalog.OpMultiIf, "UInt8", boolean(false), fails(), lit("UInt8", "2")), "2"},
		{"coalesce takes the first value", call(catalog.OpCoalesce, "Int64", nullInt, someInt, lit("Int64", "5")), "7"},
		{"coalesce stops at the first value", call(catalog.OpCoalesce, "Int64", someInt, fails()), "7"},
		{"coalesce converts", call(catalog.OpCoalesce, "UInt16", col("u8", "Nullable(UInt8)"), lit("UInt16", "1")), "200"},
		{"coalesce of NULLs", call(catalog.OpCoalesce, "Nullable(Int64)", nullInt, nullInt), "NULL"},
	})
}

func TestStringFunctions(t *testing.T) {
	t.Parallel()
	nullStr := col("null_str", "Nullable(String)")
	runEval(t, []evalCase{
		{"lower changes ASCII only", call(catalog.OpLower, "String", lit("String", "ÀB")), `"Àb"`},
		{"upper changes ASCII only", call(catalog.OpUpper, "String", lit("String", "àb")), `"àB"`},
		{"lower changes A to Z only", call(catalog.OpLower, "String", lit("String", "@AZ[")), `"@az["`},
		{"upper changes a to z only", call(catalog.OpUpper, "String", lit("String", "`az{")), "\"`AZ{\""},
		{"lower NULL", call(catalog.OpLower, "Nullable(String)", nullStr), "NULL"},
		{"concat", call(catalog.OpConcat, "String", lit("String", "a"), lit("String", "b"), lit("String", "c")), `"abc"`},
		{"concat NULL", call(catalog.OpConcat, "Nullable(String)", lit("String", "a"), nullStr), "NULL"},
		{"length counts bytes", call(catalog.OpLength, "UInt64", lit("String", "é")), "2"},
		{"length counts elements", call(catalog.OpLength, "UInt64", col("tags", "Array(String)")), "3"},
		{"length of an empty Array", call(catalog.OpLength, "UInt64", col("no_tags", "Array(String)")), "0"},
		{"length NULL", call(catalog.OpLength, "Nullable(UInt64)", nullStr), "NULL"},
	})
}

func TestTimeFunctions(t *testing.T) {
	t.Parallel()
	week := func(day, mode string) catalog.Expr {
		return call(catalog.OpToStartOfWeek, "Date", lit("Date", day), lit("UInt8", mode))
	}
	runEval(t, []evalCase{
		{"toDate", call(catalog.OpToDate, "Date", lit("DateTime64(3)", "2024-05-31 23:59:59.999")), "2024-05-31"},
		{"toDate NULL", call(catalog.OpToDate, "Nullable(Date)", col("null_time", "Nullable(DateTime64(3))")), "NULL"},
		{"toStartOfMinute", call(catalog.OpToStartOfMinute, "DateTime", lit("DateTime64(6)", "2024-05-31 13:45:30.250000")), "2024-05-31 13:45:00"},
		{"toStartOfFiveMinutes", call(catalog.OpToStartOfFiveMinutes, "DateTime", lit("DateTime64(3)", "2024-05-31 13:49:59.999")), "2024-05-31 13:45:00"},
		{"toStartOfHour", call(catalog.OpToStartOfHour, "DateTime", lit("DateTime", "2024-05-31 13:59:59")), "2024-05-31 13:00:00"},
		{"toStartOfDay of a Date", call(catalog.OpToStartOfDay, "DateTime", lit("Date", "2024-05-31")), "2024-05-31 00:00:00"},
		{"toStartOfDay", call(catalog.OpToStartOfDay, "DateTime", lit("DateTime64(3)", "2024-05-31 13:45:00.000")), "2024-05-31 00:00:00"},
		{"mode 0 weeks start on Sunday", week("2024-05-31", "0"), "2024-05-26"},
		{"mode 2 weeks start on Sunday", week("2024-05-31", "2"), "2024-05-26"},
		{"mode 1 weeks start on Monday", week("2024-05-31", "1"), "2024-05-27"},
		{"mode 9 weeks start on Monday", week("2024-05-31", "9"), "2024-05-27"},
		{"a Sunday starts its week", week("2024-05-26", "0"), "2024-05-26"},
		{"a Sunday ends a Monday week", week("2024-05-26", "1"), "2024-05-20"},
		{
			"start of week from a time",
			call(catalog.OpToStartOfWeek, "Date", lit("DateTime64(6)", "2024-05-27 23:59:59.999999"), lit("UInt8", "1")),
			"2024-05-27",
		},
		{"a week starting before 1970 starts on 1970-01-01", week("1970-01-03", "0"), "1970-01-01"},
		{"a Monday week starting before 1970", week("1970-01-01", "1"), "1970-01-01"},
		{"the first Sunday week", week("1970-01-04", "0"), "1970-01-04"},
		{"toStartOfMonth", call(catalog.OpToStartOfMonth, "Date", lit("DateTime64(3)", "2024-02-29 10:00:00.000")), "2024-02-01"},
		{"toYYYYMM", call(catalog.OpToYYYYMM, "UInt32", lit("Date", "2024-05-31")), "202405"},
		{"toYYYYMM at the end of a year", call(catalog.OpToYYYYMM, "UInt32", lit("DateTime64(6)", "1999-12-31 23:59:59.999999")), "199912"},
	})
}

// TestNullInGivesNullOut checks the default rule of sql.md §5.5 for every op that follows
// it: a NULL in any operand position makes the result NULL. IN, IS NULL, AND, OR and the
// conditionals have their own rules and cases.
func TestNullInGivesNullOut(t *testing.T) {
	t.Parallel()
	type operand struct{ typ, value string }
	i, f, d := operand{"Int64", "6"}, operand{"Float64", "1.5"}, operand{"Decimal(9, 2)", "1.50"}
	s, b, tm := operand{"String", "a"}, operand{"Bool", "true"}, operand{"DateTime64(3)", "2024-05-31 13:45:30.250"}
	ops := []struct {
		op       catalog.Op
		result   string
		operands []operand
	}{
		{catalog.OpPlus, "Int64", []operand{i, i}},
		{catalog.OpPlus, "Float64", []operand{f, f}},
		{catalog.OpPlus, "Decimal(9, 2)", []operand{d, d}},
		{catalog.OpMinus, "Int64", []operand{i, i}},
		{catalog.OpMinus, "Float64", []operand{f, f}},
		{catalog.OpMinus, "Decimal(9, 2)", []operand{d, d}},
		{catalog.OpMultiply, "Int64", []operand{i, i}},
		{catalog.OpMultiply, "Float64", []operand{f, f}},
		{catalog.OpMultiply, "Decimal(9, 4)", []operand{d, d}},
		{catalog.OpDivide, "Float64", []operand{f, f}},
		{catalog.OpDivide, "Decimal(9, 2)", []operand{d, d}},
		{catalog.OpModulo, "Int64", []operand{i, i}},
		{catalog.OpIntDiv, "Int64", []operand{i, i}},
		{catalog.OpNegate, "Int64", []operand{i}},
		{catalog.OpNegate, "Float64", []operand{f}},
		{catalog.OpNegate, "Decimal(9, 2)", []operand{d}},
		{catalog.OpEquals, "Bool", []operand{i, i}},
		{catalog.OpNotEquals, "Bool", []operand{i, i}},
		{catalog.OpLess, "Bool", []operand{i, i}},
		{catalog.OpLessOrEquals, "Bool", []operand{i, i}},
		{catalog.OpGreater, "Bool", []operand{i, i}},
		{catalog.OpGreaterOrEquals, "Bool", []operand{i, i}},
		{catalog.OpNot, "Bool", []operand{b}},
		{catalog.OpLower, "String", []operand{s}},
		{catalog.OpUpper, "String", []operand{s}},
		{catalog.OpConcat, "String", []operand{s, s, s}},
		{catalog.OpLength, "UInt64", []operand{s}},
		{catalog.OpToDate, "Date", []operand{tm}},
		{catalog.OpToStartOfMinute, "DateTime", []operand{tm}},
		{catalog.OpToStartOfFiveMinutes, "DateTime", []operand{tm}},
		{catalog.OpToStartOfHour, "DateTime", []operand{tm}},
		{catalog.OpToStartOfDay, "DateTime", []operand{tm}},
		{catalog.OpToStartOfMonth, "Date", []operand{tm}},
		{catalog.OpToYYYYMM, "UInt32", []operand{tm}},
	}
	var cases []evalCase
	for _, o := range ops {
		for pos := range o.operands {
			args := make([]catalog.Expr, len(o.operands))
			for k, a := range o.operands {
				args[k] = lit(a.typ, a.value)
			}
			args[pos] = nullOf(o.operands[pos].typ)
			name := fmt.Sprintf("%s %s NULL operand %d", o.op, o.result, pos+1)
			cases = append(cases, evalCase{name, call(o.op, "Nullable("+o.result+")", args...), "NULL"})
		}
	}
	// A conditional converts the value it returns to its own type, and NULL stays NULL.
	convert := func(to string) catalog.Expr {
		return call(catalog.OpIf, "Nullable("+to+")", boolean(true), nullOf("Int8"), lit(to, "1"))
	}
	runEval(t, append(cases,
		evalCase{"toStartOfWeek NULL", call(catalog.OpToStartOfWeek, "Nullable(Date)", nullOf("DateTime64(3)"), lit("UInt8", "1")), "NULL"},
		evalCase{"NULL converted to Int16", convert("Int16"), "NULL"},
		evalCase{"NULL converted to Float64", convert("Float64"), "NULL"},
		evalCase{"NULL converted to Decimal", call(catalog.OpIf, "Nullable(Decimal(9, 2))", boolean(true), nullOf("Int8"), lit("Decimal(9, 2)", "1.00")), "NULL"},
	))
}

// TestNullDoesNotSkipOperands checks that only the ops sql.md §5.8 names skip operands: a
// NULL operand of any other op still lets a later operand fail.
func TestNullDoesNotSkipOperands(t *testing.T) {
	t.Parallel()
	failingStr := call(catalog.OpIf, "String", call(catalog.OpEquals, "Bool", fails(), lit("UInt8", "1")), lit("String", "a"), lit("String", "b"))
	runEval(t, []evalCase{
		{"NULL + failing", call(catalog.OpPlus, "Nullable(Int64)", nullOf("Int64"), fails()), "error intDiv"},
		{"failing + NULL", call(catalog.OpPlus, "Nullable(Int64)", fails(), nullOf("Int64")), "error intDiv"},
		{"NULL = failing", call(catalog.OpEquals, "Nullable(Bool)", nullOf("Int64"), fails()), "error intDiv"},
		{"concat NULL and failing", call(catalog.OpConcat, "Nullable(String)", nullOf("String"), failingStr), "error intDiv"},
	})
}

func TestLeaves(t *testing.T) {
	t.Parallel()
	runEval(t, []evalCase{
		{"array literal", call(catalog.OpArray, "Array(String)", lit("String", "a"), lit("String", "b")), `["a", "b"]`},
		{"aggregate slot", catalog.Expr{Op: catalog.OpAggregate, Type: ty("UInt64"), Index: 1}, "42"},
		{"column slot", col("u8", "Nullable(UInt8)"), "200"},
	})
}

func TestEvalErrorMatchesErrEval(t *testing.T) {
	t.Parallel()
	e := fails()
	p, err := expr.Compile(&e, expr.Scope{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Eval(&expr.Frame{})
	if want := "evaluation error in intDiv: division by zero"; err == nil || err.Error() != want {
		t.Fatalf("got %v, want %q", err, want)
	}
	if !errors.Is(err, expr.ErrEval) {
		t.Fatal("the error doesn't match ErrEval")
	}
}

func TestCompileRejects(t *testing.T) {
	t.Parallel()
	scope := expr.Scope{Columns: []string{"a"}, Params: []string{"p"}, Aggs: 1}
	cases := []struct {
		name string
		e    *catalog.Expr
		want string // in the error message
	}{
		{"nil", nil, "nil expression"},
		{"unknown column", new(col("b", "Int64")), `"b" is not in scope`},
		{"a parameter name as a column", new(col("p", "Int64")), `"p" is not in scope`},
		{"unknown parameter", new(param("q", "Int64")), `"q" is not in scope`},
		{"aggregate out of scope", &catalog.Expr{Op: catalog.OpAggregate, Type: ty("UInt64"), Index: 1}, "index 1 is not in scope"},
		{"non-canonical literal", new(lit("UInt8", "01")), `invalid UInt8 literal "01"`},
		{"literal inside an op", new(call(catalog.OpNot, "Bool", lit("Bool", "TRUE"))), `invalid Bool literal "TRUE"`},
		{"unknown op", &catalog.Expr{Op: "sqrt", Type: ty("Float64")}, `unknown op "sqrt"`},
		{"arithmetic with no such result", new(call(catalog.OpModulo, "Float64", lit("Float64", "1"), lit("UInt8", "2"))), "no Float64 result"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			_, err := expr.Compile(c.e, scope)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want an error with %q", err, c.want)
			}
		})
	}
}

func TestEvalRejectsAFrameSmallerThanTheScope(t *testing.T) {
	t.Parallel()
	e := col("b", "Int64")
	p, err := expr.Compile(&e, expr.Scope{Columns: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []*expr.Frame{nil, {Cols: []expr.Value{expr.Int(1)}}} {
		if _, evalErr := p.Eval(f); evalErr == nil || errors.Is(evalErr, expr.ErrEval) {
			t.Errorf("Eval(%v) = %v, want an error that isn't an evaluation error", f, evalErr)
		}
	}
	v, err := p.Eval(&expr.Frame{Cols: []expr.Value{expr.Int(1), expr.Int(2)}})
	if err != nil || v.AsInt() != 2 {
		t.Errorf("Eval reads %v, %v from column b, want 2", v.AsInt(), err)
	}
}

func TestProgramIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()
	e := call(catalog.OpDivide, "Decimal(9, 2)",
		call(catalog.OpNegate, "Decimal(9, 2)", lit("Decimal(9, 2)", "1.00")), lit("Decimal(9, 1)", "3.0"))
	p, err := expr.Compile(&e, expr.Scope{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				if got := result(e.Type, p, &expr.Frame{}); got != "-0.33" {
					t.Errorf("got %s, want -0.33", got)
					return
				}
			}
		})
	}
	wg.Wait()
}

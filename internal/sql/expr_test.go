// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql_test

import (
	"math"
	"testing"

	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
)

// parseItem parses SELECT <src> FROM t and returns the file and its first
// item's expression.
func parseItem(t *testing.T, src string) (*sql.File, sql.Expr) {
	t.Helper()
	f, ds := sql.Parse("q.sql", []byte("SELECT "+src+" FROM t"))
	if len(ds) != 0 {
		t.Fatalf("Parse(%q):\n%s", src, diagLines(ds))
	}
	return f, f.Stmts[0].(*sql.Query).Select.Items[0].X
}

// The precedence and associativity table, with level 4 built left-nested so
// that the compiler can see chains.
func TestPrecedence(t *testing.T) {
	t.Parallel()
	tests := []struct{ src, want string }{
		{"a OR b AND c", "(or a (and b c))"},
		{"a AND b OR c", "(or (and a b) c)"},
		{"a OR b OR c", "(or (or a b) c)"},
		{"a AND b AND c", "(and (and a b) c)"},
		{"NOT a = b", "(not (= a b))"},
		{"NOT a AND b", "(and (not a) b)"},
		{"NOT NOT a", "(not (not a))"},
		{"NOT a BETWEEN 1 AND 2", "(not (between a 1 2))"},
		{"NOT x IS NULL", "(not (is-null x))"},
		{"a = b AND c != d OR e < f", "(or (and (= a b) (!= c d)) (< e f))"},
		{"a == b", "(= a b)"},
		{"a <> b", "(!= a b)"},
		{"a <= b", "(<= a b)"},
		{"a >= b", "(>= a b)"},
		{"a > b", "(> a b)"},
		{"a + b = c * d", "(= (+ a b) (* c d))"},
		{"a = b = c", "(= (= a b) c)"},
		{"a = b IS NULL", "(is-null (= a b))"},
		{"x BETWEEN 1 AND 2 = 1", "(= (between x 1 2) 1)"},
		{"x IN (1) IN (2)", "(in (in x (list 1)) (list 2))"},
		{"(a = b) = c", "(= (paren (= a b)) c)"},
		{"x BETWEEN 1 AND 2 AND y", "(and (between x 1 2) y)"},
		{"x NOT BETWEEN a + 1 AND b * 2", "(not-between x (+ a 1) (* b 2))"},
		{"x IS NOT NULL AND y", "(and (is-not-null x) y)"},
		{"x NOT IN (1, -2)", "(not-in x (list 1 -2))"},
		{"x IN {p:Array(UInt8)}", "(in x {p:Array(UInt8)})"},
		{"a + b * c", "(+ a (* b c))"},
		{"a - b - c", "(- (- a b) c)"},
		{"a / b * c % d", "(% (* (/ a b) c) d)"},
		{"a * b + c % d", "(+ (* a b) (% c d))"},
		{"-a * b", "(* (neg a) b)"},
		{"- - a", "(neg (neg a))"},
		{"-1", "-1"},
		{"- 1", "-1"},
		{"-1 * 2", "(* -1 2)"},
		{"-(1)", "(neg (paren 1))"},
		{"5 -3", "(- 5 3)"},
		{"5 - -3", "(- 5 -3)"},
		{"a-1", "(- a 1)"},
		{"f(a, b + 1)", "f(a, (+ b 1))"},
		{"and(a, b) OR c", "(or and(a, b) c)"},
		{"NOT(a)", "(not (paren a))"},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			t.Parallel()
			if _, x := parseItem(t, tt.src); expr(x) != tt.want {
				t.Errorf("got %s, want %s", expr(x), tt.want)
			}
		})
	}
}

func TestStringLiterals(t *testing.T) {
	t.Parallel()
	tests := []struct{ src, want string }{
		{`'home'`, "home"},
		{`''`, ""},
		{`'it''s'`, "it's"},
		{`'it\'s'`, "it's"},
		{`'a\\b'`, `a\b`},
		{`'\n\r\t\0'`, "\n\r\t\x00"},
		{`'\x41\x6a'`, "Aj"},
		{`'\xC3\xA9'`, "é"},
		{`'é'`, "é"},
		{`'-- /* not comments */'`, "-- /* not comments */"},
		{"'a;b'", "a;b"},
		{"'a\rb'", "a\rb"}, // a line ends only at a line feed
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			t.Parallel()
			_, x := parseItem(t, tt.src)
			if s, ok := x.(*sql.StringLit); !ok || s.Value != tt.want {
				t.Errorf("got %s, want %q", expr(x), tt.want)
			}
		})
	}
}

// A line ends only at a line feed, so a lone carriage return is part of a quoted name.
func TestQuotedNameWithCarriageReturn(t *testing.T) {
	t.Parallel()
	for _, src := range []string{"\"a\rb\"", "`a\rb`"} {
		_, x := parseItem(t, src)
		if c, ok := x.(*sql.ColumnRef); !ok || c.Name != "a\rb" {
			t.Errorf("%q: got %s, want the name %q", src, expr(x), "a\rb")
		}
	}
}

func TestNumberLiterals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		src  string
		want sql.NumberLit
	}{
		{"0", sql.NumberLit{Text: "0", Int: 0}},
		{"42", sql.NumberLit{Text: "42", Int: 42}},
		{"1_000_000", sql.NumberLit{Text: "1000000", Int: 1000000}},
		{"08", sql.NumberLit{Text: "08", Int: 8}},
		{"18446744073709551615", sql.NumberLit{Text: "18446744073709551615", Int: math.MaxUint64}},
		{"-1", sql.NumberLit{Neg: true, Text: "1", Int: 1}},
		{"-0", sql.NumberLit{Neg: true, Text: "0", Int: 0}},
		{"-9223372036854775808", sql.NumberLit{Neg: true, Text: "9223372036854775808", Int: 1 << 63}},
		{"1.5", sql.NumberLit{Text: "1.5", IsFloat: true, Float: 1.5}},
		{"2.0e-3", sql.NumberLit{Text: "2.0e-3", IsFloat: true, Float: 0.002}},
		{"1e6", sql.NumberLit{Text: "1e6", IsFloat: true, Float: 1e6}},
		{"1E+2", sql.NumberLit{Text: "1E+2", IsFloat: true, Float: 100}},
		{"-0.0", sql.NumberLit{Neg: true, Text: "0.0", IsFloat: true, Float: 0}},
		{"1_0.2_5e1_0", sql.NumberLit{Text: "10.25e10", IsFloat: true, Float: 10.25e10}},
		{"1e-400", sql.NumberLit{Text: "1e-400", IsFloat: true, Float: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			t.Parallel()
			_, x := parseItem(t, tt.src)
			n, ok := x.(*sql.NumberLit)
			if !ok {
				t.Fatalf("got %s, want a number", expr(x))
			}
			got := *n
			got.Span = sql.Span{}
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// Diagnostics quote expressions without comments and with whitespace runs
// collapsed, but strings and quoted names as written.
func TestText(t *testing.T) {
	t.Parallel()
	tests := []struct{ src, want string }{
		{"count( /* c */ x )  +  1", "count( x ) + 1"},
		{"a\n  -- why\n  AND\tb", "a AND b"},
		{"NOT/**/x", "NOT x"},
		{"/* a /* nested */ b */ x", "x"},
		{"concat('a  b', \"c  d\", '--', 'it''s')", "concat('a  b', \"c  d\", '--', 'it''s')"},
		{"x\r\n= 1", "x = 1"},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			t.Parallel()
			f, x := parseItem(t, tt.src)
			if got := f.Text(x); got != tt.want {
				t.Errorf("Text = %q, want %q", got, tt.want)
			}
		})
	}
}

// Columns count Unicode code points, a tab is one column, a byte-order mark
// takes none, and CRLF ends a line.
func TestPositions(t *testing.T) {
	t.Parallel()
	pos := func(line, col int) diag.Pos { return diag.Pos{File: "q.sql", Line: line, Col: col} }
	item := func(f *sql.File, stmt, i int) sql.Expr { return f.Stmts[stmt].(*sql.Query).Select.Items[i].X }
	tests := []struct {
		name string
		src  string
		node func(*sql.File) diag.Pos
		want diag.Pos
	}{
		{"tab", "SELECT\tx FROM t", func(f *sql.File) diag.Pos { return item(f, 0, 0).Extent().Pos }, pos(1, 8)},
		{"code points", "SELECT 'é', x FROM t", func(f *sql.File) diag.Pos { return item(f, 0, 1).Extent().Pos }, pos(1, 13)},
		{"byte-order mark", "\xEF\xBB\xBFSELECT x FROM t", func(f *sql.File) diag.Pos { return f.Stmts[0].Extent().Pos }, pos(1, 1)},
		{"crlf", "SELECT 1 FROM t;\r\nSELECT y FROM t", func(f *sql.File) diag.Pos { return item(f, 1, 0).Extent().Pos }, pos(2, 8)},
		{"second operator of a chain", "SELECT a = b = c FROM t", func(f *sql.File) diag.Pos {
			return item(f, 0, 0).(*sql.BinaryExpr).OpPos
		}, pos(1, 14)},
		{"NOT IN starts at NOT", "SELECT a NOT IN (1) FROM t", func(f *sql.File) diag.Pos {
			return item(f, 0, 0).(*sql.InExpr).OpPos
		}, pos(1, 10)},
		{"negative literal starts at the minus", "SELECT - 1 FROM t", func(f *sql.File) diag.Pos {
			return item(f, 0, 0).Extent().Pos
		}, pos(1, 8)},
		{"annotation name", "--\tname:  Q\t:one\nSELECT x FROM t", func(f *sql.File) diag.Pos {
			return f.Stmts[0].(*sql.Query).Annotations[0].NamePos
		}, pos(1, 11)},
		{"annotation in a CRLF file", "-- name: Q :one\r\nSELECT x FROM t;\r\n", func(f *sql.File) diag.Pos {
			return f.Stmts[0].(*sql.Query).Annotations[0].NamePos
		}, pos(1, 10)},
		{"qualified name starts at the qualifier", "SELECT x FROM db.t", func(f *sql.File) diag.Pos {
			return f.Stmts[0].(*sql.Query).Select.From.Pos
		}, pos(1, 15)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, ds := sql.Parse("q.sql", []byte(tt.src))
			if len(ds) != 0 {
				t.Fatalf("Parse:\n%s", diagLines(ds))
			}
			if got := tt.node(f); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsReserved(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"AND", "BETWEEN", "CASE", "FALSE", "IN", "INF", "INTERVAL", "IS", "NAN", "NOT", "NULL", "OR", "TRUE",
		"and", "Null", "nan",
	} {
		if !sql.IsReserved(name) {
			t.Errorf("IsReserved(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"day", "order", "like", "select", "from", "android", "nullable", ""} {
		if sql.IsReserved(name) {
			t.Errorf("IsReserved(%q) = true, want false", name)
		}
	}
}

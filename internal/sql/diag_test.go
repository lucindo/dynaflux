// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
)

func short(ds []diag.Diagnostic) []string {
	lines := make([]string, len(ds))
	for i, d := range ds {
		lines[i] = fmt.Sprintf("%d:%d %s %s", d.Pos.Line, d.Pos.Col, d.Code, d.Message)
	}
	return lines
}

// Each lexical and syntax error, with its position and hint. Hints of DF1101
// that depend on the context are in TestAliasHint.
func TestSyntaxDiagnostics(t *testing.T) {
	t.Parallel()
	// The hints the specification gives for the codes whose hint doesn't
	// depend on the context.
	specHints := map[string]string{
		"DF1001": "save the file as UTF-8",
		"DF1003": `close it with ', and write a line break as \n`,
		"DF1004": "add */; block comments nest, so every /* needs its own */",
		"DF1005": "use -- or /* */",
		"DF1006": `the escapes are '' \' \\ \n \r \t \0 \xHH`,
		"DF1007": `check the \xHH escapes`,
		"DF1008": "write decimal digits with digits on both sides of the point, such as 0.5; inf and nan aren't allowed",
		"DF1009": "integers are between -9223372036854775808 and 18446744073709551615",
		"DF1010": `a quoted name can't be empty or contain its quote character, \ or a line break`,
		"DF1103": "use CREATE STREAM, CREATE MATERIALIZED VIEW or a named SELECT; for CREATE TABLE, use CREATE STREAM",
	}
	tests := []struct {
		name string
		src  string
		want string
	}{
		{"DF1001 invalid UTF-8, only diagnostic of the file", "SELECT @ FROM t;\nSELECT 'a\xffb'", "1:1 DF1001 file is not valid UTF-8 (byte 26)"},
		{"DF1002 character", "SELECT a @ b FROM t", "1:10 DF1002 unexpected character @"},
		{"DF1002 lone !", "SELECT a ! b FROM t", "1:10 DF1002 unexpected character !"},
		{"DF1002 non-ASCII letter", "SELECT é FROM t", "1:8 DF1002 unexpected character é"},
		{"DF1002 invisible character", "SELECT\u00a0a FROM t", "1:7 DF1002 unexpected character U+00A0"},
		{"DF1002 dot after a name", "SELECT t.* FROM t", "1:9 DF1002 unexpected character ."},
		{"DF1002 dot", "SELECT . FROM t", "1:8 DF1002 unexpected character ."},
		{"DF1003 end of file", "SELECT 'abc", "1:8 DF1003 string literal is not closed on this line"},
		{"DF1003 line break", "SELECT x FROM t WHERE y = 'a\nb'", "1:27 DF1003 string literal is not closed on this line"},
		{"DF1003 backslash at line end", "SELECT 'a\\\n' FROM t", "1:8 DF1003 string literal is not closed on this line"},
		{"DF1004", "SELECT x FROM t /* a /* b */ c", "1:17 DF1004 block comment is not closed"},
		{"DF1005 #", "SELECT x # c\nFROM t", "1:10 DF1005 # comments are not supported"},
		{"DF1005 #!", "#!/bin/sh\nSELECT x FROM t", "1:1 DF1005 #! comments are not supported"},
		{"DF1005 //", "SELECT x // c\nFROM t", "1:10 DF1005 // comments are not supported"},
		{"DF1006 unknown escape", `SELECT 'ab\q' FROM t`, `1:11 DF1006 invalid escape \q in string literal`},
		{"DF1006 short hex escape", `SELECT '\x4' FROM t`, `1:9 DF1006 invalid escape \x in string literal`},
		{"DF1006 no unicode escape", `SELECT '\u00e9' FROM t`, `1:9 DF1006 invalid escape \u in string literal`},
		{"DF1006 before an unclosed string", `SELECT '\q`, `1:9 DF1006 invalid escape \q in string literal`},
		{"DF1007", `SELECT 'a\xFFb' FROM t`, "1:8 DF1007 string literal is not valid UTF-8"},
		{"DF1008 letters", "SELECT 123abc FROM t", "1:8 DF1008 unsupported number 123abc"},
		{"DF1008 no exponent digits", "SELECT 1e FROM t", "1:8 DF1008 unsupported number 1e"},
		{"DF1008 two points", "SELECT 1.5.2 FROM t", "1:8 DF1008 unsupported number 1.5.2"},
		{"DF1008 hexadecimal", "SELECT 0x1F FROM t", "1:8 DF1008 unsupported number 0x1F"},
		{"DF1008 binary", "SELECT 0b101 FROM t", "1:8 DF1008 unsupported number 0b101"},
		{"DF1008 leading point", "SELECT .5 FROM t", "1:8 DF1008 unsupported number .5"},
		{"DF1008 trailing point", "SELECT 5. FROM t", "1:8 DF1008 unsupported number 5."},
		{"DF1008 trailing underscore", "SELECT 1_ FROM t", "1:8 DF1008 unsupported number 1_"},
		{"DF1008 double underscore", "SELECT 1__0 FROM t", "1:8 DF1008 unsupported number 1__0"},
		{"DF1008 inf", "SELECT -inf FROM t", "1:9 DF1008 unsupported number inf"},
		{"DF1008 nan in any case", "SELECT NaN FROM t", "1:8 DF1008 unsupported number NaN"},
		{"DF1008 float overflow", "SELECT 1e400 FROM t", "1:8 DF1008 unsupported number 1e400"},
		{"DF1009 above UInt64", "SELECT 18446744073709551616 FROM t", "1:8 DF1009 integer 18446744073709551616 is out of range"},
		{"DF1009 below Int64", "SELECT - 9223372036854775809 FROM t", "1:8 DF1009 integer -9223372036854775809 is out of range"},
		{"DF1009 negative UInt64 max", "SELECT x FROM t WHERE y IN (-18446744073709551615)", "1:29 DF1009 integer -18446744073709551615 is out of range"},
		{"DF1009 as written", "CREATE STREAM s (a String) SHARDS 99_999_999_999_999_999_999", "1:35 DF1009 integer 99_999_999_999_999_999_999 is out of range"},
		{"DF1010 empty", `SELECT "" FROM t`, "1:8 DF1010 invalid quoted identifier"},
		{"DF1010 empty backticks", "SELECT `` FROM t", "1:8 DF1010 invalid quoted identifier"},
		{"DF1010 backslash", `SELECT "a\"b" FROM t`, "1:8 DF1010 invalid quoted identifier"},
		{"DF1010 line break", "SELECT `a\nb` FROM t", "1:8 DF1010 invalid quoted identifier"},
		{"DF1101 no FROM", "SELECT 1;", "1:9 DF1101 expected FROM, found ;"},
		{"DF1101 end of file", "SELECT x FROM", "1:14 DF1101 expected a name, found end of file"},
		{"DF1101 column list", "CREATE STREAM s (a String b String)", "1:27 DF1101 expected , or ), found b"},
		{"DF1101 type", "CREATE STREAM s (a 1)", "1:20 DF1101 expected a type, found 1"},
		{"DF1101 float type argument", "CREATE STREAM s (a Decimal(1.5))", "1:28 DF1101 expected a type, an integer or a string, found 1.5"},
		{"DF1101 trailing comma in a type", "CREATE STREAM s (a Decimal(10,)) PRIMARY KEY a", "1:31 DF1101 expected a type, an integer or a string, found )"},
		{"DF1101 trailing comma in an array", "CREATE STREAM s (a Array(UInt8) DEFAULT [1,]) PRIMARY KEY a", "1:44 DF1101 expected a literal, found ]"},
		{"DF1101 float Enum value", "CREATE STREAM s (a Enum8('a' = 1.5)) PRIMARY KEY a", "1:32 DF1101 expected an integer, found 1.5"},
		{"DF1101 stream clause", "CREATE STREAM s (a String) ORDER BY a", "1:28 DF1101 expected PRIMARY KEY, DEDUP BY, LOG RETENTION, ACCEPT, SHARDS, CONSISTENCY or ;, found ORDER"},
		{"DF1101 view clause", "CREATE MATERIALIZED VIEW v PARTITION BY a AS SELECT a FROM s", "1:28 DF1101 expected MODE, ORDER BY, PRIMARY KEY, TTL, SPLIT or AS, found PARTITION"},
		{"DF1101 MATERIALIZED without VIEW", "CREATE MATERIALIZED TABLE t", "1:21 DF1101 expected VIEW, found TABLE"},
		{"DF1101 CREATE alone", "CREATE;", "1:7 DF1101 expected STREAM or MATERIALIZED VIEW, found ;"},
		{"DF1101 not a statement", "42;", "1:1 DF1101 expected CREATE or SELECT, found 42"},
		{"DF1101 mode", "CREATE MATERIALIZED VIEW v MODE FAST AS SELECT a FROM s", "1:33 DF1101 expected ASYNC or SYNC, found FAST"},
		{"DF1101 clause order", "SELECT a FROM t GROUP BY a WHERE b", "1:28 DF1101 expected ORDER BY, LIMIT or ;, found WHERE"},
		{"DF1101 after WHERE", "SELECT a FROM t WHERE b c", "1:25 DF1101 expected GROUP BY, ORDER BY, LIMIT or ;, found c"},
		{"DF1101 operator as a name", "SELECT a = AND FROM t", "1:12 DF1101 expected an expression, found AND"},
		{"DF1101 IN as a function", "SELECT in(a, b) FROM t", "1:8 DF1101 expected an expression, found in"},
		{"DF1101 IN list item", "SELECT a FROM t WHERE b IN (c)", "1:29 DF1101 expected a literal or a parameter, found c"},
		{"DF1101 interval unit", "SELECT a FROM t WHERE b < INTERVAL 5 AND c", "1:38 DF1101 expected a unit, found AND"},
		{"DF1101 interval unit before AS", "SELECT INTERVAL 5 AS d FROM t", "1:19 DF1101 expected a unit, found AS"},
		{"DF1101 TTL interval unit", "CREATE MATERIALIZED VIEW v TTL a + INTERVAL 1 AS SELECT a FROM s", "1:47 DF1101 expected a unit, found AS"},
		{"DF1101 LOG RETENTION unit", "CREATE STREAM s (a String) LOG RETENTION 2 PRIMARY KEY a", "1:44 DF1101 expected a unit, found PRIMARY"},
		{"DF1101 negative FOREVER", "CREATE STREAM s (a String) LOG RETENTION - FOREVER", "1:44 DF1101 expected a number, found FOREVER"},
		{"DF1101 OR without REPLACE", "CREATE OR STREAM s (a String)", "1:11 DF1101 expected REPLACE, found STREAM"},
		{"DF1101 LIMIT", "SELECT a FROM t LIMIT 1.5", "1:23 DF1101 expected an integer or a parameter, found 1.5"},
		{"DF1101 quoted function name", `SELECT "f"(x) FROM t`, "1:11 DF1101 expected FROM, found ("},
		{"DF1101 NULLS", "SELECT a FROM t ORDER BY a NULLS LATER", "1:34 DF1101 expected FIRST or LAST, found LATER"},
		{"DF1103 INSERT", "insert into t values (1)", "1:1 DF1103 INSERT statements are not supported"},
		{"DF1103 CREATE TABLE", "CREATE TABLE t (a String)", "1:1 DF1103 CREATE TABLE statements are not supported"},
		{"DF1103 CREATE VIEW", "create view v as select 1", "1:1 DF1103 CREATE VIEW statements are not supported"},
		{"DF1103 CREATE", "CREATE DATABASE d", "1:1 DF1103 CREATE DATABASE statements are not supported"},
		{"one error per statement", "SELECT @ # FROM", "1:8 DF1002 unexpected character @"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, ds := sql.Parse("q.sql", []byte(tt.src))
			if got := short(ds); !slices.Equal(got, []string{tt.want}) {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			if len(ds) == 1 && ds[0].Hint != specHints[ds[0].Code] {
				t.Errorf("hint %q, want %q", ds[0].Hint, specHints[ds[0].Code])
			}
			for _, s := range f.Stmts {
				if !s.Header().Broken {
					t.Errorf("statement at %v is not Broken", s.Extent().Pos)
				}
			}
		})
	}
}

// SQL that ClickHouse accepts and Dynaflux doesn't is DF1102, with the hint
// the specification gives for the feature.
func TestUnsupported(t *testing.T) {
	t.Parallel()
	const (
		oneSource  = "a statement reads one stream or view; put the data you need into the stream, and split the work into views and queries"
		managed    = "Dynaflux creates and manages the tables"
		pagination = "no pagination in v1; narrow the key range instead"
		merged     = "not needed: view rows are always merged"
		conversion = "no type conversions in v1"
		compare    = "compare with = or IN"
		nullSafe   = "use IS NULL and ="
		notInV1    = "not in v1"
		nameColumn = "name the column"
	)
	tests := []struct {
		src  string
		want string // position and feature
		hint string
	}{
		{"SELECT a FROM t JOIN u", "1:17 JOIN", oneSource},
		{"SELECT a FROM t LEFT JOIN u", "1:17 JOIN", oneSource},
		{"SELECT a FROM t ARRAY JOIN x AS y INNER JOIN u", "1:35 JOIN", oneSource},
		{"SELECT a FROM t, u", "1:16 JOIN", oneSource},
		{"SELECT a FROM (SELECT 1)", "1:15 subquery", oneSource},
		{"SELECT a FROM t WHERE b IN (SELECT c FROM u)", "1:28 subquery", oneSource},
		{"SELECT (SELECT 1) FROM t", "1:8 subquery", oneSource},
		{"(SELECT 1)", "1:1 subquery", oneSource},
		{"WITH x AS (SELECT 1) SELECT x FROM t", "1:1 WITH", oneSource},
		{"CREATE MATERIALIZED VIEW v ORDER BY a AS WITH 1 AS b SELECT a FROM s", "1:42 WITH", oneSource},
		{"SELECT a FROM t UNION ALL SELECT b FROM u", "1:17 UNION", oneSource},
		{"SELECT a FROM t INTERSECT SELECT b FROM u", "1:17 INTERSECT", oneSource},
		{"SELECT a FROM t EXCEPT SELECT b FROM u", "1:17 EXCEPT", oneSource},
		{"SELECT DISTINCT a FROM t", "1:1 SELECT DISTINCT", "use GROUP BY"},
		{"SELECT a FROM t GROUP BY a HAVING a > 1", "1:28 HAVING", "not in v1; filter the rows in the caller"},
		{"SELECT a FROM t PREWHERE a", "1:17 PREWHERE", merged},
		{"SELECT a FROM t FINAL", "1:17 FINAL", merged},
		{"SELECT a FROM t SAMPLE 0.1", "1:17 SAMPLE", merged},
		{"SELECT a FROM t SETTINGS x = 1", "1:17 SETTINGS", merged},
		{"SELECT a FROM t FORMAT JSON", "1:17 FORMAT", merged},
		{"SELECT a FROM t LIMIT 1 OFFSET 2", "1:25 OFFSET", pagination},
		{"SELECT a FROM t OFFSET 2", "1:17 OFFSET", pagination},
		{"SELECT a FROM t LIMIT 1, 2", "1:17 LIMIT m, n", pagination},
		{"SELECT a FROM t LIMIT 1 BY a", "1:17 LIMIT n BY", pagination},
		{"SELECT a FROM t ORDER BY a LIMIT 1 WITH TIES", "1:36 WITH TIES", pagination},
		{"SELECT TOP 1 a FROM t", "1:8 TOP", pagination},
		{"SELECT a FROM t ORDER BY ALL", "1:17 ORDER BY ALL", nameColumn},
		{"SELECT a FROM t ORDER BY 1", "1:26 positional ORDER BY", nameColumn},
		{"SELECT count() OVER (PARTITION BY a) FROM t", "1:16 OVER", "window functions are not in v1"},
		{"SELECT CASE WHEN a THEN 1 END FROM t", "1:8 CASE", "use multiIf(cond1, value1, cond2, value2, default)"},
		{"SELECT cast(a AS String) FROM t", "1:8 CAST", conversion},
		{"SELECT a::String FROM t", "1:9 ::", conversion},
		{"SELECT a LIKE 'x%' FROM t", "1:10 LIKE", compare},
		{"SELECT a ILIKE 'x%' FROM t", "1:10 ILIKE", compare},
		{"SELECT a NOT LIKE 'x%' FROM t", "1:10 NOT LIKE", compare},
		{"SELECT a REGEXP 'x' FROM t", "1:10 REGEXP", compare},
		{"SELECT a || b FROM t", "1:10 ||", "use concat(a, b)"},
		{"SELECT a ? b : c FROM t", "1:10 ?", "use if(cond, a, b)"},
		{"SELECT a DIV b FROM t", "1:10 DIV", "use intDiv(a, b) or a % b"},
		{"SELECT a mod b FROM t", "1:10 MOD", "use intDiv(a, b) or a % b"},
		{"SELECT a IS DISTINCT FROM b FROM t", "1:10 IS DISTINCT FROM", nullSafe},
		{"SELECT a IS NOT DISTINCT FROM b FROM t", "1:10 IS NOT DISTINCT FROM", nullSafe},
		{"SELECT a <=> b FROM t", "1:10 <=>", nullSafe},
		{"SELECT a FROM t WHERE b GLOBAL IN (1)", "1:25 GLOBAL IN", notInV1},
		{"SELECT (a, b) FROM t", "1:8 tuple", notInV1},
		{"SELECT a[1] FROM t", "1:9 [ ] subscript", notInV1},
		{"SELECT [1, 2] FROM t", "1:8 array literal outside DEFAULT", notInV1},
		{"SELECT arrayMap(x -> x, a) FROM t", "1:19 ->", notInV1},
		{"CREATE STREAM IF NOT EXISTS s (a String)", "1:15 IF NOT EXISTS", managed},
		{"CREATE MATERIALIZED VIEW IF NOT EXISTS v AS SELECT a FROM s", "1:26 IF NOT EXISTS", managed},
		{"CREATE OR REPLACE STREAM s (a String)", "1:8 OR REPLACE", managed},
		{"CREATE OR REPLACE VIEW v AS SELECT 1", "1:8 OR REPLACE", managed},
		{"CREATE STREAM s ON CLUSTER c (a String)", "1:17 ON CLUSTER", managed},
		{"CREATE STREAM s (a String) ENGINE = MergeTree", "1:28 ENGINE", managed},
		{"CREATE STREAM s (a String) SETTINGS x = 1", "1:28 SETTINGS", managed},
		{"CREATE MATERIALIZED VIEW v TO t AS SELECT a FROM s", "1:28 TO", managed},
		{"CREATE MATERIALIZED VIEW v ENGINE = MergeTree AS SELECT a FROM s", "1:28 ENGINE", managed},
		{"CREATE MATERIALIZED VIEW v POPULATE AS SELECT a FROM s", "1:28 POPULATE", managed},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			t.Parallel()
			_, ds := sql.Parse("q.sql", []byte(tt.src))
			want := []string{strings.Replace(tt.want, " ", " DF1102 ", 1) + " is not supported"}
			if got := short(ds); !slices.Equal(got, want) {
				t.Errorf("got %q, want %q", got, want)
			}
			if len(ds) == 1 && ds[0].Hint != tt.hint {
				t.Errorf("hint %q, want %q", ds[0].Hint, tt.hint)
			}
		})
	}
}

// Nesting is bounded so that no input exhausts the stack.
func TestNestingLimit(t *testing.T) {
	t.Parallel()
	parens := func(n int) string {
		return "SELECT " + strings.Repeat("(", n) + "1" + strings.Repeat(")", n) + " FROM t"
	}
	const tooDeep = "DF1102 nesting deeper than 1000 levels is not supported"
	tests := []struct {
		name string
		src  string
		want []string
	}{
		{"999 parentheses", parens(999), nil},
		{"1000 parentheses", parens(1000), []string{"1:1008 " + tooDeep}},
		{"NOT", "SELECT " + strings.Repeat("NOT ", 1000) + "x FROM t", []string{"1:4008 " + tooDeep}},
		{"minus", "SELECT " + strings.Repeat("- ", 1000) + "x FROM t", []string{"1:2008 " + tooDeep}},
		{"types", "CREATE STREAM s (a " + strings.Repeat("Array(", 1000) + "String" + strings.Repeat(")", 1000) + ")",
			[]string{"1:6020 " + tooDeep}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, ds := sql.Parse("q.sql", []byte(tt.src))
			if got := short(ds); !slices.Equal(got, tt.want) {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// An alias without AS is a syntax error whose hint shows the fix.
func TestAliasHint(t *testing.T) {
	t.Parallel()
	tests := []struct{ src, want string }{
		{"SELECT count()  views FROM t", "aliases need AS: count() AS views"},
		{"SELECT a + /* c */ 1 n, b FROM t", "aliases need AS: a + 1 AS n"},
		{"SELECT a \"n\" FROM t", `aliases need AS: a AS "n"`},
		{"CREATE MATERIALIZED VIEW v AS SELECT a FROM s ARRAY JOIN tags tag GROUP BY a", "aliases need AS: tags AS tag"},
		{"SELECT a b c FROM t", ""},
		{"SELECT a, FROM t", "remove the , before FROM"},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			t.Parallel()
			_, ds := sql.Parse("q.sql", []byte(tt.src))
			if len(ds) != 1 || ds[0].Code != "DF1101" || ds[0].Hint != tt.want {
				t.Errorf("got %q, want DF1101 with hint %q", diagLines(ds), tt.want)
			}
		})
	}
}

// Annotation and duplicate-clause errors don't end the statement.
func TestNonFatalDiagnostics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		src  string
		want []string
	}{
		{"DF1105 stream clause", "CREATE STREAM s (a String) SHARDS 2 SHARDS 3 PRIMARY KEY a",
			[]string{"1:37 DF1105 SHARDS appears twice"}},
		{"DF1105 view clause", "CREATE MATERIALIZED VIEW v TTL a TTL b ORDER BY a AS SELECT a FROM s",
			[]string{"1:34 DF1105 TTL appears twice"}},
		{"DF1105 column option", "CREATE STREAM s (a String CODEC(LZ4) CODEC(ZSTD))",
			[]string{"1:38 DF1105 CODEC appears twice"}},
		{"DF1201 and DF1202 before one statement", "-- name: Q\n-- expose: grpc\nSELECT a FROM t",
			[]string{"1:1 DF1201 malformed annotation: -- name: Q", "2:1 DF1202 unknown annotation -- expose: grpc"}},
		{"DF1203 inside a statement", "SELECT a\n  -- name: Q :one\nFROM t",
			[]string{"2:3 DF1203 annotation must come right before a statement"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, ds := sql.Parse("q.sql", []byte(tt.src))
			if got := short(ds); !slices.Equal(got, tt.want) {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			if len(f.Stmts) != 1 || f.Stmts[0].Header().Broken {
				t.Errorf("want one statement that isn't Broken, got:\n%s", dump(f))
			}
		})
	}
}

// Diagnostics carry the statement's name once the parser has seen it: the
// stream or view name, or the query's -- name:, even a bad one.
func TestDiagnosticStatement(t *testing.T) {
	t.Parallel()
	tests := []struct{ src, want string }{
		{"CREATE STREAM s (a @)", "s"},
		{"CREATE STREAM @", ""},
		{"CREATE STREAM IF NOT EXISTS s (a String)", "s"},
		{"CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT a FROM s", "v"},
		{"CREATE MATERIALIZED VIEW v AS SELECT @", "v"},
		{"-- name: Q :one\nSELECT @", "Q"},
		{"-- name: Q :ONE\nSELECT @", "Q"},
		{"SELECT @", ""},
		{"CREATE STREAM s (a String) PRIMARY KEY a;\nSELECT @", ""},
	}
	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			t.Parallel()
			_, ds := sql.Parse("q.sql", []byte(tt.src))
			if len(ds) == 0 || ds[len(ds)-1].Statement != tt.want {
				t.Errorf("got %q, want the statement's error for statement %q", diagLines(ds), tt.want)
			}
		})
	}
}

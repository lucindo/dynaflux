// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
	"github.com/lucindo/dynaflux/internal/txtar"
)

// FuzzParse checks that Parse never panics or hangs, that every position it
// reports is in the input, and that every node of a statement without errors
// has a span whose position matches its offset.
func FuzzParse(f *testing.F) {
	for _, src := range fuzzSeeds(f) {
		f.Add(src)
	}
	f.Fuzz(func(t *testing.T, src string) {
		file, ds := sql.Parse("f.sql", []byte(src))
		cursors := cursorPositions(src)
		for _, d := range ds {
			if d.Code == "" || d.Severity != diag.SeverityError || d.Message == "" {
				t.Errorf("incomplete diagnostic %+v", d)
			}
			if _, ok := cursors[[2]int{d.Pos.Line, d.Pos.Col}]; !ok || d.Pos.File != "f.sql" {
				t.Errorf("diagnostic outside the input: %v", d)
			}
		}
		for _, s := range file.Stmts {
			if s.Header().Broken {
				continue
			}
			checkSpans(t, file, src, cursors, reflect.ValueOf(s))
		}
	})
}

// fuzzSeeds returns the specification's examples, the golden inputs and some
// unfinished statements.
func fuzzSeeds(f *testing.F) []string {
	f.Helper()
	seeds := specExamples(f)
	paths, err := filepath.Glob("testdata/*.txtar")
	if err != nil {
		f.Fatal(err)
	}
	for _, path := range paths {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			f.Fatal(readErr)
		}
		in, _ := section(txtar.Parse(data), "in.sql")
		seeds = append(seeds, in)
	}
	return append(seeds,
		"SELECT 'a\\x", "SELECT \"a", "/* /* */", "SELECT -", "-- name: Q :one", "\xEF\xBB\xBFSELECT\r\n",
		"SELECT x IN ({p:Array(String)", "CREATE STREAM s (a Decimal(1, 'x', Array(", "SELECT INTERVAL '1' DAY TO",
	)
}

// cursorPositions maps each line and column of src to its byte offset,
// including the end of the input.
func cursorPositions(src string) map[[2]int]int {
	m := map[[2]int]int{}
	line, col, off := 1, 1, 0
	if strings.HasPrefix(src, "\xEF\xBB\xBF") {
		off = len("\xEF\xBB\xBF")
	}
	for off < len(src) {
		m[[2]int{line, col}] = off
		r, size := utf8.DecodeRuneInString(src[off:])
		if r == '\n' {
			line, col = line+1, 1
		} else {
			col++
		}
		off += size
	}
	m[[2]int{line, col}] = off
	return m
}

// checkSpans walks the nodes under v. A zero struct is an absent optional
// node, such as the unit of INTERVAL '2 days'.
func checkSpans(t *testing.T, f *sql.File, src string, cursors map[[2]int]int, v reflect.Value) {
	t.Helper()
	switch kind := v.Kind(); {
	case kind == reflect.Pointer || kind == reflect.Interface:
		if v.IsNil() {
			return
		}
		if x, ok := reflect.TypeAssert[sql.Expr](v); ok && f.Text(x) == "" {
			t.Errorf("empty Text for %T at %v", x, x.Extent().Pos)
		}
		checkSpans(t, f, src, cursors, v.Elem())
	case kind == reflect.Slice:
		for i := range v.Len() {
			checkSpans(t, f, src, cursors, v.Index(i))
		}
	case kind == reflect.Struct && !v.IsZero():
		if s, ok := reflect.TypeAssert[sql.Span](v); ok {
			off, found := cursors[[2]int{s.Pos.Line, s.Pos.Col}]
			if !found || off != s.Start || s.Start >= s.End || s.End > len(src) {
				t.Errorf("bad span %+v (position at offset %d, found %v)", s, off, found)
			}
			return
		}
		for _, field := range v.Fields() {
			checkSpans(t, f, src, cursors, field)
		}
	}
}

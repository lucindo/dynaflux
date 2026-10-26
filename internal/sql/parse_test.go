// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
	"github.com/lucindo/dynaflux/internal/txtar"
)

// Each testdata/*.txtar holds a file in.sql, its dump in ast, and the
// diagnostics Parse reports for it. DYNAFLUX_UPDATE_GOLDEN=1 rewrites ast and
// diagnostics from the parser's output; review the diff.
func TestParseGolden(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/*.txtar")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no golden files: %v", err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()
			checkGolden(t, path)
		})
	}
}

func checkGolden(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	a := txtar.Parse(data)
	in, ok := section(a, "in.sql")
	if !ok {
		t.Fatal("no in.sql section")
	}
	f, ds := sql.Parse("in.sql", []byte(in))
	gotAST, gotDiags := dump(f), diagLines(ds)
	if os.Getenv("DYNAFLUX_UPDATE_GOLDEN") != "" {
		setSection(a, "ast", gotAST)
		setSection(a, "diagnostics", gotDiags)
		if writeErr := os.WriteFile(path, txtar.Format(a), 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
		return
	}
	if want, _ := section(a, "ast"); gotAST != want {
		t.Errorf("ast:\n%s\nwant:\n%s", gotAST, want)
	}
	if want, _ := section(a, "diagnostics"); gotDiags != want {
		t.Errorf("diagnostics:\n%s\nwant:\n%s", gotDiags, want)
	}
}

func section(a *txtar.Archive, name string) (string, bool) {
	for _, f := range a.Files {
		if f.Name == name {
			return string(f.Data), true
		}
	}
	return "", false
}

func setSection(a *txtar.Archive, name, content string) {
	for i := range a.Files {
		if a.Files[i].Name == name {
			a.Files[i].Data = []byte(content)
			return
		}
	}
	a.Files = append(a.Files, txtar.File{Name: name, Data: []byte(content)})
}

func diagLines(ds []diag.Diagnostic) string {
	var b strings.Builder
	for _, d := range ds {
		b.WriteString(d.String())
		b.WriteByte('\n')
	}
	return b.String()
}

// specExamples returns the sql code blocks of the dialect specification.
func specExamples(tb testing.TB) []string {
	tb.Helper()
	data, err := os.ReadFile("../../docs/spec/sql.md")
	if err != nil {
		tb.Fatal(err)
	}
	blocks := regexp.MustCompile("(?s)```sql\n(.*?)```").FindAllStringSubmatch(string(data), -1)
	examples := make([]string, len(blocks))
	for i, b := range blocks {
		examples[i] = b[1]
	}
	return examples
}

// Every SQL example in the specification is valid syntax; the mistakes in its
// diagnostics example are for the compiler to find.
func TestSpecExamplesParse(t *testing.T) {
	t.Parallel()
	examples := specExamples(t)
	if len(examples) == 0 {
		t.Fatal("found no sql blocks in the spec")
	}
	for i, src := range examples {
		f, ds := sql.Parse("spec.sql", []byte(src))
		if len(ds) != 0 {
			t.Errorf("example %d: %s\n%s", i, diagLines(ds), src)
		}
		for _, s := range f.Stmts {
			if s.Header().Broken {
				t.Errorf("example %d: broken statement at %v", i, s.Extent().Pos)
			}
		}
	}
}

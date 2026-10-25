// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package diag_test

import (
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/diag"
)

func TestDiagnosticString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		d    diag.Diagnostic
		want string
	}{
		{
			name: "statement and hint",
			d: diag.Diagnostic{
				Pos:       diag.Pos{File: "queries/page.sql", Line: 3, Col: 7},
				Severity:  diag.SeverityError,
				Code:      "DF4101",
				Statement: "PageStats",
				Message:   "WHERE does not bind \"page\"",
				Hint:      "add  page = {page:String}",
			},
			want: "queries/page.sql:3:7: error DF4101: PageStats: WHERE does not bind \"page\"\n" +
				"  hint: add  page = {page:String}",
		},
		{
			name: "no statement, no hint",
			d: diag.Diagnostic{
				Pos:      diag.Pos{File: "a.sql", Line: 1, Col: 1},
				Severity: diag.SeverityWarning,
				Code:     "DF2501",
				Message:  "m",
			},
			want: "a.sql:1:1: warning DF2501: m",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.d.String(); got != tt.want {
				t.Errorf("String() =\n%s\nwant\n%s", got, tt.want)
			}
		})
	}
}

func TestSortAndWrite(t *testing.T) {
	t.Parallel()
	at := func(file string, line, col int, code string) diag.Diagnostic {
		return diag.Diagnostic{
			Pos: diag.Pos{File: file, Line: line, Col: col}, Severity: diag.SeverityError, Code: code, Message: "m",
		}
	}
	ds := []diag.Diagnostic{
		at("b.sql", 1, 1, "DF1001"),
		at("a.sql", 10, 1, "DF1001"),
		at("a.sql", 2, 5, "DF2002"),
		at("a.sql", 2, 5, "DF2001"),
		at("a.sql", 2, 1, "DF3001"),
	}
	diag.Sort(ds)
	var b strings.Builder
	if err := diag.Write(&b, ds); err != nil {
		t.Fatal(err)
	}
	want := "a.sql:2:1: error DF3001: m\n" +
		"a.sql:2:5: error DF2001: m\n" +
		"a.sql:2:5: error DF2002: m\n" +
		"a.sql:10:1: error DF1001: m\n" +
		"b.sql:1:1: error DF1001: m\n"
	if b.String() != want {
		t.Errorf("sorted output =\n%s\nwant\n%s", b.String(), want)
	}
}

func TestHasErrors(t *testing.T) {
	t.Parallel()
	warn := diag.Diagnostic{Severity: diag.SeverityWarning}
	errd := diag.Diagnostic{Severity: diag.SeverityError}
	if diag.HasErrors(nil) || diag.HasErrors([]diag.Diagnostic{warn}) {
		t.Error("HasErrors reported an error for warnings only")
	}
	if !diag.HasErrors([]diag.Diagnostic{warn, errd}) {
		t.Error("HasErrors missed an error")
	}
}

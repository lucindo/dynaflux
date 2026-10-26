// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain_test

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/explain"
	"github.com/lucindo/dynaflux/internal/txtar"
)

var update = flag.Bool("update", false, "write the explain sections of testdata/reports from the actual output") //nolint:gochecknoglobals // go test flags are package variables

// TestExplainGolden runs each archive in testdata/reports as a project. Each of its
// sections named "explain <name>" holds the report on <name> with the default workload,
// and go test -update rewrites them. The parity tests check the model's numbers; these
// check the text, whose figures for costs.py's scenarios were checked against costs.py.
func TestExplainGolden(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/reports/*.txtar")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no archives in testdata/reports: %v", err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			cfg, cat := load(t, data)
			ar := txtar.Parse(data)
			changed := false
			for i := range ar.Files {
				changed = checkReport(t, cfg, cat, &ar.Files[i]) || changed
			}
			if changed {
				if writeErr := os.WriteFile(path, txtar.Format(ar), 0o644); writeErr != nil {
					t.Fatal(writeErr)
				}
			}
		})
	}
}

// checkReport compares the report that a section names with the section, and with -update
// replaces the section's text. It returns whether it did.
func checkReport(t *testing.T, cfg *config.Config, cat *catalog.Catalog, f *txtar.File) bool {
	t.Helper()
	name, ok := strings.CutPrefix(f.Name, "explain ")
	if !ok {
		return false
	}
	r, err := explain.Explain(cfg, cat, explain.DefaultWorkload(), name)
	if err != nil {
		t.Errorf("explain %s: %v", name, err)
		return false
	}
	if hasError := strings.Contains("\n"+r.Text, "\nerror "); r.Failed != hasError {
		t.Errorf("explain %s: Failed is %v for a report with an error line: %v", name, r.Failed, hasError)
	}
	if r.Text == string(f.Data) {
		return false
	}
	if *update {
		f.Data = []byte(r.Text)
		return true
	}
	t.Errorf("explain %s:\n%s\nwant:\n%s", name, r.Text, f.Data)
	return false
}

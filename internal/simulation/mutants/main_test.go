// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/lucindo/dynaflux/internal/simulation"
)

func TestMutateNeedsExactlyOneOccurrence(t *testing.T) {
	t.Parallel()
	got, err := mutate([]byte("if a > b {"), "a > b", "a >= b")
	if err != nil || string(got) != "if a >= b {" {
		t.Errorf("mutate = %q, %v; want %q", got, err, "if a >= b {")
	}
	for _, src := range []string{"if a < b {", "a > b || a > b"} {
		if _, stale := mutate([]byte(src), "a > b", "a >= b"); stale == nil {
			t.Errorf("mutate(%q) succeeded; want an error", src)
		}
	}
}

func TestParseCaught(t *testing.T) {
	t.Parallel()
	out := []byte("=== RUN   TestControl\n    sim_test.go:209: control C21: caught in 7/20 seeds\n--- PASS: TestControl\n")
	if caught, seeds, ok := parseCaught(out, "C21"); !ok || caught != 7 || seeds != 20 {
		t.Errorf("parseCaught = %d, %d, %v; want 7, 20, true", caught, seeds, ok)
	}
	if _, _, ok := parseCaught(out, "C2"); ok {
		t.Error("parseCaught found C2 in C21's line")
	}
}

// TestTableMatchesSources fails on a stale row: a mutant whose text no longer occurs once
// in its file, or an unknown scenario.
func TestTableMatchesSources(t *testing.T) {
	t.Parallel()
	var names, ids []string
	for _, sc := range simulation.Scenarios() {
		names = append(names, sc.Name)
	}
	for _, c := range table() {
		if slices.Contains(ids, c.ID) || c.Test == "" && (!slices.Contains(names, c.Scenario) || c.Seeds < 1) {
			t.Errorf("%s: a repeated ID, an unknown scenario %q or no seeds", c.ID, c.Scenario)
		}
		ids = append(ids, c.ID)
		if c.File == "" {
			continue
		}
		src, err := os.ReadFile(filepath.Join("..", "..", "..", c.File))
		if err != nil {
			t.Fatal(err)
		}
		if _, stale := mutate(src, c.Old, c.New); stale != nil {
			t.Errorf("%s: %s: %v", c.ID, c.File, stale)
		}
	}
}

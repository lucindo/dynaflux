// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Command mutants runs the simulation harness's negative controls, several at once. Each
// must make TestControl catch a property violation. A code mutant is applied at build time
// with go test -overlay, so no source file changes.
//
//	go run ./internal/simulation/mutants [-run regexp] [-parallel n]
package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

const (
	weak           = 0.1 // the share of seeds under which a caught control is still flagged
	cpusPerControl = 4   // each control's test runs its seeds in parallel too
)

func main() {
	pattern := flag.String("run", "", "run only the controls whose ID matches this regular expression")
	parallel := flag.Int("parallel", max(1, runtime.NumCPU()/cpusPerControl), "controls to run at once")
	flag.Parse()
	if err := run(context.Background(), *pattern, *parallel); err != nil {
		fmt.Fprintln(os.Stderr, "mutants:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, pattern string, parallel int) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("-run: %w", err)
	}
	root, err := repoRoot(ctx)
	if err != nil {
		return err
	}
	var controls []control
	for _, c := range table() {
		if re.MatchString(c.ID) {
			controls = append(controls, c)
		}
	}
	lines := make([]string, len(controls))
	passed := make([]bool, len(controls))
	sem := make(chan struct{}, max(1, parallel))
	var wg sync.WaitGroup
	for i, c := range controls {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			lines[i], passed[i] = runControl(ctx, root, c)
		})
	}
	wg.Wait()
	failed := 0
	for i, line := range lines {
		fmt.Fprintln(os.Stdout, line)
		if !passed[i] {
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d controls failed", failed, len(controls))
	}
	return nil
}

func repoRoot(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "go", "env", "GOMOD").Output()
	if err != nil {
		return "", fmt.Errorf("go env GOMOD: %w", err)
	}
	return filepath.Dir(strings.TrimSpace(string(out))), nil
}

// runControl runs c's test and returns its report line and whether c was caught.
func runControl(ctx context.Context, root string, c control) (string, bool) {
	test := cmp.Or(c.Test, "TestControl")
	args := []string{"test", "-count=1", "-v", "-run", "^" + test + "$"}
	if c.File != "" {
		dir, err := os.MkdirTemp("", "mutant-"+c.ID+"-")
		if err != nil {
			return c.ID + "\terror: " + err.Error(), false
		}
		defer removeAll(dir)
		overlay, err := writeOverlay(root, dir, c)
		if err != nil {
			return c.ID + "\ttable error: " + err.Error(), false
		}
		args = append(args, "-overlay="+overlay)
	}
	args = append(args, cmp.Or(c.Package, "./internal/simulation"))
	if c.Test == "" {
		args = append(args, "-sim.control="+c.ID, "-sim.scenario="+c.Scenario, "-sim.seeds="+strconv.Itoa(c.Seeds), "-sim.want="+c.Want)
	}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if c.Test != "" {
		return testVerdict(c, out, err)
	}
	caught, seeds, found := parseCaught(out, c.ID)
	switch {
	case !found:
		return fmt.Sprintf("%s\terror: TestControl reported nothing\n%s", c.ID, out), false
	case err != nil || caught == 0:
		return fmt.Sprintf("%s\tNOT CAUGHT by %s in %d seeds", c.ID, c.Want, seeds), false
	case float64(caught) < weak*float64(seeds):
		return fmt.Sprintf("%s\tcaught by %s in %d/%d seeds: weak", c.ID, c.Want, caught, seeds), true
	}
	return fmt.Sprintf("%s\tcaught by %s in %d/%d seeds", c.ID, c.Want, caught, seeds), true
}

// testVerdict judges a control whose test must fail: a failure of anything else, such as
// the build, is an error.
func testVerdict(c control, out []byte, err error) (string, bool) {
	switch {
	case err == nil:
		return c.ID + "\tNOT CAUGHT: " + c.Test + " passed", false
	case !bytes.Contains(out, []byte("--- FAIL: "+c.Test)):
		return fmt.Sprintf("%s\terror: %v\n%s", c.ID, err, out), false
	}
	return c.ID + "\tcaught: " + c.Test + " failed", true
}

func removeAll(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		fmt.Fprintln(os.Stderr, "mutants:", err)
	}
}

// writeOverlay writes c's mutated file into dir, and the overlay that puts it in place
// of the original; it returns the overlay's path.
func writeOverlay(root, dir string, c control) (string, error) {
	file := filepath.Join(root, c.File)
	src, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	mutated, err := c.mutate(src)
	if err != nil {
		return "", fmt.Errorf("%s: %w", c.File, err)
	}
	backing := filepath.Join(dir, filepath.Base(file))
	if err = os.WriteFile(backing, mutated, 0o600); err != nil { //nolint:gosec // backing is in a temporary directory of ours
		return "", err
	}
	js, err := json.Marshal(struct {
		Replace map[string]string `json:"Replace"`
	}{map[string]string{file: backing}})
	if err != nil {
		return "", err
	}
	overlay := filepath.Join(dir, "overlay.json")
	return overlay, os.WriteFile(overlay, js, 0o600)
}

// mutate applies c's replacements to src, its File.
func (c control) mutate(src []byte) ([]byte, error) {
	out, err := mutate(src, c.Old, c.New)
	for _, r := range c.More {
		if err != nil {
			break
		}
		out, err = mutate(out, r[0], r[1])
	}
	return out, err
}

// mutate replaces from, which must occur exactly once in src, with to: a mutant whose
// text matches nowhere, or twice, is a stale table row.
func mutate(src []byte, from, to string) ([]byte, error) {
	if n := bytes.Count(src, []byte(from)); n != 1 {
		return nil, fmt.Errorf("%q occurs %d times, want once", from, n)
	}
	return bytes.Replace(src, []byte(from), []byte(to), 1), nil
}

// parseCaught finds TestControl's result line for id in go test's output.
func parseCaught(out []byte, id string) (int, int, bool) {
	prefix := "control " + id + ": caught in "
	for line := range strings.Lines(string(out)) {
		_, rest, ok := strings.Cut(line, prefix)
		if !ok {
			continue
		}
		var caught, seeds int
		if _, err := fmt.Sscanf(rest, "%d/%d seeds", &caught, &seeds); err != nil {
			return 0, 0, false
		}
		return caught, seeds, true
	}
	return 0, 0, false
}

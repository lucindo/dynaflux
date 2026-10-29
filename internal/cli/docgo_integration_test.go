// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cli_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var testFunc = regexp.MustCompile(`(?m)^func Test`)

// TestDocGo compiles each Go block that the README and the guides mark go, as a package of
// its own in the example, so that it can import examples/analytics/internal/flux. A build
// overlay adds the packages, so nothing is written into the tree. go vet can't run on them:
// it changes to the package's directory, which doesn't exist.
func TestDocGo(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	o := docOverlay{root: root, tmp: t.TempDir(), replace: map[string]string{}}
	for _, path := range testedDocs(t) {
		blocks, blocksErr := docBlocks(readMarkdown(t, path))
		if blocksErr != nil {
			t.Fatal(blocksErr)
		}
		for _, b := range blocks {
			if b.goBlock {
				o.add(t, path, b)
			}
		}
	}
	if len(o.where) == 0 {
		t.Fatal("no document marks a Go block go")
	}
	ov := filepath.Join(o.tmp, "overlay.json")
	data, err := json.Marshal(map[string]any{"Replace": o.replace})
	if err == nil {
		err = os.WriteFile(ov, data, 0o644)
	}
	if err != nil {
		t.Fatal(err)
	}
	builds := [][]string{append([]string{"build", "-overlay=" + ov, "-o", os.DevNull}, o.pkgs...)}
	for i, pkg := range o.tests {
		builds = append(builds, []string{"test", "-c", "-vet=off", "-overlay=" + ov, "-o", filepath.Join(o.tmp, strconv.Itoa(i)+".test"), pkg})
	}
	for _, args := range builds {
		cmd := exec.CommandContext(t.Context(), "go", args...)
		cmd.Dir = root
		if out, buildErr := cmd.CombinedOutput(); buildErr != nil {
			t.Errorf("go %s: %v\n%s\n%s", args[0], buildErr, out, strings.Join(o.where, "\n"))
		}
	}
}

// docOverlay is a build overlay that adds a package of the example for each Go block.
type docOverlay struct {
	root, tmp   string
	replace     map[string]string // the overlay's files, by their path in the tree
	pkgs, tests []string          // the packages, without and with a test
	where       []string          // the block of each package
}

// add writes block b of the document at path to a file of tmp, and adds it to the overlay.
func (o *docOverlay) add(t *testing.T, path string, b docBlock) {
	t.Helper()
	n := len(o.where)
	src := strings.Join(b.lines, "\n") + "\n"
	name := "block.go"
	if testFunc.MatchString(src) {
		name = "block_test.go"
	}
	pkg := fmt.Sprintf("examples/analytics/internal/docblocks/block%d", n)
	file := filepath.Join(o.tmp, fmt.Sprintf("block%d_%s", n, name))
	if err := os.WriteFile(file, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	o.replace[filepath.Join(o.root, filepath.FromSlash(pkg), name)] = file
	if name == "block.go" {
		o.pkgs = append(o.pkgs, "./"+pkg)
	} else {
		o.tests = append(o.tests, "./"+pkg)
	}
	o.where = append(o.where, fmt.Sprintf("%s is %s:%d", pkg, path, b.line+1))
}

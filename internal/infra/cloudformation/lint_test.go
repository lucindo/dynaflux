// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/txtar"
)

// cfnLintInfo is the exit status bit with which cfn-lint reports informational findings
// only; 2 is errors and 4 is warnings.
const cfnLintInfo = 8

// TestCfnLint lints the YAML templates of every golden case (TestGolden in package infra
// keeps them equal to what Render returns) with cfn-lint in every region it knows, so a
// resource type or property that some region lacks fails here rather than in a deploy.
// It is skipped when cfn-lint isn't on PATH; make infra-lint requires it.
func TestCfnLint(t *testing.T) {
	t.Parallel()
	bin, err := exec.LookPath("cfn-lint")
	if err != nil {
		t.Skip("cfn-lint isn't on PATH")
	}
	paths, err := filepath.Glob("../testdata/cases/*.txtar")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no cases in ../testdata/cases: %v", err)
	}
	dir := t.TempDir()
	args := []string{"--regions", "ALL_REGIONS", "--"}
	for _, path := range paths {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		caseDir := filepath.Join(dir, strings.TrimSuffix(filepath.Base(path), ".txtar"))
		if mkErr := os.Mkdir(caseDir, 0o755); mkErr != nil {
			t.Fatal(mkErr)
		}
		for _, f := range txtar.Parse(data).Files {
			name, ok := strings.CutPrefix(f.Name, "want/")
			if !ok || filepath.Ext(name) != ".yaml" {
				continue
			}
			file := filepath.Join(caseDir, name)
			if writeErr := os.WriteFile(file, f.Data, 0o644); writeErr != nil {
				t.Fatal(writeErr)
			}
			args = append(args, file)
		}
	}
	out, err := exec.CommandContext(t.Context(), bin, args...).CombinedOutput()
	var exit *exec.ExitError
	if err != nil && (!errors.As(err, &exit) || exit.ExitCode()&^cfnLintInfo != 0) {
		t.Fatalf("cfn-lint: %v\n%s", err, out)
	}
}

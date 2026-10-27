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
)

// cfnLintInfo is the exit status bit with which cfn-lint reports informational findings
// only; 2 is errors and 4 is warnings.
const cfnLintInfo = 8

// TestCfnLint lints the YAML templates of every golden case (TestGolden in package infra
// keeps them equal to what Render returns) and of examples/analytics with cfn-lint in every
// region it knows, so a resource type or property that some region lacks fails here rather
// than in a deploy. It is skipped when cfn-lint isn't on PATH; make infra-lint requires it.
func TestCfnLint(t *testing.T) {
	t.Parallel()
	bin, err := exec.LookPath("cfn-lint")
	if err != nil {
		t.Skip("cfn-lint isn't on PATH")
	}
	// TestExampleIsGenerated in package cli keeps these equal to what generate writes.
	example, err := filepath.Glob("../../../examples/analytics/infra/dynaflux/*.yaml")
	if err != nil || len(example) != 2 {
		t.Fatalf("want the example's global.yaml and regional.yaml, found %v: %v", example, err)
	}
	args := append([]string{"--regions", "ALL_REGIONS", "--"}, example...)
	dir := t.TempDir()
	for _, file := range []string{"global.yaml", "regional.yaml"} {
		for name, data := range goldenTemplates(t, file) {
			path := filepath.Join(dir, strings.TrimSuffix(name, ".txtar")+"-"+file)
			if writeErr := os.WriteFile(path, data, 0o644); writeErr != nil {
				t.Fatal(writeErr)
			}
			args = append(args, path)
		}
	}
	out, err := exec.CommandContext(t.Context(), bin, args...).CombinedOutput()
	var exit *exec.ExitError
	if err != nil && (!errors.As(err, &exit) || exit.ExitCode()&^cfnLintInfo != 0) {
		t.Fatalf("cfn-lint: %v\n%s", err, out)
	}
}

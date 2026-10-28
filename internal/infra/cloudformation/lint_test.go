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

	"github.com/lucindo/dynaflux/internal/infra/cloudformation"
)

// cfnLintInfo is the exit status bit with which cfn-lint reports informational findings
// only; 2 is errors and 4 is warnings.
const cfnLintInfo = 8

// queryLintRegions are the regions in which query.yaml is linted: function URLs aren't in
// every region that the other templates are linted in.
func queryLintRegions() []string {
	return []string{"us-east-1", "us-west-2", "ap-northeast-1", "eu-west-1"}
}

// TestCfnLint lints the YAML templates of every golden case (TestGolden in package infra
// keeps them equal to what Render returns) and of examples/analytics with cfn-lint: global.yaml
// and regional.yaml in every region it knows, so a resource type or property that some region
// lacks fails here rather than in a deploy, and query.yaml in queryLintRegions. It is skipped
// when cfn-lint isn't on PATH; make infra-lint requires it.
func TestCfnLint(t *testing.T) {
	t.Parallel()
	bin, err := exec.LookPath("cfn-lint")
	if err != nil {
		t.Skip("cfn-lint isn't on PATH")
	}
	// TestExampleIsGenerated in package cli keeps these equal to what generate writes.
	const example = "../../../examples/analytics/infra/dynaflux/"
	dir := t.TempDir()
	lint(t, bin, []string{"ALL_REGIONS"}, append([]string{example + cloudformation.GlobalTemplate, example + cloudformation.RegionalTemplate},
		goldenFiles(t, dir, cloudformation.GlobalTemplate, cloudformation.RegionalTemplate)...))
	lint(t, bin, queryLintRegions(), append([]string{example + cloudformation.QueryTemplate}, goldenFiles(t, dir, cloudformation.QueryTemplate)...))
}

// goldenFiles writes the golden templates named files into dir, and returns their paths.
func goldenFiles(t *testing.T, dir string, files ...string) []string {
	t.Helper()
	var paths []string
	for _, file := range files {
		for name, data := range goldenTemplates(t, file) {
			path := filepath.Join(dir, strings.TrimSuffix(name, ".txtar")+"-"+file)
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, path)
		}
	}
	return paths
}

// lint runs cfn-lint on paths in regions, and fails on any finding but an informational one.
func lint(t *testing.T, bin string, regions, paths []string) {
	t.Helper()
	if len(paths) == 0 {
		t.Fatalf("no templates to lint in %v", regions)
	}
	args := append(append([]string{"--regions"}, regions...), "--")
	out, err := exec.CommandContext(t.Context(), bin, append(args, paths...)...).CombinedOutput()
	var exit *exec.ExitError
	if err != nil && (!errors.As(err, &exit) || exit.ExitCode()&^cfnLintInfo != 0) {
		t.Fatalf("cfn-lint: %v\n%s", err, out)
	}
}

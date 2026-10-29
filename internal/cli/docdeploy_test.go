// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The deploy steps, by command and template.
const (
	stepBuild    = "sam build infra/dynaflux/regional.yaml"
	stepGlobal   = "sam deploy infra/dynaflux/global.yaml"
	stepPolicy   = "aws cloudformation set-stack-policy"
	stepRegional = "sam deploy, in each region"
	stepBackfill = "dynaflux backfill -all"
)

// TestDocDeploySequence checks that getting-started.md deploys in the order that
// scripts/e2e/deploy.sh does, which the AWS end-to-end suite runs before its scenarios. Only
// the guide sets the stack policy, right after global.yaml: deploy.sh leaves it out so that
// teardown.sh can delete the stack.
func TestDocDeploySequence(t *testing.T) {
	t.Parallel()
	want := []string{stepBuild, stepGlobal, stepRegional, stepBackfill}
	data, err := os.ReadFile(filepath.Join(repoRoot, "scripts", "e2e", "deploy.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if got := deploySteps(fence{lang: "sh", lines: strings.Split(string(data), "\n")}); !slices.Equal(got, want) {
		t.Errorf("deploy.sh deploys with\n%q\nwant\n%q", got, want)
	}
	guide := deploySteps(readMarkdown(t, "docs/guide/getting-started.md").fences()...)
	if i := slices.Index(guide, stepPolicy); i < 1 || guide[i-1] != stepGlobal {
		t.Errorf("getting-started.md deploys with\n%q\nwant %q right after %q", guide, stepPolicy, stepGlobal)
	} else if got := slices.Delete(guide, i, i+1); !slices.Equal(got, want) {
		t.Errorf("getting-started.md deploys with\n%q, besides the stack policy\nwant, as deploy.sh does,\n%q", got, want)
	}
}

// deploySteps returns the deploy steps of the sh blocks, in order.
func deploySteps(blocks ...fence) []string {
	var steps []string
	for _, f := range blocks {
		if f.lang != "sh" {
			continue
		}
		for _, c := range f.commands() {
			for _, sc := range shellCommands(c.text) {
				if step, ok := deployStep(sc.words); ok {
					steps = append(steps, step)
				}
			}
		}
	}
	return steps
}

// deployStep returns the step that a command takes, if it is one.
func deployStep(words []string) (string, bool) {
	flag := func(name string) string {
		i := slices.Index(words, name)
		if i < 0 || i+1 == len(words) {
			return ""
		}
		return words[i+1]
	}
	switch {
	case len(words) > 1 && words[0] == "sam" && words[1] == "build":
		return "sam build " + flag("-t"), true
	case len(words) > 1 && words[0] == "sam" && words[1] == "deploy" && flag("-t") != "":
		return "sam deploy " + flag("-t"), true
	case len(words) > 1 && words[0] == "sam" && words[1] == "deploy" && flag("--region") == "$region":
		return stepRegional, true
	case len(words) > 2 && strings.Join(words[:3], " ") == stepPolicy:
		return stepPolicy, true
	}
	if args, ok := dynafluxArgs(words); ok && len(args) > 0 && args[0] == "backfill" && slices.Contains(args, "-all") {
		return stepBackfill, true
	}
	return "", false
}

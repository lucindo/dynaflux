// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/cli"
)

func TestModuleVersion(t *testing.T) {
	t.Parallel()
	const module = "github.com/lucindo/dynaflux"
	vcs := []debug.BuildSetting{
		{Key: "vcs", Value: "git"},
		{Key: "vcs.revision", Value: "2478e0ffa3927374307619bfd0a0c807700e1aa2"},
		{Key: "vcs.modified", Value: "true"},
	}
	tests := []struct {
		name string
		bi   *debug.BuildInfo
		want string
	}{
		{"no build info", nil, "(unknown)"},
		{
			"installed with go install",
			&debug.BuildInfo{Main: debug.Module{Path: module, Version: "v0.3.0"}},
			"v0.3.0",
		},
		{
			"built from a checkout with changes",
			&debug.BuildInfo{Main: debug.Module{Path: module, Version: "v0.0.0-20261026024858-2478e0ffa392+dirty"}, Settings: vcs},
			"v0.0.0-20261026024858-2478e0ffa392+dirty",
		},
		{
			"built with -buildvcs=false",
			&debug.BuildInfo{Main: debug.Module{Path: module, Version: "(devel)"}},
			"(devel)",
		},
		{
			"run with go tool, ignoring the project's revision",
			&debug.BuildInfo{
				Main:     debug.Module{Path: "example.com/shop", Version: "(devel)"},
				Deps:     []*debug.Module{{Path: "example.com/other", Version: "v1.0.0"}, {Path: module, Version: "v0.3.0"}},
				Settings: vcs,
			},
			"v0.3.0",
		},
		{
			"run with go tool, replaced by a local directory",
			&debug.BuildInfo{Main: debug.Module{
				Path: module, Version: "v0.0.0-00010101000000-000000000000",
				Replace: &debug.Module{Path: "/home/ana/dynaflux", Version: "(devel)"},
			}},
			"v0.0.0-00010101000000-000000000000 => /home/ana/dynaflux (devel)",
		},
		{
			"replaced by a local directory",
			&debug.BuildInfo{
				Main: debug.Module{Path: "example.com/shop", Version: "(devel)"},
				Deps: []*debug.Module{{Path: module, Version: "v0.3.0", Replace: &debug.Module{Path: "../dynaflux"}}},
			},
			"v0.3.0 => ../dynaflux",
		},
		{
			"replaced by a fork",
			&debug.BuildInfo{
				Main: debug.Module{Path: "example.com/shop", Version: "(devel)"},
				Deps: []*debug.Module{{
					Path: module, Version: "v0.3.0",
					Replace: &debug.Module{Path: "example.com/fork/dynaflux", Version: "v0.3.1"},
				}},
			},
			"v0.3.0 => example.com/fork/dynaflux v0.3.1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := cli.ModuleVersion(tt.bi); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestVersion checks the versions that version prints: the CLI's, from its build, and the
// runtime library's, from the go.mod of the working directory's module.
func TestVersion(t *testing.T) {
	t.Parallel()
	build := &debug.BuildInfo{Main: debug.Module{Path: "github.com/lucindo/dynaflux", Version: "v1.2.0"}}
	tests := []struct {
		name, gomod string
		library     string
	}{
		{"required", "module example.com/shop\n\nrequire github.com/lucindo/dynaflux v1.1.0\n", "v1.1.0"},
		{
			"replaced", "module example.com/shop\n\nrequire github.com/lucindo/dynaflux v1.1.0\n\nreplace github.com/lucindo/dynaflux => ../dynaflux\n",
			"v1.1.0 => ../dynaflux",
		},
		{"not required", "module example.com/shop\n", "(not in go.mod)"},
		{"Dynaflux's own module", "module github.com/lucindo/dynaflux\n", "(devel)"},
		{"no go.mod", "", "(no go.mod)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if tt.gomod != "" {
				writeFile(t, filepath.Join(dir, "go.mod"), tt.gomod)
			}
			sub := filepath.Join(dir, "svc") // the module's go.mod is above the working directory
			must(t, os.Mkdir(sub, 0o755))
			var stdout, stderr strings.Builder
			status := cli.RunEnv(t.Context(), []string{"version"}, nil, &stdout, &stderr, sub, cli.Env{Build: build, GoModCeiling: dir})
			got := result{status, stdout.String(), stderr.String()}
			want := result{0, "cli             v1.2.0\nlibrary         " + tt.library + "\ngenerated code  version 1\n", ""}
			if got != want {
				t.Errorf("got  %+v\nwant %+v", got, want)
			}
		})
	}
}

// TestParseGoMod reads the module path, and the require and replace directives that name
// Dynaflux, in each form that go.mod allows.
func TestParseGoMod(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, gomod string
		want        cli.GoMod
	}{
		{"module only", "module example.com/shop\n\ngo 1.26\n", cli.GoMod{Module: "example.com/shop"}},
		{"quoted module with a comment", "// The app.\nmodule \"example.com/app\" // quoted\n", cli.GoMod{Module: "example.com/app"}},
		{"a word that starts with module", "modules example.com/x\nmodule example.com/shop\n", cli.GoMod{Module: "example.com/shop"}},
		{"one-line require", "module m\nrequire github.com/lucindo/dynaflux v1.0.0\n", cli.GoMod{Module: "m", Library: "v1.0.0"}},
		{"quoted require", "module m\nrequire \"github.com/lucindo/dynaflux\" v1.0.0\n", cli.GoMod{Module: "m", Library: "v1.0.0"}},
		{
			"require block, indirect",
			"module m\n\nrequire (\n\tgithub.com/aws/aws-sdk-go-v2 v1.47.4 // indirect\n\tgithub.com/lucindo/dynaflux v1.0.0-rc.1 // indirect\n)\n",
			cli.GoMod{Module: "m", Library: "v1.0.0-rc.1"},
		},
		{"another module with the prefix", "module m\nrequire github.com/lucindo/dynaflux-extra v1.0.0\n", cli.GoMod{Module: "m"}},
		{"a tool block", "module m\ntool (\n\tgithub.com/lucindo/dynaflux/cmd/dynaflux\n)\n", cli.GoMod{Module: "m"}},
		{
			"replace by a directory",
			"module m\nrequire github.com/lucindo/dynaflux v0.0.0-00010101000000-000000000000\nreplace github.com/lucindo/dynaflux => ../dynaflux\n",
			cli.GoMod{Module: "m", Library: "v0.0.0-00010101000000-000000000000", Replace: "../dynaflux"},
		},
		{
			"replace block of one version by a fork",
			"module m\nrequire github.com/lucindo/dynaflux v1.0.0\nreplace (\n\tgithub.com/lucindo/dynaflux v1.0.0 => example.com/fork v1.0.1\n)\n",
			cli.GoMod{Module: "m", Library: "v1.0.0", Replace: "example.com/fork v1.0.1"},
		},
		{
			"blocks without a space before the parenthesis",
			"module m\nrequire(\n\tgithub.com/lucindo/dynaflux v1.0.0\n)\nreplace(\n\tgithub.com/lucindo/dynaflux => ../dynaflux\n)\n",
			cli.GoMod{Module: "m", Library: "v1.0.0", Replace: "../dynaflux"},
		},
		{"no module directive", "go 1.26\n", cli.GoMod{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := cli.ParseGoMod([]byte(tt.gomod)); got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

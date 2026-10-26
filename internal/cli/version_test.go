// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"runtime/debug"
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

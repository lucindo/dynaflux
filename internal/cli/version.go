// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"flag"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/lucindo/dynaflux"
)

// modulePath is the module of both the CLI and the runtime library.
const modulePath = "github.com/lucindo/dynaflux"

func versionCommand() command {
	return command{
		name:    "version",
		summary: "show the versions of the CLI, the runtime library and generated code",
		doc: `Version shows the version of the CLI and of the runtime library, which are one Go
module, and the version of the code that dynaflux generate writes. Generated code
compiles only with a runtime library that still supports its version.
`,
		setup: func(*flag.FlagSet) func(*cli, []string) int {
			return (*cli).version
		},
	}
}

func (c *cli) version(args []string) int {
	if len(args) > 0 {
		return c.usageError("version", "unexpected arguments: "+strings.Join(args, " "))
	}
	bi, _ := debug.ReadBuildInfo()
	v := moduleVersion(bi)
	fmt.Fprintf(&c.out, "cli              %s\nruntime library  %s\ngenerated code   version %d\n",
		v, v, dynaflux.GeneratedCodeVersion1)
	return exitOK
}

// moduleVersion returns the version of the Dynaflux module in the binary that bi
// describes, which is the main module when the CLI is built on its own, and a
// dependency when a project runs it with go tool.
func moduleVersion(bi *debug.BuildInfo) string {
	const unknown = "(unknown)"
	if bi == nil {
		return unknown
	}
	if bi.Main.Path == modulePath {
		// Go stamps a build from a checkout with a pseudo-version that names the commit.
		return bi.Main.Version
	}
	for _, m := range bi.Deps {
		if m.Path != modulePath {
			continue
		}
		if m.Replace != nil {
			return strings.TrimSpace(m.Version + " => " + m.Replace.Path + " " + m.Replace.Version)
		}
		return m.Version
	}
	return unknown
}

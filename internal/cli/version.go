// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"flag"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux"
)

const (
	// modulePath is the module of both the CLI and the library.
	modulePath = "github.com/lucindo/dynaflux"
	// develVersion is Go's version of a module built from its directory.
	develVersion = "(devel)"
)

func versionCommand() command {
	return command{
		name:    "version",
		summary: "show the versions of the CLI, the library and generated code",
		doc: `Version shows the version of the CLI, the version of the Dynaflux library that the
go.mod of the working directory's module requires, and the version of the code that
dynaflux generate writes. Generated code compiles only with a library that still
supports its version.
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
	mod, m, err := c.readGoMod(c.workdir)
	if err != nil {
		c.errorf("dynaflux version: %v", err)
		return exitUsage
	}
	library := "(no go.mod)"
	if mod != "" {
		library = m.libraryText()
	}
	fmt.Fprintf(&c.out, "cli             %s\nlibrary         %s\ngenerated code  version %d\n",
		moduleVersion(c.build), library, dynaflux.GeneratedCodeVersion1)
	return exitOK
}

// builtModule returns the Dynaflux module in the binary that bi describes, or nil: the main
// module when the CLI is built on its own or run with go tool, and a dependency when it is
// built in another module. It also reports a main module built from a checkout, which Go
// stamps with a pseudo-version that names the commit.
func builtModule(bi *debug.BuildInfo) (*debug.Module, bool) {
	switch {
	case bi == nil:
		return nil, false
	case bi.Main.Path == modulePath:
		return &bi.Main, slices.ContainsFunc(bi.Settings, func(s debug.BuildSetting) bool { return s.Key == "vcs" })
	}
	if i := slices.IndexFunc(bi.Deps, func(d *debug.Module) bool { return d.Path == modulePath }); i >= 0 {
		return bi.Deps[i], false
	}
	return nil, false
}

// moduleVersion returns the version of the Dynaflux module in the binary that bi
// describes, as version prints it. A replaced module shows its replacement.
func moduleVersion(bi *debug.BuildInfo) string {
	m, _ := builtModule(bi)
	switch {
	case m == nil:
		return "(unknown)"
	case m.Replace != nil:
		return strings.TrimSpace(m.Version + " => " + m.Replace.Path + " " + m.Replace.Version)
	}
	return m.Version
}

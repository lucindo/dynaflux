// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"cmp"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"path"
	"runtime/debug"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/codegen"
)

func generateCommand() command {
	return command{
		name:    "generate",
		alias:   "gen",
		summary: "write the Go client package and update dynaflux.lock",
		doc: `Generate checks the project as validate does. Without errors, it writes the typed
Go client package to go.out and numbers the view versions in dynaflux.lock. It
prints one line for each version that is new, current again or retained, and
nothing more when no version changed. It leaves unchanged files alone, and won't
overwrite a Go file in go.out that has no "// Code generated ... DO NOT EDIT." line.

Exit status: 0 when the files are written (warnings are allowed), 1 when the project
has an error or go.out holds a file that isn't generated, and 2 for usage and I/O
problems.
`,
		setup: func(*flag.FlagSet) func(*cli, []string) int {
			return (*cli).generate
		},
	}
}

func (c *cli) generate(args []string) int {
	if len(args) > 0 {
		return c.usageError("generate", "unexpected arguments: "+strings.Join(args, " "))
	}
	p, ds, status := c.load()
	if status = c.diagnostics(ds, status); status != exitOK {
		return status
	}
	root, err := os.OpenRoot(p.dir)
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitUsage
	}
	defer root.Close()
	outputs, err := render(p)
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitUsage
	}
	if status = c.write(root, outputs); status != exitOK {
		return status
	}
	for _, ch := range p.lockChanges {
		fmt.Fprintln(&c.out, ch)
	}
	return exitOK
}

// output is a file that generate writes, by its slash path from the project root.
type output struct {
	path string
	data []byte
}

// render returns the files to write, with dynaflux.lock last so that a failed run leaves
// the old lock in place.
func render(p *project) ([]output, error) {
	lockData, err := catalog.MarshalLock(p.lock)
	if err != nil {
		return nil, err
	}
	bi, _ := debug.ReadBuildInfo() // A nil BuildInfo reads as (devel).
	files, err := codegen.Package(p.cat, codegen.Options{Package: p.cfg.Go.Package, Version: headerVersion(bi)})
	if err != nil {
		return nil, err
	}
	var outputs []output
	for _, name := range slices.Sorted(maps.Keys(files)) {
		outputs = append(outputs, output{path.Join(p.cfg.Go.Out, name), files[name]})
	}
	return append(outputs, output{catalog.LockFileName, lockData}), nil
}

// write writes the outputs whose bytes changed, each through a temporary file and a
// rename. It first checks that no Go file it would replace is someone's own code.
func (c *cli) write(root *os.Root, outputs []output) int {
	var changed []output
	for _, o := range outputs {
		old, err := root.ReadFile(o.path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			c.errorf("dynaflux generate: %v", err)
			return exitUsage
		case bytes.Equal(old, o.data):
			continue
		case path.Ext(o.path) == ".go" && !isGenerated(old):
			c.errorf("dynaflux generate: %s has no \"// Code generated ... DO NOT EDIT.\" line, so it isn't "+
				"generated code; move it out of go.out", o.path)
			return exitErrors
		}
		changed = append(changed, o)
	}
	for _, o := range changed {
		if err := writeFile(root, o); err != nil {
			c.errorf("dynaflux generate: %v", err)
			return exitUsage
		}
	}
	return exitOK
}

func writeFile(root *os.Root, o output) error {
	const dirPerm, filePerm = 0o755, 0o644
	dir, name := path.Split(o.path)
	if dir != "" {
		if err := root.MkdirAll(dir, dirPerm); err != nil {
			return err
		}
	}
	tmp := dir + "." + name + ".tmp"
	if err := root.WriteFile(tmp, o.data, filePerm); err != nil {
		return err
	}
	if err := root.Rename(tmp, o.path); err != nil {
		return errors.Join(err, root.Remove(tmp))
	}
	return nil
}

// isGenerated reports whether src is Go source with the generated-code header.
func isGenerated(src []byte) bool {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, parser.PackageClauseOnly|parser.ParseComments)
	return err == nil && ast.IsGenerated(f)
}

// headerVersion returns the version in the header of generated files: the Dynaflux
// release that go.mod pins, or (devel) for a CLI built from a checkout or a module
// replaced by a local directory, whose versions change with each commit or name a path.
func headerVersion(bi *debug.BuildInfo) string {
	const devel = "(devel)"
	switch {
	case bi == nil:
		return devel
	case bi.Main.Path == modulePath:
		if slices.ContainsFunc(bi.Settings, func(s debug.BuildSetting) bool { return s.Key == "vcs" }) {
			return devel
		}
		return bi.Main.Version
	}
	for _, m := range bi.Deps {
		if m.Path != modulePath {
			continue
		}
		if m.Replace != nil {
			m = m.Replace
		}
		return cmp.Or(m.Version, devel)
	}
	return devel
}

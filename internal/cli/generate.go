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
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/codegen"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/infra/cloudformation"
)

func generateCommand() command {
	return command{
		name:    "generate",
		alias:   "gen",
		args:    "[-unprotect-region region] [-rebuild view]",
		summary: "write the Go client package and templates, and update dynaflux.lock",
		doc: `Generate checks the project as validate does. Without errors, it writes the typed
Go client package to go.out, the runtime binary's main.go to runtime.out, the
CloudFormation templates to infra.out, and numbers the view versions in
dynaflux.lock. It prints one line for each version that is new, current again or
retained, and nothing more when no version changed. main.go imports go.out by the
module path in the go.mod of the project root or the nearest directory above it.

It leaves unchanged files alone. It won't overwrite a Go file in go.out or
runtime.out that has no "// Code generated ... DO NOT EDIT." line, or a template
in infra.out whose first line isn't "# Code generated ... DO NOT EDIT.". It
overwrites global.stack-policy.json, which can't carry that line, only when the
global.yaml beside it carries it.

It compares the new templates with those it wrote last time, and writes nothing
when a view version's table would disappear or hold another definition (DF6016),
unless dynaflux.lock retires the version, when the replica regions would change by
more than one region (DF6017), or when a region would go before a run with
-unprotect-region for it was deployed (DF6018).

-unprotect-region applies to one run: the next run without it turns protection
back on.

-rebuild gives a view's unchanged definition a new version, for a backfill to fill
again after a gap or a fix in Dynaflux. It raises the view's rebuild count in
dynaflux.lock by one.

Exit status: 0 when the files are written (warnings are allowed), 1 when the project
has an error, has no stream to deploy, fails a comparison with the last templates,
or go.out, runtime.out or infra.out holds a file that isn't generated, and 2 for
usage and I/O problems, and for a project outside a Go module.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			var opts infra.Options
			var rebuild string
			fs.StringVar(&opts.UnprotectRegion, "unprotect-region", "",
				"turn off deletion protection on the stream table replicas in `region`, the first step of removing it from regions")
			fs.StringVar(&rebuild, "rebuild", "", "give the current definition of `view` a new version")
			return func(c *cli, args []string) int { return c.generate(opts, rebuild, args) }
		},
	}
}

func (c *cli) generate(opts infra.Options, rebuild string, args []string) int {
	if len(args) > 0 {
		return c.usageError("generate", "unexpected arguments: "+strings.Join(args, " "))
	}
	var rebuilds []string
	if rebuild != "" {
		rebuilds = []string{rebuild}
	}
	p, ds, status := c.load(rebuilds...)
	if status = c.diagnostics(ds, status); status != exitOK {
		return status
	}
	if r := opts.UnprotectRegion; r != "" && !slices.Contains(p.cfg.Regions, r) {
		return c.usageError("generate", fmt.Sprintf("-unprotect-region %s isn't in regions (%s)", r, strings.Join(p.cfg.Regions, ", ")))
	}
	if rebuild != "" {
		v, ok := p.cat.View(rebuild)
		if ok {
			_, ok = v.Current()
		}
		if !ok {
			return c.usageError("generate", fmt.Sprintf("-rebuild %s: the SQL files define no view %s", rebuild, rebuild))
		}
	}
	root, err := os.OpenRoot(p.dir)
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitUsage
	}
	defer root.Close()
	bi, _ := debug.ReadBuildInfo() // A nil BuildInfo reads as (devel).
	version := headerVersion(bi)
	infraFiles, err := templates(p, version, opts)
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitErrors
	}
	if status = c.checkPrevious(root, p, infraFiles); status != exitOK {
		return status
	}
	outputs, err := render(p, version, infraFiles)
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
	// vouch is the generated file beside this one whose generated-file line also covers
	// this one, which can't carry the line; empty when it carries its own.
	vouch string
}

// templates builds the store's infrastructure manifest and renders it in infra.out.
func templates(p *project, version string, opts infra.Options) ([]output, error) {
	m, err := infra.Build(p.cfg, p.cat, opts)
	if err != nil {
		return nil, err
	}
	files, err := cloudformation.Render(m, version)
	if err != nil {
		return nil, err
	}
	var outputs []output
	for _, name := range slices.Sorted(maps.Keys(files)) {
		o := output{path: path.Join(p.cfg.Infra.Out, name), data: files[name]}
		if name == cloudformation.GlobalStackPolicy {
			o.vouch = path.Join(p.cfg.Infra.Out, cloudformation.GlobalTemplate)
		}
		outputs = append(outputs, o)
	}
	return outputs, nil
}

// render returns the files to write: the Go client package, the templates, and
// dynaflux.lock last, so that a failed run leaves the old lock in place.
func render(p *project, version string, templates []output) ([]output, error) {
	lockData, err := catalog.MarshalLock(p.lock)
	if err != nil {
		return nil, err
	}
	o := codegen.Options{Package: p.cfg.Go.Package, Version: version}
	files, err := codegen.Package(p.cat, o)
	if err != nil {
		return nil, err
	}
	importPath, err := goOutImportPath(p.dir, p.cfg.Go.Out)
	if err != nil {
		return nil, err
	}
	mainGo, err := codegen.Main(p.cat, importPath, path.Base(p.cfg.Runtime.Out), o)
	if err != nil {
		return nil, err
	}
	var outputs []output
	for _, name := range slices.Sorted(maps.Keys(files)) {
		outputs = append(outputs, output{path: path.Join(p.cfg.Go.Out, name), data: files[name]})
	}
	outputs = append(outputs, output{path: path.Join(p.cfg.Runtime.Out, "main.go"), data: mainGo})
	outputs = append(outputs, templates...)
	return append(outputs, output{path: catalog.LockFileName, data: lockData}), nil
}

// goOutImportPath returns the import path of go.out, which is relative to the project root
// dir: the path of the module whose go.mod is in dir or the nearest directory above it,
// joined with go.out's directory in that module.
func goOutImportPath(dir, goOut string) (string, error) {
	for mod := dir; ; {
		data, err := os.ReadFile(filepath.Join(mod, "go.mod"))
		if err == nil {
			modPath := goModulePath(data)
			if modPath == "" {
				return "", fmt.Errorf("%s has no module line", filepath.Join(mod, "go.mod"))
			}
			rel, relErr := filepath.Rel(mod, filepath.Join(dir, filepath.FromSlash(goOut)))
			if relErr != nil {
				return "", relErr
			}
			return path.Join(modPath, filepath.ToSlash(rel)), nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(mod)
		if parent == mod {
			return "", fmt.Errorf("no go.mod in %s or a directory above it: the main.go in runtime.out imports go.out "+
				"by its import path, so the project must be in a Go module", dir)
		}
		mod = parent
	}
}

// goModulePath returns the path in go.mod's module directive, or "" without one.
func goModulePath(gomod []byte) string {
	for line := range strings.Lines(string(gomod)) {
		line, _, _ = strings.Cut(line, "//")
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module")
		if !ok || rest == strings.TrimLeft(rest, " \t") {
			continue // another directive, or a word that starts with module
		}
		rest = strings.TrimSpace(rest)
		if unquoted, err := strconv.Unquote(rest); err == nil {
			return unquoted
		}
		return rest
	}
	return ""
}

// write writes the outputs whose bytes changed, each through a temporary file and a
// rename. It first checks that no Go file or template it would replace is someone's own.
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
		default:
			if status := c.checkGenerated(root, o, old); status != exitOK {
				return status
			}
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

// checkGenerated checks that old, the file that o would replace, was generated.
func (c *cli) checkGenerated(root *os.Root, o output, old []byte) int {
	switch {
	case o.vouch != "":
		owner, err := root.ReadFile(o.vouch)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			c.errorf("dynaflux generate: %v", err)
			return exitUsage
		}
		if err != nil || !isGeneratedTemplate(owner) {
			c.errorf("dynaflux generate: %s can't carry a \"# Code generated ... DO NOT EDIT.\" line, and there is "+
				"no generated %s beside it, so it isn't generated; move it out of infra.out", o.path, path.Base(o.vouch))
			return exitErrors
		}
	case path.Ext(o.path) == ".go" && !isGenerated(old):
		c.errorf("dynaflux generate: %s has no \"// Code generated ... DO NOT EDIT.\" line, so it isn't "+
			"generated code; move it out of %s", o.path, path.Dir(o.path))
		return exitErrors
	case path.Ext(o.path) == ".yaml" && !isGeneratedTemplate(old):
		c.errorf("dynaflux generate: %s doesn't start with a \"# Code generated ... DO NOT EDIT.\" line, so it "+
			"isn't a generated template; move it out of infra.out", o.path)
		return exitErrors
	}
	return exitOK
}

// isGenerated reports whether src is Go source with the generated-code header.
func isGenerated(src []byte) bool {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, parser.PackageClauseOnly|parser.ParseComments)
	return err == nil && ast.IsGenerated(f)
}

// isGeneratedTemplate reports whether src is YAML whose first line is the generated-file
// line.
func isGeneratedTemplate(src []byte) bool {
	line, _, _ := strings.Cut(string(src), "\n")
	line = strings.TrimSuffix(line, "\r")
	return strings.HasPrefix(line, "# Code generated ") && strings.HasSuffix(line, " DO NOT EDIT.")
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

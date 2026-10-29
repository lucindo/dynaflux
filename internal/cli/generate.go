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
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/codegen"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
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

It writes query.yaml, the query function's template, only when a query is
annotated -- expose: http. Otherwise it deletes the query.yaml it wrote before, and
prints a line that says so: the deployed query stacks stay until you delete them.

It leaves unchanged files alone. It won't overwrite a Go file in go.out or
runtime.out that has no "// Code generated ... DO NOT EDIT." line, or a template
in infra.out whose first line isn't "# Code generated ... DO NOT EDIT.". It
overwrites global.stack-policy.json, which can't carry that line, only when the
global.yaml beside it carries it.

It writes nothing when the catalog is too big for the meta table to store
(DF6019). It compares the new templates with those it wrote last time, and writes
nothing when a view version's table would disappear or hold another definition
(DF6016), unless dynaflux.lock retires the version, when the replica regions
would change by more than one region (DF6017), or when a region would go before
a run with -unprotect-region for it was deployed (DF6018).

-unprotect-region applies to one run: the next run without it turns protection
back on.

-rebuild gives a view's unchanged definition a new version, for a backfill to fill
again after a gap or a fix in Dynaflux. It raises the view's rebuild count in
dynaflux.lock by one.

Exit status: 0 when the files are written (warnings are allowed), 1 when the project
has an error, has no stream to deploy, has a catalog too big to store, fails a
comparison with the last templates, or go.out, runtime.out or infra.out holds a
file that isn't generated, and 2 for usage and I/O problems, and for a project
outside a Go module.
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
	if status = c.checkCatalogSize(p); status != exitOK {
		return status
	}
	root, err := os.OpenRoot(p.dir)
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitUsage
	}
	defer root.Close()
	version := headerVersion(c.build)
	mod, gm, err := c.readGoMod(p.dir)
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitUsage
	}
	if w := gm.otherLibrary(version); w != "" {
		c.errorf("dynaflux generate: warning: %s", w)
	}
	importPath, err := goOutImportPath(p.dir, p.cfg.Go.Out, mod, gm.module)
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitUsage
	}
	infraFiles, err := templates(p, version, opts)
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitErrors
	}
	if status = c.checkPrevious(root, p, infraFiles); status != exitOK {
		return status
	}
	outputs, err := render(p, version, importPath, infraFiles)
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
	return c.removeQueryTemplate(root, p, outputs)
}

// removeQueryTemplate deletes the query.yaml that generate wrote before, when outputs have
// none because no query is exposed. A query.yaml without the generated-file line isn't
// generate's, and stays.
func (c *cli) removeQueryTemplate(root *os.Root, p *project, outputs []output) int {
	name := path.Join(p.cfg.Infra.Out, cloudformation.QueryTemplate)
	if slices.ContainsFunc(outputs, func(o output) bool { return o.path == name }) {
		return exitOK
	}
	old, err := root.ReadFile(name)
	switch {
	case errors.Is(err, fs.ErrNotExist), err == nil && !isGeneratedTemplate(old):
		return exitOK
	case err == nil:
		err = root.Remove(name)
	}
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitUsage
	}
	fmt.Fprintf(&c.out, "%s: removed, since no query is annotated -- expose: http; delete its stack in every region\n", name)
	return exitOK
}

// checkCatalogSize refuses a catalog too big for the meta table's item (DF6019).
func (c *cli) checkCatalogSize(p *project) int {
	data, err := catalog.Marshal(p.cat)
	if err == nil {
		data, err = catalog.Compress(data)
	}
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitErrors
	}
	if len(data) <= catalog.MaxCompressedSize {
		return exitOK
	}
	return c.diagnostics([]diag.Diagnostic{{
		Pos: diag.Pos{File: config.FileName, Line: 1, Col: 1}, Severity: diag.SeverityError, Code: "DF6019",
		Message: fmt.Sprintf("the catalog takes %d bytes compressed, more than the %d that the meta table can store in one item",
			len(data), catalog.MaxCompressedSize),
		Hint: "retire view versions that are no longer needed, remove views, queries or streams, or split the store",
	}}, exitErrors)
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
func render(p *project, version, importPath string, templates []output) ([]output, error) {
	lockData, err := catalog.MarshalLock(p.lock)
	if err != nil {
		return nil, err
	}
	o := codegen.Options{Package: p.cfg.Go.Package, Version: version}
	files, err := codegen.Package(p.cat, o)
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
// dir: the path of module modPath, whose go.mod is in mod, joined with go.out's directory in
// that module. mod is "" when no go.mod holds dir.
func goOutImportPath(dir, goOut, mod, modPath string) (string, error) {
	switch {
	case mod == "":
		return "", fmt.Errorf("no go.mod in %s or a directory above it: the main.go in runtime.out imports go.out "+
			"by its import path, so the project must be in a Go module", dir)
	case modPath == "":
		return "", fmt.Errorf("%s has no module line", filepath.Join(mod, "go.mod"))
	}
	rel, err := filepath.Rel(mod, filepath.Join(dir, filepath.FromSlash(goOut)))
	if err != nil {
		return "", err
	}
	return path.Join(modPath, filepath.ToSlash(rel)), nil
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
	m, checkout := builtModule(bi)
	switch {
	case m == nil:
		return develVersion
	case m.Replace != nil: // a local directory has no version
		return cmp.Or(m.Replace.Version, develVersion)
	case checkout:
		return develVersion
	}
	return cmp.Or(m.Version, develVersion)
}

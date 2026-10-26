// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package validate is the compiler front end of Dynaflux. It loads a project's
// dynaflux.yaml and SQL files, checks every rule of docs/spec/sql.md and
// docs/spec/config.md, and compiles the definitions into a [catalog.Catalog].
package validate

import (
	"io/fs"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
)

// Load validates the project whose root is fsys and compiles it. root names that
// directory as the user should see it, such as its absolute path; it must not be
// empty. Load returns the config and the catalog exactly when no diagnostic is an
// error, and the diagnostics sorted.
func Load(fsys fs.FS, root string) (*config.Config, *catalog.Catalog, []diag.Diagnostic) {
	cfg, ds := config.Load(fsys)
	if cfg == nil {
		return nil, nil, ds
	}
	paths, fileDiags := cfg.SQLFiles(fsys, root)
	ds = append(ds, fileDiags...)
	if diag.HasErrors(fileDiags) {
		diag.Sort(ds)
		return nil, nil, ds
	}
	p := &project{cfg: cfg, tables: map[string]*decl{}, queries: map[string]*decl{}}
	for _, path := range paths {
		p.parse(fsys, path)
	}
	cat := p.compile()
	ds = append(ds, p.d.list...)
	diag.Sort(ds)
	if diag.HasErrors(ds) {
		return nil, nil, ds
	}
	return cfg, cat, ds
}

// project is the state of one compilation.
type project struct {
	cfg *config.Config
	d   diags
	// decls are the statements of every file, in path order and then source order.
	decls []*decl
	// tables and queries hold the first definition of each name: streams and views share
	// one namespace, and queries have their own.
	tables  map[string]*decl
	queries map[string]*decl
}

func (p *project) parse(fsys fs.FS, path string) {
	src, err := fs.ReadFile(fsys, path)
	if err != nil {
		p.d.add(diag.SeverityError, "", diag.Pos{File: path, Line: 1, Col: 1}, "DF6002",
			"check the path and its permissions", "can't read "+path+": "+err.Error())
		return
	}
	f, ds := sql.Parse(path, src)
	p.d.list = append(p.d.list, ds...)
	for _, s := range f.Stmts {
		p.declare(f, s)
	}
}

// compile runs every check on the parsed files and builds the catalog.
func (p *project) compile() *catalog.Catalog {
	for _, d := range p.decls {
		checkNames(d)
	}
	for _, d := range p.decls {
		if s, ok := d.node.(*sql.CreateStream); ok && !d.broken() {
			p.compileStream(d, s)
		}
	}
	for _, d := range p.decls {
		if s, ok := d.node.(*sql.CreateView); ok && !d.broken() {
			p.compileView(d, s)
		}
	}
	for _, d := range p.decls {
		if s, ok := d.node.(*sql.Query); ok && !d.broken() {
			p.compileQuery(d, s)
		}
	}
	p.checkSyncTransactions()
	p.checkGoNames()
	p.checkConfigNames()
	return p.catalog()
}

func (p *project) catalog() *catalog.Catalog {
	c := &catalog.Catalog{
		Store:              p.cfg.Store,
		Regions:            p.cfg.Regions,
		PruneWindowSeconds: p.cfg.Runtime.PruneWindowSeconds,
	}
	for _, d := range p.decls {
		switch {
		case d.stream != nil:
			c.Streams = append(c.Streams, d.stream.stream)
		case d.view != nil:
			c.Views = append(c.Views, catalog.View{Name: d.name, Versions: []catalog.ViewVersion{d.view.version}})
		case d.query != nil:
			c.Queries = append(c.Queries, d.query.query)
		}
	}
	return c
}

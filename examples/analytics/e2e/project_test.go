// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"bytes"
	"io/fs"
	"os"
	"path"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/validate"
)

// The e2e store is the example with e2eSQL added, as scripts/e2e/deploy.sh generates it.
const (
	exampleDir = ".."
	e2eSQL     = "testdata/e2e.sql"
	// ratioVersion is e2eSQL's view, which a page view with duration_ms 0 breaks.
	ratioVersion = "e2e_ratio@v1"
	// e2eQueries read what the example's queries leave out. Queries aren't deployed, so
	// they are in a catalog of their own, which the oracle can query too: without e2eSQL,
	// it has no view that breaks.
	e2eQueries = "testdata/queries.sql"
)

// project is the e2e store compiled.
type project struct {
	cfg      *config.Config
	def      *catalog.Catalog
	json     []byte // the catalog as generate embeds it
	manifest *infra.Manifest
	queries  *dynaflux.Catalog // the example with e2eQueries
}

func loadProject(t *testing.T) *project {
	t.Helper()
	cfg, def := compile(t, e2eSQL)
	p := &project{cfg: cfg, def: def}
	var err error
	if p.json, err = catalog.Marshal(def); err != nil {
		t.Fatal(err)
	}
	if p.manifest, err = infra.Build(cfg, def, infra.Options{}); err != nil {
		t.Fatal(err)
	}
	_, queries := compile(t, e2eQueries)
	data, err := catalog.Marshal(queries)
	if err != nil {
		t.Fatal(err)
	}
	p.queries = fluxgen.LoadCatalog(string(data))
	return p
}

// compile compiles the example with an SQL file of testdata added.
func compile(t *testing.T, sql string) (*config.Config, *catalog.Catalog) {
	t.Helper()
	fsys := fstest.MapFS{}
	example := os.DirFS(exampleDir)
	for _, root := range []string{"dynaflux.yaml", catalog.LockFileName, "dynaflux"} {
		err := fs.WalkDir(example, root, func(name string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			data, err := fs.ReadFile(example, name)
			fsys[name] = &fstest.MapFile{Data: data}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(sql)
	if err != nil {
		t.Fatal(err)
	}
	fsys["dynaflux/"+path.Base(sql)] = &fstest.MapFile{Data: data}
	cfg, def, ds := validate.Load(fsys, exampleDir)
	if def == nil {
		var b bytes.Buffer
		if err = diag.Write(&b, ds); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("the example with %s doesn't compile:\n%s", sql, b.Bytes())
	}
	return cfg, def
}

// fingerprint is DYNAFLUX_ARCHIVE_SCHEMA in region.
func (p *project) fingerprint(region string) string {
	return archive.SchemaFor(p.def, region).Fingerprint()
}

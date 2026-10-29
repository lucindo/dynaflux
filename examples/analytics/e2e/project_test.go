// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"os"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/infra"
)

const (
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

// loadProject compiles the e2e store in the regions of E2E_REGIONS, a list separated by
// spaces, or in the example's.
func loadProject(t *testing.T) *project {
	t.Helper()
	regions := strings.Fields(os.Getenv("E2E_REGIONS"))
	cfg, def := compile(t, e2eSQL, regions)
	p := &project{cfg: cfg, def: def}
	var err error
	if p.json, err = catalog.Marshal(def); err != nil {
		t.Fatal(err)
	}
	if p.manifest, err = infra.Build(cfg, def, infra.Options{}); err != nil {
		t.Fatal(err)
	}
	_, queries := compile(t, e2eQueries, regions)
	data, err := catalog.Marshal(queries)
	if err != nil {
		t.Fatal(err)
	}
	p.queries = fluxgen.LoadCatalog(string(data))
	return p
}

// fingerprint is DYNAFLUX_ARCHIVE_SCHEMA in region.
func (p *project) fingerprint(region string) string {
	return archive.SchemaFor(p.def, region).Fingerprint()
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package fluxtest_test

import (
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/fluxtest"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
)

// recorder records Errorf and Fatalf instead of failing the test.
type recorder struct {
	testing.TB

	errors []string
	fatal  string
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func (r *recorder) Fatalf(format string, args ...any) { r.fatal = fmt.Sprintf(format, args...) }

func TestNewStoreFailsOnUnreadableCatalog(t *testing.T) {
	t.Parallel()
	r := &recorder{TB: t}
	fluxtest.NewStore(r, fluxgen.LoadCatalog(`{"format":99}`))
	if !strings.Contains(r.fatal, "can't load the generated catalog") {
		t.Errorf("NewStore failed the test with %q, want the catalog's load error", r.fatal)
	}
}

func TestNewStoreFailsOnZeroCatalog(t *testing.T) {
	t.Parallel()
	r := &recorder{TB: t}
	fluxtest.NewStore(r, new(dynaflux.Catalog))
	if !strings.Contains(r.fatal, "the Catalog is nil or zero") {
		t.Errorf("NewStore failed the test with %q, want a zero Catalog named", r.fatal)
	}
}

func TestBrokenViewFailsTheTest(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const sql = `
CREATE STREAM readings (n UInt32) PRIMARY KEY (n);
CREATE MATERIALIZED VIEW shares ORDER BY (n) AS SELECT n, sum(intDiv(100, n)) AS share FROM readings GROUP BY n;
`
		cat := fluxgen.LoadCatalog(catalogJSON(t, sql))
		r := &recorder{TB: t}
		s := fluxtest.NewStore(r, cat)
		var (
			n          uint32
			id, region string
			at, ingest time.Time
		)
		if _, err := fluxgen.AppendEvent(t.Context(), s, cat, "readings", []any{&n, &id, &at, &ingest, &region}); err != nil {
			t.Fatalf("the append failed: %v", err)
		}
		if len(r.errors) != 1 || !strings.Contains(r.errors[0], "view shares@v1 broke on event") || !strings.Contains(r.errors[0], "intDiv") {
			t.Errorf("the test got errors %q, want one that names the broken view and the failure", r.errors)
		}
	})
}

// catalogJSON is what dynaflux generate embeds for sql.
func catalogJSON(t *testing.T, sql string) string {
	t.Helper()
	return string(catalogtest.Marshal(t, "app", sql))
}

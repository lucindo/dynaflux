// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package fluxtest_test

import (
	"errors"
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

// quota is a quota-style counter: a SYNC view, which a poison event with no units fails,
// and an ASYNC view of the same stream.
const quota = `
CREATE STREAM api_calls (account_id String, units UInt32, request_id String)
PRIMARY KEY (account_id)
DEDUP BY (request_id);
CREATE MATERIALIZED VIEW quota_usage
MODE SYNC
ORDER BY (account_id)
AS SELECT account_id, sum(units) AS used, sum(intDiv(100, units)) AS shares FROM api_calls GROUP BY account_id;
CREATE MATERIALIZED VIEW call_counts
ORDER BY (account_id)
AS SELECT account_id, count() AS calls FROM api_calls GROUP BY account_id;
-- name: Used :one
SELECT used FROM quota_usage WHERE account_id = {account_id:String};
-- name: Calls :one
SELECT calls FROM call_counts WHERE account_id = {account_id:String};
`

type apiCall struct {
	account, request      string
	units                 uint32
	id, region            string
	eventTime, ingestTime time.Time
}

func (c *apiCall) fields() []any {
	return []any{&c.account, &c.units, &c.request, &c.id, &c.eventTime, &c.ingestTime, &c.region}
}

// count reads a :one query's one UInt64 column for account, or the query's error.
func count(t *testing.T, s *dynaflux.Store, cat *dynaflux.Catalog, query, account string) (uint64, error) {
	t.Helper()
	return fluxgen.QueryRow(t.Context(), s, cat, query, []any{&account}, func(n *uint64) []any { return []any{n} })
}

// A quota check sees its own append at once: the append's transaction updates the SYNC
// view, and the projector the ASYNC one. An append that fails the SYNC view's evaluation
// fails with ErrEvaluation and writes nothing, without failing the test.
func TestSyncViewSeesItsOwnAppend(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cat := fluxgen.LoadCatalog(catalogJSON(t, quota))
		r := &recorder{TB: t}
		s := fluxtest.NewStore(r, cat)
		calls := []apiCall{{account: "acme", request: "r1", units: 5}, {account: "acme", request: "r2", units: 3}, {account: "acme", request: "r3"}}
		var errs []error
		for i := range calls {
			calls[i].eventTime = time.Now()
			_, err := fluxgen.AppendEvent(t.Context(), s, cat, "api_calls", calls[i].fields())
			errs = append(errs, err)
		}
		if errs[0] != nil || errs[1] != nil || !errors.Is(errs[2], dynaflux.ErrEvaluation) ||
			errs[2].Error() != "dynaflux: append to api_calls: projector: view quota_usage@v1: evaluation error in intDiv: division by zero" {
			t.Errorf("appends failed with %v; want only the third, with ErrEvaluation naming quota_usage@v1", errs)
		}
		used, uerr := count(t, s, cat, "Used", "acme")
		n, cerr := count(t, s, cat, "Calls", "acme")
		if used != 8 || n != 2 || uerr != nil || cerr != nil || len(r.errors) > 0 {
			t.Errorf("Used %d (%v), Calls %d (%v), test errors %q; want 8 and 2, no errors", used, uerr, n, cerr, r.errors)
		}
	})
}

// The projector writes a receipt before the append returns, so a wait for a Waitable append
// returns at once, from this process or, through the token's text, from another. A wait for
// an append without Waitable fails, also as one of two waits.
func TestWaitForReturnsAtOnce(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cat := fluxgen.LoadCatalog(catalogJSON(t, quota))
		r := &recorder{TB: t}
		s := fluxtest.NewStore(r, cat)
		begin := time.Now()
		call := apiCall{account: "acme", request: "r1", units: 5, eventTime: begin}
		tok, err := fluxgen.AppendEvent(t.Context(), s, cat, "api_calls", call.fields(), dynaflux.Waitable())
		if err != nil {
			t.Fatal(err)
		}
		parsed, perr := dynaflux.ParseToken(tok.String())
		account := "acme"
		n, werr := fluxgen.QueryRow(t.Context(), s, cat, "Calls", []any{&account}, func(n *uint64) []any { return []any{n} },
			dynaflux.WaitFor(parsed, time.Second))
		if perr != nil || parsed != tok || n != 1 || werr != nil || time.Since(begin) != 0 {
			t.Errorf("ParseToken: %v; Calls after the wait = %d, %v, %v later; want the token back, then 1 at once", perr, n, werr, time.Since(begin))
		}
		call.request = "r2"
		plain, err := fluxgen.AppendEvent(t.Context(), s, cat, "api_calls", call.fields())
		if err != nil {
			t.Fatal(err)
		}
		_, werr = fluxgen.QueryRow(t.Context(), s, cat, "Calls", []any{&account}, func(n *uint64) []any { return []any{n} },
			dynaflux.WaitFor(plain, time.Second))
		if werr == nil || !strings.Contains(werr.Error(), "append with dynaflux.Waitable() to wait for it") || len(r.errors) > 0 {
			t.Errorf("a wait for an append without Waitable: %v, test errors %q; want it refused", werr, r.errors)
		}
		_, werr = fluxgen.QueryRow(t.Context(), s, cat, "Calls", []any{&account}, func(n *uint64) []any { return []any{n} },
			dynaflux.WaitFor(plain, time.Second), dynaflux.WaitFor(parsed, time.Second))
		if werr == nil || !strings.Contains(werr.Error(), "append with dynaflux.Waitable() to wait for it") {
			t.Errorf("waits for an append without Waitable, then one with: %v; want them refused", werr)
		}
	})
}

// catalogJSON is what dynaflux generate embeds for sql.
func catalogJSON(t *testing.T, sql string) string {
	t.Helper()
	return string(catalogtest.Marshal(t, "app", sql))
}

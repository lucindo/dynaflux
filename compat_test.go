// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux_test

import (
	"bufio"
	"bytes"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/fluxtest"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// The fixtures in testdata/compat/v1 are frozen at v1.0.0, and every v1 release reads them
// (ADR 0019). A new format adds a fixture next to them; nobody edits these.

// examplePageView, pageStatsParams and pageStatsRow are what dynaflux generate writes for
// page_views and PageStats in the catalog of testdata/compat/v1.
type examplePageView struct {
	UserID       string
	Page         string
	Referrer     *string
	Tags         []string
	DurationMS   uint32
	Country      string
	EventTime    time.Time
	EventID      string
	IngestTime   time.Time
	OriginRegion string
}

func (e *examplePageView) fields() []any {
	return []any{
		&e.UserID, &e.Page, &e.Referrer, &e.Tags, &e.DurationMS, &e.Country,
		&e.EventID, &e.EventTime, &e.IngestTime, &e.OriginRegion,
	}
}

type pageStatsParams struct {
	Page     string
	From, To time.Time
}

func (p *pageStatsParams) fields() []any { return []any{&p.Page, &p.From, &p.To} }

type pageStatsRow struct {
	Day             time.Time
	Views, Visitors uint64
}

func (r *pageStatsRow) fields() []any { return []any{&r.Day, &r.Views, &r.Visitors} }

func compatFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/compat/v1/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// pageStats runs PageStats for /home on 2000-01-01 with opts.
func pageStats(t *testing.T, s *dynaflux.Store, cat *dynaflux.Catalog, opts ...dynaflux.QueryOption) ([]pageStatsRow, error) {
	t.Helper()
	day := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	params := pageStatsParams{Page: "/home", From: day, To: day}
	return fluxgen.QueryRows(t.Context(), s, cat, "PageStats", params.fields(), (*pageStatsRow).fields, opts...)
}

// Generated code that embeds the v1.0.0 catalog appends and queries through it.
func TestCompatCatalogEmbedded(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cat := fluxgen.LoadCatalog(string(compatFixture(t, "catalog.json")))
		s := fluxtest.NewStore(t, cat)
		for _, user := range []string{"u1", "u2", "u1"} {
			ev := examplePageView{UserID: user, Page: "/home", Tags: []string{}}
			if _, err := fluxgen.AppendEvent(t.Context(), s, cat, "page_views", ev.fields()); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := pageStats(t, s, cat)
		want := []pageStatsRow{{Day: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), Views: 3, Visitors: 2}}
		if err != nil || !slices.Equal(rows, want) {
			t.Errorf("PageStats = %+v, %v; want %+v", rows, err, want)
		}
	})
}

// The operator commands read the v1.0.0 catalog as a deployed one. Decoding it also checks
// each view version's fingerprint against its definition, so a release that computed
// another fingerprint for an unchanged view fails here.
func TestCompatCatalogDeployed(t *testing.T) {
	t.Parallel()
	data := compatFixture(t, "catalog.json")
	m := meta.New(mem.New(), catalog.Naming{Prefix: "prod-", Store: "analytics"}.MetaTable())
	if _, err := m.RegisterCatalog(t.Context(), data, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	dc, err := deployed.ReadCatalog(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	def, err := catalog.Unmarshal(dc.Data)
	if err != nil {
		t.Fatal(err)
	}
	got := slices.Concat([]string{def.Store}, def.Regions, slices.Sorted(maps.Keys(maps.Collect(def.Versions()))))
	want := []string{
		"analytics", "us-east-1", "us-west-2",
		"member_latest@v1", "page_daily@v1", "quota_usage@v1", "tag_hourly@v1", "workspace_daily@v1",
	}
	if dc.Generator != "v1.0.0" || dc.Format != 1 || !slices.Equal(got, want) {
		t.Errorf("deployed catalog of %s, format %d: %q; want v1.0.0, format 1: %q", dc.Generator, dc.Format, got, want)
	}
}

// ParseToken reads every v1.0.0 token as the same text, and a wait decodes from it the
// events, their receipt flags and their stream. The engine's TestCompatTokenFormat1 checks
// each event's receipt key.
func TestCompatTokens(t *testing.T) {
	t.Parallel()
	tokens := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(compatFixture(t, "tokens.txt")))
	for sc.Scan() {
		if name, text, ok := strings.Cut(sc.Text(), " "); ok && !strings.HasPrefix(name, "#") {
			tokens[name] = text
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	// The event IDs were decoded from the fixture apart from this code.
	tests := []struct {
		name string
		want string // the error of a query that waits for the token
	}{
		{"one-event", "dynaflux: query PageStats: event 00VHNCZB00V63QXKKD6T5AR6KF was appended without a receipt: " +
			"append with dynaflux.Waitable() to wait for it"},
		{"batch", "dynaflux: query PageStats: event 5VASCAJ90WHA7EFP4QXZPV8N5G was appended without a receipt: " +
			"append with dynaflux.Waitable() to wait for it"},
		{"waitable", "dynaflux: query PageStats: wait for an append timed out: 1 of 1 events of page_views have no receipt after 1s"},
		{"batch-unordered", "dynaflux: query PageStats: event 6FHA2KH8PTSPM3JDW4D68JSPP9 was appended without a receipt: " +
			"append with dynaflux.Waitable() to wait for it"},
	}
	if len(tokens) != len(tests) {
		t.Fatalf("tokens.txt holds %d tokens, want %d", len(tokens), len(tests))
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			text := tokens[tt.name]
			tok, err := dynaflux.ParseToken(text)
			if err != nil || tok.String() != text {
				t.Fatalf("ParseToken = %q, %v; want the same text", tok, err)
			}
			synctest.Test(t, func(t *testing.T) {
				cat := fluxgen.LoadCatalog(string(compatFixture(t, "catalog.json")))
				// A new store holds no receipt.
				_, waitErr := pageStats(t, fluxtest.NewStore(t, cat), cat, dynaflux.WaitFor(tok, time.Second))
				if waitErr == nil || waitErr.Error() != tt.want {
					t.Errorf("a query that waits for it: %v, want %q", waitErr, tt.want)
				}
			})
		})
	}
}

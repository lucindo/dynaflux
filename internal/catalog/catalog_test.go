// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

func TestCatalogLookups(t *testing.T) {
	t.Parallel()
	c := fixture(t)
	if s, ok := c.Stream("api_calls"); !ok || s.GoType != "APICall" {
		t.Errorf("Stream(api_calls) = %+v, %v", s, ok)
	}
	if _, ok := c.Stream("page_daily"); ok {
		t.Error("Stream(page_daily) found a view")
	}
	v, found := c.View("page_daily")
	if !found {
		t.Fatal("View(page_daily) not found")
	}
	if cur, ok := v.Current(); !ok || cur.Number != 2 {
		t.Errorf("page_daily Current() = %+v, %v; want v2", cur, ok)
	}
	removed := catalog.View{Name: "old", Versions: []catalog.ViewVersion{pageDailyV1()}}
	if cur, ok := removed.Current(); ok {
		t.Errorf("Current() of a view with only retained versions = v%d", cur.Number)
	}
	var ids []string
	for id, vv := range c.Versions() {
		ids = append(ids, fmt.Sprintf("%s %s v%d", id, vv.Stream, vv.Number))
	}
	want := []string{"page_daily@v1 page_views v1", "page_daily@v2 page_views v2", "quota_usage@v1 api_calls v1", "tag_hourly@v1 page_views v1"}
	if !slices.Equal(ids, want) {
		t.Errorf("Versions() = %q, want %q", ids, want)
	}
	if vv, ok := c.Version("page_daily@v2"); !ok || vv.Number != 2 {
		t.Errorf("Version(page_daily@v2) = %+v, %v; want v2", vv, ok)
	}
	if _, ok := c.Version("page_daily@v3"); ok {
		t.Error("Version(page_daily@v3) found a version")
	}
}

func TestExactHandoff(t *testing.T) {
	t.Parallel()
	c := fixture(t)
	local := fixture(t)
	local.Streams[1].Archive.Events = catalog.ArchiveEventsLocal
	tests := []struct {
		name           string
		c              *catalog.Catalog
		region, stream string
		want           bool
	}{
		{"archived with events all", c, "us-west-2", "page_views", true},
		{"archived with events local", local, "us-west-2", "page_views", false},
		{"region that doesn't archive", c, "eu-west-1", "page_views", false},
		{"stream that isn't archived", c, "us-east-1", "api_calls", false},
		{"unknown stream", c, "us-east-1", "clicks", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.c.ExactHandoff(tt.region, tt.stream); got != tt.want {
				t.Errorf("ExactHandoff(%s, %s) = %v, want %v", tt.region, tt.stream, got, tt.want)
			}
		})
	}
}

func TestVersionID(t *testing.T) {
	t.Parallel()
	if got := catalog.VersionID("page_daily", 12); got != "page_daily@v12" {
		t.Errorf("VersionID = %q, want page_daily@v12", got)
	}
}

func TestStreamColumn(t *testing.T) {
	t.Parallel()
	s := pageViews()
	tests := []struct {
		name string
		want string
		ok   bool
	}{
		{"referrer", "Nullable(String)", true},
		{"tags", "Array(String)", true},
		{"event_id", "String", true},
		{"event_time", "DateTime64(6)", true},
		{"ingest_time", "DateTime64(6)", true},
		{"origin_region", "String", true},
		{"pk", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, ok := s.Column(tt.name)
			if ok != tt.ok || ok && c.Type.String() != tt.want {
				t.Errorf("Column(%q) = %s, %v; want %s, %v", tt.name, c.Type, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestFencedHandoff(t *testing.T) {
	t.Parallel()
	latest := func(fns ...catalog.AggFunc) *catalog.ViewVersion {
		v := pageDailyV2()
		v.Aggregates = nil
		for _, fn := range fns {
			v.Aggregates = append(v.Aggregates, agg(string(fn), fn, u64T()))
		}
		return &v
	}
	tests := []struct {
		name string
		v    *catalog.ViewVersion
		want bool
	}{
		{"keys only", latest(), false},
		{"idempotent only", latest(catalog.AggMin, catalog.AggMax, catalog.AggArgMax, catalog.AggArgMin,
			catalog.AggAny, catalog.AggUniq, catalog.AggUniqExact), false},
		{"count", latest(catalog.AggMax, catalog.AggCount), true},
		{"sum", latest(catalog.AggSum), true},
		{"avg", latest(catalog.AggAvg), true},
		{"quantile", latest(catalog.AggQuantile), true},
		{"quantileDD", latest(catalog.AggQuantileDD), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.v.FencedHandoff(); got != tt.want {
				t.Errorf("FencedHandoff() = %v, want %v", got, tt.want)
			}
		})
	}
}

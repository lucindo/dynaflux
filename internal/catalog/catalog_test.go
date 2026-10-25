// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"testing"
	"time"

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
}

func TestInvocationLifetime(t *testing.T) {
	t.Parallel()
	c := catalog.Catalog{TimeoutSeconds: 900}
	if got, want := c.InvocationLifetime(), 16*time.Minute; got != want {
		t.Errorf("InvocationLifetime() = %v, want %v", got, want)
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

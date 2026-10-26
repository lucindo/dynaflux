// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// The expected names are the examples in docs/spec/config.md.
func TestNaming(t *testing.T) {
	t.Parallel()
	n := catalog.Naming{Prefix: "prod-", Store: "analytics"}
	tests := []struct {
		name      string
		got, want string
	}{
		{"StreamTable", n.StreamTable("page_views"), "prod-analytics-stream-page_views"},
		{"ViewTable", n.ViewTable("page_daily", 2), "prod-analytics-view-page_daily-v2"},
		{"MetaTable", n.MetaTable(), "prod-analytics-meta"},
		{"ProjectorFunction", n.ProjectorFunction(), "prod-analytics-projector"},
		{"StreamProjectorFunction", n.StreamProjectorFunction("page_views"), "prod-analytics-projector-page_views"},
		{"SetupFunction", n.SetupFunction(), "prod-analytics-setup"},
		{"Bucket", n.Bucket("123456789012", "us-east-1"), "prod-analytics-123456789012-us-east-1-an"},
		{"FunctionLogGroup", catalog.FunctionLogGroup(n.SetupFunction()), "/aws/lambda/prod-analytics-setup"},
		{"QueryFunction", n.QueryFunction(), "prod-analytics-query"},
		{"BackfillFunction", n.BackfillFunction(), "prod-analytics-backfill"},
		{"BackfillStateMachine", n.BackfillStateMachine(), "prod-analytics-backfill"},
		{"ArchiveStream", n.ArchiveStream("page_views"), "prod-analytics-archive-page_views"},
		{"ArchiveDatabase", n.ArchiveDatabase(), "prod_analytics"},
		{"ArchiveWorkGroup", n.ArchiveWorkGroup(), "prod-analytics"},
		{"ArchiveLogGroup", n.ArchiveLogGroup(), "/aws/kinesisfirehose/prod-analytics-archive"},
		{"IcebergTable", catalog.IcebergTable("page_views"), "page_views"},
		{"DedupView", catalog.DedupView("page_views"), "page_views_dedup"},
		{"StreamTable without prefix", catalog.Naming{Store: "analytics"}.StreamTable("page_views"), "analytics-stream-page_views"},
		{"ArchiveDatabase with dashes", catalog.Naming{Prefix: "staging-eu-", Store: "web-analytics"}.ArchiveDatabase(),
			"staging_eu_web_analytics"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.got != tt.want {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
}

// config.md's budgets: P is len(prefix) + len(store), and page_views has 10 characters.
func TestResourceNameLimits(t *testing.T) {
	t.Parallel()
	c := &catalog.Catalog{Streams: []catalog.Stream{pageViews()}}
	tests := []struct {
		name       string
		p          int
		perStream  bool
		kind       catalog.ResourceKind
		wantTooBig bool
	}{
		{"projector at P = 54", 54, false, catalog.ResourceProjectorFunction, false},
		{"projector at P = 55", 55, false, catalog.ResourceProjectorFunction, true},
		{"stream projector at P + stream = 53", 43, true, catalog.ResourceProjectorFunction, false},
		{"stream projector at P + stream = 54", 44, true, catalog.ResourceProjectorFunction, true},
		{"setup function at P = 58", 58, false, catalog.ResourceSetupFunction, false},
		{"setup function at P = 59", 59, false, catalog.ResourceSetupFunction, true},
		{"query function at P = 58", 58, false, catalog.ResourceQueryFunction, false},
		{"query function at P = 59", 59, false, catalog.ResourceQueryFunction, true},
		{"Firehose at P + stream = 55", 45, false, catalog.ResourceArchiveStream, false},
		{"Firehose at P + stream = 56", 46, false, catalog.ResourceArchiveStream, true},
		{"Athena workgroup at P = 128", 128, false, catalog.ResourceArchiveWorkGroup, false},
		{"Athena workgroup at P = 129", 129, false, catalog.ResourceArchiveWorkGroup, true},
		{"state machine at P = 71", 71, false, catalog.ResourceBackfillStateMachine, false},
		{"state machine at P = 72", 72, false, catalog.ResourceBackfillStateMachine, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			n := catalog.Naming{Prefix: "p-", Store: strings.Repeat("s", tt.p-len("p-"))}
			found := false
			for _, r := range slices.Concat(n.StoreResources(tt.perStream), n.CatalogResources(c, tt.perStream)) {
				if r.Kind != tt.kind {
					continue
				}
				found = true
				if r.TooLong() != tt.wantTooBig {
					t.Errorf("%s (%d characters, limit %d): TooLong() = %v, want %v",
						r.Name, len(r.Name), r.Kind.MaxLen(), r.TooLong(), tt.wantTooBig)
				}
			}
			if !found {
				t.Errorf("Resources has no %s", tt.kind)
			}
		})
	}
}

func TestStoreResources(t *testing.T) {
	t.Parallel()
	n := catalog.Naming{Prefix: "prod-", Store: "analytics"}
	store := []catalog.Resource{
		{Kind: catalog.ResourceMetaTable, Name: "prod-analytics-meta"},
		{Kind: catalog.ResourceSetupFunction, Name: "prod-analytics-setup"},
		{Kind: catalog.ResourceQueryFunction, Name: "prod-analytics-query"},
		{Kind: catalog.ResourceBackfillFunction, Name: "prod-analytics-backfill"},
		{Kind: catalog.ResourceBackfillStateMachine, Name: "prod-analytics-backfill"},
		{Kind: catalog.ResourceArchiveDatabase, Name: "prod_analytics"},
		{Kind: catalog.ResourceArchiveWorkGroup, Name: "prod-analytics"},
	}
	tests := []struct {
		name      string
		perStream bool
		want      []catalog.Resource
	}{
		{"split store", false, append(slices.Clone(store),
			catalog.Resource{Kind: catalog.ResourceProjectorFunction, Name: "prod-analytics-projector"})},
		{"split stream", true, store},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := n.StoreResources(tt.perStream); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StoreResources:\n got %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestCatalogResources(t *testing.T) {
	t.Parallel()
	n := catalog.Naming{Prefix: "prod-", Store: "analytics"}
	tests := []struct {
		name      string
		perStream bool
		want      []catalog.Resource
	}{
		{"split store", false, []catalog.Resource{
			{Kind: catalog.ResourceStreamTable, Owner: "api_calls", Name: "prod-analytics-stream-api_calls"},
			{Kind: catalog.ResourceStreamTable, Owner: "page_views", Name: "prod-analytics-stream-page_views"},
			{Kind: catalog.ResourceArchiveStream, Owner: "page_views", Name: "prod-analytics-archive-page_views"},
			{Kind: catalog.ResourceIcebergTable, Owner: "page_views", Name: "page_views"},
			{Kind: catalog.ResourceDedupView, Owner: "page_views", Name: "page_views_dedup"},
			{Kind: catalog.ResourceViewTable, Owner: "page_daily", Name: "prod-analytics-view-page_daily-v1"},
			{Kind: catalog.ResourceViewTable, Owner: "page_daily", Name: "prod-analytics-view-page_daily-v2"},
			{Kind: catalog.ResourceViewTable, Owner: "quota_usage", Name: "prod-analytics-view-quota_usage-v1"},
			{Kind: catalog.ResourceViewTable, Owner: "tag_hourly", Name: "prod-analytics-view-tag_hourly-v1"},
		}},
		{"split stream", true, []catalog.Resource{
			{Kind: catalog.ResourceStreamTable, Owner: "api_calls", Name: "prod-analytics-stream-api_calls"},
			{Kind: catalog.ResourceProjectorFunction, Owner: "api_calls", Name: "prod-analytics-projector-api_calls"},
			{Kind: catalog.ResourceStreamTable, Owner: "page_views", Name: "prod-analytics-stream-page_views"},
			{Kind: catalog.ResourceProjectorFunction, Owner: "page_views", Name: "prod-analytics-projector-page_views"},
			{Kind: catalog.ResourceArchiveStream, Owner: "page_views", Name: "prod-analytics-archive-page_views"},
			{Kind: catalog.ResourceIcebergTable, Owner: "page_views", Name: "page_views"},
			{Kind: catalog.ResourceDedupView, Owner: "page_views", Name: "page_views_dedup"},
			{Kind: catalog.ResourceViewTable, Owner: "page_daily", Name: "prod-analytics-view-page_daily-v1"},
			{Kind: catalog.ResourceViewTable, Owner: "page_daily", Name: "prod-analytics-view-page_daily-v2"},
			{Kind: catalog.ResourceViewTable, Owner: "quota_usage", Name: "prod-analytics-view-quota_usage-v1"},
			{Kind: catalog.ResourceViewTable, Owner: "tag_hourly", Name: "prod-analytics-view-tag_hourly-v1"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := n.CatalogResources(fixture(t), tt.perStream)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CatalogResources:\n got %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

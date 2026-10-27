// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// TestCompareStreams covers every rule of sql.md §8.9, comparing a changed page_views with
// the deployed one.
func TestCompareStreams(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(s *catalog.Stream)
		want   []string // kind, then column or region
	}{
		{"unchanged", func(*catalog.Stream) {}, nil},
		{"new Nullable column", func(s *catalog.Stream) {
			s.Columns = append(s.Columns, catalog.Column{Name: "os", Type: catalog.Type{Kind: catalog.KindString, Nullable: true}})
		}, []string{"column added os"}},
		{"new column with a DEFAULT", func(s *catalog.Stream) {
			s.Columns = append(s.Columns, catalog.Column{Name: "lang", Type: strT(), Default: new(lit(strT(), "en"))})
		}, []string{"column added lang"}},
		{"new column without a DEFAULT", func(s *catalog.Stream) {
			s.Columns = append(s.Columns, catalog.Column{Name: "lang", Type: strT()})
		}, []string{"column required lang"}},
		{"removed column", func(s *catalog.Stream) { s.Columns = slices.Delete(s.Columns, 4, 5) },
			[]string{"column removed duration_ms"}},
		{"renamed column", func(s *catalog.Stream) { s.Columns[4].Name = "duration" },
			[]string{"column removed duration_ms", "column required duration"}},
		{"retyped column", func(s *catalog.Stream) { s.Columns[4].Type = u64T() }, []string{"column retyped duration_ms"}},
		{"column made Nullable", func(s *catalog.Stream) { s.Columns[1].Type.Nullable = true }, []string{"column retyped page"}},
		{"changed DEFAULT", func(s *catalog.Stream) { s.Columns[5].Default = new(lit(strT(), "US")) },
			[]string{"default changed country"}},
		{"DEFAULT removed", func(s *catalog.Stream) { s.Columns[5].Default = nil }, []string{"default changed country"}},
		{"array DEFAULT given an element", func(s *catalog.Stream) {
			s.Columns[3].Default = new(call(catalog.OpArray, s.Columns[3].Type, lit(strT(), "x")))
		}, []string{"default changed tags"}},
		{"array DEFAULT spelled with empty arguments", func(s *catalog.Stream) { s.Columns[3].Default.Args = []catalog.Expr{} }, nil},
		{"columns reordered", func(s *catalog.Stream) { slices.Reverse(s.Columns) }, nil},
		{"PRIMARY KEY", func(s *catalog.Stream) { s.PrimaryKey = []string{"user_id", "page"} }, []string{"primary key changed"}},
		{"DEDUP BY", func(s *catalog.Stream) { s.DedupBy = []string{"page"} }, []string{"dedup by changed"}},
		{"SHARDS", func(s *catalog.Stream) { s.Shards = 2 }, []string{"shards changed"}},
		{"LOG RETENTION", func(s *catalog.Stream) { s.LogRetention = catalog.RetainForever() }, []string{"log retention changed"}},
		{"ACCEPT past bound", func(s *catalog.Stream) { s.Accept.Past = new(interval(7, catalog.UnitDay)) },
			[]string{"accept changed"}},
		{"ACCEPT without a past bound", func(s *catalog.Stream) { s.Accept.Past = nil }, []string{"accept changed"}},
		{"ACCEPT future bound", func(s *catalog.Stream) { s.Accept.Future = interval(1, catalog.UnitMinute) },
			[]string{"accept changed"}},
		{"go:type", func(s *catalog.Stream) { s.GoType = "PageView" }, []string{"go type changed"}},
		{"archive regions listed in another order", func(s *catalog.Stream) {
			s.Archive = &catalog.Archive{Regions: []string{"us-west-2", "us-east-1"}, Events: catalog.ArchiveEventsAll}
		}, nil},
		{"archive in a new region", func(s *catalog.Stream) {
			s.Archive = &catalog.Archive{Regions: []string{"eu-west-1", "us-east-1", "us-west-2"}, Events: catalog.ArchiveEventsAll}
		}, []string{"archive changed"}},
		{"archive stopped in one region", func(s *catalog.Stream) {
			s.Archive = &catalog.Archive{Regions: []string{"us-west-2"}, Events: catalog.ArchiveEventsAll}
		}, []string{"archive changed", "archive stopped us-east-1"}},
		{"archive of local events only", func(s *catalog.Stream) { s.Archive = &catalog.Archive{Regions: s.Archive.Regions} },
			[]string{"archive changed", "archive stopped us-east-1", "archive stopped us-west-2"}},
		{"archive turned off", func(s *catalog.Stream) { s.Archive = nil },
			[]string{"archive changed", "archive stopped us-east-1", "archive stopped us-west-2"}},
		{"several changes, in order", func(s *catalog.Stream) {
			s.Shards = 2
			s.GoType = "PageView"
			s.Columns = append(s.Columns[1:], catalog.Column{Name: "os", Type: catalog.Type{Kind: catalog.KindString, Nullable: true}})
			s.Columns[0].Type = catalog.Type{Kind: catalog.KindString, Nullable: true}
		}, []string{"shards changed", "column removed user_id", "column retyped page", "column added os", "go type changed"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			deployed, local := pageViews(), pageViews()
			tt.change(&local)
			if got := changeList(catalog.CompareStreams(&deployed, &local)); !slices.Equal(got, tt.want) {
				t.Errorf("CompareStreams = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCompareStreamsAddedAndDropped(t *testing.T) {
	t.Parallel()
	s := pageViews()
	if got := changeList(catalog.CompareStreams(nil, &s)); !slices.Equal(got, []string{"stream added"}) {
		t.Errorf("new stream: %q", got)
	}
	if got := changeList(catalog.CompareStreams(&s, nil)); !slices.Equal(got, []string{"stream dropped"}) {
		t.Errorf("dropped stream: %q", got)
	}
}

func changeList(changes []catalog.StreamChange) []string {
	var out []string
	for _, c := range changes {
		out = append(out, strings.TrimSpace(string(c.Kind)+" "+c.Column+c.Region))
	}
	return out
}

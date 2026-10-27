// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain_test

import (
	"math"
	"os"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/explain"
)

func loadFile(t *testing.T, path string) (*config.Config, *catalog.Catalog) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return load(t, data)
}

func TestExplainRejects(t *testing.T) {
	t.Parallel()
	cfg, cat := loadFile(t, "testdata/reports/two_days.txtar")
	tests := []struct {
		name   string
		change func(*explain.Workload)
		target string
		want   string
	}{
		{"unknown name", nil, "pageviews", "no stream, view or query named pageviews; did you mean page_views?"},
		{"no events", func(w *explain.Workload) { w.Rate = 0 }, "page_views", "-rate"},
		{
			"events past the most", func(w *explain.Workload) { w.Rate = 1e7 + 1 }, "page_views",
			"-rate must be over 0 and at most 10,000,000 events per second, not 1.0000001e+07",
		},
		{"NaN event size", func(w *explain.Workload) { w.EventKB = math.NaN() }, "page_views", "-event-kb"},
		{"event over the item limit", func(w *explain.Workload) { w.EventKB = 401 }, "page_views", "-event-kb"},
		{"no horizon", func(w *explain.Workload) { w.Horizon = 0 }, "page_views", "-horizon"},
		{
			"rows of an unknown view", func(w *explain.Workload) { w.Rows = map[string]float64{"page_dayly": 5} }, "page_views",
			"-rows page_dayly=5: no view named page_dayly; did you mean page_daily?",
		},
		{
			"rows of a per-entity view", func(w *explain.Workload) { w.Rows = map[string]float64{"user_latest": 5} }, "page_views",
			"-rows user_latest=5: the view has a row per entity",
		},
		{"less than a row", func(w *explain.Workload) { w.Rows = map[string]float64{"page_daily": 0.5} }, "page_views", "at least 1"},
		{
			"new values without uniqExact", func(w *explain.Workload) { w.UniqNew = map[string]float64{"page_daily": 0.5} }, "page_views",
			"-uniq-new page_daily=0.5: the view has no uniqExact aggregate",
		},
		{"new values over 1", func(w *explain.Workload) { w.UniqNew = map[string]float64{"user_daily": 1.5} }, "page_views", "from 0 to 1"},
		{
			"source rows of a view that keeps every event", func(w *explain.Workload) { w.Pass = map[string]float64{"user_daily": 0.5} },
			"page_views", "-pass user_daily=0.5: the view has no WHERE or ARRAY JOIN, so each event is one source row",
		},
		// A flag is checked whatever the report is on.
		{"flag for another view", func(w *explain.Workload) { w.Pass = map[string]float64{"nope": 1} }, "LastPage", "-pass nope=1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := explain.DefaultWorkload()
			if tt.change != nil {
				tt.change(&w)
			}
			_, err := explain.Explain(cfg, cat, w, tt.target)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Explain error = %v, want one with %q", err, tt.want)
			}
		})
	}
}

// -pass is at most 1 for a view with WHERE, which can only drop events, and at most the
// array cap with ARRAY JOIN.
func TestExplainRejectsSourceRows(t *testing.T) {
	t.Parallel()
	cfg, cat := loadFile(t, "testdata/reports/features.txtar") // store_hourly has WHERE, item_daily ARRAY JOIN
	const (
		where     = "the source rows per event must be over 0 and at most 1 without ARRAY JOIN"
		arrayJoin = "the source rows per event must be over 0 and at most 1000, the most elements an array holds"
	)
	tests := []struct {
		name string
		pass map[string]float64
		want string // the error, or "" for none
	}{
		{"none", map[string]float64{"store_hourly": 0}, "-pass store_hourly=0: " + where},
		{"every event", map[string]float64{"store_hourly": 1}, ""},
		{"WHERE adding rows", map[string]float64{"store_hourly": 1.5}, "-pass store_hourly=1.5: " + where},
		{"full arrays", map[string]float64{"item_daily": 1000}, ""},
		{"past the array cap", map[string]float64{"item_daily": 1001}, "-pass item_daily=1001: " + arrayJoin},
		{"infinite", map[string]float64{"item_daily": math.Inf(1)}, "-pass item_daily=+Inf: " + arrayJoin},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := explain.DefaultWorkload()
			w.Pass = tt.pass
			_, err := explain.Explain(cfg, cat, w, "orders")
			switch {
			case tt.want == "" && err != nil:
				t.Errorf("Explain error = %v, want none", err)
			case tt.want != "" && (err == nil || err.Error() != tt.want):
				t.Errorf("Explain error = %v, want %q", err, tt.want)
			}
		})
	}
}

// One active row takes every event's write: in scenario B, ~869 invocations a second each
// write the row once, and every one of the ~3,500 shards leaves a mark in it, which makes it
// ~82 KB, or 83 write units a write.
func TestExplainOneActiveRow(t *testing.T) {
	t.Parallel()
	cfg, cat := loadFile(t, "testdata/reports/one_year.txtar")
	w := explain.DefaultWorkload()
	w.Rows = map[string]float64{"page_daily": 1}
	r, err := explain.Explain(cfg, cat, w, "page_daily")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"-rows page_daily=1\n",
		"warning page_daily: each row takes ~79,319 write",
		"read units per second, over the 1,000 and 3,000 that a DynamoDB partition takes",
	} {
		if !strings.Contains(r.Text, want) {
			t.Errorf("the report lacks %q:\n%s", want, r.Text)
		}
	}
}

// Counts past the int64 range print in full instead of wrapping around.
func TestExplainManyActiveRows(t *testing.T) {
	t.Parallel()
	cfg, cat := loadFile(t, "testdata/reports/two_days.txtar")
	w := explain.DefaultWorkload()
	w.Rows = map[string]float64{"page_daily": 1e20}
	r, err := explain.Explain(cfg, cat, w, "page_daily")
	if err != nil {
		t.Fatal(err)
	}
	if want := "so any shard can write any row; ~100,000,000,000,000,000,000 active\n"; !strings.Contains(r.Text, want) {
		t.Errorf("the report lacks %q:\n%s", want, r.Text)
	}
}

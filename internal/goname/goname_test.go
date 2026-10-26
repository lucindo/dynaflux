// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package goname_test

import (
	"testing"

	"github.com/lucindo/dynaflux/internal/goname"
)

func TestField(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, want string }{
		{"user_id", "UserID"},
		{"duration_ms", "DurationMS"},
		{"api_url", "APIURL"},
		{"page", "Page"},
		{"a1_b2", "A1B2"},
		{"utf8_text", "UTF8Text"},
		// Empty parts vanish, so these collide with the implicit columns' fields.
		{"event__time", "EventTime"},
		{"event_time_", "EventTime"},
		{"idx", "Idx"},
	}
	for _, tt := range tests {
		if got := goname.Field(tt.name); got != tt.want {
			t.Errorf("Field(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestSingular(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, want string }{
		{"page_views", "page_view"},
		{"categories", "category"},
		{"addresses", "address"},
		{"wishes", "wish"},
		{"matches", "match"},
		{"boxes", "box"},
		{"buzzes", "buzz"},
		{"access", "access"},
		{"status", "status"},
		{"api_analysis", "api_analysis"},
		{"user_events", "user_event"},
		{"member_data", "member_data"},
		// A lone s is no plural ending: removing it would leave no word.
		{"s", "s"},
		{"x_s", "x_s"},
	}
	for _, tt := range tests {
		if got := goname.Singular(tt.name); got != tt.want {
			t.Errorf("Singular(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestForStream(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, goType string
		want         goname.Stream
	}{
		{"page_views", "", goname.Stream{
			Event: "PageView", Key: "PageViewsKey", Append: "AppendPageView", AppendBatch: "AppendPageViewBatch",
			Read: "ReadPageViews", Import: "ImportPageViews",
		}},
		{"api_calls", "", goname.Stream{
			Event: "APICall", Key: "APICallsKey", Append: "AppendAPICall", AppendBatch: "AppendAPICallBatch",
			Read: "ReadAPICalls", Import: "ImportAPICalls",
		}},
		// -- go:type names the event type only; the stream's own name stays in the rest.
		{"visits", "Hit", goname.Stream{
			Event: "Hit", Key: "VisitsKey", Append: "AppendHit", AppendBatch: "AppendHitBatch",
			Read: "ReadVisits", Import: "ImportVisits",
		}},
	}
	for _, tt := range tests {
		if got := goname.ForStream(tt.name, tt.goType); got != tt.want {
			t.Errorf("ForStream(%q, %q) = %+v, want %+v", tt.name, tt.goType, got, tt.want)
		}
	}
}

func TestForQuery(t *testing.T) {
	t.Parallel()
	want := goname.Query{Method: "PageStats", Params: "PageStatsParams", Row: "PageStatsRow"}
	if got := goname.ForQuery("PageStats"); got != want {
		t.Errorf("ForQuery(PageStats) = %+v, want %+v", got, want)
	}
}

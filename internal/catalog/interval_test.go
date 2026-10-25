// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

func TestIntervalText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		text string
		want catalog.Interval
		ok   bool
	}{
		{"5 MINUTE", interval(5, catalog.UnitMinute), true},
		{"0 SECOND", interval(0, catalog.UnitSecond), true},
		{"400 QUARTER", interval(400, catalog.UnitQuarter), true},
		{"+5 DAY", catalog.Interval{}, false},
		{"05 DAY", catalog.Interval{}, false},
		{"-1 DAY", catalog.Interval{}, false},
		{"5 day", catalog.Interval{}, false},
		{"5 DAYS", catalog.Interval{}, false},
		{"5  DAY", catalog.Interval{}, false},
		{"5DAY", catalog.Interval{}, false},
		{"DAY", catalog.Interval{}, false},
		{"", catalog.Interval{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			t.Parallel()
			var got catalog.Interval
			err := got.UnmarshalText([]byte(tt.text))
			if (err == nil) != tt.ok || got != tt.want {
				t.Fatalf("UnmarshalText(%q) = %+v, %v; want %+v, ok %v", tt.text, got, err, tt.want, tt.ok)
			}
			if !tt.ok {
				return
			}
			if text, merr := got.MarshalText(); merr != nil || string(text) != tt.text {
				t.Errorf("MarshalText = %q, %v; want %q", text, merr, tt.text)
			}
		})
	}
}

func TestRetentionPeriod(t *testing.T) {
	t.Parallel()
	if p, ok := catalog.RetainFor(interval(5, catalog.UnitDay)).Period(); !ok || p != interval(5, catalog.UnitDay) {
		t.Errorf("RetainFor(5 DAY).Period() = %+v, %v; want 5 DAY, true", p, ok)
	}
	if _, ok := catalog.RetainForever().Period(); ok {
		t.Error("RetainForever().Period() has a period")
	}
}

// LOG RETENTION takes a day to 100 years, in hours, days, weeks, months or years, or FOREVER.
func TestRetentionText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		text string
		ok   bool
	}{
		{"FOREVER", true},
		{"24 HOUR", true},
		{"36 HOUR", true},
		{"876600 HOUR", true},
		{"1 DAY", true},
		{"36525 DAY", true},
		{"1 WEEK", true},
		{"5217 WEEK", true},
		{"1 MONTH", true},
		{"1200 MONTH", true},
		{"1 YEAR", true},
		{"100 YEAR", true},
		{"1 HOUR", false},
		{"23 HOUR", false},
		{"876601 HOUR", false},
		{"0 DAY", false},
		{"36526 DAY", false},
		{"5218 WEEK", false},
		{"1201 MONTH", false},
		{"101 YEAR", false},
		{"90 MINUTE", false},
		{"3600 SECOND", false},
		{"1 QUARTER", false},
		{"forever", false},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			t.Parallel()
			var r catalog.Retention
			if err := r.UnmarshalText([]byte(tt.text)); (err == nil) != tt.ok {
				t.Fatalf("UnmarshalText(%q) error = %v, want ok %v", tt.text, err, tt.ok)
			}
			if !tt.ok {
				return
			}
			if text, err := r.MarshalText(); err != nil || string(text) != tt.text {
				t.Errorf("MarshalText = %q, %v; want %q", text, err, tt.text)
			}
		})
	}
	if text, err := catalog.RetainFor(interval(1, catalog.UnitQuarter)).MarshalText(); err == nil {
		t.Errorf("MarshalText(1 QUARTER) = %q, want an error", text)
	}
}

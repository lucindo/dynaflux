// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
)

// The watermark passes the cut when a datapoint's period start, less its freshness, is past
// the cut and the clock margin; or, past 2 h 30 min, when Firehose is empty now and the metric
// is the stream's (ADR 0016 §2).
func TestDelivered(t *testing.T) {
	t.Parallel()
	cut := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	at := cut.Add
	point := func(start, freshness time.Duration) backfill.Datapoint {
		return backfill.Datapoint{At: at(start), Freshness: freshness}
	}
	tests := []struct {
		name   string
		points []backfill.Datapoint
		now    time.Duration // after the cut
		quiet  bool          // without the evidence that the metric is the stream's
		want   bool
	}{
		{"past the margin", []backfill.Datapoint{point(5*time.Minute, 4*time.Minute-time.Second)}, 6 * time.Minute, false, true},
		{"at the margin", []backfill.Datapoint{point(5*time.Minute, 4*time.Minute)}, 6 * time.Minute, false, false},
		// The period's end, less its freshness, is past the margin: not its start.
		{"by the period's end only", []backfill.Datapoint{point(2*time.Minute, 90*time.Second)}, 3 * time.Minute, false, false},
		{"5-minute data", []backfill.Datapoint{point(10*time.Minute, 5*time.Minute)}, 15 * time.Minute, false, true},
		{"no datapoint, before the idle wait", nil, 2*time.Hour + 29*time.Minute, false, false},
		{"no datapoint, after it", nil, 2*time.Hour + 31*time.Minute, false, true},
		// A record that arrived just before the cut left long ago: Firehose is empty now.
		{"a record before the cut, then silence", []backfill.Datapoint{point(0, 2*time.Minute)}, 2*time.Hour + 31*time.Minute, false, true},
		{"a datapoint 6 minutes ago", []backfill.Datapoint{point(2*time.Hour+25*time.Minute, 3*time.Hour)}, 2*time.Hour + 31*time.Minute, false, false},
		{"a datapoint 4 minutes ago", []backfill.Datapoint{point(2*time.Hour+27*time.Minute, 3*time.Hour)}, 2*time.Hour + 31*time.Minute, false, true},
		{"a datapoint 21 minutes ago", []backfill.Datapoint{point(2*time.Hour+10*time.Minute, 3*time.Hour)}, 2*time.Hour + 31*time.Minute, false, true},
		{"no evidence that the metric is the stream's", nil, 10 * time.Hour, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := backfill.Freshness{Points: tt.points, Active: true, Listed: !tt.quiet}
			if got := backfill.Delivered(f, cut, at(tt.now)); got != tt.want {
				t.Errorf("Delivered = %v, want %v", got, tt.want)
			}
		})
	}
	t.Run("a stream that isn't ACTIVE", func(t *testing.T) {
		t.Parallel()
		if backfill.Delivered(backfill.Freshness{Listed: true}, cut, at(10*time.Hour)) {
			t.Error("Delivered = true, want false")
		}
	})
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// dayOf returns the start of the d-th day of 2026.
func dayOf(d int) time.Time { return time.Date(2026, 1, 1+d, 0, 0, 0, 0, time.UTC) }

// sliceText describes s as "from-to records/bytes", in hours from dayOf(0) and gigabytes, with
// "*" when it is whole.
func sliceText(s slice) string {
	h := func(t time.Time) int { return int(t.Sub(dayOf(0)) / time.Hour) }
	whole := ""
	if s.Whole {
		whole = "*"
	}
	return fmt.Sprintf("%d-%d %d/%dG%s", h(s.From), h(s.To), s.Records, s.Bytes/1e9, whole)
}

// Consecutive partitions join a slice up to 25 GB, whatever lies between them; a larger one is
// a slice alone.
func TestPlanSlices(t *testing.T) {
	t.Parallel()
	part := func(d int, gb int64) Partition {
		return Partition{From: dayOf(d), To: dayOf(d + 1), Records: 10, Bytes: gb * 1e9}
	}
	tests := []struct {
		name  string
		parts []Partition
		want  []string
	}{
		{"none", nil, nil},
		{"up to the target", []Partition{part(0, 10), part(1, 10), part(2, 5), part(3, 1)}, []string{"0-72 30/25G*", "72-96 10/1G*"}},
		{"in order of their days", []Partition{part(2, 10), part(0, 10), part(1, 10)}, []string{"0-48 20/20G*", "48-72 10/10G*"}},
		{"a larger day alone", []Partition{part(0, 5), part(1, 30), part(2, 5)}, []string{"0-24 10/5G*", "24-48 10/30G*", "48-72 10/5G*"}},
		{"across days without records", []Partition{part(0, 5), part(3, 5)}, []string{"0-96 20/10G*"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got []string
			for _, s := range planSlices(tt.parts) {
				got = append(got, sliceText(s))
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("slices %q, want %q", got, tt.want)
			}
		})
	}
}

// A slice halves after whole hours, the most up to half its length, down to an hour, with its
// bytes in proportion; the halves aren't whole, so no record count holds for them.
func TestHalves(t *testing.T) {
	t.Parallel()
	tests := []struct {
		hours time.Duration
		want  []string
	}{
		{24 * time.Hour, []string{"0-12 0/5G", "12-24 0/5G"}},
		{3 * time.Hour, []string{"0-1 0/3G", "1-3 0/6G"}},
		{2 * time.Hour, []string{"0-1 0/5G", "1-2 0/5G"}},
		{2*time.Hour - time.Minute, nil},
		{time.Hour, nil},
	}
	for _, tt := range tests {
		s := slice{From: dayOf(0), To: dayOf(0).Add(tt.hours), Records: 10, Bytes: 10e9, Whole: true}
		first, second, ok := halves(s)
		var got []string
		if ok {
			got = []string{sliceText(first), sliceText(second)}
		}
		if !slices.Equal(got, tt.want) {
			t.Errorf("halves of %v: %q, want %q", tt.hours, got, tt.want)
		}
	}
}

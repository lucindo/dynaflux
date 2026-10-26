// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/infra"
)

// CloudWatch's alarm widget lists at most 100 alarms. 15 archived streams, each with its
// own projector, have 15 × 9 + 2 = 137.
func TestDashboardSplitsAlarms(t *testing.T) {
	t.Parallel()
	var sql strings.Builder
	for s := range 15 {
		fmt.Fprintf(&sql, "CREATE STREAM s%d (k String) PRIMARY KEY (k);\n", s)
	}
	m, err := buildWith(t, "store: analytics\nregions: [us-east-1]\nruntime: {split: stream}\n", sql.String(), infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Widgets []struct {
			Type       string `json:"type"`
			Properties struct {
				Alarms []string `json:"alarms"`
			} `json:"properties"`
		} `json:"widgets"`
	}
	if err = json.Unmarshal([]byte(m.Regional.Dashboards[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	var sizes []int
	for _, w := range body.Widgets {
		if w.Type == "alarm" {
			sizes = append(sizes, len(w.Properties.Alarms))
		}
	}
	if want := []int{100, 37}; !slices.Equal(sizes, want) {
		t.Errorf("alarm widgets list %v alarms, want %v", sizes, want)
	}
}

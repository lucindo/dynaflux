// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr_test

import (
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

func TestAddInterval(t *testing.T) {
	t.Parallel()
	const layout = "2006-01-02 15:04:05.000000"
	cases := []struct {
		from string
		n    int
		unit catalog.Unit
		want string
	}{
		{"2024-01-31 00:00:00.000000", 1, catalog.UnitMonth, "2024-02-29 00:00:00.000000"},
		{"2023-01-31 00:00:00.000000", 1, catalog.UnitMonth, "2023-02-28 00:00:00.000000"},
		{"2024-03-31 00:00:00.000000", -1, catalog.UnitMonth, "2024-02-29 00:00:00.000000"},
		{"2024-11-30 00:00:00.000000", 3, catalog.UnitMonth, "2025-02-28 00:00:00.000000"},
		{"2024-01-31 13:45:00.250123", 1, catalog.UnitMonth, "2024-02-29 13:45:00.250123"},
		{"2024-01-15 08:00:00.000000", 1, catalog.UnitMonth, "2024-02-15 08:00:00.000000"},
		{"2024-05-31 00:00:00.000000", 1, catalog.UnitQuarter, "2024-08-31 00:00:00.000000"},
		{"2024-03-31 00:00:00.000000", 1, catalog.UnitQuarter, "2024-06-30 00:00:00.000000"},
		{"2024-02-29 12:00:00.000000", 1, catalog.UnitYear, "2025-02-28 12:00:00.000000"},
		{"2024-02-29 12:00:00.000000", 4, catalog.UnitYear, "2028-02-29 12:00:00.000000"},
		{"2024-02-29 12:00:00.000000", -100, catalog.UnitYear, "1924-02-29 12:00:00.000000"},
		{"2024-12-31 23:59:59.000000", 1, catalog.UnitSecond, "2025-01-01 00:00:00.000000"},
		{"2024-05-31 13:58:00.000000", 5, catalog.UnitMinute, "2024-05-31 14:03:00.000000"},
		{"2024-05-31 13:00:00.000000", -2, catalog.UnitHour, "2024-05-31 11:00:00.000000"},
		{"2024-03-10 01:30:00.000000", 1, catalog.UnitDay, "2024-03-11 01:30:00.000000"},
		{"2024-05-31 00:00:00.000000", 1, catalog.UnitWeek, "2024-06-07 00:00:00.000000"},
		{"2024-05-31 00:00:00.000000", 0, catalog.UnitMonth, "2024-05-31 00:00:00.000000"},
	}
	for _, c := range cases {
		from, err := time.Parse(layout, c.from)
		if err != nil {
			t.Fatal(err)
		}
		got, err := expr.AddInterval(from.UnixMicro(), catalog.Interval{N: c.n, Unit: c.unit})
		if err != nil {
			t.Errorf("%s + %d %s: %v", c.from, c.n, c.unit, err)
			continue
		}
		if s := time.UnixMicro(got).UTC().Format(layout); s != c.want {
			t.Errorf("%s + %d %s = %s, want %s", c.from, c.n, c.unit, s, c.want)
		}
	}
}

func TestAddIntervalRejectsAnUnknownUnit(t *testing.T) {
	t.Parallel()
	if got, err := expr.AddInterval(0, catalog.Interval{N: 1, Unit: "FORTNIGHT"}); err == nil {
		t.Fatalf("got %d, want an error", got)
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package analytics_test

import (
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/examples/analytics"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/fluxtest"
)

func TestRecordPageViewCountsIt(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		record := func(view flux.PageView, want flux.PageStatsRow) {
			t.Helper()
			got, err := analytics.RecordPageView(t.Context(), c, view, time.Second)
			if err != nil || !reflect.DeepEqual(got, []flux.PageStatsRow{want}) {
				t.Errorf("RecordPageView(%+v) = %+v, %v; want %+v", view, got, err, want)
			}
		}
		// No EventTime: the view happened at the call.
		record(flux.PageView{UserID: "u1", Page: "/home"}, flux.PageStatsRow{Day: start, Views: 1, Visitors: 1})
		time.Sleep(5 * time.Hour)
		record(flux.PageView{UserID: "u2", Page: "/home"}, flux.PageStatsRow{Day: start, Views: 2, Visitors: 2})
		// The day before counts only its own views.
		record(flux.PageView{UserID: "u1", Page: "/home", EventTime: start.Add(-time.Hour)},
			flux.PageStatsRow{Day: start.Add(-24 * time.Hour), Views: 1, Visitors: 1})
	})
}

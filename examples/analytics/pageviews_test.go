// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package analytics_test

import (
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/fluxtest"
)

// start is where the fake clock of a synctest bubble starts.
var start = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) //nolint:gochecknoglobals // a constant time.Time

func readPageViews(t *testing.T, c *flux.Client, user string, opts ...dynaflux.ReadOption) []flux.PageView {
	t.Helper()
	var evs []flux.PageView
	for ev, err := range c.ReadPageViews(t.Context(), flux.PageViewsKey{UserID: user}, opts...) {
		if err != nil {
			t.Fatal(err)
		}
		evs = append(evs, ev)
	}
	return evs
}

func TestReadPageViews(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		_, err := c.AppendPageViewBatch(t.Context(), []flux.PageView{
			{UserID: "u1", Page: "/pricing", DurationMS: 200, EventTime: start.Add(-time.Hour)},
			{UserID: "u1", Page: "/home", DurationMS: 100, Tags: []string{"landing"}, EventTime: start.Add(-2 * time.Hour)},
			{UserID: "u2", Page: "/home", DurationMS: 300, EventTime: start.Add(-time.Hour)},
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		// No EventTime: the event happened when it is appended.
		_, err = c.AppendPageView(t.Context(), flux.PageView{UserID: "u1", Page: "/signup", Referrer: new("https://example.com")})
		if err != nil {
			t.Fatal(err)
		}
		got := readPageViews(t, c, "u1", dynaflux.Since(start.Add(-time.Hour)))
		// fluxtest's event IDs repeat in every run: ULIDs of the bubble's clock and a fixed
		// random source.
		want := []flux.PageView{
			{
				UserID: "u1", Page: "/pricing", DurationMS: 200, EventTime: start.Add(-time.Hour),
				EventID: "00VHNCZB00V63QXKKD6T5AR6KF", IngestTime: start, OriginRegion: "local",
			},
			{
				UserID: "u1", Page: "/signup", Referrer: new("https://example.com"), EventTime: start.Add(time.Second),
				EventID: "00VHNCZBZ886FCC9Y7DCDZP7X3", IngestTime: start.Add(time.Second), OriginRegion: "local",
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("read\n%+v\nwant\n%+v", got, want)
		}
	})
}

func TestAppendPageViewOutsideWindow(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		old := flux.PageView{UserID: "u1", Page: "/home", EventTime: start.Add(-31 * 24 * time.Hour)}
		if _, err := c.AppendPageView(t.Context(), old); !errors.Is(err, dynaflux.ErrOutsideWindow) {
			t.Errorf("AppendPageView of an event 31 days old: got %v, want ErrOutsideWindow", err)
		}
		if err := c.ImportPageViews(t.Context(), []flux.PageView{old}); err != nil {
			t.Errorf("ImportPageViews of an event 31 days old: %v", err)
		}
	})
}

func TestPageViewsExpireAfterLogRetention(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		if _, err := c.AppendPageView(t.Context(), flux.PageView{UserID: "u1", Page: "/home"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * 24 * time.Hour)
		if evs := readPageViews(t, c, "u1"); len(evs) != 0 {
			t.Errorf("read %d events 2 days after ingestion, want 0", len(evs))
		}
	})
}

func TestPageStats(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		day1, day2 := start.Add(-48*time.Hour), start.Add(-24*time.Hour)
		_, err := c.AppendPageViewBatch(t.Context(), []flux.PageView{
			{UserID: "u1", Page: "/home", DurationMS: 100, EventTime: day1.Add(10 * time.Hour)},
			{UserID: "u2", Page: "/home", DurationMS: 200, EventTime: day1.Add(11 * time.Hour)},
			{UserID: "u1", Page: "/home", DurationMS: 300, EventTime: day1.Add(12 * time.Hour)},
			{UserID: "u1", Page: "/home", DurationMS: 400, EventTime: day2.Add(9 * time.Hour)},
			{UserID: "u3", Page: "/pricing", DurationMS: 500, EventTime: day1.Add(9 * time.Hour)},
			// WHERE page != '' leaves this one out of the view.
			{UserID: "u4", Page: "", DurationMS: 600, EventTime: day1.Add(9 * time.Hour)},
		})
		if err != nil {
			t.Fatal(err)
		}
		// A Date parameter is the UTC date of the time given.
		got, err := c.PageStats(t.Context(), flux.PageStatsParams{Page: "/home", From: day1.Add(time.Hour), To: day2})
		if err != nil {
			t.Fatal(err)
		}
		want := []flux.PageStatsRow{
			{Day: day1, Views: 3, Visitors: 2},
			{Day: day2, Views: 1, Visitors: 1},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("PageStats\n%+v\nwant\n%+v", got, want)
		}
	})
}

func TestTagTotals(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		_, err := c.AppendPageViewBatch(t.Context(), []flux.PageView{
			{UserID: "u1", Page: "/a", Tags: []string{"go", "db"}, EventTime: start.Add(-3 * time.Hour)},
			{UserID: "u2", Page: "/a", Tags: []string{"go"}, EventTime: start.Add(-2 * time.Hour)},
			// ARRAY JOIN makes a row of each element, so go counts twice.
			{UserID: "u1", Page: "/b", Tags: []string{"go", "go"}, EventTime: start.Add(-time.Hour)},
			{UserID: "u4", Page: "/c", Tags: []string{"web"}, EventTime: start.Add(-time.Hour)},
			// Its hour is before since.
			{UserID: "u3", Page: "/c", Tags: []string{"db"}, EventTime: start.Add(-30 * time.Hour)},
		})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.TagTotals(t.Context(), flux.TagTotalsParams{
			Tags:  []string{"web", "go", "db", "none"},
			Since: start.Add(-24 * time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
		// uniqMerge counts u1 once over its hours. Ties on total_views go by tag, also
		// descending.
		want := []flux.TagTotalsRow{
			{Tag: "go", TotalViews: 4, TotalUsers: 2},
			{Tag: "web", TotalViews: 1, TotalUsers: 1},
			{Tag: "db", TotalViews: 1, TotalUsers: 1},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("TagTotals\n%+v\nwant\n%+v", got, want)
		}
	})
}

func TestRecentPageViews(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		_, err := c.AppendPageViewBatch(t.Context(), []flux.PageView{
			{UserID: "u1", Page: "/a", DurationMS: 100, EventTime: start.Add(-3 * time.Hour)},
			{UserID: "u1", Page: "/b", DurationMS: 200, EventTime: start.Add(-2 * time.Hour)},
			{UserID: "u1", Page: "/c", DurationMS: 300, EventTime: start.Add(-time.Hour)},
			{UserID: "u2", Page: "/d", DurationMS: 400, EventTime: start.Add(-time.Minute)},
			{UserID: "u1", Page: "/e", DurationMS: 500, EventTime: start},
		})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.RecentPageViews(t.Context(), flux.RecentPageViewsParams{User: "u1", Since: start.Add(-150 * time.Minute), N: 2})
		if err != nil {
			t.Fatal(err)
		}
		want := []flux.RecentPageViewsRow{
			{EventTime: start, Page: "/e", DurationMS: 500},
			{EventTime: start.Add(-time.Hour), Page: "/c", DurationMS: 300},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("RecentPageViews\n%+v\nwant\n%+v", got, want)
		}
	})
}

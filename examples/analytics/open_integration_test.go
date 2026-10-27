// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package analytics_test

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
	"github.com/lucindo/dynaflux/internal/validate"
)

// deployment is the example's store on DynamoDB Local: the tables of its templates, made
// for one test. No projector runs, so the views stay empty.
type deployment struct {
	aws    aws.Config
	naming catalog.Naming
	cat    *catalog.Catalog
}

func deploy(t *testing.T) deployment {
	t.Helper()
	cfg, cat, ds := validate.Load(os.DirFS("."), ".")
	if cat == nil {
		t.Fatalf("the example doesn't compile: %v", ds)
	}
	m, err := infra.Build(cfg, cat, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	awsCfg := emulatortest.Config(t, nil)
	return deployment{aws: awsCfg, naming: emulatortest.Tables(t, awsCfg, m), cat: cat}
}

// client returns the generated client on a store that dynaflux.Open backs with d.
func (d deployment) client(t *testing.T) *flux.Client {
	t.Helper()
	s, err := dynaflux.Open(dynaflux.Config{AWS: d.aws, Prefix: d.naming.Prefix})
	if err != nil {
		t.Fatal(err)
	}
	return flux.New(s)
}

func openClient(t *testing.T) *flux.Client {
	t.Helper()
	return deploy(t).client(t)
}

// appendPageViews appends two page views of u1, at - 2h and at - 1h, and one of u2.
func appendPageViews(t *testing.T, c *flux.Client, at time.Time) {
	t.Helper()
	_, err := c.AppendPageViewBatch(t.Context(), []flux.PageView{
		{UserID: "u1", Page: "/home", Tags: []string{"landing"}, DurationMS: 100, EventTime: at.Add(-2 * time.Hour)},
		{UserID: "u1", Page: "/pricing", Referrer: new("https://example.com"), DurationMS: 200, EventTime: at.Add(-time.Hour)},
		{UserID: "u2", Page: "/home", DurationMS: 300, EventTime: at.Add(-time.Hour)},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpenAppendsAndReads(t *testing.T) {
	t.Parallel()
	c := openClient(t)
	at := time.Now().UTC().Truncate(time.Second)
	appendPageViews(t, c, at)
	got := readPageViews(t, c, "u1", dynaflux.Since(at.Add(-90*time.Minute)))
	for i, ev := range got {
		// The writer's, different on every run.
		if ev.EventID == "" || ev.IngestTime.IsZero() {
			t.Errorf("event %d has event_id %q and ingest_time %v; want both set", i, ev.EventID, ev.IngestTime)
		}
		got[i].EventID, got[i].IngestTime = "", time.Time{}
	}
	want := []flux.PageView{{
		UserID: "u1", Page: "/pricing", Referrer: new("https://example.com"), DurationMS: 200,
		EventTime: at.Add(-time.Hour), OriginRegion: emulatortest.Region,
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("read\n%+v\nwant\n%+v", got, want)
	}
}

func TestOpenDuplicateAppend(t *testing.T) {
	t.Parallel()
	c := openClient(t)
	charge := flux.APICall{
		AccountID: "acme", Endpoint: "/search", Units: 5,
		RequestID: requestID(t, "6f1c2b8e-4d3a-4b5c-9e7f-0a1b2c3d4e5f"), EventTime: time.Now().UTC(),
	}
	first, err := c.AppendAPICall(t.Context(), charge)
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.AppendAPICall(t.Context(), charge)
	if !errors.Is(err, dynaflux.ErrDuplicate) || again != first {
		t.Errorf("retry = %v, %v; want ErrDuplicate with the first append's token %v", again, err, first)
	}
}

func TestOpenQueries(t *testing.T) {
	t.Parallel()
	c := openClient(t)
	at := time.Now().UTC().Truncate(time.Second)
	appendPageViews(t, c, at)
	recent, err := c.RecentPageViews(t.Context(), flux.RecentPageViewsParams{User: "u1", Since: at.Add(-3 * time.Hour), N: 10})
	want := []flux.RecentPageViewsRow{
		{EventTime: at.Add(-time.Hour), Page: "/pricing", DurationMS: 200},
		{EventTime: at.Add(-2 * time.Hour), Page: "/home", DurationMS: 100},
	}
	if err != nil || !reflect.DeepEqual(recent, want) {
		t.Errorf("RecentPageViews = %+v, %v; want %+v", recent, err, want)
	}
}

func TestOpenQueryNeedsALiveVersion(t *testing.T) {
	t.Parallel()
	d := deploy(t)
	c := d.client(t)
	at := time.Now().UTC()
	params := flux.PageStatsParams{Page: "/home", From: at.Add(-48 * time.Hour), To: at}
	// No backfill ran, so page_daily@v1 has no state item.
	const want = "dynaflux: query PageStats: view version not ready: page_daily@v1 is pending_backfill in us-east-1"
	if _, err := c.PageStats(t.Context(), params); !errors.Is(err, dynaflux.ErrViewNotReady) || err.Error() != want {
		t.Errorf("PageStats of a pending version: %v, want %q", err, want)
	}
	// An error here would be a view table that isn't where the templates put it.
	if stats, err := c.PageStats(t.Context(), params, dynaflux.AllowPartial()); err != nil || len(stats) != 0 {
		t.Errorf("PageStats with AllowPartial = %+v, %v; want no rows", stats, err)
	}
	if err := meta.New(dynamo.New(dynamodb.NewFromConfig(d.aws)), d.naming.MetaTable()).SetLive(t.Context(), d.cat); err != nil {
		t.Fatal(err)
	}
	// A new client, since c keeps the states it read for 30 seconds.
	if stats, err := d.client(t).PageStats(t.Context(), params); err != nil || len(stats) != 0 {
		t.Errorf("PageStats of a live version = %+v, %v; want no rows", stats, err)
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// benchBatch is the most records a delivery holds: the event source mapping's batch size.
const benchBatch = 100

// BenchmarkHandle applies full batches of page views to the example's views on the
// in-memory store: page_daily (uniqExact markers) and tag_hourly (ARRAY JOIN into a uniq
// sketch). It measures the projector's CPU and allocations, not DynamoDB's latency. pages
// is how many rows a batch spreads over: 1 is one hot row, 1,000 nearly one row per event.
func BenchmarkHandle(b *testing.B) {
	for _, pages := range []int{1, 50, 1000} {
		b.Run(fmt.Sprintf("pages=%d", pages), func(b *testing.B) {
			s := newBenchSetup(b, pages)
			n, tags := 0, 0
			for b.Loop() {
				batch := make([]projector.Record, benchBatch)
				for i := range batch {
					n++
					r := n % len(s.recs)
					tags += s.tags[r]
					// Each record is new to the unit: progress skips repeated sequence numbers.
					batch[i] = projector.Record{Op: projector.Insert, Seq: seq(n), New: s.recs[r].New}
				}
				// Lambda gives each invocation a deadline, which the core needs.
				ctx, cancel := context.WithTimeout(b.Context(), time.Minute)
				res, err := s.core.Handle(ctx, projector.Batch{Stream: "page_views", Unit: unit, Records: batch})
				cancel()
				if err != nil {
					b.Fatal(err)
				}
				if res.FailedSeq != "" {
					b.Fatal(res.RowErr)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchBatch), "ns/event")
			// A core that skipped the records, or a view, would be fast: check that both views
			// counted them.
			if views, tagViews := s.counted(b, pages); views != uint64(n) || tagViews != uint64(tags) {
				b.Fatalf("page_daily counted %d views and tag_hourly %d, want the %d events and %d tags delivered", views, tagViews, n, tags)
			}
		})
	}
}

// benchSetup is a core over the example store with every version live, and stored page
// views to deliver: 10,000 users, pages pages, and 0 to 2 of benchTags tags each, all in
// one hour.
type benchSetup struct {
	core *projector.Core
	e    *engine.Engine
	cat  *engine.Catalog
	recs []projector.Record
	tags []int     // the number of tags of each record's event
	hour time.Time // the events' event_time, and so their tag_hourly hour
}

// benchTags is how many tags the events draw theirs from.
const benchTags = 20

func newBenchSetup(b *testing.B, pages int) *benchSetup {
	b.Helper()
	cat, err := engine.Compile(catalogtest.MarshalFS(b, os.DirFS("../../examples/analytics")))
	if err != nil {
		b.Fatal(err)
	}
	m := mem.New()
	naming := catalog.Naming{Store: cat.Catalog().Store}
	if err = meta.New(m, naming.MetaTable()).SetLive(b.Context(), cat.Catalog()); err != nil {
		b.Fatal(err)
	}
	s := &benchSetup{
		e:    engine.New(m, engine.Options{Region: region, Entropy: rand.NewChaCha8([32]byte{})}),
		cat:  cat,
		hour: time.Now().UTC().Truncate(time.Hour),
	}
	r := rand.New(rand.NewPCG(1, 2))
	evs := make([][]expr.Value, 10*benchBatch)
	s.tags = make([]int, len(evs))
	for i := range evs {
		tags := make([]expr.Value, r.IntN(3))
		for j := range tags {
			tags[j] = expr.String(fmt.Sprintf("tag-%d", r.IntN(benchTags)))
		}
		s.tags[i] = len(tags)
		evs[i] = []expr.Value{
			expr.String(fmt.Sprintf("user-%d", r.IntN(10_000))),
			expr.String(fmt.Sprintf("/page/%d", r.IntN(pages))),
			expr.Null(),
			expr.Array(tags),
			expr.Uint(uint64(1 + r.IntN(5000))),
			expr.String("BR"),
			expr.Micros(s.hour.UnixMicro()),
		}
	}
	// An import applies no SYNC version and skips ACCEPT: the core gets every event.
	res, err := s.e.Append(b.Context(), cat, "page_views", evs, engine.AppendMode{Import: true})
	if err != nil {
		b.Fatal(err)
	}
	s.recs = make([]projector.Record, len(res))
	for i, a := range res {
		if a.Err != nil {
			b.Fatal(a.Err)
		}
		s.recs[i] = projector.Record{Op: projector.Insert, New: a.Stored}
	}
	s.core = projector.New(cat.Projector(), m, nil, projector.Config{Naming: naming, Region: region})
	return s
}

// counted returns the views that page_daily and tag_hourly hold for the events' day and hour.
func (s *benchSetup) counted(b *testing.B, pages int) (uint64, uint64) {
	b.Helper()
	var views, tagViews uint64
	day := s.hour.Truncate(24 * time.Hour)
	for p := range pages {
		// The keys, then the aggregates by name: last_view, total_ms, views, visitors.
		views += s.viewsOf(b, "page_daily@v1", 4, expr.String(fmt.Sprintf("/page/%d", p)), expr.Micros(day.UnixMicro()))
	}
	for t := range benchTags {
		// The keys, then users and views.
		tagViews += s.viewsOf(b, "tag_hourly@v1", 3, expr.String(fmt.Sprintf("tag-%d", t)), expr.Micros(s.hour.UnixMicro()))
	}
	return views, tagViews
}

// viewsOf returns column col of the row of version at key, or 0 when there is none.
func (s *benchSetup) viewsOf(b *testing.B, version string, col int, key ...expr.Value) uint64 {
	b.Helper()
	row, ok, err := s.e.ReadRow(b.Context(), s.cat, version, key)
	if err != nil {
		b.Fatal(err)
	}
	if !ok {
		return 0
	}
	return row[col].AsUint()
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine_test

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
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// benchStore is the example store on the in-memory store, every version live, with a
// projector to fill its views. It measures the engine's CPU and allocations, not
// DynamoDB's latency.
type benchStore struct {
	cat  *engine.Catalog
	mem  *mem.Mem
	e    *engine.Engine
	core *projector.Core
	seq  int
}

// writeless is a backend that drops writes, so that every iteration of an append
// benchmark finds the store as the one before did: in a growing store, later iterations
// are slower, and the result would depend on the iteration count.
type writeless struct{ store.Backend }

func (writeless) Put(context.Context, string, store.Item, store.Cond) error { return nil }

func (writeless) Transact(context.Context, []store.TxPut) error { return nil }

// appender returns an engine over the store that does all of an append's work but the
// write itself.
func (s *benchStore) appender() *engine.Engine {
	return engine.New(writeless{s.mem}, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})})
}

func newBenchStore(b *testing.B) *benchStore {
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
	return &benchStore{
		cat:  cat,
		mem:  m,
		e:    engine.New(m, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})}),
		core: projector.New(cat.Projector(), m, nil, projector.Config{Naming: naming, Region: "us-east-1"}),
	}
}

// fill imports events into stream and projects them, as the change stream would.
func (s *benchStore) fill(b *testing.B, stream string, evs [][]expr.Value) {
	b.Helper()
	res, err := s.e.Append(b.Context(), s.cat, stream, evs, engine.AppendMode{Import: true})
	if err != nil {
		b.Fatal(err)
	}
	for start := 0; start < len(res); start += 100 {
		recs := make([]projector.Record, 0, 100)
		for _, a := range res[start:min(start+100, len(res))] {
			if a.Err != nil {
				b.Fatal(a.Err)
			}
			s.seq++
			recs = append(recs, projector.Record{Op: projector.Insert, Seq: fmt.Sprintf("1%020d", s.seq), New: a.Stored})
		}
		ctx, cancel := context.WithTimeout(b.Context(), time.Minute)
		r, herr := s.core.Handle(ctx, projector.Batch{Stream: stream, Unit: "shardId-bench", Records: recs})
		cancel()
		if herr != nil {
			b.Fatal(herr)
		}
		if r.FailedSeq != "" {
			b.Fatal(r.RowErr)
		}
	}
}

// benchPageView is a page_views event; a zero at leaves event_time to the writer's clock.
func benchPageView(user, page string, tags []string, at time.Time) []expr.Value {
	vs := make([]expr.Value, len(tags))
	for i, t := range tags {
		vs[i] = expr.String(t)
	}
	ts := expr.Null()
	if !at.IsZero() {
		ts = expr.Micros(at.UnixMicro())
	}
	return []expr.Value{expr.String(user), expr.String(page), expr.Null(), expr.Array(vs), expr.Uint(120), expr.String("BR"), ts}
}

// BenchmarkAppend appends to page_views, which has only ASYNC views, and to api_calls,
// whose SYNC view quota_usage the append applies in its transaction.
func BenchmarkAppend(b *testing.B) {
	b.Run("async", func(b *testing.B) {
		s := newBenchStore(b)
		e := s.appender()
		ev := [][]expr.Value{benchPageView("user-1", "/pricing", []string{"landing", "ad"}, time.Time{})}
		for b.Loop() {
			if err := appendErr(e.Append(b.Context(), s.cat, "page_views", ev, engine.AppendMode{})); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("async_batch100", func(b *testing.B) {
		s := newBenchStore(b)
		e := s.appender()
		evs := make([][]expr.Value, 100)
		for i := range evs {
			evs[i] = benchPageView(fmt.Sprintf("user-%d", i), "/pricing", []string{"landing"}, time.Time{})
		}
		for b.Loop() {
			if err := appendErr(e.Append(b.Context(), s.cat, "page_views", evs, engine.AppendMode{})); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(evs)), "ns/event")
	})
	b.Run("sync", benchAppendSync)
}

// benchAppendSync appends to api_calls after storing 100 calls of the same account and month,
// so every iteration merges into a populated quota_usage row, as in a running store.
func benchAppendSync(b *testing.B) {
	s := newBenchStore(b)
	// A DEDUP BY stream's events carry their event_time.
	at := expr.Micros(time.Now().UnixMicro())
	call := func(id [16]byte) []expr.Value {
		return []expr.Value{expr.String("acme"), expr.String("/search"), expr.Uint(5), expr.UUID(id), at}
	}
	seed := make([][]expr.Value, 100)
	for i := range seed {
		seed[i] = call([16]byte{2, byte(i)})
	}
	if _, err := s.e.Append(b.Context(), s.cat, "api_calls", seed, engine.AppendMode{}); err != nil {
		b.Fatal(err)
	}
	rows, err := s.e.Query(b.Context(), s.cat, "QuotaUsed", []expr.Value{expr.String("acme"), at}, engine.QueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if len(rows) != 1 || rows[0][1].AsUint() != 100 {
		b.Fatalf("QuotaUsed = %v, want 100 seeded calls", rows)
	}
	e := s.appender()
	// The write is dropped, so the same event is new to every iteration.
	ev := [][]expr.Value{call([16]byte{1})}
	for b.Loop() {
		if aerr := appendErr(e.Append(b.Context(), s.cat, "api_calls", ev, engine.AppendMode{})); aerr != nil {
			b.Fatal(aerr)
		}
	}
}

// appendErr is the error of an Append call, or else of its first event that wasn't stored,
// so that a benchmark of a failing append fails instead of timing the error path.
func appendErr(res []engine.Appended, err error) error {
	if err != nil {
		return err
	}
	for _, r := range res {
		switch {
		case r.Err != nil:
			return r.Err
		case r.Stored == nil:
			return fmt.Errorf("event %v: not stored", r.Key)
		}
	}
	return nil
}

// BenchmarkQuery runs the example's named queries over filled views and a filled stream.
func BenchmarkQuery(b *testing.B) {
	s := newBenchStore(b)
	now := time.Now().UTC()
	day := now.Truncate(24 * time.Hour)
	r := rand.New(rand.NewPCG(1, 2))
	tags := make([]string, 10)
	for i := range tags {
		tags[i] = fmt.Sprintf("tag-%d", i)
	}
	// 30 days of /pricing, 24 hours of every tag, and one user's last 200 views.
	var evs [][]expr.Value
	for i := range 3000 {
		at := day.Add(-time.Duration(i%30) * 24 * time.Hour).Add(time.Duration(r.IntN(3600)) * time.Second)
		evs = append(evs, benchPageView(fmt.Sprintf("user-%d", r.IntN(500)), "/pricing", nil, at))
	}
	for i := range 2400 {
		at := now.Add(-time.Duration(i%24) * time.Hour)
		evs = append(evs, benchPageView(fmt.Sprintf("user-%d", r.IntN(500)), "/blog", []string{tags[i%10]}, at))
	}
	for i := range 200 {
		evs = append(evs, benchPageView("user-hot", "/docs", nil, now.Add(-time.Duration(i)*time.Minute)))
	}
	s.fill(b, "page_views", evs)
	tagVals := make([]expr.Value, len(tags))
	for i, t := range tags {
		tagVals[i] = expr.String(t)
	}
	cases := []struct {
		name   string
		params []expr.Value
		rows   int
	}{
		{"PageStats", []expr.Value{expr.String("/pricing"), expr.Micros(day.AddDate(0, 0, -29).UnixMicro()), expr.Micros(day.UnixMicro())}, 30},
		{"TagTotals", []expr.Value{expr.Array(tagVals), expr.Micros(now.Add(-25 * time.Hour).UnixMicro())}, 10},
		{"RecentPageViews", []expr.Value{expr.String("user-hot"), expr.Micros(now.Add(-24 * time.Hour).UnixMicro()), expr.Uint(20)}, 20},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			rows, err := s.e.Query(b.Context(), s.cat, c.name, c.params, engine.QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if len(rows) != c.rows {
				b.Fatalf("%s returned %d rows, want %d", c.name, len(rows), c.rows)
			}
			for b.Loop() {
				if _, err = s.e.Query(b.Context(), s.cat, c.name, c.params, engine.QueryOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

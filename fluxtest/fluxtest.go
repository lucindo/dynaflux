// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package fluxtest runs a Dynaflux store in memory, for tests of code that uses a generated
// client.
//
// It differs from a deployed store:
//   - Every view, ASYNC ones included, is updated before an append returns, so a query sees
//     the append at once. A deployed ASYNC view lags by about a second, and an evaluation
//     error in one fails the test.
//   - SYNC views are updated in the append's transaction, as when deployed: an evaluation
//     error in one fails the append with dynaflux.ErrEvaluation, and nothing is written.
//   - Reads are consistent: a read sees every append that returned before it started.
//   - Every view version is live from the start, so queries never return
//     dynaflux.ErrViewNotReady. A deployed version is live once its backfill finished.
//   - The receipt of a dynaflux.Waitable append is written before the append returns, so
//     dynaflux.WaitFor returns at once. A deployed wait takes about a second.
//   - Every event's origin_region is "local".
//   - Event IDs come from a fixed random source, so inside testing/synctest a test's IDs are
//     the same in every run.
//
// Its clock is [time.Now], so tests of ACCEPT windows, LOG RETENTION and view TTL run inside
// testing/synctest, whose clock starts at 2000-01-01 00:00:00 UTC. A table-driven test calls
// synctest.Test inside each t.Run, not t.Run inside a bubble. While the bubble's clock stands
// still, each append call gets an ingest_time 1 µs after the call before, and so does an
// event_time left zero.
package fluxtest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/internal/access"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// unit is the delivery unit of every event, and region the origin_region of each, the
// projector's region.
const (
	unit   = "fluxtest"
	region = "local"
)

// NewStore returns an empty in-memory store for the catalog of a generated client, such as
// flux.Catalog. It fails the test when the catalog can't be loaded, and when a view's
// breaker opens.
func NewStore(tb testing.TB, cat *dynaflux.Catalog) *dynaflux.Store {
	tb.Helper()
	c, err := access.Get().Catalog(cat)
	if err != nil {
		tb.Fatalf("fluxtest: %v", err)
		return nil
	}
	b := mem.New()
	naming := catalog.Naming{Store: c.Catalog().Store}
	m := meta.New(b, naming.MetaTable())
	if err = m.SetLive(tb.Context(), c.Catalog()); err != nil {
		tb.Fatalf("fluxtest: %v", err)
	}
	p := &projecting{
		Backend: b,
		tb:      tb,
		meta:    m,
		core:    projector.New(c.Projector(), b, nil, projector.Config{Naming: naming, Region: region}),
		// The longest invocation the prune window allows.
		timeout: time.Duration(c.Catalog().PruneWindowSeconds-catalog.LifetimeMarginSeconds) * time.Second,
		streams: map[string]string{},
	}
	for _, s := range c.Catalog().Streams {
		p.streams[naming.StreamTable(s.Name)] = s.Name
	}
	e := engine.New(p, engine.Options{
		Region:  region,
		Entropy: rand.NewChaCha8([32]byte{}),
	})
	s := new(dynaflux.Store)
	access.Get().InitStore(s, e)
	return s
}

// projecting projects each event it stores before the write returns, as the change stream's
// next record. The projector writes to the backend underneath.
type projecting struct {
	store.Backend

	tb      testing.TB
	meta    *meta.Meta
	core    *projector.Core
	timeout time.Duration     // of each projection
	streams map[string]string // stream table → stream

	mu  sync.Mutex
	seq int64 // the last record's sequence number, less 10^20
}

func (p *projecting) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if err := p.Backend.Put(ctx, table, it, c); err != nil {
		return err
	}
	if stream, ok := p.streams[table]; ok {
		p.project(ctx, stream, it)
	}
	return nil
}

// Transact projects the event that a SYNC append's transaction stored, as Put does: the
// projector skips the versions that the transaction applied.
func (p *projecting) Transact(ctx context.Context, puts []store.TxPut) error {
	if err := p.Backend.Transact(ctx, puts); err != nil {
		return err
	}
	for _, put := range puts {
		if stream, ok := p.streams[put.Table]; ok {
			p.project(ctx, stream, put.Item)
		}
	}
	return nil
}

// project applies one stored event to the views, under the lock, so records reach the
// projector in sequence order. A failure fails the test; the append succeeded.
func (p *projecting) project(ctx context.Context, stream string, it store.Item) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seq++
	// 10^20 + seq: DynamoDB's sequence numbers have at least 21 digits.
	seq := fmt.Sprintf("1%020d", p.seq)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.timeout)
	defer cancel()
	res, err := p.core.Handle(ctx, projector.Batch{
		Stream:  stream,
		Unit:    unit,
		Records: []projector.Record{{Op: projector.Insert, Seq: seq, New: it}},
	})
	if err != nil {
		p.tb.Errorf("fluxtest: project an event of %s: %v", stream, err)
		return
	}
	if res.FailedSeq != "" {
		p.tb.Errorf("fluxtest: project an event of %s: %v", stream, res.RowErr)
	}
	if len(res.Stats.Broken) > 0 {
		p.broken(ctx, res.Stats.Broken)
	}
}

// broken fails the test with the failure that broke each version.
func (p *projecting) broken(ctx context.Context, versions []string) {
	states, err := p.meta.ViewStates(ctx)
	if err != nil {
		p.tb.Errorf("fluxtest: views %v broke: %v", versions, err)
		return
	}
	for _, v := range versions {
		f := states[v].Failure
		p.tb.Errorf("fluxtest: view %s broke on event %s: %s", v, f.EventID, f.Error)
	}
}

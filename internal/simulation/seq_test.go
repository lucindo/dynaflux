// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
)

const quietEvery = 20 // the quiet stream gets every quietEvery-th event

// TestEveryStreamCrossesTheWidthBoundary: each stream's sequence numbers, in each region,
// start with 21 digits and reach 22, however few events the stream gets.
func TestEveryStreamCrossesTheWidthBoundary(t *testing.T) {
	t.Parallel()
	sc := oneEvent(simulation.Faults{})
	sc.Events, sc.Regions = 10*quietEvery, 2
	sc.Topology.UnitRecords = 40
	sys := &widths{cat: compile(t, streamsSQL), seen: map[store.Backend]map[string]map[int]bool{}}
	out := simulation.Run(t, sc, sys, 1)
	if len(out.Violations) > 0 {
		t.Fatalf("violations: %v", out.Violations)
	}
	if len(sys.seen) != sc.Regions {
		t.Fatalf("records of %d regions, want %d", len(sys.seen), sc.Regions)
	}
	for _, streams := range sys.seen {
		for _, stream := range []string{streamName, quietStream} {
			if w := streams[stream]; !w[21] || !w[22] {
				t.Errorf("stream %s: sequence number widths %v, want 21 and 22", stream, w)
			}
		}
	}
}

// widths is a system whose cores record the widths of the sequence numbers that each
// region's change streams deliver.
type widths struct {
	cat *engine.Catalog

	mu   sync.Mutex
	seen map[store.Backend]map[string]map[int]bool // by region's store, stream
}

func (w *widths) Config() simulation.Config {
	return simulation.Config{Catalog: w.cat, Naming: catalog.Naming{Store: storeName}}
}

func (*widths) Event(_ *rand.Rand, n int, _ bool) simulation.Event {
	stream := streamName
	if n%quietEvery == 0 {
		stream = quietStream
	}
	return simulation.Event{Stream: stream, Vals: []expr.Value{expr.Uint(0), expr.Null()}}
}

func (w *widths) NewCore(_ string, b store.Backend, _ simulation.Archiver, _ *projector.Compiled) simulation.Core {
	return widthsCore{w, b}
}

func (*widths) Expect(*simulation.Oracle, string, string, projector.Record) error { return nil }

func (*widths) Read(context.Context, store.Backend) (simulation.State, error) {
	return simulation.State{}, nil
}

type widthsCore struct {
	w *widths
	b store.Backend
}

func (widthsCore) ApplyBackfill(context.Context, string, []projector.BackfillRecord) error {
	return errors.New("widths has no views to backfill")
}

func (c widthsCore) Handle(_ context.Context, b projector.Batch) (projector.Result, error) {
	c.w.mu.Lock()
	defer c.w.mu.Unlock()
	if c.w.seen[c.b] == nil {
		c.w.seen[c.b] = map[string]map[int]bool{}
	}
	if c.w.seen[c.b][b.Stream] == nil {
		c.w.seen[c.b][b.Stream] = map[int]bool{}
	}
	for _, rec := range b.Records {
		c.w.seen[c.b][b.Stream][len(rec.Seq)] = true
	}
	return projector.Result{}, nil
}

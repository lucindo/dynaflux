// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
)

const (
	quietStream = "quiet"
	quietTable  = "stream-quiet"
	quietEvery  = 20 // the quiet stream gets every quietEvery-th event
)

// TestEveryStreamCrossesTheWidthBoundary: each stream's sequence numbers, in each region,
// start with 21 digits and reach 22, however few events the stream gets.
func TestEveryStreamCrossesTheWidthBoundary(t *testing.T) {
	t.Parallel()
	sc := oneEvent(simulation.Faults{})
	sc.Events, sc.Regions = 10*quietEvery, 2
	sc.Topology.UnitRecords = 40
	sys := &widths{seen: map[*simulation.Oracle]map[string]map[int]bool{}}
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

// widths is a system whose cores do nothing; it records the widths of the sequence
// numbers each region's change streams hold.
type widths struct {
	seen map[*simulation.Oracle]map[string]map[int]bool // by region's oracle, stream
}

func (*widths) Config() simulation.Config {
	return simulation.Config{
		Streams: []simulation.Stream{{Name: streamName, Table: streamTable}, {Name: quietStream, Table: quietTable}},
		Meta:    metaTable,
		Window:  time.Hour,
	}
}

func (*widths) Event(_ *rand.Rand, n int) (string, store.Item) {
	stream := streamName
	if n%quietEvery == 0 {
		stream = quietStream
	}
	return stream, store.Item{
		catalog.AttrPK: str(fmt.Sprintf("e%06d", n)),
		catalog.AttrSK: str("!"),
		"stream":       str(stream),
	}
}

func (*widths) NewCore(store.Backend, simulation.Archiver) simulation.Core { return idle{} }

func (w *widths) Expect(o *simulation.Oracle, _ string, rec simulation.Record) {
	if w.seen[o] == nil {
		w.seen[o] = map[string]map[int]bool{}
	}
	stream := rec.New["stream"].Str
	if w.seen[o][stream] == nil {
		w.seen[o][stream] = map[int]bool{}
	}
	w.seen[o][stream][len(rec.Seq)] = true
}

func (*widths) Read(context.Context, store.Backend) (simulation.State, error) {
	return simulation.State{}, nil
}

type idle struct{}

func (idle) Handle(context.Context, simulation.Batch) (simulation.Result, error) {
	return simulation.Result{}, nil
}

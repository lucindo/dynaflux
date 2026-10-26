// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"cmp"
	"context"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
)

// oneEvent delivers a single event to a single invocation at a time; retried batches
// come back after the mapping's backoff.
func oneEvent(f simulation.Faults) simulation.Scenario {
	return simulation.Scenario{
		Name:     "scripted",
		Regions:  1,
		Events:   1,
		Writers:  1,
		Faults:   f,
		Topology: simulation.Topology{Partitions: 1, UnitRecords: 1},
		ESM:      simulation.ESM{Timeout: 2 * time.Second, MaxBatch: 1, PF: 1},
		Budget:   2 * time.Hour,
	}
}

// TestKillBoundary pins Lambda's kill: a write sent 1 µs before the deadline lands, one
// sent at the deadline doesn't, and the invocation dies there.
func TestKillBoundary(t *testing.T) {
	t.Parallel()
	sys := &scripted{script: func(ctx context.Context, b store.Backend, n int64) error {
		if n > 1 {
			return nil
		}
		deadline, _ := ctx.Deadline()
		time.Sleep(time.Until(deadline) - time.Microsecond)
		if err := put(ctx, b, "before"); err != nil {
			return err
		}
		time.Sleep(time.Microsecond)
		// Without cancellation, only the harness's kill can stop this write.
		return put(context.WithoutCancel(ctx), b, "at")
	}}
	out := simulation.Run(t, oneEvent(simulation.Faults{}), sys, 1)
	if len(out.Violations) > 0 {
		t.Fatalf("violations: %v", out.Violations)
	}
	if want := []string{"before"}; !reflect.DeepEqual(sys.stored, want) {
		t.Errorf("stored %v, want %v", sys.stored, want)
	}
	if out.Stats.Killed != 1 || out.Stats.Invocations != 2 {
		t.Errorf("%d requests refused in %d invocations, want 1 in 2: the first dies, the retry succeeds",
			out.Stats.Killed, out.Stats.Invocations)
	}
}

// TestLateWriteLandsAfterItsInvocationDied: a write in flight at the deadline lands later,
// after the retry of its batch has looked for it.
func TestLateWriteLandsAfterItsInvocationDied(t *testing.T) {
	t.Parallel()
	var foundByRetry atomic.Bool
	sys := &scripted{script: func(ctx context.Context, b store.Backend, n int64) error {
		if n == 1 {
			return put(ctx, b, "late")
		}
		it, err := b.Get(ctx, viewTable, store.Key{PK: "late", SK: "!"}, true)
		foundByRetry.Store(it != nil)
		return err
	}}
	out := simulation.Run(t, oneEvent(simulation.Faults{Late: 1, LateMax: time.Hour}), sys, 1)
	if len(out.Violations) > 0 {
		t.Fatalf("violations: %v", out.Violations)
	}
	if foundByRetry.Load() || !reflect.DeepEqual(sys.stored, []string{"late"}) {
		t.Errorf("the retry found the write: %v; stored at the end: %v; want false, [late]", foundByRetry.Load(), sys.stored)
	}
	if out.Stats.Late != 1 || out.Stats.Timeouts != 1 || out.Stats.Invocations != 2 {
		t.Errorf("%d late writes, %d timeouts in %d invocations; want 1, 1 in 2", out.Stats.Late, out.Stats.Timeouts, out.Stats.Invocations)
	}
}

// TestCrashOnAGoroutineHandleStarted: a crash ends the whole invocation, even when it
// strikes a goroutine Handle waits for, so the batch is retried on a new container.
func TestCrashOnAGoroutineHandleStarted(t *testing.T) {
	t.Parallel()
	sys := &scripted{script: func(ctx context.Context, b store.Backend, n int64) error {
		if n > 1 {
			return nil
		}
		var wg sync.WaitGroup
		var err error
		wg.Go(func() { err = put(ctx, b, "before") })
		wg.Wait()
		return err
	}}
	out := simulation.Run(t, oneEvent(simulation.Faults{Crash: 1}), sys, 1)
	if len(out.Violations) > 0 {
		t.Fatalf("violations: %v", out.Violations)
	}
	s := out.Stats
	if s.Crashes != 1 || s.Invocations != 2 || s.Containers != 2 || len(sys.stored) != 0 {
		t.Errorf("%d crashes, %d invocations, %d containers, stored %v; want 1, 2, 2, []",
			s.Crashes, s.Invocations, s.Containers, sys.stored)
	}
}

// TestIdenticalRequestsInFlight: two goroutines of one invocation parked on the same
// request would be released in the runtime's order, not the seed's, so the run fails.
func TestIdenticalRequestsInFlight(t *testing.T) {
	t.Parallel()
	sys := &scripted{script: func(ctx context.Context, b store.Backend, _ int64) error {
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() { b.Get(ctx, viewTable, store.Key{PK: "before", SK: "!"}, true) })
		}
		wg.Wait()
		return nil
	}}
	expectViolation(t, simulation.Run(t, oneEvent(simulation.Faults{}), sys, 1), "D3:")
}

// TestRequestLongAfterHandleReturned: a goroutine that outlives Handle and sends after
// the run's last actor exited is still caught.
func TestRequestLongAfterHandleReturned(t *testing.T) {
	t.Parallel()
	sys := &scripted{script: func(ctx context.Context, b store.Backend, n int64) error {
		if n == 1 {
			go func() {
				time.Sleep(time.Minute)
				put(ctx, b, "late")
			}()
		}
		return nil
	}}
	out := simulation.Run(t, oneEvent(simulation.Faults{}), sys, 1)
	if !hasViolation(out, "P7:") || len(sys.stored) != 0 {
		t.Errorf("violations %q, stored %v; want P7 and nothing stored", out.Violations, sys.stored)
	}
}

// TestQueryPages: each page of a Query is a request of its own, refused once the
// invocation's deadline has passed.
func TestQueryPages(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		slow     bool
		want     []string
		requests int64 // the append, three puts and the pages
		killed   int64
	}{
		{"pages of one item, two and none", false, []string{"a", "b", "c"}, 1 + 3 + 3, 0},
		{"the second page after the deadline", true, []string{"a"}, 1 + 3 + 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := &pagedReader{slow: tc.slow}
			out := simulation.Run(t, oneEvent(simulation.Faults{}), &scripted{script: r.script}, 1)
			if len(out.Violations) > 0 {
				t.Fatalf("violations: %v", out.Violations)
			}
			if !reflect.DeepEqual(r.got, tc.want) || r.err != nil {
				t.Errorf("read %v, %v; want %v, no error", r.got, r.err, tc.want)
			}
			if out.Stats.Requests != tc.requests || out.Stats.Killed != tc.killed {
				t.Errorf("%d requests, %d refused; want %d, %d", out.Stats.Requests, out.Stats.Killed, tc.requests, tc.killed)
			}
		})
	}
}

// pagedReader's first invocation writes three items to one partition, then reads them a
// page at a time; when slow, it waits for its deadline after each item.
type pagedReader struct {
	slow bool
	got  []string
	err  error
}

func (r *pagedReader) script(ctx context.Context, b store.Backend, n int64) error {
	if n > 1 {
		return nil
	}
	for _, sk := range []string{"a", "b", "c"} {
		if err := b.Put(ctx, viewTable, store.Item{catalog.AttrPK: str("q"), catalog.AttrSK: str(sk)}, store.Cond{}); err != nil {
			return err
		}
	}
	for it, err := range b.Query(ctx, viewTable, store.Query{PK: "q", PageSize: 1}) {
		if r.err = err; err != nil {
			return err
		}
		r.got = append(r.got, it[catalog.AttrSK].Str)
		if deadline, _ := ctx.Deadline(); r.slow {
			time.Sleep(time.Until(deadline))
		}
	}
	return nil
}

// TestStoreHistoryChecks: P5 catches a marker's owner, a unit's progress or its fence
// going back, and only in the items that hold them.
func TestStoreHistoryChecks(t *testing.T) {
	t.Parallel()
	unit := func(attrs store.Item) write {
		attrs[catalog.AttrPK], attrs[catalog.AttrSK] = str("unit#events#U1"), str("!")
		return write{metaTable, attrs}
	}
	fences := func(fence string) store.Value {
		return store.Value{Type: store.M, Map: map[string]store.Value{"page@v1": str(fence)}}
	}
	row := func(name string, v store.Value) write {
		return write{viewTable, store.Item{catalog.AttrPK: str("page#p1"), catalog.AttrSK: str("!"), name: v}}
	}
	for _, tc := range []struct {
		name   string
		writes []write
		want   string
	}{
		{"marker owner changed", []write{row(catalog.AttrMarkerOwner, str("U1/1")), row(catalog.AttrMarkerOwner, str("U2/1"))}, "P5:"},
		{"progress back", []write{unit(store.Item{"progress": str("0002")}), unit(store.Item{"progress": str("0001")})}, "P5:"},
		{"fence changed", []write{unit(store.Item{"fences": fences("0005")}), unit(store.Item{"fences": fences("0006")})}, "P5:"},
		{"progress up, a fence added", []write{unit(store.Item{"progress": str("0001")}), unit(store.Item{"progress": str("0002"), "fences": fences("0005")})}, ""},
		{"a view column named progress", []write{row("progress", num(9)), row("progress", num(10))}, ""},
		{"a view column named fences", []write{row("fences", fences("0005")), row("fences", fences("0006"))}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := simulation.Run(t, oneEvent(simulation.Faults{}), &scripted{script: writes(tc.writes...)}, 1)
			expectViolation(t, out, tc.want)
		})
	}
}

// TestHighWaterMarkBound: P4 bounds a row by the live units of one stream, since a view
// reads one stream, and leaves room for a write that landed late.
func TestHighWaterMarkBound(t *testing.T) {
	t.Parallel()
	perStream := oneEvent(simulation.Faults{})
	perStream.Topology = simulation.Topology{Partitions: 2, UnitRecords: 1000}
	late := oneEvent(simulation.Faults{Late: 1, LateMax: time.Hour})
	late.Window = time.Second
	for _, tc := range []struct {
		name    string
		sc      simulation.Scenario
		entries int
		want    string
	}{
		// Each stream opens two units, and the event's may roll over into a third: a row
		// of one stream may hold 3 + 1 entries, but both streams have 5 units.
		{"two streams, over one stream's bound", perStream, 5, "P4:"},
		{"two streams, within one stream's bound", perStream, 2, ""},
		// The unit closes after its event; the write lands late, after the retry of its
		// batch finished the unit, and may still carry that unit's entry.
		{"a late write", late, 3, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			marks := map[string]store.Value{}
			for i := range tc.entries {
				marks["U"+strconv.Itoa(i)] = str("1")
			}
			hwm := write{viewTable, store.Item{
				catalog.AttrPK:             str("page#p1"),
				catalog.AttrSK:             str("!"),
				catalog.AttrHighWaterMarks: {Type: store.M, Map: marks},
			}}
			sys := &scripted{quiet: true, script: writes(hwm)}
			expectViolation(t, simulation.Run(t, tc.sc, sys, 1), tc.want)
		})
	}
}

type write struct {
	table string
	item  store.Item
}

// writes is a script whose first invocation puts ws in order, unconditionally.
func writes(ws ...write) func(context.Context, store.Backend, int64) error {
	return func(ctx context.Context, b store.Backend, n int64) error {
		for _, w := range ws {
			if n > 1 {
				break
			}
			if err := b.Put(ctx, w.table, w.item, store.Cond{}); err != nil {
				return err
			}
		}
		return nil
	}
}

// expectViolation fails t unless out violates property, or for "", nothing.
func expectViolation(t *testing.T, out simulation.Outcome, property string) {
	t.Helper()
	if property == "" && len(out.Violations) > 0 || property != "" && !hasViolation(out, property) {
		t.Errorf("violations %q, want %s", out.Violations, cmp.Or(property, "none"))
	}
}

func hasViolation(out simulation.Outcome, property string) bool {
	return slices.ContainsFunc(out.Violations, func(v string) bool { return strings.HasPrefix(v, property) })
}

// scripted is a system whose cores run script, numbering invocations from 1 across
// containers. Read records which of the names in viewTable exist.
type scripted struct {
	script      func(ctx context.Context, b store.Backend, n int64) error
	quiet       bool // a second stream, without events
	invocations atomic.Int64
	stored      []string
}

func (s *scripted) Config() simulation.Config {
	cfg := simulation.Config{
		Streams: []simulation.Stream{{Name: streamName, Table: streamTable}},
		Meta:    metaTable,
		Window:  time.Hour,
	}
	if s.quiet {
		cfg.Streams = append(cfg.Streams, simulation.Stream{Name: quietStream, Table: quietTable})
	}
	return cfg
}

func (*scripted) Event(*rand.Rand, int) (string, store.Item) {
	return streamName, store.Item{catalog.AttrPK: str("e"), catalog.AttrSK: str("!")}
}

func (s *scripted) NewCore(b store.Backend, _ simulation.Archiver) simulation.Core {
	return scriptedCore{s, b}
}

func (*scripted) Expect(*simulation.Oracle, string, simulation.Record) {}

func (s *scripted) Read(ctx context.Context, b store.Backend) (simulation.State, error) {
	for _, name := range []string{"before", "at", "late"} {
		it, err := b.Get(ctx, viewTable, store.Key{PK: name, SK: "!"}, true)
		if err != nil {
			return simulation.State{}, err
		}
		if it != nil {
			s.stored = append(s.stored, name)
		}
	}
	return simulation.State{}, nil
}

type scriptedCore struct {
	sys *scripted
	b   store.Backend
}

func (c scriptedCore) Handle(ctx context.Context, _ simulation.Batch) (simulation.Result, error) {
	return simulation.Result{}, c.sys.script(ctx, c.b, c.sys.invocations.Add(1))
}

func put(ctx context.Context, b store.Backend, name string) error {
	return b.Put(ctx, viewTable, store.Item{catalog.AttrPK: str(name), catalog.AttrSK: str("!")}, store.IfAbsent())
}

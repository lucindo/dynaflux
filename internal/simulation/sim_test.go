// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"context"
	"errors"
	"flag"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
)

var (
	full         = flag.Bool("sim.full", false, "run each scenario's full tier of seeds")                  //nolint:gochecknoglobals // go test flags are package variables
	seedCount    = flag.Int("sim.seeds", 0, "seeds per scenario or control; 0 uses the tier's")            //nolint:gochecknoglobals // go test flags are package variables
	controlID    = flag.String("sim.control", "", "run TestControl for this negative control")             //nolint:gochecknoglobals // go test flags are package variables
	scenarioName = flag.String("sim.scenario", "faults", "the scenario TestControl runs a code mutant in") //nolint:gochecknoglobals // go test flags are package variables
	onlySeed     = flag.Int("sim.seed", -1, "run only this seed in TestSim")                               //nolint:gochecknoglobals // go test flags are package variables
)

func scenario(t *testing.T, name string) simulation.Scenario {
	t.Helper()
	for _, sc := range simulation.Scenarios() {
		if sc.Name == name {
			return sc
		}
	}
	t.Fatalf("no scenario named %q", name)
	return simulation.Scenario{}
}

func seeds(sc simulation.Scenario) int {
	switch {
	case *seedCount > 0:
		return *seedCount
	case *full:
		return sc.Seeds.Full
	}
	return sc.Seeds.Smoke
}

// seedList returns the seeds to run: the one -sim.seed names, or the first n.
func seedList(n int) []int {
	if *onlySeed >= 0 {
		return []int{*onlySeed}
	}
	list := make([]int, n)
	for i := range list {
		list[i] = i
	}
	return list
}

// report fails t on out's violations; tests names the -run pattern that reruns seed.
func report(t *testing.T, tests string, seed int, out simulation.Outcome) {
	t.Helper()
	if len(out.Violations) == 0 {
		return
	}
	shown := out.Violations[:min(len(out.Violations), 10)]
	t.Errorf("seed %d: %d violations (rerun: go test ./internal/simulation -run '%s' -sim.seed=%d -v):\n%s",
		seed, len(out.Violations), tests, seed, strings.Join(shown, "\n"))
}

// TestSim runs every scenario exact, and checks that a tier of seeds made its faults
// happen.
func TestSim(t *testing.T) {
	t.Parallel()
	sys := newPageViews(t)
	for _, sc := range simulation.Scenarios() {
		t.Run(sc.Name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var total simulation.Stats
			ran := 0
			t.Cleanup(func() { checkTier(t, sc, ran, total) })
			for _, seed := range seedList(seeds(sc)) {
				t.Run(strconv.Itoa(seed), func(t *testing.T) {
					t.Parallel()
					out := simulation.Run(t, sc, sys, uint64(seed))
					report(t, "^TestSim$/^"+sc.Name+"$", seed, out)
					mu.Lock()
					add(&total, out.Stats)
					ran++
					mu.Unlock()
				})
			}
		})
	}
}

// checkTier fails t when ran seeds, a whole tier, didn't exercise what sc sets up.
func checkTier(t *testing.T, sc simulation.Scenario, ran int, total simulation.Stats) {
	t.Helper()
	t.Logf("%d seeds: %+v", ran, total)
	if ran < sc.Seeds.Smoke {
		return
	}
	if total.Invocations == 0 || total.Units <= int64(sc.Regions*sc.Topology.Partitions) {
		t.Errorf("nothing was delivered, or no unit rolled over: %+v", total)
	}
	if sc.Faults.Transient > 0 && min(total.Transient, total.Ambiguous, total.Retried, total.Crashes,
		total.Killed, total.Timeouts, total.Slow, total.Stale, total.Dups, total.Empty) == 0 {
		t.Errorf("a fault never happened: %+v", total)
	}
	if sc.Poison > 0 && total.Poison == 0 {
		t.Errorf("no event was poison: %+v", total)
	}
}

func add(total *simulation.Stats, s simulation.Stats) {
	a, b := reflect.ValueOf(total).Elem(), reflect.ValueOf(s)
	for i := range a.NumField() {
		a.Field(i).SetInt(a.Field(i).Int() + b.Field(i).Int())
	}
}

// TestDeterministic replays seeds: a seed's run is the same every time, and two seeds
// differ.
func TestDeterministic(t *testing.T) {
	t.Parallel()
	sc, sys := scenario(t, "faults"), newPageViews(t)
	var mu sync.Mutex
	traces := map[uint64]bool{}
	t.Cleanup(func() {
		if len(traces) != 2 {
			t.Errorf("two seeds gave %d traces, want 2", len(traces))
		}
	})
	for seed := range uint64(2) {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			first := simulation.Run(t, sc, sys, seed)
			second := simulation.Run(t, sc, sys, seed)
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("run 1 = %+v\nrun 2 = %+v", first, second)
			}
			if first.Steps == 0 {
				t.Fatal("no steps")
			}
			mu.Lock()
			traces[first.Trace] = true
			mu.Unlock()
		})
	}
}

// expectCaught runs n seeds of sc in parallel, and fails unless one violates a property.
func expectCaught(t *testing.T, control string, sc simulation.Scenario, sys simulation.System, n int) {
	t.Helper()
	var caught atomic.Int64
	t.Cleanup(func() {
		t.Logf("control %s: caught in %d/%d seeds", control, caught.Load(), n)
		if caught.Load() == 0 {
			t.Errorf("the harness did not catch control %s", control)
		}
	})
	for seed := range n {
		t.Run(strconv.Itoa(seed), func(t *testing.T) {
			t.Parallel()
			if out := simulation.Run(t, sc, sys, uint64(seed)); len(out.Violations) > 0 {
				caught.Add(1)
			}
		})
	}
}

// environmentControls change the environment or the configuration, not the code.
func environmentControls(t *testing.T) map[string]simulation.Scenario {
	t.Helper()
	pf2 := scenario(t, "clean")
	pf2.ESM.PF = 2
	return map[string]simulation.Scenario{"C3": pf2}
}

// TestControl runs one negative control, an environment control or a code mutant that
// go run ./internal/simulation/mutants applied, and passes only if the harness catches it.
func TestControl(t *testing.T) {
	t.Parallel()
	if *controlID == "" {
		t.Skip("set -sim.control to run a negative control")
	}
	sc, ok := environmentControls(t)[*controlID]
	if !ok {
		sc = scenario(t, *scenarioName)
	}
	expectCaught(t, *controlID, sc, newPageViews(t), seeds(sc))
}

// TestParallelizationFactorControl is the smoke tier's environment control: two batches
// of one unit at once lose updates.
func TestParallelizationFactorControl(t *testing.T) {
	t.Parallel()
	expectCaught(t, "C3", environmentControls(t)["C3"], newPageViews(t), 5)
}

// TestPropertiesCatchBugs runs the real core with one defect each and expects the
// property that defect breaks.
func TestPropertiesCatchBugs(t *testing.T) {
	t.Parallel()
	small := scenario(t, "clean")
	small.Regions, small.Events, small.Budget = 1, 60, 10*time.Minute
	empty := small
	empty.ESM.Empty = 0.5
	// The core doesn't prune high-water marks yet: units that finished a W ago keep theirs.
	pruned := small
	pruned.Topology.UnitRecords, pruned.Window = 2, time.Second
	sys := newPageViews(t)
	for _, tc := range []struct {
		name     string
		sc       simulation.Scenario
		bug      bug
		property string
	}{
		{"high-water marks never pruned", pruned, noBug, "P4:"},
		{"a unit never drains", small, alwaysFail, "P6:"},
		{"a panic", small, panics, "P8:"},
		{"a request for an empty batch", empty, emptyCall, "P9:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := simulation.Run(t, tc.sc, &buggy{pageViews: sys, bug: tc.bug}, 1)
			for _, v := range out.Violations {
				if strings.HasPrefix(v, tc.property) {
					return
				}
			}
			t.Errorf("no %s violation; got %d others: %.300q", tc.property, len(out.Violations), out.Violations)
		})
	}
}

// bug is a defect the harness must catch.
type bug uint8

const (
	noBug      bug = iota
	alwaysFail     // fails every batch
	panics         // panics once
	emptyCall      // reads the store for an empty batch
)

// buggy is pageViews with a defect in its cores.
type buggy struct {
	*pageViews

	bug  bug
	once atomic.Bool // for bugs that strike once per run
}

func (s *buggy) NewCore(b store.Backend, a simulation.Archiver) simulation.Core {
	return buggyCore{Core: s.pageViews.NewCore(b, a), sys: s, b: b}
}

type buggyCore struct {
	simulation.Core

	sys *buggy
	b   store.Backend
}

func (c buggyCore) Handle(ctx context.Context, b projector.Batch) (projector.Result, error) {
	switch {
	case c.sys.bug == alwaysFail:
		return projector.Result{}, errors.New("stuck")
	case c.sys.bug == panics && c.sys.once.CompareAndSwap(false, true):
		panic("boom")
	case c.sys.bug == emptyCall && len(b.Records) == 0:
		_, err := c.b.Get(ctx, metaTable(), store.Key{PK: "view", SK: pageRates}, true)
		return projector.Result{}, err
	}
	return c.Core.Handle(ctx, b)
}

// TestChangeStreamCheck: P0 catches a change stream that repeats an append, lacks one, or
// holds one that no append wrote.
func TestChangeStreamCheck(t *testing.T) {
	t.Parallel()
	a, b := event("a"), event("b")
	for _, tc := range []struct {
		name             string
		applied, records []store.Item
		want             int
	}{
		{"each append once", []store.Item{a, b}, []store.Item{b, a}, 0},
		{"an append twice", []store.Item{a, b}, []store.Item{a, b, a}, 1},
		{"an append missing", []store.Item{a, b}, []store.Item{a}, 1},
		{"a record without an append", []store.Item{a}, []store.Item{a, b}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := simulation.CheckChangeStream(tc.applied, tc.records)
			p0 := slices.DeleteFunc(slices.Clone(got), func(v string) bool { return !strings.HasPrefix(v, "P0:") })
			if len(got) != tc.want || len(p0) != tc.want {
				t.Errorf("violations %q, want %d of P0", got, tc.want)
			}
		})
	}
}

func event(pk string) store.Item {
	return store.Item{catalog.AttrPK: str(pk), catalog.AttrSK: str("!")}
}

// TestViewChecks: P1 compares a region's views with its oracle, P2 the regions with each
// other, and P3 the breakers with the records that fail; a broken view is left out of P1
// and P2.
func TestViewChecks(t *testing.T) {
	t.Parallel()
	row := simulation.RowID{View: pageHourly, Key: "p1"}
	broken := simulation.RowID{View: pageRates, Key: "p1"}
	poison := simulation.RecordID{Unit: "U1", Seq: "100000000000000000001"}
	oracle := simulation.NewOracle()
	oracle.Count(row, "views", 2)
	oracle.Poison(pageRates, poison)
	state := func(views, rate string, breaker *simulation.RecordID) simulation.State {
		st := simulation.State{Rows: simulation.Rows{row: {"views": views}, broken: {"rate": rate}}, Broken: map[string]simulation.RecordID{}}
		if breaker != nil {
			st.Broken[pageRates] = *breaker
		}
		return st
	}
	other := simulation.RecordID{Unit: "U1", Seq: "100000000000000000002"}
	for _, tc := range []struct {
		name   string
		r0, r1 simulation.State
		want   []string
	}{
		{"exact, a broken view differs", state("2", "1", &poison), state("2", "2", &poison), nil},
		{"a count off in r0", state("3", "1", &poison), state("3", "1", &poison), []string{"P1:"}},
		{"regions differ", state("2", "1", &poison), state("1", "1", &poison), []string{"P2:"}},
		{"broken by a record that doesn't fail it", state("2", "1", &other), state("2", "1", &other), []string{"P3:"}},
		{"not broken", state("2", "1", nil), state("2", "1", nil), []string{"P1:", "P3:"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got []string
			for _, v := range simulation.CheckViews(oracle, tc.r0, tc.r1) {
				got = append(got, v[:3])
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("violations %q, want properties %q", simulation.CheckViews(oracle, tc.r0, tc.r1), tc.want)
			}
		})
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
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
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
)

var (
	full         = flag.Bool("sim.full", false, "run each scenario's full tier of seeds")                  //nolint:gochecknoglobals // go test flags are package variables
	seedCount    = flag.Int("sim.seeds", 0, "seeds per scenario or control; 0 uses the tier's")            //nolint:gochecknoglobals // go test flags are package variables
	controlID    = flag.String("sim.control", "", "run TestControl for this negative control")             //nolint:gochecknoglobals // go test flags are package variables
	scenarioName = flag.String("sim.scenario", "faults", "the scenario TestControl runs a code mutant in") //nolint:gochecknoglobals // go test flags are package variables
	onlySeed     = flag.Int("sim.seed", -1, "run only this seed in TestSim and TestBreaker")               //nolint:gochecknoglobals // go test flags are package variables
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
	for _, sc := range simulation.Scenarios() {
		t.Run(sc.Name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var total simulation.Stats
			ran := 0
			t.Cleanup(func() {
				t.Logf("%d seeds: %+v", ran, total)
				if ran < sc.Seeds.Smoke {
					return
				}
				if total.Invocations == 0 || total.Units <= int64(sc.Regions*sc.Topology.Partitions) {
					t.Errorf("nothing was delivered, or no unit rolled over: %+v", total)
				}
				if sc.Name == "faults" && min(total.Transient, total.Ambiguous, total.Retried, total.Crashes,
					total.Killed, total.Timeouts, total.Slow, total.Stale, total.Dups, total.Empty) == 0 {
					t.Errorf("a fault never happened: %+v", total)
				}
			})
			for _, seed := range seedList(seeds(sc)) {
				t.Run(strconv.Itoa(seed), func(t *testing.T) {
					t.Parallel()
					out := simulation.Run(t, sc, &counter{}, uint64(seed))
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
	sc := scenario(t, "faults")
	traces := map[uint64]bool{}
	for seed := range uint64(2) {
		first := simulation.Run(t, sc, &counter{}, seed)
		second := simulation.Run(t, sc, &counter{}, seed)
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("seed %d: run 1 = %+v\nrun 2 = %+v", seed, first, second)
		}
		if first.Steps == 0 {
			t.Fatalf("seed %d made no steps", seed)
		}
		traces[first.Trace] = true
	}
	if len(traces) != 2 {
		t.Fatal("two seeds gave the same trace")
	}
}

// expectCaught runs n seeds of sc in parallel, and fails unless one violates a property.
func expectCaught(t *testing.T, control string, sc simulation.Scenario, newSys func() simulation.System, n int) {
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
			if out := simulation.Run(t, sc, newSys(), uint64(seed)); len(out.Violations) > 0 {
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
	expectCaught(t, *controlID, sc, func() simulation.System { return &counter{} }, seeds(sc))
}

// TestParallelizationFactorControl is the smoke tier's environment control: two batches
// of one unit at once lose updates.
func TestParallelizationFactorControl(t *testing.T) {
	t.Parallel()
	expectCaught(t, "C3", environmentControls(t)["C3"], func() simulation.System { return &counter{} }, 5)
}

// TestPropertiesCatchBugs gives the harness a core with one defect each and expects the
// property that defect breaks.
func TestPropertiesCatchBugs(t *testing.T) {
	t.Parallel()
	clean, faults := scenario(t, "clean"), scenario(t, "faults")
	small := func(sc simulation.Scenario) simulation.Scenario {
		sc.Regions, sc.Events, sc.Budget = 1, 60, 10*time.Minute
		return sc
	}
	empty := small(clean)
	empty.ESM.Empty = 0.5
	pruned := small(clean)
	pruned.Topology.UnitRecords, pruned.Window = 2, time.Second
	for _, tc := range []struct {
		name     string
		sc       simulation.Scenario
		sys      *counter
		property string
	}{
		{"redeliveries counted twice", faults, &counter{bug: skipHWM}, "P1:"},
		{"regions disagree", clean, &counter{bug: unitColumn}, "P2:"},
		{"poison counted", small(clean), &counter{poison: 0.1, bug: ignorePoison}, "P3:"},
		{"high-water marks never pruned", pruned, &counter{}, "P4:"},
		{"row version jumps", small(clean), &counter{bug: versionJump}, "P5:"},
		{"a unit never drains", small(clean), &counter{bug: alwaysFail}, "P6:"},
		{"a write after Handle returned", small(clean), &counter{bug: straggle}, "P7:"},
		{"a panic", small(clean), &counter{bug: panics}, "P8:"},
		{"a request for an empty batch", empty, &counter{bug: emptyCall}, "P9:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := simulation.Run(t, tc.sc, tc.sys, 1)
			for _, v := range out.Violations {
				if strings.HasPrefix(v, tc.property) {
					return
				}
			}
			t.Errorf("no %s violation; got %d others: %.300q", tc.property, len(out.Violations), out.Violations)
		})
	}
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

// TestBreaker opens page's breaker with poison events, under faults: it opens in every
// region, P3 holds and the views that stay live are exact.
func TestBreaker(t *testing.T) {
	t.Parallel()
	sc := scenario(t, "faults")
	for _, seed := range seedList(2) {
		sys := &counter{poison: 0.02}
		out := simulation.Run(t, sc, sys, uint64(seed))
		report(t, "^TestBreaker$", seed, out)
		if got := sys.broken.Load(); got != int64(sc.Regions) {
			t.Errorf("seed %d: page is broken in %d regions, want %d", seed, got, sc.Regions)
		}
	}
}

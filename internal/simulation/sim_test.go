// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
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
	full         = flag.Bool("sim.full", false, "run each scenario's full tier of seeds")                      //nolint:gochecknoglobals // go test flags are package variables
	seedCount    = flag.Int("sim.seeds", 0, "seeds per scenario or control; 0 uses the tier's")                //nolint:gochecknoglobals // go test flags are package variables
	controlID    = flag.String("sim.control", "", "run TestControl for this negative control")                 //nolint:gochecknoglobals // go test flags are package variables
	scenarioName = flag.String("sim.scenario", "faults", "the scenario TestControl runs a code mutant in")     //nolint:gochecknoglobals // go test flags are package variables
	want         = flag.String("sim.want", "", "the property prefix a seed must violate to catch the control") //nolint:gochecknoglobals // go test flags are package variables
	onlySeed     = flag.Int("sim.seed", -1, "run only this seed in TestSim")                                   //nolint:gochecknoglobals // go test flags are package variables
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

// report fails t on the violations of seed of sc.
func report(t *testing.T, sc simulation.Scenario, seed int, out simulation.Outcome) {
	t.Helper()
	if len(out.Violations) == 0 {
		return
	}
	shown := out.Violations[:min(len(out.Violations), 10)]
	t.Errorf("seed %d: %d violations (rerun: make sim-seed SCENARIO=%s SEED=%d):\n%s",
		seed, len(out.Violations), sc.Name, seed, strings.Join(shown, "\n"))
}

// outlivesWindow reports whether a scenario runs long past W, so that its hot rows exceed
// P4's bound unless the core prunes.
func outlivesWindow(sc simulation.Scenario) bool {
	return slices.Contains([]string{"d23a", "long", "late", "regions"}, sc.Name)
}

// TestSim runs every scenario exact, and checks that a tier of seeds made its faults
// happen.
func TestSim(t *testing.T) {
	t.Parallel()
	sys := newPageViews(t)
	for _, sc := range simulation.Scenarios() {
		t.Run(sc.Name, func(t *testing.T) {
			t.Parallel()
			runTier(t, sc, sys, outlivesWindow(sc), func(t *testing.T, seed int, out simulation.Outcome) {
				t.Helper()
				report(t, sc, seed, out)
			})
		})
	}
}

// runTier runs sc's seeds in parallel, gives each outcome to check, and checks that the
// tier made sc's faults happen, and that rows dropped marks when prunes is set.
func runTier(t *testing.T, sc simulation.Scenario, sys simulation.System, prunes bool, check func(t *testing.T, seed int, out simulation.Outcome)) {
	t.Helper()
	list := seedList(seeds(sc))
	if len(list) == 0 {
		t.Skip("no smoke seeds: the full tier runs this scenario")
	}
	var mu sync.Mutex
	var total simulation.Stats
	ran, most := 0, 0
	t.Cleanup(func() {
		t.Logf("%d seeds: %+v", ran, total)
		t.Logf("a row write left up to %d high-water marks", most)
		// One seed, or some of a tier, needn't make every fault happen.
		if *onlySeed < 0 && ran == len(list) {
			checkTier(t, sc, prunes, total)
		}
	})
	for _, seed := range list {
		t.Run(strconv.Itoa(seed), func(t *testing.T) {
			t.Parallel()
			out := simulation.Run(t, sc, sys, uint64(seed))
			check(t, seed, out)
			mu.Lock()
			add(&total, out.Stats)
			ran++
			most = max(most, out.MaxHWM)
			mu.Unlock()
		})
	}
}

// checkTier fails t when a tier of seeds of sc didn't exercise what sc sets up, and
// pruning when prunes is set.
func checkTier(t *testing.T, sc simulation.Scenario, prunes bool, total simulation.Stats) {
	t.Helper()
	if total.Invocations == 0 || total.Units <= int64(sc.Regions*sc.Topology.Partitions) {
		t.Errorf("nothing was delivered, or no unit rolled over: %+v", total)
	}
	if sc.Faults.Transient > 0 && min(total.Transient, total.Ambiguous, total.Retried, total.Crashes,
		total.Killed, total.Timeouts, total.Slow, total.Stale, total.Dups, total.Empty) == 0 {
		t.Errorf("a fault never happened: %+v", total)
	}
	if sc.Topology.Idle > 0 && total.Idle == 0 {
		t.Errorf("no unit went quiet: %+v", total)
	}
	if prunes && min(total.Pruned, total.PrunedOpen) == 0 {
		t.Errorf("no row write dropped a high-water mark, or none of a unit still delivering: %+v", total)
	}
	if sc.Faults.Late > 0 && total.Late == 0 {
		t.Errorf("no write landed late: %+v", total)
	}
	if sc.Poison > 0 && total.Poison == 0 {
		t.Errorf("no event was poison: %+v", total)
	}
	// Only the archive source lists units.
	if sc.Backfill.Within > 0 && (min(total.Backfills, total.Chunks) == 0 || !sc.Backfill.Table && total.Unlisted == 0) {
		t.Errorf("no backfill went live, or no unit was created after a listing: %+v", total)
	}
	checkJobsTier(t, sc, total)
	checkSplitTier(t, sc, total)
	checkRegionsTier(t, sc, total)
	checkBuildsTier(t, sc, total)
}

// checkJobsTier fails t when a tier of sc didn't take both paths of a fresh stream, or didn't
// make the job faults it sets happen.
func checkJobsTier(t *testing.T, sc simulation.Scenario, total simulation.Stats) {
	t.Helper()
	b := sc.Backfill
	if sc.WriteStart > 0 && (total.Fresh == 0 || total.Fresh == total.Backfills) {
		t.Errorf("no job found its stream fresh, or every one did: %+v", total)
	}
	// One seed seldom makes these happen at the rates most scenarios set.
	if *full && b.Crash > 0 && b.Twin > 0 && b.Lose > 0 && min(total.Resumed, total.Twins, total.Lost) == 0 {
		t.Errorf("no job was resumed, had a duplicate worker, or lost its runs: %+v", total)
	}
}

// checkSplitTier fails t when a tier of sc didn't fill a version split by time from the
// table, shift writer clocks, or, where it breaks the split's bounds, land writes late and
// break versions by their split.
func checkSplitTier(t *testing.T, sc simulation.Scenario, total simulation.Stats) {
	t.Helper()
	ts := sc.TimeSplit
	if sc.Backfill.Table && total.TimeSplits == 0 {
		t.Errorf("no version split by time went live: %+v", total)
	}
	if ts.Skew > 0 && total.Skewed == 0 {
		t.Errorf("no writer clock shifted an append: %+v", total)
	}
	if ts.Late > 0 && min(total.LateLandings, total.SplitBreaks) == 0 {
		t.Errorf("no write landed late, or no time split broke: %+v", total)
	}
}

// checkBuildsTier fails t when a tier of sc didn't change builds, fail a batch under G3 where
// a build lacks active versions, or write a tombstone.
func checkBuildsTier(t *testing.T, sc simulation.Scenario, total simulation.Stats) {
	t.Helper()
	b := sc.Builds
	if b != (simulation.Builds{}) && total.Deploys == 0 {
		t.Errorf("no region changed builds: %+v", total)
	}
	if (b.Switch > 0 || b.Flap > 0) && total.Guarded == 0 {
		t.Errorf("no batch failed under G3: %+v", total)
	}
	if b.Retire > 0 && total.Tombstones == 0 {
		t.Errorf("no region wrote a tombstone: %+v", total)
	}
}

// checkRegionsTier fails t when a tier of sc didn't exercise its cross-region duplicates,
// isolations or deletes.
func checkRegionsTier(t *testing.T, sc simulation.Scenario, total simulation.Stats) {
	t.Helper()
	if sc.Duplicate > 0 && min(total.Modifies, total.Divergent, total.Reported) == 0 {
		t.Errorf("no event was appended in two regions before either replicated, none with two payloads, or no core reported one: %+v", total)
	}
	if sc.Isolation.Share > 0 && min(total.Cuts, total.Held) == 0 {
		t.Errorf("no region was cut off, or no replicated write was held back: %+v", total)
	}
	if sc.Sweep.Delay > 0 && total.Removes == 0 {
		t.Errorf("no event expired: %+v", total)
	}
}

func add(total *simulation.Stats, s simulation.Stats) {
	a, b := reflect.ValueOf(total).Elem(), reflect.ValueOf(s)
	for i := range a.NumField() {
		a.Field(i).SetInt(a.Field(i).Int() + b.Field(i).Int())
	}
}

// TestDeterministic runs two seeds of late, which has every fault, twice each: a seed's run
// is the same every time, and two seeds differ. The full tier adds regions, which cuts
// regions off and deletes events, and runs four seeds of each.
func TestDeterministic(t *testing.T) {
	t.Parallel()
	sys := newPageViews(t)
	names, n := []string{"late"}, 2
	if *full {
		names, n = append(names, "regions"), 4
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			replay(t, scenario(t, name), sys, n)
		})
	}
}

// replay runs the first n seeds of sc twice each, every run in parallel.
func replay(t *testing.T, sc simulation.Scenario, sys simulation.System, n int) {
	t.Helper()
	runs := make([][2]simulation.Outcome, n)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		traces := map[uint64]bool{}
		for seed, r := range runs {
			if !reflect.DeepEqual(r[0], r[1]) {
				t.Errorf("seed %d: run 1 = %+v\nrun 2 = %+v", seed, r[0], r[1])
			}
			if r[0].Steps == 0 {
				t.Errorf("seed %d: no steps", seed)
			}
			traces[r[0].Trace] = true
		}
		if len(traces) != n {
			t.Errorf("%d seeds gave %d traces", n, len(traces))
		}
	})
	for seed := range n {
		for i := range runs[seed] {
			t.Run(fmt.Sprintf("%d/run%d", seed, i+1), func(t *testing.T) {
				t.Parallel()
				runs[seed][i] = simulation.Run(t, sc, sys, uint64(seed))
			})
		}
	}
}

// expectCaught runs n seeds of sc in parallel, and fails unless one violates a property
// whose violations start with want, such as "P1:".
func expectCaught(t *testing.T, control, want string, sc simulation.Scenario, sys simulation.System, n int) {
	t.Helper()
	var caught atomic.Int64
	t.Cleanup(func() {
		t.Logf("control %s: caught in %d/%d seeds by %s", control, caught.Load(), n, want)
		if caught.Load() == 0 {
			t.Errorf("the harness did not catch control %s by %s", control, want)
		}
	})
	for seed := range n {
		t.Run(strconv.Itoa(seed), func(t *testing.T) {
			t.Parallel()
			out := simulation.Run(t, sc, sys, uint64(seed))
			if i := slices.IndexFunc(out.Violations, func(v string) bool { return strings.HasPrefix(v, want) }); i >= 0 {
				caught.Add(1)
				t.Logf("caught by %.200s", out.Violations[i])
			} else if len(out.Violations) > 0 {
				t.Logf("not caught by %s, but by %.200s", want, out.Violations[0])
			}
		})
	}
}

// envControl is a control that changes the environment or the configuration, not the
// code: a scenario, and the system it runs.
type envControl struct {
	sc  simulation.Scenario
	sys simulation.System
}

func environmentControls(t *testing.T) map[string]envControl {
	t.Helper()
	sys := newPageViews(t)
	pf2 := scenario(t, "clean")
	pf2.ESM.PF = 2
	pruning := scenario(t, "d23a")
	pruning.ESM.PF = 2
	lie := scenario(t, "handoff")
	lie.Archive.Lie = true
	return map[string]envControl{
		"C3":     {pf2, sys},
		"C16-C3": {pruning, sys},
		"H2":     {lie, sys},
		// S9's baseline: cores that never prune keep the marks of units that finished long
		// ago, against P4, while P4 checks the catalog's W.
		"B1": {scenario(t, "d23a"), sys.withCoreWindow(t, catalog.MaxPruneWindowSeconds*time.Second)},
	}
}

// TestControl runs one negative control, an environment control or a code mutant that
// go run ./internal/simulation/mutants applied, and passes only if the harness catches it.
func TestControl(t *testing.T) {
	t.Parallel()
	if *controlID == "" {
		t.Skip("set -sim.control to run a negative control")
	}
	c, ok := environmentControls(t)[*controlID]
	if !ok {
		c = envControl{scenario(t, *scenarioName), newPageViews(t)}
	}
	if *want == "" {
		t.Fatal("set -sim.want to the property the control breaks")
	}
	expectCaught(t, *controlID, *want, c.sc, c.sys, seeds(c.sc))
}

// TestParallelizationFactorControl is the smoke tier's environment control: two batches
// of one unit at once lose updates. Every seed catches it, so two are enough.
func TestParallelizationFactorControl(t *testing.T) {
	t.Parallel()
	c := environmentControls(t)["C3"]
	expectCaught(t, "C3", "P1:", c.sc, c.sys, 2)
}

// TestPropertiesCatchBugs runs the real core with one defect each and expects the
// property that defect breaks.
func TestPropertiesCatchBugs(t *testing.T) {
	t.Parallel()
	small := scenario(t, "clean")
	small.Regions, small.Events, small.Budget = 1, 60, 10*time.Minute
	empty := small
	empty.ESM.Empty = 0.5
	pruned := small
	pruned.Topology.UnitRecords, pruned.WriteGap = 2, 5*time.Second
	sys := newPageViews(t)
	// P4 checks a W of a second, and the cores prune with pageViews' W of minutes.
	shortW := *sys
	shortW.cat = fixtureWindow(t, time.Second)
	for _, tc := range []struct {
		name     string
		sc       simulation.Scenario
		sys      *pageViews
		bug      bug
		property string
	}{
		{"high-water marks kept past P4's window", pruned, &shortW, noBug, "P4:"},
		{"a unit never drains", small, sys, alwaysFail, "P6:"},
		{"a panic", small, sys, panics, "P8:"},
		{"a request for an empty batch", empty, sys, idleCall, "P9:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := simulation.Run(t, tc.sc, &buggy{pageViews: tc.sys, bug: tc.bug}, 1)
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
	idleCall       // reads the store for a batch with neither INSERT nor MODIFY
)

// buggy is pageViews with a defect in its cores.
type buggy struct {
	*pageViews

	bug  bug
	once atomic.Bool // for bugs that strike once per run
}

func (s *buggy) NewCore(b store.Backend, a simulation.Archiver, cat *projector.Compiled) simulation.Core {
	return buggyCore{Core: s.pageViews.NewCore(b, a, cat), sys: s, b: b}
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
	case c.sys.bug == idleCall && !slices.ContainsFunc(b.Records, func(r projector.Record) bool { return r.Op != projector.Remove }):
		_, err := c.b.Get(ctx, metaTable(), store.Key{PK: "view", SK: pageRates}, true)
		return projector.Result{}, err
	}
	return c.Core.Handle(ctx, b)
}

// TestChangeStreamCheck: P0 catches a change stream that repeats an append, lacks one, or
// holds one that no append wrote; that lacks the REMOVE of a deleted item, holds one of an
// item never deleted, or one before its INSERT; or that holds a MODIFY of an event appended
// in one region only.
func TestChangeStreamCheck(t *testing.T) {
	t.Parallel()
	a, b := event("a"), event("b")
	const narrow, wide = "999999999999999999999", "1000000000000000000000"
	ins := func(it store.Item, seq string) projector.Record {
		return projector.Record{Op: projector.Insert, Seq: seq, New: it}
	}
	mod := func(it store.Item, seq string) projector.Record {
		return projector.Record{Op: projector.Modify, Seq: seq, Old: it, New: it}
	}
	rem := func(it store.Item, seq string) projector.Record {
		return projector.Record{Op: projector.Remove, Seq: seq, Old: it}
	}
	for _, tc := range []struct {
		name             string
		applied, deleted []store.Item
		records          []projector.Record
		want             int
	}{
		{"each append once", []store.Item{a, b}, nil, []projector.Record{ins(b, narrow), ins(a, wide)}, 0},
		{"an append twice", []store.Item{a, b}, nil, []projector.Record{ins(a, narrow), ins(b, narrow), ins(a, wide)}, 1},
		{"an append missing", []store.Item{a, b}, nil, []projector.Record{ins(a, narrow)}, 1},
		{"a record without an append", []store.Item{a}, nil, []projector.Record{ins(a, narrow), ins(b, wide)}, 1},
		{"a REMOVE past the width boundary", []store.Item{a}, []store.Item{a}, []projector.Record{ins(a, narrow), rem(a, wide)}, 0},
		{"a REMOVE missing", []store.Item{a}, []store.Item{a}, []projector.Record{ins(a, narrow)}, 1},
		{"a REMOVE of an item never deleted", []store.Item{a}, nil, []projector.Record{ins(a, narrow), rem(a, wide)}, 1},
		{"a REMOVE before its INSERT", []store.Item{a}, []store.Item{a}, []projector.Record{rem(a, narrow), ins(a, wide)}, 1},
		{"a MODIFY of an event appended twice", []store.Item{a, a}, nil, []projector.Record{ins(a, narrow), mod(a, wide)}, 0},
		{"a MODIFY of an event appended once", []store.Item{a}, nil, []projector.Record{ins(a, narrow), mod(a, wide)}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := simulation.CheckChangeStream(tc.applied, tc.deleted, tc.records)
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
// other, P3 the breakers with the records that fail, a table item's by its event_id, and P10
// the views split by time; a broken view is left out of P1 and P2, a row that a divergent
// duplicate reached out of P2, and out of P1 for a view split by time.
func TestViewChecks(t *testing.T) {
	t.Parallel()
	row := simulation.RowID{View: pageHourly, Key: "p1"}
	broken := simulation.RowID{View: pageRates, Key: "p1"}
	poison := simulation.RecordID{Unit: "U1", Seq: "100000000000000000001", EventID: "E1"}
	oracle := simulation.NewOracle()
	oracle.Count(row, "views", 2)
	oracle.Poison(pageRates, poison)
	state := func(views, rate string, breaker *simulation.Breaker) simulation.State {
		st := simulation.State{Rows: simulation.Rows{row: {"views": views}, broken: {"rate": rate}}, Broken: map[string]simulation.Breaker{}}
		if breaker != nil {
			st.Broken[pageRates] = *breaker
		}
		return st
	}
	by := func(rec simulation.RecordID, split bool) *simulation.Breaker {
		return &simulation.Breaker{Record: rec, Split: split}
	}
	other := simulation.RecordID{Unit: "U1", Seq: "100000000000000000002", EventID: "E2"}
	divergent := map[simulation.RowID]bool{row: true}
	timeSplit := map[string]bool{pageHourly: true}
	for _, tc := range []struct {
		name      string
		r0, r1    simulation.State
		skip      map[simulation.RowID]bool
		timeSplit map[string]bool
		want      []string
	}{
		{"exact, a broken view differs", state("2", "1", by(poison, false)), state("2", "2", by(poison, false)), nil, nil, nil},
		{"a count off in r0", state("3", "1", by(poison, false)), state("3", "1", by(poison, false)), nil, nil, []string{"P1"}},
		{"regions differ", state("2", "1", by(poison, false)), state("1", "1", by(poison, false)), nil, nil, []string{"P2"}},
		{"regions differ in a divergent row", state("2", "1", by(poison, false)), state("1", "1", by(poison, false)), divergent, nil, nil},
		{"broken by a record that doesn't fail it", state("2", "1", by(other, false)), state("2", "1", by(other, false)), nil, nil, []string{"P3"}},
		{"broken by a table item that fails it", state("2", "1", by(simulation.RecordID{EventID: "E1"}, false)), state("2", "1", nil), nil, nil, nil},
		{"broken by a table item that doesn't", state("2", "1", by(simulation.RecordID{EventID: "E2"}, false)), state("2", "1", nil), nil, nil, []string{"P3"}},
		{"broken by its time split", state("2", "1", by(other, true)), state("2", "1", nil), nil, nil, nil},
		{"not broken", state("2", "1", nil), state("2", "1", nil), nil, nil, []string{"P1", "P3"}},
		{"a view split by time off", state("3", "1", by(poison, false)), state("3", "1", by(poison, false)), nil, timeSplit, []string{"P1", "P10"}},
		{"a view split by time off in a divergent row", state("3", "1", by(poison, false)), state("3", "1", by(poison, false)), divergent, timeSplit, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			violations := simulation.CheckViews(oracle, tc.r0, tc.r1, tc.skip, tc.timeSplit)
			var got []string
			for _, v := range violations {
				property, _, _ := strings.Cut(v, ":")
				got = append(got, property)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("violations %q, want properties %q", violations, tc.want)
			}
		})
	}
}

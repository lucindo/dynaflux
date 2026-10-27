// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:mnd // Scenario presets: their numbers are the scenarios.
package simulation

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Scenarios returns the named scenarios: clean has no faults; faults has transient,
// ambiguous and retried writes, crashes, latency spikes, slow invocations, a replaying
// mapping, and duplicate, stale and empty deliveries; poison is faults with 2% of events
// failing a view's evaluation; handoff is faults with backfills that start while writers
// append for minutes, 1% poison events, and cross-region duplicates. d23a, long and late
// outlive the prune window W many times: d23a is handoff over an hour with many short units,
// idle units, and more slow and stale invocations; long has few long units; late is d23a with
// writes that land after their invocation's deadline. regions is d23a with regions cut off
// for up to two hours, more cross-region duplicates, events that expire a day after their
// append, a delete each region makes on its own and replicates, and replays and backfills
// that start up to a day in. guardScenarios, jobScenarios and tableScenarios add eight more.
func Scenarios() []Scenario {
	clean := Scenario{
		Name:     "clean",
		Regions:  3,
		Events:   250,
		Writers:  4,
		WriteGap: 200 * time.Millisecond,
		Faults:   Faults{Latency: 10 * time.Millisecond},
		Topology: Topology{
			Partitions:  4,
			UnitRecords: 40,
			Split:       0.2,
			MaxDepth:    2,
		},
		ESM: ESM{
			Timeout:  2 * time.Second,
			Window:   time.Second,
			MaxBatch: 12,
			PF:       1,
		},
		Archive: Archive{MinDelay: time.Minute, MaxDelay: 15 * time.Minute, Dup: 0.05, Error: 0.02},
		Budget:  24 * time.Hour,
	}
	// The scenarios below keep these seeds unless they say otherwise.
	clean.Seeds.Smoke, clean.Seeds.Full = 1, 300

	faults := clean
	faults.Name = "faults"
	faults.Faults = Faults{
		Transient: 0.05,
		Ambiguous: 0.05,
		Retried:   0.05,
		Crash:     0.003,
		Spike:     0.01,
		Latency:   10 * time.Millisecond,
		SpikeMax:  3 * time.Second,
	}
	faults.ESM.Replay = 30 * time.Second
	faults.ESM.Dup, faults.ESM.Stale, faults.ESM.Empty, faults.ESM.Slow = 0.1, 0.1, 0.05, 0.1

	poison := faults
	poison.Name = "poison"
	poison.Poison = 0.02
	// A core that failed a poison record's batch would block its unit for the whole budget.
	poison.Budget = time.Hour
	// handoff and the scenarios after it have poison events in the smoke tier.
	poison.Seeds.Smoke = 0

	// Writes span about four minutes, so units are created after a backfill lists them,
	// which it does meta.DefaultBounds.Listing after the activation.
	handoff := faults
	handoff.Name = "handoff"
	handoff.WriteGap = 8 * time.Second
	handoff.Poison = 0.01
	handoff.Duplicate = 0.3
	// A step lives 3 minutes, so it works for 2. Now and then one dies, a duplicate worker
	// steps alongside, or the staged runs are lost.
	handoff.Backfill = Backfill{Within: 5 * time.Minute, Chunk: 8, Step: 3 * time.Minute, Crash: 0.02, Twin: 0.02, Lose: 0.05}
	handoff.Budget = time.Hour
	// Invocations that outlive meta.DefaultBounds.Listing: one that read the view states
	// before an activation can still be running when its unit is fenced. Seeds seldom line up
	// the races that follow, so scripted tests pin those (handoff_test.go). Spikes keep their
	// share of the timeout, so some still time an invocation out.
	handoff.ESM.Timeout, handoff.Faults.SpikeMax = 2*time.Minute, 3*time.Minute

	// P4 checks the catalog's W. The timings suit a W of a few minutes, the least a
	// 2-minute timeout allows: writes span an hour and page_views has about 40 units per
	// region, so hot rows are written long after most units finished, and an idle unit
	// stays quiet for longer than W. Half the invocations stall near their deadline and a
	// third of the batches get a stale twin, so an invocation often reads progress before a
	// late rise and writes a row long after: the race that pruning's time rule guards.
	d23a := handoff
	d23a.Name = "d23a"
	d23a.WriteGap = 2 * time.Minute
	d23a.Topology.UnitRecords = 8
	d23a.Topology.Idle, d23a.Topology.IdleFor = 0.05, 20*time.Minute
	d23a.ESM.Slow, d23a.ESM.Stale = 0.5, 0.3
	d23a.Backfill.Within = 40 * time.Minute
	// A backfill step outlives W: it neither skips by progress nor writes live marks, and P4
	// allows its writes the time it lives.
	d23a.Backfill.Step = 5 * time.Minute
	d23a.Budget = 3 * time.Hour

	// Two generations of units: a mark of a unit still delivering becomes prunable while
	// the unit is idle.
	long := d23a
	long.Name = "long"
	long.Events = 1000
	long.WriteGap = 30 * time.Second
	long.Topology.UnitRecords, long.Topology.Split = 200, 0
	long.Budget = 8 * time.Hour
	// A seed takes three times d23a's, too long for the smoke tier.
	long.Seeds.Smoke = 0

	// A write lands up to the margin W keeps over the longest invocation after its deadline.
	late := d23a
	late.Name = "late"
	late.Faults.Late, late.Faults.LateMax = 0.01, catalog.LifetimeMarginSeconds*time.Second

	// A run lasts until every event's delete is delivered: its _ttl is a day and an hour
	// after the append, and the delete comes up to Sweep.Delay later. A mapping that replays
	// near the end of the 1-day log retention needs unit items that outlive their records,
	// and a backfill that starts after the deletes reads an archive that has seen REMOVEs.
	regions := d23a
	regions.Name = "regions"
	regions.Duplicate = 0.5
	regions.Isolation = Isolation{Share: 0.5, Within: time.Hour, Max: 2 * time.Hour}
	regions.Sweep = Sweep{Delay: 2 * time.Hour}
	regions.ESM.Replay, regions.Backfill.Within, regions.Budget = 22*time.Hour, 27*time.Hour, 40*time.Hour

	return slices.Concat([]Scenario{clean, faults, poison, handoff, d23a, long, late, regions}, guardScenarios(handoff),
		jobScenarios(handoff), tableScenarios(handoff))
}

// tableScenarios returns the scenarios of jobs that read the stream table, from handoff:
// timesplit, where every bound of the time split holds; timesplit-late, which breaks each of
// them; and table, with resume's faults.
func tableScenarios(handoff Scenario) []Scenario {
	// Writes span about 16 minutes, past every split time. Writer clocks are up to ε off, a
	// replicated write lands within L of its ingest_time, and invocations end in seconds, so
	// a batch delivers a record within ε of its landing.
	timesplit := handoff
	timesplit.Name = "timesplit"
	timesplit.WriteGap = 30 * time.Second
	timesplit.Backfill.Table = true
	timesplit.TimeSplit = TimeSplit{Skew: 5 * time.Minute, Landing: 15 * time.Minute, Spread: true}
	timesplit.ESM.Timeout, timesplit.Faults.SpikeMax = 2*time.Second, 3*time.Second
	timesplit.Budget = 2 * time.Hour

	// Some writes land up to half an hour past L, a client fails over to a second region with
	// another clock, units stall for up to half an hour, and each region returns to a build
	// without the backfilled versions for a while, which ends before any late bound. Rare
	// duplicates and many units leave most versions unbroken otherwise, and a straddling
	// MODIFY often alone in a batch of the older build.
	late := timesplit
	late.Name = "timesplit-late"
	late.TimeSplit.Late, late.TimeSplit.LateMax, late.TimeSplit.Failover = 0.01, 30*time.Minute, true
	late.Topology.Idle, late.Topology.IdleFor = 0.005, 30*time.Minute
	late.Builds.Flap = 13 * time.Minute
	late.Duplicate, late.Topology.Partitions = 0.2, 16
	late.Budget = 3 * time.Hour
	late.Seeds.Full = 100

	// At 2 records a second, a job stages and applies for minutes, so steps and duplicate
	// workers end partway through it.
	table := handoff
	table.Name = "table"
	table.Backfill.Table, table.Backfill.Rate = true, 2
	table.Backfill.Crash, table.Backfill.Twin, table.Backfill.Lose = 0.3, 0.1, 0.5
	table.Budget = 2 * time.Hour
	table.Seeds.Full = 100
	return []Scenario{timesplit, late, table}
}

// splitBoundsHold reports whether sc keeps every bound of the time split: no replicated write
// lands past L, no client fails over to a second clock, no unit idles, no region is cut off
// and no build lacks a version. Then only a MODIFY that straddles a split time breaks one.
func (sc Scenario) splitBoundsHold() bool {
	return sc.TimeSplit.Late == 0 && !sc.TimeSplit.Failover && sc.Topology.Idle == 0 &&
		sc.Isolation == (Isolation{}) && sc.Builds == (Builds{})
}

// jobScenarios returns the scenarios of a backfill job's life, from handoff: fresh, where
// writers start a while into the run, so some jobs find their stream fresh and go live
// without a read; and resume, where steps die often, duplicate workers step alongside, and
// staged runs are lost.
func jobScenarios(handoff Scenario) []Scenario {
	// Short units finish within the listing delay, so a unit with events can be one that the
	// listing no longer returns, and only the scan finds its events.
	fresh := handoff
	fresh.Name = "fresh"
	fresh.WriteStart = 5 * time.Minute
	fresh.Backfill.Within = 3 * time.Minute
	fresh.Topology.UnitRecords = 4
	fresh.Budget = 2 * time.Hour

	resume := handoff
	resume.Name = "resume"
	resume.Backfill.Crash, resume.Backfill.Twin, resume.Backfill.Lose = 0.3, 0.1, 0.5
	resume.Budget = 2 * time.Hour
	return []Scenario{fresh, resume}
}

// guardScenarios returns the scenarios of activation's guards and of retired versions, from
// handoff: guards, with regions that run a build without the backfilled versions until a
// while after the backfills start; guards-table, guards with jobs that read the stream table;
// rollback, guards with a return to that build once a region's backfills ended; and retire,
// with a live version retired, regions that move to a build that lists it as retired and
// back to one that still defines it, and tombstones.
func guardScenarios(handoff Scenario) []Scenario {
	// Each region's backfills activate at once while its cores still run a build without
	// their versions, for a while: those that read the states before fail under G3 once they
	// read again, and fences keep the rest out of each version, idempotent ones too (G2).
	// Short units close only on their next record, so a unit's child can start after a
	// listing that found nothing left in the unit. Writes span about eight minutes, and
	// archived records arrive two minutes after their put, so one put after a backfill's cut
	// misses its read.
	guards := handoff
	guards.Name = "guards"
	guards.Topology.UnitRecords, guards.Topology.CloseOnNext = 10, true
	guards.WriteGap = 16 * time.Second
	guards.Backfill.Within, guards.Backfill.Together = 3*time.Minute, true
	guards.Archive.MinDelay, guards.Archive.MaxDelay = 2*time.Minute, 2*time.Minute
	guards.Builds.Switch = 10 * time.Minute

	// A version that isn't split is read once every projector knows it is activating: a batch
	// of the older build that read the states before then fails under G3 once it reads them
	// again, rather than consume a record that the scan passed. Jobs start within a minute,
	// while most of a row's records are still to come.
	guardsTable := guards
	guardsTable.Name = "guards-table"
	guardsTable.Backfill.Table, guardsTable.Backfill.Within = true, time.Minute
	guardsTable.Budget = 2 * time.Hour
	guardsTable.Seeds.Full = 100

	// Records keep coming long after the backfills ended, so old cores meet live versions.
	rollback := guards
	rollback.Name = "rollback"
	rollback.WriteGap = time.Minute
	rollback.Builds.Rollback = 10 * time.Minute
	rollback.Budget = 2 * time.Hour
	rollback.Seeds.Full = 100

	retire := handoff
	retire.Name = "retire"
	retire.WriteGap = 20 * time.Second
	retire.Builds.Retire = 2 * time.Minute
	retire.Seeds.Full = 100
	return []Scenario{guards, guardsTable, rollback, retire}
}

func (sc Scenario) check() error {
	f, e := sc.Faults, sc.ESM
	switch {
	case sc.Regions < 1 || sc.Writers < 1 || sc.Events < 0 || sc.WriteGap < 0:
		return errors.New("needs a region, a writer, and no negative event count or write gap")
	case sc.Poison < 0 || sc.Poison > 1 || sc.Duplicate < 0 || sc.Duplicate > 1:
		return errors.New("needs poison and duplicate shares from 0 to 1")
	case e.Timeout <= 0 || e.MaxBatch < 1 || e.PF < 1 || e.Window < 0 || e.Replay < 0:
		return errors.New("needs a timeout, a batch size, a parallelization factor and no negative window or replay start")
	case f.Spike > 0 && f.SpikeMax <= 0, f.Late > 0 && f.LateMax <= 0:
		return errors.New("a spike or late rate needs its maximum")
	case sc.Archive.MinDelay < 0 || sc.Archive.MaxDelay < sc.Archive.MinDelay:
		return fmt.Errorf("archive delays %v to %v", sc.Archive.MinDelay, sc.Archive.MaxDelay)
	case sc.Budget <= 0:
		return errors.New("needs a budget")
	}
	if err := sc.checkBuilds(); err != nil {
		return err
	}
	if err := sc.checkBackfill(); err != nil {
		return err
	}
	return sc.checkRegions()
}

func (sc Scenario) checkBuilds() error {
	if b := sc.Builds; b.Switch < 0 || b.Rollback < 0 || b.Retire < 0 || b.Flap < 0 || b.Rollback > 0 && b.Switch == 0 ||
		b.Retire > 0 && b.Switch > 0 || b.Flap > 0 && (b.Switch > 0 || b.Retire > 0) {
		return errors.New("builds need no negative time, a switch before a rollback, and a retire or a flap on its own")
	}
	return nil
}

// checkBackfill checks the backfills and when writers start.
func (sc Scenario) checkBackfill() error {
	b := sc.Backfill
	switch {
	case b.Within < 0 || b.Within > 0 && (b.Chunk < 1 || b.Step <= time.Minute):
		return errors.New("a backfill needs a chunk size, and steps longer than their minute of reserve")
	case b.Crash < 0 || b.Crash > 1 || b.Twin < 0 || b.Twin > 1 || b.Lose < 0 || b.Lose > 1 || b.Rate < 0:
		return errors.New("needs crash, twin and lose shares from 0 to 1, and no negative rate")
	case sc.WriteStart < 0:
		return errors.New("needs no negative write start")
	}
	ts := sc.TimeSplit
	if ts.Skew < 0 || ts.Landing < 0 || ts.Late < 0 || ts.Late > 1 || ts.Late > 0 && (ts.LateMax <= 0 || ts.Landing == 0) {
		return errors.New("a time split needs no negative bound, and a late share from 0 to 1 with a landing bound and a maximum")
	}
	return nil
}

// checkRegions checks what shapes a region's change streams and replication.
func (sc Scenario) checkRegions() error {
	topo, iso := sc.Topology, sc.Isolation
	switch {
	case topo.Partitions < 1 || topo.UnitRecords < 1 || topo.MaxDepth < 0:
		return errors.New("needs a partition, a positive unit size and no negative depth")
	case topo.Idle < 0 || topo.Idle > 1 || topo.Idle > 0 && topo.IdleFor <= 0:
		return errors.New("an idle share needs to be from 0 to 1, and a quiet time")
	case iso.Share < 0 || iso.Share > 1 || iso.Within < 0 || iso.Share > 0 && iso.Max <= 0:
		return errors.New("an isolation share needs to be from 0 to 1, a start and a longest cut")
	case sc.Sweep.Delay < 0:
		return errors.New("needs no negative sweep delay")
	}
	return nil
}

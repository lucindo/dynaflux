// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:mnd // Scenario presets: their numbers are the scenarios.
package simulation

import (
	"errors"
	"fmt"
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
// that start up to a day in.
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
	// which it does meta.ListingDelay after the activation.
	handoff := faults
	handoff.Name = "handoff"
	handoff.WriteGap = 8 * time.Second
	handoff.Poison = 0.01
	handoff.Duplicate = 0.3
	handoff.Backfill = Backfill{Within: 5 * time.Minute, Chunk: 8}
	handoff.Budget = time.Hour
	// Invocations that outlive meta.ListingDelay: one that read the view states before an
	// activation can still be running when its unit is fenced. Seeds seldom line up the
	// races that follow, so scripted tests pin those (handoff_test.go). Spikes keep their
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
	return []Scenario{clean, faults, poison, handoff, d23a, long, late, regions}
}

func (sc Scenario) check() error {
	f, e := sc.Faults, sc.ESM
	switch {
	case sc.Regions < 1 || sc.Writers < 1 || sc.Events < 0 || sc.WriteGap < 0:
		return errors.New("needs a region, a writer, and no negative event count or write gap")
	case sc.Poison < 0 || sc.Poison > 1 || sc.Duplicate < 0 || sc.Duplicate > 1:
		return errors.New("needs poison and duplicate shares from 0 to 1")
	case sc.Backfill.Within < 0 || sc.Backfill.Within > 0 && sc.Backfill.Chunk < 1:
		return errors.New("a backfill needs a chunk size")
	case e.Timeout <= 0 || e.MaxBatch < 1 || e.PF < 1 || e.Window < 0 || e.Replay < 0:
		return errors.New("needs a timeout, a batch size, a parallelization factor and no negative window or replay start")
	case f.Spike > 0 && f.SpikeMax <= 0, f.Late > 0 && f.LateMax <= 0:
		return errors.New("a spike or late rate needs its maximum")
	case sc.Archive.MinDelay < 0 || sc.Archive.MaxDelay < sc.Archive.MinDelay:
		return fmt.Errorf("archive delays %v to %v", sc.Archive.MinDelay, sc.Archive.MaxDelay)
	case sc.Budget <= 0:
		return errors.New("needs a budget")
	}
	return sc.checkRegions()
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

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:mnd // Scenario presets: their numbers are the scenarios.
package simulation

import (
	"errors"
	"fmt"
	"time"
)

// Scenarios returns the named scenarios: clean has no faults; faults has transient,
// ambiguous and retried writes, crashes, latency spikes, slow invocations, a replaying
// mapping, and duplicate, stale and empty deliveries.
func Scenarios() []Scenario {
	clean := Scenario{
		Name:    "clean",
		Regions: 3,
		Events:  250,
		Writers: 4,
		Faults:  Faults{Latency: 10 * time.Millisecond},
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
	clean.Seeds.Smoke, clean.Seeds.Full = 4, 50

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
	faults.ESM.Replay = true
	faults.ESM.Dup, faults.ESM.Stale, faults.ESM.Empty, faults.ESM.Slow = 0.1, 0.1, 0.05, 0.1
	faults.Seeds.Smoke, faults.Seeds.Full = 6, 300
	return []Scenario{clean, faults}
}

func (sc Scenario) check() error {
	f, e := sc.Faults, sc.ESM
	switch {
	case sc.Regions < 1 || sc.Writers < 1 || sc.Events < 0:
		return errors.New("needs a region, a writer and no negative event count")
	case sc.Topology.Partitions < 1 || sc.Topology.UnitRecords < 1 || sc.Topology.MaxDepth < 0:
		return errors.New("needs a partition, a positive unit size and no negative depth")
	case e.Timeout <= 0 || e.MaxBatch < 1 || e.PF < 1 || e.Window < 0:
		return errors.New("needs a timeout, a batch size, a parallelization factor and no negative window")
	case f.Spike > 0 && f.SpikeMax <= 0, f.Late > 0 && f.LateMax <= 0:
		return errors.New("a spike or late rate needs its maximum")
	case sc.Archive.MinDelay < 0 || sc.Archive.MaxDelay < sc.Archive.MinDelay:
		return fmt.Errorf("archive delays %v to %v", sc.Archive.MinDelay, sc.Archive.MaxDelay)
	case sc.Budget <= 0:
		return errors.New("needs a budget")
	}
	return nil
}

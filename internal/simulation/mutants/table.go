// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:mnd // The table: its seed counts are data.
package main

// control is one negative control. With File set it is a code mutant: Old, which must
// occur exactly once in File, becomes New. Without, TestControl changes the environment or
// the configuration itself. TestControl catches a control when a seed of Scenario violates
// a property; a control with Test set is caught when that test fails instead.
type control struct {
	ID       string
	File     string // relative to the repository root
	Old, New string
	Scenario string
	Seeds    int
	Test     string
}

// The code mutants act on the stand-in core of the harness's tests until the projector
// replaces it.
func table() []control {
	const (
		standIn = "internal/simulation/actor_test.go"
		kill    = "!now.Before(inv.deadline)"
	)
	return []control{
		{
			ID:       "C2", // a marker counts only when this attempt wrote it
			File:     standIn,
			Old:      "return it[catalog.AttrMarkerOwner].Str == owner, nil",
			New:      "return false, nil",
			Scenario: "faults",
			Seeds:    20,
		},
		{ID: "C3", Scenario: "clean", Seeds: 50}, // ParallelizationFactor 2
		{
			ID:   "C15d", // a request at the deadline is sent
			File: "internal/simulation/region.go",
			Old:  kill,
			New:  "now.After(inv.deadline)",
			Test: "TestKillBoundary",
		},
		{
			ID:   "C15e", // the deadline is a microsecond late
			File: "internal/simulation/region.go",
			Old:  kill,
			New:  "!now.Before(inv.deadline.Add(time.Microsecond))",
			Test: "TestKillBoundary",
		},
		{
			ID:       "C21", // sequence numbers compared unpadded
			File:     standIn,
			Old:      "pad(rec.Seq) <= pad(mark)",
			New:      "rec.Seq <= mark",
			Scenario: "clean",
			Seeds:    20,
		},
	}
}

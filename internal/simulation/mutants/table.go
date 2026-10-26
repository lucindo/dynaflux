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

func table() []control {
	const (
		apply  = "internal/projector/apply.go"
		eval   = "internal/projector/eval.go"
		handle = "internal/projector/handle.go"
		kill   = "!now.Before(inv.deadline)"
		breaks = "opened, err := c.meta.MarkBroken(ctx, v.id, f)"
		faults = "faults"
		poison = "poison"
	)
	return []control{
		{
			ID:       "C1", // no high-water marks
			File:     apply,
			Old:      "last := cur.hwm.Units[unitKey]",
			New:      `last := ""`,
			Scenario: faults,
			Seeds:    20,
		},
		{
			ID:       "C2", // a marker counts only when this attempt wrote it
			File:     apply,
			Old:      "return o.Str, nil",
			New:      `return "", nil`,
			Scenario: faults,
			Seeds:    50,
		},
		{ID: "C3", Scenario: "clean", Seeds: 50}, // ParallelizationFactor 2
		{
			ID:       "C4", // a value that repeats within one record and row counts each time
			File:     eval,
			Old:      "if !c.seen[k] {",
			New:      "if true {",
			Scenario: faults,
			Seeds:    50,
		},
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
			File:     handle,
			Old:      "return strings.Compare(a.seq, b.seq)",
			New:      "return strings.Compare(a.Seq, b.Seq)",
			Scenario: faults,
			Seeds:    20,
		},
		{
			ID:   "C24a", // an evaluation error fails the batch
			File: handle,
			Old:  "err := rs.add(v, r, vals, bad)",
			New: `if err := rs.add(v, r, vals, bad); err != nil {
			return err
		}
		var err error`,
			Scenario: poison,
			Seeds:    20,
		},
		{
			ID:   "C24b", // an evaluation error opens every view's breaker
			File: handle,
			Old: breaks + `
		if err != nil {
			return fmt.Errorf("projector: %w", err)
		}
		if opened {
			st.Broken = append(st.Broken, v.id)
		}
		broken[v] = true`,
			New: `for _, v := range active {
			` + breaks + `
			if err != nil {
				return fmt.Errorf("projector: %w", err)
			}
			if opened {
				st.Broken = append(st.Broken, v.id)
			}
			broken[v] = true
		}`,
			Scenario: poison,
			Seeds:    20,
		},
		{
			ID:       "C24c", // a failed MarkBroken is swallowed
			File:     handle,
			Old:      breaks + "\n\t\tif err != nil {",
			New:      breaks + "\n\t\tif false {",
			Scenario: poison,
			Seeds:    100,
		},
	}
}

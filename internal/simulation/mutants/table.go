// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:mnd // The table: its seed counts are data.
package main

import (
	"slices"
	"strings"
)

// control is one negative control. With File set it is a code mutant: Old, which must
// occur exactly once in File, becomes New, and so does each pair of More in turn. Without,
// TestControl changes the environment or the configuration itself. TestControl catches a
// control when a seed of Scenario violates Want, the property that the control's rule
// guards; a control with Test set is caught when that test, of Package, fails instead.
type control struct {
	ID       string
	File     string // relative to the repository root
	Old, New string
	More     [][2]string
	Scenario string
	Seeds    int
	Want     string // the prefix of the violations, such as "P1:"
	Test     string
	Package  string // ./internal/simulation when empty
}

const (
	apply         = "internal/projector/apply.go"
	backfill      = "internal/projector/backfill.go"
	eval          = "internal/projector/eval.go"
	handle        = "internal/projector/handle.go"
	unit          = "internal/meta/unit.go"
	appendGo      = "internal/engine/append.go"
	driver        = "internal/simulation/backfill.go"
	core          = "./internal/projector"
	region        = "internal/simulation/region.go"
	killBoundary  = "TestKillBoundary"
	kill          = "!now.Before(inv.deadline)"
	breaks        = "opened, err := c.meta.MarkBroken(ctx, v.id, f)"
	faults        = "faults"
	poison        = "poison"
	handoff       = "handoff"
	d23a          = "d23a"
	regions       = "regions"
	pruneBoundary = "TestPruneBoundary"
	always        = "if true {"
	prune         = "time.Since(u.ProgressAt) > c.window"
	skip          = "\trecs = slices.DeleteFunc(recs, func(r record) bool { return r.seq <= rc.unit.Progress })"
	advance       = "c.advance(ctx, rc, recs, failed, deadline)"
	archive       = "\tif err = c.archive(ctx, b, recs); err != nil {\n\t\treturn Result{}, err\n\t}"
	statesAt      = "statesAt time.Time                 // when the read that returned states was sent"
	fence         = "\t\tif _, ok := u.Fences[version]; ok {\n\t\t\treturn u, nil\n\t\t}"
	// fenceBody follows fence in EnsureFence, up to the fence's value.
	fenceBody = `
		next := u
		next.Fences = make(map[string]string, len(u.Fences)+1)
		maps.Copy(next.Fences, u.Fences)
		next.Fences[version] = cmp.Or(`
	watermark  = "for !c.rg.archive.completeThrough(cut, time.Now()) {"
	ownerKind  = "keys.Owner(g.owner, m.ct.unitKey, m.ct.seq)"
	selectHead = `	recs := c.read()
	var sel []projector.BackfillRecord
	if !c.do("select", func() error {
		fences, err := c.fences(unitsOf(recs))`
	skipPoison = `			if _, err = c.breakVersion(ctx, v, r, vals, err); err != nil {
				return err
			}
			return fmt.Errorf("projector: backfill of %s: unit %s record %s: %w", version, r.unit, r.Seq, ErrBroken)`
	applyChunks = `	for chunk := range slices.Chunk(sel, r.sc.Backfill.Chunk) {
		if !c.do("apply", func() error { return c.apply(chunk) }) {
			return
		}
	}`
)

// The properties that controls break.
const (
	p1 = "P1:"
	p3 = "P3:"
	p4 = "P4:"
	p6 = "P6:"
	p7 = "P7:"
)

func table() []control {
	cs := slices.Concat(liveControls(), regionControls(), fenceControls(), handoffControls(), backfillControls(),
		orderControls(), applyControls(), pruneControls(), progressControls(), replayControls())
	return append(cs, rerunOnD23a(cs)...)
}

// rerunOnD23a returns C16: the seed controls of C1–C7 again where rows prune (S9).
func rerunOnD23a(cs []control) []control {
	var out []control
	for _, c := range cs {
		if slices.Contains([]string{"C1", "C2", "C3", "C4", "C5", "C7"}, c.ID) {
			c.ID, c.Scenario, c.Seeds = "C16-"+c.ID, d23a, 50
			out = append(out, c)
		}
	}
	return out
}

// liveControls returns the controls of live projection (S1).
func liveControls() []control {
	return []control{
		{
			ID:       "C1", // no high-water marks
			File:     apply,
			Old:      "return ct.seq <= h.Units[ct.unitKey]",
			New:      "return false",
			Scenario: faults,
			Seeds:    20,
			Want:     p1,
		},
		{
			ID:       "C2", // a marker counts only when this attempt wrote it
			File:     apply,
			Old:      "return o.Str, nil",
			New:      `return "", nil`,
			Scenario: faults,
			Seeds:    50,
			Want:     p1,
		},
		{ID: "C3", Scenario: "clean", Seeds: 50, Want: p1}, // ParallelizationFactor 2
		{
			ID:       "C4", // a value that repeats within one record and row counts each time
			File:     eval,
			Old:      "if !c.seen[k] {",
			New:      always,
			Scenario: faults,
			Seeds:    50,
			Want:     p1,
		},
		{
			ID:   "C15d", // a request at the deadline is sent
			File: region,
			Old:  kill,
			New:  "now.After(inv.deadline)",
			Test: killBoundary,
		},
		{
			ID:   "C15e", // the deadline is a microsecond late
			File: region,
			Old:  kill,
			New:  "!now.Before(inv.deadline.Add(time.Microsecond))",
			Test: killBoundary,
		},
		{
			// No deadline kill. d23a doesn't diverge without it, as the core's requests end
			// with their context (P7, C43), so only the scripted boundary shows it.
			ID:   "H1",
			File: region,
			Old:  kill,
			New:  "false",
			Test: killBoundary,
		},
		{
			ID:       "C21", // sequence numbers compared unpadded
			File:     handle,
			Old:      "return strings.Compare(a.seq, b.seq)",
			New:      "return strings.Compare(a.Seq, b.Seq)",
			Scenario: faults,
			Seeds:    20,
			Want:     p1,
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
			Want:     p6,
		},
		{
			ID:   "C24b", // an evaluation error opens every view's breaker
			File: handle,
			Old: `opened, err := c.breakVersion(ctx, v, r, vals, err)
		if err != nil {
			return err
		}
		if opened {
			st.Broken = append(st.Broken, v.id)
		}
		broken[v] = true`,
			New: `cause := err
		for _, v := range rc.views {
			opened, err := c.breakVersion(ctx, v, r, vals, cause)
			if err != nil {
				return err
			}
			if opened {
				st.Broken = append(st.Broken, v.id)
			}
			broken[v] = true
		}`,
			Scenario: poison,
			Seeds:    20,
			Want:     p3,
		},
		{
			ID:       "C24c", // a failed MarkBroken is swallowed
			File:     handle,
			Old:      breaks + "\n\tif err != nil {",
			New:      breaks + "\n\tif false {",
			Scenario: poison,
			Seeds:    100,
			Want:     p3,
		},
	}
}

// regionControls returns the controls of what a region's change stream holds besides its
// own appends, of an append that finds its event stored, and of a batch's rows and unit
// item (S9 requirements 1 and 3).
func regionControls() []control {
	const op = "\t\t\tif err = c.evaluate(ctx, rs, s, r, rc, broken, &res.Stats); err != nil {\n\t\t\t\treturn Result{}, err\n\t\t\t}"
	return []control{
		{
			ID:       "C19", // a MODIFY, a cross-region duplicate, is applied
			File:     handle,
			Old:      "\t\tcase Modify:\n\t\t\tif !s.SamePayload(r.Old, r.New) {\n\t\t\t\tres.Stats.Divergent++\n\t\t\t}",
			New:      "\t\tcase Modify:\n" + op,
			Scenario: regions,
			Seeds:    20,
			Want:     p1,
		},
		{
			ID:       "C20", // a REMOVE, an expired event, is applied as its old image
			File:     handle,
			Old:      "\t\tcase Remove:\n\t\t}",
			New:      "\t\tcase Remove:\n\t\t\tr.New = r.Old\n" + op + "\n\t\t}",
			Scenario: regions,
			Seeds:    20,
			Want:     p1,
		},
		{
			ID:       "C22", // one high-water mark for every unit
			File:     apply,
			Old:      "return ct.seq <= h.Units[ct.unitKey]",
			New:      `return ct.seq <= h.Units[keys.UnitKey("")]`,
			More:     [][2]string{{"h.Units[ct.unitKey] = ct.seq", `h.Units[keys.UnitKey("")] = ct.seq`}},
			Scenario: faults,
			Seeds:    20,
			Want:     p1,
		},
		{
			ID:       "C25", // a unit item expires while a mapping can still deliver its records
			File:     unit,
			Old:      "const unitLifetime = 400 * 24 * time.Hour",
			New:      "const unitLifetime = 12 * time.Hour",
			Scenario: regions,
			Seeds:    20,
			Want:     p1,
		},
		{
			ID:       "C26", // Handle returns with row writes in flight
			File:     apply,
			Old:      "\trows := logicalRows(groups)\n\tfanOut(len(rows), func(r int) {",
			New:      "\trows := logicalRows(groups)\n\tgo fanOut(len(rows), func(r int) {",
			Scenario: faults,
			Seeds:    20,
			Want:     p7,
		},
		{
			ID:       "C45", // an event that another call stored is taken for the call's own
			File:     appendGo,
			Old:      "if s.sameEvent(ce.Current, it) {",
			New:      always,
			Scenario: regions,
			Seeds:    20,
			Want:     "events:",
		},
		{
			ID:       "C46", // an identical cross-region duplicate is reported as divergent
			File:     handle,
			Old:      "if !s.SamePayload(r.Old, r.New) {",
			New:      "if s.SamePayload(r.Old, r.New) {",
			Scenario: regions,
			Seeds:    20,
			Want:     "Stats:",
		},
	}
}

// fenceControls returns the controls of the fences and view states (S1b, D-17).
func fenceControls() []control {
	return []control{
		{
			ID:       "C5", // progress advances past a fence for a version the batch didn't apply
			File:     unit,
			Old:      "if !covered(v) {",
			New:      "if false {",
			Scenario: handoff,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:   "C6", // a fence from the progress read before a write that lost the race
			File: unit,
			Old:  "\tfor range maxAttempts {\n" + fence + fenceBody + "u.Progress, zeroFence())",
			New:  "\tread := u.Progress\n\tfor range maxAttempts {\n" + fence + fenceBody + "read, zeroFence())",
			Test: "TestRecordArchivedWhileFencingReachesTheBackfill",
		},
		{
			ID:   "C17", // units listed as soon as the version is activating
			File: unit,
			Old:  "waitUntil(ctx, vs.ActivatedAt.Add(ListingDelay))",
			New:  "waitUntil(ctx, vs.ActivatedAt)",
			Test: "TestUnitsAreListedOnceEveryCoreSawTheActivation",
		},
		{
			ID:   "C29", // a finished backfill makes a broken version live
			File: "internal/meta/view.go",
			Old:  "case s == Live && vs.State == Activating:",
			New: `case s == Live && (vs.State == Activating || vs.State == Broken):
			vs.Failure = nil`,
			Scenario: handoff,
			Seeds:    100,
			Want:     p3,
		},
		{
			ID:   "C30", // a retried write's own fence is taken for a lost race
			File: unit,
			Old:  "\t\tu = stored\n\t}\n\treturn Unit{}, m.errAttempts",
			New: `		u = stored
		if f, ok := u.Fences[version]; ok && f == next.Fences[version] {
			return Unit{}, fmt.Errorf("meta: fence of %s: %w", version, ErrFenced)
		}
	}
	return Unit{}, m.errAttempts`,
			Scenario: handoff,
			Seeds:    50,
			Want:     p6,
		},
		{
			ID:      "C37", // cached view states never expire
			File:    handle,
			Old:     "time.Since(at) < meta.MaxViewStateAge",
			New:     "time.Since(at) < 1<<62",
			Test:    "TestHandleAppliesFencedVersionAboveItsFence",
			Package: core,
		},
	}
}

// handoffControls returns the controls of a batch's part in the handoff (S1b rule 1, D-12).
func handoffControls() []control {
	return []control{
		{
			ID:   "C33", // a batch archives before it fences its unit
			File: handle,
			Old:  archive + "\n\tvar res Result",
			New:  "\tvar res Result",
			More: [][2]string{{"\trc, err := c.reach(ctx, b.Stream, b.Unit)", archive + "\n\trc, err := c.reach(ctx, b.Stream, b.Unit)"}},
			Test: "TestUnitFencesItselfBeforeItArchives",
		},
		{
			ID:      "C38", // progress advances past records the archive failed to take
			File:    handle,
			Old:     "if err = c.archive(ctx, b, recs); err != nil {",
			New:     "if err = c.archive(ctx, b, recs); false {",
			Test:    "TestHandleFailsWhenTheArchiveFails",
			Package: core,
		},
		{
			ID:      "C40", // an idempotent-only version takes no live records while pending
			File:    handle,
			Old:     "case meta.PendingBackfill:\n\t\t\tif !v.def.FencedHandoff() {",
			New:     "case meta.PendingBackfill:\n\t\t\tif false {",
			Test:    "TestHandleAppliesAdditiveVersionOnlyOnceActivating",
			Package: core,
		},
	}
}

// backfillControls returns the controls of the backfill (S1b rule 2, S9 rule 4).
func backfillControls() []control {
	return []control{
		{ID: "H2", Scenario: handoff, Seeds: 100, Want: p1}, // the archive's watermark lies
		{
			ID:   "C14", // each unit's records in turn, as parallel workers per unit interleave
			File: driver,
			Old:  applyChunks,
			New: `	var units [][]projector.BackfillRecord
	for i, rec := range sel {
		if i == 0 || rec.Unit != sel[i-1].Unit {
			units = append(units, nil)
		}
		units[len(units)-1] = append(units[len(units)-1], rec)
	}
	for len(units) > 0 {
		var left [][]projector.BackfillRecord
		for _, recs := range units {
			n := min(len(recs), r.sc.Backfill.Chunk)
			if !c.do("apply", func() error { return c.apply(recs[:n]) }) {
				return
			}
			if n < len(recs) {
				left = append(left, recs[n:])
			}
		}
		units = left
	}`,
			Scenario: handoff,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:   "C23", // count is taken for idempotent, so its views skip the fenced handoff
			File: "internal/catalog/view.go",
			Old: `	case AggMin, AggMax, AggArgMax, AggArgMin, AggAny, AggUniq, AggUniqExact:
		return true
	case AggCount, AggSum,`,
			New: `	case AggCount, AggMin, AggMax, AggArgMax, AggArgMin, AggAny, AggUniq, AggUniqExact:
		return true
	case AggSum,`,
			Scenario: handoff,
			Seeds:    50,
			Want:     p1,
		},
		{
			ID:       "C27", // a MODIFY's new image is archived too
			File:     handle,
			Old:      "if recs[i].Op == Insert {",
			New:      "if recs[i].Op != Remove {",
			Scenario: handoff,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:       "C27b", // a REMOVE is archived, as its missing new image
			File:     handle,
			Old:      "if recs[i].Op == Insert {",
			New:      "if recs[i].Op != Modify {",
			Scenario: regions,
			Seeds:    20,
			Want:     p3,
		},
		{
			ID:       "C28", // fences only for the units the backfill listed
			File:     driver,
			Old:      "c.fences(unitsOf(recs))",
			New:      "c.fences(c.listed)",
			Scenario: handoff,
			Seeds:    100,
			Want:     p1,
		},
	}
}

// orderControls returns the controls of the backfill's order: the cut follows the
// fences, the archive read follows the watermark, the fences follow the archive read, and the
// version goes live at the end.
func orderControls() []control {
	return []control{
		{
			ID:       "C7", // the backfill reads the archive without waiting for its watermark
			File:     driver,
			Old:      watermark,
			New:      "_ = cut\n\tfor false {",
			Scenario: handoff,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:   "C34", // the cut is taken before the fences are written
			File: unit,
			Old: `	for _, id := range unitIDs {
		u, uerr := m.Unit(ctx, stream, id)
		if uerr == nil {
			_, uerr = m.EnsureFence(ctx, u, version)
		}
		if uerr != nil {
			return time.Time{}, uerr
		}
	}
	return time.Now(), nil`,
			New: `	cut := time.Now()
	for _, id := range unitIDs {
		u, uerr := m.Unit(ctx, stream, id)
		if uerr == nil {
			_, uerr = m.EnsureFence(ctx, u, version)
		}
		if uerr != nil {
			return time.Time{}, uerr
		}
	}
	return cut, nil`,
			Test: "TestRecordArchivedWhileFencingReachesTheBackfill",
		},
		{
			ID:   "C35", // the fences are read at the cut, before the archive
			File: driver,
			Old:  watermark + "\n\t\ttime.Sleep(watermarkPoll)\n\t\tr.sched.gate(c.id, \"watermark\")\n\t}\n" + selectHead,
			New: `var early map[string]string
	c.do("fences", func() error {
		var units []string
		for _, u := range c.stream.units {
			units = append(units, u.id)
		}
		var err error
		early, err = c.fences(units)
		return err
	})
	` + watermark + "\n\t\ttime.Sleep(watermarkPoll)\n\t\tr.sched.gate(c.id, \"watermark\")\n\t}\n" +
				strings.Replace(selectHead, "c.fences(unitsOf(recs))", "early, error(nil)", 1),
			Scenario: handoff,
			Seeds:    50,
			Want:     p1,
		},
		{
			ID:       "C36", // a finished backfill leaves the version activating
			File:     driver,
			Old:      `if c.do("live", func() error { return c.meta.SetViewState(c.ctx, c.version, meta.Live) }) {`,
			New:      always,
			Scenario: handoff,
			Seeds:    20,
			Want:     p6,
		},
	}
}

// applyControls returns the controls of ApplyBackfill (S9 rule 4).
func applyControls() []control {
	return []control{
		{
			ID:      "C31a", // the backfill skips a record whose evaluation fails
			File:    backfill,
			Old:     skipPoison,
			New:     "\t\t\tcontinue",
			Test:    "TestApplyBackfillOpensTheBreaker",
			Package: core,
		},
		{
			ID:       "C31b", // the same, where a poison record only the backfill sees
			File:     backfill,
			Old:      skipPoison,
			New:      "\t\t\tcontinue",
			Scenario: handoff,
			Seeds:    100,
			Want:     p3,
		},
		{
			ID:      "C32a", // the backfill's uniqExact markers have live owners
			File:    apply,
			Old:     ownerKind,
			New:     "keys.Owner(keys.OwnerLive, m.ct.unitKey, m.ct.seq)",
			Test:    "TestLiveAndBackfillCountAValueOnce",
			Package: core,
		},
		{
			ID:       "C32b", // the same, in user_latest's pages, unless poison broke the view
			File:     apply,
			Old:      ownerKind,
			New:      "keys.Owner(keys.OwnerLive, m.ct.unitKey, m.ct.seq)",
			Scenario: handoff,
			Seeds:    50,
			Want:     p1,
		},
		{
			ID:       "C39", // the backfill applies a record its row's mark B already holds
			File:     apply,
			Old:      "return ct.mark().Compare(last) <= 0",
			New:      "return ct.mark().Compare(last) < -1",
			Scenario: handoff,
			Seeds:    20,
			Want:     p1,
		},
	}
}

// pruneControls returns the controls of D23a's prune (S9 rule 3), and S9's baselines: no
// pruning, and a backfill namespace per unit. Seeds seldom line up the races that the time rule and the
// margin guard, so the scripted tests are their acceptance: TestPruneBoundary,
// TestLateProgress, TestWarmContainerReadsProgressAgain and the projector's lifetime tests.
func pruneControls() []control {
	return []control{
		{
			ID:       "C8", // prune as soon as progress covers the mark
			File:     apply,
			Old:      prune,
			New:      "time.Since(u.ProgressAt) > 0",
			Scenario: d23a,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:       "C9", // prune once progress rose ¾ of the longest invocation ago
			File:     apply,
			Old:      prune,
			New:      "time.Since(u.ProgressAt) > c.lifetime*3/4",
			Scenario: d23a,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:       "C10", // the same, with long units
			File:     apply,
			Old:      prune,
			New:      "time.Since(u.ProgressAt) > c.lifetime*3/4",
			Scenario: "long",
			Seeds:    100,
			Want:     p1,
		},
		{ID: "B1", Scenario: d23a, Seeds: 20, Want: p4}, // no pruning: cores with W = ∞
		{
			ID:       "C44", // prune a mark that progress doesn't cover yet
			File:     apply,
			Old:      "u.Progress >= seq &&",
			New:      `u.Progress != "" && seq != "" &&`,
			Scenario: d23a,
			Seeds:    20,
			Want:     p1,
		},
		{
			ID:   "C15a", // prune at exactly W
			File: apply,
			Old:  prune,
			New:  "time.Since(u.ProgressAt) >= c.window",
			Test: pruneBoundary,
		},
		{
			ID:   "C15b", // prune at W − 1 s
			File: apply,
			Old:  prune,
			New:  "time.Since(u.ProgressAt) > c.window-time.Second",
			Test: pruneBoundary,
		},
		{
			ID:   "C42", // a backfill namespace per unit, never pruned
			File: apply,
			Old: `		var last keys.Mark
		if h.Backfill != nil {
			last = *h.Backfill
		}
		out := slices.DeleteFunc(slices.Clone(g.contribs), func(ct *contribution) bool { return ct.mark().Compare(last) <= 0 })
		if len(out) > 0 {
			m := out[len(out)-1].mark()
			h.Backfill = &m
		}`,
			New: `		out := slices.DeleteFunc(slices.Clone(g.contribs), func(ct *contribution) bool {
			return ct.seq <= h.Units[keys.UnitKey("B:"+ct.unitKey)]
		})
		for _, ct := range out {
			h.Units[keys.UnitKey("B:"+ct.unitKey)] = ct.seq
		}`,
			Scenario: d23a,
			Seeds:    20,
			Want:     p4,
		},
	}
}

// progressControls returns the controls of D23a's progress: the skip, the stamp and the
// invocation's lifetime (S9 rules 1–2, requirements 1–2).
func progressControls() []control {
	return []control{
		{
			ID:   "C11", // skip by progress a container read up to one lifetime ago
			File: handle,
			Old:  statesAt,
			New:  statesAt + "\n\tprogress     map[string]string\n\tprogressRead map[string]time.Time",
			More: [][2]string{{skip, `	c.mu.Lock()
	if read, ok := c.progressRead[b.Unit]; !ok || time.Since(read) > c.lifetime {
		if c.progress == nil {
			c.progress, c.progressRead = map[string]string{}, map[string]time.Time{}
		}
		c.progress[b.Unit], c.progressRead[b.Unit] = rc.unit.Progress, time.Now()
	}
	progress := c.progress[b.Unit]
	c.mu.Unlock()
	recs = slices.DeleteFunc(recs, func(r record) bool { return r.seq <= progress })`}},
			Test: "TestWarmContainerReadsProgressAgain",
		},
		{
			ID:       "C12", // prune without the skip by progress
			File:     handle,
			Old:      skip,
			New:      "",
			Scenario: d23a,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:       "C13", // progress_at is when the advancing invocation started
			File:     handle,
			Old:      "deadline, ok := ctx.Deadline()",
			New:      "deadline, ok := ctx.Deadline()\n\tstart := time.Now()",
			More:     [][2]string{{advance, "c.advance(ctx, rc, recs, failed, start)"}},
			Scenario: d23a,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:   "C15c", // progress_at a microsecond before the deadline
			File: handle,
			Old:  advance,
			New:  "c.advance(ctx, rc, recs, failed, deadline.Add(-time.Microsecond))",
			Test: pruneBoundary,
		},
		{
			ID:   "C18", // W = L + m − 1: a progress write that lands m late reaches a pruned row
			File: handle,
			Old:  "lifetime: w - catalog.LifetimeMarginSeconds*time.Second,",
			New:  "lifetime: w - (catalog.LifetimeMarginSeconds-1)*time.Second,",
			Test: "TestLateProgress",
		},
		{
			ID:   "C41", // an invocation may live longer than W − 60 s
			File: handle,
			Old:  "left > c.lifetime {",
			New:  "false {",
			Test: "TestLateProgress",
		},
		{
			ID:       "C43", // the progress write outlives the invocation's deadline
			File:     handle,
			Old:      "c.meta.Advance(ctx,",
			New:      "c.meta.Advance(context.WithoutCancel(ctx),",
			Scenario: d23a,
			Seeds:    10,
			Want:     p7,
		},
	}
}

// replayControls returns the self-checks of determinism (H3): TestDeterministic fails when a
// run doesn't replay, or when the seed doesn't shape it.
func replayControls() []control {
	return []control{
		{
			ID:   "H3a", // the scheduler draws from an unseeded generator
			File: "internal/simulation/sched.go",
			Old:  "i := s.rng.IntN(len(s.ready))",
			New:  "i := rand.IntN(len(s.ready))",
			Test: "TestDeterministic",
		},
		{
			ID:   "H3b", // every seed runs as seed 0
			File: "internal/simulation/sim.go",
			Old:  "newRun(t.Context(), sc, sys, seed)",
			New:  "newRun(t.Context(), sc, sys, 0)",
			Test: "TestDeterministic",
		},
	}
}

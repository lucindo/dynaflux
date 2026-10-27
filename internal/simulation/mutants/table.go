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
	cache         = "internal/meta/cache.go"
	metaPackage   = "./internal/meta"
	appendGo      = "internal/engine/append.go"
	step          = "internal/backfill/step.go"
	backfillPkg   = "./internal/backfill"
	core          = "./internal/projector"
	region        = "internal/simulation/region.go"
	killBoundary  = "TestKillBoundary"
	kill          = "!now.Before(inv.deadline)"
	breaks        = "vs, opened, err := c.meta.MarkBroken(ctx, v.id, v.def.Stream, f)"
	faults        = "faults"
	poison        = "poison"
	handoff       = "handoff"
	d23a          = "d23a"
	regions       = "regions"
	guards        = "guards"
	retire        = "retire"
	pruneBoundary = "TestPruneBoundary"
	lateProgress  = "TestLateProgress"
	always        = "if true {"
	prune         = "time.Since(u.ProgressAt) > c.window"
	skip          = "\trecs = slices.DeleteFunc(recs, func(r record) bool { return r.seq <= rc.unit.Progress })"
	advance       = "c.advance(progressCtx, rc, recs, failed, deadline)"
	archive       = "\tif err = c.archive(ctx, b, recs); err != nil {\n\t\treturn err\n\t}"
	applies       = "if r := &recs[i]; r.Op == Insert {"
	noInsert      = "if inserts == 0 {" // a batch without an INSERT returns early
	stateCache    = "states *meta.StateCache"
	stampedAhead  = "TestStateCacheRereadsStatesStampedAhead"
	g3            = `if unknown != "" {`
	never         = "if false {"
	fence         = "\t\tif _, ok := u.Fences[version]; ok {\n\t\t\treturn u, nil\n\t\t}"
	// fenceBody follows fence in EnsureFence, up to the fence's value.
	fenceBody = `
		next := u
		next.Fences = make(map[string]string, len(u.Fences)+1)
		maps.Copy(next.Fences, u.Fences)
		next.Fences[version] = cmp.Or(`
	skipPoison = `			if _, err = c.breakVersion(ctx, v, r, meta.CauseRecord, err); err != nil {
				return err
			}
			return fmt.Errorf("projector: backfill of %s: %s: %w", version, r.name(), ErrBroken)`
	// applyLoop is apply's loop over chunks.
	applyLoop = `	for s.left() {
		wait, end, cerr := s.applyChunk(ctx, m, j.Stream)
		if cerr != nil || wait > 0 || end {
			return wait, false, cerr
		}
	}`
	fenceUnits  = "cut, fences, err := s.env.Meta.FenceUnits(ctx, j.Stream, s.version, s.env.Bounds, s.lister(j.Stream))"
	freshCheck  = "if fresh, _, err = CheckStream(ctx, s.env.Tables, s.env.Store, s.env.Naming.StreamTable(j.Stream)); err != nil {"
	readFence   = "u, err := s.env.Meta.Unit(ctx, stream, rec.unit)"
	earlyKey    = `fmt.Sprintf("%p %s", s.env.Meta, s.version)`
	syncImport  = "\t\"log/slog\"\n\t\"time\""
	earlyStore  = "var early sync.Map\n\nvar (\n\terrNotOwner"
	errNotOwner = "var (\n\terrNotOwner"
	fresh       = "fresh"
	resume      = "resume"
	timesplit   = "timesplit"
	splitLate   = "timesplit-late"
)

// The properties that controls break.
const (
	p1  = "P1:"
	p3  = "P3:"
	p4  = "P4:"
	p6  = "P6:"
	p7  = "P7:"
	p10 = "P10:"
)

func table() []control {
	cs := slices.Concat(liveControls(), regionControls(), fenceControls(), handoffControls(), backfillControls(),
		orderControls(), applyControls(), pruneControls(), progressControls(), guardControls(), jobControls(),
		tableControls(), replayControls())
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
			Old:  "if err = rs.add(v, r, vals, bad); err != nil {",
			New: `if err = rs.add(v, r, vals, bad); err != nil {
			return err
		}
		if false {`,
			Scenario: poison,
			Seeds:    20,
			Want:     p6,
		},
		{
			ID:   "C24b", // an evaluation error opens every view's breaker
			File: handle,
			Old:  "if err = c.open(ctx, v, r, meta.CauseRecord, err, broken, st); err != nil {",
			New: `cause := err
			for _, v := range rc.views {
				if err = c.open(ctx, v, r, meta.CauseRecord, cause, broken, st); err != nil {
					return err
				}
			}
			if false {`,
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

// everyBatch is the More of a mutant that applies or archives records other than INSERTs:
// batches without an INSERT reach it too.
func everyBatch() [][2]string { return [][2]string{{noInsert, "if inserts < 0 {"}} }

// regionControls returns the controls of what a region's change stream holds besides its
// own appends, of an append that finds its event stored, and of a batch's rows and unit
// item (S9 requirements 1 and 3).
func regionControls() []control {
	return []control{
		{
			ID:       "C19", // a MODIFY, a cross-region duplicate, is applied
			File:     handle,
			Old:      applies,
			New:      "if r := &recs[i]; r.Op != Remove {",
			More:     everyBatch(),
			Scenario: regions,
			Seeds:    20,
			Want:     p1,
		},
		{
			ID:       "C20", // a REMOVE, an expired event, is applied as its old image
			File:     handle,
			Old:      applies,
			New:      "if r := &recs[i]; r.Op != Modify {\n\t\t\tif r.Op == Remove {\n\t\t\t\tr.New = r.Old\n\t\t\t}",
			More:     everyBatch(),
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
			Old:      "\trows := logicalRows(groups)\n\tfanOut(len(rows), parallel, func(r int) {",
			New:      "\trows := logicalRows(groups)\n\tgo fanOut(len(rows), parallel, func(r int) {",
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
			New:      never,
			Scenario: handoff,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:   "C6", // a fence from the progress read before a write that lost the race
			File: unit,
			Old:  "\tfor range maxAttempts {\n" + fence + fenceBody + "u.Progress, ZeroFence())",
			New:  "\tread := u.Progress\n\tfor range maxAttempts {\n" + fence + fenceBody + "read, ZeroFence())",
			Test: "TestRecordArchivedWhileFencingReachesTheBackfill",
		},
		{
			ID:   "C17", // units listed as soon as the version is activating
			File: unit,
			Old:  "waitUntil(ctx, vs.ActivatedAt.Add(b.Listing))",
			New:  "waitUntil(ctx, vs.ActivatedAt)",
			Test: "TestUnitsAreListedOnceEveryCoreSawTheActivation",
		},
		{
			ID:      "C29", // a finished backfill makes a broken version live
			File:    "internal/meta/view.go",
			Old:     "if vs.State != Activating {",
			New:     "if vs.State != Activating && vs.State != Broken {",
			More:    [][2]string{{"vs.State = Live\n", "vs.State, vs.Failure = Live, nil\n"}},
			Test:    "TestStepEndsBrokenOnAVersionBrokenWhileItApplies",
			Package: backfillPkg,
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
			File:    cache,
			Old:     "age < MaxViewStateAge",
			New:     "age < 1<<62",
			Test:    "TestHandleAppliesFencedVersionAboveItsFence",
			Package: core,
		},
		{
			ID:      "C50", // the view states' age is on the monotonic clock, which a freeze stops
			File:    cache,
			Old:     "sent := time.Now().Round(0)",
			New:     "sent := time.Now()",
			Test:    stampedAhead,
			Package: metaPackage,
		},
		{
			ID:      "C51", // cached view states stamped ahead, by a clock that stepped back, are fresh
			File:    cache,
			Old:     "age >= 0 && ",
			New:     "",
			Test:    stampedAhead,
			Package: metaPackage,
		},
		{
			ID:      "C24d", // a state read that lands after a breaker opened closes it again
			File:    cache,
			Old:     "for version, vs := range c.states {",
			New:     "for version, vs := range map[string]ViewState(nil) {",
			Test:    "TestStateCacheKeepsAStateSetDuringARead",
			Package: metaPackage,
		},
	}
}

// handoffControls returns the controls of a batch's part in the handoff (S1b rule 1, D-12).
func handoffControls() []control {
	return []control{
		{
			ID:   "C33", // a batch archives before it fences its unit
			File: handle,
			Old:  archive + "\n\trs := &rows{",
			New:  "\trs := &rows{",
			More: [][2]string{{"\trc, err := c.reach(ctx, b.Stream, b.Unit)", strings.Replace(archive, "err =", "err :=", 1) + "\n\trc, err := c.reach(ctx, b.Stream, b.Unit)"}},
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
			File: step,
			Old:  applyLoop,
			New: `	byUnit, order := map[string][]projector.BackfillRecord{}, []string(nil)
	for {
		rec, ok, err := m.next(ctx)
		if err != nil {
			return 0, false, err
		}
		if !ok {
			break
		}
		keep, err := s.keeps(ctx, j.Stream, &rec)
		if err != nil {
			return 0, false, err
		}
		if !keep {
			continue
		}
		if _, seen := byUnit[rec.unit]; !seen {
			order = append(order, rec.unit)
		}
		byUnit[rec.unit] = append(byUnit[rec.unit], projector.BackfillRecord{Unit: rec.unit, Seq: rec.seq, Item: rec.item})
	}
	for len(order) > 0 {
		var left []string
		for _, u := range order {
			n := min(len(byUnit[u]), s.env.Chunk)
			if err := s.applyRecords(ctx, byUnit[u][:n]); err != nil {
				return 0, false, err
			}
			if byUnit[u] = byUnit[u][n:]; len(byUnit[u]) > 0 {
				left = append(left, u)
			}
		}
		order = left
	}
	return 0, false, s.moveCursor(ctx, chunk{end: true})`,
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
			// The harness backfills no view whose only additive aggregate is count.
			Test:    "TestFencedHandoff",
			Package: "./internal/catalog",
		},
		{
			ID:      "C70", // uniqExact versions skip the fenced handoff, as other idempotent ones do
			File:    "internal/catalog/view.go",
			Old:     "return !a.Fn.Idempotent() || a.Fn == AggUniqExact",
			New:     "return !a.Fn.Idempotent()",
			Test:    "TestStepUniqExactTakesASplit",
			Package: backfillPkg,
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
			More:     everyBatch(),
			Scenario: regions,
			Seeds:    20,
			Want:     p3,
		},
		{
			ID:   "C28", // fences only for the units a listing returns
			File: step,
			Old:  readFence,
			New: `listed, err := s.env.Units.Units(ctx, s.env.Naming.StreamTable(stream))
		u := meta.Unit{}
		if err == nil && slices.Contains(listed, rec.unit) {
			u, err = s.env.Meta.Unit(ctx, stream, rec.unit)
		}`,
			More:     [][2]string{{syncImport, "\t\"log/slog\"\n\t\"slices\"\n\t\"time\""}},
			Scenario: handoff,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:       "C68", // the archive read skips the error prefix
			File:     "internal/simulation/archive.go",
			Old:      "for _, prefix := range []string{prefixDelivered, prefixErrors} {",
			New:      "for _, prefix := range []string{prefixDelivered} {",
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
			File:     step,
			Old:      "ok, err := s.env.Archive.Complete(ctx, j.Stream, j.Cut)",
			New:      "ok, err := true, error(nil)",
			Scenario: handoff,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:   "C34", // the cut is taken before the fences are written
			File: unit,
			Old:  "\tfences := make(map[string]string, len(unitIDs))\n",
			New:  "\tcut := time.Now()\n\tfences := make(map[string]string, len(unitIDs))\n",
			More: [][2]string{{"return time.Now(), fences, nil", "return cut, fences, nil"}},
			Test: "TestRecordArchivedWhileFencingReachesTheBackfill",
		},
		{
			ID:   "C35", // the fences are read at the cut, before the archive
			File: step,
			Old:  "s.fences = fences\n",
			New:  "s.fences = fences\n\tearly.Store(" + earlyKey + ", fences)\n",
			More: [][2]string{
				{readFence, `var u meta.Unit
		var err error
		if f, ok := early.Load(` + earlyKey + `); ok {
			if fence, fenced := f.(map[string]string)[rec.unit]; fenced {
				u.Fences = map[string]string{s.version: fence}
			}
		}`},
				{errNotOwner, earlyStore},
				{syncImport, "\t\"log/slog\"\n\t\"sync\"\n\t\"time\""},
			},
			Scenario: handoff,
			Seeds:    50,
			Want:     p1,
		},
		{
			ID:       "C36", // a finished backfill leaves the version activating
			File:     step,
			Old:      "if err = s.env.Meta.MakeLive(ctx, s.version, since); err != nil {\n\t\treturn err\n\t}\n\tif err = s.env.Runs.DeletePrefix",
			New:      "_ = since\n\tif err = s.env.Runs.DeletePrefix",
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
			New:     "\t\t\t_ = meta.CauseRecord\n\t\t\tcontinue",
			Test:    "TestApplyBackfillOpensTheBreaker",
			Package: core,
		},
		{
			ID:       "C31b", // the same, where a poison record only the backfill sees
			File:     backfill,
			Old:      skipPoison,
			New:      "\t\t\t_ = meta.CauseRecord\n\t\t\tcontinue",
			Scenario: handoff,
			Seeds:    100,
			Want:     p3,
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
			Old:  stateCache,
			New:  stateCache + "\n\tmu           sync.Mutex\n\tprogress     map[string]string\n\tprogressRead map[string]time.Time",
			More: [][2]string{{"\t\"strings\"\n", "\t\"strings\"\n\t\"sync\"\n"}, {skip, `	c.mu.Lock()
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
			Old:      "c.apply(rowCtx, ctx, b, s, recs, start, deadline, &res)",
			New:      "c.apply(rowCtx, ctx, b, s, recs, start, time.Now(), &res)",
			Scenario: d23a,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:   "C15c", // progress_at a microsecond before the deadline
			File: handle,
			Old:  advance,
			New:  "c.advance(progressCtx, rc, recs, failed, deadline.Add(-time.Microsecond))",
			Test: pruneBoundary,
		},
		{
			ID:   "C18", // W = L + m − 1: a progress write that lands m late reaches a pruned row
			File: handle,
			Old:  "lifetime:     w - catalog.LifetimeMarginSeconds*time.Second,",
			New:  "lifetime:     w - (catalog.LifetimeMarginSeconds-1)*time.Second,",
			Test: lateProgress,
		},
		{
			ID:   "C41", // an invocation may live longer than W − 60 s
			File: handle,
			Old:  "left > c.lifetime {",
			New:  "false {",
			Test: lateProgress,
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
		{
			ID:   "C47", // no reserve: the core sends until its deadline
			File: handle,
			Old:  "reserve := min(maxReserve, left/reserveShare)",
			New:  "reserve := time.Duration(0)",
			Test: lateProgress,
		},
		{
			ID:   "C48", // progress_at is the send cutoff, not the deadline
			File: handle,
			Old:  advance,
			New:  "c.advance(progressCtx, rc, recs, failed, func() time.Time { d, _ := progressCtx.Deadline(); return d }())",
			Test: pruneBoundary,
		},
		{
			ID:      "C49", // the progress write stops with the rows, so a hung row fails the batch
			File:    handle,
			Old:     advance,
			New:     "c.advance(ctx, rc, recs, failed, deadline)",
			Test:    "TestHandleStopsSendingAtTheCutoff",
			Package: core,
		},
	}
}

// guardControls returns the controls of activation's guards and of retired versions (ADR 0012).
func guardControls() []control {
	return []control{
		{
			ID:       "C52", // no G3: a build without an activating version applies its stream's records
			File:     handle,
			Old:      g3,
			New:      never,
			Scenario: guards,
			Seeds:    100,
			Want:     p1,
		},
		{
			// FenceUnits only for additive versions, cut right after activation for the others.
			// Seeds of guards catch it seldom: a batch of an older build must read its unit
			// before the activation and move its progress after the cut.
			ID:   "C53",
			File: step,
			Old:  fenceUnits,
			New: `cut, fences, err := time.Now(), map[string]string{}, error(nil)
	if vv, ok := s.env.Catalog.Version(s.version); ok && vv.FencedHandoff() {
		cut, fences, err = s.env.Meta.FenceUnits(ctx, j.Stream, s.version, s.env.Bounds, s.lister(j.Stream))
	}`,
			Test:    "TestStepFencesEveryVersion",
			Package: backfillPkg,
		},
		{
			ID:       "C54", // no G3: a rollback to a build without a live version
			File:     handle,
			Old:      g3,
			New:      never,
			Scenario: "rollback",
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:       "C55", // a build that lists a live version as retired skips it without its tombstone
			File:     handle,
			Old:      "vs, err := c.meta.Retire(ctx, id, states[id].Stream)",
			New:      "vs, err := meta.ViewState{Version: id, State: meta.Retired}, error(nil)",
			Scenario: retire,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:       "C55b", // covered ignores a retired state
			File:     handle,
			Old:      "state == meta.Broken || state == meta.Retired ||",
			New:      "state == meta.Broken ||",
			Scenario: retire,
			Seeds:    100,
			Want:     p6,
		},
		{
			ID:      "C55d", // a build applies an additive version split neither at fences nor by time
			File:    handle,
			Old:     `if split != "" {`,
			New:     never,
			Test:    "TestHandleFailsOnASplitItDoesNotApply",
			Package: core,
		},
		{
			ID:       "C55c", // a build that defines a retired version applies it
			File:     handle,
			Old:      "case meta.Broken, meta.Retired:\n\t\tcase meta.Activating, meta.Live:",
			New:      "case meta.Broken:\n\t\tcase meta.Activating, meta.Live, meta.Retired:",
			Scenario: retire,
			Seeds:    100,
			Want:     p1,
		},
	}
}

// jobControls returns the controls of a backfill job's fresh rule (lifecycle.md §4.3) and of
// its cursor and order across attempts (ADR 0011 §3).
func jobControls() []control {
	return []control{
		{
			ID:       "C56", // the fresh rule without F3: listed units without progress are enough
			File:     step,
			Old:      freshCheck,
			New:      "if err != nil {",
			Scenario: fresh,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:   "C57", // F3's scan is sent right after the activation, before the fences
			File: step,
			Old:  fenceUnits,
			New: "early, _, _ := CheckStream(ctx, s.env.Tables, s.env.Store, s.env.Naming.StreamTable(j.Stream))\n\tbeforeFences.Store(" +
				earlyKey + ", early)\n\t" + fenceUnits,
			More: [][2]string{
				{freshCheck, "if v, ok := beforeFences.Load(" + earlyKey + "); ok {\n\t\t\tfresh = v.(bool)\n\t\t}\n\t\tif err != nil {"},
				{errNotOwner, "var beforeFences sync.Map\n\nvar (\n\terrNotOwner"},
				{syncImport, "\t\"log/slog\"\n\t\"sync\"\n\t\"time\""},
			},
			Test:    "TestStepFreshScansAfterTheFences",
			Package: backfillPkg,
		},
		{
			ID:      "C58", // F1 dropped: a fence above zero doesn't stop the fresh rule
			File:    step,
			Old:     "fresh := zeroFences(fences)",
			New:     "fresh := len(fences) >= 0",
			Test:    "TestStepFreshStream",
			Package: backfillPkg,
		},
		{
			ID:      "C69", // a staged run claims export files whose runs a restage dropped
			File:    "internal/backfill/archive.go",
			Old:     "if slices.ContainsFunc(*held, func(name string) bool { return !listed(name) }) {",
			New:     never,
			Test:    "TestStepStagesAgainAfterARestageItDidNotSee",
			Package: backfillPkg,
		},
		{
			ID:       "C66", // the cursor is stored before the chunk's rows succeeded
			File:     step,
			Old:      "\tif err = s.applyRecords(ctx, c.recs); err != nil {\n\t\treturn 0, false, err\n\t}\n\treturn 0, c.end, s.moveCursor(ctx, c)",
			New:      "\tif err = s.moveCursor(ctx, c); err != nil {\n\t\treturn 0, false, err\n\t}\n\treturn 0, c.end, s.applyRecords(ctx, c.recs)",
			Scenario: resume,
			Seeds:    50,
			Want:     p1,
		},
		{
			ID:   "C67", // a worker of a resumed attempt walks its chunks backwards
			File: step,
			Old:  applyLoop,
			New: `	if s.attempt > 1 {
		var chunks []chunk
		for {
			c, err := s.nextChunk(ctx, m, j.Stream)
			if err != nil {
				return 0, false, err
			}
			chunks = append(chunks, c)
			if c.end {
				break
			}
		}
		for i := len(chunks) - 1; i >= 0; i-- {
			if err := s.applyRecords(ctx, chunks[i].recs); err != nil {
				return 0, false, err
			}
			if err := s.moveCursor(ctx, chunks[i]); err != nil {
				return 0, false, err
			}
		}
		return 0, false, nil
	}
` + applyLoop,
			Scenario: resume,
			Seeds:    50,
			Want:     p1,
		},
	}
}

// tableControls returns the controls of the table source and its time split (lifecycle.md
// §5.1, ADR 0011 §6).
func tableControls() []control {
	return []control{
		{
			ID:       "C59", // live projection applies an INSERT at or before the split time too
			File:     handle,
			Old:      "case at > vs.SplitAt.UnixMicro():",
			New:      "case true:",
			Scenario: timesplit,
			Seeds:    20,
			Want:     p1,
		},
		{
			ID:       "C60", // ε = 0 while writer clocks are off
			File:     "internal/meta/meta.go",
			Old:      "Skew: 5 * time.Minute,",
			New:      "Skew: 0,",
			Scenario: timesplit,
			Seeds:    100,
			Want:     p1,
		},
		{
			ID:       "C61", // the table is read at the split time, before its late bound and the clock margin
			File:     step,
			Old:      "readAfter = vs.SplitLate.Add(s.env.Bounds.Clock)",
			New:      "readAfter = vs.SplitAt",
			Scenario: timesplit,
			Seeds:    50,
			Want:     p1,
		},
		{
			ID:       "C62", // no late check
			File:     handle,
			Old:      "case !start.Before(vs.SplitLate):",
			New:      "case false:",
			Scenario: splitLate,
			Seeds:    100,
			Want:     p10,
		},
		{
			ID:       "C63", // no check of a MODIFY that straddles the split time
			File:     handle,
			Old:      "if err == nil && (from <= b) == (to <= b) {",
			New:      always,
			Scenario: splitLate,
			Seeds:    100,
			Want:     p10,
		},
		{
			ID:       "C64", // G3 only for batches with an INSERT
			File:     handle,
			Old:      "states, err := c.viewStates(ctx, s.Def.Name, false)",
			New:      "states, err := c.states.States(ctx, false)",
			Scenario: splitLate,
			Seeds:    100,
			Want:     p10,
		},
		{
			ID:      "C72", // no clock margin: the table is read at the late bound by the worker's clock
			File:    step,
			Old:     "readAfter = vs.SplitLate.Add(s.env.Bounds.Clock)",
			New:     "readAfter = vs.SplitLate",
			Test:    "TestStepTimeSplitAcrossClocks",
			Package: backfillPkg,
		},
		{
			ID:       "C71", // the late bound without L: split_late is the split time plus ε
			File:     "internal/meta/view.go",
			Old:      "vs.SplitLate = stamp(vs.SplitAt.Add(b.Skew + b.Landing))",
			New:      "vs.SplitLate = stamp(vs.SplitAt.Add(b.Skew))",
			Scenario: timesplit,
			Seeds:    20,
			Want:     p10,
		},
		{
			// A table source not split is read at the activation, before every projector knows
			// the version. Seeds of guards-table catch it seldom: the batch of an older build
			// must take the only record of a row.
			ID:      "C73",
			File:    step,
			Old:     "readAfter := vs.ActivatedAt.Add(s.env.Bounds.Listing)",
			New:     "readAfter := vs.ActivatedAt",
			Test:    "TestStepNoSplit",
			Package: backfillPkg,
		},
		{
			ID:       "C65", // runs named by their input and rewritten, not committed
			File:     "internal/backfill/merge.go",
			Old:      `return fmt.Sprintf("runs/%d-%s%s", j.Attempt, hex.EncodeToString(b[:]), suffix), nil`,
			New:      `return fmt.Sprintf("runs/%d-%d%s%.0s", j.Attempt, len(j.Runs), suffix, hex.EncodeToString(b[:])), nil`,
			Scenario: "table",
			Seeds:    100,
			Want:     p1,
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

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
)

// Reserve is how close to its deadline a step starts no more work: a chunk or a block started
// before it finishes, and the step returns.
const Reserve = time.Minute

const (
	watermarkPoll = time.Minute
	exportPoll    = time.Minute
	chunkRecords  = 2000
	chunkTries    = 3
	chunkRetry    = time.Second
)

var (
	errNotOwner = errors.New("backfill: the job isn't this attempt's to run")
	errMoved    = errors.New("backfill: another worker moved the job on")
)

// Step does the current phase of version's job, and the phases after it, until ctx's
// deadline less a minute. It returns how long to wait before the next step, or done when the
// job needs no more: it ended, or another attempt owns it. attempt is the caller's, and
// execution its name, written to the job item when it changed: an execution's ARN, or
// "local". An error is recorded in the job item as well as returned, for the caller to step
// again.
func Step(ctx context.Context, env *Env, version string, attempt int, execution string) (time.Duration, bool, error) {
	deadline, ok := ctx.Deadline()
	switch {
	case !ok:
		return 0, false, errors.New("backfill: a step needs a context with a deadline")
	case env.Meta == nil || env.Store == nil || env.Core == nil || env.Units == nil || env.Tables == nil ||
		env.Runs == nil || env.Lag == nil || env.Catalog == nil || env.Log == nil || env.Memory <= 0 ||
		env.Archive != nil && (env.Witness == nil || env.Region == ""):
		return 0, false, errors.New("backfill: the step's environment lacks a part")
	case version == "" || attempt < 1:
		return 0, false, fmt.Errorf("backfill: step version %q attempt %d", version, attempt)
	}
	s := &step{env: env, version: version, attempt: attempt, execution: execution, cutoff: deadline.Add(-Reserve), budget: budgetOf(env.Memory)}
	if s.ended(ctx) {
		return 0, true, nil
	}
	for s.left() {
		j, err := s.job(ctx)
		if err != nil {
			return s.fail(ctx, "", err)
		}
		wait, done, err := s.phase(ctx, j)
		if errors.Is(err, ErrStagingMissing) {
			err = s.restage(ctx, j)
		}
		switch {
		case errors.Is(err, errMoved):
		case err != nil:
			return s.fail(ctx, j.Phase, err)
		case wait > 0 || done:
			return wait, done, nil
		}
	}
	return 0, false, nil
}

// step is one call of Step.
type step struct {
	env       *Env
	version   string
	attempt   int
	execution string
	cutoff    time.Time // the step starts no work after it
	budget    budget
	fences    map[string]string // FenceUnits' fences, when this step fenced
	// unit, fence and fenced are the fence of the unit of the last record apply judged:
	// records come by unit, so one is enough.
	unit, fence string
	fenced      bool
	lagAt       time.Time // the step's last lag check
	born        time.Time // the stream table's creation, once bornAt read it
	pace        pacer
	// planName and slices are the last archive plan the step read.
	planName string
	slices   []slice
}

func (s *step) left() bool { return time.Now().Before(s.cutoff) }

func (s *step) owns(j meta.Job) bool { return j.State == meta.JobRunning && j.Attempt == s.attempt }

// phase does the job's phase. The archive source fences, checks the fresh rule and waits for
// the watermark; the table source waits for its read_after (lifecycle.md §5.1).
func (s *step) phase(ctx context.Context, j meta.Job) (time.Duration, bool, error) {
	archive := j.Split == meta.SplitFence
	switch {
	case j.Phase == meta.PhaseActivate:
		return 0, false, s.activate(ctx, j)
	case archive && j.Phase == meta.PhaseFence:
		return 0, false, s.fenceUnits(ctx, j)
	case archive && j.Phase == meta.PhaseFresh:
		return s.fresh(ctx, j)
	case archive && j.Phase == meta.PhaseWatermark:
		return s.watermark(ctx, j)
	case !archive && j.Phase == meta.PhaseWait:
		return s.wait(ctx, j)
	case archive && j.Phase == meta.PhaseStage:
		return s.stage(ctx, j)
	case j.Phase == meta.PhaseStage:
		return s.stageTable(ctx, j)
	case j.Phase == meta.PhaseApply:
		return s.apply(ctx, j)
	case j.Phase == meta.PhaseLive:
		return 0, true, s.live(ctx, j)
	}
	return 0, false, fmt.Errorf("backfill: %s: phase %s doesn't go with the %s split", s.version, j.Phase, j.Split)
}

// activate checks guard 1 and makes the version activating with the job's split. A table
// source then waits for its read_after: the time split's late bound plus the clock margin, or
// the listing delay, after which every projector applies the version.
func (s *step) activate(ctx context.Context, j meta.Job) error {
	if err := s.deployed(ctx); err != nil {
		return err
	}
	if vv, _ := s.env.Catalog.Version(s.version); j.Split == meta.SplitNone && vv.FencedHandoff() {
		return fmt.Errorf("backfill: %s needs a split by fences or by time: with none, live projection and the backfill would both apply its events", s.version)
	}
	vs, err := s.env.Meta.Activate(ctx, s.version, j.Stream, j.Split, s.env.Bounds)
	if err != nil {
		return err
	}
	if j.Split == meta.SplitFence {
		return s.advance(ctx, meta.PhaseActivate, meta.PhaseFence, nil)
	}
	readAfter := vs.ActivatedAt.Add(s.env.Bounds.Listing)
	if j.Split == meta.SplitTime {
		readAfter = vs.SplitLate.Add(s.env.Bounds.Clock)
	}
	return s.advance(ctx, meta.PhaseActivate, meta.PhaseWait, func(j *meta.Job) { j.ReadAfter = readAfter })
}

// wait waits until the table source may read: until read_after, by the host's clock.
func (s *step) wait(ctx context.Context, j meta.Job) (time.Duration, bool, error) {
	if left := j.ReadAfter.Sub(s.env.now()); left > 0 {
		return left, false, nil
	}
	return 0, false, s.advance(ctx, meta.PhaseWait, meta.PhaseStage, nil)
}

// deployed checks guard 1 (lifecycle.md §4.1): the region's deployed catalog defines the
// version, and so does the catalog of the code that steps.
func (s *step) deployed(ctx context.Context) error {
	if _, ok := s.env.Catalog.Version(s.version); !ok {
		return fmt.Errorf("backfill: %s isn't in this build's catalog", s.version)
	}
	_, def, ok, err := deployedCatalog(ctx, s.env)
	if err != nil {
		return err
	}
	if _, defined := def.Version(s.version); !ok || !defined {
		return fmt.Errorf("backfill: %s isn't in the region's deployed catalog; deploy regional.yaml there first", s.version)
	}
	return nil
}

// fenceUnits fences every unit of the stream for the version, idempotent ones too (guard 2),
// once every projector knows the version is activating, and records the cut.
func (s *step) fenceUnits(ctx context.Context, j meta.Job) error {
	cut, fences, err := s.env.Meta.FenceUnits(ctx, j.Stream, s.version, s.env.Bounds, s.lister(j.Stream))
	if err != nil {
		return err
	}
	s.fences = fences
	return s.advance(ctx, meta.PhaseFence, meta.PhaseFresh, func(j *meta.Job) { j.Cut = cut })
}

func (s *step) lister(stream string) func(context.Context) ([]string, error) {
	table := s.env.Naming.StreamTable(stream)
	return func(ctx context.Context) ([]string, error) { return s.env.Units.Units(ctx, table) }
}

// fresh makes the version live without a read when the region's stream has never held an
// event (lifecycle.md §4.3): every fence is zero (F1), and the stream table is young and
// empty by a scan sent after the fences (F2, F3).
func (s *step) fresh(ctx context.Context, j meta.Job) (time.Duration, bool, error) {
	fences := s.fences
	if fences == nil {
		var err error
		if _, fences, err = s.env.Meta.FenceUnits(ctx, j.Stream, s.version, s.env.Bounds, s.lister(j.Stream)); err != nil {
			return 0, false, err
		}
	}
	fresh := zeroFences(fences)
	if fresh {
		var err error
		if fresh, _, err = CheckStream(ctx, s.env.Tables, s.env.Store, s.env.Naming.StreamTable(j.Stream)); err != nil {
			return 0, false, err
		}
	}
	if !fresh {
		return 0, false, s.advance(ctx, meta.PhaseFresh, meta.PhaseWatermark, nil)
	}
	if err := s.env.Meta.MakeLive(ctx, s.version, time.Time{}); err != nil {
		return 0, false, err
	}
	return 0, true, s.done(ctx, true)
}

// zeroFences reports whether every fence is zero: no unit had progress when it was fenced.
func zeroFences(fences map[string]string) bool {
	for _, f := range fences {
		if f != meta.ZeroFence() {
			return false
		}
	}
	return true
}

// watermark waits until the archive holds every record put before the cut: then it holds
// every record at or below a fence. A stream that wasn't fresh needs the region's archive.
func (s *step) watermark(ctx context.Context, j meta.Job) (time.Duration, bool, error) {
	if s.env.Archive == nil {
		return 0, false, fmt.Errorf("backfill: %s: the stream has events, and this environment can't read its archive", s.version)
	}
	ok, err := s.env.Archive.Complete(ctx, j.Stream, j.Cut)
	switch {
	case err != nil:
		return 0, false, err
	case !ok:
		return watermarkPoll, false, nil
	}
	// Before the job pays for an export: a job that an older build left here has no origin.
	if err = s.checkArchive(ctx, j); err != nil {
		return 0, false, err
	}
	return 0, false, s.advance(ctx, meta.PhaseWatermark, meta.PhaseStage, nil)
}

// apply merges the runs, at most a fan-in of chains of them, and applies the records the
// split keeps a chunk at a time. The cursor moves only after a chunk's rows all succeeded, so
// every worker starts at a completed prefix (ADR 0011 §3).
func (s *step) apply(ctx context.Context, j meta.Job) (time.Duration, bool, error) {
	cs := chains(j.Runs)
	if len(cs) > s.budget.fanIn {
		return 0, false, s.mergePass(ctx, j, cs)
	}
	m, err := openMerge(ctx, s.env.Runs, j.Staging, cs, j.Mark)
	if err != nil {
		return 0, false, err
	}
	for s.left() {
		wait, end, cerr := s.applyChunk(ctx, m, j.Stream)
		if cerr != nil || wait > 0 || end {
			return wait, false, cerr
		}
	}
	return 0, false, nil
}

// applyChunk reads the job again, for its state and rate, and applies the next chunk unless
// the projector's lag or the rate makes the step wait. It reports whether the merge ended.
func (s *step) applyChunk(ctx context.Context, m *merger, stream string) (time.Duration, bool, error) {
	cur, err := s.job(ctx)
	switch {
	case err != nil:
		return 0, false, err
	case cur.Phase != meta.PhaseApply:
		return 0, false, errMoved
	}
	paused, err := s.lagged(ctx, cur)
	switch {
	case err != nil:
		return 0, false, err
	case paused:
		return lagPoll, false, nil
	}
	c, err := s.nextChunk(ctx, m, stream)
	if err != nil {
		return 0, false, err
	}
	if len(c.recs) == 0 {
		return 0, c.end, s.moveCursor(ctx, c)
	}
	if wait := s.pace.take(cur.Rate, len(c.recs)); wait > 0 {
		if time.Now().Add(wait).After(s.cutoff) {
			return wait, false, nil
		}
		if err = sleep(ctx, wait); err != nil {
			return 0, false, err
		}
	}
	if err = s.applyRecords(ctx, c.recs); err != nil {
		return 0, false, err
	}
	return 0, c.end, s.moveCursor(ctx, c)
}

// applyRecords applies a chunk, trying it again a few times while the step has time: rows
// skip the records their marks hold, and a new step would read the job and open the runs
// again.
func (s *step) applyRecords(ctx context.Context, recs []projector.BackfillRecord) error {
	for try := 1; ; try++ {
		err := s.env.Core.ApplyBackfill(ctx, s.version, recs)
		if err == nil || errors.Is(err, projector.ErrBroken) || try == chunkTries || !s.left() {
			return err
		}
		if err = sleep(ctx, chunkRetry); err != nil {
			return err
		}
	}
}

// chunk is what apply takes from the merge at once: the records it applies, the mark of the
// last record it took, kept or not, the records it skipped, and whether the merge ended.
type chunk struct {
	recs    []projector.BackfillRecord
	last    *keys.Mark
	skipped int64
	end     bool
}

// nextChunk takes the merge's next records, at most a chunk of them, kept or not: the cursor
// then moves past records the split skips as often as past those it applies.
func (s *step) nextChunk(ctx context.Context, m *merger, stream string) (chunk, error) {
	var c chunk
	size := s.env.Chunk
	if size <= 0 {
		size = chunkRecords
	}
	repeats := m.repeats
	for range size {
		rec, ok, err := m.next(ctx)
		if err != nil {
			return chunk{}, err
		}
		if !ok {
			c.end = true
			break
		}
		c.last = &rec.mark
		keep, err := s.keeps(ctx, stream, &rec)
		if err != nil {
			return chunk{}, err
		}
		if !keep {
			c.skipped++
			continue
		}
		c.recs = append(c.recs, rec.backfillRecord())
	}
	c.skipped += m.repeats - repeats
	return c, nil
}

// keeps reports whether the split gives rec to the backfill (lifecycle.md §5.1): with the
// fence split, a record at or below its unit's fence, or any record of a unit without one.
// Fences are read after the export, when every unit with a record that a projector applied
// the version to has one. A table item was kept or not when it was staged.
func (s *step) keeps(ctx context.Context, stream string, rec *record) (bool, error) {
	if rec.unit == "" {
		return true, nil
	}
	if s.unit != rec.unit {
		u, err := s.env.Meta.Unit(ctx, stream, rec.unit)
		if err != nil {
			return false, err
		}
		s.unit = rec.unit
		s.fence, s.fenced = u.Fences[s.version]
	}
	return !s.fenced || rec.seq <= s.fence, nil
}

// moveCursor stores a chunk's last mark, once its rows succeeded, and moves the job to live
// after the last chunk. A cursor that another worker moved further stays.
func (s *step) moveCursor(ctx context.Context, c chunk) error {
	_, err := s.update(ctx, meta.PhaseApply, func(j *meta.Job) (bool, error) {
		ahead := c.last != nil && (j.Mark == nil || j.Mark.Compare(*c.last) < 0)
		if ahead {
			j.Mark = c.last
			j.Applied += int64(len(c.recs))
			j.Skipped += c.skipped
			if len(c.recs) > 0 {
				j.Chunks++
			}
		}
		if c.end {
			j.Phase = meta.PhaseLive
		}
		return ahead || c.end, nil
	})
	return err
}

// live makes the version live, partial when it was filled from a table that may have lost
// events, and deletes the staged objects. From the archive, it checks first that the archive
// is still complete.
func (s *step) live(ctx context.Context, j meta.Job) error {
	if j.Split == meta.SplitFence {
		if err := s.checkArchive(ctx, j); err != nil {
			return err
		}
	}
	since, err := s.partialSince(ctx, j)
	if err != nil {
		return err
	}
	if err = s.env.Meta.MakeLive(ctx, s.version, since); err != nil {
		return err
	}
	if err = s.env.Runs.DeletePrefix(ctx, j.Staging); err != nil {
		return fmt.Errorf("backfill: delete the staging of %s: %w", s.version, err)
	}
	return s.done(ctx, false)
}

// restage stages again after a staged object went missing (lifecycle.md §5.3), a table
// source from the start of its scan. The cursor stays: the kept set doesn't change, but what
// TTL deleted meanwhile, and rows skip what their marks hold.
func (s *step) restage(ctx context.Context, j meta.Job) error {
	s.env.Log.WarnContext(ctx, "backfill: a staged object is missing; staging again", slog.String("version", s.version))
	_, err := s.update(ctx, j.Phase, func(j *meta.Job) (bool, error) {
		j.Phase, j.Runs, j.Merge, j.Stage = meta.PhaseStage, nil, nil, meta.Stage{}
		j.Archive = meta.ArchiveStage{Scanned: j.Archive.Scanned, Tries: j.Archive.Tries}
		if j.Split == meta.SplitFence {
			// A new count gives the partitions a new listing: the old one's results may be gone.
			j.Archive.Tries++
		}
		return true, nil
	})
	return err
}

// job reads the job, which this attempt must own, and records the execution in it.
func (s *step) job(ctx context.Context) (meta.Job, error) {
	j, ok, err := s.env.Meta.Job(ctx, s.version)
	switch {
	case err != nil:
		return meta.Job{}, err
	case !ok:
		return meta.Job{}, fmt.Errorf("backfill: %s: %w", s.version, meta.ErrNoJob)
	case !s.owns(j):
		return meta.Job{}, errNotOwner
	case j.Execution == s.execution:
		return j, nil
	}
	return s.update(ctx, j.Phase, func(j *meta.Job) (bool, error) {
		write := j.Execution != s.execution
		j.Execution = s.execution
		return write, nil
	})
}

// update changes the job while this attempt owns it in phase: errNotOwner when it doesn't,
// errMoved when the job is in another phase.
func (s *step) update(ctx context.Context, phase meta.Phase, change func(*meta.Job) (bool, error)) (meta.Job, error) {
	return s.env.Meta.UpdateJob(ctx, s.version, func(j *meta.Job) (bool, error) {
		switch {
		case !s.owns(*j):
			return false, errNotOwner
		case j.Phase != phase:
			return false, errMoved
		}
		return change(j)
	})
}

// advance moves the job from phase from to phase to, with change.
func (s *step) advance(ctx context.Context, from, to meta.Phase, change func(*meta.Job)) error {
	_, err := s.update(ctx, from, func(j *meta.Job) (bool, error) {
		j.Phase = to
		if change != nil {
			change(j)
		}
		return true, nil
	})
	if err == nil {
		s.env.Log.InfoContext(ctx, "backfill: phase", slog.String("version", s.version), slog.String("phase", string(to)))
	}
	return err
}

// stop ends the job in state, recording cause when it isn't nil.
func (s *step) stop(ctx context.Context, state meta.JobState, phase meta.Phase, cause error) error {
	return s.end(ctx, state, func(j *meta.Job) {
		if cause != nil {
			j.Error = &meta.JobError{Phase: phase, Message: cause.Error(), At: time.Now()}
		}
	})
}

// done ends the job done, its version live, by the fresh rule when fresh is set.
func (s *step) done(ctx context.Context, fresh bool) error {
	return s.end(ctx, meta.JobDone, func(j *meta.Job) { j.Phase, j.Fresh = meta.PhaseLive, fresh })
}

// end ends the job in state, with change, unless another attempt owns it.
func (s *step) end(ctx context.Context, state meta.JobState, change func(*meta.Job)) error {
	_, err := s.env.Meta.UpdateJob(ctx, s.version, func(j *meta.Job) (bool, error) {
		if !s.owns(*j) {
			return false, errNotOwner
		}
		j.State = state
		change(j)
		return true, nil
	})
	if errors.Is(err, errNotOwner) {
		return nil
	}
	if err == nil {
		s.env.Log.InfoContext(ctx, "backfill: job ended", slog.String("version", s.version), slog.String("state", string(state)))
	}
	return err
}

// ended ends the job, and reports whether it did, when its version broke or was retired,
// whatever its phase: a late record or a live one can break the version, and a deploy can
// retire it, while the job runs. It only saves the job's work, since MakeLive refuses such a
// version, so a read or write that fails leaves it to the next step.
func (s *step) ended(ctx context.Context) bool {
	states, err := s.env.Meta.ViewStates(ctx)
	vs := states[s.version]
	if err != nil || vs.State != meta.Broken && vs.State != meta.Retired {
		return false
	}
	state := meta.JobBroken
	if vs.State == meta.Retired {
		state = meta.JobCancelled
	}
	j, err := s.job(ctx)
	return err == nil && s.stop(ctx, state, j.Phase, fmt.Errorf("backfill: %s is %s", s.version, vs.State)) == nil
}

// fail ends the step on err. A broken version ends the job broken, a retired one cancelled,
// and one that another worker of the attempt made live done; any other error is recorded in
// the job and returned.
func (s *step) fail(ctx context.Context, phase meta.Phase, err error) (time.Duration, bool, error) {
	switch {
	case errors.Is(err, errNotOwner):
		return 0, true, nil
	case errors.Is(err, projector.ErrBroken), errors.Is(err, ErrIncomplete):
		return 0, true, s.stop(ctx, meta.JobBroken, phase, err)
	case errors.Is(err, meta.ErrTransition), errors.Is(err, meta.ErrNotActivating):
		states, serr := s.env.Meta.ViewStates(ctx)
		if serr != nil {
			return 0, false, errors.Join(err, serr)
		}
		switch states[s.version].State {
		case meta.Broken:
			return 0, true, s.stop(ctx, meta.JobBroken, phase, err)
		case meta.Retired:
			return 0, true, s.stop(ctx, meta.JobCancelled, phase, err)
		case meta.Live:
			return 0, true, s.done(ctx, false)
		case meta.PendingBackfill, meta.Activating:
		}
	}
	// A step whose caller cancelled it, or whose deadline passed, can't record anything.
	if phase != "" && ctx.Err() == nil {
		_, rerr := s.update(ctx, phase, func(j *meta.Job) (bool, error) {
			j.Error = &meta.JobError{Phase: phase, Message: err.Error(), At: time.Now()}
			return true, nil
		})
		if rerr != nil {
			s.env.Log.WarnContext(ctx, "backfill: record a failure", slog.String("version", s.version), slog.Any("error", rerr))
		}
	}
	return 0, false, err
}

// sleep waits d, or until ctx ends.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

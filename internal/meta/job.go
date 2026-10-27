// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// Job item attributes, besides the view-state item's stream, split, state and error.
const (
	jobPK         = "job"
	attrSource    = "source"
	attrPhase     = "phase"
	attrAttempt   = "attempt"
	attrName      = "name"
	attrExecution = "execution"
	attrCreatedAt = "created_at"
	attrUpdatedAt = "updated_at"
	attrCut       = "cut"
	attrReadAfter = "read_after"
	attrScannedAt = "scanned_at"
	attrFresh     = "fresh"
	attrPaused    = "paused"
	attrWaits     = "waits"
	attrStaging   = "staging"
	attrStage     = "stage"
	attrSegments  = "segments"
	attrNext      = "next"
	attrFinished  = "finished"
	attrRuns      = "runs"
	attrRecords   = "records"
	attrFirst     = "first"
	attrLast      = "last"
	attrMerge     = "merge"
	attrInputs    = "inputs"
	attrOutputs   = "outputs"
	attrMark      = "mark"
	attrStaged    = "staged"
	attrApplied   = "applied"
	attrSkipped   = "skipped"
	attrChunks    = "chunks"
	attrRate      = "rate"
	attrMessage   = "message"

	sourceArchive = "archive"
	sourceTable   = "table"

	// hwmBackfill is the _hwm entry that holds a backfill's mark (encoding.md §7.6).
	hwmBackfill = "B"
)

// jobLifetime is how long a done or broken job's item stays.
const jobLifetime = 400 * 24 * time.Hour

var (
	ErrJobExists = errors.New("meta: the version has a job")
	ErrNoJob     = errors.New("meta: the version has no job")
)

// JobState is a backfill job's state.
type JobState string

// Job states.
const (
	JobRunning   JobState = "running"
	JobCancelled JobState = "cancelled"
	JobDone      JobState = "done"
	JobBroken    JobState = "broken"
)

// Wait is what a running job waits for that this release can't do: no step runs it until a
// later release resumes it.
type Wait string

// WaitArchive is the read of a region's archive of a stream that has events, which arrives in
// a later release (M6): a job split by fences waits for it at its watermark.
const WaitArchive Wait = "archive_read"

// Phase is the step a backfill job is at (docs/spec/encoding.md §8.3).
type Phase string

// Phases.
const (
	PhaseActivate  Phase = "activate"
	PhaseFence     Phase = "fence"
	PhaseFresh     Phase = "fresh"
	PhaseWatermark Phase = "watermark"
	PhaseWait      Phase = "wait"
	PhaseStage     Phase = "stage"
	PhaseApply     Phase = "apply"
	PhaseLive      Phase = "live"
)

// Job is a backfill job's item (docs/spec/encoding.md §8.3). The source follows from Split.
type Job struct {
	Version, Stream string
	Split           Split
	State           JobState
	Phase           Phase
	Attempt         int    // the attempt that owns the job, from 1
	Name            string // the name of its executions without -a<attempt> and -c<k>
	Execution       string // the ARN of the execution that ran the last step, or "local"

	CreatedAt, UpdatedAt time.Time
	Cut                  time.Time // fence split: when the fences were taken; zero before
	ReadAfter            time.Time // table source: the earliest time the scan may start; zero before
	ScannedAt            time.Time // table source: when the scan ended; zero before
	Fresh                bool      // once done: whether the fresh-stream rule made the version live

	Staging string // the staging prefix
	Stage   Stage
	Runs    []Run  // the committed runs
	Merge   *Merge // the merge pass in progress; nil when none
	Staged  int64  // records staged

	Mark                     *keys.Mark // the cursor: the mark of the last record of the last chunk done
	Applied, Skipped, Chunks int64
	Rate                     int       // records per second; 0 for no limit
	Paused                   bool      // apply waits for the projector's lag to fall
	Waits                    Wait      // what the job waits for; "" when nothing
	Error                    *JobError // the last failure; nil when none

	version int64 // _v; 0 before the item exists
}

// Stage is where a table source's staging resumes.
type Stage struct {
	Segments int               // the scan's segment count; 0 before staging
	Next     map[int]store.Key // a segment → the key of the last item staged from it
	Finished []int             // the segments read to their end
}

// Run is a committed run of staged records.
type Run struct {
	Name        string
	Records     int64
	First, Last keys.Mark // the marks of its first and last records
}

// Merge is a merge pass in progress.
type Merge struct {
	Inputs  []string   // the names of the runs it merges
	Outputs []Run      // the runs it wrote so far
	Mark    *keys.Mark // the last mark it wrote; nil before the first
}

// JobError is a job's last failure.
type JobError struct {
	Phase   Phase
	Message string // at most 1 KB is stored
	At      time.Time
}

func jobKey(version string) store.Key { return store.Key{PK: jobPK, SK: version} }

// Job reads a version's job item with a consistent read, and reports whether it exists.
func (m *Meta) Job(ctx context.Context, version string) (Job, bool, error) {
	it, err := m.b.Get(ctx, m.table, jobKey(version), true)
	if err != nil {
		return Job{}, false, fmt.Errorf("meta: read the job of %s: %w", version, err)
	}
	if it == nil {
		return Job{}, false, nil
	}
	j, err := m.decodeJob(it)
	return j, err == nil, err
}

// Jobs reads every job item with consistent reads, in the order of their versions.
func (m *Meta) Jobs(ctx context.Context) ([]Job, error) {
	var out []Job
	for it, err := range m.b.Query(ctx, m.table, store.Query{PK: jobPK, Consistent: true}) {
		if err != nil {
			return nil, fmt.Errorf("meta: read jobs: %w", err)
		}
		j, derr := m.decodeJob(it)
		if derr != nil {
			return nil, derr
		}
		out = append(out, j)
	}
	return out, nil
}

// CreateJob writes j as the version's job, stamping UpdatedAt, and returns it as stored. It
// fails with [ErrJobExists] when the version has a job.
func (m *Meta) CreateJob(ctx context.Context, j Job) (Job, error) {
	j.UpdatedAt, j.version = time.Now(), 1
	it, stored, err := m.encodeJob(j)
	if err != nil {
		return Job{}, err
	}
	err = m.b.Put(ctx, m.table, it, store.IfAbsent())
	var ce *store.ConditionError
	if !errors.As(err, &ce) {
		if err != nil {
			return Job{}, fmt.Errorf("meta: create the job of %s: %w", j.Version, err)
		}
		return stored, nil
	}
	found, err := m.decodeJob(ce.Current)
	if err != nil {
		return Job{}, err
	}
	// A write the SDK retried after a lost response finds itself.
	if !reflect.DeepEqual(found, stored) {
		return Job{}, fmt.Errorf("meta: create the job of %s: %w", j.Version, ErrJobExists)
	}
	return stored, nil
}

// UpdateJob applies change to a version's job until the conditional write holds, stamping
// UpdatedAt, and returns the job as stored. change reports whether there is anything to
// write. After the SDK retried a write whose response was lost, change runs again on the job
// that write stored, so it must find its own change done there. A version without a job
// fails with [ErrNoJob].
func (m *Meta) UpdateJob(ctx context.Context, version string, change func(*Job) (bool, error)) (Job, error) {
	j, ok, err := m.Job(ctx, version)
	if err != nil {
		return Job{}, err
	}
	for range maxAttempts {
		if !ok {
			return Job{}, fmt.Errorf("meta: update the job of %s: %w", version, ErrNoJob)
		}
		write, cerr := change(&j)
		if cerr != nil {
			return Job{}, cerr
		}
		if !write {
			return j, nil
		}
		if j.Version != version {
			return Job{}, fmt.Errorf("meta: update the job of %s: the change renamed it %s", version, j.Version)
		}
		stored, done, werr := m.writeJob(ctx, j)
		if werr != nil || done {
			return stored, werr
		}
		j, ok = stored, stored.version > 0
	}
	return Job{}, m.errAttempts(jobKey(version))
}

// writeJob writes j, stamping UpdatedAt, in place of the job it was read as, and returns it as
// stored and true. When that is no longer the stored job, it returns the stored one, the zero
// Job when there is none, and false.
func (m *Meta) writeJob(ctx context.Context, j Job) (Job, bool, error) {
	prev := j.version
	j.UpdatedAt, j.version = time.Now(), prev+1
	it, stored, err := m.encodeJob(j)
	if err != nil {
		return Job{}, false, err
	}
	err = m.b.Put(ctx, m.table, it, cond(prev))
	ce, lost := errors.AsType[*store.ConditionError](err)
	switch {
	case !lost && err != nil:
		return Job{}, false, fmt.Errorf("meta: write the job of %s: %w", j.Version, err)
	case !lost:
		return stored, true, nil
	case ce.Current == nil:
		return Job{}, false, nil
	}
	current, err := m.decodeJob(ce.Current)
	return current, false, err
}

// encodeJob returns j's item, and j as a read of that item returns it, so a job that can't be
// read back fails before it is written.
func (m *Meta) encodeJob(j Job) (store.Item, Job, error) {
	it, err := encodeJob(j)
	if err != nil {
		return nil, Job{}, fmt.Errorf("meta: job of %s: %w", j.Version, err)
	}
	if size := store.ItemSize(it); size > store.MaxItemSize {
		return nil, Job{}, fmt.Errorf("meta: the job of %s would take %d bytes with %d staged runs, over DynamoDB's item limit of %d: "+
			"its source is too large to stage (lifecycle.md §11)", j.Version, size, len(j.Runs), store.MaxItemSize)
	}
	stored, err := m.decodeJob(it)
	return it, stored, err
}

func encodeJob(j Job) (store.Item, error) {
	k := jobKey(j.Version)
	it := store.Item{
		catalog.AttrPK:         str(k.PK),
		catalog.AttrSK:         str(k.SK),
		attrStream:             str(j.Stream),
		attrSource:             str(sourceOf(j.Split)),
		attrSplit:              str(string(j.Split)),
		attrState:              str(string(j.State)),
		attrPhase:              str(string(j.Phase)),
		attrAttempt:            num(int64(j.Attempt)),
		attrName:               str(j.Name),
		attrExecution:          str(j.Execution),
		attrStaging:            str(j.Staging),
		attrStaged:             num(j.Staged),
		attrApplied:            num(j.Applied),
		attrSkipped:            num(j.Skipped),
		attrChunks:             num(j.Chunks),
		attrRate:               num(int64(j.Rate)),
		catalog.AttrRowVersion: num(j.version),
	}
	putTime(it, attrCreatedAt, j.CreatedAt)
	putTime(it, attrUpdatedAt, j.UpdatedAt)
	putTime(it, attrCut, j.Cut)
	putTime(it, attrReadAfter, j.ReadAfter)
	putTime(it, attrScannedAt, j.ScannedAt)
	if j.State == JobDone || j.Fresh {
		it[attrFresh] = store.Value{Type: store.BOOL, Bool: j.Fresh}
	}
	if j.Paused {
		it[attrPaused] = store.Value{Type: store.BOOL, Bool: true}
	}
	if j.Waits != "" {
		it[attrWaits] = str(string(j.Waits))
	}
	if s := j.Stage; s.Segments != 0 || len(s.Next) > 0 || len(s.Finished) > 0 {
		it[attrStage] = encodeStage(s)
	}
	if j.State == JobDone || j.State == JobBroken {
		it[catalog.AttrTTL] = keys.TTL(micros(j.UpdatedAt.Add(jobLifetime)))
	}
	if j.Error != nil {
		it[attrError] = store.Value{Type: store.M, Map: map[string]store.Value{
			attrPhase:   str(string(j.Error.Phase)),
			attrMessage: str(cutError(j.Error.Message)),
			attrAt:      num(micros(j.Error.At)),
		}}
	}
	return it, encodeMarks(it, j)
}

// sourceOf returns the source that a split reads.
func sourceOf(s Split) string {
	if s == SplitFence {
		return sourceArchive
	}
	return sourceTable
}

func encodeStage(s Stage) store.Value {
	stage := map[string]store.Value{attrSegments: num(int64(s.Segments))}
	if len(s.Next) > 0 {
		next := make(map[string]store.Value, len(s.Next))
		for seg, k := range s.Next {
			next[strconv.Itoa(seg)] = store.Value{Type: store.M, Map: map[string]store.Value{
				catalog.AttrPK: str(k.PK),
				catalog.AttrSK: str(k.SK),
			}}
		}
		stage[attrNext] = store.Value{Type: store.M, Map: next}
	}
	if len(s.Finished) > 0 {
		finished := make([]store.Value, len(s.Finished))
		for i, seg := range s.Finished {
			finished[i] = num(int64(seg))
		}
		stage[attrFinished] = store.Value{Type: store.L, List: finished}
	}
	return store.Value{Type: store.M, Map: stage}
}

// encodeMarks sets the attributes of j that hold marks: runs, merge and mark.
func encodeMarks(it store.Item, j Job) error {
	if len(j.Runs) > 0 {
		runs, err := encodeRuns(j.Runs)
		if err != nil {
			return err
		}
		it[attrRuns] = runs
	}
	if mg := j.Merge; mg != nil {
		inputs := make([]store.Value, len(mg.Inputs))
		for i, name := range mg.Inputs {
			inputs[i] = str(name)
		}
		merge := map[string]store.Value{attrInputs: {Type: store.L, List: inputs}}
		if len(mg.Outputs) > 0 {
			outputs, err := encodeRuns(mg.Outputs)
			if err != nil {
				return err
			}
			merge[attrOutputs] = outputs
		}
		if err := putMark(merge, attrMark, mg.Mark); err != nil {
			return err
		}
		it[attrMerge] = store.Value{Type: store.M, Map: merge}
	}
	return putMark(it, attrMark, j.Mark)
}

func encodeRuns(runs []Run) (store.Value, error) {
	out := make([]store.Value, len(runs))
	for i, r := range runs {
		run := map[string]store.Value{attrName: str(r.Name), attrRecords: num(r.Records)}
		if err := putMark(run, attrFirst, &r.First); err != nil {
			return store.Value{}, err
		}
		if err := putMark(run, attrLast, &r.Last); err != nil {
			return store.Value{}, err
		}
		out[i] = store.Value{Type: store.M, Map: run}
	}
	return store.Value{Type: store.L, List: out}, nil
}

// putMark sets the attribute name to mk as encoding.md §7.6's B value, leaving a nil mk out.
// The _hwm codec owns that form, so the mark goes through it.
func putMark(attrs map[string]store.Value, name string, mk *keys.Mark) error {
	if mk == nil {
		return nil
	}
	h, err := keys.HWM{Backfill: mk}.Encode()
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	attrs[name] = h.Map[hwmBackfill]
	return nil
}

// mark reads a mark that [putMark] wrote, and reports whether it is present.
func (r *reader) mark(name string, required bool) (keys.Mark, bool) {
	v, ok := r.value(name, store.B, required)
	if !ok {
		return keys.Mark{}, false
	}
	h, err := keys.DecodeHWM(store.Value{Type: store.M, Map: map[string]store.Value{hwmBackfill: v}}, true)
	if err != nil {
		r.failf("%s: %w", name, err)
		return keys.Mark{}, false
	}
	return *h.Backfill, true
}

// optMark reads a mark; nil when it is absent.
func (r *reader) optMark(name string) *keys.Mark {
	if mk, ok := r.mark(name, false); ok {
		return &mk
	}
	return nil
}

func (m *Meta) decodeJob(it store.Item) (Job, error) {
	r := reader{attrs: it}
	r.str(catalog.AttrPK)
	j := Job{
		Version:   r.str(catalog.AttrSK),
		Stream:    r.text(attrStream),
		Split:     Split(r.str(attrSplit)),
		State:     JobState(r.str(attrState)),
		Phase:     Phase(r.str(attrPhase)),
		Attempt:   int(r.count(attrAttempt)),
		Name:      r.text(attrName),
		Execution: r.text(attrExecution),
		CreatedAt: r.instant(attrCreatedAt, true),
		UpdatedAt: r.instant(attrUpdatedAt, true),
		Staging:   r.text(attrStaging),
		Runs:      r.runs(attrRuns),
		Merge:     r.merge(),
		Staged:    r.count(attrStaged),
		Mark:      r.optMark(attrMark),
		Applied:   r.count(attrApplied),
		Skipped:   r.count(attrSkipped),
		Chunks:    r.count(attrChunks),
		Rate:      int(r.count(attrRate)),
		Error:     r.jobError(),
	}
	if err := checkSplit(j.Split); err != nil {
		r.failf("%w", err)
	}
	if source := r.str(attrSource); source != sourceOf(j.Split) {
		r.failf("%s %q doesn't go with %s %q", attrSource, source, attrSplit, j.Split)
	}
	if !validJobState(j.State) || !validPhase(j.Phase) || j.Attempt < 1 {
		r.failf("%s %q, %s %q or %s %d is out of range", attrState, j.State, attrPhase, j.Phase, attrAttempt, j.Attempt)
	}
	if j.Split == SplitFence {
		j.Cut = r.instant(attrCut, false)
	} else {
		j.ReadAfter = r.instant(attrReadAfter, false)
		j.ScannedAt = r.instant(attrScannedAt, false)
		j.Stage = r.stage()
	}
	if j.State == JobDone {
		j.Fresh = r.boolean(attrFresh)
	}
	if _, ok := it[attrPaused]; ok {
		j.Paused = r.boolean(attrPaused)
	}
	if _, ok := it[attrWaits]; ok {
		if j.Waits = Wait(r.str(attrWaits)); j.Waits != WaitArchive || j.Split != SplitFence {
			r.failf("%s %q doesn't go with %s %q", attrWaits, j.Waits, attrSplit, j.Split)
		}
	}
	if j.State == JobDone || j.State == JobBroken {
		r.integer(catalog.AttrTTL, true)
	}
	j.version = r.version()
	if err := r.close(); err != nil {
		return Job{}, m.decodeError(it, err)
	}
	return j, nil
}

func validJobState(s JobState) bool {
	switch s {
	case JobRunning, JobCancelled, JobDone, JobBroken:
		return true
	}
	return false
}

func validPhase(p Phase) bool {
	switch p {
	case PhaseActivate, PhaseFence, PhaseFresh, PhaseWatermark, PhaseWait, PhaseStage, PhaseApply, PhaseLive:
		return true
	}
	return false
}

// stage reads a table source's stage; the zero Stage when it is absent.
func (r *reader) stage() Stage {
	var s Stage
	r.nested(attrStage, false, func(sr *reader) {
		s.Segments = int(sr.count(attrSegments))
		segment := func(n int) int {
			if n < 0 || n >= s.Segments {
				sr.failf("segment %d of %d", n, s.Segments)
			}
			return n
		}
		sr.nested(attrNext, false, func(nr *reader) {
			s.Next = make(map[int]store.Key, len(nr.attrs))
			for _, name := range slices.Sorted(maps.Keys(nr.attrs)) {
				n, err := strconv.Atoi(name)
				if err != nil || strconv.Itoa(n) != name {
					nr.failf("segment %q", name)
				}
				nr.nested(name, true, func(kr *reader) {
					s.Next[segment(n)] = store.Key{PK: kr.str(catalog.AttrPK), SK: kr.str(catalog.AttrSK)}
				})
			}
		})
		for _, v := range sr.list(attrFinished, false) {
			n, err := strconv.Atoi(v.Str)
			if v.Type != store.N || err != nil {
				sr.failf("%s holds %v", attrFinished, v)
			}
			s.Finished = append(s.Finished, segment(n))
		}
	})
	return s
}

func (r *reader) runs(name string) []Run {
	var out []Run
	r.each(name, false, func(rr *reader) {
		run := Run{Name: rr.text(attrName), Records: rr.count(attrRecords)}
		run.First, _ = rr.mark(attrFirst, true)
		run.Last, _ = rr.mark(attrLast, true)
		out = append(out, run)
	})
	return out
}

// merge reads the merge pass; nil when it is absent.
func (r *reader) merge() *Merge {
	var mg *Merge
	r.nested(attrMerge, false, func(mr *reader) {
		mg = &Merge{Outputs: mr.runs(attrOutputs), Mark: mr.optMark(attrMark)}
		for _, in := range mr.list(attrInputs, true) {
			if in.Type != store.S || in.Str == "" {
				mr.failf("%s holds %v", attrInputs, in)
			}
			mg.Inputs = append(mg.Inputs, in.Str)
		}
	})
	return mg
}

// jobError reads a job's last failure; nil when it is absent.
func (r *reader) jobError() *JobError {
	var e *JobError
	r.nested(attrError, false, func(er *reader) {
		e = &JobError{Phase: Phase(er.str(attrPhase)), Message: er.str(attrMessage), At: er.instant(attrAt, true)}
		if !validPhase(e.Phase) {
			er.failf("%s %q is unknown", attrPhase, e.Phase)
		}
	})
	return e
}

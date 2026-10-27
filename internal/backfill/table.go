// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// A table scan reads scanWorkers segments at once, and has segmentsPerWorker times as many
// segments by default, so that its segments stay small.
const (
	scanWorkers       = 4
	segmentsPerWorker = 16
)

// deleteMargin is how long after an event's expiry DynamoDB may delete its item: its _ttl
// (encoding.md §5.4).
const deleteMargin = time.Hour

// stageTable reads the region's replica of the stream table into runs, with consistent reads:
// with the time split, the items whose ingest_time is at or before the split time, expired
// ones included; with none, every item. Each run commits with the scan positions it reaches,
// in one write that holds only while the positions are those the step read from: a stale or
// duplicate worker's run is then garbage, and no range is in two runs or in none.
func (s *step) stageTable(ctx context.Context, j meta.Job) (time.Duration, bool, error) {
	if j.Stage.Segments == 0 {
		return 0, false, s.startScan(ctx)
	}
	splitAt, err := s.splitAt(ctx, j)
	if err != nil {
		return 0, false, err
	}
	from, at := j.Stage, j.Stage
	var buf []record
	size := 0
	for segs := pending(at, scanWorkers); len(segs) > 0 && s.left(); segs = pending(at, scanWorkers) {
		recs, read, next, rerr := s.readRound(ctx, j.Stream, segs, at, splitAt)
		if rerr != nil {
			return 0, false, rerr
		}
		buf, at = append(buf, recs...), next
		for i := range recs {
			size += recs[i].size
		}
		wait, perr := s.paceScan(ctx, j.Rate, read)
		switch {
		case perr != nil:
			return 0, false, perr
		case wait > 0:
			return wait, false, s.commitScan(ctx, &j, from, buf, at)
		}
		if size >= s.budget.buffer {
			if err = s.commitScan(ctx, &j, from, buf, at); err != nil {
				return 0, false, err
			}
			from, buf, size = at, nil, 0
		}
	}
	return 0, false, s.commitScan(ctx, &j, from, buf, at)
}

// paceScan books read items at rate, records a second, and waits for them; when that would
// pass the step's cutoff, it returns how long the next step should wait instead.
func (s *step) paceScan(ctx context.Context, rate, read int) (time.Duration, error) {
	wait := s.pace.take(rate, read)
	switch {
	case wait <= 0:
		return 0, nil
	case time.Now().Add(wait).After(s.cutoff):
		return wait, nil
	}
	return 0, sleep(ctx, wait)
}

// startScan fixes the scan's segment count, on which the positions in the job depend.
func (s *step) startScan(ctx context.Context) error {
	_, err := s.update(ctx, meta.PhaseStage, func(j *meta.Job) (bool, error) {
		if j.Stage.Segments != 0 {
			return false, nil
		}
		j.Stage = meta.Stage{Segments: cmp.Or(s.env.Segments, scanWorkers*segmentsPerWorker)}
		return true, nil
	})
	return err
}

// splitAt returns the version's split time in microseconds; with the none split, the latest
// time, which keeps every item.
func (s *step) splitAt(ctx context.Context, j meta.Job) (int64, error) {
	if j.Split != meta.SplitTime {
		return catalog.TimeLimitMicros, nil
	}
	states, err := s.env.Meta.ViewStates(ctx)
	if err != nil {
		return 0, err
	}
	if vs := states[s.version]; vs.Split == meta.SplitTime {
		return vs.SplitAt.UnixMicro(), nil
	}
	return 0, fmt.Errorf("backfill: %s has no split time", s.version)
}

// pending returns up to n segments that the scan at st hasn't read to their end.
func pending(st meta.Stage, n int) []int {
	var out []int
	for seg := 0; seg < st.Segments && len(out) < n; seg++ {
		if !slices.Contains(st.Finished, seg) {
			out = append(out, seg)
		}
	}
	return out
}

// moved returns st after a page of seg that ends at next, nil after the segment's last page.
func moved(st meta.Stage, seg int, next *store.Key) meta.Stage {
	out := meta.Stage{Segments: st.Segments, Next: maps.Clone(st.Next), Finished: slices.Clone(st.Finished)}
	if next == nil {
		delete(out.Next, seg)
		out.Finished = append(out.Finished, seg)
		slices.Sort(out.Finished)
		return out
	}
	if out.Next == nil {
		out.Next = map[int]store.Key{}
	}
	out.Next[seg] = *next
	return out
}

func sameStage(a, b meta.Stage) bool {
	return a.Segments == b.Segments && maps.Equal(a.Next, b.Next) && slices.Equal(a.Finished, b.Finished)
}

// readRound reads the next page of each of segs, at once, and returns the records the backfill
// keeps, how many items it read, and the scan's positions after them. An item that isn't an
// event, or whose ingest_time the time split can't read, breaks the version, as it does in
// live projection.
func (s *step) readRound(ctx context.Context, stream string, segs []int, at meta.Stage, splitAt int64) ([]record, int, meta.Stage, error) {
	var recs []record
	read := 0
	table := s.env.Naming.StreamTable(stream)
	for i, p := range s.readPages(ctx, table, segs, at) {
		if p.err != nil {
			return nil, 0, at, p.err
		}
		for _, it := range p.items {
			rec, keep, f := tableRecord(it, splitAt)
			if f != nil {
				if _, _, err := s.env.Meta.MarkBroken(ctx, s.version, stream, *f); err != nil {
					return nil, 0, at, err
				}
				return nil, 0, at, fmt.Errorf("backfill: %s: %s: %w", table, f.Error, projector.ErrBroken)
			}
			if keep {
				recs = append(recs, rec)
			}
		}
		read += len(p.items)
		at = moved(at, segs[i], p.next)
	}
	return recs, read, at, nil
}

// page is a page of a segment, and the key to continue from, nil after the last.
type page struct {
	items []store.Item
	next  *store.Key
	err   error
}

// readPages reads the next page of each of segs at once, and returns them in segs's order.
func (s *step) readPages(ctx context.Context, table string, segs []int, at meta.Stage) []page {
	pages := make([]page, len(segs))
	var wg sync.WaitGroup
	for i, seg := range segs {
		var start *store.Key
		if k, ok := at.Next[seg]; ok {
			start = &k
		}
		sc := store.Scan{Segment: seg, Segments: at.Segments, Consistent: true}
		wg.Go(func() { pages[i] = s.readPage(ctx, table, sc, start) })
	}
	wg.Wait()
	return pages
}

// readPage reads a page, trying again a few times while the step has time.
func (s *step) readPage(ctx context.Context, table string, sc store.Scan, start *store.Key) page {
	for try := 1; ; try++ {
		items, next, err := s.env.Store.ScanPage(ctx, table, sc, start)
		if err == nil || try == chunkTries || !s.left() {
			if err != nil {
				err = fmt.Errorf("backfill: scan %s segment %d: %w", table, sc.Segment, err)
			}
			return page{items: items, next: next, err: err}
		}
		if err = sleep(ctx, chunkRetry); err != nil {
			return page{err: err}
		}
	}
}

// tableRecord returns an item of a stream table as a staged record with its time mark, and
// whether the backfill keeps it: when its ingest_time is at or before splitAt. An item it
// can't stage returns the failure it breaks the version with: cause record for an item that
// isn't an event, which its sort key then names, and cause split for an ingest_time the split
// can't read.
func tableRecord(it store.Item, splitAt int64) (record, bool, *meta.Failure) {
	k := store.Key{PK: it[catalog.AttrPK].Str, SK: it[catalog.AttrSK].Str}
	fail := func(cause meta.Cause, id string, err error) (record, bool, *meta.Failure) {
		return record{}, false, &meta.Failure{Cause: cause, EventID: id, Error: fmt.Sprintf("item %q/%q: %v", k.PK, k.SK, err), At: time.Now()}
	}
	et, id, err := keys.ParseStreamSK(k.SK)
	if err != nil {
		return fail(meta.CauseRecord, k.SK, fmt.Errorf("not an event: %w", err))
	}
	mark, err := keys.TimeMark(et, id)
	if err != nil {
		return fail(meta.CauseRecord, id, err)
	}
	if splitAt < catalog.TimeLimitMicros {
		v, present := it[catalog.AttrIngestTime]
		at, derr := keys.DecodeValue(catalog.EventTimeType(), v, present)
		if derr != nil {
			return fail(meta.CauseSplit, id, fmt.Errorf("%s: %w", catalog.ColumnIngestTime, derr))
		}
		if at.AsMicros() > splitAt {
			return record{}, false, nil
		}
	}
	return record{mark: mark, item: it, size: store.ItemSize(it)}, true, nil
}

// commitScan writes buf as a run and commits it with the scan positions to, in one write that
// holds only while the job's positions are from. The last commit moves the job to apply.
func (s *step) commitScan(ctx context.Context, j *meta.Job, from meta.Stage, buf []record, to meta.Stage) error {
	done := len(pending(to, 1)) == 0
	if len(buf) == 0 && !done && sameStage(from, to) {
		return nil
	}
	run, wrote, err := s.sortedRun(ctx, j, "", buf)
	if err != nil {
		return err
	}
	*j, err = s.update(ctx, meta.PhaseStage, func(j *meta.Job) (bool, error) {
		if !sameStage(j.Stage, from) {
			return false, errMoved
		}
		if wrote {
			j.Runs = append(j.Runs, run)
			j.Staged += run.Records
		}
		j.Stage = to
		if done {
			j.Phase, j.ScannedAt = meta.PhaseApply, time.Now()
		}
		return true, nil
	})
	return err
}

// partialSince returns the earliest ingest_time from which a version filled from the stream
// table holds every event, or zero when it holds them all (lifecycle.md §5.2). An event is in
// the table from its landing, before the scan began, until DynamoDB deletes it once its _ttl
// passed; so the version holds every event from the scan's earliest start, read_after, less
// the stream's LOG RETENTION, and from the scan's end less that and an hour, whichever is
// later. A table born after that, with writer clocks ε off, held every event.
func (s *step) partialSince(ctx context.Context, j meta.Job) (time.Time, error) {
	if j.Split == meta.SplitFence {
		return time.Time{}, nil
	}
	def, ok := s.env.Catalog.Stream(j.Stream)
	if !ok {
		return time.Time{}, fmt.Errorf("backfill: no stream %s in this build's catalog", j.Stream)
	}
	r, expires := def.LogRetention.Period()
	if !expires {
		return time.Time{}, nil
	}
	back := catalog.Interval{N: -r.N, Unit: r.Unit}
	start, err := expr.AddInterval(j.ReadAfter.UnixMicro(), back)
	if err != nil {
		return time.Time{}, err
	}
	end, err := expr.AddInterval(j.ScannedAt.UnixMicro(), back)
	if err != nil {
		return time.Time{}, err
	}
	since := time.UnixMicro(max(start, end-deleteMargin.Microseconds()))
	born, _, err := s.env.Tables.Born(ctx, s.env.Naming.StreamTable(j.Stream))
	switch {
	case err != nil:
		return time.Time{}, fmt.Errorf("backfill: describe %s: %w", s.env.Naming.StreamTable(j.Stream), err)
	case born.Add(-s.env.Bounds.Skew).After(since):
		return time.Time{}, nil
	}
	return since, nil
}

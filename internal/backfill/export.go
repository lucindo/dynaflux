// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
)

// Query is a query that a job starts on the region's archive.
type Query struct {
	Kind   QueryKind
	Stream string
	// From and To bound an export's event times: [From, To). Dest is its destination, a
	// prefix of the job's staging of its own.
	From, To time.Time
	Dest     string
}

// QueryKind is what a query does.
type QueryKind int

// Query kinds.
const (
	// QueryPartitions lists the partitions of the stream's table.
	QueryPartitions QueryKind = iota + 1
	// QueryExport exports the rows of the stream's table whose event time is in a range.
	QueryExport
)

// QueryState is where a query is.
type QueryState int

// Query states.
const (
	QueryRunning QueryState = iota + 1
	QuerySucceeded
	QueryFailed
	QueryCancelled
)

// FailureKind is how a query failed, which decides what the job does next (ADR 0016 §6).
type FailureKind int

// Failure kinds.
const (
	// FailureOther takes a new try, at most sliceTries of them a slice.
	FailureOther FailureKind = iota
	// FailureTimeout is a query that timed out or exhausted its resources: its slice is halved.
	FailureTimeout
	// FailureRetryable takes a new try after a minute, which doesn't count.
	FailureRetryable
	// FailurePermission fails the step; the slice's next try, for once the grants are fixed,
	// doesn't count.
	FailurePermission
)

// QueryStatus is a query's state, and how it failed.
type QueryStatus struct {
	State   QueryState
	Failure FailureKind // when failed
	Reason  string      // Athena's, when failed
	Scanned int64       // the bytes it scanned
}

// Partition is a partition of a stream's table: the event times it holds, [From, To), its
// records and the bytes of its data files.
type Partition struct {
	From, To       time.Time
	Records, Bytes int64
}

// ErrThrottled is a query that Athena didn't start for now: the job starts it again a minute
// later with the same token.
var ErrThrottled = errors.New("backfill: Athena throttled the query's start")

const (
	// sliceBytes is the most bytes of Parquet that consecutive partitions join a slice up to,
	// until the end-to-end suite measures Athena's throughput.
	sliceBytes = 25e9
	// sliceTries is how many failed tries that count a slice takes before the job fails.
	sliceTries = 3
	// pruneSlack is how much more than twice its bytes the first slice may scan before the job
	// fails for partitions that Athena didn't prune.
	pruneSlack = 1e9
	// throttleWait is how long a job waits after Athena throttled it.
	throttleWait = time.Minute
	// maxPlan bounds a plan object.
	maxPlan = 16 << 20
)

// slice is the export of the rows whose event time is in [From, To). A whole slice is made
// of partitions, whose records it yields at least; a half of a slice that timed out isn't.
type slice struct {
	From, To       time.Time
	Records, Bytes int64
	Whole          bool
}

// sliceJSON is a slice in a plan object.
type sliceJSON struct {
	From    int64 `json:"from"`
	To      int64 `json:"to"`
	Records int64 `json:"records,omitzero"`
	Bytes   int64 `json:"bytes"`
	Whole   bool  `json:"whole,omitzero"`
}

// planSlices joins partitions, in the order of their event times, into slices of up to
// sliceBytes each; a larger partition is a slice alone. A range between partitions held
// nothing when they were listed, after the watermark, so whatever is there now is above the
// fences.
func planSlices(parts []Partition) []slice {
	parts = slices.SortedFunc(slices.Values(parts), func(a, b Partition) int { return a.From.Compare(b.From) })
	var out []slice
	for _, p := range parts {
		if n := len(out); n > 0 && out[n-1].Bytes+p.Bytes <= sliceBytes {
			last := &out[n-1]
			last.To, last.Records, last.Bytes = p.To, last.Records+p.Records, last.Bytes+p.Bytes
			continue
		}
		out = append(out, slice{From: p.From, To: p.To, Records: p.Records, Bytes: p.Bytes, Whole: true})
	}
	return out
}

// halves splits s after a whole number of hours, the most up to half its length, and its
// bytes in proportion: false when that is none, as for a slice under two hours.
func halves(s slice) (slice, slice, bool) {
	length := s.To.Sub(s.From)
	half := (length / 2).Truncate(time.Hour) //nolint:mnd // a half
	if half == 0 {
		return slice{}, slice{}, false
	}
	mid, first := s.From.Add(half), s.Bytes*int64(half/time.Hour)/int64(length/time.Hour)
	return slice{From: s.From, To: mid, Bytes: first}, slice{From: mid, To: s.To, Bytes: s.Bytes - first}, true
}

// stage reads the region's archive of the job's stream into runs (ADR 0016): the error prefix,
// then the plan of slices, then each slice's export, each started after the watermark passed
// the cut. Each commit holds only while the job's stage is the one the step read, as moved by
// its own commits: a stale or duplicate worker's run is then garbage. Apply reads the fences
// after the last read ended.
func (s *step) stage(ctx context.Context, j meta.Job) (time.Duration, bool, error) {
	a := j.Archive
	switch {
	case !a.Errors.Done:
		return s.stageErrors(ctx, j)
	case a.Plan == "":
		return s.plan(ctx, j)
	}
	plan, err := s.readPlan(ctx, j)
	if err != nil {
		return 0, false, err
	}
	if a.Slice < len(plan) {
		return s.stageSlice(ctx, j, plan)
	}
	// The last read of the archive ended: whatever made it miss events was recorded by now.
	if err = s.checkArchive(ctx, j); err != nil {
		return 0, false, err
	}
	return 0, false, s.commitArchive(ctx, &j, nil, a, true)
}

// commitArchive writes buf as a run and commits it with the stage to, in one write that holds
// only while the job's phase and stage are j's. With last it moves the job to apply.
func (s *step) commitArchive(ctx context.Context, j *meta.Job, buf []record, to meta.ArchiveStage, last bool) error {
	from := j.Archive
	if len(buf) == 0 && !last && to == from {
		return nil
	}
	run, wrote, err := s.sortedRun(ctx, j, buf)
	if err != nil {
		return err
	}
	*j, err = s.update(ctx, j.Phase, func(j *meta.Job) (bool, error) {
		if j.Archive != from {
			return false, errMoved
		}
		if wrote {
			j.Runs = append(j.Runs, run)
			j.Staged += run.Records
		}
		j.Archive = to
		if last {
			j.Phase = meta.PhaseApply
		}
		return true, nil
	})
	return err
}

// plan lists the table's partitions, with a query started after the watermark passed, and
// commits the slices that export them as a plan object (ADR 0016 §4). A failed listing takes
// a new try, whose token the count of failed tries changes.
func (s *step) plan(ctx context.Context, j meta.Job) (time.Duration, bool, error) {
	a := j.Archive
	if a.PlanQuery == "" {
		id, wait, err := s.start(ctx, Query{Kind: QueryPartitions, Stream: j.Stream}, token(j, "partitions", strconv.Itoa(a.Tries)))
		if err != nil || wait > 0 {
			return wait, false, err
		}
		to := a
		to.PlanQuery = id
		return 0, false, s.commitArchive(ctx, &j, nil, to, false)
	}
	st, err := s.env.Archive.Status(ctx, a.PlanQuery)
	if err != nil {
		return 0, false, fmt.Errorf("backfill: the status of query %s: %w", a.PlanQuery, err)
	}
	to := a
	switch st.State {
	case QueryRunning:
		return exportPoll, false, nil
	case QuerySucceeded:
		parts, perr := s.env.Archive.Partitions(ctx, a.PlanQuery)
		if perr != nil {
			return 0, false, fmt.Errorf("backfill: the partitions of %s: %w", j.Stream, perr)
		}
		if to.Plan, err = s.putPlan(ctx, j, planSlices(parts)); err != nil {
			return 0, false, err
		}
		to.Scanned += st.Scanned
		return 0, false, s.commitArchive(ctx, &j, nil, to, false)
	case QueryFailed, QueryCancelled:
	}
	to.PlanQuery, to.Tries = "", a.Tries+1
	if err = s.commitArchive(ctx, &j, nil, to, false); err != nil {
		return 0, false, err
	}
	wait, err := failure(st, "the listing of the partitions of "+j.Stream)
	return wait, false, err
}

// stageSlice exports the slice being staged, a try at a time, and stages the files of the try
// that succeeded (ADR 0016 §6).
func (s *step) stageSlice(ctx context.Context, j meta.Job, plan []slice) (time.Duration, bool, error) {
	a := j.Archive
	sl := plan[a.Slice]
	if a.Query != "" {
		return 0, false, s.readSlice(ctx, j, sl)
	}
	try := a.Export
	if try.Try == 0 {
		try = meta.Export{From: sl.From, To: sl.To, Try: 1}
	}
	if try.ID == "" {
		return s.startTry(ctx, j, try)
	}
	st, err := s.env.Archive.Status(ctx, try.ID)
	if err != nil {
		return 0, false, fmt.Errorf("backfill: the status of query %s: %w", try.ID, err)
	}
	switch st.State {
	case QueryRunning:
		return exportPoll, false, nil
	case QuerySucceeded:
		return 0, false, s.exported(ctx, j, sl, st)
	case QueryFailed, QueryCancelled:
	}
	return s.failedTry(ctx, j, plan, st)
}

// startTry starts a try of the slice being staged, into a prefix of its own, with a token that
// names the plan, the slice and the try: a restage commits a new plan, so no try meets an
// older one's query or files. Its ID is committed before the job waits for it.
func (s *step) startTry(ctx context.Context, j meta.Job, try meta.Export) (time.Duration, bool, error) {
	a := j.Archive
	if try.Try > 1 {
		// Cleanup only: a try reads only what its own query's manifest lists.
		if err := s.env.Runs.DeletePrefix(ctx, dest(j, try.From, try.To, try.Try-1)); err != nil {
			return 0, false, fmt.Errorf("backfill: delete a failed try's files: %w", err)
		}
	}
	q := Query{Kind: QueryExport, Stream: j.Stream, From: try.From, To: try.To, Dest: dest(j, try.From, try.To, try.Try)}
	id, wait, err := s.start(ctx, q, token(j, a.Plan, micros(try.From), micros(try.To), strconv.Itoa(try.Try)))
	if err != nil || wait > 0 {
		return wait, false, err
	}
	to := a
	try.ID = id
	to.Export = try
	return 0, false, s.commitArchive(ctx, &j, nil, to, false)
}

// exported takes the slice's try that succeeded, whose files are staged next. The first
// slice's scan, when it is made of partitions, must show that Athena pruned partitions by the
// slice's range: a half holds a share of its partitions' bytes, and may scan them whole.
func (s *step) exported(ctx context.Context, j meta.Job, sl slice, st QueryStatus) error {
	a := j.Archive
	if a.Slice == 0 && sl.Whole && st.Scanned > 2*sl.Bytes+pruneSlack {
		return fmt.Errorf("backfill: the export of %s from %s to %s scanned %d bytes for partitions of %d: Athena didn't prune "+
			"partitions by event_time, so each slice would read the whole table", j.Stream, rfc3339(sl.From), rfc3339(sl.To), st.Scanned, sl.Bytes)
	}
	to := a
	to.Query, to.File, to.Skip, to.Rows, to.Scanned = a.Export.ID, 0, 0, 0, a.Scanned+st.Scanned
	return s.commitArchive(ctx, &j, nil, to, false)
}

// failedTry takes the slice's try that failed or was cancelled: a timeout halves the slice;
// a cancellation, a retryable or a permission failure takes a new try that doesn't count;
// another failure takes one that does, while the slice has tries left.
func (s *step) failedTry(ctx context.Context, j meta.Job, plan []slice, st QueryStatus) (time.Duration, bool, error) {
	a := j.Archive
	sl := plan[a.Slice]
	what := fmt.Sprintf("the export of %s from %s to %s", j.Stream, rfc3339(sl.From), rfc3339(sl.To))
	if st.State == QueryFailed && st.Failure == FailureTimeout {
		return 0, false, s.halve(ctx, j, plan, what, st.Reason)
	}
	counted := st.State == QueryFailed && st.Failure == FailureOther
	if counted && a.Export.Failed+1 >= sliceTries {
		return 0, false, fmt.Errorf("backfill: %s failed %d tries: %s", what, sliceTries, st.Reason)
	}
	if err := s.commitArchive(ctx, &j, nil, nextTry(a, counted), false); err != nil {
		return 0, false, err
	}
	if counted {
		s.env.Log.WarnContext(ctx, "backfill: "+what+" failed; trying again", slog.String("version", s.version), slog.String("reason", st.Reason))
		return 0, false, nil
	}
	wait, err := failure(st, what)
	return wait, false, err
}

// failure returns what a step does once a query that failed or was cancelled has a new try:
// after a cancellation it goes on, after a retryable failure it waits, and after any other
// it fails.
func failure(st QueryStatus, what string) (time.Duration, error) {
	switch {
	case st.State == QueryCancelled:
		return 0, nil
	case st.Failure == FailureRetryable:
		return throttleWait, nil
	case st.Failure == FailurePermission:
		return 0, fmt.Errorf("backfill: %s needs a grant that RoleBackfill lacks (infra.md §9): %s", what, st.Reason)
	}
	return 0, fmt.Errorf("backfill: %s failed: %s", what, st.Reason)
}

// nextTry returns a with a new try of the slice being staged, counted against its limit or
// not.
func nextTry(a meta.ArchiveStage, counted bool) meta.ArchiveStage {
	x := a.Export
	x.Try, x.ID = x.Try+1, ""
	if counted {
		x.Failed++
	}
	a.Export, a.Query, a.File, a.Skip, a.Rows, a.Tries = x, "", 0, 0, 0, a.Tries+1
	return a
}

// halve replaces the slice being staged with its halves, in a new plan.
func (s *step) halve(ctx context.Context, j meta.Job, plan []slice, what, reason string) error {
	a := j.Archive
	first, second, ok := halves(plan[a.Slice])
	if !ok {
		return fmt.Errorf("backfill: %s timed out, and it is too short to halve: %s", what, reason)
	}
	name, err := s.putPlan(ctx, j, slices.Concat(plan[:a.Slice], []slice{first, second}, plan[a.Slice+1:]))
	if err != nil {
		return err
	}
	to := a
	to.Plan, to.Export, to.Tries = name, meta.Export{}, a.Tries+1
	return s.commitArchive(ctx, &j, nil, to, false)
}

// readSlice stages the files of the slice's succeeded try, from the file and row the job
// holds, but for an older table's records; a read that fails commits what was read before it.
func (s *step) readSlice(ctx context.Context, j meta.Job, sl slice) error {
	a := j.Archive
	files, err := s.env.Archive.Files(ctx, a.Query)
	if err != nil {
		return fmt.Errorf("backfill: the files of query %s: %w", a.Query, err)
	}
	dec, err := s.decoder(j.Stream)
	if err != nil {
		return err
	}
	born, err := s.bornAt(ctx, j.Stream)
	if err != nil {
		return err
	}
	to := a
	var buf []record
	size := 0
	for to.File < len(files) && s.left() {
		end, rerr := s.readFile(ctx, files[to.File], to.Skip, dec, func(r record) (bool, error) {
			if !r.older(born) {
				buf, size = append(buf, r), size+r.size
			}
			to.Skip++
			to.Rows++
			if size >= s.budget.buffer {
				if cerr := s.commitArchive(ctx, &j, buf, to, false); cerr != nil {
					return false, cerr
				}
				buf, size = nil, 0
			}
			return s.left(), nil
		})
		if rerr != nil {
			return errors.Join(rerr, s.commitArchive(ctx, &j, buf, to, false))
		}
		if !end {
			break
		}
		to.File, to.Skip = to.File+1, 0
	}
	if to.File < len(files) {
		return s.commitArchive(ctx, &j, buf, to, false)
	}
	return s.endSlice(ctx, j, sl, to, buf)
}

// endSlice commits buf, the last records of a slice whose files are all read, at stage to. A
// whole slice must have yielded at least the records its partitions held when they were
// listed, since rows are only ever added; otherwise it takes a new try.
func (s *step) endSlice(ctx context.Context, j meta.Job, sl slice, to meta.ArchiveStage, buf []record) error {
	if sl.Whole && to.Rows < sl.Records {
		what := fmt.Sprintf("the export of %s from %s to %s yielded %d rows, fewer than the %d its partitions held", j.Stream,
			rfc3339(sl.From), rfc3339(sl.To), to.Rows, sl.Records)
		if to.Export.Failed+1 >= sliceTries {
			return fmt.Errorf("backfill: %s, in %d tries", what, sliceTries)
		}
		s.env.Log.WarnContext(ctx, "backfill: "+what+"; trying again", slog.String("version", s.version))
		// What it read is right, and the merge drops the records the next try reads again.
		return s.commitArchive(ctx, &j, buf, nextTry(to, true), false)
	}
	to.Slice, to.Export, to.Query, to.File, to.Skip, to.Rows = to.Slice+1, meta.Export{}, "", 0, 0, 0
	return s.commitArchive(ctx, &j, buf, to, false)
}

// maxLine bounds a line of an export's file or of an error object: an archive record, with
// room for JSON escapes and base64.
const maxLine = 4 * archive.MaxRecordSize

// readFile reads an export's file from its line skip, until add returns false, and reports
// whether it read the file to its end.
func (s *step) readFile(ctx context.Context, file string, skip int, dec *archive.Decoder, add func(record) (bool, error)) (bool, error) {
	return s.readLines(ctx, file, func(n int, line []byte) (bool, error) {
		if n < skip {
			return true, nil
		}
		r, err := dec.Decode(line)
		if err != nil {
			return false, fmt.Errorf("backfill: %s: %w", file, err)
		}
		return add(archived(r, len(line)))
	})
}

// archived returns a decoded archive record, of size bytes, as a staged record.
func archived(r archive.Record, size int) record {
	return record{mark: keys.Mark{UnitKey: keys.UnitKey(r.Unit), Seq: r.Seq}, unit: r.Unit, seq: r.Seq, item: r.Item, size: size, arrival: r.Arrival}
}

// bornAt returns when the stream table was created, by its replicas' earliest creation, to the
// second that ApproximateCreationDateTime keeps: the region's stream carries nothing older,
// and the archive's records that arrived before are an older table's.
func (s *step) bornAt(ctx context.Context, stream string) (time.Time, error) {
	if s.born.IsZero() {
		table := s.env.Naming.StreamTable(stream)
		born, _, err := s.env.Tables.Born(ctx, table)
		if err != nil {
			return time.Time{}, fmt.Errorf("backfill: describe %s: %w", table, err)
		}
		s.born = born.Truncate(time.Second)
	}
	return s.born, nil
}

// gzipMagic starts a gzip stream.
var gzipMagic = []byte{0x1f, 0x8b} //nolint:gochecknoglobals // read-only, as a constant

// readLines reads name's lines, gunzipped when they start as gzip does, until read returns
// false, and reports whether it read them all. n counts every line, empty ones too.
func (s *step) readLines(ctx context.Context, name string, read func(n int, line []byte) (bool, error)) (bool, error) {
	rc, err := s.env.Archive.Open(ctx, name)
	if err != nil {
		return false, fmt.Errorf("backfill: open %s: %w", name, err)
	}
	all, err := scanLines(name, rc, read)
	if cerr := rc.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("backfill: close %s: %w", name, cerr)
	}
	return all && err == nil, err
}

// scanLines gives read the lines of r, the object name, until it returns false, and reports
// whether it read them all.
func scanLines(name string, r io.Reader, read func(n int, line []byte) (bool, error)) (bool, error) {
	br := bufio.NewReader(r)
	var in io.Reader = br
	// A shorter object fails Peek, and isn't gzip; a read that fails fails the scan.
	if magic, err := br.Peek(len(gzipMagic)); err == nil && bytes.Equal(magic, gzipMagic) {
		zr, zerr := gzip.NewReader(br)
		if zerr != nil {
			return false, fmt.Errorf("backfill: read %s: %w", name, zerr)
		}
		in = zr
	}
	sc := bufio.NewScanner(in)
	sc.Buffer(nil, maxLine)
	for n := 0; sc.Scan(); n++ {
		if more, err := read(n, sc.Bytes()); err != nil || !more {
			return false, err
		}
	}
	if err := sc.Err(); err != nil {
		return false, fmt.Errorf("backfill: read %s: %w", name, err)
	}
	return true, nil
}

// decoder returns the decoder of stream's records, by this build's catalog.
func (s *step) decoder(stream string) (*archive.Decoder, error) {
	def, ok := s.env.Catalog.Stream(stream)
	if !ok {
		return nil, fmt.Errorf("backfill: no stream %s in this build's catalog", stream)
	}
	return archive.NewDecoder(def), nil
}

// start starts q with token, or returns how long to wait when Athena throttled it.
func (s *step) start(ctx context.Context, q Query, token string) (string, time.Duration, error) {
	id, err := s.env.Archive.Start(ctx, q, token)
	switch {
	case errors.Is(err, ErrThrottled):
		return "", throttleWait, nil
	case err != nil:
		return "", 0, fmt.Errorf("backfill: start a query of %s: %w", q.Stream, err)
	}
	return id, 0, nil
}

// token returns a query's ClientRequestToken: the same for every step of the job that starts
// that query, and for no other.
func token(j meta.Job, parts ...string) string {
	h := sha256.New()
	for _, p := range append([]string{j.Version, micros(j.CreatedAt)}, parts...) {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// dest returns the destination of a slice's try: <staging>unload/<plan>/<from>-<to>/<try>/.
func dest(j meta.Job, from, to time.Time, try int) string {
	return fmt.Sprintf("%sunload/%s/%s-%s/%d/", j.Staging, j.Archive.Plan, micros(from), micros(to), try)
}

func micros(t time.Time) string { return strconv.FormatInt(t.UnixMicro(), 10) }

// putPlan stores plan as a new object of the job's staging, and returns its name.
func (s *step) putPlan(ctx context.Context, j meta.Job, plan []slice) (string, error) {
	out := make([]sliceJSON, len(plan))
	for i, sl := range plan {
		out[i] = sliceJSON{From: sl.From.UnixMicro(), To: sl.To.UnixMicro(), Records: sl.Records, Bytes: sl.Bytes, Whole: sl.Whole}
	}
	data, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("backfill: encode a plan: %w", err)
	}
	nonce, err := s.nonce()
	if err != nil {
		return "", err
	}
	name := fmt.Sprintf("plan-%d-%s", j.Attempt, nonce)
	err = s.env.Runs.Put(ctx, j.Staging+name, func(w io.Writer) error {
		_, werr := w.Write(data)
		return werr
	})
	if err != nil {
		return "", fmt.Errorf("backfill: store plan %s: %w", name, err)
	}
	return name, nil
}

// readPlan reads the job's plan, once per step.
func (s *step) readPlan(ctx context.Context, j meta.Job) ([]slice, error) {
	if s.planName == j.Archive.Plan {
		return s.slices, nil
	}
	data, err := s.env.Runs.ReadAt(ctx, j.Staging+j.Archive.Plan, 0, maxPlan+1)
	if err != nil {
		return nil, fmt.Errorf("backfill: plan %s: %w", j.Archive.Plan, err)
	}
	var in []sliceJSON
	if len(data) > maxPlan {
		err = errors.New("over its size limit")
	} else {
		err = json.Unmarshal(data, &in)
	}
	if err != nil {
		return nil, fmt.Errorf("backfill: plan %s: %w", j.Archive.Plan, err)
	}
	plan := make([]slice, len(in))
	for i, sl := range in {
		plan[i] = slice{From: time.UnixMicro(sl.From).UTC(), To: time.UnixMicro(sl.To).UTC(), Records: sl.Records, Bytes: sl.Bytes, Whole: sl.Whole}
	}
	s.planName, s.slices = j.Archive.Plan, plan
	return plan, nil
}

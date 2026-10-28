// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	// The archive's records; archive is the model here.
	records "github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/projector"
)

// archive models Firehose delivering into Iceberg, and Athena reading it: an acknowledged put
// arrives after a delay, in any order, sometimes twice; some records land in the error prefix
// instead, which a backfill must read too, and the records of some puts fail to commit, which
// only an error object naming their data files shows. A query reads what had arrived when it
// started, and a try that fails leaves some of its files. Only gated code calls its methods.
type archive struct {
	cfg     Archive
	entries []archived
	objects map[string]*errorObject // the error prefix, by name
	queries []*query
	tokens  map[string]int    // a query's token → its index in queries
	files   map[string]fileOf // an export's files, by name
	// skew is how far CloudWatch's clock is off; samples are taken phase into each Freshness.
	skew, phase time.Duration
}

type archived struct {
	stream, unit string
	rec          projector.Record
	at           time.Time // the record's event_time
	put, due     time.Time
	errorPrefix  bool
	failed       bool // its commit failed: no table holds it
}

// errorObject is an object of a stream's error prefix: records Firehose couldn't deliver, or
// the data files of a commit that failed, and maybe a line Dynaflux didn't write. It lands at
// due.
type errorObject struct {
	stream  string
	due     time.Time
	entries []int
	failed  bool
	foreign bool
	gzipped bool
}

// query is a query of the archive's table that a backfill started: it ends at done with
// status, and holds what had arrived when it started.
type query struct {
	q      backfill.Query
	done   time.Time
	status backfill.QueryStatus
	parts  []backfill.Partition
	rows   []int // an export's entries, in table order
	files  int   // the files it wrote
}

// fileOf is the k-th file of an export, which holds archiveFile of its rows.
type fileOf struct {
	q *query
	k int
}

// archiveFile is how many rows a file of an export holds.
const archiveFile = 50

// A query runs between queryLeast and queryMost; an export of a slice at least timeoutAt
// long may time out. Only a slice's first try fails otherwise, so none runs out of tries.
const (
	queryLeast = 5 * time.Second
	queryMost  = 3 * time.Minute
	timeoutAt  = 2 * time.Hour
)

func newArchive(cfg Archive, rng *rand.Rand) *archive {
	a := &archive{cfg: cfg, objects: map[string]*errorObject{}, tokens: map[string]int{}, files: map[string]fileOf{}}
	if cfg.Freshness > 0 {
		a.skew, a.phase = between(rng, -cfg.Skew, cfg.Skew), between(rng, 0, cfg.Freshness-1)
	}
	return a
}

// put takes the records a projector archives, and reports whether their commit failed.
func (a *archive) put(rng *rand.Rand, stream, unit string, recs []projector.Record) bool {
	now := time.Now()
	failed := chance(rng, a.cfg.Commit)
	var commit *errorObject
	if failed {
		commit = a.newObject(stream, now.Add(between(rng, a.cfg.MinDelay, a.cfg.MaxDelay)), true, false)
	}
	for _, rec := range recs {
		e := archived{stream: stream, unit: unit, rec: cloneRecord(rec), at: eventTime(rec), put: now, failed: failed}
		switch {
		case failed:
			e.due = commit.due
			commit.entries = append(commit.entries, len(a.entries))
			a.entries = append(a.entries, e)
			continue
		case chance(rng, a.cfg.Error):
			e.errorPrefix, e.due = true, now.Add(between(rng, a.cfg.MinDelay, a.cfg.MaxDelay))
			o := a.newObject(stream, e.due, false, chance(rng, a.cfg.Foreign))
			o.entries = append(o.entries, len(a.entries))
		default:
			e.due = now.Add(between(rng, a.cfg.MinDelay, a.cfg.MaxDelay))
		}
		a.entries = append(a.entries, e)
		if chance(rng, a.cfg.Dup) {
			e.due = now.Add(between(rng, a.cfg.MinDelay, a.cfg.MaxDelay))
			if e.errorPrefix {
				a.newObject(stream, e.due, false, false).entries = []int{len(a.entries)}
			}
			a.entries = append(a.entries, e)
		}
	}
	return failed
}

// eventTime returns a stream record's event_time, from its sort key; the zero time for a
// record without one, as the model's own tests put.
func eventTime(rec projector.Record) time.Time {
	at, _, err := keys.ParseStreamSK(rec.New[catalog.AttrSK].Str)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMicro(at).UTC()
}

// newObject adds an object to stream's error prefix that lands at due, named so that names
// sort by landing.
func (a *archive) newObject(stream string, due time.Time, failed, foreign bool) *errorObject {
	name := fmt.Sprintf("errors/firehose/%s/%020d-%06d", stream, due.UnixMicro(), len(a.objects))
	o := &errorObject{stream: stream, due: due, failed: failed, foreign: foreign, gzipped: len(a.objects)%2 == 1}
	a.objects[name] = o
	return o
}

// arrived returns what a reader finds at now, delivered or in the error prefix.
func (a *archive) arrived(now time.Time) []archived {
	var out []archived
	for _, e := range a.entries {
		if !e.due.After(now) && !e.failed {
			out = append(out, e)
		}
	}
	return out
}

// completeThrough is the exact watermark at now: whether everything put by t has left
// Firehose. A lying archive always says so.
func (a *archive) completeThrough(t, now time.Time) bool {
	if a.cfg.Lie {
		return true
	}
	for _, e := range a.entries {
		if !e.put.After(t) && e.due.After(now) {
			return false
		}
	}
	return true
}

// delivered is the watermark as CloudWatch shows it at now, through backfill.Delivered:
// Firehose samples the age of stream's oldest record every Freshness while it holds one, and
// CloudWatch stamps the samples' periods by its own clock. Only the last half hour is
// sampled: a job looks every minute, and fewer datapoints never pass a watermark sooner.
func (a *archive) delivered(stream string, cut, now time.Time) bool {
	if a.cfg.Lie {
		return true
	}
	points := map[time.Time]time.Duration{}
	from := now.Add(-30 * time.Minute)
	for at := from.Truncate(a.cfg.Freshness).Add(a.phase); !at.After(now); at = at.Add(a.cfg.Freshness) {
		oldest, held := time.Time{}, false
		for _, e := range a.entries {
			if e.stream == stream && !e.put.After(at) && e.due.After(at) && (!held || e.put.Before(oldest)) {
				oldest, held = e.put, true
			}
		}
		if held {
			period := at.Add(a.skew).Truncate(time.Minute)
			points[period] = max(points[period], at.Sub(oldest))
		}
	}
	f := backfill.Freshness{Active: true, Listed: true}
	for _, at := range slices.SortedFunc(maps.Keys(points), time.Time.Compare) {
		f.Points = append(f.Points, backfill.Datapoint{At: at, Freshness: points[at]})
	}
	return backfill.Delivered(f, cut, now)
}

// start starts q at now, or returns the query that token started.
func (a *archive) start(r *run, q backfill.Query, token string, now time.Time) string {
	if i, ok := a.tokens[token]; ok {
		return queryName(i)
	}
	qu := &query{q: q, done: now.Add(between(r.arch, queryLeast, queryMost)), status: backfill.QueryStatus{State: backfill.QuerySucceeded}}
	a.tokens[token] = len(a.queries)
	a.queries = append(a.queries, qu)
	if q.Kind == backfill.QueryPartitions {
		qu.parts = a.partitions(q.Stream, now)
		return queryName(len(a.queries) - 1)
	}
	for i, e := range a.table(q.Stream, now) {
		if !e.at.Before(q.From) && e.at.Before(q.To) {
			qu.rows = append(qu.rows, i)
		}
	}
	qu.files = (len(qu.rows) + archiveFile - 1) / archiveFile
	switch {
	case q.To.Sub(q.From) >= timeoutAt && chance(r.arch, a.cfg.Timeout):
		r.count(&r.stats.Halved)
		qu.status = backfill.QueryStatus{State: backfill.QueryFailed, Failure: backfill.FailureTimeout, Reason: "Query timeout"}
		qu.files = r.arch.IntN(qu.files + 1)
	case strings.HasSuffix(q.Dest, "/1/") && chance(r.arch, a.cfg.Fail):
		r.count(&r.stats.FailedTries)
		qu.status = backfill.QueryStatus{State: backfill.QueryFailed, Reason: "GENERIC_INTERNAL_ERROR"}
		qu.files = r.arch.IntN(qu.files + 1)
	}
	qu.status.Scanned = int64(len(qu.rows))
	for k := range qu.files {
		a.files[fmt.Sprintf("%spart-%04d.gz", q.Dest, k)] = fileOf{qu, k}
	}
	r.count(&r.stats.Exports)
	return queryName(len(a.queries) - 1)
}

func queryName(i int) string { return "q" + strconv.Itoa(i) }

// table yields the indexes in entries of stream's records in the table at now.
func (a *archive) table(stream string, now time.Time) iter.Seq2[int, archived] {
	return func(yield func(int, archived) bool) {
		for i, e := range a.entries {
			if e.stream == stream && !e.errorPrefix && !e.failed && !e.due.After(now) && !yield(i, e) {
				return
			}
		}
	}
}

// partitions returns the partitions of stream's table at now, by the minute of event_time:
// finer than Iceberg's days, so that a plan taken early misses some.
func (a *archive) partitions(stream string, now time.Time) []backfill.Partition {
	byMinute := map[time.Time]*backfill.Partition{}
	for _, e := range a.table(stream, now) {
		from := e.at.Truncate(time.Minute)
		p := byMinute[from]
		if p == nil {
			p = &backfill.Partition{From: from, To: from.Add(time.Minute)}
			byMinute[from] = p
		}
		p.Records++
		p.Bytes += max(1, a.cfg.RecordBytes)
	}
	var out []backfill.Partition
	for _, from := range slices.SortedFunc(maps.Keys(byMinute), time.Time.Compare) {
		out = append(out, *byMinute[from])
	}
	return out
}

// query returns the query named id.
func (a *archive) query(id string) (*query, error) {
	i, err := strconv.Atoi(strings.TrimPrefix(id, "q"))
	if err != nil || i < 0 || i >= len(a.queries) {
		return nil, fmt.Errorf("harness: no query %s", id)
	}
	return a.queries[i], nil
}

// archiver is a region's archive as a core sees it: behind the gates, with transient
// and ambiguous faults.
type archiver struct{ rg *region }

func (a archiver) Archive(ctx context.Context, stream, unit string, recs []projector.Record) error {
	op := "archive " + stream + " " + unit
	if len(recs) > 0 {
		op += " " + recs[0].Seq
	}
	lat, f := a.rg.enter(ctx, kindArchive, op)
	defer time.Sleep(lat)
	if f == faultTransient {
		return errTransient
	}
	if r := a.rg.run; a.rg.archive.put(r.arch, stream, unit, recs) {
		r.count(&r.stats.CommitFailures)
	}
	if f == faultAmbiguous {
		return errAmbiguous
	}
	return nil
}

// archiveReader is a region's archive as a backfill reads it, behind the gates: the
// watermark, Athena's queries and the files of their exports, and the error prefix.
type archiveReader struct{ rg *region }

func (a archiveReader) Complete(ctx context.Context, stream string, cut time.Time) (bool, error) {
	lat, f := a.rg.enter(ctx, kindRead, "watermark "+stream)
	defer time.Sleep(lat)
	if f == faultTransient {
		return false, errTransient
	}
	if a.rg.archive.cfg.Freshness > 0 {
		return a.rg.archive.delivered(stream, cut, time.Now()), nil
	}
	return a.rg.archive.completeThrough(cut, time.Now()), nil
}

// Start creates its query even when its caller sees a timeout.
func (a archiveReader) Start(ctx context.Context, q backfill.Query, token string) (string, error) {
	lat, f := a.rg.enter(ctx, kindArchive, "start "+q.Stream+" "+token)
	defer time.Sleep(lat)
	if f == faultTransient {
		return "", errTransient
	}
	id := a.rg.archive.start(a.rg.run, q, token, time.Now())
	if f == faultAmbiguous {
		return "", errAmbiguous
	}
	return id, nil
}

func (a archiveReader) Status(ctx context.Context, id string) (backfill.QueryStatus, error) {
	lat, f := a.rg.enter(ctx, kindRead, "status "+id)
	defer time.Sleep(lat)
	if f == faultTransient {
		return backfill.QueryStatus{}, errTransient
	}
	q, err := a.rg.archive.query(id)
	switch {
	case err != nil:
		return backfill.QueryStatus{}, err
	case time.Now().Before(q.done):
		return backfill.QueryStatus{State: backfill.QueryRunning}, nil
	}
	return q.status, nil
}

func (a archiveReader) Stop(ctx context.Context, id string) error {
	lat, f := a.rg.enter(ctx, kindArchive, "stop "+id)
	defer time.Sleep(lat)
	if f == faultTransient {
		return errTransient
	}
	q, err := a.rg.archive.query(id)
	if err == nil && time.Now().Before(q.done) {
		q.done, q.status = time.Now(), backfill.QueryStatus{State: backfill.QueryCancelled}
	}
	return err
}

func (a archiveReader) Partitions(ctx context.Context, id string) ([]backfill.Partition, error) {
	lat, f := a.rg.enter(ctx, kindRead, "partitions "+id)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	q, err := a.rg.archive.query(id)
	if err != nil || q.q.Kind != backfill.QueryPartitions || time.Now().Before(q.done) {
		return nil, fmt.Errorf("harness: query %s listed no partitions: %w", id, err)
	}
	return slices.Clone(q.parts), nil
}

// Files lists the files an export wrote: those of a try that failed too, which only a
// listing of its prefix would show.
func (a archiveReader) Files(ctx context.Context, id string) ([]string, error) {
	lat, f := a.rg.enter(ctx, kindRead, "files "+id)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	q, err := a.rg.archive.query(id)
	if err != nil || q.q.Kind != backfill.QueryExport || time.Now().Before(q.done) {
		return nil, fmt.Errorf("harness: query %s exported nothing: %w", id, err)
	}
	var out []string
	for k := range q.files {
		out = append(out, fmt.Sprintf("%spart-%04d.gz", q.q.Dest, k))
	}
	return out, nil
}

func (a archiveReader) ErrorObjects(ctx context.Context, stream, after string, _ time.Time) ([]string, error) {
	lat, f := a.rg.enter(ctx, kindRead, "list errors "+stream+" "+after)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	now := time.Now()
	a.rg.listed[invocationOf(ctx).id+" "+stream] = now
	var out []string
	for name, o := range a.rg.archive.objects {
		if o.stream == stream && !o.due.After(now) && name > after {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out, nil
}

func (a archiveReader) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	lat, f := a.rg.enter(ctx, kindRead, "open "+name)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	var b bytes.Buffer
	var err error
	if o, ok := a.rg.archive.objects[name]; ok {
		err = a.writeObject(&b, o)
	} else {
		err = a.writeFile(&b, a.rg.archive.files[name])
	}
	return io.NopCloser(&b), err
}

// writeObject writes an error object as Firehose does: a JSON line per record it couldn't
// deliver, with the record as rawData, or a line naming the data files of a commit that
// failed; gzipped every other object.
func (a archiveReader) writeObject(w *bytes.Buffer, o *errorObject) error {
	var lines bytes.Buffer
	if o.failed {
		fmt.Fprintf(&lines, `{"errorCode":"Iceberg.CommitFailed","dataFiles":["s3://archive/%s/data/%d.parquet"]}`+"\n", o.stream, o.due.UnixMicro())
	} else {
		for _, i := range o.entries {
			rec, err := a.rg.run.archiveRecord(&a.rg.archive.entries[i])
			if err != nil {
				return err
			}
			fmt.Fprintf(&lines, `{"attemptsMade":4,"errorCode":"Iceberg.InvalidRecord","rawData":"%s"}`+"\n", base64.StdEncoding.EncodeToString(rec))
		}
	}
	if o.foreign {
		fmt.Fprintf(&lines, `{"errorCode":"Iceberg.InvalidRecord","rawData":"%s"}`+"\n", base64.StdEncoding.EncodeToString([]byte(`{"probe":true}`)))
	}
	if !o.gzipped {
		w.Write(lines.Bytes())
		return nil
	}
	return gzipTo(w, lines.Bytes())
}

// writeFile writes an export's file: gzip JSON, a line per row.
func (a archiveReader) writeFile(w *bytes.Buffer, f fileOf) error {
	if f.q == nil {
		return errors.New("harness: no such file")
	}
	var lines bytes.Buffer
	for _, i := range f.q.rows[f.k*archiveFile : min(len(f.q.rows), (f.k+1)*archiveFile)] {
		rec, err := a.rg.run.archiveRecord(&a.rg.archive.entries[i])
		if err != nil {
			return err
		}
		lines.Write(rec)
		lines.WriteByte('\n')
	}
	return gzipTo(w, lines.Bytes())
}

func gzipTo(w io.Writer, data []byte) error {
	zw := gzip.NewWriter(w)
	if _, err := zw.Write(data); err != nil {
		return err
	}
	return zw.Close()
}

// archiveRecord returns an archived entry's record as the projector's Firehose writer sends it.
func (r *run) archiveRecord(e *archived) ([]byte, error) {
	s, _ := r.cfg.Catalog.Projector().Stream(e.stream)
	vals, bad := s.Decode(e.rec.New)
	if err := errors.Join(bad...); err != nil {
		return nil, fmt.Errorf("harness: archived record %s: %w", e.rec.Seq, err)
	}
	seq, err := keys.PadSeq(e.rec.Seq)
	if err != nil {
		return nil, err
	}
	return records.NewEncoder(s.Def).AppendRecord(nil, vals, e.unit, seq, time.Time{}), nil
}

// gatedRuns is a region's staging as a backfill sees it: behind the gates, with transient
// faults, and writes that apply though their caller sees a timeout.
type gatedRuns struct{ rg *region }

func (g gatedRuns) Put(ctx context.Context, name string, write func(io.Writer) error) error {
	lat, f := g.rg.enter(ctx, kindArchive, "runs put "+name)
	defer time.Sleep(lat)
	if f == faultTransient {
		return errTransient
	}
	if err := g.rg.runs.Put(ctx, name, write); err != nil {
		return err
	}
	if f == faultAmbiguous {
		return errAmbiguous
	}
	return nil
}

func (g gatedRuns) ReadAt(ctx context.Context, name string, off, n int64) ([]byte, error) {
	lat, f := g.rg.enter(ctx, kindRead, fmt.Sprintf("runs read %s %d %d", name, off, n))
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	return g.rg.runs.ReadAt(ctx, name, off, n)
}

func (g gatedRuns) DeletePrefix(ctx context.Context, prefix string) error {
	lat, f := g.rg.enter(ctx, kindArchive, "runs delete "+prefix)
	defer time.Sleep(lat)
	if f == faultTransient {
		return errTransient
	}
	err := g.rg.runs.DeletePrefix(ctx, prefix)
	if err == nil && f == faultAmbiguous {
		err = errAmbiguous
	}
	return err
}

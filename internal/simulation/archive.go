// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	// The archive's records; archive is the model here.
	records "github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
)

// archive models Firehose delivering into Iceberg: an acknowledged put arrives after a
// delay, in any order, sometimes twice, and some records land in the error prefix, which
// a backfill must read too. Only gated code calls its methods.
type archive struct {
	cfg     Archive
	entries []archived
}

type archived struct {
	stream, unit string
	rec          projector.Record
	put, due     time.Time
	errorPrefix  bool
}

func (a *archive) put(rng *rand.Rand, stream, unit string, recs []projector.Record) {
	now := time.Now()
	for _, rec := range recs {
		e := archived{
			stream:      stream,
			unit:        unit,
			rec:         cloneRecord(rec),
			put:         now,
			due:         now.Add(between(rng, a.cfg.MinDelay, a.cfg.MaxDelay)),
			errorPrefix: chance(rng, a.cfg.Error),
		}
		a.entries = append(a.entries, e)
		if chance(rng, a.cfg.Dup) {
			e.due = now.Add(between(rng, a.cfg.MinDelay, a.cfg.MaxDelay))
			a.entries = append(a.entries, e)
		}
	}
}

// arrived returns what a reader finds at now, delivered or in the error prefix.
func (a *archive) arrived(now time.Time) []archived {
	var out []archived
	for _, e := range a.entries {
		if !e.due.After(now) {
			out = append(out, e)
		}
	}
	return out
}

// completeThrough is the freshness watermark at now: whether everything put by t has
// arrived. A lying archive always says so.
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
	switch f {
	case faultTransient:
		return errTransient
	case faultAmbiguous:
		a.rg.archive.put(a.rg.run.arch, stream, unit, recs)
		return errAmbiguous
	case faultNone, faultRetried, faultLate:
	}
	a.rg.archive.put(a.rg.run.arch, stream, unit, recs)
	return nil
}

// streamEntries yields the indexes in entries of the archive entries of stream, in the order
// they were put.
func (a *archive) streamEntries(stream string) iter.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		k := 0
		for i := range a.entries {
			if a.entries[i].stream != stream {
				continue
			}
			if !yield(k, i) {
				return
			}
			k++
		}
	}
}

// archiveFile is how many of a stream's archive entries a file of an export spans.
const archiveFile = 50

// archiveReader is a region's archive as a backfill reads it, behind the gates: the
// watermark, and an export of a stream's entries in files, each those of a span of them that
// arrived delivered or in the error prefix, a line per record as Firehose received it.
type archiveReader struct{ rg *region }

func (a archiveReader) Complete(ctx context.Context, stream string, cut time.Time) (bool, error) {
	lat, f := a.rg.enter(ctx, kindRead, "watermark "+stream)
	defer time.Sleep(lat)
	if f == faultTransient {
		return false, errTransient
	}
	return a.rg.archive.completeThrough(cut, time.Now()), nil
}

func (a archiveReader) Export(ctx context.Context, j *meta.Job) ([]string, bool, error) {
	lat, f := a.rg.enter(ctx, kindRead, "export "+j.Stream)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, false, errTransient
	}
	var files []string
	for k := range a.rg.archive.streamEntries(j.Stream) {
		if k%archiveFile != 0 {
			continue
		}
		for _, prefix := range []string{prefixDelivered, prefixErrors} {
			files = append(files, prefix+"/"+j.Stream+"/"+strconv.Itoa(k))
		}
	}
	return files, true, nil
}

const (
	prefixDelivered = "delivered"
	prefixErrors    = "errors"
)

func (a archiveReader) Open(ctx context.Context, file string) (io.ReadCloser, error) {
	lat, f := a.rg.enter(ctx, kindRead, "open "+file)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	parts := strings.Split(file, "/")
	from, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, err
	}
	now := time.Now()
	var b bytes.Buffer
	for k, i := range a.rg.archive.streamEntries(parts[1]) {
		e := &a.rg.archive.entries[i]
		if k >= from+archiveFile {
			break
		}
		if k < from || e.errorPrefix != (parts[0] == prefixErrors) || e.due.After(now) {
			continue
		}
		rec, rerr := a.rg.run.archiveRecord(e)
		if rerr != nil {
			return nil, rerr
		}
		b.Write(rec)
		b.WriteByte('\n')
	}
	return io.NopCloser(&b), nil
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

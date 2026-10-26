// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"math/rand/v2"
	"time"

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

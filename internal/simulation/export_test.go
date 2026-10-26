// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"math/rand/v2"
	"time"

	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// ArchiveModel exposes a region's archive model to tests.
type ArchiveModel struct {
	a   *archive
	rng *rand.Rand
}

func NewArchiveModel(cfg Archive, seed uint64) *ArchiveModel {
	return &ArchiveModel{a: &archive{cfg: cfg}, rng: newRand(seed, streamArchive)}
}

func (m *ArchiveModel) Put(unit string, recs []projector.Record) { m.a.put(m.rng, "s", unit, recs) }

// Arrived returns the sequence numbers a reader finds at now, and how many of the
// entries are in the error prefix.
func (m *ArchiveModel) Arrived(now time.Time) ([]string, int) {
	var seqs []string
	errs := 0
	for _, e := range m.a.arrived(now) {
		seqs = append(seqs, e.rec.Seq)
		if e.errorPrefix {
			errs++
		}
	}
	return seqs, errs
}

func (m *ArchiveModel) CompleteThrough(t, now time.Time) bool { return m.a.completeThrough(t, now) }

// CheckChangeStream checks P0 on one region whose change stream holds records, one per
// item, against the appends applied.
func CheckChangeStream(applied, records []store.Item) []string {
	r := &run{applied: map[tableKey]bool{}}
	for _, it := range applied {
		r.applied[tableKey{"t", keyOf(it)}] = true
	}
	u := &unit{}
	for _, it := range records {
		u.recs = append(u.recs, projector.Record{Op: projector.Insert, New: it})
	}
	r.checkStream(&region{name: "r0", order: []*stream{{table: "t", units: []*unit{u}}}})
	return r.violations
}

func NewOracle() *Oracle { return newOracle() }

// CheckViews checks P1 and P3 on region r0, whose oracle is o, and P2 between r0 and r1.
func CheckViews(o *Oracle, r0, r1 State) []string {
	r := &run{}
	r.checkRegion("r0", o, r0)
	r.checkConvergence("r0 vs r1", r0, r1)
	return r.violations
}

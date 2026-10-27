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

// CheckChangeStream checks P0 on one region whose change stream holds records, against
// the appends applied, one per region that applied it, and the items some region deleted.
func CheckChangeStream(applied, deleted []store.Item, records []projector.Record) []string {
	r := &run{applied: map[tableKey]int{}, expired: map[tableKey]bool{}}
	for _, it := range applied {
		r.applied[tableKey{"t", keyOf(it)}]++
	}
	for _, it := range deleted {
		r.expired[tableKey{"t", keyOf(it)}] = true
	}
	r.checkStream(&region{name: "r0", order: []*stream{{table: "t", units: []*unit{{recs: records}}}}})
	return r.violations
}

func NewOracle() *Oracle { return newOracle() }

// CheckViews checks P1, P3 and P10 on region r0, whose oracle is o and whose views timeSplit
// names are split by time, and P2 between r0 and r1, but for the rows in skip, which a
// divergent duplicate reached.
func CheckViews(o *Oracle, r0, r1 State, skip map[RowID]bool, timeSplit map[string]bool) []string {
	r := &run{}
	r.checkRegion("r0", o, r0, timeSplit, skip)
	r.checkConvergence("r0", "r1", r0, r1, skip)
	return r.violations
}

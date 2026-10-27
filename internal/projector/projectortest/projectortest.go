// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package projectortest selects a backfill's archive records the simple way, as the oracle
// of tests of the backfill.
package projectortest

import (
	"fmt"
	"slices"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/projector"
)

// SelectBackfill returns the archive records that a backfill of a version applies: recs
// without repeats of a unit and sequence number and, of a unit with a fence in fences, only
// those at or below it. A unit without one keeps every record. The result is in the order
// the backfill applies it: by unit key, then sequence number.
func SelectBackfill(recs []projector.BackfillRecord, fences map[string]string) ([]projector.BackfillRecord, error) {
	type kept struct {
		rec  projector.BackfillRecord
		mark keys.Mark
	}
	var out []kept
	for _, r := range recs {
		seq, err := keys.PadSeq(r.Seq)
		if err != nil {
			return nil, fmt.Errorf("record %s of unit %s: %w", r.Seq, r.Unit, err)
		}
		if f, ok := fences[r.Unit]; !ok || seq <= f {
			out = append(out, kept{r, keys.Mark{UnitKey: keys.UnitKey(r.Unit), Seq: seq}})
		}
	}
	slices.SortStableFunc(out, func(a, b kept) int { return a.mark.Compare(b.mark) })
	out = slices.CompactFunc(out, func(a, b kept) bool { return a.rec.Unit == b.rec.Unit && a.mark.Seq == b.mark.Seq })
	sel := make([]projector.BackfillRecord, len(out))
	for i, k := range out {
		sel[i] = k.rec
	}
	return sel, nil
}

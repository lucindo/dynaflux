// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// ErrBroken is the error of a backfill chunk with a record whose evaluation fails: the
// version's breaker is open, and a retry would fail again.
var ErrBroken = errors.New("projector: the backfill opened the version's breaker")

// BackfillRecord is an archived INSERT record.
type BackfillRecord struct {
	Unit string     // the delivery unit's ID
	Seq  string     // the sequence number as delivered
	Item store.Item // the record's new image
}

// SelectBackfill returns the records that a backfill of a version applies: recs without
// repeats of a unit and sequence number and, of a unit with a fence for the version, only
// those at or below it. fences maps a unit ID to its fence, read from the unit's item after
// recs were read from the archive: a batch that applies the version fences its unit before
// it archives, so a record read that a batch applied has its unit's fence by then. A unit
// without one keeps every record: a unit older than the stream's retention, which nobody
// fenced, or any unit when the version doesn't need the fenced handoff. The result is in the
// order the backfill applies it: by unit key, then sequence number.
func SelectBackfill(recs []BackfillRecord, fences map[string]string) ([]BackfillRecord, error) {
	out := make([]record, 0, len(recs))
	for _, b := range recs {
		r, err := b.record()
		if err != nil {
			return nil, err
		}
		if f, ok := fences[b.Unit]; !ok || r.seq <= f {
			out = append(out, r)
		}
	}
	slices.SortStableFunc(out, func(a, b record) int { return a.mark().Compare(b.mark()) })
	out = slices.CompactFunc(out, func(a, b record) bool { return a.unit == b.unit && a.seq == b.seq })
	sel := make([]BackfillRecord, len(out))
	for i, r := range out {
		sel[i] = BackfillRecord{Unit: r.unit, Seq: r.Seq, Item: r.New}
	}
	return sel, nil
}

func (b BackfillRecord) record() (record, error) {
	return newRecord(Record{Op: Insert, Seq: b.Seq, New: b.Item}, b.Unit)
}

// ApplyBackfill applies a chunk of SelectBackfill's result to version, in the backfill's one
// namespace B: a row holds the records after the last one it holds, by unit key and sequence
// number. The caller applies the chunks in that order, each once the one before succeeded.
// ctx must have a deadline; nothing is sent after it. A record whose evaluation fails opens
// the version's breaker and fails the chunk with ErrBroken; any other error leaves the chunk
// to be applied again.
func (c *Core) ApplyBackfill(ctx context.Context, version string, chunk []BackfillRecord) error {
	v, ok := c.c.views[version]
	if !ok {
		return fmt.Errorf("projector: no view version %s in the catalog", version)
	}
	if _, ok = ctx.Deadline(); !ok {
		return errors.New("projector: a backfill chunk needs a context with a deadline")
	}
	recs := make([]record, len(chunk))
	for i, b := range chunk {
		var err error
		if recs[i], err = b.record(); err != nil {
			return fmt.Errorf("projector: backfill of %s: %w", version, err)
		}
		if i > 0 && recs[i-1].mark().Compare(recs[i].mark()) >= 0 {
			return fmt.Errorf("projector: backfill of %s: unit %s record %s is out of order", version, b.Unit, b.Seq)
		}
	}
	rs := &rows{owner: keys.OwnerBackfill, groups: map[groupKey]*rowGroup{}}
	for i := range recs {
		r := &recs[i]
		vals, bad := v.stream.Decode(r.New)
		if err := rs.add(v, r, vals, bad); err != nil {
			if _, err = c.breakVersion(ctx, v, r, vals, err); err != nil {
				return err
			}
			return fmt.Errorf("projector: backfill of %s: unit %s record %s: %w", version, r.unit, r.Seq, ErrBroken)
		}
	}
	return errors.Join(c.applyRows(ctx, v.stream.Def.Name, "", rs.order)...)
}

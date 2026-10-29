// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"context"
	"errors"
	"fmt"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
)

// ErrBroken is the error of a backfill chunk with a record whose evaluation fails: the
// version's breaker is open, and a retry would fail again.
var ErrBroken = errors.New("projector: the backfill opened the version's breaker")

// BackfillRecord is a record a backfill applies: an archived INSERT record, or an item read
// from the stream table, which has neither unit nor sequence number.
type BackfillRecord struct {
	Unit string     // the archive record's delivery unit ID; "" for a table item
	Seq  string     // its sequence number as delivered; "" for a table item
	Item store.Item // the record's new image, or the table item
	// Mark is a table item's keys.TimeMark, which orders it and stands for it in the rows;
	// zero for an archive record.
	Mark keys.Mark
}

func (b BackfillRecord) record() (record, error) {
	if b.Mark == (keys.Mark{}) {
		return newRecord(Record{Op: Insert, Seq: b.Seq, New: b.Item}, b.Unit)
	}
	if b.Unit != "" || b.Seq != "" {
		return record{}, fmt.Errorf("a table item with time mark %+v has unit %q and sequence number %q", b.Mark, b.Unit, b.Seq)
	}
	return record{Record: Record{Op: Insert, New: b.Item}, seq: b.Mark.Seq, unitKey: b.Mark.UnitKey}, nil
}

// ApplyBackfill applies a chunk of archive records in mark order, by unit key then sequence
// number and without repeats, or of table items in time-mark order, to version, in the
// backfill's one namespace B: a row holds the records after the
// last one it holds, by mark. The caller applies the chunks in that order, each once the one
// before succeeded. A table item goes to SPLIT item bucket(event_id, n), so every worker puts
// it in the same item.
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
			return fmt.Errorf("projector: backfill of %s: %s is out of order", version, recs[i].name())
		}
	}
	rs := &rows{owner: keys.OwnerBackfill, groups: map[groupKey]*rowGroup{}}
	for i := range recs {
		r := &recs[i]
		vals, bad := v.stream.Decode(r.New)
		if err := rs.add(v, r, vals, bad); err != nil {
			if _, err = c.breakVersion(ctx, v, r, meta.CauseRecord, err); err != nil {
				return err
			}
			return fmt.Errorf("projector: backfill of %s: %s: %w", version, r.name(), ErrBroken)
		}
	}
	return errors.Join(c.applyRows(ctx, v.stream.Def.Name, "", rs.order, c.backfillRows)...)
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
)

// waitable returns the INSERTs of recs whose append in the core's region asked for a
// receipt (ADR 0015).
func (c *Core) waitable(recs []record) []record {
	var out []record
	for _, r := range recs {
		rcpt, origin := r.New[catalog.AttrReceipt], r.New[catalog.AttrOriginRegion]
		if r.Op == Insert && rcpt.Type == store.BOOL && rcpt.Bool && origin.Type == store.S && origin.Str == c.region {
			out = append(out, r)
		}
	}
	return out
}

// receipts writes the receipt of each record of waitable, sorted, below failed, every one
// when failed is "". Progress covers them by then, so a receipt never lands for a record
// that a batch failed by a fence after its rows, and a redelivered record at or below
// progress gets its receipt too. A receipt that fails to write fails its record: Lambda
// delivers it again.
func (c *Core) receipts(ctx context.Context, stream string, waitable []record, failed string, res *Result) {
	if failed != "" {
		waitable = slices.DeleteFunc(waitable, func(r record) bool { return r.seq >= failed })
	}
	errs := make([]error, len(waitable))
	now := c.now()
	fanOut(len(waitable), rowParallelism, func(i int) {
		r := &waitable[i]
		at, err := ingestTime(r.New)
		// A token names an event by its event_id and ingest_time: without them, none waits.
		if id := r.eventID(); err == nil && id != "" {
			errs[i] = c.meta.PutReceipt(ctx, stream, meta.ReceiptKey{EventID: id, It: at}, now)
		}
	})
	for i, err := range errs {
		if err != nil {
			res.FailedSeq = waitable[i].Seq
			res.RowErr = errors.Join(res.RowErr, fmt.Errorf("projector: %s: %w", waitable[i].name(), err))
			return
		}
	}
}

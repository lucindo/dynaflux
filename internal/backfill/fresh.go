// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/lucindo/dynaflux/internal/store"
)

// freshAge is how young a stream table must be for an empty replica to prove it never held an
// event: every event is kept at least a day after its write (DF2109), less an hour of margin.
const freshAge = 23 * time.Hour

// CheckStream reports whether a stream table is fresh (lifecycle.md §4.3, F2 and F3): no
// replica was restored from a backup, the earliest replica is younger than 23 hours when the
// check ends, and a consistent scan of b's replica, sent now, finds no item. It returns the
// table's birth too. The job's fresh rule and the CLI's choice of a source share it.
func CheckStream(ctx context.Context, t Tables, b store.Backend, streamTable string) (bool, time.Time, error) {
	born, restored, err := t.Born(ctx, streamTable)
	if err != nil {
		return false, time.Time{}, fmt.Errorf("backfill: describe %s: %w", streamTable, err)
	}
	if restored || time.Since(born) >= freshAge {
		return false, born, nil
	}
	var start *store.Key
	for {
		items, next, serr := b.ScanPage(ctx, streamTable, store.Scan{Segments: 1, Consistent: true, PageSize: 1}, start)
		if serr != nil {
			return false, born, fmt.Errorf("backfill: scan %s: %w", streamTable, serr)
		}
		if len(items) > 0 {
			return false, born, nil
		}
		if next == nil {
			return time.Since(born) < freshAge, born, nil
		}
		start = next
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projectortest_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/projector/projectortest"
)

func TestSelectBackfill(t *testing.T) {
	t.Parallel()
	// Name the units so that a's key sorts before b's.
	a, b := "shardId-a", "shardId-b"
	if keys.UnitKey(a) > keys.UnitKey(b) {
		a, b = b, a
	}
	seq := func(n int) string { return fmt.Sprintf("1%020d", n) }
	rec := func(u string, n int) projector.BackfillRecord { return projector.BackfillRecord{Unit: u, Seq: seq(n)} }
	recs := []projector.BackfillRecord{rec(b, 5), rec(a, 9), rec(b, 2), rec(a, 1), rec(b, 2), rec(b, 9), rec(a, 30)}
	fence, err := keys.PadSeq(seq(5))
	if err != nil {
		t.Fatal(err)
	}
	got, err := projectortest.SelectBackfill(recs, map[string]string{b: fence})
	if err != nil {
		t.Fatal(err)
	}
	// a has no fence and keeps every record; b keeps those up to its fence, once each.
	same := func(x, y projector.BackfillRecord) bool { return x.Unit == y.Unit && x.Seq == y.Seq }
	if want := []projector.BackfillRecord{rec(a, 1), rec(a, 9), rec(a, 30), rec(b, 2), rec(b, 5)}; !slices.EqualFunc(got, want, same) {
		t.Errorf("SelectBackfill = %v, want %v", got, want)
	}
	if _, err = projectortest.SelectBackfill([]projector.BackfillRecord{rec(a, 1), {Unit: a, Seq: "12"}}, nil); err == nil {
		t.Error("SelectBackfill took a sequence number of 2 digits")
	}
}

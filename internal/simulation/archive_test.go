// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/simulation"
)

func TestArchiveDeliversLateOutOfOrderAndTwice(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const n = 200
		cfg := simulation.Archive{MinDelay: time.Minute, MaxDelay: 15 * time.Minute, Dup: 0.5, Error: 0.2}
		a := simulation.NewArchiveModel(cfg, 1)
		put := time.Now()
		var recs []projector.Record
		for i := range n {
			recs = append(recs, projector.Record{Op: projector.Insert, Seq: strconv.Itoa(i)})
		}
		a.Put("u", recs)

		before := put.Add(cfg.MinDelay - time.Nanosecond)
		if seqs, _ := a.Arrived(before); len(seqs) != 0 || a.CompleteThrough(put, before) {
			t.Errorf("before the shortest delay: %d records arrived, complete %v; want none, false",
				len(seqs), a.CompleteThrough(put, before))
		}
		mid, _ := a.Arrived(put.Add(8 * time.Minute))
		if a.CompleteThrough(put, put.Add(8*time.Minute)) {
			t.Error("complete halfway through the delays")
		}
		all, errorPrefix := a.Arrived(put.Add(cfg.MaxDelay))
		if !a.CompleteThrough(put, put.Add(cfg.MaxDelay)) {
			t.Error("not complete after the longest delay")
		}
		if len(mid) == 0 || len(mid) >= len(all) {
			t.Errorf("%d records arrived halfway, %d in all; want some, not all", len(mid), len(all))
		}
		// In put order, the k distinct records that arrived first would be 0 to k-1.
		if early := distinct(t, mid); slices.Max(early) == len(early)-1 {
			t.Error("records arrived in the order they were put")
		}
		if len(all) <= n || errorPrefix == 0 || errorPrefix == len(all) {
			t.Errorf("%d entries, %d in the error prefix; want duplicates and some errors", len(all), errorPrefix)
		}
		if got := distinct(t, all); len(got) != n {
			t.Errorf("%d distinct records arrived, want %d", len(got), n)
		}
	})
}

func TestLyingArchiveIsCompleteAtOnce(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a := simulation.NewArchiveModel(simulation.Archive{MinDelay: time.Minute, MaxDelay: time.Hour, Lie: true}, 1)
		a.Put("u", []projector.Record{{Op: projector.Insert, Seq: "1"}})
		if !a.CompleteThrough(time.Now(), time.Now()) {
			t.Error("a lying watermark waited for delivery")
		}
	})
}

func distinct(t *testing.T, seqs []string) []int {
	t.Helper()
	var out []int
	for _, s := range seqs {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

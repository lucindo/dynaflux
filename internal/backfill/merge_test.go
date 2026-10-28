// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/projector/projectortest"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

var accounts = []string{"a", "b", "c", "d"} //nolint:gochecknoglobals // read-only, as a constant

// Whatever its memory, chunk size and number of workers at once, a backfill applies exactly
// what projectortest.SelectBackfill selects from every archived record, repeats included.
func TestStepAppliesWhatSelectBackfillSelects(t *testing.T) {
	t.Parallel()
	for seed := range 40 {
		t.Run(strconv.Itoa(seed), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				rng := rand.New(rand.NewPCG(uint64(seed), 7))
				f := newFixture(t)
				f.env.Chunk = 1 + rng.IntN(7)
				f.env.Memory = []int64{8 << 10, 64 << 10, 64 << 20}[rng.IntN(3)]
				fences := randomArchive(f, rng)
				want := selected(t, f, fences)
				f.start()
				var wg sync.WaitGroup
				for w := range 1 + rng.IntN(3) {
					wg.Go(func() { f.runFor(time.Duration(2+w) * time.Minute) })
				}
				wg.Wait()
				if got := f.rows(accounts...); !slices.Equal(got, want) {
					t.Errorf("rows %q, want %q (memory %d, chunk %d)", got, want, f.env.Memory, f.env.Chunk)
				}
			})
		})
	}
}

// randomArchive fills the archive with the records of up to five units, some listed with a
// fence at a random progress, and repeats some of them in other files. It returns the fences.
func randomArchive(f *fixture, rng *rand.Rand) map[string]string {
	files := []string{"delivered/1", "delivered/2", "errors/1"}
	fences := map[string]string{}
	var listed []string
	for u := range 1 + rng.IntN(5) {
		unit := "shardId-" + strconv.Itoa(u)
		n := rng.IntN(30)
		for i := range n {
			f.record(files[rng.IntN(len(files))], unit, i+1, accounts[rng.IntN(len(accounts))], 1+rng.Uint64N(100))
		}
		if rng.IntN(4) == 0 {
			continue
		}
		listed = append(listed, unit)
		fences[unit] = seq(0)
		if p := rng.IntN(n + 1); p > 0 {
			f.progress(unit, p)
			fences[unit] = seq(p)
		}
	}
	f.units.set(listed...)
	for _, file := range files {
		for line := range bytes.SplitSeq(bytes.TrimSuffix(f.archive.files[file], []byte("\n")), []byte("\n")) {
			if len(line) > 0 && rng.IntN(5) == 0 {
				f.archive.add(files[rng.IntN(len(files))], line)
			}
		}
	}
	return fences
}

// selected returns the rows that projectortest.SelectBackfill's selection from every archived
// record makes, applied in one store of its own.
func selected(t *testing.T, f *fixture, fences map[string]string) []string {
	t.Helper()
	def, _ := f.cat.Catalog().Stream(stream)
	dec := archive.NewDecoder(def)
	var recs []projector.BackfillRecord
	for _, file := range f.archive.files {
		for line := range bytes.Lines(file) {
			r, err := dec.Decode(bytes.TrimSuffix(line, []byte("\n")))
			if err != nil {
				t.Fatal(err)
			}
			recs = append(recs, projector.BackfillRecord{Unit: r.Unit, Seq: r.Seq, Item: r.Item})
		}
	}
	sel, err := projectortest.SelectBackfill(recs, fences)
	if err != nil {
		t.Fatal(err)
	}
	b := mem.New()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if len(sel) > 0 {
		if err = projector.New(f.cat.Projector(), b, nil, projector.Config{Naming: f.env.Naming}).ApplyBackfill(ctx, totals, sel); err != nil {
			t.Fatal(err)
		}
	}
	return rowsIn(t, b, f.cat, accounts...)
}

// runFor steps attempt 1, each step d long, until the job ends.
func (f *fixture) runFor(d time.Duration) {
	for range 1000 {
		ctx, cancel := context.WithTimeout(f.t.Context(), d)
		wait, done, err := backfill.Step(ctx, f.env, totals, 1, "local")
		cancel()
		if err != nil {
			f.t.Error(err)
			return
		}
		if done {
			return
		}
		time.Sleep(wait)
	}
	f.t.Error("the job never ended")
}

// With a megabyte, a backfill stages more runs than a merge reads at once and merges them in
// passes; a pass that outlives its step goes on in the next from the last mark it committed.
func TestStepMergesRunsInPasses(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.env.Memory = 1 << 20
		f.env.Chunk = 100
		f.env.Runs = slowRuns{Runs: f.runs, latency: 3 * time.Second}
		f.stepTime = 2 * time.Minute
		want := f.manyRecords(6000)
		f.start()
		mostRuns, passes := 0, 0
		f.runWatching(func(j meta.Job) {
			mostRuns = max(mostRuns, len(j.Runs))
			if j.Merge != nil {
				passes++
			}
		})
		if got := f.rows(accounts...); !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
		if j := f.job(); j.State != meta.JobDone || j.Applied != 6000 {
			t.Errorf("job %s applied %d, want done, 6000", j.State, j.Applied)
		}
		if mostRuns <= 2 || passes == 0 {
			t.Errorf("at most %d runs, and %d steps ended in a pass; want more than a merge reads at once, and a pass that outlived its step", mostRuns, passes)
		}
	})
}

// manyRecords archives n records of four fenced units, every one at or below its fence, and
// returns the rows they make.
func (f *fixture) manyRecords(n int) []string {
	units := []string{"shardId-1", "shardId-2", "shardId-3", "shardId-4"}
	f.units.set(units...)
	sums := map[string][2]uint64{}
	for i := range n {
		account, v := accounts[i%3], uint64(1+i%100)
		f.record(fmt.Sprintf("delivered/%d", i/1000), units[i%len(units)], i/len(units)+1, account, v)
		s := sums[account]
		sums[account] = [2]uint64{s[0] + 1, s[1] + 100/v}
	}
	for _, u := range units {
		f.progress(u, n)
	}
	var rows []string
	for _, a := range accounts {
		if s, ok := sums[a]; ok {
			rows = append(rows, fmt.Sprintf("%s %d %d", a, s[0], s[1]))
		} else {
			rows = append(rows, a+" -")
		}
	}
	return rows
}

// runWatching runs the job as run does, and shows watch the job after each step.
func (f *fixture) runWatching(watch func(meta.Job)) {
	f.t.Helper()
	for range 1000 {
		wait, done, err := f.step()
		if err != nil {
			f.t.Fatal(err)
		}
		watch(f.job())
		if done {
			return
		}
		time.Sleep(wait)
	}
	f.t.Fatal("the job never ended")
}

// slowRuns takes latency to read.
type slowRuns struct {
	backfill.Runs

	latency time.Duration
}

func (r slowRuns) ReadAt(ctx context.Context, name string, off, n int64) ([]byte, error) {
	time.Sleep(r.latency)
	return r.Runs.ReadAt(ctx, name, off, n)
}

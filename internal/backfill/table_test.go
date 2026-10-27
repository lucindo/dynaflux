// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// appendEvents appends an event of account with each n, now, as the application does.
func (f *fixture) appendEvents(account string, ns ...uint64) {
	f.t.Helper()
	if f.engine == nil {
		f.engine = engine.New(f.b, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})})
	}
	for _, n := range ns {
		res, err := f.engine.Append(f.t.Context(), f.cat, stream, [][]expr.Value{{expr.String(account), expr.Uint(n), expr.Null()}}, engine.AppendMode{})
		if err == nil {
			err = res[0].Err
		}
		if err != nil {
			f.t.Fatal(err)
		}
	}
}

// startTable starts version's job with a table source.
func (f *fixture) startTable(version string, split meta.Split) {
	f.t.Helper()
	r := backfill.Request{Version: version, Stream: stream, Name: strings.ReplaceAll(version, "@", "-"), Execution: backfill.LocalExecution, Split: split}
	if _, err := backfill.Start(f.t.Context(), f.m, r); err != nil {
		f.t.Fatal(err)
	}
}

// stepOf steps attempt 1 of version's job as worker, for f.stepTime.
func (f *fixture) stepOf(version, worker string) (time.Duration, bool, error) {
	ctx, cancel := context.WithTimeout(f.t.Context(), f.stepTime)
	defer cancel()
	return backfill.Step(ctx, f.env, version, 1, worker)
}

// runOf steps attempt 1 of version's job until it is done, waiting as each step says.
func (f *fixture) runOf(version string) {
	f.t.Helper()
	for range 1000 {
		wait, done, err := f.stepOf(version, "local")
		if err != nil {
			f.t.Fatal(err)
		}
		if done {
			return
		}
		time.Sleep(wait)
	}
	f.t.Fatal("the job never ended")
}

func (f *fixture) jobOf(version string) meta.Job {
	f.t.Helper()
	j, ok, err := f.m.Job(f.t.Context(), version)
	if err != nil || !ok {
		f.t.Fatalf("job of %s: %v, %v", version, ok, err)
	}
	return j
}

// rowsOf returns version's rows of accounts: the account and each aggregate, or "account -"
// when absent. Every aggregate of the fixture is an integer.
func (f *fixture) rowsOf(version string, accounts ...string) []string {
	f.t.Helper()
	e := engine.New(f.b, engine.Options{Region: "us-east-1"})
	var out []string
	for _, a := range accounts {
		row, ok, err := e.ReadRow(f.t.Context(), f.cat, version, []expr.Value{expr.String(a)})
		if err != nil {
			f.t.Fatal(err)
		}
		if !ok {
			out = append(out, a+" -")
			continue
		}
		parts := []string{a}
		for _, v := range row[1:] {
			parts = append(parts, strconv.FormatUint(v.AsUint(), 10))
		}
		out = append(out, strings.Join(parts, " "))
	}
	return out
}

// The time split's backfill waits until 21 minutes after the split time, then reads the
// table's events at or before it; live projection takes the rest. A table older than a day
// of LOG RETENTION may have lost events: the version is live, partial since a day before the
// scan could start.
func TestStepFillsAVersionFromTheTable(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.env.Chunk = 2
		f.appendEvents("a", 10)
		f.startTable(totals, meta.SplitTime)
		wait, done, err := f.stepOf(totals, "local")
		vs := f.state()
		if want := vs.SplitLate.Add(time.Minute).Sub(vs.ActivatedAt); wait != want || done || err != nil || want != 27*time.Minute+30*time.Second {
			t.Fatalf("wait %v, done %v, error %v; want 27m30s", wait, done, err)
		}
		time.Sleep(time.Until(vs.SplitAt))
		f.appendEvents("a", 20, 25) // the second one a microsecond later: live projection's
		f.appendEvents("b", 50)
		f.runOf(totals)
		if got, want := f.rowsOf(totals, "a", "b"), []string{"a 2 15", "b -"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
		j, vs := f.jobOf(totals), f.state()
		if j.State != meta.JobDone || j.Staged != 2 || j.Applied != 2 || j.Chunks != 1 || !j.ReadAfter.Equal(vs.SplitLate.Add(time.Minute)) ||
			!j.ScannedAt.Equal(j.ReadAfter) {
			t.Errorf("job %s, staged %d, applied %d in %d chunks, read after %v, scanned at %v; want done, 2, 2 in 1, %v, %v",
				j.State, j.Staged, j.Applied, j.Chunks, j.ReadAfter, j.ScannedAt, vs.SplitLate.Add(time.Minute), j.ReadAfter)
		}
		if want := j.ReadAfter.Add(-24 * time.Hour); vs.State != meta.Live || !vs.PartialSince.Equal(want) {
			t.Errorf("version %s, partial since %v; want live, partial since %v", vs.State, vs.PartialSince, want)
		}
	})
}

// partial_since is the later of read_after and the scan's end less an hour, less the LOG
// RETENTION of a day, however long apply takes after the scan; a table born more than ε
// after it held every event. The job starts at T, so read_after is T + 27m30s.
func TestStepPartialSince(t *testing.T) {
	t.Parallel()
	const earliest = 24*time.Hour - 27*time.Minute - 30*time.Second - 5*time.Minute // T − born at the edge
	for _, tt := range []struct {
		name string
		born time.Duration // T − born
		lag  time.Duration // how long apply pauses after read_after
		want func(j meta.Job) time.Time
	}{
		{"a table three days old", 72 * time.Hour, 0, func(j meta.Job) time.Time { return j.ReadAfter.Add(-24 * time.Hour) }},
		{"a table an hour old", time.Hour, 0, func(meta.Job) time.Time { return time.Time{} }},
		{"born ε after the earliest complete time", earliest, 0, func(j meta.Job) time.Time { return j.ReadAfter.Add(-24 * time.Hour) }},
		{"born a microsecond later", earliest - time.Microsecond, 0, func(meta.Job) time.Time { return time.Time{} }},
		{"applied three hours after the scan", 72 * time.Hour, 3 * time.Hour, func(j meta.Job) time.Time { return j.ReadAfter.Add(-24 * time.Hour) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.tables.born = time.Now().Add(-tt.born)
				f.appendEvents("a", 10)
				f.lag.set(time.Hour)
				f.startTable(totals, meta.SplitTime)
				for until := time.Now().Add(27*time.Minute + 30*time.Second + tt.lag); time.Now().Before(until); {
					wait, _, err := f.stepOf(totals, "local")
					if err != nil {
						t.Fatal(err)
					}
					time.Sleep(min(wait, time.Until(until)))
				}
				f.lag.set(0)
				f.runOf(totals)
				j, vs := f.jobOf(totals), f.state()
				if want := tt.want(j); vs.State != meta.Live || !vs.PartialSince.Equal(want) {
					t.Errorf("version %s, partial since %v; want live, %v", vs.State, vs.PartialSince, want)
				}
			})
		})
	}
}

// An idempotent version takes the none split: its backfill applies every event in the table
// after the listing delay. One with an additive aggregate or uniqExact refuses it.
func TestStepNoSplit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.appendEvents("a", 10, 30)
		f.startTable(peaks, meta.SplitNone)
		wait, _, err := f.stepOf(peaks, "local")
		if j := f.jobOf(peaks); wait != 90*time.Second || err != nil || j.Phase != meta.PhaseWait {
			t.Fatalf("wait %v, error %v, phase %s; want 90s in wait", wait, err, j.Phase)
		}
		time.Sleep(time.Hour)
		f.appendEvents("a", 50)
		f.runOf(peaks)
		if got, want := f.rowsOf(peaks, "a"), []string{"a 50"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
		for _, version := range []string{totals, kinds} {
			f.startTable(version, meta.SplitNone)
			_, _, err = f.stepOf(version, "local")
			want := "backfill: " + version + " needs a split by fences or by time: with none, live projection and the backfill would both apply its events"
			if states, _ := f.m.ViewStates(t.Context()); err == nil || err.Error() != want || states[version].State != "" {
				t.Errorf("error %v, version %s; want %q before any activation", err, states[version].State, want)
			}
		}
	})
}

// failRows fails the puts of row items of account a: an invocation that dies after its
// markers, before its rows.
type failRows struct{ store.Backend }

func (b failRows) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if it[catalog.AttrPK].Str == "a" && strings.Contains(table, "-view-") {
		return errors.New("the invocation died")
	}
	return b.Backend.Put(ctx, table, it, c)
}

// A version with uniqExact takes a split, though it is idempotent: a value counts for the
// record whose marker holds it, so a live attempt on a pending version that wrote its marker,
// died before its row and was never retried, because a build without the version moved the
// unit's progress past it, would leave a value that the backfill never counts.
func TestStepUniqExactTakesASplit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.appendEvents("a", 7)
		items, _, serr := f.b.ScanPage(t.Context(), f.env.Naming.StreamTable(stream), store.Scan{Segments: 1}, nil)
		if serr != nil || len(items) != 1 {
			t.Fatalf("scan: %d items, %v", len(items), serr)
		}
		core := projector.New(f.cat.Projector(), failRows{f.b}, nil, projector.Config{Naming: f.env.Naming})
		batch := projector.Batch{Stream: stream, Unit: "u1", Records: []projector.Record{{Op: projector.Insert, Seq: seq(1), New: items[0]}}}
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		res, err := core.Handle(ctx, batch)
		cancel()
		if err != nil || res.FailedSeq != seq(1) {
			t.Fatalf("live attempt: failed from %q, error %v; want its rows failed", res.FailedSeq, err)
		}
		f.progress("u1", 1)
		f.startTable(kinds, meta.SplitTime)
		f.runOf(kinds)
		if got, want := f.rowsOf(kinds, "a"), []string{"a 1"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
	})
}

// A step ends the job of a version that broke or was retired while it waited: the late check
// can break it as staging starts, and a deploy can retire it and drop its table, so the job
// reads and applies nothing more.
func TestStepEndsWithItsVersion(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		end   func(f *fixture) error
		state meta.JobState
	}{
		{"broken", func(f *fixture) error {
			_, _, err := f.m.MarkBroken(f.t.Context(), totals, stream, meta.Failure{Cause: meta.CauseSplit, EventID: "e", Error: "late"})
			return err
		}, meta.JobBroken},
		{"retired", func(f *fixture) error {
			_, err := f.m.Retire(f.t.Context(), totals, stream)
			return err
		}, meta.JobCancelled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.appendEvents("a", 10)
				f.startTable(totals, meta.SplitTime)
				wait, _, err := f.stepOf(totals, "local")
				if err != nil {
					t.Fatal(err)
				}
				if err = tt.end(f); err != nil {
					t.Fatal(err)
				}
				time.Sleep(wait)
				if _, done, serr := f.stepOf(totals, "local"); !done || serr != nil {
					t.Fatalf("done %v, error %v; want done", done, serr)
				}
				want := fmt.Sprintf("backfill: %s is %s", totals, f.state().State)
				if j := f.jobOf(totals); j.State != tt.state || j.Staged != 0 || j.Error == nil || j.Error.Phase != meta.PhaseWait || j.Error.Message != want {
					t.Errorf("job %s, staged %d, error %+v; want %s, 0, %q in wait", j.State, j.Staged, j.Error, tt.state, want)
				}
			})
		})
	}
}

// read_after leaves the clock margin between the backfill's host and a projector's: with the
// worker's clock half the margin fast and the projector's half slow, an event at or before
// the split time that lands just after the scan read its place reaches a projector whose
// clock has passed the late bound, which breaks the version rather than lose the event.
func TestStepTimeSplitAcrossClocks(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		half := f.env.Bounds.Clock / 2
		f.env.Now = func() time.Time { return time.Now().Add(half) }
		core := projector.New(f.cat.Projector(), f.b, nil, projector.Config{Naming: f.env.Naming, Now: func() time.Time { return time.Now().Add(-half) }})
		f.env.Core = core
		elsewhere := mem.New() // the region the event is appended in, now
		e := engine.New(elsewhere, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})})
		if res, err := e.Append(t.Context(), f.cat, stream, [][]expr.Value{{expr.String("a"), expr.Uint(10), expr.Null()}}, engine.AppendMode{}); err != nil || res[0].Err != nil {
			t.Fatalf("append: %v, %+v", err, res)
		}
		items, _, err := elsewhere.ScanPage(t.Context(), f.env.Naming.StreamTable(stream), store.Scan{Segments: 1}, nil)
		if err != nil || len(items) != 1 {
			t.Fatalf("scan: %d items, %v", len(items), err)
		}
		f.startTable(totals, meta.SplitTime)
		f.runOf(totals)
		time.Sleep(time.Second)
		if err = f.b.Put(t.Context(), f.env.Naming.StreamTable(stream), items[0], store.Cond{}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		res, err := core.Handle(ctx, projector.Batch{Stream: stream, Unit: "u1", Records: []projector.Record{{Op: projector.Insert, Seq: seq(1), New: items[0]}}})
		if vs := f.state(); err != nil || !slices.Equal(res.Stats.Broken, []string{totals}) || vs.State != meta.Broken || vs.Failure.Cause != meta.CauseSplit {
			t.Errorf("Handle broke %v, %v; version %s with %+v; want %s broken by its split", res.Stats.Broken, err, vs.State, vs.Failure, totals)
		}
	})
}

// foreignEvent appends an event, replaces its stream item with what foreign makes of it, as a
// foreign writer would, and returns the event's event_id.
func (f *fixture) foreignEvent(foreign func(it store.Item)) string {
	f.t.Helper()
	f.appendEvents("a", 10)
	table := f.env.Naming.StreamTable(stream)
	items, _, err := f.b.ScanPage(f.t.Context(), table, store.Scan{Segments: 1}, nil)
	if err != nil || len(items) != 1 {
		f.t.Fatalf("scan: %d items, %v", len(items), err)
	}
	it := maps.Clone(items[0])
	if err = f.b.Delete(f.t.Context(), table, store.Key{PK: it[catalog.AttrPK].Str, SK: it[catalog.AttrSK].Str}); err != nil {
		f.t.Fatal(err)
	}
	_, id, err := keys.ParseStreamSK(it[catalog.AttrSK].Str)
	if err != nil {
		f.t.Fatal(err)
	}
	foreign(it)
	if err = f.b.Put(f.t.Context(), table, it, store.Cond{}); err != nil {
		f.t.Fatal(err)
	}
	return id
}

// An item of the stream table that the job can't stage, which a foreign writer can leave,
// breaks the version, as live projection does with it: cause record for an item whose sort
// key isn't an event's, and cause split for an ingest_time the time split can't read.
func TestStepBreaksOnAForeignItem(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		version string
		split   meta.Split
		foreign func(it store.Item)
		cause   meta.Cause
		named   string // the failure's EventID; "" for the event's
	}{
		{"no ingest_time", totals, meta.SplitTime, func(it store.Item) { delete(it, catalog.AttrIngestTime) }, meta.CauseSplit, ""},
		{"not an event, split by time", totals, meta.SplitTime, func(it store.Item) { it[catalog.AttrSK] = store.Value{Type: store.S, Str: "x"} }, meta.CauseRecord, "x"},
		{"not an event, not split", peaks, meta.SplitNone, func(it store.Item) { it[catalog.AttrSK] = store.Value{Type: store.S, Str: "x"} }, meta.CauseRecord, "x"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				want := cmp.Or(tt.named, f.foreignEvent(tt.foreign))
				f.startTable(tt.version, tt.split)
				f.runOf(tt.version)
				states, err := f.m.ViewStates(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				j, vs := f.jobOf(tt.version), states[tt.version]
				if j.State != meta.JobBroken || vs.State != meta.Broken || vs.Failure.Cause != tt.cause || vs.Failure.EventID != want {
					t.Errorf("job %s, version %s with %+v; want broken, broken by cause %s at %q", j.State, vs.State, vs.Failure, tt.cause, want)
				}
			})
		})
	}
}

// manyEvents appends n events over accounts a0 to a9, and returns the rows of sums they make.
func (f *fixture) manyEvents(rng *rand.Rand, n int) []string {
	type sum struct{ events, total uint64 }
	sums := map[string]*sum{}
	for range n {
		a, v := "a"+strconv.Itoa(rng.IntN(10)), 1+rng.Uint64N(100)
		f.appendEvents(a, v)
		if sums[a] == nil {
			sums[a] = &sum{}
		}
		sums[a].events, sums[a].total = sums[a].events+1, sums[a].total+v
	}
	var out []string
	for i := range 10 {
		a := "a" + strconv.Itoa(i)
		if s := sums[a]; s != nil {
			out = append(out, fmt.Sprintf("%s %d %d", a, s.events, s.total))
		} else {
			out = append(out, a+" -")
		}
	}
	return out
}

func sumAccounts() []string {
	out := make([]string, 10)
	for i := range out {
		out[i] = "a" + strconv.Itoa(i)
	}
	return out
}

// smallPages returns scan pages of at most 3 items, as DynamoDB does for large items.
type smallPages struct{ store.Backend }

func (b smallPages) ScanPage(ctx context.Context, table string, s store.Scan, start *store.Key) ([]store.Item, *store.Key, error) {
	s.PageSize = 3
	return b.Backend.ScanPage(ctx, table, s, start)
}

// A scan that spans steps, at a rate, in small runs that merge, resumes each segment after the
// last key a commit left it at: every event counts once.
func TestStepScansAcrossSteps(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.env.Store, f.env.Memory, f.env.Segments, f.stepTime = smallPages{f.b}, 4<<10, 4, time.Minute+5*time.Second
		want := f.manyEvents(rand.New(rand.NewPCG(1, 2)), 60)
		r := backfill.Request{Version: sums, Stream: stream, Name: "sums-v1", Execution: backfill.LocalExecution, Split: meta.SplitTime, Rate: 2}
		if _, err := backfill.Start(t.Context(), f.m, r); err != nil {
			t.Fatal(err)
		}
		steps, resumed := 0, 0
		for done := false; !done; steps++ {
			wait, end, err := f.stepOf(sums, "local")
			if err != nil {
				t.Fatal(err)
			}
			if j := f.jobOf(sums); j.Phase == meta.PhaseStage && len(j.Stage.Next) > 0 {
				resumed++
			}
			done = end
			time.Sleep(wait)
		}
		if got := f.rowsOf(sums, sumAccounts()...); !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
		if j := f.jobOf(sums); j.Staged != 60 || resumed < 4 {
			t.Errorf("staged %d, %d steps ended mid-segment; want 60 and at least 4", j.Staged, resumed)
		}
	})
}

// scanHook runs do before the first page a scan reads.
type scanHook struct {
	store.Backend

	mu    sync.Mutex
	fired bool
	do    func()
}

func (h *scanHook) ScanPage(ctx context.Context, table string, s store.Scan, start *store.Key) ([]store.Item, *store.Key, error) {
	h.mu.Lock()
	fire := !h.fired
	h.fired = true
	h.mu.Unlock()
	if fire {
		h.do()
	}
	return h.Backend.ScanPage(ctx, table, s, start)
}

// A worker whose scan another worker committed past writes a run that the job never lists:
// garbage, which applies nothing twice.
func TestStepScanOfAStaleWorker(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		want := f.manyEvents(rand.New(rand.NewPCG(3, 4)), 20)
		f.startTable(sums, meta.SplitTime)
		wait, _, err := f.stepOf(sums, "local")
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(wait)
		f.env.Store = &scanHook{Backend: f.b, do: func() { f.runOf(sums) }}
		if _, done, serr := f.stepOf(sums, "stale"); !done || serr != nil {
			t.Fatalf("done %v, error %v; want done", done, serr)
		}
		if got := f.rowsOf(sums, sumAccounts()...); !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
		j := f.jobOf(sums)
		if left := f.runs.Names(j.Staging); j.Staged != 20 || j.State != meta.JobDone || len(left) != 1 {
			t.Errorf("job %s, staged %d, objects left %q; want done, 20, and the stale run", j.State, j.Staged, left)
		}
	})
}

// Two workers of one attempt step a SPLIT version's table job at once, with their own step
// times, chunk sizes and rate: each event counts once, whatever they interleave.
func TestStepDuplicateWorkersOnASplitView(t *testing.T) {
	t.Parallel()
	for seed := range uint64(20) {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				rng := rand.New(rand.NewPCG(seed, 5))
				f := newFixture(t)
				f.env.Memory, f.env.Segments, f.env.Chunk = 4<<10, 1+rng.IntN(8), 1+rng.IntN(5)
				want := f.manyEvents(rng, 40)
				r := backfill.Request{Version: sums, Stream: stream, Name: "sums-v1", Execution: backfill.LocalExecution, Split: meta.SplitTime, Rate: rng.IntN(3) * 10}
				if _, err := backfill.Start(t.Context(), f.m, r); err != nil {
					t.Fatal(err)
				}
				var wg sync.WaitGroup
				for w := range 2 {
					d := time.Minute + time.Duration(1+rng.IntN(5))*time.Second
					wg.Go(func() { f.worker(sums, "w"+strconv.Itoa(w), d) })
				}
				time.Sleep(10 * time.Minute)
				f.appendEvents("a0", 1000) // after the split time: live projection's
				wg.Wait()
				if got := f.rowsOf(sums, sumAccounts()...); !slices.Equal(got, want) {
					t.Errorf("rows %q, want %q", got, want)
				}
			})
		})
	}
}

// worker steps attempt 1 of version's job, each step lasting stepTime, until it is done.
func (f *fixture) worker(version, name string, stepTime time.Duration) {
	for range 1000 {
		ctx, cancel := context.WithTimeout(f.t.Context(), stepTime)
		wait, done, err := backfill.Step(ctx, f.env, version, 1, name)
		cancel()
		switch {
		case done:
			return
		case err != nil:
			time.Sleep(time.Second)
		}
		time.Sleep(wait)
	}
	f.t.Error("a worker never ended")
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// The fence split keeps each listed unit's records at or below its fence and every record of
// an unlisted unit, applies each repeated record once, and makes the version live.
func TestStepFillsAVersionFromTheArchive(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.env.Chunk = 3
		// u1 and u2 are fenced at their progress; u3 finished before the listing, so it has none.
		f.units.set("u1", "u2")
		f.progress("u1", 3)
		f.progress("u2", 2)
		for i, n := range []uint64{10, 20, 25, 50, 100} {
			f.record("delivered", "u1", i+1, "a", n)
		}
		for i := range 3 {
			f.record("delivered", "u2", i+1, "b", 100)
		}
		f.record("delivered", "u3", 1, "a", 4)
		f.record("delivered", "u3", 2, "a", 4)
		// A record Firehose delivered twice, the second time to the error prefix, which also
		// holds a line of a test record that Dynaflux didn't write.
		f.repeat("delivered", 2, "errors/firehose/events/1")
		f.archive.addError("errors/firehose/events/1", []byte(`{"probe":true}`))
		f.start()
		f.run()
		if got, want := f.rows("a", "b"), []string{"a 5 69", "b 2 2"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
		j := f.job()
		if j.State != meta.JobDone || j.Phase != meta.PhaseLive || j.Fresh || j.Error != nil {
			t.Errorf("job %s in %s, fresh %v, error %v; want done in live, not fresh, no error", j.State, j.Phase, j.Fresh, j.Error)
		}
		// The repeat is staged in the error prefix's run, and skipped with the 3 records above
		// fences.
		if j.Staged != 11 || j.Applied != 7 || j.Skipped != 4 || j.Chunks != 3 || j.Archive.Errors.Foreign != 1 {
			t.Errorf("staged %d, applied %d, skipped %d, chunks %d, foreign %d; want 11, 7, 4, 3, 1", j.Staged, j.Applied, j.Skipped, j.Chunks, j.Archive.Errors.Foreign)
		}
		if s := f.state().State; s != meta.Live {
			t.Errorf("version %s, want live", s)
		}
		if left := f.runs.Names("backfill/"); len(left) > 0 {
			t.Errorf("staged objects left: %q", left)
		}
	})
}

// repeat adds the record on the line-th line of file to the error object to.
func (f *fixture) repeat(file string, line int, to string) {
	f.archive.mu.Lock()
	lines := strings.SplitAfter(string(f.archive.files[file]), "\n")
	f.archive.mu.Unlock()
	f.archive.addError(to, []byte(strings.TrimSuffix(lines[line-1], "\n")))
}

// A stream that never held an event in the region goes live right after its fences, without
// a read (lifecycle.md §4.3). Any sign of an event sends the job down the normal path.
func TestStepFreshStream(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		setup func(f *fixture)
		fresh bool
	}{
		{"no event ever", func(*fixture) {}, true},
		{"a unit with progress", func(f *fixture) { f.units.set("u1"); f.progress("u1", 1) }, false},
		{"an item in the replica", func(f *fixture) { f.putEvent() }, false},
		{"a replica restored from a backup", func(f *fixture) { f.tables.restored = true }, false},
		{"a table 23 hours old", func(f *fixture) { f.tables.born = time.Now().Add(-23 * time.Hour) }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.tables.born = time.Now().Add(-time.Hour)
				tt.setup(f)
				f.start()
				f.run()
				j := f.job()
				if j.State != meta.JobDone || j.Fresh != tt.fresh {
					t.Errorf("job %s, fresh %v; want done, fresh %v", j.State, j.Fresh, tt.fresh)
				}
				if s := f.state().State; s != meta.Live {
					t.Errorf("version %s, want live", s)
				}
			})
		})
	}
}

// The fresh rule's scan is sent after the fences: an event that a projector handled after the
// activation, in a unit that finished before the listing, so that no fence of the version
// covers it, is in the table by then.
func TestStepFreshScansAfterTheFences(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.tables.born = time.Now().Add(-time.Hour)
		f.units.onList = f.putEvent
		f.start()
		f.run()
		if j := f.job(); j.State != meta.JobDone || j.Fresh {
			t.Errorf("job %s, fresh %v; want done, not fresh", j.State, j.Fresh)
		}
	})
}

// With an archive source, the job fences an idempotent version too (guard 3). A batch of a
// build without the version, which read its unit before the fence, then can't move progress
// past an event the version never got: it fails, and its retry by a build with the version
// applies the event. Without the fence, the event, archived after the cut, would be lost to
// the version.
func TestStepFencesEveryVersion(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.units.set("u1")
		f.record("delivered", "u1", 1, "a", 10)
		f.progress("u1", 1)
		f.appendEvents("a", 50)
		items, _, err := f.b.ScanPage(t.Context(), f.env.Naming.StreamTable(stream), store.Scan{Segments: 1}, nil)
		if err != nil || len(items) != 1 {
			t.Fatalf("scan: %d items, %v", len(items), err)
		}
		batch := projector.Batch{Stream: stream, Unit: "u1", Records: []projector.Record{{Op: projector.Insert, Seq: seq(2), New: items[0]}}}
		older, err := engine.Compile(catalogtest.Marshal(t, storeName, "CREATE STREAM events (account String, n UInt32) PRIMARY KEY (account);"))
		if err != nil {
			t.Fatal(err)
		}
		firehose := pausedArchiver{entered: make(chan struct{}), release: make(chan struct{})}
		core := projector.New(older.Projector(), f.b, firehose, projector.Config{Naming: f.env.Naming, Region: "us-east-1"})
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		// The older build's batch reads the unit, then waits at its archive write.
		handled := make(chan error, 1)
		go func() {
			_, herr := core.Handle(ctx, batch)
			handled <- herr
		}()
		<-firehose.entered
		// The job fences u1 at its progress, 1, then waits at the watermark.
		f.archive.setComplete(false)
		r := backfill.Request{Version: peaks, Stream: stream, Name: "peaks-v1", Execution: backfill.LocalExecution, Split: meta.SplitFence}
		if _, err = backfill.Start(t.Context(), f.m, r); err != nil {
			t.Fatal(err)
		}
		if _, _, err = f.stepOf(peaks, "local"); err != nil {
			t.Fatal(err)
		}
		close(firehose.release)
		if err = <-handled; !errors.Is(err, meta.ErrFenced) {
			t.Fatalf("the older build's batch: %v; want it to fail on the fence", err)
		}
		// Lambda retries the batch, now on a build with the version.
		current := projector.New(f.cat.Projector(), f.b, nil, projector.Config{Naming: f.env.Naming, Region: "us-east-1"})
		if _, err = current.Handle(ctx, batch); err != nil {
			t.Fatal(err)
		}
		f.archive.setComplete(true)
		f.runOf(peaks)
		if got, want := f.rowsOf(peaks, "a"), []string{"a 50"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
	})
}

// pausedArchiver holds a batch at its archive write until released, and delivers nothing to
// the job's archive: the delivery comes after the job reads it.
type pausedArchiver struct{ entered, release chan struct{} }

func (a pausedArchiver) Archive(context.Context, string, string, []projector.Record) error {
	close(a.entered)
	<-a.release
	return nil
}

// A job split by fences of a stream with events fails each step at the watermark in an
// environment without the region's archive, and stays running for a step that has one.
func TestStepWithoutAnArchive(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.env.Archive = nil
		f.putEvent()
		f.start()
		if _, done, err := f.step(); done || err == nil || !strings.Contains(err.Error(), "can't read its archive") {
			t.Fatalf("done %v, error %v; want the archive unreadable", done, err)
		}
		if j := f.job(); j.State != meta.JobRunning || j.Phase != meta.PhaseWatermark {
			t.Errorf("job %s in %s; want running at the watermark", j.State, j.Phase)
		}
	})
}

// F2's edge: an empty replica proves nothing once the table is 23 hours old.
func TestCheckStream(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		table := f.env.Naming.StreamTable(stream)
		for _, tt := range []struct {
			name  string
			age   time.Duration
			fresh bool
		}{
			{"23 hours less a microsecond", 23*time.Hour - time.Microsecond, true},
			{"23 hours", 23 * time.Hour, false},
		} {
			born := time.Now().Add(-tt.age)
			fresh, gotBorn, err := backfill.CheckStream(t.Context(), &tablesFake{born: born}, f.b, table)
			if err != nil || fresh != tt.fresh || !gotBorn.Equal(born) {
				t.Errorf("%s: fresh %v, born %v, error %v; want %v, %v", tt.name, fresh, gotBorn, err, tt.fresh, born)
			}
		}
	})
}

// The job reads the archive only once it holds every record put before the cut.
func TestStepWaitsForTheWatermark(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.archive.setComplete(false)
		f.units.set("u1")
		f.progress("u1", 1)
		f.record("delivered", "u1", 1, "a", 10)
		f.start()
		wait, done, err := f.step()
		if wait != time.Minute || done || err != nil {
			t.Fatalf("wait %v, done %v, error %v; want a minute", wait, done, err)
		}
		if j := f.job(); j.Phase != meta.PhaseWatermark {
			t.Errorf("job in %s, want watermark", j.Phase)
		}
		f.archive.setComplete(true)
		f.run()
		if got, want := f.rows("a"), []string{"a 1 10"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
	})
}

// Guard 1: a job activates its version only when the region's deployed catalog defines it.
func TestStepChecksTheDeployedCatalog(t *testing.T) {
	t.Parallel()
	const refused = "backfill: totals@v1 isn't in the region's deployed catalog; deploy regional.yaml there first"
	for _, tt := range []struct {
		name     string
		deployed string // the SQL of the deployed catalog; "" for none
		want     string
	}{
		{"none deployed", "", refused},
		{"deployed without the version", "CREATE STREAM events (account String, n UInt32) PRIMARY KEY (account);", refused},
		{"another build's catalog that defines it", storeSQL + `
CREATE MATERIALIZED VIEW counts ORDER BY (account) AS SELECT account, count() AS c FROM events GROUP BY account;`, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.m = meta.New(f.b, "other-meta")
				f.env.Meta = f.m
				if tt.deployed != "" {
					if _, err := f.m.RegisterCatalog(t.Context(), catalogtest.Marshal(t, storeName, tt.deployed), "test"); err != nil {
						t.Fatal(err)
					}
				}
				f.recordOrigin()
				f.start()
				_, _, err := f.step()
				checkActivation(t, f, err, tt.want)
			})
		})
	}
}

// checkActivation checks a step that activated the version when want is "", and otherwise one
// that guard 1 refused with want.
func checkActivation(t *testing.T, f *fixture, err error, want string) {
	t.Helper()
	j, state := f.job(), f.state().State
	switch {
	case want == "" && (err != nil || j.Phase == meta.PhaseActivate || state != meta.Live):
		t.Errorf("error %v, job in %s, version %s; want it past activation and live", err, j.Phase, state)
	case want == "":
	case err == nil || err.Error() != want || j.Phase != meta.PhaseActivate || j.Error == nil || j.Error.Phase != meta.PhaseActivate:
		t.Errorf("error %v, job in %s with %+v; want %q in activate", err, j.Phase, j.Error, want)
	case state != "":
		t.Errorf("version %s, want no state", state)
	}
}

// A step of an attempt that no longer owns the job does nothing and says it is done.
func TestStepLeavesAJobToItsAttempt(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.start()
		f.start()
		ctx, cancel := context.WithTimeout(t.Context(), stepTime)
		defer cancel()
		wait, done, err := backfill.Step(ctx, f.env, totals, 1, "stale")
		if wait != 0 || !done || err != nil {
			t.Fatalf("wait %v, done %v, error %v; want done", wait, done, err)
		}
		if j := f.job(); j.Phase != meta.PhaseActivate || j.Attempt != 2 || j.Execution != "local" {
			t.Errorf("job in %s, attempt %d, execution %q; want untouched", j.Phase, j.Attempt, j.Execution)
		}
	})
}

// A record that fails to evaluate breaks the version and ends the job broken.
func TestStepEndsBrokenOnAFailingRecord(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.units.set("u1")
		f.progress("u1", 2)
		f.record("delivered", "u1", 1, "a", 10)
		f.record("delivered", "u1", 2, "a", 0)
		f.start()
		f.run()
		j, vs := f.job(), f.state()
		if j.State != meta.JobBroken || j.Error == nil || j.Error.Phase != meta.PhaseApply {
			t.Errorf("job %s with %+v; want broken in apply", j.State, j.Error)
		}
		if vs.State != meta.Broken || vs.Failure == nil || vs.Failure.Unit != "u1" || vs.Failure.Seq != seq(2) {
			t.Errorf("version %s with %+v; want broken by u1 %s", vs.State, vs.Failure, seq(2))
		}
	})
}

// breaking marks the version broken before it applies a chunk: a live record broke it while
// the step ran.
type breaking struct {
	backfill.Applier

	m *meta.Meta
}

func (b breaking) ApplyBackfill(ctx context.Context, version string, chunk []projector.BackfillRecord) error {
	failure := meta.Failure{Cause: meta.CauseRecord, Unit: "u9", Seq: seq(1), Error: "a live record failed"}
	if _, _, err := b.m.MarkBroken(ctx, version, stream, failure); err != nil {
		return err
	}
	return b.Applier.ApplyBackfill(ctx, version, chunk)
}

// A version that breaks while its step applies stays broken: the job ends broken, not live.
func TestStepEndsBrokenOnAVersionBrokenWhileItApplies(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.units.set("u1")
		f.progress("u1", 1)
		f.record("delivered", "u1", 1, "a", 10)
		f.env.Core = breaking{Applier: f.env.Core, m: f.m}
		f.start()
		f.run()
		if j, vs := f.job(), f.state(); j.State != meta.JobBroken || vs.State != meta.Broken || vs.Failure.Unit != "u9" {
			t.Errorf("job %s, version %s with %+v; want both broken by u9", j.State, vs.State, vs.Failure)
		}
	})
}

// A version retired before its job activated it ends the job cancelled.
func TestStepEndsCancelledOnARetiredVersion(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.start()
		if _, err := f.m.Retire(t.Context(), totals, stream); err != nil {
			t.Fatal(err)
		}
		f.run()
		if j := f.job(); j.State != meta.JobCancelled || j.Error == nil || j.Error.Phase != meta.PhaseActivate {
			t.Errorf("job %s with %+v; want cancelled in activate", j.State, j.Error)
		}
	})
}

// Apply pauses while the projector is more than 5 minutes behind, and a paused job resumes
// only below 2.5, in a later step too.
func TestStepPausesForTheProjectorsLag(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		lags []time.Duration // a step's each
		want []bool          // whether the record is applied after each step
	}{
		{"steady 4 minutes", []time.Duration{4 * time.Minute}, []bool{true}},
		{"6, then 4", []time.Duration{6 * time.Minute, 4 * time.Minute, 4 * time.Minute}, []bool{false, false, false}},
		{"6, then 2", []time.Duration{6 * time.Minute, 2 * time.Minute}, []bool{false, true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.units.set("u1")
				f.progress("u1", 1)
				f.record("delivered", "u1", 1, "a", 10)
				f.start()
				var got []bool
				for _, lag := range tt.lags {
					f.lag.set(lag)
					wait, _, err := f.step()
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, slices.Equal(f.rows("a"), []string{"a 1 10"}))
					time.Sleep(wait)
				}
				if !slices.Equal(got, tt.want) {
					t.Errorf("applied after each step %v, want %v", got, tt.want)
				}
			})
		})
	}
}

// A job's rate spaces its chunks: 2 records a second in chunks of 2 is a chunk a second.
func TestStepPacesToTheRate(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.env.Chunk = 2
		core := &timedCore{Applier: f.env.Core}
		f.env.Core = core
		f.units.set("u1")
		f.progress("u1", 6)
		for i := range 6 {
			f.record("delivered", "u1", i+1, "a", 10)
		}
		r := request()
		r.Rate = 2
		if _, err := backfill.Start(t.Context(), f.m, r); err != nil {
			t.Fatal(err)
		}
		f.run()
		var gaps []time.Duration
		for i := 1; i < len(core.at); i++ {
			gaps = append(gaps, core.at[i].Sub(core.at[i-1]))
		}
		if want := []time.Duration{time.Second, time.Second}; !slices.Equal(gaps, want) {
			t.Errorf("chunks %v apart, want %v", gaps, want)
		}
	})
}

type timedCore struct {
	backfill.Applier

	at []time.Time
}

func (c *timedCore) ApplyBackfill(ctx context.Context, version string, chunk []projector.BackfillRecord) error {
	c.at = append(c.at, time.Now())
	return c.Applier.ApplyBackfill(ctx, version, chunk)
}

// A staged run that goes missing sends the job back to stage; the cursor keeps rows exact.
func TestStepRestagesAMissingRun(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.env.Chunk = 1
		f.units.set("u1")
		f.progress("u1", 3)
		for i := range 3 {
			f.record("delivered", "u1", i+1, "a", 10)
		}
		f.start()
		f.lag.set(time.Hour)
		if _, _, err := f.step(); err != nil {
			t.Fatal(err)
		}
		j := f.job()
		if j.Phase != meta.PhaseApply {
			t.Fatalf("job in %s, want apply", j.Phase)
		}
		if err := f.runs.DeletePrefix(t.Context(), j.Staging); err != nil {
			t.Fatal(err)
		}
		f.lag.set(0)
		f.run()
		if got, want := f.rows("a"), []string{"a 3 30"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
		if j = f.job(); j.Staged != 6 || j.State != meta.JobDone {
			t.Errorf("job %s, staged %d; want done, staged twice: 6", j.State, j.Staged)
		}
	})
}

// A step that reaches its cutoff while it stages the archive commits what it read: the next
// step resumes after the last row it read.
func TestStepCommitsTheArchiveAtItsCutoff(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.units.set("u1")
		f.progress("u1", 3)
		for i := range 3 {
			f.record("delivered/"+strconv.Itoa(i), "u1", i+1, "a", 10)
		}
		f.archive.setComplete(false)
		f.start()
		if _, _, err := f.step(); err != nil || f.job().Phase != meta.PhaseWatermark {
			t.Fatalf("job in %s, %v; want the watermark", f.job().Phase, err)
		}
		f.archive.setComplete(true)
		f.archive.onOpen = func(string) { time.Sleep(4 * time.Minute) }
		ctx, cancel := context.WithTimeout(t.Context(), backfill.Reserve+7*time.Minute+30*time.Second)
		defer cancel()
		if _, _, err := backfill.Step(ctx, f.env, totals, 1, "local"); err != nil {
			t.Fatal(err)
		}
		j := f.job()
		if len(j.Runs) != 1 || j.Staged != 2 || j.Archive.File != 1 || j.Archive.Skip != 1 || j.Phase != meta.PhaseStage {
			t.Errorf("job in %s at %+v, staged %d in runs %+v; want 2 in a run, in stage after file 1's first row", j.Phase, j.Archive, j.Staged, j.Runs)
		}
		f.run()
		if got, want := f.rows("a"), []string{"a 3 30"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
	})
}

// A step that is staging when another worker stages again, after a run went missing, can't
// commit a run that claims the files whose runs the restage dropped.
func TestStepStagesAgainAfterARestageItDidNotSee(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.env.Memory = 4 << 10
		f.units.set("u1")
		f.progress("u1", 30)
		for i := range 30 {
			f.record("delivered/"+strconv.Itoa(i/10), "u1", i+1, "a", 10)
		}
		restaged := false
		f.archive.onOpen = func(file string) {
			if file != "delivered/2" || restaged {
				return
			}
			restaged = true
			j := f.job()
			if err := f.runs.DeletePrefix(t.Context(), j.Staging); err != nil {
				t.Fatal(err)
			}
			if _, err := f.m.UpdateJob(t.Context(), totals, func(j *meta.Job) (bool, error) {
				j.Runs, j.Phase, j.Archive = nil, meta.PhaseStage, meta.ArchiveStage{Scanned: j.Archive.Scanned, Tries: j.Archive.Tries}
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		f.start()
		f.run()
		if !restaged {
			t.Fatal("no restage")
		}
		if got, want := f.rows("a"), []string{"a 30 300"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
	})
}

// Start creates a job, claims one for its next attempt, and keeps an activating version's
// split.
func TestStart(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		ctx := t.Context()
		r := request()
		r.Rate = 50
		j, err := backfill.Start(ctx, f.m, r)
		if err != nil {
			t.Fatal(err)
		}
		staging := "backfill/totals@v1/" + strconv.FormatInt(time.Now().UnixMicro(), 10) + "/"
		if j.State != meta.JobRunning || j.Phase != meta.PhaseActivate || j.Attempt != 1 || j.Split != meta.SplitFence ||
			j.Name != "totals-v1" || j.Execution != "local" || j.Rate != 50 || j.Staging != staging {
			t.Errorf("new job %+v", j)
		}
		r.Execution, r.Rate = func(attempt int) string { return claimed + strconv.Itoa(attempt) }, 0
		if j, err = backfill.Start(ctx, f.m, r); err != nil || j.Attempt != 2 || j.Rate != 50 || j.Execution != claimed+"2" {
			t.Errorf("claimed job attempt %d rate %d execution %q, %v; want attempt 2, rate 50, %q", j.Attempt, j.Rate, j.Execution, err, claimed+"2")
		}
		if _, err = f.m.Activate(ctx, totals, stream, meta.SplitFence, meta.DefaultBounds); err != nil {
			t.Fatal(err)
		}
		r.Split = meta.SplitTime
		if _, err = backfill.Start(ctx, f.m, r); !errors.Is(err, meta.ErrTransition) {
			t.Errorf("another split: %v, want ErrTransition", err)
		}
		if _, err = f.m.UpdateJob(ctx, totals, func(j *meta.Job) (bool, error) {
			j.State = meta.JobDone
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		r.Split = ""
		if _, err = backfill.Start(ctx, f.m, r); !errors.Is(err, meta.ErrJobExists) {
			t.Errorf("a done job: %v, want ErrJobExists", err)
		}
		if err = f.m.MakeLive(ctx, totals, time.Time{}); err != nil {
			t.Fatal(err)
		}
		if _, err = backfill.Start(ctx, f.m, r); !errors.Is(err, meta.ErrTransition) {
			t.Errorf("a live version: %v, want ErrTransition", err)
		}
	})
}

const claimed = "arn:aws:states:us-east-1:1:execution:m:totals-v1-a"

// A claim records the execution of the attempt it claims, also when another run claimed the
// attempt after the one it read.
func TestStartNamesTheClaimedExecution(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.start()
		r := request()
		r.Execution = func(attempt int) string { return claimed + strconv.Itoa(attempt) }
		for attempt := 2; attempt <= 3; attempt++ {
			if j, err := backfill.Start(t.Context(), f.m, r); err != nil || j.Attempt != attempt || j.Execution != claimed+strconv.Itoa(attempt) {
				t.Errorf("claim: attempt %d execution %q, %v; want attempt %d, %s%d", j.Attempt, j.Execution, err, attempt, claimed, attempt)
			}
		}
	})
}

// missedJob reads no job item the first time: a caller that read before another created it.
type missedJob struct {
	store.Backend

	missed bool
}

func (b *missedJob) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	if !b.missed && k.PK == "job" {
		b.missed = true
		return nil, nil //nolint:nilnil // no item, as Get says
	}
	return b.Backend.Get(ctx, table, k, consistent)
}

// A job takes the split that a claim asks for until its version is activated; then a claim
// takes the state item's split, as one racing an older attempt's activation needs, and a claim
// that asks for another fails. A caller that lost the race to create the job steps the
// winner's attempt.
func TestStartSplit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		ctx := t.Context()
		r := request()
		r.Split = meta.SplitTime
		if _, err := backfill.Start(ctx, f.m, r); err != nil {
			t.Fatal(err)
		}
		late := meta.New(&missedJob{Backend: f.b}, f.env.Naming.MetaTable())
		if j, err := backfill.Start(ctx, late, r); err != nil || j.Attempt != 1 || j.Split != meta.SplitTime {
			t.Errorf("lost the race: job %s attempt %d, %v; want time, 1", j.Split, j.Attempt, err)
		}
		r.Split = meta.SplitFence
		if j, err := backfill.Start(ctx, f.m, r); err != nil || j.Attempt != 2 || j.Split != meta.SplitFence {
			t.Errorf("claim of a pending version with another split: job %s attempt %d, %v; want fence, 2", j.Split, j.Attempt, err)
		}
		if _, err := f.m.Activate(ctx, totals, stream, meta.SplitTime, meta.DefaultBounds); err != nil {
			t.Fatal(err)
		}
		r.Split = ""
		if j, err := backfill.Start(ctx, f.m, r); err != nil || j.Attempt != 3 || j.Split != meta.SplitTime {
			t.Errorf("claim of a version activated with another split: job %s attempt %d, %v; want time, 3", j.Split, j.Attempt, err)
		}
		r.Split = meta.SplitFence
		if j, err := backfill.Start(ctx, f.m, r); !errors.Is(err, meta.ErrTransition) {
			t.Errorf("claim with another split once activated: job %s attempt %d, %v; want ErrTransition", j.Split, j.Attempt, err)
		}
	})
}

// cancelling cancels its step's context while it applies a chunk.
type cancelling struct {
	backfill.Applier

	cancel context.CancelFunc
}

func (c cancelling) ApplyBackfill(ctx context.Context, _ string, _ []projector.BackfillRecord) error {
	c.cancel()
	return ctx.Err()
}

// A step whose caller cancels it, as an interrupt of dynaflux dev does, returns the
// cancellation and records nothing, which it no longer can: the job stays as it was.
func TestStepCancelledByItsCaller(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		var logs bytes.Buffer
		f.env.Log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
		f.units.set("u1")
		f.progress("u1", 1)
		f.record("delivered", "u1", 1, "a", 10)
		f.start()
		ctx, cancel := context.WithTimeout(t.Context(), stepTime)
		defer cancel()
		f.env.Core = cancelling{Applier: f.env.Core, cancel: cancel}
		if _, _, err := backfill.Step(ctx, f.env, totals, 1, "local"); !errors.Is(err, context.Canceled) {
			t.Errorf("Step = %v, want context.Canceled", err)
		}
		if j := f.job(); logs.Len() > 0 || j.Error != nil || j.Phase != meta.PhaseApply {
			t.Errorf("logs %q, job in %s with %+v; want no log, apply, no error", logs.String(), j.Phase, j.Error)
		}
	})
}

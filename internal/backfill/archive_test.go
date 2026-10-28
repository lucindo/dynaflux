// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/meta"
)

// archived gives the fixture a unit fenced at 3 with 3 records of account a, the job's view
// version a row "a 3 30", and the archive one partition of 3 records: the day they happened.
func archived(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	f.units.set("u1")
	f.progress("u1", 3)
	for i := range 3 {
		f.record("delivered", "u1", i+1, "a", 10)
	}
	f.archive.parts = []backfill.Partition{{From: day(0), To: day(1), Records: 3, Bytes: 1 << 20}}
	return f
}

// day returns the start of the d-th day from the bubble's first.
func day(d int) time.Time { return time.Date(2000, 1, 1+d, 0, 0, 0, 0, time.UTC) }

// exports returns the exports the archive started, as "from-to #try" in hours of day(0).
func (f *fixture) exports() []string {
	var out []string
	for _, q := range f.archive.started() {
		if q.q.Kind == backfill.QueryExport {
			out = append(out, fmt.Sprintf("%d-%d #%s", hours(q.q.From), hours(q.q.To), path(q.q.Dest, -2)))
		}
	}
	return out
}

func hours(t time.Time) int { return int(t.Sub(day(0)) / time.Hour) }

// path returns the i-th element of a /-separated path from its end, -1 being the last.
func path(p string, i int) string {
	parts := strings.Split(p, "/")
	return parts[len(parts)+i]
}

// runToError steps attempt 1 until a step fails, and returns its error, or nil when the job
// ended.
func (f *fixture) runToError() error {
	f.t.Helper()
	for range 1000 {
		wait, done, err := f.step()
		if err != nil || done {
			return err
		}
		time.Sleep(wait)
	}
	f.t.Fatal("the job never ended")
	return nil
}

// checkEnd checks how the job ended: with a step's error that holds want, or done with its
// row when want is "".
func (f *fixture) checkEnd(err error, want string) {
	f.t.Helper()
	switch {
	case want == "" && err != nil, want != "" && (err == nil || !strings.Contains(err.Error(), want)):
		f.t.Errorf("job: %v, want %q", err, want)
	case want == "":
		if got, row := f.rows("a"), []string{"a 3 30"}; !slices.Equal(got, row) {
			f.t.Errorf("rows %q, want %q", got, row)
		}
	}
}

func failed(kind backfill.FailureKind, reason string) backfill.QueryStatus {
	return backfill.QueryStatus{State: backfill.QueryFailed, Failure: kind, Reason: reason}
}

// Each outcome of a slice's try leads where ADR 0016 §6 says: a new try, which a cancellation,
// a retryable or a permission failure doesn't count against the slice's three; halves of a
// slice that timed out; or a job error once the slice has no tries left.
func TestStepTakesTheTriesOfASlice(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		parts    []backfill.Partition // the archive's, when not one day
		outcomes []backfill.QueryStatus
		exports  []string
		err      string // what a step fails with; "" when the job ends done
	}{
		{name: "succeeded", exports: []string{"0-24 #1"}},
		{name: "failed, then succeeded", outcomes: []backfill.QueryStatus{failed(backfill.FailureOther, "boom")}, exports: []string{"0-24 #1", "0-24 #2"}},
		{name: "cancelled", outcomes: []backfill.QueryStatus{{State: backfill.QueryCancelled}}, exports: []string{"0-24 #1", "0-24 #2"}},
		{name: "retryable", outcomes: []backfill.QueryStatus{failed(backfill.FailureRetryable, "slow down")}, exports: []string{"0-24 #1", "0-24 #2"}},
		{
			name:     "permission",
			outcomes: []backfill.QueryStatus{failed(backfill.FailurePermission, "Access denied")},
			exports:  []string{"0-24 #1"},
			err:      "needs a grant that RoleBackfill lacks (infra.md §9): Access denied",
		},
		{
			name: "two failures don't count, three do",
			outcomes: []backfill.QueryStatus{
				failed(backfill.FailureOther, "boom"), {State: backfill.QueryCancelled}, failed(backfill.FailureRetryable, "slow down"),
				failed(backfill.FailureOther, "boom"), failed(backfill.FailureOther, "boom 3"),
			},
			exports: []string{"0-24 #1", "0-24 #2", "0-24 #3", "0-24 #4", "0-24 #5"},
			err:     "failed 3 tries: boom 3",
		},
		{
			name:     "timed out",
			outcomes: []backfill.QueryStatus{failed(backfill.FailureTimeout, "timed out"), failed(backfill.FailureTimeout, "timed out")},
			exports:  []string{"0-24 #1", "0-12 #1", "0-6 #1", "6-12 #1", "12-24 #1"},
		},
		{
			name:     "timed out at three hours",
			parts:    []backfill.Partition{{From: day(0), To: day(0).Add(3 * time.Hour), Bytes: 1 << 20}},
			outcomes: []backfill.QueryStatus{failed(backfill.FailureTimeout, "timed out"), failed(backfill.FailureTimeout, "timed out")},
			exports:  []string{"0-3 #1", "0-1 #1"},
			err:      "too short to halve: timed out",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := archived(t)
				if tt.parts != nil {
					f.archive.parts = tt.parts
				}
				f.archive.setOutcomes(tt.outcomes...)
				f.start()
				f.checkEnd(f.runToError(), tt.err)
				if got := f.exports(); !slices.Equal(got, tt.exports) {
					t.Errorf("exports %q, want %q", got, tt.exports)
				}
			})
		})
	}
}

// A slice that has no tries left fails every step after, without another query.
func TestStepStopsASliceWithoutTries(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := archived(t)
		boom := failed(backfill.FailureOther, "boom")
		f.archive.setOutcomes(boom, boom, boom)
		f.start()
		for range 3 {
			if err := f.runToError(); err == nil || !strings.Contains(err.Error(), "failed 3 tries: boom") {
				t.Fatalf("job: %v, want 3 failed tries", err)
			}
		}
		if got, want := f.exports(), []string{"0-24 #1", "0-24 #2", "0-24 #3"}; !slices.Equal(got, want) {
			t.Errorf("exports %q, want %q", got, want)
		}
		if j := f.job(); j.Archive.Tries != 2 || j.Archive.Export.Failed != 2 || j.State != meta.JobRunning {
			t.Errorf("job %s with %+v; want running, 2 tries failed before the last", j.State, j.Archive)
		}
	})
}

// Every try of a slice has a token and a prefix of its own; a throttled start keeps its token.
func TestStepGivesEachTryItsToken(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := archived(t)
		f.archive.throttle = 1
		f.archive.setOutcomes(failed(backfill.FailureOther, "boom"))
		f.start()
		start := time.Now()
		f.run()
		if waited := time.Since(start); waited < time.Minute {
			t.Errorf("the job ended %v after a throttled start, want a minute's wait", waited)
		}
		qs := f.archive.started()
		if len(qs) != 3 || qs[1].token == qs[2].token || qs[1].q.Dest == qs[2].q.Dest || path(qs[1].q.Dest, -2) != "1" {
			t.Fatalf("queries %+v; want a listing and 2 tries, with tokens and prefixes of their own", qs)
		}
		j := f.job()
		if want := fmt.Sprintf("%sunload/%s/%d-%d/2/", j.Staging, j.Archive.Plan, day(0).UnixMicro(), day(1).UnixMicro()); qs[2].q.Dest != want {
			t.Errorf("the second try's prefix %q, want %q", qs[2].q.Dest, want)
		}
	})
}

// The job holds a query's ID before it waits for the query, and a step that lost the start's
// response starts it again with the same token: one query.
func TestStepSavesTheQueryBeforeItWaits(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := archived(t)
		f.archive.loseNext = true
		f.archive.running = 1
		f.start()
		if err := f.runToError(); err == nil || !strings.Contains(err.Error(), "connection reset") {
			t.Fatalf("job: %v, want the lost response", err)
		}
		wait, _, err := f.step()
		if err != nil || wait != time.Minute {
			t.Fatalf("wait %v, %v; want a minute for the running query", wait, err)
		}
		if j := f.job(); j.Archive.Export.ID != "q1" {
			t.Errorf("job waits with export %+v, want query q1", j.Archive.Export)
		}
		f.run()
		if got, want := f.exports(), []string{"0-24 #1"}; !slices.Equal(got, want) {
			t.Errorf("exports %q, want %q", got, want)
		}
	})
}

// A restage lists the partitions again and plans again, so its queries' tokens and prefixes
// are new though the tries' count starts over: the first listing's results may be gone.
func TestStepRestagesWithANewPlan(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := archived(t)
		f.start()
		f.lag.set(time.Hour)
		if _, _, err := f.step(); err != nil || f.job().Phase != meta.PhaseApply {
			t.Fatalf("job in %s, %v; want apply", f.job().Phase, err)
		}
		if err := f.runs.DeletePrefix(t.Context(), f.job().Staging); err != nil {
			t.Fatal(err)
		}
		f.lag.set(0)
		f.run()
		var exports, listings []fakeQuery
		for _, q := range f.archive.started() {
			if q.q.Kind == backfill.QueryExport {
				exports = append(exports, q)
			} else {
				listings = append(listings, q)
			}
		}
		if len(exports) != 2 || exports[0].token == exports[1].token || exports[0].q.Dest == exports[1].q.Dest {
			t.Errorf("exports %+v; want 2, with tokens and prefixes of their own", exports)
		}
		if len(listings) != 2 {
			t.Errorf("listings %+v; want 2, with tokens of their own", listings)
		}
		if got, want := f.rows("a"), []string{"a 3 30"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
	})
}

// A step whose read fails keeps what it read before: the next step reads on from there.
func TestStepKeepsWhatItReadBeforeAFailure(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		failing string
		staged  int64 // before the failed read
	}{
		{"errors/firehose/events/1", 1},
		{"delivered/1", 3},
	} {
		t.Run(tt.failing, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.units.set("u1")
				f.progress("u1", 4)
				f.recordError("errors/firehose/events/0", "u1", 1, "a", 10)
				f.recordError("errors/firehose/events/1", "u1", 2, "a", 10)
				f.record("delivered/0", "u1", 3, "a", 10)
				f.record("delivered/1", "u1", 4, "a", 10)
				f.archive.failOpen = tt.failing
				f.start()
				if err := f.runToError(); err == nil || !strings.Contains(err.Error(), "SlowDown") {
					t.Fatalf("job: %v, want the failed read", err)
				}
				if j := f.job(); j.Staged != tt.staged {
					t.Errorf("staged %d after the failed read, want %d", j.Staged, tt.staged)
				}
				if err := f.runToError(); err != nil {
					t.Fatal(err)
				}
				if got, want := f.rows("a"), []string{"a 4 40"}; !slices.Equal(got, want) {
					t.Errorf("rows %q, want %q", got, want)
				}
			})
		})
	}
}

// Two checks fail a slice: a whole slice that yields fewer rows than its partitions held takes
// a new try, three at most; the first slice, when whole, may scan at most twice its bytes and
// a gigabyte. A half's bytes are a share of its partitions', which it may scan whole.
func TestStepChecksASlice(t *testing.T) {
	t.Parallel()
	t.Run("integrity", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			f := archived(t)
			f.archive.parts[0].Records = 4
			f.start()
			if err := f.runToError(); err == nil || !strings.Contains(err.Error(), "yielded 3 rows, fewer than the 4 its partitions held, in 3 tries") {
				t.Errorf("job: %v, want the integrity check", err)
			}
			if got, want := f.exports(), []string{"0-24 #1", "0-24 #2", "0-24 #3"}; !slices.Equal(got, want) {
				t.Errorf("exports %q, want %q", got, want)
			}
		})
	})
	t.Run("pruning", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			f := archived(t)
			f.archive.setOutcomes(backfill.QueryStatus{State: backfill.QuerySucceeded, Scanned: 2<<20 + 1e9 + 1})
			f.start()
			if err := f.runToError(); err == nil || !strings.Contains(err.Error(), "Athena didn't prune partitions") {
				t.Errorf("job: %v, want the pruning check", err)
			}
		})
	})
	t.Run("pruning of a halved first slice", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			f := archived(t)
			f.archive.setOutcomes(failed(backfill.FailureTimeout, "timed out"), backfill.QueryStatus{State: backfill.QuerySucceeded, Scanned: 2<<20 + 1e9 + 1})
			f.start()
			f.checkEnd(f.runToError(), "")
			if got, want := f.exports(), []string{"0-24 #1", "0-12 #1", "12-24 #1"}; !slices.Equal(got, want) {
				t.Errorf("exports %q, want %q", got, want)
			}
		})
	})
	t.Run("pruning at the limit", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			f := archived(t)
			f.archive.setOutcomes(backfill.QueryStatus{State: backfill.QuerySucceeded, Scanned: 2<<20 + 1e9})
			f.start()
			if err := f.runToError(); err != nil {
				t.Errorf("job: %v", err)
			}
			if j := f.job(); j.Archive.Scanned != 100+2<<20+1e9 {
				t.Errorf("scanned %d, want the listing's and the export's", j.Archive.Scanned)
			}
		})
	})
}

// A stream table re-created by hand starts a new history (lifecycle.md §10): the job leaves
// out the archive's records that arrived before the table's creation, and the error objects
// written before it, so neither an older table's event nor its failed commit reaches the
// version.
func TestStepLeavesOutAnOlderTable(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.archive.mu.Lock()
		f.archive.errors["errors/firehose/events/old-commit"] = []byte(`{"errorCode":"Iceberg.CommitFailed","dataFiles":["s3://b/1.parquet"]}` + "\n")
		f.archive.written["errors/firehose/events/old-commit"] = time.Now()
		f.archive.mu.Unlock()
		f.archive.addError("errors/firehose/events/old-record", f.eventAt("old", 8, "a", 1000, time.Now()))
		time.Sleep(2 * time.Hour)
		f.tables.born = time.Now().Add(-time.Hour)
		f.archive.add("delivered", f.eventAt("old", 9, "a", 100, time.Now().Add(-90*time.Minute)))
		f.archive.addError("errors/firehose/events/new", f.eventAt("old", 10, "a", 10000, f.tables.born.Truncate(time.Second).Add(-time.Second)))
		f.units.set("u1")
		f.progress("u1", 1)
		f.record("delivered", "u1", 1, "a", 10)
		f.start()
		if err := f.runToError(); err != nil {
			t.Fatal(err)
		}
		if got, want := f.rows("a"), []string{"a 1 10"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
		if item, _, err := f.m.Stream(t.Context(), stream); err != nil || len(item.Holes) != 0 {
			t.Errorf("holes %+v, %v; want none", item.Holes, err)
		}
	})
}

// The job reads the error prefix first: Dynaflux's records go to the runs, gzipped or not;
// a line that holds anything else records an errors hole and breaks the job, as an incomplete
// archive, before any query.
func TestStepReadsTheErrorPrefixFirst(t *testing.T) {
	t.Parallel()
	record := func(f *fixture) []byte {
		f.archive.mu.Lock()
		defer f.archive.mu.Unlock()
		first, _, _ := bytes.Cut(f.archive.files["delivered"], []byte("\n"))
		return first
	}
	tests := []struct {
		name   string
		object func(f *fixture) []byte
		broken string // the reason the job breaks with; "" when it ends done
	}{
		{"a record of Dynaflux", func(f *fixture) []byte { return errorLine(record(f)) }, ""},
		{"gzipped", func(f *fixture) []byte { return gzipped(t, errorLine(record(f))) }, ""},
		{"a record Dynaflux didn't write", func(*fixture) []byte { return errorLine([]byte(`{"probe":1}`)) }, ""},
		{"data files of a failed commit", func(*fixture) []byte {
			return []byte(`{"errorCode":"Iceberg.CommitFailed","dataFiles":["s3://b/archive/events/data/1.parquet"]}` + "\n")
		}, "holds no record: the data files of a commit that failed"},
		{"a record of Dynaflux that doesn't decode", func(*fixture) []byte {
			return errorLine([]byte(`{"_shard":"u1","_seq":"x","account":"a"}`))
		}, "a record of Dynaflux doesn't decode"},
		{"not JSON", func(*fixture) []byte { return []byte("PAR1\n") }, "a line isn't JSON"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := archived(t)
				const object = "errors/firehose/events/2000/01/01/00/x"
				data := tt.object(f)
				f.archive.mu.Lock()
				f.archive.errors[object], f.archive.written[object] = data, time.Now()
				f.archive.mu.Unlock()
				f.start()
				err := f.runToError()
				if tt.broken == "" {
					f.checkEnd(err, "")
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				f.checkBrokenByErrors(object, tt.broken)
			})
		})
	}
}

// checkBrokenByErrors checks a job that broke on an error object it can't read, before any
// query, and the errors hole it recorded.
func (f *fixture) checkBrokenByErrors(object, reason string) {
	f.t.Helper()
	j := f.job()
	if j.State != meta.JobBroken || j.Error == nil || !strings.Contains(j.Error.Message, reason) ||
		!strings.Contains(j.Error.Message, backfill.ErrIncomplete.Error()) {
		f.t.Errorf("job %s with %+v; want broken as an incomplete archive: %s", j.State, j.Error, reason)
	}
	item, _, err := f.m.Stream(f.t.Context(), stream)
	if holes := item.After(); err != nil || len(holes) != 1 || holes[0].Cause != meta.HoleErrors || holes[0].Object != object {
		f.t.Errorf("holes %+v, %v; want an errors hole of %s", holes, err, object)
	}
	if qs := f.archive.started(); len(qs) > 0 {
		f.t.Errorf("queries %+v before the error prefix was read", qs)
	}
}

// errorLine returns the line of an error object that holds rec, as Firehose writes it.
func errorLine(rec []byte) []byte {
	a := &archiveFake{errors: map[string][]byte{}, written: map[string]time.Time{}}
	a.addError("x", rec)
	return a.errors["x"]
}

func gzipped(t *testing.T, data []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// A job that an older build left at the watermark, with no origin, breaks before it reads
// anything.
func TestStepWithoutAnOriginBreaksAtTheWatermark(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := archived(t)
		f.m = meta.New(f.b, "other-meta")
		f.env.Meta = f.m
		if _, err := f.m.RegisterCatalog(t.Context(), catalogtest.Marshal(t, storeName, storeSQL), "test"); err != nil {
			t.Fatal(err)
		}
		f.progress("u1", 3)
		f.start()
		if err := f.runToError(); err != nil {
			t.Fatal(err)
		}
		j := f.job()
		if j.State != meta.JobBroken || j.Error == nil || j.Error.Phase != meta.PhaseWatermark || !strings.Contains(j.Error.Message, "no origin was recorded") {
			t.Errorf("job %s with %+v; want broken at the watermark for no origin", j.State, j.Error)
		}
		if qs := f.archive.started(); len(qs) > 0 {
			t.Errorf("queries %+v", qs)
		}
	})
}

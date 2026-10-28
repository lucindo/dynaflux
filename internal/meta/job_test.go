// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// Marks whose B values are easy to write down: the unit hash's last byte, then the sequence
// number's bytes.
var (
	mark1 = keys.Mark{UnitKey: "0000000000001", Seq: seq(7)}   //nolint:gochecknoglobals // a constant
	mark2 = keys.Mark{UnitKey: "000000000000Z", Seq: seq(300)} //nolint:gochecknoglobals // a constant
)

func b(v ...byte) store.Value { return store.Value{Type: store.B, Bin: v} }

// tableJob is a new job of the version that reads the stream table.
func tableJob() meta.Job {
	return meta.Job{
		Version: version, Stream: stream, Split: meta.SplitTime, State: meta.JobRunning, Phase: meta.PhaseActivate,
		Attempt: 1, Name: "page_daily-v2-a1b2c3", Execution: "local", CreatedAt: bubbleStart,
		Staging: "backfill/page_daily@v2/946684800000000/",
	}
}

func createJob(t *testing.T, m *meta.Meta, j meta.Job) meta.Job {
	t.Helper()
	created, err := m.CreateJob(t.Context(), j)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	return created
}

func readJob(t *testing.T, m *meta.Meta) meta.Job {
	t.Helper()
	j, ok, err := m.Job(t.Context(), version)
	if err != nil || !ok {
		t.Fatalf("Job = %t, %v; want the job", ok, err)
	}
	return j
}

// sameJob reports whether a and b agree on every exported field.
func sameJob(a, b meta.Job) bool {
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	for i := range va.NumField() {
		if va.Type().Field(i).IsExported() && !reflect.DeepEqual(va.Field(i).Interface(), vb.Field(i).Interface()) {
			return false
		}
	}
	return true
}

// The stored layout is part of the storage format.
func TestJobLayout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		bk := mem.New()
		m := meta.New(bk, table)
		j := tableJob()
		j.State, j.Phase = meta.JobDone, meta.PhaseLive
		j.ReadAfter, j.ScannedAt = bubbleStart.Add(1650*time.Second), bubbleStart.Add(1700*time.Second)
		j.Stage = meta.Stage{Segments: 4, Next: map[int]store.Key{1: {PK: "pk1", SK: "sk1"}}, Finished: []int{0, 2}}
		j.Runs = []meta.Run{{Name: "runs/1-abc", Records: 3, First: mark1, Last: mark2}}
		j.Merge = &meta.Merge{Inputs: []string{"runs/1-a", "runs/1-b"}, Outputs: []meta.Run{{Name: "runs/1-m", Records: 2, First: mark1, Last: mark1}}, Mark: &mark1}
		j.Staged, j.Mark, j.Applied, j.Skipped, j.Chunks, j.Rate, j.Paused = 5, &mark2, 3, 2, 1, 100, true
		j.Error = &meta.JobError{Phase: meta.PhaseStage, Message: "throttled", At: bubbleStart.Add(time.Second)}
		created := createJob(t, m, j)

		m1, m2 := b(0, 0, 0, 0, 0, 0, 0, 1, 7), b(0, 0, 0, 0, 0, 0, 0, 0x1f, 0x01, 0x2c)
		wantItem(t, bk, store.Item{
			"pk":         s("job"),
			"sk":         s(version),
			"stream":     s(stream),
			"source":     s("table"),
			"split":      s("time"),
			"state":      s("done"),
			"phase":      s("live"),
			"attempt":    n("1"),
			"name":       s("page_daily-v2-a1b2c3"),
			"execution":  s("local"),
			"created_at": n("946684800000000"),
			"updated_at": n("946684800000000"),
			"read_after": n("946686450000000"),
			"scanned_at": n("946686500000000"),
			"fresh":      {Type: store.BOOL, Bool: false},
			"staging":    s("backfill/page_daily@v2/946684800000000/"),
			"stage": {Type: store.M, Map: map[string]store.Value{
				"segments": n("4"),
				"next": {Type: store.M, Map: map[string]store.Value{
					"1": {Type: store.M, Map: map[string]store.Value{"pk": s("pk1"), "sk": s("sk1")}},
				}},
				"finished": {Type: store.L, List: []store.Value{n("0"), n("2")}},
			}},
			"runs": {Type: store.L, List: []store.Value{{Type: store.M, Map: map[string]store.Value{
				"name": s("runs/1-abc"), "records": n("3"), "first": m1, "last": m2,
			}}}},
			"merge": {Type: store.M, Map: map[string]store.Value{
				"inputs": {Type: store.L, List: []store.Value{s("runs/1-a"), s("runs/1-b")}},
				"outputs": {Type: store.L, List: []store.Value{{Type: store.M, Map: map[string]store.Value{
					"name": s("runs/1-m"), "records": n("2"), "first": m1, "last": m1,
				}}}},
				"mark": m1,
			}},
			"staged":  n("5"),
			"mark":    m2,
			"applied": n("3"),
			"skipped": n("2"),
			"chunks":  n("1"),
			"rate":    n("100"),
			"paused":  {Type: store.BOOL, Bool: true},
			"error": {Type: store.M, Map: map[string]store.Value{
				"phase": s("stage"), "message": s("throttled"), "at": n("946684801000000"),
			}},
			"_v":   n("1"),
			"_ttl": n("981248400"), // 2000-01-01 + 400 days + 3600 s
		})
		j.UpdatedAt = bubbleStart
		if read := readJob(t, m); !sameJob(read, j) || !reflect.DeepEqual(read, created) {
			t.Errorf("read back\n %+v\nwant\n %+v\nas created\n %+v", read, j, created)
		}
	})
}

func TestArchiveJobLayout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		bk := mem.New()
		m := meta.New(bk, table)
		j := tableJob()
		j.Split, j.Phase, j.Attempt, j.Execution = meta.SplitFence, meta.PhaseWatermark, 2, "arn:aws:states:execution:x"
		j.Cut = bubbleStart.Add(90 * time.Second)
		created := createJob(t, m, j)
		wantItem(t, bk, store.Item{
			"pk": s("job"), "sk": s(version), "stream": s(stream), "source": s("archive"), "split": s("fence"),
			"state": s("running"), "phase": s("watermark"), "attempt": n("2"),
			"name": s("page_daily-v2-a1b2c3"), "execution": s("arn:aws:states:execution:x"),
			"created_at": n("946684800000000"), "updated_at": n("946684800000000"), "cut": n("946684890000000"),
			"staging": s("backfill/page_daily@v2/946684800000000/"),
			"staged":  n("0"), "applied": n("0"), "skipped": n("0"), "chunks": n("0"), "rate": n("0"),
			"_v": n("1"),
		})
		if read := readJob(t, m); !reflect.DeepEqual(read, created) {
			t.Errorf("read %+v, want %+v", read, created)
		}
	})
}

// An archive source's stage stores the fields that aren't zero.
func TestArchiveStageLayout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		bk := mem.New()
		m := meta.New(bk, table)
		j := tableJob()
		j.Split, j.Phase, j.Cut = meta.SplitFence, meta.PhaseStage, bubbleStart.Add(90*time.Second)
		j.Archive = meta.ArchiveStage{
			Errors:    meta.ErrorPrefix{After: "errors/firehose/page_views/2000/01/01/00/a.gz", Done: true, Foreign: 2},
			PlanQuery: "q-plan",
			Plan:      "plan-1-0a1b",
			Export:    meta.Export{From: bubbleStart.Add(-24 * time.Hour), To: bubbleStart.Add(-12 * time.Hour), Try: 3, Failed: 1, ID: "q-slice"},
			Slice:     1,
			Query:     "q-slice",
			File:      4,
			Skip:      120,
			Rows:      5000,
			Scanned:   1 << 30,
			Tries:     2,
		}
		created := createJob(t, m, j)
		item, err := bk.Get(t.Context(), table, store.Key{PK: "job", SK: version}, true)
		if err != nil {
			t.Fatal(err)
		}
		want := store.Value{Type: store.M, Map: map[string]store.Value{
			"errors": {Type: store.M, Map: map[string]store.Value{
				"after": s("errors/firehose/page_views/2000/01/01/00/a.gz"), "done": {Type: store.BOOL, Bool: true}, "foreign": n("2"),
			}},
			"plan_query": s("q-plan"),
			"plan":       s("plan-1-0a1b"),
			"export": {Type: store.M, Map: map[string]store.Value{
				"from": n("946598400000000"), "to": n("946641600000000"), "try": n("3"), "failed": n("1"), "id": s("q-slice"),
			}},
			"slice": n("1"), "query": s("q-slice"), "file": n("4"), "skip": n("120"), "rows": n("5000"),
			"scanned": n("1073741824"), "tries": n("2"),
		}}
		if !reflect.DeepEqual(item["stage"], want) {
			t.Errorf("stage %+v, want %+v", item["stage"], want)
		}
		if read := readJob(t, m); !reflect.DeepEqual(read, created) || read.Archive != j.Archive {
			t.Errorf("read %+v, want %+v", read.Archive, j.Archive)
		}
	})
}

func TestCreateJob(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(t), table)
			created := createJob(t, m, tableJob())
			if read := readJob(t, m); !reflect.DeepEqual(read, created) {
				t.Errorf("read %+v, want %+v", read, created)
			}
			again := tableJob()
			again.Attempt = 2
			if _, err := m.CreateJob(t.Context(), again); !errors.Is(err, meta.ErrJobExists) {
				t.Errorf("a second CreateJob = %v, want ErrJobExists", err)
			}
			if read := readJob(t, m); read.Attempt != 1 {
				t.Errorf("attempt %d after a second CreateJob, want 1", read.Attempt)
			}
		})
	}
}

// A job the decoder would reject is never written.
func TestCreateJobRejectsWhatItCantReadBack(t *testing.T) {
	t.Parallel()
	tests := map[string]func(j *meta.Job){
		"attempt 0":              func(j *meta.Job) { j.Attempt = 0 },
		"no creation time":       func(j *meta.Job) { j.CreatedAt = time.Time{} },
		"a cut on a table job":   func(j *meta.Job) { j.Cut = bubbleStart },
		"fresh while running":    func(j *meta.Job) { j.Fresh = true },
		"a mark that isn't one":  func(j *meta.Job) { j.Mark = &keys.Mark{UnitKey: "u1", Seq: seq(1)} },
		"segment 4 of 4":         func(j *meta.Job) { j.Stage = meta.Stage{Segments: 4, Finished: []int{4}} },
		"an unknown phase":       func(j *meta.Job) { j.Phase = "export" },
		"negative records":       func(j *meta.Job) { j.Applied = -1 },
		"no execution":           func(j *meta.Job) { j.Execution = "" },
		"a split it doesn't use": func(j *meta.Job) { j.Split = "" },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(mem.New(), table)
			j := tableJob()
			change(&j)
			if _, err := m.CreateJob(t.Context(), j); err == nil {
				t.Error("CreateJob succeeded")
			}
			if _, ok, err := m.Job(t.Context(), version); ok || err != nil {
				t.Errorf("Job = %t, %v; want no job", ok, err)
			}
		})
	}
}

// setRate is a change that sets the rate; it finds a rate already set done, as a change must
// when the SDK retried its write.
func setRate(rate int) func(j *meta.Job) (bool, error) {
	return func(j *meta.Job) (bool, error) {
		if j.Rate == rate {
			return false, nil
		}
		j.Rate = rate
		return true, nil
	}
}

func TestUpdateJob(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(t), table)
			createJob(t, m, tableJob())
			updated, err := m.UpdateJob(t.Context(), version, setRate(50))
			if err != nil || updated.Rate != 50 {
				t.Fatalf("UpdateJob = %+v, %v; want rate 50", updated, err)
			}
			if read := readJob(t, m); !reflect.DeepEqual(read, updated) {
				t.Errorf("read %+v, want %+v", read, updated)
			}
		})
	}
}

func TestUpdateJobStampsOnlyWrites(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		m := meta.New(mem.New(), table)
		createJob(t, m, tableJob())
		time.Sleep(time.Minute)
		if j, err := m.UpdateJob(t.Context(), version, setRate(0)); err != nil || !j.UpdatedAt.Equal(bubbleStart) {
			t.Errorf("no change: %v, updated at %s; want %s", err, j.UpdatedAt, bubbleStart)
		}
		if j, err := m.UpdateJob(t.Context(), version, setRate(5)); err != nil || !j.UpdatedAt.Equal(bubbleStart.Add(time.Minute)) {
			t.Errorf("a change: %v, updated at %s; want %s", err, j.UpdatedAt, bubbleStart.Add(time.Minute))
		}
	})
}

// raceFirstPut runs race before its first Put, as a writer that wins the race would.
type raceFirstPut struct {
	store.Backend

	race func()
	once sync.Once
}

func (b *raceFirstPut) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	b.once.Do(b.race)
	return b.Backend.Put(ctx, table, it, c)
}

// An update that loses the race applies its change to the job the winner stored.
func TestUpdateJobAfterALostRace(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			stored := bk.new(t)
			m := meta.New(stored, table)
			createJob(t, m, tableJob())
			racing := &raceFirstPut{Backend: stored, race: func() {
				if _, err := m.UpdateJob(t.Context(), version, setRate(50)); err != nil {
					t.Error(err)
				}
			}}
			cancel := func(j *meta.Job) (bool, error) {
				if j.State == meta.JobCancelled {
					return false, nil
				}
				j.State = meta.JobCancelled
				return true, nil
			}
			if _, err := meta.New(racing, table).UpdateJob(t.Context(), version, cancel); err != nil {
				t.Fatal(err)
			}
			if j := readJob(t, m); j.State != meta.JobCancelled || j.Rate != 50 {
				t.Errorf("job %s at rate %d, want cancelled at 50", j.State, j.Rate)
			}
		})
	}
}

func TestUpdateJobErrors(t *testing.T) {
	t.Parallel()
	m := meta.New(mem.New(), table)
	if _, err := m.UpdateJob(t.Context(), version, setRate(5)); !errors.Is(err, meta.ErrNoJob) {
		t.Errorf("without a job: %v, want ErrNoJob", err)
	}
	createJob(t, m, tableJob())
	errChange := errors.New("change failed")
	if _, err := m.UpdateJob(t.Context(), version, func(*meta.Job) (bool, error) { return true, errChange }); !errors.Is(err, errChange) {
		t.Errorf("a failed change: %v, want its error", err)
	}
	rename := func(j *meta.Job) (bool, error) { j.Version = "other@v1"; return true, nil }
	if _, err := m.UpdateJob(t.Context(), version, rename); err == nil {
		t.Error("a change that renames the job succeeded")
	}
	runs := func(j *meta.Job) (bool, error) {
		for i := range 8000 {
			j.Runs = append(j.Runs, meta.Run{Name: fmt.Sprintf("runs/1-%016x", i), Records: 1, First: mark1, Last: mark2})
		}
		return true, nil
	}
	const tooLarge = "over DynamoDB's item limit of 409600: its source is too large to stage (lifecycle.md §11)"
	if _, err := m.UpdateJob(t.Context(), version, runs); err == nil || !strings.HasSuffix(err.Error(), "with 8000 staged runs, "+tooLarge) {
		t.Errorf("8,000 runs: %v, want the item limit", err)
	}
	if jobs, err := m.Jobs(t.Context()); err != nil || len(jobs) != 1 || !reflect.DeepEqual(jobs[0], readJob(t, m)) {
		t.Errorf("Jobs = %v, %v; want the one job unchanged", jobs, err)
	}
}

func TestJobsListsOnlyJobs(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(t), table)
			activate(t, m)
			other := tableJob()
			other.Version = "other@v1"
			createJob(t, m, other)
			createJob(t, m, tableJob())
			jobs, err := m.Jobs(t.Context())
			if err != nil || len(jobs) != 2 || jobs[0].Version != "other@v1" || jobs[1].Version != version {
				t.Errorf("Jobs = %+v, %v; want other@v1 and %s", jobs, err, version)
			}
		})
	}
}

func TestDecodeRejectsForeignJobs(t *testing.T) {
	t.Parallel()
	job := func(attrs store.Item) store.Item {
		it := store.Item{
			"pk": s("job"), "sk": s(version), "stream": s(stream), "source": s("archive"), "split": s("fence"),
			"state": s("running"), "phase": s("fence"), "attempt": n("1"), "name": s("x"), "execution": s("local"),
			"created_at": n("1"), "updated_at": n("1"), "staging": s("p/"),
			"staged": n("0"), "applied": n("0"), "skipped": n("0"), "chunks": n("0"), "rate": n("0"), "_v": n("1"),
		}
		maps.Copy(it, attrs)
		return it
	}
	tableSourced := func(attrs store.Item) store.Item {
		it := store.Item{"source": s("table"), "split": s("none")}
		maps.Copy(it, attrs)
		return job(it)
	}
	done := store.Item{"state": s("done"), "fresh": {Type: store.BOOL}, "_ttl": n("1")}
	tests := []struct {
		name string
		item store.Item
		ok   bool
	}{
		{"an archive job", job(nil), true},
		{"a done table job", tableSourced(done), true},
		{"a table source split by fences", job(store.Item{"source": s("table")}), false},
		{"an unknown state", job(store.Item{"state": s("paused")}), false},
		{"attempt 0", job(store.Item{"attempt": n("0")}), false},
		{"read_after on an archive job", job(store.Item{"read_after": n("1")}), false},
		{"a stage on an archive job", job(store.Item{"stage": {Type: store.M, Map: map[string]store.Value{"segments": n("1")}}}), false},
		{"a cut on a table job", tableSourced(store.Item{"cut": n("1")}), false},
		{"fresh while running", job(store.Item{"fresh": {Type: store.BOOL}}), false},
		{"done without fresh", tableSourced(store.Item{"state": s("done"), "_ttl": n("1")}), false},
		{"done without _ttl", tableSourced(store.Item{"state": s("done"), "fresh": {Type: store.BOOL}}), false},
		{"_ttl while running", job(store.Item{"_ttl": n("1")}), false},
		{"an empty name", job(store.Item{"name": s("")}), false},
		{"a mark of one byte", job(store.Item{"mark": b(7)}), false},
		{"a run without its last mark", job(store.Item{"runs": {Type: store.L, List: []store.Value{{Type: store.M, Map: map[string]store.Value{
			"name": s("r"), "records": n("1"), "first": b(0, 0, 0, 0, 0, 0, 0, 1, 7),
		}}}}}), false},
		{"segment 2 of 2", tableSourced(store.Item{"stage": {Type: store.M, Map: map[string]store.Value{
			"segments": n("2"), "finished": {Type: store.L, List: []store.Value{n("2")}},
		}}}), false},
		{"segment 01", tableSourced(store.Item{"stage": {Type: store.M, Map: map[string]store.Value{
			"segments": n("2"), "next": {Type: store.M, Map: map[string]store.Value{
				"01": {Type: store.M, Map: map[string]store.Value{"pk": s("a"), "sk": s("b")}},
			}},
		}}}), false},
		{"a merge without inputs", job(store.Item{"merge": {Type: store.M, Map: map[string]store.Value{}}}), false},
		{"an error in an unknown phase", job(store.Item{"error": {Type: store.M, Map: map[string]store.Value{
			"phase": s("export"), "message": s("m"), "at": n("1"),
		}}}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bk := mem.New()
			if err := bk.Put(t.Context(), table, tt.item, store.Cond{}); err != nil {
				t.Fatal(err)
			}
			_, ok, err := meta.New(bk, table).Job(t.Context(), version)
			if tt.ok != (err == nil) || tt.ok != ok {
				t.Errorf("Job = %t, %v; want ok=%t", ok, err, tt.ok)
			}
		})
	}
}

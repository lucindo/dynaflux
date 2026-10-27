// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/infra/cloudformation"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

var backfillRegions = []string{"us-east-1", "us-west-2"} //nolint:gochecknoglobals // test fixture

// backfillStore is testdata/backfill.txtar deployed in us-east-1 and us-west-2 in memory,
// with page_counts@v1 live in us-east-1 and an event of page_views in each region. signups'
// table is new in us-east-1; every other stream table is three days old. Make it in a
// synctest bubble, whose clock reads 2000-01-01T00:00:00Z.
type backfillStore struct {
	dir      string
	backends map[string]store.Backend
	metas    map[string]*meta.Meta
	sfn      map[string]*fakeSFN
	tables   map[string]*fakeReplicas
}

func newBackfillStore(t *testing.T) *backfillStore {
	t.Helper()
	ctx := t.Context()
	dir := writeProject(t, "backfill")
	res := compile(t, dir)
	lock, err := catalog.MarshalLock(res.Lock)
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, catalog.LockFileName), lock, 0o644)
	}
	if err != nil {
		t.Fatal(err)
	}
	data, err := catalog.Marshal(res.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	cat, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	s := &backfillStore{dir: dir, backends: map[string]store.Backend{}, metas: map[string]*meta.Meta{},
		sfn: map[string]*fakeSFN{}, tables: map[string]*fakeReplicas{}}
	naming := catalog.Naming{Store: "shop"}
	old := time.Now().Add(-72 * time.Hour)
	for _, r := range backfillRegions {
		b := mem.New()
		s.backends[r], s.metas[r] = b, meta.New(b, naming.MetaTable())
		s.sfn[r] = &fakeSFN{status: map[string]types.ExecutionStatus{}}
		s.tables[r] = &fakeReplicas{born: map[string]time.Time{naming.StreamTable("page_views"): old, naming.StreamTable("signups"): old}}
		if _, err = s.metas[r].RegisterCatalog(ctx, data, "v0.5.0"); err != nil {
			t.Fatal(err)
		}
		e := engine.New(b, engine.Options{Region: r})
		out, aerr := e.Append(ctx, cat, "page_views", [][]expr.Value{{expr.String("u1"), expr.String("/home"), expr.Null()}}, engine.AppendMode{})
		if aerr == nil {
			aerr = out[0].Err
		}
		if aerr != nil {
			t.Fatal(aerr)
		}
	}
	s.tables["us-east-1"].born[naming.StreamTable("signups")] = time.Now()
	s.live(t, "us-east-1", "page_counts@v1", "page_views")
	return s
}

func (s *backfillStore) live(t *testing.T, region, version, stream string) {
	t.Helper()
	m := s.metas[region]
	if _, err := m.Activate(t.Context(), version, stream, meta.SplitTime, meta.DefaultBounds); err != nil {
		t.Fatal(err)
	}
	if err := m.MakeLive(t.Context(), version, time.Time{}); err != nil {
		t.Fatal(err)
	}
}

// job gives page_seen@v1 a running job in us-east-1 whose execution
// page-seen-v1-0a1b2c-a1 has status, or doesn't exist when status is "".
func (s *backfillStore) job(t *testing.T, status types.ExecutionStatus) {
	t.Helper()
	const region, name = "us-east-1", "page-seen-v1-0a1b2c"
	arn := s.sfn[region].arn(region, name+"-a1")
	if _, err := backfill.Start(t.Context(), s.metas[region], backfill.Request{
		Version: "page_seen@v1", Stream: "page_views", Name: name, Execution: func(int) string { return arn }, Split: meta.SplitNone,
	}); err != nil {
		t.Fatal(err)
	}
	if status != "" {
		s.sfn[region].status[arn] = status
	}
}

func (s *backfillStore) run(t *testing.T, stdin string, terminal bool, args ...string) result {
	t.Helper()
	return s.runContext(t.Context(), stdin, terminal, args...)
}

func (s *backfillStore) runContext(ctx context.Context, stdin string, terminal bool, args ...string) result {
	var stdout, stderr strings.Builder
	env := cli.Env{
		Region: "us-east-1", Account: account, Terminal: terminal, Backends: s.backends,
		StepFunctions: map[string]cli.StepFunctions{}, Tables: map[string]backfill.Tables{},
	}
	for _, r := range backfillRegions {
		env.StepFunctions[r], env.Tables[r] = s.sfn[r], s.tables[r]
	}
	status := cli.RunEnv(ctx, append([]string{"backfill"}, args...), strings.NewReader(stdin), &stdout, &stderr, s.dir, env)
	return result{status, stdout.String(), stderr.String()}
}

// fakeSFN is a region's Step Functions: executions by ARN, which a start makes running and a
// stop aborted. It keeps what was started and stopped.
type fakeSFN struct {
	mu      sync.Mutex
	status  map[string]types.ExecutionStatus
	started []string // name and input
	stopped []string // ARNs
	fail    error    // DescribeExecution's
}

func (f *fakeSFN) arn(region, name string) string {
	return "arn:aws:states:" + region + ":" + account + ":execution:shop-backfill:" + name
}

func (f *fakeSFN) StartExecution(_ context.Context, in *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	arn := strings.Replace(aws.ToString(in.StateMachineArn), ":stateMachine:", ":execution:", 1) + ":" + aws.ToString(in.Name)
	f.status[arn] = types.ExecutionStatusRunning
	f.started = append(f.started, aws.ToString(in.Name)+" "+aws.ToString(in.Input))
	return &sfn.StartExecutionOutput{ExecutionArn: &arn}, nil
}

func (f *fakeSFN) DescribeExecution(_ context.Context, in *sfn.DescribeExecutionInput, _ ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return nil, f.fail
	}
	status, ok := f.status[aws.ToString(in.ExecutionArn)]
	if !ok {
		return nil, &types.ExecutionDoesNotExist{Message: aws.String("Execution Does Not Exist: '" + aws.ToString(in.ExecutionArn) + "'")}
	}
	return &sfn.DescribeExecutionOutput{ExecutionArn: in.ExecutionArn, Status: status}, nil
}

func (f *fakeSFN) StopExecution(_ context.Context, in *sfn.StopExecutionInput, _ ...func(*sfn.Options)) (*sfn.StopExecutionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, aws.ToString(in.ExecutionArn))
	f.status[aws.ToString(in.ExecutionArn)] = types.ExecutionStatusAborted
	return &sfn.StopExecutionOutput{}, nil
}

// fakeReplicas gives each stream table its creation time.
type fakeReplicas struct{ born map[string]time.Time }

func (f *fakeReplicas) Born(_ context.Context, table string) (time.Time, bool, error) {
	return f.born[table], false, nil
}

// backfillCase is a run of backfill on the deployed store.
type backfillCase struct {
	name     string
	args     []string
	stdin    string
	terminal bool
	status   int
	change   func(t *testing.T, s *backfillStore)
	// progress moves the jobs while backfill runs, as their executions would.
	progress func(t *testing.T, s *backfillStore)
	// interrupt, when set, interrupts the run after it.
	interrupt time.Duration
	check     func(t *testing.T, s *backfillStore)
}

// backfillCases are TestBackfill's runs. Each one's output is in testdata/backfill.txtar.
func backfillCases() []backfillCase {
	east := []string{"-region", "us-east-1"}
	return []backfillCase{
		{name: "all", args: []string{"-all", "-yes"}, check: func(t *testing.T, s *backfillStore) {
			t.Helper()
			checkStarted(t, s, "us-east-1",
				`page_seen-v1-`, `{"job":"page_seen@v1","attempt":1,"name":"page_seen-v1-`,
				`plan_counts-v1-`, `{"job":"plan_counts@v1","attempt":1,"name":"plan_counts-v1-`)
			checkJob(t, s, "us-east-1", "plan_counts@v1", meta.SplitFence, 1)
			checkJob(t, s, "us-west-2", "page_counts@v1", meta.SplitTime, 1)
			checkJob(t, s, "us-west-2", "page_seen@v1", meta.SplitNone, 1)
		}},
		{name: "named", args: slices.Concat([]string{"page_seen", "-yes", "-rate", "500"}, east), check: func(t *testing.T, s *backfillStore) {
			t.Helper()
			if j := checkJob(t, s, "us-east-1", "page_seen@v1", meta.SplitNone, 1); j.Rate != 500 {
				t.Errorf("rate %d, want 500", j.Rate)
			}
			checkNoJob(t, s, "us-west-2", "page_seen@v1")
		}},
		{name: "declined", args: []string{"-all"}, stdin: "n\n", terminal: true, status: 1, check: func(t *testing.T, s *backfillStore) {
			t.Helper()
			checkNoJob(t, s, "us-east-1", "page_seen@v1")
		}},
		{name: "no terminal", args: []string{"-all"}, status: 2, check: func(t *testing.T, s *backfillStore) {
			t.Helper()
			checkNoJob(t, s, "us-west-2", "page_seen@v1")
		}},
		{
			name: "running", args: slices.Concat([]string{"page_seen@v1", "-rate", "100"}, east),
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				s.job(t, types.ExecutionStatusRunning)
			},
			check: func(t *testing.T, s *backfillStore) {
				t.Helper()
				if j := checkJob(t, s, "us-east-1", "page_seen@v1", meta.SplitNone, 1); j.Rate != 100 {
					t.Errorf("rate %d, want 100", j.Rate)
				}
				checkStarted(t, s, "us-east-1")
			},
		},
		{
			name: "interrupted", args: slices.Concat([]string{"page_seen@v1"}, east),
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				s.job(t, types.ExecutionStatusFailed)
			},
			check: func(t *testing.T, s *backfillStore) {
				t.Helper()
				checkJob(t, s, "us-east-1", "page_seen@v1", meta.SplitNone, 2)
				checkStarted(t, s, "us-east-1", "page-seen-v1-0a1b2c-a2",
					`{"job":"page_seen@v1","attempt":2,"name":"page-seen-v1-0a1b2c","k":0,"steps":0}`)
			},
		},
		{
			name: "never started", args: slices.Concat([]string{"page_seen@v1"}, east),
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				s.job(t, "")
			},
		},
		{
			name: "continuation running", args: slices.Concat([]string{"page_seen@v1"}, east),
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				s.job(t, types.ExecutionStatusSucceeded)
				s.sfn["us-east-1"].status[s.sfn["us-east-1"].arn("us-east-1", "page-seen-v1-0a1b2c-a1-c1")] = types.ExecutionStatusRunning
			},
		},
		{
			name: "continuation missing", args: slices.Concat([]string{"page_seen@v1"}, east),
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				s.job(t, types.ExecutionStatusSucceeded)
			},
		},
		{
			name: "cancel", args: slices.Concat([]string{"-cancel", "page_seen@v1", "plan_counts@v1"}, east), status: 1,
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				s.job(t, types.ExecutionStatusRunning)
			},
			check: func(t *testing.T, s *backfillStore) {
				t.Helper()
				j, _, err := s.metas["us-east-1"].Job(t.Context(), "page_seen@v1")
				if err != nil || j.State != meta.JobCancelled {
					t.Errorf("job %+v, %v; want cancelled", j, err)
				}
				if want := []string{s.sfn["us-east-1"].arn("us-east-1", "page-seen-v1-0a1b2c-a1")}; !slices.Equal(s.sfn["us-east-1"].stopped, want) {
					t.Errorf("stopped %q, want %q", s.sfn["us-east-1"].stopped, want)
				}
			},
		},
		{
			name: "all skips a cancelled job", args: slices.Concat([]string{"-all", "-yes"}, east),
			change: cancelledJob,
		},
		{
			name: "cancelled job named", args: slices.Concat([]string{"page_seen@v1"}, east),
			change: cancelledJob,
		},
		{
			name: "broken", args: []string{"page_seen@v1", "-yes"}, status: 1,
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				f := meta.Failure{Cause: meta.CauseRecord, EventID: "e1", Error: "boom", At: time.Now()}
				if _, _, err := s.metas["us-west-2"].MarkBroken(t.Context(), "page_seen@v1", "page_views", f); err != nil {
					t.Fatal(err)
				}
			},
		},
		{name: "not in the lock", args: []string{"page_seen@v2"}, status: 2},
		{name: "not deployed yet", args: []string{"page_seen@v2"}, status: 1, change: rebuiltInTheLock},
		{
			name: "region not deployed", args: []string{"page_seen", "-yes"}, status: 1,
			change: func(_ *testing.T, s *backfillStore) { s.backends["us-west-2"] = mem.New() },
		},
		{name: "archive not fresh", args: []string{"-source", "archive", "page_counts", "-region", "us-west-2"}, status: 1},
		{name: "archive of a fresh stream", args: slices.Concat([]string{"-source", "archive", "plan_counts", "-yes"}, east)},
		{name: "table of a fresh stream", args: slices.Concat([]string{"-source", "table", "plan_counts", "-yes"}, east)},
		{
			name: "source of an activating version", args: slices.Concat([]string{"-source", "table", "plan_counts"}, east), status: 2,
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				if _, err := s.metas["us-east-1"].Activate(t.Context(), "plan_counts@v1", "signups", meta.SplitFence, meta.DefaultBounds); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "activating without a job", args: slices.Concat([]string{"plan_counts", "-yes"}, east),
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				if _, err := s.metas["us-east-1"].Activate(t.Context(), "plan_counts@v1", "signups", meta.SplitTime, meta.DefaultBounds); err != nil {
					t.Fatal(err)
				}
			},
			check: func(t *testing.T, s *backfillStore) {
				t.Helper()
				checkJob(t, s, "us-east-1", "plan_counts@v1", meta.SplitTime, 1)
			},
		},
		{name: "live", args: slices.Concat([]string{"page_counts"}, east)},
		{name: "no versions", args: nil, status: 2},
		{name: "versions and all", args: []string{"-all", "page_seen"}, status: 2},
		{name: "cancel with rate", args: []string{"-cancel", "-rate", "5", "page_seen"}, status: 2},
		{name: "unknown source", args: []string{"-source", "s3", "page_seen"}, status: 2},
		{name: "not a version", args: []string{"page_seen@2"}, status: 2},
		{name: "nothing to cancel", args: []string{"-all", "-cancel"}},
		{name: "unknown region", args: []string{"page_seen", "-region", "eu-west-1"}, status: 2},
		{name: "unknown view", args: []string{"page_sen"}, status: 2},
	}
}

// backfillWatchCases are TestBackfill's runs with -watch, which follows the jobs until each
// ends, with a line when one moves, and of jobs that wait or resume.
func backfillWatchCases() []backfillCase {
	east := []string{"-region", "us-east-1"}
	return []backfillCase{
		{name: "watch", args: slices.Concat([]string{"page_seen", "-yes", "-watch"}, east), progress: watchLive},
		{name: "watch failed execution", args: slices.Concat([]string{"page_seen", "-yes", "-watch"}, east), status: 1, progress: watchFail},
		{name: "watch through a failed read", args: slices.Concat([]string{"page_seen", "-yes", "-watch"}, east), progress: watchFailedRead},
		{name: "watch json", args: slices.Concat([]string{"page_seen", "-yes", "-watch", "-json"}, east), progress: watchLive},
		{name: "json without watch", args: []string{"page_seen", "-json"}, status: 2},
		{name: "watch interrupted", args: slices.Concat([]string{"page_seen", "-yes", "-watch"}, east), status: 1, interrupt: 12 * time.Second},
		{
			// us-west-2 refuses page_seen@v1, and us-east-1's job is still followed.
			name: "watch with a refusal", args: []string{"page_seen", "-yes", "-watch"}, status: 1, progress: watchLive,
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				f := meta.Failure{Cause: meta.CauseRecord, EventID: "e1", Error: "boom", At: time.Now()}
				if _, _, err := s.metas["us-west-2"].MarkBroken(t.Context(), "page_seen@v1", "page_views", f); err != nil {
					t.Fatal(err)
				}
			},
		},
		{name: "job waits", args: slices.Concat([]string{"plan_counts"}, east), status: 1, change: waitingJob},
		{name: "all skips a job that waits", args: slices.Concat([]string{"-all", "-yes"}, east), change: waitingJob},
		{
			name: "source of a running job", args: slices.Concat([]string{"page_seen", "-source", "archive"}, east), status: 2,
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				s.job(t, types.ExecutionStatusRunning)
			},
		},
		{
			// plan_counts@v1's job was made for a fresh signups, and cancelled before it activated;
			// signups has an event now, so the job resumes from the table.
			name: "resume before activation", args: slices.Concat([]string{"plan_counts"}, east),
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				m := s.metas["us-east-1"]
				if _, err := backfill.Start(t.Context(), m, backfill.Request{
					Version: "plan_counts@v1", Stream: "signups", Name: "plan-counts-v1-0a1b2c", Execution: backfill.LocalExecution, Split: meta.SplitFence,
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := backfill.Cancel(t.Context(), m, "plan_counts@v1"); err != nil {
					t.Fatal(err)
				}
				s.tables["us-east-1"].born[catalog.Naming{Store: "shop"}.StreamTable("signups")] = time.Now().Add(-72 * time.Hour)
			},
			check: func(t *testing.T, s *backfillStore) {
				t.Helper()
				checkJob(t, s, "us-east-1", "plan_counts@v1", meta.SplitTime, 2)
			},
		},
		{
			name: "no lock", args: []string{"page_seen"}, status: 2,
			change: func(t *testing.T, s *backfillStore) {
				t.Helper()
				if err := os.Remove(filepath.Join(s.dir, catalog.LockFileName)); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
}

// TestBackfill runs backfill on the deployed store.
func TestBackfill(t *testing.T) {
	t.Parallel()
	g := readGoldens(t, "backfill")
	for _, tt := range slices.Concat(backfillCases(), backfillWatchCases()) {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) { tt.run(t, g) })
		})
	}
}

func (tt backfillCase) run(t *testing.T, g *goldens) {
	t.Helper()
	s := newBackfillStore(t)
	if tt.change != nil {
		tt.change(t, s)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if tt.progress != nil {
			tt.progress(t, s)
		}
	}()
	ctx := t.Context()
	if tt.interrupt > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, tt.interrupt)
		defer cancel()
	}
	got := s.runContext(ctx, tt.stdin, tt.terminal, tt.args...)
	<-done
	if got.status != tt.status {
		t.Errorf("status %d, want %d; stderr:\n%s", got.status, tt.status, got.stderr)
	}
	g.check(t, strings.ReplaceAll(tt.name, " ", "_"), got)
	if tt.check != nil {
		tt.check(t, s)
	}
}

// checkStarted checks the executions started in region, each by a prefix of its name and of
// its input.
func checkStarted(t *testing.T, s *backfillStore, region string, prefixes ...string) {
	t.Helper()
	started := s.sfn[region].started
	if len(started) != len(prefixes)/2 {
		t.Fatalf("started %q, want %d executions", started, len(prefixes)/2)
	}
	for i, st := range started {
		name, input, _ := strings.Cut(st, " ")
		if !strings.HasPrefix(name, prefixes[2*i]) || !strings.HasPrefix(input, prefixes[2*i+1]) {
			t.Errorf("started %s with %s, want %s… with %s…", name, input, prefixes[2*i], prefixes[2*i+1])
		}
	}
}

func checkJob(t *testing.T, s *backfillStore, region, version string, split meta.Split, attempt int) meta.Job {
	t.Helper()
	j, ok, err := s.metas[region].Job(t.Context(), version)
	if err != nil || !ok || j.State != meta.JobRunning || j.Split != split || j.Attempt != attempt {
		t.Errorf("%s: job %+v, %v, %v; want running with the %s split at attempt %d", region, j, ok, err, split, attempt)
	}
	return j
}

func checkNoJob(t *testing.T, s *backfillStore, region, version string) {
	t.Helper()
	if j, ok, err := s.metas[region].Job(t.Context(), version); err != nil || ok {
		t.Errorf("%s: job %+v, %v; want none", region, j, err)
	}
}

// watchLive applies part of page_seen@v1's records, then the rest, and makes it live.
func watchLive(t *testing.T, s *backfillStore) {
	t.Helper()
	m := s.metas["us-east-1"]
	time.Sleep(7 * time.Second)
	setJob(t, m, func(j *meta.Job) { j.Phase, j.Staged, j.Applied = meta.PhaseApply, 3000, 1000 })
	time.Sleep(5 * time.Second)
	setJob(t, m, func(j *meta.Job) { j.Applied = 2000 })
	time.Sleep(5 * time.Second)
	if _, err := m.Activate(t.Context(), "page_seen@v1", "page_views", meta.SplitNone, meta.DefaultBounds); err != nil {
		t.Error(err)
	}
	if err := m.MakeLive(t.Context(), "page_seen@v1", time.Time{}); err != nil {
		t.Error(err)
	}
	setJob(t, m, func(j *meta.Job) { j.State, j.Phase, j.Applied = meta.JobDone, meta.PhaseLive, 3000 })
}

// watchFailedRead fails Step Functions' reads for 35 seconds, then makes page_seen@v1 live.
func watchFailedRead(t *testing.T, s *backfillStore) {
	t.Helper()
	f := s.sfn["us-east-1"]
	f.mu.Lock()
	f.fail = errors.New("ThrottlingException: Rate exceeded")
	f.mu.Unlock()
	time.Sleep(35 * time.Second)
	f.mu.Lock()
	f.fail = nil
	f.mu.Unlock()
	time.Sleep(30 * time.Second)
	m := s.metas["us-east-1"]
	if _, err := m.Activate(t.Context(), "page_seen@v1", "page_views", meta.SplitNone, meta.DefaultBounds); err != nil {
		t.Error(err)
	}
	if err := m.MakeLive(t.Context(), "page_seen@v1", time.Time{}); err != nil {
		t.Error(err)
	}
	setJob(t, m, func(j *meta.Job) { j.State, j.Phase = meta.JobDone, meta.PhaseLive })
}

// watchFail fails page_seen@v1's execution.
func watchFail(t *testing.T, s *backfillStore) {
	t.Helper()
	time.Sleep(20 * time.Second)
	f := s.sfn["us-east-1"]
	f.mu.Lock()
	defer f.mu.Unlock()
	for arn := range f.status {
		f.status[arn] = types.ExecutionStatusFailed
	}
}

// waitingJob gives plan_counts@v1, activated in us-east-1 with the fence split, a job that
// waits for the archive read of a later release.
func waitingJob(t *testing.T, s *backfillStore) {
	t.Helper()
	m := s.metas["us-east-1"]
	if _, err := backfill.Start(t.Context(), m, backfill.Request{
		Version: "plan_counts@v1", Stream: "signups", Name: "plan-counts-v1-0a1b2c", Execution: backfill.LocalExecution, Split: meta.SplitFence,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Activate(t.Context(), "plan_counts@v1", "signups", meta.SplitFence, meta.DefaultBounds); err != nil {
		t.Fatal(err)
	}
	if _, err := m.UpdateJob(t.Context(), "plan_counts@v1", func(j *meta.Job) (bool, error) {
		j.Phase, j.Waits = meta.PhaseWatermark, meta.WaitArchive
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
}

// rebuiltInTheLock gives dynaflux.lock page_seen@v2, a rebuild that isn't deployed yet.
func rebuiltInTheLock(t *testing.T, s *backfillStore) {
	t.Helper()
	path := filepath.Join(s.dir, catalog.LockFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := catalog.ParseLock(data)
	if err != nil {
		t.Fatal(err)
	}
	v2 := lock.Views["page_seen"][0]
	v2.Number, v2.Rebuild = 2, 1
	if v2.Fingerprint, err = catalog.Fingerprint(&v2); err != nil {
		t.Fatal(err)
	}
	lock.Views["page_seen"][0].State = catalog.StateRetained
	lock.Views["page_seen"] = append([]catalog.ViewVersion{v2}, lock.Views["page_seen"]...)
	lock.Rebuilds = map[string]int{"page_seen": 1}
	if data, err = catalog.MarshalLock(lock); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// cancelledJob gives page_seen@v1 a cancelled job in us-east-1.
func cancelledJob(t *testing.T, s *backfillStore) {
	t.Helper()
	s.job(t, types.ExecutionStatusAborted)
	if _, err := backfill.Cancel(t.Context(), s.metas["us-east-1"], "page_seen@v1"); err != nil {
		t.Fatal(err)
	}
}

func setJob(t *testing.T, m *meta.Meta, change func(*meta.Job)) {
	t.Helper()
	if _, err := m.UpdateJob(t.Context(), "page_seen@v1", func(j *meta.Job) (bool, error) {
		change(j)
		return true, nil
	}); err != nil {
		t.Error(err)
	}
}

// An execution's name holds the view, its version and a hash of the time its job was made,
// within Step Functions' 80 characters with a 3-digit attempt and a 5-digit continuation; a
// hash of the version stands in for a view too long.
func TestExecutionNames(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	long := strings.Repeat("v", 62)
	tests := []struct{ version, want string }{
		{"page_daily@v2", "page_daily-v2-"},
		{strings.Repeat("v", 57) + "@v12", strings.Repeat("v", 57) + "-v12-"},
		{long + "@v12", ""},
	}
	for _, tt := range tests {
		got := cli.ExecutionName(tt.version, at)
		if len(got)+len("-a999-c99999") > 80 || tt.want != "" && !strings.HasPrefix(got, tt.want) || strings.HasPrefix(got, long) {
			t.Errorf("ExecutionName(%s) = %s (%d characters)", tt.version, got, len(got))
		}
	}
	if a, b := cli.ExecutionName("page_daily@v2", at), cli.ExecutionName("page_daily@v2", at.Add(time.Microsecond)); a == b {
		t.Errorf("jobs made at different times share the name %s", a)
	}
}

// The input of an execution that backfill starts carries every field of the input that the
// state machine of regional.yaml reads: JSONata reads a missing field as undefined, and a
// machine without steps would never continue.
func TestBackfillInputFields(t *testing.T) {
	t.Parallel()
	var input string
	synctest.Test(t, func(t *testing.T) {
		s := newBackfillStore(t)
		if got := s.run(t, "", false, "page_seen", "-region", "us-east-1", "-yes"); got.status != 0 || len(s.sfn["us-east-1"].started) != 1 {
			t.Fatalf("backfill: %+v", got)
		}
		_, input, _ = strings.Cut(s.sfn["us-east-1"].started[0], " ")
	})
	var fields map[string]any
	if err := json.Unmarshal([]byte(input), &fields); err != nil {
		t.Fatal(err)
	}
	res := compile(t, writeProject(t, "backfill"))
	m, err := infra.Build(res.Config, res.Catalog, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	files, err := cloudformation.Render(m, "v0.5.0")
	if err != nil {
		t.Fatal(err)
	}
	regional := string(files[cloudformation.RegionalTemplate])
	_, machine, found := strings.Cut(regional, "  BackfillStateMachine:")
	_, states, _ := strings.Cut(machine, "        States:")
	states, _, done := strings.Cut(states, "          Done:")
	if !found || !done {
		t.Fatalf("no state machine in\n%s", regional)
	}
	read := map[string]bool{}
	for _, f := range regexp.MustCompile(`\$states\.input\.(\w+)`).FindAllStringSubmatch(states, -1) {
		read[f[1]] = true
	}
	if len(read) == 0 {
		t.Fatalf("no field read in\n%s", states)
	}
	delete(read, "done") // each step's output sets these two
	delete(read, "wait")
	for f := range read {
		if _, ok := fields[f]; !ok {
			t.Errorf("the state machine reads %s, which the input %s lacks", f, input)
		}
	}
}

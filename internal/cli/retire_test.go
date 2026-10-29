// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

const retiring = "page_counts@v1"

// retireStore is testdata/retire.txtar deployed in us-east-1 and us-west-2 in memory, with
// page_counts@v1 retained and page_counts@v2 current. page_counts@v2 is live in both regions;
// page_counts@v1 is live in us-east-1 and pending in us-west-2.
type retireStore struct {
	dir      string
	backends map[string]store.Backend
	metas    map[string]*meta.Meta
	// first is the catalog that made page_counts@v1 current, and current the deployed one.
	first, current []byte
}

func newRetireStore(t *testing.T) *retireStore {
	t.Helper()
	ctx := t.Context()
	s := &retireStore{dir: writeProject(t, "retire"), backends: map[string]store.Backend{}, metas: map[string]*meta.Meta{}}
	s.first = s.generate(t, 1)
	s.current = s.generate(t, 2)
	naming := catalog.Naming{Store: "shop"}
	for _, r := range backfillRegions {
		b := mem.New()
		s.backends[r], s.metas[r] = b, meta.New(b, naming.MetaTable())
		_, err := s.metas[r].RegisterCatalog(ctx, s.current, "v0.5.0")
		must(t, err)
		makeLive(t, s.metas[r], "page_counts@v2")
	}
	makeLive(t, s.metas["us-east-1"], retiring)
	return s
}

// generate compiles the project with page_counts_v<n>.sql as the definition of page_counts,
// writes its dynaflux.lock as generate does, and returns its catalog.
func (s *retireStore) generate(t *testing.T, n int) []byte {
	t.Helper()
	sql, err := os.ReadFile(filepath.Join(s.dir, "page_counts_v"+strconv.Itoa(n)+".sql"))
	must(t, err)
	must(t, os.WriteFile(filepath.Join(s.dir, "dynaflux", "page_counts.sql"), sql, 0o644))
	res := compile(t, s.dir)
	lock, err := catalog.MarshalLock(res.Lock)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(s.dir, catalog.LockFileName), lock, 0o644))
	data, err := catalog.Marshal(res.Catalog)
	must(t, err)
	return data
}

func makeLive(t *testing.T, m *meta.Meta, version string) {
	t.Helper()
	_, err := m.Activate(t.Context(), version, "page_views", meta.SplitFence, meta.DefaultBounds)
	must(t, err)
	must(t, m.MakeLive(t.Context(), version, time.Time{}))
}

// retireLock marks page_counts@v1 retired in dynaflux.lock, as a run of retire for another
// environment did.
func (s *retireStore) retireLock(t *testing.T) {
	t.Helper()
	lock := s.lock(t)
	must(t, lock.Retire("page_counts", 1))
	data, err := catalog.MarshalLock(lock)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(s.dir, catalog.LockFileName), data, 0o644))
}

func (s *retireStore) lock(t *testing.T) *catalog.Lock {
	t.Helper()
	lock, err := catalog.ParseLock([]byte(s.lockFile(t)))
	must(t, err)
	return lock
}

func (s *retireStore) lockFile(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(s.dir, catalog.LockFileName))
	must(t, err)
	return string(data)
}

func (s *retireStore) run(t *testing.T, stdin string, terminal bool, args ...string) result {
	t.Helper()
	var stdout, stderr strings.Builder
	env := cli.Env{Region: "us-east-1", Account: account, Terminal: terminal, Backends: s.backends}
	status := cli.RunEnv(t.Context(), append([]string{"retire"}, args...), strings.NewReader(stdin), &stdout, &stderr, s.dir, env)
	return result{status, stdout.String(), stderr.String()}
}

// retiredIn returns the regions whose meta holds page_counts@v1 retired, of stream page_views.
func (s *retireStore) retiredIn(t *testing.T) []string {
	t.Helper()
	var regions []string
	for _, r := range backfillRegions {
		states, err := s.metas[r].ViewStates(t.Context())
		must(t, err)
		if vs := states[retiring]; vs.State == meta.Retired && vs.Stream == "page_views" {
			regions = append(regions, r)
		}
	}
	return regions
}

func (s *retireStore) register(t *testing.T, region string, data []byte) {
	t.Helper()
	_, err := s.metas[region].RegisterCatalog(t.Context(), data, "v0.5.0")
	must(t, err)
}

// The changes that make each check of retire fail, or pass.

// recreated re-creates us-west-2's regional stack: its meta table holds the deployed
// catalog, and no view state.
func recreated(t *testing.T, s *retireStore) {
	t.Helper()
	recreatedIn("us-west-2")(t, s)
}

func recreatedIn(region string) func(*testing.T, *retireStore) {
	return func(t *testing.T, s *retireStore) {
		t.Helper()
		b := mem.New()
		s.backends[region], s.metas[region] = b, meta.New(b, catalog.Naming{Store: "shop"}.MetaTable())
		s.register(t, region, s.current)
	}
}

func deployedCurrent(t *testing.T, s *retireStore) {
	t.Helper()
	s.register(t, "us-west-2", s.first)
}

func currentBroken(region string) func(*testing.T, *retireStore) {
	return func(t *testing.T, s *retireStore) {
		t.Helper()
		markBroken(t, s.metas[region], "page_counts@v2", "page_views", meta.Failure{Cause: meta.CauseRecord, EventID: "e1", Error: "boom", At: time.Now()})
	}
}

func runningJob(t *testing.T, s *retireStore) {
	t.Helper()
	_, err := backfill.Start(t.Context(), s.metas["us-west-2"], backfill.Request{
		Version: retiring, Stream: "page_views", Name: "page-counts-v1-0a1b2c", Execution: backfill.LocalExecution, Split: meta.SplitFence,
	})
	must(t, err)
}

// retireCase is a run of retire on the deployed store.
type retireCase struct {
	name     string
	args     []string
	stdin    string
	terminal bool
	status   int
	change   func(t *testing.T, s *retireStore)
	retired  []string // the regions that hold page_counts@v1 retired after the run
	// rerun runs it again as for another environment: dynaflux.lock already retires the
	// version, and the deployed catalogs still keep it.
	rerun bool
	// lockKept is a run that retires but leaves dynaflux.lock: it didn't check every region.
	lockKept bool
}

func retireCases() []retireCase {
	both := []string{"us-east-1", "us-west-2"}
	yes := []string{retiring, "-yes"}
	checks := []retireCase{
		{name: "retire", args: yes, retired: both},
		{
			name: "with query.yaml", args: yes, retired: both,
			change: func(t *testing.T, s *retireStore) {
				t.Helper()
				must(t, os.MkdirAll(filepath.Join(s.dir, "infra", "dynaflux"), 0o755))
				must(t, os.WriteFile(filepath.Join(s.dir, "infra", "dynaflux", "query.yaml"), nil, 0o644))
			},
		},
		{name: "deployed current", args: yes, status: 1, change: deployedCurrent},
		{name: "current not live", args: yes, status: 1, change: recreated},
		{name: "current broken", args: yes, status: 1, change: currentBroken("us-west-2")},
		{
			name: "broken version", args: yes, retired: both,
			change: func(t *testing.T, s *retireStore) {
				t.Helper()
				recreated(t, s)
				markBroken(t, s.metas["us-west-2"], retiring, "page_views", meta.Failure{Cause: meta.CauseRecord, EventID: "e2", Error: "boom", At: time.Now()})
			},
		},
		{name: "running job", args: yes, status: 1, change: runningJob},
		{
			name: "every refusal", args: yes, status: 1,
			change: func(t *testing.T, s *retireStore) {
				t.Helper()
				currentBroken("us-east-1")(t, s)
				runningJob(t, s)
				deployedCurrent(t, s)
			},
		},
	}
	cases := slices.Clone(checks)
	for _, c := range checks {
		c.name, c.rerun = "rerun "+c.name, true
		cases = append(cases, c)
	}
	return append(cases, []retireCase{
		{
			name: "already retired skips the checks", args: yes, retired: both,
			change: func(t *testing.T, s *retireStore) {
				t.Helper()
				_, err := s.metas["us-west-2"].Retire(t.Context(), retiring, "page_views")
				must(t, err)
				currentBroken("us-west-2")(t, s)
			},
		},
		{
			name: "not deployed", args: yes, retired: []string{"us-east-1"},
			change: func(t *testing.T, s *retireStore) {
				t.Helper()
				def, err := catalog.Unmarshal(s.current)
				must(t, err)
				def.Views[0].Versions = slices.DeleteFunc(def.Views[0].Versions, func(vv catalog.ViewVersion) bool { return vv.Number == 1 })
				def.Retired = []string{retiring}
				data, err := catalog.Marshal(def)
				must(t, err)
				s.register(t, "us-west-2", data)
			},
		},
		{
			name: "everything retired", args: []string{retiring}, retired: both, rerun: true,
			change: func(t *testing.T, s *retireStore) {
				t.Helper()
				for _, m := range s.metas {
					_, err := m.Retire(t.Context(), retiring, "page_views")
					must(t, err)
				}
			},
		},
		{name: "one region", args: []string{retiring, "-yes", "-region", "us-east-1"}, retired: []string{"us-east-1"}, lockKept: true},
		// DynamoDB Local serves every region from one database, so retire checks them all.
		{name: "on DynamoDB Local", args: []string{retiring, "-yes", "-endpoint", "http://127.0.0.1:8000"}, retired: []string{"us-east-1"}},
		{
			name: "current not live on DynamoDB Local", args: []string{retiring, "-yes", "-endpoint", "http://127.0.0.1:8000"}, status: 1,
			change: recreatedIn("us-east-1"),
		},
		{
			name: "every region after one", args: yes, retired: both,
			change: func(t *testing.T, s *retireStore) {
				t.Helper()
				if got := s.run(t, "", false, retiring, "-yes", "-region", "us-west-2"); got.status != 0 {
					t.Fatalf("retire in us-west-2: %+v", got)
				}
			},
		},
		{name: "declined", args: []string{retiring}, stdin: "n\n", terminal: true, status: 1},
		{name: "no terminal", args: []string{retiring}, status: 2},
		{
			name: "region not deployed", args: yes, status: 1,
			change: func(_ *testing.T, s *retireStore) { s.backends["us-west-2"] = mem.New() },
		},
		{name: "current in the lock", args: []string{"page_counts@v2", "-yes"}, status: 1},
		{name: "not in the lock", args: []string{"page_counts@v3", "-yes"}, status: 1},
		{name: "not a version", args: []string{"page_counts", "-yes"}, status: 2},
		{name: "two versions", args: []string{retiring, "page_counts@v2", "-yes"}, status: 2},
	}...)
}

// TestRetire runs retire on the deployed store: each check on a first run and on a re-run
// for another environment, whose dynaflux.lock already retires the version. A run that
// refuses writes nothing; one that retires writes the tombstones, then the lock's slim entry.
func TestRetire(t *testing.T) {
	t.Parallel()
	g := readGoldens(t, "retire")
	for _, tt := range retireCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, g)
		})
	}
}

func (tt retireCase) check(t *testing.T, g *goldens) {
	t.Helper()
	s := newRetireStore(t)
	if tt.rerun {
		s.retireLock(t)
	}
	if tt.change != nil {
		tt.change(t, s)
	}
	before := s.lockFile(t)
	got := s.run(t, tt.stdin, tt.terminal, tt.args...)
	if got.status != tt.status {
		t.Errorf("status %d, want %d; stderr:\n%s", got.status, tt.status, got.stderr)
	}
	g.check(t, strings.ReplaceAll(tt.name, " ", "_"), got)
	if regions := s.retiredIn(t); !slices.Equal(regions, tt.retired) {
		t.Errorf("%s is retired in %q, want %q", retiring, regions, tt.retired)
	}
	after := s.lockFile(t)
	switch {
	case !tt.rerun && tt.status == 0 && !tt.lockKept:
		g.checkFile(t, catalog.LockFileName, after)
	case after != before:
		t.Errorf("%s changed:\n%s", catalog.LockFileName, after)
	}
}

// A retired number never comes back: generate gives page_counts@v1's definition, retired,
// a new number.
func TestGenerateAfterRetire(t *testing.T) {
	t.Parallel()
	s := newRetireStore(t)
	if got := s.run(t, "", false, retiring, "-yes"); got.status != 0 {
		t.Fatalf("retire: %+v", got)
	}
	s.generate(t, 1)
	var got []string
	fingerprints := map[int]string{}
	for _, vv := range s.lock(t).Views["page_counts"] {
		got = append(got, catalog.VersionID("page_counts", vv.Number)+" "+string(vv.State))
		fingerprints[vv.Number] = vv.Fingerprint
	}
	if want := []string{"page_counts@v3 current", "page_counts@v2 retained", "page_counts@v1 retired"}; !slices.Equal(got, want) {
		t.Errorf("dynaflux.lock holds %q, want %q", got, want)
	}
	if fingerprints[3] != fingerprints[1] {
		t.Errorf("page_counts@v3's fingerprint %s, want page_counts@v1's %s", fingerprints[3], fingerprints[1])
	}
}

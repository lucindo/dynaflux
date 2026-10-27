// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// diffStore is testdata/diff.txtar numbered in dynaflux.lock and deployed in memory to
// us-east-1 and us-west-2, every version live. Make it in a synctest bubble.
type diffStore struct {
	dir      string
	deployed *catalog.Catalog // the catalog deployed, for a case to change and deploy again
	backends map[string]store.Backend
	metas    map[string]*meta.Meta
}

func newDiffStore(t *testing.T) *diffStore {
	t.Helper()
	s := &diffStore{dir: writeProject(t, "diff"), backends: map[string]store.Backend{}, metas: map[string]*meta.Meta{}}
	res := compile(t, s.dir)
	s.deployed = res.Catalog
	s.writeLock(t, res.Lock)
	for _, r := range backfillRegions {
		b := mem.New()
		s.backends[r], s.metas[r] = b, meta.New(b, shop.MetaTable())
		s.deploy(t, r)
		for id, vv := range s.deployed.Versions() {
			s.live(t, r, id, vv.Stream)
		}
	}
	return s
}

func (s *diffStore) writeLock(t *testing.T, lock *catalog.Lock) {
	t.Helper()
	data, err := catalog.MarshalLock(lock)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(s.dir, catalog.LockFileName), data, 0o644))
}

// deploy registers s.deployed in region.
func (s *diffStore) deploy(t *testing.T, region string) {
	t.Helper()
	data, err := catalog.Marshal(s.deployed)
	must(t, err)
	_, err = s.metas[region].RegisterCatalog(t.Context(), data, "v0.5.0")
	must(t, err)
}

func (s *diffStore) live(t *testing.T, region, version, stream string) {
	t.Helper()
	m := s.metas[region]
	_, err := m.Activate(t.Context(), version, stream, meta.SplitNone, meta.DefaultBounds)
	must(t, err)
	must(t, m.MakeLive(t.Context(), version, time.Time{}))
}

// changeViews changes the SQL of page_counts and page_seen, so that generate makes v2
// current for both, and retires page_seen@v1 in dynaflux.lock.
func (s *diffStore) changeViews(t *testing.T) {
	t.Helper()
	path := filepath.Join(s.dir, "dynaflux", "shop.sql")
	src, err := os.ReadFile(path)
	must(t, err)
	changed := strings.NewReplacer("count() AS views", "count() AS views, uniq(user_id) AS users", "max(user_id)", "min(user_id)").Replace(string(src))
	must(t, os.WriteFile(path, []byte(changed), 0o644))
	lock := compile(t, s.dir).Lock
	must(t, lock.Retire("page_seen", 1))
	s.writeLock(t, lock)
}

func (s *diffStore) run(t *testing.T, args ...string) result {
	t.Helper()
	var stdout, stderr strings.Builder
	sfn := map[string]cli.StepFunctions{}
	for r := range s.backends {
		sfn[r] = &fakeSFN{status: map[string]types.ExecutionStatus{}}
	}
	status := cli.RunEnv(t.Context(), append([]string{"diff"}, args...), nil, &stdout, &stderr, s.dir,
		cli.Env{Region: "us-east-1", Account: account, Backends: s.backends, StepFunctions: sfn})
	return result{status, stdout.String(), stderr.String()}
}

// TestDiff runs diff on the project and the deployed store. Each case's output is in
// testdata/diff.txtar.
func TestDiff(t *testing.T) {
	t.Parallel()
	g := readGoldens(t, "diff")
	tests := []struct {
		name   string
		args   []string
		status int
		change func(t *testing.T, s *diffStore)
	}{
		{"same", nil, 0, nil},
		{"streams", nil, 1, func(t *testing.T, s *diffStore) {
			t.Helper()
			pv, _ := s.deployed.Stream("page_views")
			for i, c := range pv.Columns {
				switch c.Name {
				case "duration":
					pv.Columns[i].Type = catalog.Type{Kind: catalog.KindUInt64}
				case "source":
					d := *c.Default
					d.Value = "app"
					pv.Columns[i].Default = &d
				}
			}
			pv.Columns = slices.DeleteFunc(pv.Columns, func(c catalog.Column) bool { return c.Name == "tag" || c.Name == "country" })
			pv.Columns = append(pv.Columns, catalog.Column{Name: "referrer", Type: catalog.Type{Kind: catalog.KindString}})
			pv.PrimaryKey = []string{"user_id", "page"}
			su, _ := s.deployed.Stream("signups")
			su.Archive = &catalog.Archive{Regions: []string{"us-east-1", "us-west-2"}, Events: catalog.ArchiveEventsAll}
			legacy := *su
			legacy.Name = "legacy"
			s.deployed.Streams = append(s.deployed.Streams, legacy)
			s.deploy(t, "us-east-1")
			s.deploy(t, "us-west-2")
		}},
		{"versions", nil, 1, func(t *testing.T, s *diffStore) {
			t.Helper()
			s.changeViews(t)
		}},
		{"versions json", []string{"-json"}, 1, func(t *testing.T, s *diffStore) {
			t.Helper()
			s.changeViews(t)
		}},
		{"versions after the backfill", nil, 0, func(t *testing.T, s *diffStore) {
			t.Helper()
			s.changeViews(t)
			for _, r := range backfillRegions {
				s.live(t, r, "page_counts@v2", "page_views")
				s.live(t, r, "page_seen@v2", "page_views")
			}
		}},
		{"retiring while a backfill runs", []string{"-region", "us-east-1", "-prefix", ""}, 1, func(t *testing.T, s *diffStore) {
			t.Helper()
			b := mem.New()
			s.backends["us-east-1"], s.metas["us-east-1"] = b, meta.New(b, shop.MetaTable())
			s.deploy(t, "us-east-1")
			_, err := backfill.Start(t.Context(), s.metas["us-east-1"], backfill.Request{
				Version: "page_seen@v1", Stream: "page_views", Name: "page-seen-v1-0a1b2c", Execution: backfill.LocalExecution, Split: meta.SplitNone,
			})
			must(t, err)
			s.changeViews(t)
			s.live(t, "us-east-1", "page_seen@v2", "page_views")
		}},
		{"other definitions", nil, 1, func(t *testing.T, s *diffStore) {
			t.Helper()
			v, _ := s.deployed.View("plan_counts")
			v.Versions[0].Rebuild = 1
			fp, err := catalog.Fingerprint(&v.Versions[0])
			must(t, err)
			v.Versions[0].Fingerprint = fp
			gone := *v
			gone.Name = "gone"
			s.deployed.Views = append(s.deployed.Views, gone)
			s.deploy(t, "us-east-1")
			s.deploy(t, "us-west-2")
		}},
		{"retired in a region", nil, 1, func(t *testing.T, s *diffStore) {
			t.Helper()
			_, err := s.metas["us-west-2"].Retire(t.Context(), "plan_counts@v1", "signups")
			must(t, err)
		}},
		{"queries and prune window", nil, 0, func(t *testing.T, s *diffStore) {
			t.Helper()
			c := s.deployed
			legacy := c.Queries[slices.IndexFunc(c.Queries, func(q catalog.Query) bool { return q.Name == "PageViews" })]
			legacy.Name = "Legacy"
			c.Queries = slices.DeleteFunc(c.Queries, func(q catalog.Query) bool { return q.Name == "Plans" })
			c.Queries = append(c.Queries, legacy)
			c.Queries[0].Cardinality = catalog.Cardinality("one")
			c.PruneWindowSeconds = 1200
			s.deploy(t, "us-east-1")
			s.deploy(t, "us-west-2")
		}},
		{"regions apart", nil, 0, func(t *testing.T, s *diffStore) {
			t.Helper()
			s.deployed.Queries = slices.DeleteFunc(s.deployed.Queries, func(q catalog.Query) bool { return q.Name == "Plans" })
			s.deploy(t, "us-west-2")
		}},
		// us-east-1's catalog archives signups with every event in us-west-2 as well, which
		// us-west-2's own catalog doesn't: only us-west-2's judges a change there.
		{"archive in another group", nil, 0, func(t *testing.T, s *diffStore) {
			t.Helper()
			su, _ := s.deployed.Stream("signups")
			su.Archive = &catalog.Archive{Regions: []string{"us-east-1", "us-west-2"}, Events: catalog.ArchiveEventsAll}
			s.deploy(t, "us-east-1")
		}},
		{"not deployed", nil, 0, func(t *testing.T, s *diffStore) {
			t.Helper()
			s.backends["us-west-2"] = mem.New()
		}},
		{"newer catalog format", nil, 1, func(t *testing.T, s *diffStore) {
			t.Helper()
			_, err := s.metas["us-east-1"].RegisterCatalog(t.Context(), []byte(`{"format":2}`), "v9.0.0")
			must(t, err)
		}},
		{"unreachable", nil, 2, func(t *testing.T, s *diffStore) {
			t.Helper()
			s.backends["us-west-2"] = unreachable{s.backends["us-west-2"]}
		}},
		{"local", []string{"-endpoint", "http://127.0.0.1:8000"}, 0, nil},
		{"negative timeout", []string{"-timeout", "-1m"}, 2, nil},
		{"project with an error", nil, 1, func(t *testing.T, s *diffStore) {
			t.Helper()
			must(t, os.WriteFile(filepath.Join(s.dir, "dynaflux", "bad.sql"), []byte("CREATE STREAM;\n"), 0o644))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newDiffStore(t)
				if tt.change != nil {
					tt.change(t, s)
				}
				got := s.run(t, tt.args...)
				if got.status != tt.status {
					t.Errorf("status %d, want %d; stderr:\n%s", got.status, tt.status, got.stderr)
				}
				g.check(t, strings.ReplaceAll(tt.name, " ", "_"), got)
			})
		})
	}
}

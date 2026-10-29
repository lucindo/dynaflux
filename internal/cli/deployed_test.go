// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"cmp"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/txtar"
	"github.com/lucindo/dynaflux/internal/validate"
)

const (
	account = "123456789012"
	header  = "store shop  prefix (none)  account 123456789012  region us-east-1\n"
)

// deployed is testdata/deployed.txtar's store in memory, deployed in us-east-1: its catalog
// registered, page_daily@v1 live and user_pages@v1 pending, and four events projected.
type deployed struct {
	dir  string // the project
	b    store.Backend
	meta *meta.Meta
}

// newDeployed deploys the store in memory, in a synctest bubble, whose clock reads
// 2000-01-01T00:00:00Z.
func newDeployed(t *testing.T) *deployed {
	t.Helper()
	return deploy(t, writeProject(t, "deployed"), mem.New(), catalog.Naming{Store: "shop"})
}

// deploy deploys the store of the project in dir to the tables of b that naming names.
func deploy(t *testing.T, dir string, b store.Backend, naming catalog.Naming) *deployed {
	t.Helper()
	ctx := t.Context()
	d := &deployed{dir: dir, b: b, meta: meta.New(b, naming.MetaTable())}
	data, err := catalog.Marshal(compile(t, dir).Catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.meta.RegisterCatalog(ctx, data, "v0.5.0"); err != nil {
		t.Fatal(err)
	}
	if _, err = d.meta.Activate(ctx, "page_daily@v1", "page_views", meta.SplitFence, meta.DefaultBounds); err != nil {
		t.Fatal(err)
	}
	if err = d.meta.MakeLive(ctx, "page_daily@v1", time.Time{}); err != nil {
		t.Fatal(err)
	}
	cat, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	project(t, d.b, naming, cat)
	return d
}

// compile compiles the project in dir.
func compile(t *testing.T, dir string) *validate.Result {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	res, ds := validate.Compile(root.FS(), dir)
	if err = root.Close(); err != nil || res == nil {
		t.Fatalf("compile: %v %v", err, ds)
	}
	return res
}

// project appends four events to page_views and projects them, each in a batch.
func project(t *testing.T, b store.Backend, naming catalog.Naming, cat *engine.Catalog) {
	t.Helper()
	ctx := t.Context()
	e := engine.New(b, engine.Options{Region: "us-east-1", Prefix: naming.Prefix, Entropy: rand.NewChaCha8([32]byte{})})
	at := func(s string) expr.Value {
		tm, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatal(err)
		}
		return expr.Micros(tm.UnixMicro())
	}
	tags := func(ts ...string) expr.Value {
		vs := []expr.Value{}
		for _, s := range ts {
			vs = append(vs, expr.String(s))
		}
		return expr.Array(vs)
	}
	events := [][]expr.Value{
		{expr.String("u1"), expr.String("/home"), expr.Uint(100), tags("a", "b"), at("1999-12-31T10:00:00Z")},
		{expr.String("u2"), expr.String("/home"), expr.Uint(200), tags(), at("1999-12-31T11:00:00Z")},
		{expr.String("u1"), expr.String("/home"), expr.Uint(300), tags("it's"), at("1999-12-30T09:00:00Z")},
		{expr.String("u1"), expr.String("/cart"), expr.Uint(50), tags(), at("1999-12-31T12:00:00.123456Z")},
	}
	core := projector.New(cat.Projector(), b, nil, projector.Config{Naming: naming, Region: "us-east-1"})
	for i, ev := range events {
		res, err := e.Append(ctx, cat, "page_views", [][]expr.Value{ev}, engine.AppendMode{})
		if err == nil {
			err = res[0].Err
		}
		if err != nil {
			t.Fatal(err)
		}
		it, err := b.Get(ctx, naming.StreamTable("page_views"), res[0].Key, true)
		if err != nil {
			t.Fatal(err)
		}
		hctx, cancel := context.WithTimeout(ctx, time.Minute)
		out, err := core.Handle(hctx, projector.Batch{
			Stream: "page_views", Unit: "shard-1",
			Records: []projector.Record{{Op: projector.Insert, Seq: fmt.Sprintf("1%020d", i+1), New: it}},
		})
		cancel()
		if err != nil || out.FailedSeq != "" {
			t.Fatalf("project event %d: %v %v", i, err, out.RowErr)
		}
	}
}

func (d *deployed) run(t *testing.T, args ...string) result {
	t.Helper()
	var stdout, stderr strings.Builder
	status := cli.RunEnv(t.Context(), args, nil, &stdout, &stderr, d.dir, cli.Env{
		Region: "us-west-2", Account: account, Backends: map[string]store.Backend{"us-east-1": d.b, "us-west-2": mem.New()},
	})
	return result{status, stdout.String(), stderr.String()}
}

// goldens holds the want/ sections of a txtar archive, and rewrites them with -update.
type goldens struct {
	path string
	mu   sync.Mutex
	ar   *txtar.Archive
}

func readGoldens(t *testing.T, name string) *goldens {
	t.Helper()
	g := &goldens{path: filepath.Join("testdata", name+".txtar")}
	data, err := os.ReadFile(g.path)
	if err != nil {
		t.Fatal(err)
	}
	g.ar = txtar.Parse(data)
	if *update {
		t.Cleanup(func() {
			// Parallel cases add their sections in any order.
			slices.SortStableFunc(g.ar.Files, func(a, b txtar.File) int {
				aw, bw := strings.HasPrefix(a.Name, wantPrefix), strings.HasPrefix(b.Name, wantPrefix)
				if aw && bw {
					return strings.Compare(a.Name, b.Name)
				}
				return cmp.Compare(boolInt(aw), boolInt(bw))
			})
			if werr := os.WriteFile(g.path, txtar.Format(g.ar), 0o644); werr != nil {
				t.Error(werr)
			}
		})
	}
	return g
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// check compares got with the sections want/<name>.stdout and want/<name>.stderr.
func (g *goldens) check(t *testing.T, name string, got result) {
	t.Helper()
	g.checkFile(t, name+".stdout", got.stdout)
	g.checkFile(t, name+".stderr", got.stderr)
}

// checkFile checks text against the section want/<name>.
func (g *goldens) checkFile(t *testing.T, name, text string) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	file := wantPrefix + name
	i := slices.IndexFunc(g.ar.Files, func(f txtar.File) bool { return f.Name == file })
	if *update {
		if i < 0 {
			g.ar.Files = append(g.ar.Files, txtar.File{Name: file})
			i = len(g.ar.Files) - 1
		}
		g.ar.Files[i].Data = []byte(text)
		return
	}
	if i < 0 {
		t.Errorf("no section %s; run go test -update and review the diff", file)
		return
	}
	if want := string(g.ar.Files[i].Data); text != want {
		t.Errorf("%s:\n%s\nwant\n%s", name, text, want)
	}
}

// TestDeployed runs query and inspect on the deployed store. Each case's output is in
// testdata/deployed.txtar.
func TestDeployed(t *testing.T) {
	t.Parallel()
	g := readGoldens(t, "deployed")
	us := []string{"-region", "us-east-1"}
	tests := []struct {
		name   string
		args   []string
		status int
		// change changes the deployed store before the run.
		change func(t *testing.T, d *deployed)
	}{
		{"query", slices.Concat([]string{"query", "PageStats", "-param", "page=/home", "-param", "from=1999-12-30", "-param", "to=1999-12-31"}, us), 0, nil},
		{"query json", slices.Concat([]string{"query", "-json", "PageStats", "-param", "page=/home", "-param", "from=1999-12-30", "-param", "to=1999-12-31"}, us), 0, nil},
		{"query without rows", slices.Concat([]string{"query", "PageStats", "-param", "page=/none", "-param", "from=1999-12-30", "-param", "to=1999-12-31"}, us), 0, nil},
		{"query of a stream", slices.Concat([]string{"query", "UserEvents", "-param", "users=['u1','u2']"}, us), 0, nil},
		{"query of a stream json", slices.Concat([]string{"query", "UserEvents", "-json", "-param", "users=['u1']"}, us), 0, nil},
		{"query one row", slices.Concat([]string{"query", "DayViews", "-param", "page=/home", "-param", "day=1999-12-31"}, us), 0, nil},
		{"query one row not found", slices.Concat([]string{"query", "DayViews", "-param", "page=/home", "-param", "day=1999-12-29"}, us), 1, nil},
		{"query version not live", slices.Concat([]string{"query", "UserViews", "-param", "users=['u1']"}, us), 1, nil},
		{
			"query version activating", slices.Concat([]string{"query", "UserViews", "-prefix", "", "-param", "users=['u1']"}, us), 1,
			func(t *testing.T, d *deployed) {
				t.Helper()
				if _, err := d.meta.Activate(t.Context(), "user_pages@v1", "page_views", meta.SplitFence, meta.DefaultBounds); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			"query version broken", slices.Concat([]string{"query", "UserViews", "-param", "users=['u1']"}, us), 1,
			func(t *testing.T, d *deployed) {
				t.Helper()
				f := meta.Failure{Cause: meta.CauseRecord, EventID: "e1", Error: "division by zero", At: time.Unix(946684800, 0)}
				if _, _, err := d.meta.MarkBroken(t.Context(), "user_pages@v1", "page_views", f); err != nil {
					t.Fatal(err)
				}
			},
		},
		{"query version not live on DynamoDB Local", []string{"query", "UserViews", "-param", "users=['u1']", "-endpoint", "http://127.0.0.1:8000"}, 1, nil},
		{
			"query version activating on DynamoDB Local", []string{"query", "UserViews", "-param", "users=['u1']", "-endpoint", "http://127.0.0.1:8000"}, 1,
			func(t *testing.T, d *deployed) {
				t.Helper()
				if _, err := d.meta.Activate(t.Context(), "user_pages@v1", "page_views", meta.SplitFence, meta.DefaultBounds); err != nil {
					t.Fatal(err)
				}
			},
		},
		{"query version not live allowed", slices.Concat([]string{"query", "UserViews", "-allow-partial", "-param", "users=['u1']"}, us), 0, nil},
		{"query unknown", slices.Concat([]string{"query", "PageStat"}, us), 2, nil},
		{"query unknown parameter", slices.Concat([]string{"query", "PageStats", "-param", "pge=/home"}, us), 2, nil},
		{"query missing parameters", slices.Concat([]string{"query", "PageStats", "-param", "page=/home"}, us), 2, nil},
		{"query invalid value", slices.Concat([]string{"query", "PageStats", "-param", "page=/home", "-param", "from=yesterday"}, us), 2, nil},
		{"query repeated parameter", slices.Concat([]string{"query", "PageStats", "-param", "page=/a", "-param", "page=/b"}, us), 2, nil},
		{"query no name", slices.Concat([]string{"query"}, us), 2, nil},
		{"region not in regions", []string{"query", "PageStats", "-region", "eu-west-1"}, 2, nil},
		{"region from the SDK", []string{"query", "PageStats"}, 1, nil},
		{"invalid prefix", slices.Concat([]string{"query", "PageStats", "-prefix", "Prod"}, us), 2, nil},
		{"remote endpoint", slices.Concat([]string{"query", "PageStats", "-endpoint", "http://dynamodb.us-east-1.amazonaws.com"}, us), 2, nil},
		{"another prefix not deployed", slices.Concat([]string{"query", "PageStats", "-prefix", "prod-"}, us), 1, nil},
		{
			"newer catalog format", slices.Concat([]string{"inspect", "page_views", "-key", "user_id=u1"}, us), 1,
			func(t *testing.T, d *deployed) {
				t.Helper()
				if _, err := d.meta.RegisterCatalog(t.Context(), []byte(`{"format":2}`), "v9.0.0"); err != nil {
					t.Fatal(err)
				}
			},
		},
		{"inspect stream", slices.Concat([]string{"inspect", "page_views", "-key", "user_id=u1"}, us), 0, nil},
		{"inspect stream json", slices.Concat([]string{"inspect", "page_views", "-json", "-key", "user_id=u2"}, us), 0, nil},
		{
			"inspect stream range", slices.Concat([]string{"inspect", "page_views", "-key", "user_id=u1", "-from", "1999-12-30T00:00:00Z",
				"-to", "1999-12-31T12:00:00Z", "-limit", "1"}, us), 0, nil,
		},
		{
			"inspect stream with typed keys", slices.Concat([]string{"inspect", "visits", "-key", "site=61F0C404-5CB3-11E7-907B-A6006AD3DBA0",
				"-key", "day=1999-12-31", "-key", "kind=7", "-from", "1999-12-01T00:00:00Z", "-to", "1999-12-31T00:00:00.5Z"}, us), 0, nil,
		},
		{"inspect stream not archived", slices.Concat([]string{"inspect", "orders", "-key", "order_id=o'1"}, us), 0, nil},
		{"inspect stream on DynamoDB Local", []string{"inspect", "page_views", "-key", "user_id=u1", "-endpoint", "http://127.0.0.1:8000"}, 0, nil},
		{"inspect stream without its key", slices.Concat([]string{"inspect", "page_views"}, us), 2, nil},
		{"inspect stream with -raw", slices.Concat([]string{"inspect", "page_views", "-raw", "-key", "user_id=u1"}, us), 2, nil},
		{"inspect view row", slices.Concat([]string{"inspect", "page_daily", "-key", "page=/home", "-key", "day=1999-12-31T00:00:00Z"}, us), 0, nil},
		{"inspect view partition", slices.Concat([]string{"inspect", "page_daily@v1", "-key", "page=/home"}, us), 0, nil},
		{"inspect view json", slices.Concat([]string{"inspect", "page_daily", "-json", "-key", "page=/home"}, us), 0, nil},
		{
			"inspect view in a reordered SELECT", slices.Concat([]string{"inspect", "page_daily", "-key", "page=/home"}, us), 0,
			editSQL("count()                  AS views,\n    uniqExact(user_id)       AS visitors,\n    avg(duration_ms)         AS avg_ms",
				"uniqExact(user_id) AS visitors, avg(duration_ms) AS avg_ms, count() AS views"),
		},
		{
			"inspect view whose SQL changed", slices.Concat([]string{"inspect", "page_daily", "-key", "page=/home"}, us), 0,
			editSQL("AS avg_ms\n", "AS avg_ms,\n    max(duration_ms) AS longest\n"),
		},
		{"inspect view raw row", slices.Concat([]string{"inspect", "page_daily", "-raw", "-key", "page=/home", "-key", "day=1999-12-31T00:00:00Z"}, us), 0, nil},
		{"inspect view raw partition", slices.Concat([]string{"inspect", "page_daily", "-raw", "-limit", "1", "-key", "page=/home"}, us), 0, nil},
		{"inspect view raw empty partition", slices.Concat([]string{"inspect", "page_daily", "-raw", "-key", "page=/none"}, us), 0, nil},
		{"inspect view without a current version", slices.Concat([]string{"inspect", "user_pages", "-key", "user_id=u1"}, us), 2, dropUserPages},
		{"inspect view not live", slices.Concat([]string{"inspect", "user_pages", "-key", "user_id=u1"}, us), 1, nil},
		{"inspect view row not found", slices.Concat([]string{"inspect", "page_daily", "-key", "page=/home", "-key", "day=1999-12-29T00:00:00Z"}, us), 1, nil},
		{"inspect view part of the sort key", slices.Concat([]string{"inspect", "user_pages", "-key", "views=1"}, us), 2, nil},
		{"inspect view with -from", slices.Concat([]string{"inspect", "page_daily", "-from", "1999-12-31T00:00:00Z", "-key", "page=/home"}, us), 2, nil},
		{"inspect unknown", slices.Concat([]string{"inspect", "page_dayly", "-key", "page=/home"}, us), 2, nil},
		{
			"inspect retired version", slices.Concat([]string{"inspect", "user_pages", "-key", "user_id=u1"}, us), 1,
			func(t *testing.T, d *deployed) {
				t.Helper()
				if _, err := d.meta.Retire(t.Context(), "user_pages@v1", "page_views"); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				d := newDeployed(t)
				if tt.change != nil {
					tt.change(t, d)
				}
				got := d.run(t, tt.args...)
				if got.status != tt.status {
					t.Errorf("status %d, want %d; stderr:\n%s", got.status, tt.status, got.stderr)
				}
				g.check(t, strings.ReplaceAll(tt.name, " ", "_"), got)
			})
		})
	}
}

// editSQL replaces from with to in the project's SQL file, after the deploy.
func editSQL(from, to string) func(t *testing.T, d *deployed) {
	return func(t *testing.T, d *deployed) {
		t.Helper()
		path := filepath.Join(d.dir, "dynaflux", "shop.sql")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), from) {
			t.Fatalf("%s doesn't hold %q", path, from)
		}
		if err = os.WriteFile(path, []byte(strings.Replace(string(data), from, to, 1)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// dropUserPages deploys the catalog without user_pages's SQL: its version is retained, and
// its query gone.
func dropUserPages(t *testing.T, d *deployed) {
	t.Helper()
	cat := compile(t, d.dir).Catalog
	i := slices.IndexFunc(cat.Views, func(v catalog.View) bool { return v.Name == "user_pages" })
	cat.Views[i].Versions[0].State = catalog.StateRetained
	cat.Queries = slices.DeleteFunc(cat.Queries, func(q catalog.Query) bool { return q.Name == "UserViews" })
	data, err := catalog.Marshal(cat)
	if err == nil {
		_, err = d.meta.RegisterCatalog(t.Context(), data, "v0.5.0")
	}
	if err != nil {
		t.Fatal(err)
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/queryhttp"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/validate"
)

// grantsProject reads one view by key, another by partition and a stream through exposed
// queries, and a third view through a query that isn't exposed.
var grantsProject = fstest.MapFS{ //nolint:gochecknoglobals // constant test data
	"dynaflux.yaml": {Data: []byte("store: app\nprefix: prod-\nregions: [" + region + "]\narchive: {enabled: false}\n")},
	"dynaflux/store.sql": {Data: []byte(`
CREATE STREAM clicks (user_id String, page String) PRIMARY KEY (user_id);

CREATE MATERIALIZED VIEW page_totals ORDER BY (page)
AS SELECT page, count() AS n FROM clicks GROUP BY page;

CREATE MATERIALIZED VIEW page_days ORDER BY (page, day) PRIMARY KEY (page)
AS SELECT page, toStartOfDay(event_time) AS day, count() AS n FROM clicks GROUP BY page, day;

CREATE MATERIALIZED VIEW user_totals ORDER BY (user_id)
AS SELECT user_id, count() AS n FROM clicks GROUP BY user_id;
`)},
	"dynaflux/queries.sql": {Data: []byte(`
-- name: PageTotal :one
-- expose: http
SELECT n FROM page_totals WHERE page = {page:String};

-- name: PageDays :many
-- expose: http
SELECT day, n FROM page_days WHERE page = {page:String};

-- name: RecentClicks :many
-- expose: http
SELECT event_time, page FROM clicks WHERE user_id = {user:String};

-- name: UserTotal :one
SELECT n FROM user_totals WHERE user_id = {user:String};
`)},
}

// grantsDeployment is where the test reads the query role, as IAM would.
func grantsDeployment() infra.Deployment {
	return infra.Deployment{Prefix: "prod-", Partition: "aws", Account: "123456789012", Region: region}
}

// tableCall is a DynamoDB call of the handler: its IAM action, its table and the partition it
// reads, dynamodb:LeadingKeys.
type tableCall struct {
	action, table, pk string
}

// recorder is a backend that notes each call.
type recorder struct {
	store.Backend

	mu    sync.Mutex
	calls []tableCall
}

func (r *recorder) note(action, table string, pks ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, pk := range pks {
		r.calls = append(r.calls, tableCall{action, table, pk})
	}
}

func (r *recorder) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	r.note("dynamodb:GetItem", table, k.PK)
	return r.Backend.Get(ctx, table, k, consistent)
}

func (r *recorder) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	for _, k := range ks {
		r.note("dynamodb:BatchGetItem", table, k.PK)
	}
	return r.Backend.BatchGet(ctx, table, ks, consistent)
}

func (r *recorder) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	r.note("dynamodb:Query", table, q.PK)
	return r.Backend.Query(ctx, table, q)
}

func (r *recorder) ScanPage(ctx context.Context, table string, sc store.Scan, start *store.Key) ([]store.Item, *store.Key, error) {
	r.note("dynamodb:Scan", table, "")
	return r.Backend.ScanPage(ctx, table, sc, start)
}

func (r *recorder) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	r.note("dynamodb:PutItem", table, it[catalog.AttrPK].Str)
	return r.Backend.Put(ctx, table, it, c)
}

// queryCalls returns the query role of grantsProject's templates, and the calls with which
// the handler answers each exposed query and the health check, the view states live.
func queryCalls(t *testing.T) (*infra.Manifest, infra.Role, []tableCall) {
	t.Helper()
	cfg, def, ds := validate.Load(grantsProject, "/project")
	if def == nil {
		t.Fatalf("the project doesn't compile: %v", ds)
	}
	m, err := infra.Build(cfg, def, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	role, err := m.FunctionRole(catalog.EnvModeQuery)
	if err != nil {
		t.Fatal(err)
	}
	data, err := catalog.Marshal(def)
	if err != nil {
		t.Fatal(err)
	}
	cat, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	naming := cfg.Naming()
	rec := &recorder{Backend: mem.New()}
	if err = meta.New(rec.Backend, naming.MetaTable()).SetLive(t.Context(), def); err != nil {
		t.Fatal(err)
	}
	h, err := queryhttp.New(rec, cat, queryhttp.Config{Region: region, Prefix: naming.Prefix, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{AwsRequestID: "req-1"})
	for _, r := range []struct {
		payload []byte
		read    func(*testing.T, []byte) answer
	}{
		{directPayload("PageTotal", `{"page":"/a"}`), readDirect},
		{directPayload("PageDays", `{"page":"/a"}`), readDirect},
		{directPayload("RecentClicks", `{"user":"u1"}`), readDirect},
		{functionURL.event(t, http.MethodGet, "/health", "", false), functionURL.read},
	} {
		out, ierr := h.Invoke(ctx, r.payload)
		if ierr != nil || r.read(t, out).status != http.StatusOK {
			t.Fatalf("%s: %s, %v", r.payload, out, ierr)
		}
	}
	return m, role, rec.calls
}

// checkQueryGrants compares calls with role both ways: a call that no statement allows would
// be AccessDenied on AWS, and a statement that allows what a call would name elsewhere, or an
// action that no call uses, grants too much. Elsewhere is another deployment's or store's
// table, an unexposed query's view, and the meta table's partitions besides the view states.
func checkQueryGrants(m *infra.Manifest, role infra.Role, calls []tableCall) error {
	d := grantsDeployment()
	tables := "arn:" + d.Partition + ":dynamodb:" + d.Region + ":" + d.Account + ":table/"
	var errs []error
	allowing := func(action, table, pk string) []string {
		sids, err := m.Allowing(role, d, action, tables+table, map[string]string{"dynamodb:LeadingKeys": pk})
		if err != nil {
			errs = append(errs, err)
		}
		return sids
	}
	used := map[string]bool{} // a statement's Sid and action
	foreign := map[string]bool{}
	for _, c := range calls {
		sids := allowing(c.action, c.table, c.pk)
		if len(sids) == 0 {
			errs = append(errs, fmt.Errorf("no statement allows %s on %s%s for partition %s", c.action, tables, c.table, c.pk))
		}
		for _, sid := range sids {
			used[sid+" "+c.action] = true
		}
		for _, o := range elsewhere(c) {
			for _, sid := range allowing(o.action, o.table, o.pk) {
				foreign[fmt.Sprintf("statement %s allows %s on %s%s for partition %s", sid, o.action, tables, o.table, o.pk)] = true
			}
		}
	}
	for _, f := range slices.Sorted(maps.Keys(foreign)) {
		errs = append(errs, errors.New(f))
	}
	for _, st := range role.Statements {
		for _, action := range st.Actions {
			if !used[st.Sid+" "+action] && st.Sid != "Logs" { // Lambda writes the logs
				errs = append(errs, fmt.Errorf("statement %s allows %s, which no call uses", st.Sid, action))
			}
		}
	}
	return errors.Join(errs...)
}

// elsewhere returns what call c would name in another deployment or store, on the view that
// only an unexposed query reads, and, on the meta table, in its other partitions.
func elsewhere(c tableCall) []tableCall {
	others := []tableCall{
		{c.action, strings.Replace(c.table, "prod-", "dev-", 1), c.pk},
		{c.action, strings.Replace(c.table, "-app-", "-billing-", 1), c.pk},
		{c.action, "prod-app-view-user_totals-v1", c.pk},
	}
	if c.table == "prod-app-meta" {
		for _, pk := range []string{"unit#clicks#0F3KZ8M1QW7RD", "job", "catalog", "rcpt#clicks#e1#1"} {
			others = append(others, tableCall{c.action, c.table, pk})
		}
	}
	return others
}

// The query role grants what the handler calls, on the tables and the meta partition it
// reads, and nothing else.
func TestQueryGrants(t *testing.T) {
	t.Parallel()
	m, role, calls := queryCalls(t)
	actions := map[string]bool{}
	for _, c := range calls {
		actions[c.action+" "+c.table] = true
	}
	want := []string{
		"dynamodb:BatchGetItem prod-app-view-page_totals-v1", "dynamodb:Query prod-app-meta",
		"dynamodb:Query prod-app-stream-clicks", "dynamodb:Query prod-app-view-page_days-v1",
	}
	if got := slices.Sorted(maps.Keys(actions)); !slices.Equal(got, want) {
		t.Errorf("the handler calls %q, want %q", got, want)
	}
	if err := checkQueryGrants(m, role, calls); err != nil {
		t.Error(err)
	}
}

// Each control breaks the query role one way, and the check must name it.
func TestQueryGrantsControls(t *testing.T) {
	t.Parallel()
	m, role, calls := queryCalls(t)
	const tables = "arn:aws:dynamodb:us-east-1:123456789012:table/"
	tests := []struct {
		name, sid string
		change    func(st *infra.Statement)
		want      string
	}{
		{"meta without its partition condition", "Meta", func(st *infra.Statement) { st.Conditions = nil },
			"statement Meta allows dynamodb:Query on " + tables + "prod-app-meta for partition catalog"},
		{"another partition", "Meta", func(st *infra.Statement) {
			st.Conditions = []infra.Condition{{Operator: "ForAllValues:StringEquals", Key: "dynamodb:LeadingKeys", Values: []infra.Text{"job"}}}
		}, "no statement allows dynamodb:Query on " + tables + "prod-app-meta for partition view"},
		{"no BatchGetItem", "Views", func(st *infra.Statement) { st.Actions = []string{"dynamodb:Query"} },
			"no statement allows dynamodb:BatchGetItem on " + tables + "prod-app-view-page_totals-v1"},
		{"an action no call uses", "Views", func(st *infra.Statement) { st.Actions = append(slices.Clone(st.Actions), "dynamodb:GetItem") },
			"statement Views allows dynamodb:GetItem, which no call uses"},
		{"every view table", "Views", func(st *infra.Statement) {
			st.Resources = []infra.Text{"arn:${partition}:dynamodb:${region}:${account}:table/${prefix}app-view-*"}
		}, "statement Views allows dynamodb:BatchGetItem on " + tables + "prod-app-view-user_totals-v1"},
		{"a write", "Streams", func(st *infra.Statement) { st.Actions = append(slices.Clone(st.Actions), "dynamodb:PutItem") },
			"statement Streams allows dynamodb:PutItem, which no call uses"},
	}
	for _, tt := range tests {
		broken := role
		broken.Statements = slices.Clone(role.Statements)
		k := slices.IndexFunc(broken.Statements, func(st infra.Statement) bool { return st.Sid == tt.sid })
		if k < 0 {
			t.Fatalf("%s: the role has no statement %s", tt.name, tt.sid)
		}
		tt.change(&broken.Statements[k])
		if err := checkQueryGrants(m, broken, calls); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: the check fails with %v, want %q", tt.name, err, tt.want)
		}
	}
}

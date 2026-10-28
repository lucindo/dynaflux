// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfill_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/validate"
)

// The store of the grants test: page_views has events, and a version of each kind; clicks
// has none, so its version is made live by the fresh rule.
const grantsSQL = `
CREATE STREAM page_views (page String, user_id String) PRIMARY KEY (page);
CREATE STREAM clicks (page String) PRIMARY KEY (page);
CREATE MATERIALIZED VIEW page_daily ORDER BY (page) AS SELECT page, count() AS views FROM page_views GROUP BY page;
CREATE MATERIALIZED VIEW page_users ORDER BY (page) AS SELECT page, uniqExact(user_id) AS users FROM page_views GROUP BY page;
CREATE MATERIALIZED VIEW page_seen ORDER BY (page) AS SELECT page, max(user_id) AS last FROM page_views GROUP BY page;
CREATE MATERIALIZED VIEW click_count ORDER BY (page) AS SELECT page, count() AS n FROM clicks GROUP BY page;
`

func grantsDeployment() infra.Deployment {
	return infra.Deployment{Prefix: "prod-", Partition: "aws", Account: "123456789012", Region: "us-east-1"}
}

// grantsStore is the grants test's store in us-east-1 and us-west-2, archived with every
// event, as dynaflux generate builds it.
func grantsStore(t *testing.T, store string) (*infra.Manifest, []byte) {
	t.Helper()
	cfg, cat, ds := validate.Load(fstest.MapFS{
		"dynaflux.yaml":      {Data: []byte("store: " + store + "\nregions: [us-east-1, us-west-2]\n")},
		"dynaflux/store.sql": {Data: []byte(grantsSQL)},
	}, "/project")
	if cat == nil {
		t.Fatalf("the project doesn't compile: %v", ds)
	}
	m, err := infra.Build(cfg, cat, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := catalog.Marshal(cat)
	if err != nil {
		t.Fatal(err)
	}
	return m, data
}

// recorder is a backend that notes the DynamoDB action and table of each call.
type recorder struct {
	store.Backend

	mu    sync.Mutex
	calls map[string]map[string]bool // action → tables
}

func (r *recorder) note(action, table string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls[action] == nil {
		r.calls[action] = map[string]bool{}
	}
	r.calls[action][table] = true
}

func (r *recorder) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	r.note("dynamodb:GetItem", table)
	return r.Backend.Get(ctx, table, k, consistent)
}

func (r *recorder) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	r.note("dynamodb:BatchGetItem", table)
	return r.Backend.BatchGet(ctx, table, ks, consistent)
}

func (r *recorder) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	r.note("dynamodb:Query", table)
	return r.Backend.Query(ctx, table, q)
}

func (r *recorder) ScanPage(ctx context.Context, table string, sc store.Scan, start *store.Key) ([]store.Item, *store.Key, error) {
	r.note("dynamodb:Scan", table)
	return r.Backend.ScanPage(ctx, table, sc, start)
}

func (r *recorder) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	r.note("dynamodb:PutItem", table)
	return r.Backend.Put(ctx, table, it, c)
}

type shards []string

func (s shards) Units(context.Context, string) ([]string, error) { return s, nil }

// born gives the stream tables their creation times: clicks is new.
type born map[string]time.Time

func (b born) Born(_ context.Context, table string) (time.Time, bool, error) {
	return b[table], false, nil
}

type noLag struct{}

func (noLag) Lag(context.Context) (time.Duration, error) { return 0, nil }

// storeCalls runs a job of every version of the grants store to its end, and returns the
// store calls by action, with the tables each named, and the staging prefix of a job. Each
// version of page_views reads the table, and click_count is made live by the fresh rule.
func storeCalls(t *testing.T, store string, data []byte) (map[string][]string, string) {
	t.Helper()
	var calls map[string][]string
	var staging string
	synctest.Test(t, func(t *testing.T) {
		env, rec := grantsEnv(t, store, data)
		for id, vv := range env.Catalog.Versions() {
			split := meta.SplitNone
			switch {
			case vv.Stream == "clicks":
				split = meta.SplitFence
			case vv.FencedHandoff():
				split = meta.SplitTime
			}
			j, err := backfill.Start(t.Context(), env.Meta, backfill.Request{Version: id, Stream: vv.Stream, Name: id, Execution: backfill.LocalExecution, Split: split})
			if err != nil {
				t.Fatal(err)
			}
			staging = j.Staging
			runJob(t, env, id)
		}
		calls = map[string][]string{}
		for action, tables := range rec.calls {
			calls[action] = slices.Sorted(maps.Keys(tables))
		}
	})
	return calls, staging
}

// grantsEnv deploys the grants store in memory, appends an event to page_views, which a live
// batch applies, and returns a backfill environment whose store calls rec records from then
// on.
func grantsEnv(t *testing.T, store string, data []byte) (*backfill.Env, *recorder) {
	t.Helper()
	naming := catalog.Naming{Prefix: grantsDeployment().Prefix, Store: store}
	cat, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	b := mem.New()
	if _, err = meta.New(b, naming.MetaTable()).RegisterCatalog(t.Context(), data, "v0.5.0"); err != nil {
		t.Fatal(err)
	}
	e := engine.New(b, engine.Options{Region: "us-east-1", Prefix: naming.Prefix})
	res, err := e.Append(t.Context(), cat, "page_views", [][]expr.Value{{expr.String("/a"), expr.String("u1"), expr.Null()}}, engine.AppendMode{})
	if err == nil {
		err = res[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	project(t, cat, b, naming)
	rec := &recorder{Backend: b, calls: map[string]map[string]bool{}}
	return &backfill.Env{
		Naming: naming, Catalog: cat.Catalog(), Hash: catalog.Hash(data), Meta: meta.New(rec, naming.MetaTable()), Store: rec,
		Core: projector.New(cat.Projector(), rec, nil, projector.Config{Naming: naming}), Units: shards{"shardId-1"},
		Tables: born{naming.StreamTable("page_views"): time.Now().Add(-48 * time.Hour), naming.StreamTable("clicks"): time.Now()},
		Runs:   backfill.NewMemRuns(), Lag: noLag{}, Bounds: meta.Bounds{Skew: time.Second, Landing: time.Second},
		Memory: 64 << 20, Segments: 1, Log: slog.New(slog.DiscardHandler),
	}, rec
}

// project applies the events of page_views's table in a live batch, as the projector of
// unit shardId-1 does: the rows of the versions it applies hold the unit's mark, so a job
// that writes them reads the unit's item.
func project(t *testing.T, cat *engine.Catalog, b store.Backend, naming catalog.Naming) {
	t.Helper()
	items, _, err := b.ScanPage(t.Context(), naming.StreamTable("page_views"), store.Scan{Segments: 1}, nil)
	if err != nil || len(items) == 0 {
		t.Fatalf("scan page_views: %d items, %v", len(items), err)
	}
	batch := projector.Batch{Stream: "page_views", Unit: "shardId-1"}
	for i, it := range items {
		batch.Records = append(batch.Records, projector.Record{Op: projector.Insert, Seq: fmt.Sprintf("%021d", i+1), New: it})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	res, err := projector.New(cat.Projector(), b, nil, projector.Config{Naming: naming, Region: "us-east-1"}).Handle(ctx, batch)
	if err != nil || res.FailedSeq != "" {
		t.Fatalf("the live batch: %+v, %v", res, err)
	}
}

// runJob steps a version's job until it is done, and checks that its version is live.
func runJob(t *testing.T, env *backfill.Env, version string) {
	t.Helper()
	for range 100 {
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
		wait, done, err := backfill.Step(ctx, env, version, 1, "local")
		cancel()
		if err != nil {
			t.Fatalf("%s: %v", version, err)
		}
		if done {
			break
		}
		time.Sleep(wait)
	}
	states, err := env.Meta.ViewStates(t.Context())
	if err != nil || states[version].State != meta.Live {
		t.Fatalf("%s is %s, %v; want live", version, states[version].State, err)
	}
}

// grantCall is a call by the IAM action it needs, the ARN that IAM authorizes it on, and its
// condition keys.
type grantCall struct {
	action, arn string
	keys        map[string]string
}

// backfillCalls is every call that a backfill function of deployment d can send for the
// store of m: the store calls the jobs sent, a table of the adapters' calls, and what Athena
// sends with the function's credentials while it runs a job's queries.
func backfillCalls(t *testing.T, m *infra.Manifest, data []byte, d infra.Deployment) []grantCall {
	t.Helper()
	var calls []grantCall
	add := func(action, arn string, keys map[string]string) {
		calls = append(calls, grantCall{action: action, arn: arn, keys: keys})
	}
	table := func(region, name string) string {
		return "arn:" + d.Partition + ":dynamodb:" + region + ":" + d.Account + ":table/" + strings.Replace(name, "prod-", d.Prefix, 1)
	}
	regional := func(service, resource string) string {
		return "arn:" + d.Partition + ":" + service + ":" + d.Region + ":" + d.Account + ":" + resource
	}
	prefixed := func(prefix string) map[string]string { return map[string]string{"s3:prefix": prefix} }
	stores, staging := storeCalls(t, m.Store, data)
	for _, action := range slices.Sorted(maps.Keys(stores)) {
		for _, name := range stores[action] {
			add(action, table(d.Region, name), nil)
		}
	}
	naming := catalog.Naming{Prefix: d.Prefix, Store: m.Store}
	bucket := "arn:" + d.Partition + ":s3:::" + naming.Bucket(d.Account, d.Region)
	object := bucket + "/" + staging + "runs/1-0a1b2c3d"
	add("s3:PutObject", object, nil)
	add("s3:GetObject", object, nil)
	add("s3:DeleteObject", object, nil)
	add("s3:ListBucket", bucket, prefixed(staging))
	add("cloudwatch:GetMetricData", "*", nil)
	for _, stream := range []string{"page_views", "clicks"} {
		for _, region := range m.Regions {
			add("dynamodb:DescribeTable", table(region, naming.StreamTable(stream)), nil)
		}
		add("dynamodb:DescribeStream", table(d.Region, naming.StreamTable(stream))+"/stream/2026-10-26T12:00:00.000", nil)
		add("firehose:DescribeDeliveryStream", regional("firehose", "deliverystream/"+naming.ArchiveStream(stream)), nil)
	}
	// The archive's reads: an export's manifest, which the job lists when S3 refuses it, to
	// tell a missing one; its files; the error prefix; Lambda's gaps; the evidence.
	for _, op := range []string{"StartQueryExecution", "GetQueryExecution", "GetQueryResults", "StopQueryExecution"} {
		add("athena:"+op, regional("athena", "workgroup/"+naming.BackfillWorkGroup()), nil)
	}
	manifest := "athena/backfill/q-1-manifest.csv"
	export := bucket + "/" + staging + "unload/plan-1-ab/0-86400000000/1/q-1_0.gz"
	add("s3:GetObject", bucket+"/"+manifest, nil)
	add("s3:ListBucket", bucket, prefixed(manifest))
	add("s3:GetObject", export, nil)
	add("s3:ListBucket", bucket, prefixed(archive.ErrorPrefix("page_views")))
	add("s3:GetObject", bucket+"/"+archive.ErrorPrefix("page_views")+"2026/10/01/00/x", nil)
	add("s3:ListBucket", bucket, prefixed(deployed.GapPrefix))
	add("s3:GetObject", bucket+"/"+deployed.GapPrefix+"0a1b/shardId-1/2026/10/01/2026-10-01T00.00.00-x", nil)
	add("cloudwatch:ListMetrics", "*", nil)
	add("lambda:ListEventSourceMappings", "*", nil)
	// Athena's: actions on a Glue table need the table, its database and the catalog (Glue's
	// ARN documentation), and Athena reads the default database too.
	db := naming.ArchiveDatabase()
	glueCatalog, glueDB := regional("glue", "catalog"), regional("glue", "database/"+db)
	for _, arn := range []string{glueCatalog, regional("glue", "database/default"), glueDB} {
		add("glue:GetDatabase", arn, nil)
	}
	add("glue:GetDatabases", glueCatalog, nil)
	add("glue:GetDatabases", regional("glue", "database/default"), nil)
	for _, arn := range []string{glueCatalog, glueDB, regional("glue", "table/"+db+"/page_views")} {
		for _, action := range []string{"glue:GetTable", "glue:GetTables", "glue:GetPartitions"} {
			add(action, arn, nil)
		}
	}
	add("s3:GetObject", bucket+"/warehouse/"+db+"/page_views/iceberg/data/1.parquet", nil)
	add("s3:ListBucket", bucket, prefixed("warehouse/"+db+"/page_views/iceberg/"))
	for _, action := range []string{"s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"} {
		add(action, bucket+"/athena/backfill/q-1.csv", nil)
	}
	add("s3:ListBucket", bucket, prefixed("athena/backfill/"))
	add("s3:GetBucketLocation", bucket, nil)
	add("s3:PutObject", export, nil)
	return calls
}

// checkGrants compares calls with role both ways: a call that no statement allows would be
// AccessDenied on AWS; a statement that allows a call of foreign, or an action that no call
// uses, other than the log group's, grants too much.
func checkGrants(m *infra.Manifest, role infra.Role, calls, foreign []grantCall) error {
	var errs []error
	used := map[string]bool{}
	for _, c := range calls {
		sids, err := m.Allowing(role, grantsDeployment(), c.action, c.arn, c.keys)
		if len(sids) == 0 || err != nil {
			errs = append(errs, fmt.Errorf("no statement allows %s on %s %v: %w", c.action, c.arn, c.keys, err))
		}
		for _, sid := range sids {
			used[sid+" "+c.action] = true
		}
	}
	for _, c := range foreign {
		sids, err := m.Allowing(role, grantsDeployment(), c.action, c.arn, c.keys)
		if err != nil {
			errs = append(errs, err)
		}
		for _, sid := range sids {
			errs = append(errs, fmt.Errorf("statement %s allows %s on %s %v, another store's or deployment's", sid, c.action, c.arn, c.keys))
		}
	}
	statements := slices.Clone(role.Statements)
	for _, p := range role.Managed {
		statements = append(statements, p.Statements...)
	}
	for _, st := range statements {
		for _, action := range st.Actions {
			if !used[st.Sid+" "+action] && st.Sid != "Logs" {
				errs = append(errs, fmt.Errorf("statement %s allows %s, which no call uses", st.Sid, action))
			}
		}
	}
	return errors.Join(errs...)
}

// foreignCalls is what the same calls name for the store under another prefix and for
// another store, less those of the store's own deployment, such as the calls of no resource
// type and Athena's on the Glue catalog; and the bucket's objects outside the prefixes the
// role reads, such as another database's in the warehouse.
func foreignCalls(t *testing.T, m *infra.Manifest, data []byte) []grantCall {
	t.Helper()
	dev := grantsDeployment()
	dev.Prefix = "dev-"
	other, otherData := grantsStore(t, "billing")
	own := backfillCalls(t, m, data, grantsDeployment())
	foreign := slices.DeleteFunc(slices.Concat(backfillCalls(t, m, data, dev), backfillCalls(t, other, otherData, grantsDeployment())),
		func(c grantCall) bool {
			return slices.ContainsFunc(own, func(o grantCall) bool { return o.action == c.action && o.arn == c.arn && maps.Equal(o.keys, c.keys) })
		})
	d := grantsDeployment()
	bucket := "arn:" + d.Partition + ":s3:::" + catalog.Naming{Prefix: d.Prefix, Store: m.Store}.Bucket(d.Account, d.Region)
	for _, action := range []string{"s3:GetObject", "s3:PutObject", "s3:DeleteObject"} {
		foreign = append(foreign, grantCall{action: action, arn: bucket + "/warehouse/analytics/page_views/iceberg/data/1.parquet"})
	}
	return append(foreign, grantCall{action: "s3:ListBucket", arn: bucket, keys: map[string]string{"s3:prefix": "warehouse/"}})
}

func backfillRole(t *testing.T, m *infra.Manifest) infra.Role {
	t.Helper()
	role, err := m.FunctionRole(catalog.EnvModeBackfill)
	if err != nil {
		t.Fatal(err)
	}
	return role
}

// The backfill role grants what a job's steps call, through the store and the adapters, on
// the resources they name, and nothing else but the log group's grants, which Lambda uses.
func TestBackfillGrants(t *testing.T) {
	t.Parallel()
	m, data := grantsStore(t, "analytics")
	calls := backfillCalls(t, m, data, grantsDeployment())
	var ops []string
	for service, names := range awsbackfill.Operations() {
		for _, name := range names {
			ops = append(ops, service+":"+name)
		}
	}
	// The adapters' operations by the IAM action they need.
	actions := map[string]string{
		"s3:PutObject": "s3:PutObject", "s3:GetObject": "s3:GetObject", "s3:ListObjectsV2": "s3:ListBucket",
		"s3:DeleteObjects": "s3:DeleteObject", "dynamodb:DescribeTable": "dynamodb:DescribeTable",
		"dynamodb:DescribeStream": "dynamodb:DescribeStream", "cloudwatch:GetMetricData": "cloudwatch:GetMetricData",
		"cloudwatch:ListMetrics": "cloudwatch:ListMetrics", "athena:StartQueryExecution": "athena:StartQueryExecution",
		"athena:GetQueryExecution": "athena:GetQueryExecution", "athena:GetQueryResults": "athena:GetQueryResults",
		"athena:StopQueryExecution": "athena:StopQueryExecution", "firehose:DescribeDeliveryStream": "firehose:DescribeDeliveryStream",
		"lambda:ListEventSourceMappings": "lambda:ListEventSourceMappings",
	}
	if got, want := slices.Sorted(maps.Keys(actions)), slices.Sorted(slices.Values(ops)); !slices.Equal(got, want) {
		t.Errorf("the table maps %q, but the adapters can send %q", got, want)
	}
	for _, op := range ops {
		if !slices.ContainsFunc(calls, func(c grantCall) bool { return c.action == actions[op] }) {
			t.Errorf("no call of the table needs %s, which %s does", actions[op], op)
		}
	}
	if err := checkGrants(m, backfillRole(t, m), calls, foreignCalls(t, m, data)); err != nil {
		t.Error(err)
	}
}

// Each control breaks the role one way, and the check must name it.
func TestBackfillGrantsControls(t *testing.T) {
	t.Parallel()
	m, data := grantsStore(t, "analytics")
	calls, foreign := backfillCalls(t, m, data, grantsDeployment()), foreignCalls(t, m, data)
	tests := []struct {
		name, sid string
		change    func(st *infra.Statement)
		want      string
	}{
		{"no scan", "StreamScan", func(st *infra.Statement) { st.Actions = nil },
			"no statement allows dynamodb:Scan on arn:aws:dynamodb:us-east-1:123456789012:table/prod-analytics-stream-clicks"},
		{"an unused action", "Meta", func(st *infra.Statement) { st.Actions = append(slices.Clone(st.Actions), "dynamodb:DeleteItem") },
			"statement Meta allows dynamodb:DeleteItem, which no call uses"},
		{"the whole bucket", "Staging", func(st *infra.Statement) { st.Resources = []infra.Text{"${ref:Bucket.arn}/*"} },
			"statement Staging allows s3:GetObject on arn:aws:s3:::prod-analytics-123456789012-us-east-1-an/warehouse/"},
		{"any prefix's listing", "StagingList", func(st *infra.Statement) { st.Conditions = nil },
			"statement StagingList allows s3:ListBucket on arn:aws:s3:::prod-analytics-123456789012-us-east-1-an map[s3:prefix:warehouse/], another"},
		{"every view table", "Views", func(st *infra.Statement) {
			st.Resources = []infra.Text{"arn:${partition}:dynamodb:${region}:${account}:table/*"}
		}, "statement Views allows dynamodb:PutItem on arn:aws:dynamodb:us-east-1:123456789012:table/dev-analytics-view-page_daily-v1"},
		{"every workgroup", "ArchiveQueries", func(st *infra.Statement) {
			st.Resources = []infra.Text{"arn:${partition}:athena:${region}:${account}:workgroup/*"}
		}, "statement ArchiveQueries allows athena:GetQueryExecution on arn:aws:athena:us-east-1:123456789012:workgroup/dev-analytics-backfill"},
		{"no delivery streams", "ArchiveDelivery", func(st *infra.Statement) { st.Actions = nil },
			"no statement allows firehose:DescribeDeliveryStream on arn:aws:firehose:us-east-1:123456789012:deliverystream/prod-analytics-archive-clicks"},
		{"no error prefix", "ArchiveErrorsList", func(st *infra.Statement) { st.Conditions[0].Values = []infra.Text{"aws/lambda/*"} },
			"no statement allows s3:ListBucket on arn:aws:s3:::prod-analytics-123456789012-us-east-1-an map[s3:prefix:errors/firehose/page_views/]"},
		{"one region's replicas", "StreamTables", func(st *infra.Statement) {
			st.Resources = slices.DeleteFunc(slices.Clone(st.Resources), func(r infra.Text) bool { return strings.Contains(string(r), "us-west-2") })
		}, "no statement allows dynamodb:DescribeTable on arn:aws:dynamodb:us-west-2:123456789012:table/prod-analytics-stream-clicks"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			role := backfillRole(t, m)
			role.Statements = slices.Clone(role.Statements)
			role.Managed = slices.Clone(role.Managed)
			found := false
			for _, sts := range append([][]infra.Statement{role.Statements}, managedStatements(&role)...) {
				for i := range sts {
					if sts[i].Sid == tt.sid {
						tt.change(&sts[i])
						found = true
					}
				}
			}
			if !found {
				t.Fatalf("the role has no statement %s", tt.sid)
			}
			err := checkGrants(m, role, calls, foreign)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("the check fails with %v, want %q", err, tt.want)
			}
		})
	}
}

// managedStatements returns copies of the statements of each of r's managed policies, which
// r then holds.
func managedStatements(r *infra.Role) [][]infra.Statement {
	var out [][]infra.Statement
	for i := range r.Managed {
		r.Managed[i].Statements = slices.Clone(r.Managed[i].Statements)
		out = append(out, r.Managed[i].Statements)
	}
	return out
}

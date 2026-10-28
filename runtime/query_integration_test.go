// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
	"github.com/lucindo/dynaflux/internal/validate"
)

// queryProject is store app with a view that an exposed query reads.
var queryProject = fstest.MapFS{ //nolint:gochecknoglobals // constant test data
	"dynaflux.yaml": {Data: []byte("store: app\nregions: [" + emulatortest.Region + "]\narchive: {enabled: false}\n")},
	"dynaflux/store.sql": {Data: []byte(`
CREATE STREAM clicks (user_id String, page String, ms UInt32)
PRIMARY KEY (user_id);

CREATE MATERIALIZED VIEW page_clicks
ORDER BY (page)
AS SELECT page, count() AS clicks, max(ms) AS longest
FROM clicks
GROUP BY page;
`)},
	"dynaflux/queries.sql": {Data: []byte(`
-- name: PageClicks :one
-- expose: http
SELECT page, clicks, longest FROM page_clicks WHERE page = {page:String};
`)},
}

// The runtime binary in query mode answers a direct invoke and a function URL request from
// DynamoDB Local, whose view is live and holds the clicks.
func TestRunServesQueries(t *testing.T) {
	t.Parallel()
	cfg, def, ds := validate.Load(queryProject, "/project")
	if def == nil {
		var b bytes.Buffer
		if err := diag.Write(&b, ds); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("the store doesn't compile:\n%s", b.Bytes())
	}
	data, err := catalog.Marshal(def)
	if err != nil {
		t.Fatal(err)
	}
	m, err := infra.Build(cfg, def, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	local := emulatortest.Config(t, nil)
	naming := emulatortest.Tables(t, local, m)
	fillClicks(t, data, naming, dynamo.New(dynamodb.NewFromConfig(local)))

	url, err := json.Marshal(events.LambdaFunctionURLRequest{
		Version: "2.0", RawPath: "/queries/PageClicks",
		RequestContext: events.LambdaFunctionURLRequestContext{HTTP: events.LambdaFunctionURLRequestContextHTTPDescription{Method: "POST"}},
		Body:           `{"page":"/b"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	api := newRuntimeAPI(t, newFakeDynamoDB(t), []byte(`{"query":"PageClicks","params":{"page":"/a"}}`), url)
	c := startChild(t, childRun, api, map[string]string{
		catalogVar:                  string(data),
		"DYNAFLUX_MODE":             "query",
		"DYNAFLUX_PREFIX":           naming.Prefix,
		"DYNAFLUX_CATALOG_HASH":     catalog.Hash(data),
		"AWS_ENDPOINT_URL_DYNAMODB": aws.ToString(local.BaseEndpoint),
	})
	c.serve(t, api)
	want := []post{
		{path: "/2018-06-01/runtime/invocation/req-0/response", body: `{"status":200,"meta":[{"name":"page","type":"String"},` +
			`{"name":"clicks","type":"UInt64"},{"name":"longest","type":"UInt32"}],"data":[{"page":"/a","clicks":"2","longest":300}],"rows":1}`},
		{path: "/2018-06-01/runtime/invocation/req-1/response", body: `{"statusCode":200,"headers":{"cache-control":"no-store",` +
			`"content-type":"application/json","x-dynaflux-region":"us-east-1"},"body":"{\"status\":200,\"meta\":[` +
			`{\"name\":\"page\",\"type\":\"String\"},{\"name\":\"clicks\",\"type\":\"UInt64\"},` +
			`{\"name\":\"longest\",\"type\":\"UInt32\"}],\"data\":[{\"page\":\"/b\",\"clicks\":\"1\",\"longest\":50}],` +
			`\"rows\":1}","isBase64Encoded":false,"cookies":null}`},
	}
	_, posts := api.state()
	if len(posts) != len(want) {
		t.Fatalf("posts %+v, want 2; stderr:\n%s", posts, &c.stderr)
	}
	for i, p := range posts {
		if p.path != want[i].path || strings.TrimSpace(p.body) != want[i].body {
			t.Errorf("post %d: %s %s\nwant %s %s", i, p.path, p.body, want[i].path, want[i].body)
		}
	}
}

// fillClicks makes every version of the catalog in data live in the tables of naming, and
// appends and projects three clicks: two on /a, one on /b.
func fillClicks(t *testing.T, data []byte, naming catalog.Naming, b *dynamo.Backend) {
	t.Helper()
	cat, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	if err = meta.New(b, naming.MetaTable()).SetLive(t.Context(), cat.Catalog()); err != nil {
		t.Fatal(err)
	}
	e := engine.New(b, engine.Options{Region: appendRegion, Prefix: naming.Prefix})
	core := projector.New(cat.Projector(), b, nil, projector.Config{Naming: naming, Region: appendRegion})
	for i, c := range []struct {
		user, page string
		ms         uint64
	}{{"u1", "/a", 100}, {"u2", "/a", 300}, {"u1", "/b", 50}} {
		res, aerr := e.Append(t.Context(), cat, "clicks", [][]expr.Value{{
			expr.String(c.user), expr.String(c.page), expr.Uint(c.ms), expr.Micros(time.Now().UnixMicro()),
		}}, engine.AppendMode{})
		if aerr == nil {
			aerr = res[0].Err
		}
		if aerr != nil {
			t.Fatal(aerr)
		}
		it, gerr := b.Get(t.Context(), naming.StreamTable("clicks"), res[0].Key, true)
		if gerr != nil {
			t.Fatal(gerr)
		}
		ctx, cancel := context.WithTimeout(t.Context(), functionTimeout)
		r, herr := core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: shard, Records: []projector.Record{
			{Op: projector.Insert, Seq: seq(i + 1), New: it},
		}})
		cancel()
		if herr != nil || r.FailedSeq != "" {
			t.Fatalf("project: %v %+v", herr, r)
		}
	}
}

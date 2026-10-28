// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/queryhttp"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/valuetext"
)

// storeSQL has a stream of every declared type, two views over it, of which only
// device_totals is exposed, and a stream of large values for the response limit.
const storeSQL = `
CREATE STREAM readings (
    device String,
    level  UInt8,
    delta  Int64,
    total  UInt64,
    ratio  Float64,
    price  Decimal(18, 2),
    ok     Bool,
    batch  UUID,
    day    Date,
    at_ms  DateTime64(3),
    note   Nullable(String),
    refund Nullable(Decimal(18, 2)),
    tags   Array(String)
)
PRIMARY KEY (device);

CREATE MATERIALIZED VIEW device_totals
ORDER BY (device)
AS SELECT device, count() AS readings, sum(total) AS sum_total
FROM readings
GROUP BY device;

CREATE MATERIALIZED VIEW device_levels
ORDER BY (device)
AS SELECT device, max(level) AS top
FROM readings
GROUP BY device;

CREATE STREAM blobs (k String, pad String)
PRIMARY KEY (k);
`

const queriesSQL = `
-- name: Readings :many
-- expose: http
SELECT device, level, delta, total, ratio, price, ok, batch, day, at_ms, note, refund, tags, event_time,
    toStartOfHour(event_time) AS hour
FROM readings
WHERE device = {device:String};

-- name: Match :many
-- expose: http
SELECT device FROM readings
WHERE device = {device:String} AND level = {level:UInt8} AND delta = {delta:Int64}
    AND total = {total:UInt64} AND ratio = {ratio:Float64} AND price = {price:Decimal(18, 2)}
    AND ok = {ok:Bool} AND batch = {batch:UUID} AND day = {day:Date} AND at_ms = {at:DateTime64(3)}
    AND event_time >= {since:DateTime64(6)};

-- name: DeviceTotal :one
-- expose: http
SELECT device, readings, sum_total FROM device_totals WHERE device = {device:String};

-- name: DeviceTotals :many
-- expose: http
SELECT device, readings FROM device_totals WHERE device IN {devices:Array(String)};

-- name: Share :one
-- expose: http
SELECT device, intDiv(sum_total, {per:UInt64}) AS share FROM device_totals WHERE device = {device:String};

-- name: Hidden :many
SELECT device, top FROM device_levels WHERE device = {device:String};

-- name: Blobs :many
-- expose: http
SELECT pad FROM blobs WHERE k = {k:String};
`

const (
	region  = "us-east-1"
	version = "device_totals@v1"
)

// The two readings of device d1, as JSON values of readings' columns, then event_time.
var readings = [][]string{ //nolint:gochecknoglobals // constant test data
	{`"d1"`, `7`, `-42`, `"18446744073709551615"`, `0.25`, `"1234.5"`, `true`, `"61F0C404-5CB3-11E7-907B-A6006AD3DBA0"`,
		`"2026-10-01"`, `"2026-10-01T12:00:00.123Z"`, `"café & <b>"`, `null`, `["go","dynamodb"]`, `"2026-10-01T12:34:56.123456Z"`},
	{`"d1"`, `0`, `"9223372036854775807"`, `0`, `-1e-7`, `"-0.01"`, `false`, `"00000000-0000-0000-0000-000000000001"`,
		`"1970-01-01"`, `"2026-10-01T15:00:00+02:00"`, `null`, `"12.3"`, `[]`, `"2026-10-01T13:00:00Z"`},
}

// readingsBody is Readings' body for device d1.
const readingsBody = `{"status":200,"meta":[{"name":"device","type":"String"},{"name":"level","type":"UInt8"},` +
	`{"name":"delta","type":"Int64"},{"name":"total","type":"UInt64"},{"name":"ratio","type":"Float64"},` +
	`{"name":"price","type":"Decimal(18, 2)"},{"name":"ok","type":"Bool"},{"name":"batch","type":"UUID"},` +
	`{"name":"day","type":"Date"},{"name":"at_ms","type":"DateTime64(3)"},{"name":"note","type":"Nullable(String)"},` +
	`{"name":"refund","type":"Nullable(Decimal(18, 2))"},{"name":"tags","type":"Array(String)"},` +
	`{"name":"event_time","type":"DateTime64(6)"},{"name":"hour","type":"DateTime"}],"data":[` +
	`{"device":"d1","level":7,"delta":"-42","total":"18446744073709551615","ratio":0.25,"price":"1234.50","ok":true,` +
	`"batch":"61f0c404-5cb3-11e7-907b-a6006ad3dba0","day":"2026-10-01","at_ms":"2026-10-01T12:00:00.123Z",` +
	`"note":"café & <b>","refund":null,"tags":["go","dynamodb"],"event_time":"2026-10-01T12:34:56.123456Z",` +
	`"hour":"2026-10-01T12:00:00Z"},` +
	`{"device":"d1","level":0,"delta":"9223372036854775807","total":"0","ratio":-1e-7,"price":"-0.01","ok":false,` +
	`"batch":"00000000-0000-0000-0000-000000000001","day":"1970-01-01","at_ms":"2026-10-01T13:00:00.000Z",` +
	`"note":null,"refund":"12.30","tags":[],"event_time":"2026-10-01T13:00:00.000000Z","hour":"2026-10-01T13:00:00Z"}],` +
	`"rows":2}`

// fixture is a store of storeSQL and queriesSQL in memory, and a handler over it.
type fixture struct {
	cat  *engine.Catalog
	mem  *mem.Mem
	meta *meta.Meta
	core *projector.Core
	seq  int
	logs bytes.Buffer
	h    *queryhttp.Handler
}

// newFixture returns a store with d1's readings, whose view versions are live unless
// pending.
func newFixture(t *testing.T, pending bool) *fixture {
	t.Helper()
	return newFixtureOn(t, pending, func(b store.Backend) store.Backend { return b })
}

// newFixtureOn is newFixture with a handler whose engine reads through wrap's backend.
func newFixtureOn(t *testing.T, pending bool, wrap func(store.Backend) store.Backend) *fixture {
	t.Helper()
	cat, err := engine.Compile(catalogtest.MarshalFS(t, fstest.MapFS{
		"dynaflux.yaml":        {Data: []byte("store: app\nregions: [" + region + "]\n")},
		"dynaflux/store.sql":   {Data: []byte(storeSQL)},
		"dynaflux/queries.sql": {Data: []byte(queriesSQL)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	naming := catalog.Naming{Prefix: "prod-", Store: "app"}
	m := mem.New()
	f := &fixture{
		cat: cat, mem: m, meta: meta.New(m, naming.MetaTable()),
		core: projector.New(cat.Projector(), m, nil, projector.Config{Naming: naming, Region: region}),
	}
	if !pending {
		if err = f.meta.SetLive(t.Context(), cat.Catalog()); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range readings {
		f.append(t, "readings", r...)
	}
	f.h, err = queryhttp.New(wrap(m), cat, queryhttp.Config{Region: region, Prefix: naming.Prefix, Log: slog.New(slog.NewJSONHandler(&f.logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// append appends an event of JSON values, its columns' then event_time, and projects it.
func (f *fixture) append(t *testing.T, stream string, vals ...string) {
	t.Helper()
	s, _ := f.cat.Catalog().Stream(stream)
	types := make([]catalog.Type, 0, len(s.Columns)+1)
	for _, c := range s.Columns {
		types = append(types, c.Type)
	}
	types = append(types, catalog.EventTimeType())
	ev := make([]expr.Value, len(vals))
	for i, v := range vals {
		var err error
		if ev[i], err = valuetext.ParseJSON(types[i], []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	res, err := engine.New(f.mem, engine.Options{Region: region, Prefix: "prod-"}).Append(t.Context(), f.cat, stream,
		[][]expr.Value{ev}, engine.AppendMode{})
	if err == nil {
		err = res[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	it, err := f.mem.Get(t.Context(), "prod-app-stream-"+stream, res[0].Key, true)
	if err != nil {
		t.Fatal(err)
	}
	f.seq++
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	r, err := f.core.Handle(ctx, projector.Batch{Stream: stream, Unit: "shardId-1", Records: []projector.Record{
		{Op: projector.Insert, Seq: fmt.Sprintf("1%039d", f.seq), New: it},
	}})
	if err != nil || r.FailedSeq != "" {
		t.Fatalf("project: %v %+v", err, r)
	}
}

// invoke calls the handler as Lambda does, with request ID req-1 and a deadline 25 s away.
func (f *fixture) invoke(t *testing.T, payload []byte) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	return f.h.Invoke(lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{AwsRequestID: "req-1"}), payload)
}

// logLines returns the handler's log lines, decoded.
func (f *fixture) logLines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.Lines(f.logs.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q isn't JSON: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// door builds the events of one front door and reads its responses.
type door struct {
	name  string
	event func(t *testing.T, method, path, body string, b64 bool) []byte
	read  func(t *testing.T, payload []byte) answer
}

// answer is a response as the caller sees it.
type answer struct {
	status  int
	headers map[string]string
	body    string
}

// route returns the API route of path, and its name parameter.
func route(path string) (string, map[string]string) {
	if name, ok := strings.CutPrefix(path, "/queries/"); ok {
		return "/queries/{name}", map[string]string{"name": name}
	}
	return path, nil
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func readJSON[T any](t *testing.T, payload []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(payload, &v); err != nil {
		t.Fatalf("response %s: %v", payload, err)
	}
	return v
}

var (
	functionURL = door{ //nolint:gochecknoglobals // constant test data
		name: "function_url",
		event: func(t *testing.T, method, path, body string, b64 bool) []byte {
			t.Helper()
			return mustJSON(t, events.LambdaFunctionURLRequest{
				Version: "2.0", RawPath: path, Headers: map[string]string{"content-type": "application/json"},
				RequestContext: events.LambdaFunctionURLRequestContext{
					RequestID: "id", HTTP: events.LambdaFunctionURLRequestContextHTTPDescription{Method: method, Path: path},
				},
				Body: body, IsBase64Encoded: b64,
			})
		},
		read: func(t *testing.T, payload []byte) answer {
			t.Helper()
			r := readJSON[events.LambdaFunctionURLResponse](t, payload)
			return answer{r.StatusCode, r.Headers, r.Body}
		},
	}
	httpAPI = door{ //nolint:gochecknoglobals // constant test data
		name: "http_api",
		event: func(t *testing.T, method, path, body string, b64 bool) []byte {
			t.Helper()
			res, params := route(path)
			key := method + " " + res
			return mustJSON(t, events.APIGatewayV2HTTPRequest{
				Version: "2.0", RouteKey: key, RawPath: path, PathParameters: params,
				RequestContext: events.APIGatewayV2HTTPRequestContext{
					RouteKey: key, Stage: "$default", HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{Method: method, Path: path},
				},
				Body: body, IsBase64Encoded: b64,
			})
		},
		read: func(t *testing.T, payload []byte) answer {
			t.Helper()
			r := readJSON[events.APIGatewayV2HTTPResponse](t, payload)
			return answer{r.StatusCode, r.Headers, r.Body}
		},
	}
	restAPI = door{ //nolint:gochecknoglobals // constant test data
		name: "rest_api",
		event: func(t *testing.T, method, path, body string, b64 bool) []byte {
			t.Helper()
			res, params := route(path)
			return mustJSON(t, events.APIGatewayProxyRequest{
				Resource: res, Path: path, HTTPMethod: method, PathParameters: params,
				RequestContext: events.APIGatewayProxyRequestContext{Stage: "api", ResourcePath: res, HTTPMethod: method},
				Body:           body, IsBase64Encoded: b64,
			})
		},
		read: func(t *testing.T, payload []byte) answer {
			t.Helper()
			r := readJSON[events.APIGatewayProxyResponse](t, payload)
			return answer{r.StatusCode, r.Headers, r.Body}
		},
	}
	httpDoors = []door{functionURL, httpAPI, restAPI} //nolint:gochecknoglobals // constant test data
)

// directPayload returns a direct invoke's payload; params "" leaves them out.
func directPayload(query, params string) []byte {
	if params == "" {
		return []byte(`{"query":"` + query + `"}`)
	}
	return []byte(`{"query":"` + query + `","params":` + params + `}`)
}

// readDirect reads a direct invoke's answer, whose status is in its body.
func readDirect(t *testing.T, payload []byte) answer {
	t.Helper()
	return answer{status: readJSON[struct {
		Status int `json:"status"`
	}](t, payload).Status, body: string(payload)}
}

// call sends a request to h through d and returns the answer.
func (f *fixture) call(t *testing.T, d door, method, path, body string) answer {
	t.Helper()
	out, err := f.invoke(t, d.event(t, method, path, body, false))
	if err != nil {
		t.Fatalf("%s: %v", d.name, err)
	}
	return d.read(t, out)
}

// headers are those of every HTTP answer, with extra.
func headers(extra ...string) map[string]string {
	h := map[string]string{"content-type": "application/json", "cache-control": "no-store", "x-dynaflux-region": region}
	for i := 0; i+1 < len(extra); i += 2 {
		h[extra[i]] = extra[i+1]
	}
	return h
}

// failing is a backend whose reads fail with err.
type failing struct {
	store.Backend

	err error
}

func (b failing) Get(context.Context, string, store.Key, bool) (store.Item, error) { return nil, b.err }

func (b failing) BatchGet(context.Context, string, []store.Key, bool) ([]store.Item, error) {
	return nil, b.err
}

func (b failing) Query(context.Context, string, store.Query) iter.Seq2[store.Item, error] {
	return func(yield func(store.Item, error) bool) { yield(nil, b.err) }
}

// blocking is a backend whose reads wait until their context ends.
type blocking struct{ store.Backend }

func (blocking) Query(ctx context.Context, _ string, _ store.Query) iter.Seq2[store.Item, error] {
	return func(yield func(store.Item, error) bool) {
		<-ctx.Done()
		yield(nil, ctx.Err())
	}
}

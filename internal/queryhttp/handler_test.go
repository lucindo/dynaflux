// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp_test

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/queryhttp"
	"github.com/lucindo/dynaflux/internal/store"
)

// errorJSON is the body of a failure of request req-1.
func errorJSON(status int, code, msg string) string {
	return fmt.Sprintf(`{"status":%d,"error":{"code":%q,"message":%q,"request_id":"req-1"}}`, status, code, msg)
}

// matchParams are Match's parameters for d1's first reading, with set's values in place.
func matchParams(set ...string) string {
	p := map[string]string{
		"device": `"d1"`, "level": `7`, "delta": `-42`, "total": `"18446744073709551615"`, "ratio": `0.25`,
		"price": `1234.5`, "ok": `true`, "batch": `"61f0c404-5cb3-11e7-907b-a6006ad3dba0"`, "day": `"2026-10-01"`,
		"at": `"2026-10-01T12:00:00.123Z"`, "since": `"2026-10-01T00:00:00Z"`,
	}
	for i := 0; i+1 < len(set); i += 2 {
		p[set[i]] = set[i+1]
	}
	parts := make([]string, 0, len(p))
	for _, k := range []string{"device", "level", "delta", "total", "ratio", "price", "ok", "batch", "day", "at", "since"} {
		parts = append(parts, fmt.Sprintf("%q:%s", k, p[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// Every door, direct invoke included, answers a request with the same status and the same
// body, byte for byte.
func TestDoorsAnswerAlike(t *testing.T) {
	t.Parallel()
	devices := make([]string, 101)
	for i := range devices {
		devices[i] = fmt.Sprintf(`"d%d"`, i)
	}
	const matched = `{"status":200,"meta":[{"name":"device","type":"String"}],"data":[{"device":"d1"}],"rows":1}`
	tests := []struct {
		name   string
		query  string
		params string // the body; "" for none
		status int
		body   string
	}{
		{"rows of every type", "Readings", `{"device":"d1"}`, http.StatusOK, readingsBody},
		{"no rows", "Readings", `{"device":"d2"}`, http.StatusOK,
			strings.Split(readingsBody, `"data":[`)[0] + `"data":[],"rows":0}`},
		{"every parameter type", "Match", matchParams(), http.StatusOK, matched},
		{"integers and decimals as strings", "Match", matchParams("level", `"7"`, "delta", `"-42"`, "price", `"1234.50"`),
			http.StatusOK, matched},
		{":one without a row", "DeviceTotal", `{"device":"d2"}`, http.StatusOK,
			`{"status":200,"meta":[{"name":"device","type":"String"},{"name":"readings","type":"UInt64"},` +
				`{"name":"sum_total","type":"UInt64"}],"data":[],"rows":0}`},
		{":one", "DeviceTotal", ` { "device" : "d1" } `, http.StatusOK,
			`{"status":200,"meta":[{"name":"device","type":"String"},{"name":"readings","type":"UInt64"},` +
				`{"name":"sum_total","type":"UInt64"}],"data":[{"device":"d1","readings":"2","sum_total":"18446744073709551615"}],"rows":1}`},
		{"duplicate key", "Readings", `{"device":"d1","device":"d2"}`, http.StatusBadRequest,
			errorJSON(400, "bad_request", `the parameters must be one JSON object: key "device" appears twice`)},
		{"not an object", "Readings", `["d1"]`, http.StatusBadRequest,
			errorJSON(400, "bad_request", "the parameters must be one JSON object: not a JSON object")},
		{"unknown parameter", "Readings", `{"device":"d1","devise":"d1"}`, http.StatusBadRequest,
			errorJSON(400, "invalid_params", `Readings has no parameter "devise"; did you mean device?`)},
		{"missing parameter", "Match", `{"device":"d1"}`, http.StatusBadRequest,
			errorJSON(400, "invalid_params", "Match needs level (UInt8), delta (Int64), total (UInt64), ratio (Float64), "+
				"price (Decimal(18, 2)), ok (Bool), batch (UUID), day (Date), at (DateTime64(3)), since (DateTime64(6))")},
		{"no parameters", "Readings", "", http.StatusBadRequest, errorJSON(400, "invalid_params", "Readings needs device (String)")},
		{"null", "Readings", `{"device":null}`, http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter device is null; give a String")},
		{"String as a number", "Match", matchParams("device", `1`), http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter device: invalid String: want a JSON string")},
		{"UInt8 out of range", "Match", matchParams("level", `300`), http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter level: invalid UInt8: UInt8 value 300 is out of range")},
		{"Int64 as a fraction", "Match", matchParams("delta", `1.5`), http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter delta: invalid Int64: want an integer")},
		{"Float64 as a string", "Match", matchParams("ratio", `"0.25"`), http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter ratio: invalid Float64: want a JSON number")},
		{"Decimal past its scale", "Match", matchParams("price", `1234.567`), http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter price: invalid Decimal(18, 2): more than 2 digits after the point")},
		{"Bool as a string", "Match", matchParams("ok", `"true"`), http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter ok: invalid Bool: want true or false")},
		{"bad UUID", "Match", matchParams("batch", `"61f0c404"`), http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter batch: invalid UUID: want 8-4-4-4-12 hex digits")},
		{"bad Date", "Match", matchParams("day", `"2026-10-01T00:00:00Z"`), http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter day: invalid Date: want YYYY-MM-DD")},
		{"seven fractional digits", "Match", matchParams("since", `"2026-10-01T00:00:00.1234567Z"`), http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter since: invalid DateTime64(6): want RFC 3339 with at most 6 fractional digits, such as 2026-10-26T13:45:00.250Z")},
		{"array of numbers", "DeviceTotals", `{"devices":[1]}`, http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter devices: invalid Array(String): element 1: want a JSON string")},
		{"too many keys", "DeviceTotals", `{"devices":[` + strings.Join(devices, ",") + `]}`, http.StatusBadRequest,
			errorJSON(400, "too_many_keys", "query DeviceTotals: too many key combinations: more than 100")},
		{"unexposed query", "Hidden", `{"device":"d1"}`, http.StatusNotFound, errorJSON(404, "not_found", `no exposed query "Hidden"`)},
		{"unknown query", "Nope", `{"device":"d1"}`, http.StatusNotFound, errorJSON(404, "not_found", `no exposed query "Nope"`)},
		{"exponent past int's range", "Share", `{"device":"d1","per":1e-9223372036854775808}`, http.StatusBadRequest,
			errorJSON(400, "invalid_params", "parameter per: invalid UInt64: want an integer")},
		{"evaluation error", "Share", `{"device":"d1","per":0}`, http.StatusInternalServerError,
			errorJSON(500, "evaluation_error", "query Share: evaluation error in intDiv: division by zero")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, false)
			for _, d := range httpDoors {
				got := f.call(t, d, http.MethodPost, "/queries/"+tt.query, tt.params)
				if want := (answer{tt.status, headers(), tt.body}); !equal(got, want) {
					t.Errorf("%s: %+v\nwant %+v", d.name, got, want)
				}
			}
			out, err := f.invoke(t, directPayload(tt.query, tt.params))
			if got := readDirect(t, out); err != nil || got.status != tt.status || got.body != tt.body {
				t.Errorf("direct: %+v, %v\nwant %d %s", got, err, tt.status, tt.body)
			}
		})
	}
}

func equal(a, b answer) bool {
	return a.status == b.status && a.body == b.body && maps.Equal(a.headers, b.headers)
}

// A REST or HTTP API, or a function URL, may send the body in base64.
func TestBase64Body(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	body := base64.StdEncoding.EncodeToString([]byte(`{"device":"d1"}`))
	for _, d := range httpDoors {
		out, err := f.invoke(t, d.event(t, http.MethodPost, "/queries/Readings", body, true))
		if got := d.read(t, out); err != nil || got.status != http.StatusOK || got.body != readingsBody {
			t.Errorf("%s: %+v, %v; want 200 and Readings' rows", d.name, got, err)
		}
		out, err = f.invoke(t, d.event(t, http.MethodPost, "/queries/Readings", "{not base64", true))
		want := errorJSON(400, "bad_request", "the body isn't valid base64: illegal base64 data at input byte 0")
		if got := d.read(t, out); err != nil || got.status != http.StatusBadRequest || got.body != want {
			t.Errorf("%s: %+v, %v; want %s", d.name, got, err, want)
		}
	}
}

// An HTTP body is one JSON object, whole.
func TestBodyIsOneObject(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	for body, msg := range map[string]string{
		`{"device":"d1"} {}`: "more than one JSON value",
		`{"device":`:         "unexpected EOF",
	} {
		got := f.call(t, functionURL, http.MethodPost, "/queries/Readings", body)
		if want := errorJSON(400, "bad_request", "the parameters must be one JSON object: "+msg); got.body != want {
			t.Errorf("%s: %+v\nwant %s", body, got, want)
		}
	}
}

// The handler answers 405 for the wrong method and 404 for an unknown path, on whatever door
// the request came through; only a function URL sends every method and path to it.
func TestRoutes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		method, path string
		want         answer
	}{
		{http.MethodGet, "/queries/Readings", answer{http.StatusMethodNotAllowed, headers("allow", "POST"),
			errorJSON(405, "method_not_allowed", "/queries/Readings takes POST, not GET")}},
		{http.MethodPost, "/health", answer{http.StatusMethodNotAllowed, headers("allow", "GET"),
			errorJSON(405, "method_not_allowed", "/health takes GET, not POST")}},
		{http.MethodPost, "/queries", answer{http.StatusNotFound, headers(),
			errorJSON(404, "not_found", "no route POST /queries: the routes are POST /queries/{name} and GET /health")}},
	}
	f := newFixture(t, false)
	for _, d := range []door{functionURL, restAPI} {
		for _, tt := range tests {
			if got := f.call(t, d, tt.method, tt.path, ""); !equal(got, tt.want) {
				t.Errorf("%s %s %s: %+v\nwant %+v", d.name, tt.method, tt.path, got, tt.want)
			}
		}
	}
}

// The handler reads a parameters object only up to its first key that isn't a parameter, or
// that repeats, so a body of many keys costs no more than one: past those keys, the HTTP
// bodies here aren't even JSON. The log names only the query's parameters.
func TestParametersStopAtStrayKey(t *testing.T) {
	t.Parallel()
	var keys strings.Builder
	for i := range 100_000 {
		fmt.Fprintf(&keys, `,"k%d":0`, i)
	}
	tests := []struct {
		name, head, want string
	}{
		{"unknown key", `{"device":"d1"`, errorJSON(400, "invalid_params", `Readings has no parameter "k0"`)},
		{"repeated key", `{"device":"d1","device":"d2"`,
			errorJSON(400, "bad_request", `the parameters must be one JSON object: key "device" appears twice`)},
	}
	for _, tt := range tests {
		f := newFixture(t, false)
		got := f.call(t, httpAPI, http.MethodPost, "/queries/Readings", tt.head+keys.String()+`,}`)
		if got.status != http.StatusBadRequest || got.body != tt.want {
			t.Errorf("%s: %+v\nwant %s", tt.name, got, tt.want)
		}
		// A direct invoke's params must be JSON, as part of the payload.
		out, err := f.invoke(t, directPayload("Readings", tt.head+keys.String()+`}`))
		if err != nil || string(out) != tt.want {
			t.Errorf("%s, direct: %s, %v\nwant %s", tt.name, out, err, tt.want)
		}
		for _, l := range f.logLines(t) {
			if fmt.Sprint(l["params"]) != "[device]" {
				t.Errorf("%s: log line %v, want params [device]", tt.name, l)
			}
		}
	}
	f := newFixture(t, false)
	out, err := f.invoke(t, []byte(`{"query":"Readings","x":1`+keys.String()+`,}`))
	if want := errorJSON(400, "bad_request", `unknown key "x": a direct invoke has "query" and "params"`); err != nil || string(out) != want {
		t.Errorf("direct payload: %s, %v\nwant %s", out, err, want)
	}
}

// A direct invoke's payload is {"query", "params"}, and nothing else.
func TestDirectPayload(t *testing.T) {
	t.Parallel()
	tests := []struct {
		payload, msg string
	}{
		{`[]`, `the payload must be one JSON object, {"query": <name>, "params": {…}}: not a JSON object`},
		{`{"params":{}}`, `the payload has no "query"`},
		{`{"query":1}`, `"query" must be a string`},
		{`{"query":"Readings","query":"Hidden"}`, `the payload must be one JSON object, {"query": <name>, "params": {…}}: key "query" appears twice`},
		{`{"query":"Readings","params":{"device":"d1"},"wait_for":"x"}`, `unknown key "wait_for": a direct invoke has "query" and "params"`},
		{`{"query":"Readings","params":null}`, "the parameters must be one JSON object: not a JSON object"},
		// An object without requestContext is no API Gateway or function URL event.
		{`{"httpMethod":"GET","resource":"/health"}`, `unknown key "httpMethod": a direct invoke has "query" and "params"`},
	}
	f := newFixture(t, false)
	for _, tt := range tests {
		out, err := f.invoke(t, []byte(tt.payload))
		if want := errorJSON(400, "bad_request", tt.msg); err != nil || string(out) != want {
			t.Errorf("%s: %s, %v\nwant %s", tt.payload, out, err, want)
		}
	}
}

// An event of a door the function doesn't serve is a function error, which the Errors
// alarm counts: it means a deployment the templates didn't make.
func TestUnknownEventFailsTheInvocation(t *testing.T) {
	t.Parallel()
	alb := `{"requestContext":{"elb":{"targetGroupArn":"arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/t/1"}},` +
		`"httpMethod":"GET","path":"/health","headers":{},"body":"","isBase64Encoded":false}`
	webSocket := `{"requestContext":{"routeKey":"$connect","eventType":"CONNECT","connectionId":"c1"},"isBase64Encoded":false}`
	f := newFixture(t, false)
	for _, ev := range []string{alb, webSocket} {
		out, err := f.invoke(t, []byte(ev))
		if out != nil || err == nil || !strings.Contains(err.Error(), "the event is from neither a function URL") {
			t.Errorf("%s: %s, %v; want a function error", ev, out, err)
		}
	}
}

// A view version that isn't live answers 503 with its state; a broken version's failure,
// which can quote an event, goes to the log only.
func TestViewNotReady(t *testing.T) {
	t.Parallel()
	f := newFixture(t, true)
	got := f.call(t, functionURL, http.MethodPost, "/queries/DeviceTotal", `{"device":"d1"}`)
	want := answer{http.StatusServiceUnavailable, headers(),
		errorJSON(503, "view_not_ready", "view version device_totals@v1 is pending_backfill in us-east-1")}
	if !equal(got, want) {
		t.Errorf("pending: %+v\nwant %+v", got, want)
	}

	f = newFixture(t, false)
	if _, _, err := f.meta.MarkBroken(t.Context(), version, "readings", meta.Failure{
		Cause: meta.CauseRecord, EventID: "01J9ZQ3ZQ8V7W6X5Y4Z3A2B1C0", Error: "sum overflows UInt64: secret",
	}); err != nil {
		t.Fatal(err)
	}
	got = f.call(t, functionURL, http.MethodPost, "/queries/DeviceTotal", `{"device":"d1"}`)
	want.body = errorJSON(503, "view_not_ready", "view version device_totals@v1 is broken in us-east-1")
	if !equal(got, want) {
		t.Errorf("broken: %+v\nwant %+v", got, want)
	}
	lines := f.logLines(t)
	if len(lines) != 1 || lines[0]["level"] != "WARN" || lines[0]["error"] != "dynaflux: query DeviceTotal: view version not ready: "+
		"device_totals@v1 is broken in us-east-1: event 01J9ZQ3ZQ8V7W6X5Y4Z3A2B1C0: sum overflows UInt64: secret" {
		t.Errorf("log lines %v, want the failure at WARN", lines)
	}
}

// Throttling left after the SDK's retries is a 503 to retry, not a 500.
func TestThrottled(t *testing.T) {
	t.Parallel()
	f := newFixtureOn(t, false, func(b store.Backend) store.Backend {
		return failing{Backend: b, err: fmt.Errorf("dynamo: query: %w", store.ErrThrottled)}
	})
	for _, d := range httpDoors {
		got := f.call(t, d, http.MethodPost, "/queries/Readings", `{"device":"d1"}`)
		want := answer{http.StatusServiceUnavailable, headers("retry-after", "1"),
			errorJSON(503, "throttled", "DynamoDB throttled the query's reads; retry after a second")}
		if !equal(got, want) {
			t.Errorf("%s: %+v\nwant %+v", d.name, got, want)
		}
	}
}

// Any other failure is a 500 whose details go to the log, with a QueryFailures metric that
// a raised log level keeps.
func TestInternalError(t *testing.T) {
	t.Parallel()
	f := newFixtureOn(t, false, func(b store.Backend) store.Backend {
		return failing{Backend: b, err: errors.New("AccessDeniedException: no dynamodb:Query on prod-app-stream-readings")}
	})
	got := f.call(t, restAPI, http.MethodPost, "/queries/Readings", `{"device":"d1"}`)
	want := answer{http.StatusInternalServerError, headers(),
		errorJSON(500, "internal", "internal error; the function's log has it under this request_id")}
	if !equal(got, want) {
		t.Errorf("%+v\nwant %+v", got, want)
	}
	lines := f.logLines(t)
	if len(lines) != 1 {
		t.Fatalf("log lines %v, want one", lines)
	}
	l := lines[0]
	emf := fmt.Sprint(l["_aws"])
	if l["level"] != "ERROR" || l["requestId"] != "req-1" || l["door"] != "rest_api" || l["code"] != "internal" ||
		l["QueryFailures"] != 1.0 || l["Store"] != "prod-app" ||
		l["error"] != "dynaflux: query Readings: AccessDeniedException: no dynamodb:Query on prod-app-stream-readings" ||
		!strings.Contains(emf, "map[Dimensions:[[Store]] Metrics:[map[Name:QueryFailures Unit:Count]] Namespace:Dynaflux]") {
		t.Errorf("log line %v, want the error and the QueryFailures metric", l)
	}
}

// The log names a request's parameters but never logs their values.
func TestLogLeavesOutValues(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	f.call(t, httpAPI, http.MethodPost, "/queries/Readings", `{"device":"secret-device"}`)
	f.call(t, httpAPI, http.MethodPost, "/queries/Match", matchParams("day", `"secret-day"`))
	if logs := f.logs.String(); strings.Contains(logs, "secret") {
		t.Errorf("the log has a parameter's value:\n%s", logs)
	}
	lines := f.logLines(t)
	if len(lines) != 2 || fmt.Sprint(lines[0]["params"]) != "[device]" || lines[0]["rows"] != 0.0 ||
		lines[1]["code"] != "invalid_params" || lines[1]["level"] != "INFO" {
		t.Errorf("log lines %v, want the parameters' names and the outcomes", lines)
	}
}

// The handler answers 504 a second before Lambda's deadline, so a slow query is an answer
// rather than a function timeout.
func TestDeadline(t *testing.T) {
	t.Parallel()
	// Built outside the bubble, whose clock reads 2000, before the readings' event_time.
	f := newFixtureOn(t, false, func(b store.Backend) store.Backend { return blocking{b} })
	synctest.Test(t, func(t *testing.T) {
		began := time.Now()
		got := f.call(t, functionURL, http.MethodPost, "/queries/Readings", `{"device":"d1"}`)
		want := answer{http.StatusGatewayTimeout, headers(),
			errorJSON(504, "timeout", "the query didn't finish before the function's deadline")}
		if !equal(got, want) || time.Since(began) != 24*time.Second {
			t.Errorf("after %v: %+v\nwant after 24s: %+v", time.Since(began), got, want)
		}
		if lines := f.logLines(t); len(lines) != 1 || lines[0]["QueryFailures"] != 1.0 {
			t.Errorf("log lines %v, want a QueryFailures count", lines)
		}
	})
}

// /health answers 200 while every view an exposed query reads is live, and otherwise 503
// without naming a version: it needs no auth on the API doors.
func TestHealth(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	for _, d := range httpDoors {
		if got, want := f.call(t, d, http.MethodGet, "/health", ""), (answer{http.StatusOK, headers(), `{"status":200,"region":"us-east-1"}`}); !equal(got, want) {
			t.Errorf("%s, live: %+v\nwant %+v", d.name, got, want)
		}
	}
	f = newFixture(t, true)
	for _, d := range httpDoors {
		if got, want := f.call(t, d, http.MethodGet, "/health", ""), (answer{http.StatusServiceUnavailable, headers(),
			`{"status":503,"error":{"code":"view_not_ready"}}`}); !equal(got, want) {
			t.Errorf("%s, pending: %+v\nwant %+v", d.name, got, want)
		}
	}
	if l := f.logLines(t)[0]; l["error"] != "view version not ready: device_totals@v1 is pending_backfill in us-east-1" {
		t.Errorf("log line %v, want the version's state", l)
	}
}

// When /health can't read the view states, it answers as a query does, with only the status
// and the code: throttling is a 503 to retry, anything else a 500 that QueryFailures counts.
func TestHealthReadFails(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		want     answer
		failures any // the log line's QueryFailures
	}{
		{"throttled", fmt.Errorf("dynamo: query: %w", store.ErrThrottled),
			answer{http.StatusServiceUnavailable, headers("retry-after", "1"), `{"status":503,"error":{"code":"throttled"}}`}, nil},
		{"denied", errors.New("AccessDeniedException: no dynamodb:Query on prod-app-meta"),
			answer{http.StatusInternalServerError, headers(), `{"status":500,"error":{"code":"internal"}}`}, 1.0},
	}
	for _, tt := range tests {
		f := newFixtureOn(t, false, func(b store.Backend) store.Backend { return failing{Backend: b, err: tt.err} })
		if got := f.call(t, restAPI, http.MethodGet, "/health", ""); !equal(got, tt.want) {
			t.Errorf("%s: %+v\nwant %+v", tt.name, got, tt.want)
		}
		if l := f.logLines(t)[0]; l["QueryFailures"] != tt.failures {
			t.Errorf("%s: log line %v, want QueryFailures %v", tt.name, l, tt.failures)
		}
	}
}

// A view that no exposed query reads doesn't make the region unhealthy.
func TestHealthReadsOnlyExposedViews(t *testing.T) {
	t.Parallel()
	f := newFixture(t, true)
	if _, err := f.meta.Activate(t.Context(), version, "readings", meta.SplitFence, meta.DefaultBounds); err != nil {
		t.Fatal(err)
	}
	if err := f.meta.MakeLive(t.Context(), version, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if got := f.call(t, functionURL, http.MethodGet, "/health", ""); got.status != http.StatusOK {
		t.Errorf("%+v, want 200", got)
	}
}

// Every response names the region and every request logs a line, so New needs both.
func TestNewNeedsRegionAndLog(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	for _, c := range []queryhttp.Config{
		{Prefix: "prod-", Log: slog.New(slog.DiscardHandler)},
		{Region: region, Prefix: "prod-"},
	} {
		if _, err := queryhttp.New(f.mem, f.cat, c); err == nil || err.Error() != "queryhttp: the config needs a region and a logger" {
			t.Errorf("New(%+v) = %v, want the config refused", c, err)
		}
	}
}

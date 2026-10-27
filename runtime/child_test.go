// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/setup"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/runtime"
)

// The test binary is also the runtime binary: with childVar set, TestMain serves as the
// child that the value names, instead of running tests.
const (
	childVar   = "DYNAFLUX_TEST_CHILD"
	catalogVar = "DYNAFLUX_TEST_CATALOG" // the catalog JSON that Run serves
	childRun   = "run"                   // what the generated main does
	childPanic = "panic"                 // a handler that panics
)

func TestMain(m *testing.M) {
	switch os.Getenv(childVar) {
	case childRun:
		if err := runtime.Run(fluxgen.LoadCatalog(os.Getenv(catalogVar))); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case childPanic:
		lambda.Start(func(context.Context) error { panic("boom") })
	default:
		os.Exit(runTests(m))
	}
}

// childSQL has a view without count or sum, which the projector applies while it is pending
// its backfill, so rows are written with no view state in the store.
const childSQL = `
CREATE STREAM clicks (user_id String, page String, ms UInt32)
PRIMARY KEY (user_id);

CREATE MATERIALIZED VIEW page_longest
ORDER BY (page)
AS SELECT page, max(ms) AS longest
FROM clicks
GROUP BY page;
`

// fakeDynamoDB answers as an empty DynamoDB that denies every write, with store app's stream
// table, and counts requests.
type fakeDynamoDB struct {
	srv   *httptest.Server
	calls atomic.Int64
}

func newFakeDynamoDB(t *testing.T) *fakeDynamoDB {
	t.Helper()
	f := &fakeDynamoDB{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			return
		}
		f.calls.Add(1)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch _, op, _ := strings.Cut(r.Header.Get("X-Amz-Target"), "."); op {
		case "Query":
			io.WriteString(w, `{"Count":0,"Items":[],"ScannedCount":0}`)
		case "BatchGetItem":
			io.WriteString(w, `{"Responses":{},"UnprocessedKeys":{}}`)
		case "PutItem":
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#AccessDeniedException","message":"fake"}`)
		case "DescribeTable":
			io.WriteString(w, `{"Table":{"TableName":"`+streamTable+`","TableStatus":"ACTIVE","LatestStreamArn":"`+streamARN+`",`+
				`"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

// runtimeAPI is a fake Lambda Runtime API. It serves events in order, each with a deadline
// timeout away, keeps what the runtime posts back, and notes how many DynamoDB requests had
// arrived by then.
type runtimeAPI struct {
	srv    *httptest.Server
	events [][]byte
	ddb    *fakeDynamoDB
	done   chan struct{} // closed once every event has an answer

	mu      sync.Mutex
	timeout time.Duration // the function's timeout: 30 s unless set
	next    int
	posts   []post
}

type post struct {
	path, body string
	ddbCalls   int64
}

func newRuntimeAPI(t *testing.T, ddb *fakeDynamoDB, events ...[]byte) *runtimeAPI {
	t.Helper()
	a := &runtimeAPI{events: events, ddb: ddb, done: make(chan struct{}), timeout: 30 * time.Second}
	a.srv = httptest.NewServer(http.HandlerFunc(a.serve))
	t.Cleanup(a.srv.Close)
	return a
}

func (a *runtimeAPI) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/2018-06-01/runtime/invocation/next" {
		a.mu.Lock()
		i, timeout := a.next, a.timeout
		a.next++
		a.mu.Unlock()
		if i >= len(a.events) {
			<-r.Context().Done() // until the child goes away
			return
		}
		w.Header().Set("Lambda-Runtime-Aws-Request-Id", "req-"+strconv.Itoa(i))
		w.Header().Set("Lambda-Runtime-Deadline-Ms", strconv.FormatInt(time.Now().Add(timeout).UnixMilli(), 10))
		w.Header().Set("Lambda-Runtime-Invoked-Function-Arn", "arn:aws:lambda:us-east-1:123456789012:function:app-projector")
		w.Write(a.events[i])
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.posts = append(a.posts, post{path: r.URL.Path, body: string(body), ddbCalls: a.ddb.calls.Load()})
	w.WriteHeader(http.StatusAccepted)
	if len(a.posts) == len(a.events) {
		close(a.done)
	}
}

func (a *runtimeAPI) state() (int, []post) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.next, a.posts
}

// child runs the test binary as the runtime binary in the environment of a projector of
// store app in us-east-1, built from scratch so that nothing reaches AWS. set changes it;
// "" removes a variable.
type child struct {
	cmd            *exec.Cmd
	stdout, stderr bytes.Buffer
	exited         chan struct{}
	err            error // of the process, once exited is closed
}

func startChild(t *testing.T, mode string, api *runtimeAPI, set map[string]string) *child {
	t.Helper()
	vars := map[string]string{"AWS_ENDPOINT_URL_DYNAMODB": api.ddb.srv.URL}
	maps.Copy(vars, set)
	return startChildAt(t, mode, api.srv.URL, vars)
}

// startChildAt starts a child whose Lambda Runtime API is at runtimeURL.
func startChildAt(t *testing.T, mode, runtimeURL string, set map[string]string) *child {
	t.Helper()
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{
		childVar:                      mode,
		catalogVar:                    string(childCatalog(t)),
		"AWS_LAMBDA_RUNTIME_API":      strings.TrimPrefix(runtimeURL, "http://"),
		"AWS_REGION":                  emulatortest.Region,
		"AWS_ACCESS_KEY_ID":           emulatortest.AccessKeyID,
		"AWS_SECRET_ACCESS_KEY":       "fake",
		"AWS_CONFIG_FILE":             empty,
		"AWS_SHARED_CREDENTIALS_FILE": empty,
		"AWS_EC2_METADATA_DISABLED":   "true",
		"AWS_ENDPOINT_URL_FIREHOSE":   "http://127.0.0.1:1",
		// Every other service, should a client of one be built.
		"AWS_ENDPOINT_URL": "http://127.0.0.1:1",
		"DYNAFLUX_MODE":    "projector",
		"DYNAFLUX_STORE":   "app",
	}
	maps.Copy(vars, set)
	var env []string
	for k, v := range vars {
		if v == "" {
			continue
		}
		if strings.HasPrefix(k, "AWS_ENDPOINT_URL") {
			if u, err := url.Parse(v); err != nil || u.Hostname() != "127.0.0.1" {
				t.Fatalf("%s=%s isn't a loopback endpoint", k, v)
			}
		}
		env = append(env, k+"="+v)
	}
	c := &child{exited: make(chan struct{})}
	c.cmd = exec.CommandContext(t.Context(), os.Args[0], "-test.run=^$")
	c.cmd.Env = env
	c.cmd.Stdout, c.cmd.Stderr = &c.stdout, &c.stderr
	if err := c.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		c.err = c.cmd.Wait()
		close(c.exited)
	}()
	t.Cleanup(func() { c.stop(t) })
	return c
}

// stop ends the child, if it still runs, and waits until it has exited and its output is
// written.
func (c *child) stop(t *testing.T) {
	t.Helper()
	if err := c.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Error(err)
	}
	<-c.exited
}

// serve waits until the runtime API has an answer to every event, or the child exits, and
// then stops the child.
func (c *child) serve(t *testing.T, api *runtimeAPI) {
	t.Helper()
	select {
	case <-api.done:
	case <-c.exited:
	case <-time.After(30 * time.Second):
		c.stop(t)
		t.Fatalf("the child didn't answer; stderr:\n%s", &c.stderr)
	}
	c.stop(t)
}

// childCatalog returns the catalog JSON of store app, with no archive.
func childCatalog(t *testing.T) []byte {
	t.Helper()
	return catalogtest.MarshalFS(t, fstest.MapFS{
		"dynaflux.yaml":      {Data: []byte("store: app\nregions: [us-east-1]\narchive: {enabled: false}\n")},
		"dynaflux/store.sql": {Data: []byte(childSQL)},
	})
}

// childClick returns the event of one click on store app's stream, as Lambda delivers it.
func childClick(t *testing.T, shardID string) []byte {
	t.Helper()
	cat, err := engine.Compile(childCatalog(t))
	if err != nil {
		t.Fatal(err)
	}
	m := mem.New()
	res, err := engine.New(m, engine.Options{Region: "us-west-2"}).Append(t.Context(), cat, "clicks",
		[][]expr.Value{{expr.String("u1"), expr.String("/a"), expr.Uint(100), expr.Null()}}, engine.AppendMode{})
	if err == nil {
		err = res[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	it, err := m.Get(t.Context(), streamTable, res[0].Key, true)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(strings.Replace(string(event(insert(seq(1), it))), `"shardId":"`+shard+`"`, `"shardId":"`+shardID+`"`, 1))
}

// Run on aws-lambda-go's loop: a partial failure, a final invocation and a batch without
// shardId, each answered as Lambda expects, with LoadDefaultConfig reaching DynamoDB at
// AWS_ENDPOINT_URL_DYNAMODB.
func TestRunServesInvocations(t *testing.T) {
	t.Parallel()
	ddb := newFakeDynamoDB(t)
	api := newRuntimeAPI(t, ddb, childClick(t, shard), event(), childClick(t, ""))
	c := startChild(t, childRun, api, nil)
	c.serve(t, api)
	_, posts := api.state()
	if len(posts) != 3 {
		t.Fatalf("posts %+v, want 3; stderr:\n%s", posts, &c.stderr)
	}
	want := []post{
		{path: "/2018-06-01/runtime/invocation/req-0/response", body: `{"state":{},"batchItemFailures":[{"itemIdentifier":"` + seq(1) + `"}]}`},
		{path: "/2018-06-01/runtime/invocation/req-1/response", body: finalReply},
		{path: "/2018-06-01/runtime/invocation/req-2/error", body: `{"errorMessage":"runtime: the event has records but no shardId: ` +
			`the event source mapping needs TumblingWindowInSeconds (ADR 0003)","errorType":"errorString"}`},
	}
	for i, p := range posts {
		if p.path != want[i].path || strings.TrimSpace(p.body) != want[i].body {
			t.Errorf("post %d: %s %s\nwant %s %s", i, p.path, p.body, want[i].path, want[i].body)
		}
	}
	if first := posts[0].ddbCalls; first == 0 || posts[2].ddbCalls != first {
		t.Errorf("DynamoDB requests by each post: %d, %d, %d; want some for the batch, then none",
			posts[0].ddbCalls, posts[1].ddbCalls, posts[2].ddbCalls)
	}
	var lines []map[string]any
	for line := range strings.Lines(c.stdout.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("stdout line %q isn't JSON: %v", line, err)
		}
		lines = append(lines, m)
	}
	if len(lines) != 2 || lines[0]["level"] != "WARN" || lines[0]["requestId"] != "req-0" || lines[0]["failedSeq"] != seq(1) ||
		lines[1]["level"] != "ERROR" || lines[1]["reason"] != "event" || lines[1]["requestId"] != "req-2" {
		t.Errorf("stdout lines %v, want the partial failure at WARN and the event's failure at ERROR", lines)
	}
}

// Run in setup mode answers a stream lookup's Create on aws-lambda-go's loop: it describes
// the table at AWS_ENDPOINT_URL_DYNAMODB, PUTs the reply to the response URL, and then
// reports the invocation done.
func TestRunServesSetup(t *testing.T) {
	t.Parallel()
	r := newReplies(t)
	ev, err := json.Marshal(cfnEvent(t, "Create", setup.TypeStreamARN, "LookupClicks", "", lookupProps, r.url()))
	if err != nil {
		t.Fatal(err)
	}
	ddb := newFakeDynamoDB(t)
	api := newRuntimeAPI(t, ddb, ev)
	api.mu.Lock()
	api.timeout = 900 * time.Second // the setup function's
	api.mu.Unlock()
	c := startChild(t, childRun, api, map[string]string{"DYNAFLUX_MODE": "setup"})
	c.serve(t, api)
	if _, posts := api.state(); len(posts) != 1 || posts[0].path != "/2018-06-01/runtime/invocation/req-0/response" ||
		posts[0].body != `""` || posts[0].ddbCalls != 1 {
		t.Errorf("posts %+v, want the invocation done after one DynamoDB request; stderr:\n%s", posts, &c.stderr)
	}
	want := `{"Status":"SUCCESS","RequestId":"` + requestID + `","LogicalResourceId":"LookupClicks","StackId":"` + stackID +
		`","PhysicalResourceId":"` + streamARN + `","Data":{"StreamArn":"` + streamARN + `"}}`
	if got := r.all(); len(got) != 1 || got[0].body != want {
		t.Errorf("replies %+v\nwant\n%s", got, want)
	}
}

// A panic in a handler is reported, and ends the process: nothing it started can run on in
// a frozen environment.
func TestRunExitsOnPanic(t *testing.T) {
	t.Parallel()
	api := newRuntimeAPI(t, newFakeDynamoDB(t), event())
	c := startChild(t, childPanic, api, nil)
	select {
	case <-c.exited:
	case <-time.After(30 * time.Second):
		t.Fatal("the child didn't exit after the panic")
	}
	if _, posts := api.state(); len(posts) != 1 || !strings.HasSuffix(posts[0].path, "/req-0/error") ||
		!strings.Contains(posts[0].body, `"errorMessage":"boom"`) {
		t.Errorf("posts %+v, want the panic reported", posts)
	}
	var exit *exec.ExitError
	if !errors.As(c.err, &exit) || exit.ExitCode() == 0 {
		t.Errorf("the child ended with %v, want a failed exit", c.err)
	}
}

// Run refuses, before asking for an invocation, to serve where it can't keep its promises.
func TestRunRefusesToStart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		set  map[string]string
		err  string
	}{
		{"not on Lambda", map[string]string{"AWS_LAMBDA_RUNTIME_API": ""},
			"runtime: AWS_LAMBDA_RUNTIME_API isn't set: the runtime binary runs only on AWS Lambda\n"},
		{"Lambda Managed Instances", map[string]string{"AWS_LAMBDA_MAX_CONCURRENCY": "2"},
			"runtime: AWS_LAMBDA_MAX_CONCURRENCY is 2: the projector needs on-demand Lambda, one invocation per process\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			api := newRuntimeAPI(t, newFakeDynamoDB(t), event())
			c := startChild(t, childRun, api, tt.set)
			<-c.exited
			var exit *exec.ExitError
			if !errors.As(c.err, &exit) || exit.ExitCode() != 1 || c.stderr.String() != tt.err {
				t.Errorf("the child ended with %v and stderr %q, want exit status 1 and %q", c.err, &c.stderr, tt.err)
			}
			if next, _ := api.state(); next != 0 {
				t.Errorf("the child asked for %d invocations, want none", next)
			}
		})
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/esmlocal"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// newEmulator runs the e2e store on DynamoDB Local. Each region is a database of its own,
// so nothing replicates, and its projector is the Lambda binary in a child process.
func newEmulator(t *testing.T, p *project) *harness {
	t.Helper()
	h := &harness{p: p}
	for _, name := range p.def.Regions {
		cfg := emulatortest.Config(t, nil)
		cfg.Region = name
		r := &region{
			name: name, cfg: cfg, naming: emulatortest.Tables(t, cfg, p.manifest),
			ddb: dynamodb.NewFromConfig(cfg), streams: dynamodbstreams.NewFromConfig(cfg),
		}
		r.meta = meta.New(dynamo.New(r.ddb), r.naming.MetaTable())
		err := setLive(t.Context(), r, p.def)
		if err != nil {
			t.Fatal(err)
		}
		if r.store, err = dynaflux.Open(dynaflux.Config{AWS: cfg, Prefix: r.naming.Prefix}); err != nil {
			t.Fatal(err)
		}
		r.client = flux.New(r.store)
		sink := newSink(t)
		fn := startProjector(t, p, r, sink.url)
		r.logs = fn.output
		r.settle = settler(t, p, r, fn)
		r.archive = func(_ context.Context, stream, key, prefix string) (archived, error) {
			st, ok := p.def.Stream(stream)
			if !ok {
				return archived{}, fmt.Errorf("no stream %s", stream)
			}
			return fromRecords(st, sink.records(r.naming.ArchiveStream(stream)), key, prefix)
		}
		h.regions = append(h.regions, r)
	}
	return h
}

// setLive marks every view version live on a new store, before its first append, in place of
// the backfill that DynamoDB Local has no state machine for. A store with view states is an
// earlier run's, and keeps them.
func setLive(ctx context.Context, r *region, cat *catalog.Catalog) error {
	states, err := r.meta.ViewStates(ctx)
	if err != nil || len(states) > 0 {
		return err
	}
	return r.meta.SetLive(ctx, cat)
}

// settler returns a region's settle: it drains each stream through the projector. Drain
// retries a failed batch, but nothing injects faults here, so settle fails once an
// invocation fails or reports failed records.
func settler(t *testing.T, p *project, r *region, fn *projector) func(context.Context) error {
	t.Helper()
	readers := map[string]*esmlocal.Reader{}
	for _, st := range p.def.Streams {
		readers[st.Name] = esmlocal.NewReader(r.streams, streamARN(t, r, st.Name))
	}
	var failed error
	strict := func(ctx context.Context, event []byte) ([]byte, error) {
		out, err := fn.invoke(ctx, event)
		var resp struct {
			Failures []json.RawMessage `json:"batchItemFailures"`
		}
		switch {
		case failed != nil:
		case err != nil:
			failed = fmt.Errorf("an invocation failed: %w", err)
		case json.Unmarshal(out, &resp) != nil || len(resp.Failures) > 0:
			failed = fmt.Errorf("an invocation reported failed records: %s", out)
		}
		return out, err
	}
	return func(ctx context.Context) error {
		for _, st := range p.def.Streams {
			err := esmlocal.Drain(ctx, readers[st.Name], fn.timeout, strict)
			if err == nil {
				err = failed
			}
			if err != nil {
				return fmt.Errorf("%s: %w; the projector's output:\n%s", st.Name, err, fn.output())
			}
		}
		return nil
	}
}

func streamARN(t *testing.T, r *region, stream string) string {
	t.Helper()
	out, err := r.ddb.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: new(r.naming.StreamTable(stream))})
	if err != nil {
		t.Fatal(err)
	}
	return aws.ToString(out.Table.LatestStreamArn)
}

// projector is a region's projector function: the test binary as the Lambda binary, with
// the environment the templates give the function, behind a fake Lambda Runtime API.
type projector struct {
	timeout time.Duration
	srv     *httptest.Server
	next    chan invocation
	exited  chan struct{}

	mu      sync.Mutex
	ids     int
	replies map[string]chan reply
	out     bytes.Buffer // the child's output: the function's log lines, and why it stopped
}

type invocation struct {
	id       string
	deadline time.Time
	event    []byte
}

type reply struct {
	body   []byte
	failed bool
}

func startProjector(t *testing.T, p *project, r *region, sinkURL string) *projector {
	t.Helper()
	fn := projectorFunction(t, p.manifest)
	f := &projector{
		timeout: time.Duration(fn.TimeoutSeconds) * time.Second,
		next:    make(chan invocation), exited: make(chan struct{}), replies: map[string]chan reply{},
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// Built from scratch, so that nothing reaches AWS: every endpoint is on loopback.
	vars := map[string]string{
		childVar:                      "1",
		catalogVar:                    string(p.json),
		"AWS_LAMBDA_RUNTIME_API":      strings.TrimPrefix(f.srv.URL, "http://"),
		"AWS_REGION":                  r.name,
		"AWS_ACCESS_KEY_ID":           emulatortest.AccessKeyID,
		"AWS_SECRET_ACCESS_KEY":       "fake",
		"AWS_CONFIG_FILE":             empty,
		"AWS_SHARED_CREDENTIALS_FILE": empty,
		"AWS_EC2_METADATA_DISABLED":   "true",
		"AWS_ENDPOINT_URL_DYNAMODB":   os.Getenv("DYNAFLUX_DYNAMODB_ENDPOINT"),
		"AWS_ENDPOINT_URL_FIREHOSE":   sinkURL,
		"AWS_ENDPOINT_URL":            "http://127.0.0.1:1",
		// go test -race builds the child with the race detector too: it stops at the
		// first race, and the next invocation fails with its report.
		"GORACE": "halt_on_error=1",
	}
	maps.Copy(vars, functionEnv(t, p, fn, r))
	env := make([]string, 0, len(vars))
	for k, v := range vars {
		env = append(env, k+"="+v)
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^$")
	logs := &lockedWriter{mu: &f.mu, w: &f.out}
	cmd.Env, cmd.Stdout, cmd.Stderr = env, logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		err := cmd.Wait()
		fmt.Fprintf(&lockedWriter{mu: &f.mu, w: &f.out}, "exited: %v\n", err)
		close(f.exited)
	}()
	t.Cleanup(func() {
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
		<-f.exited
		switch out := f.output(); {
		case strings.Contains(out, "WARNING: DATA RACE"):
			t.Errorf("the projector of %s raced:\n%s", r.name, out)
		case t.Failed():
			t.Logf("the projector of %s logged:\n%s", r.name, out)
		}
	})
	return f
}

// projectorFunction returns the manifest's projector, the one function of runtime.split
// store.
func projectorFunction(t *testing.T, m *infra.Manifest) infra.Function {
	t.Helper()
	for _, fn := range m.Regional.Functions {
		if fn.Mode == catalog.EnvModeProjector {
			return fn
		}
	}
	t.Fatal("the manifest has no projector function")
	return infra.Function{}
}

// functionEnv resolves the function's environment in a region: the prefix, and the archive
// schema's fingerprint, which the setup function returns on AWS.
func functionEnv(t *testing.T, p *project, fn infra.Function, r *region) map[string]string {
	t.Helper()
	env := map[string]string{}
	for k, text := range fn.Env {
		segs, err := text.Segments()
		if err != nil {
			t.Fatal(err)
		}
		var b strings.Builder
		for _, seg := range segs {
			switch {
			case seg.Placeholder == infra.PlaceholderPrefix:
				b.WriteString(r.naming.Prefix)
			case seg.Ref.Attr == infra.AttrFingerprint:
				b.WriteString(p.fingerprint(r.name))
			case seg.Placeholder == "" && seg.Ref == infra.Ref{}:
				b.WriteString(seg.Literal)
			default:
				t.Fatalf("%s=%s: the emulator can't resolve %+v", k, text, seg)
			}
		}
		env[k] = b.String()
	}
	return env
}

func (f *projector) output() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.out.String()
}

// invoke is an esmlocal.Function: one invocation of the child, with ctx's deadline.
func (f *projector) invoke(ctx context.Context, event []byte) ([]byte, error) {
	deadline, _ := ctx.Deadline()
	ch := make(chan reply, 1)
	f.mu.Lock()
	f.ids++
	id := "req-" + strconv.Itoa(f.ids)
	f.replies[id] = ch
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.replies, id)
		f.mu.Unlock()
	}()
	select {
	case f.next <- invocation{id: id, deadline: deadline, event: event}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.exited:
		return nil, errors.New("the projector exited")
	}
	select {
	case rep := <-ch:
		if rep.failed {
			return nil, fmt.Errorf("%s", rep.body)
		}
		return rep.body, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.exited:
		return nil, errors.New("the projector exited")
	}
}

// serve is the fake Lambda Runtime API.
func (f *projector) serve(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet && req.URL.Path == "/2018-06-01/runtime/invocation/next" {
		select {
		case inv := <-f.next:
			w.Header().Set("Lambda-Runtime-Aws-Request-Id", inv.id)
			w.Header().Set("Lambda-Runtime-Deadline-Ms", strconv.FormatInt(inv.deadline.UnixMilli(), 10))
			w.Header().Set("Lambda-Runtime-Invoked-Function-Arn", "arn:aws:lambda:local:000000000000:function:projector")
			w.Write(inv.event)
		case <-req.Context().Done():
		}
		return
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return
	}
	w.WriteHeader(http.StatusAccepted)
	rest, ok := strings.CutPrefix(req.URL.Path, "/2018-06-01/runtime/invocation/")
	id, kind, _ := strings.Cut(rest, "/")
	f.mu.Lock()
	ch := f.replies[id]
	f.mu.Unlock()
	if ok && ch != nil {
		ch <- reply{body: body, failed: kind == "error"}
	}
}

// lockedWriter writes to w under mu.
type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// sink is a fake Firehose that accepts every record, keeping them by Firehose stream.
type sink struct {
	url string

	mu   sync.Mutex
	recs map[string][][]byte
}

func newSink(t *testing.T) *sink {
	t.Helper()
	s := &sink{recs: map[string][][]byte{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var in struct {
			DeliveryStreamName string `json:"DeliveryStreamName"`
			Records            []struct {
				Data []byte `json:"Data"`
			} `json:"Records"`
		}
		if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		type response struct {
			RecordID string `json:"RecordId"`
		}
		out := struct {
			FailedPutCount   int        `json:"FailedPutCount"`
			RequestResponses []response `json:"RequestResponses"`
		}{RequestResponses: make([]response, len(in.Records))}
		s.mu.Lock()
		for i, rec := range in.Records {
			s.recs[in.DeliveryStreamName] = append(s.recs[in.DeliveryStreamName], rec.Data)
			out.RequestResponses[i].RecordID = strconv.Itoa(len(s.recs[in.DeliveryStreamName]))
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		if err := json.NewEncoder(w).Encode(out); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(srv.Close)
	s.url = srv.URL
	return s
}

func (s *sink) records(stream string) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recs[stream]
}

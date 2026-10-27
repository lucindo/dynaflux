// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"maps"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	fh "github.com/aws/aws-sdk-go-v2/service/firehose"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/archive/firehose"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/esmlocal"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
	"github.com/lucindo/dynaflux/internal/validate"
	"github.com/lucindo/dynaflux/runtime"
)

// The suite's streams. Both views of quota_events count and sum, so they need the fenced
// handoff.
const (
	quotaEvents = "quota_events"
	pageViews   = "page_views"
)

const (
	// appendRegion is the engine's: DynamoDB Local's records say ddblocal, and the projector
	// runs in emulatortest.Region.
	appendRegion    = "us-west-2"
	functionTimeout = 30 * time.Second
	success         = `{"state":{},"batchItemFailures":[]}`
)

var errInjected = errors.New("injected failure")

// partial is the reply that asks Lambda to deliver the batch again from seq.
func partial(seq string) string {
	return `{"state":{},"batchItemFailures":[{"itemIdentifier":"` + seq + `"}]}`
}

// rules is the suite: runTests loads it before the tests run, and checks the calls of its
// functions against the projector's grants after.
var rules *suite //nolint:gochecknoglobals // runTests reads every test's calls after the run

// suite is the store every rule test projects, its templates' manifest, and the calls of
// every projector function the tests ran.
type suite struct {
	cat         *engine.Catalog
	manifest    *infra.Manifest
	fingerprint string // DYNAFLUX_ARCHIVE_SCHEMA in emulatortest.Region

	mu    sync.Mutex
	calls []deployedCalls
}

// deployedCalls are the calls of one projector function, on the tables of prefix.
type deployedCalls struct {
	prefix string
	calls  []emulatortest.Call
}

func (s *suite) record(prefix string, calls []emulatortest.Call) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, deployedCalls{prefix: prefix, calls: calls})
}

// simStore is the simulation harness's store: the suite projects the catalog the harness
// proves exact.
const simStore = "../internal/simulation/testdata/sim"

// suiteConfig extends the harness's dynaflux.yaml: an archive of every event, and the
// shortest prune window that a 30 s function allows.
const suiteConfig = "runtime: {timeout_seconds: 30, prune_window_seconds: 90}\narchive: {enabled: true, events: all}\n"

// loadSuite compiles the harness's store with suiteConfig and builds its manifest.
func loadSuite() (*suite, error) {
	root := os.DirFS(simStore)
	fsys := fstest.MapFS{}
	err := fs.WalkDir(root, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := fs.ReadFile(root, path)
		fsys[path] = &fstest.MapFile{Data: data}
		return err
	})
	if err != nil {
		return nil, err
	}
	fsys["dynaflux.yaml"].Data = append(fsys["dynaflux.yaml"].Data, suiteConfig...)
	cfg, def, ds := validate.Load(fsys, "/project")
	if def == nil {
		var b bytes.Buffer
		if err = diag.Write(&b, ds); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("the store doesn't compile:\n%s", b.Bytes())
	}
	data, err := catalog.Marshal(def)
	if err != nil {
		return nil, err
	}
	cat, err := engine.Compile(data)
	if err != nil {
		return nil, err
	}
	m, err := infra.Build(cfg, def, infra.Options{})
	if err != nil {
		return nil, err
	}
	return &suite{cat: cat, manifest: m, fingerprint: archive.SchemaFor(def, emulatortest.Region).Fingerprint()}, nil
}

// localRig runs projector functions of emulatortest.Region on DynamoDB Local, over tables of
// its own, for the suite's store, whose events an engine appends in appendRegion.
type localRig struct {
	t      *testing.T
	naming catalog.Naming
	// cfg, db and b are the test's own: the projector's grants don't cover what they send.
	cfg     aws.Config
	db      *dynamodb.Client
	b       store.Backend
	e       *engine.Engine
	meta    *meta.Meta
	fh      *fakeFirehose
	logs    *logBuffer
	fn      *function // a function without faults
	streams map[string]*localStream
}

// localStream is a stream table's stream and its reader, which starts at TRIM_HORIZON.
type localStream struct {
	arn string
	r   *esmlocal.Reader
}

// newLocalRig returns a rig with every view version live except those of pending, which stay
// pending_backfill.
func newLocalRig(t *testing.T, pending ...string) *localRig {
	t.Helper()
	cfg := emulatortest.Config(t, nil)
	db := dynamodb.NewFromConfig(cfg)
	r := &localRig{
		t: t, naming: emulatortest.Tables(t, cfg, rules.manifest), cfg: cfg, db: db, b: dynamo.New(db),
		fh: newFakeFirehose(t), logs: &logBuffer{}, streams: map[string]*localStream{},
	}
	r.e = engine.New(r.b, engine.Options{Region: appendRegion, Prefix: r.naming.Prefix})
	r.meta = meta.New(r.b, r.naming.MetaTable())
	for _, v := range rules.cat.Catalog().Views {
		for _, vv := range v.Versions {
			id := catalog.VersionID(v.Name, vv.Number)
			if slices.Contains(pending, id) {
				continue
			}
			if _, err := r.meta.Activate(t.Context(), id, vv.Stream, meta.SplitFence, meta.DefaultBounds); err != nil {
				t.Fatal(err)
			}
			if err := r.meta.MakeLive(t.Context(), id, time.Time{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	r.fn = r.function(faults{})
	return r
}

// function is a projector function in an execution environment of its own, as Run builds
// it, with its own core and view-state cache. rec sees each attempt it sends to DynamoDB
// and Firehose.
type function struct {
	invoke esmlocal.Function
	rec    *emulatortest.Recorder
}

// faults are a function's injected failures, each nil for none. put and query run before
// the backend sends a Put or a Query, and an error they return fails the call unsent. lose
// reports the attempts whose response is lost after the server applied them.
type faults struct {
	put   func(ctx context.Context, table string, it store.Item) error
	query func(table string) error
	lose  func(target string, body []byte) bool
}

func (r *localRig) function(f faults) *function {
	ddb, _ := sdkOptions(r.t, "")
	next := ddb.HTTPClient
	if f.lose != nil {
		next = lossy{next: next, lose: f.lose}
	}
	rec := emulatortest.NewRecorder(next)
	prefix := r.naming.Prefix
	r.t.Cleanup(func() { rules.record(prefix, rec.Calls()) })
	cfg := emulatortest.Config(r.t, rec)
	b := faulty{Backend: dynamo.New(dynamodb.NewFromConfig(cfg)), f: f}
	firehoseClient := fh.NewFromConfig(cfg, func(o *fh.Options) { o.BaseEndpoint = &r.fh.url })
	env := runtime.Env{Region: emulatortest.Region, Prefix: prefix, ArchiveSchema: rules.fingerprint}
	return &function{invoke: runtime.NewProjector(rules.cat, env, b, firehose.SDK{Client: firehoseClient}, r.logs), rec: rec}
}

// faulty is a backend with faults' put and query.
type faulty struct {
	store.Backend

	f faults
}

func (b faulty) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if b.f.put != nil {
		if err := b.f.put(ctx, table, it); err != nil {
			return err
		}
	}
	return b.Backend.Put(ctx, table, it, c)
}

func (b faulty) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	if b.f.query != nil {
		if err := b.f.query(table); err != nil {
			return func(yield func(store.Item, error) bool) { yield(nil, err) }
		}
	}
	return b.Backend.Query(ctx, table, q)
}

// lossy loses the response of each attempt that lose reports, after the server applied it,
// as a network that drops it would: the SDK sees a connection error and retries.
type lossy struct {
	next aws.HTTPClient
	lose func(target string, body []byte) bool
}

func (l lossy) Do(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	req = req.Clone(req.Context())
	req.Body = io.NopCloser(bytes.NewReader(body))
	res, err := l.next.Do(req)
	if err != nil || !l.lose(req.Header.Get("X-Amz-Target"), body) {
		return res, err
	}
	res.Body.Close()
	return nil, errors.New("read: connection reset by peer")
}

// isRow reports whether a Put of it to table writes a view row: neither a meta item nor a
// uniqExact marker.
func (r *localRig) isRow(table string, it store.Item) bool {
	_, marker := it[catalog.AttrMarkerOwner]
	return table != r.naming.MetaTable() && !marker
}

// stream returns a stream table's stream.
func (r *localRig) stream(name string) *localStream {
	r.t.Helper()
	if s, ok := r.streams[name]; ok {
		return s
	}
	table := r.naming.StreamTable(name)
	out, err := r.db.DescribeTable(r.t.Context(), &dynamodb.DescribeTableInput{TableName: &table})
	if err != nil {
		r.t.Fatal(err)
	}
	arn := aws.ToString(out.Table.LatestStreamArn)
	s := &localStream{arn: arn, r: esmlocal.NewReader(dynamodbstreams.NewFromConfig(r.cfg), arn)}
	r.streams[name] = s
	return s
}

// next returns the records of a stream written since the last call, and the stream's one
// shard: DynamoDB Local doesn't split streams (docs/decisions/0009).
func (r *localRig) next(stream string) (string, []json.RawMessage) {
	r.t.Helper()
	recs, err := r.stream(stream).r.Next(r.t.Context())
	if err != nil {
		r.t.Fatal(err)
	}
	shards := slices.Collect(maps.Keys(recs))
	if len(shards) != 1 {
		r.t.Fatalf("records of shards %q, want one shard's", shards)
	}
	return shards[0], recs[shards[0]]
}

// event renders an invocation's event of a stream's records from unit.
func (r *localRig) event(stream, unit string, recs ...json.RawMessage) []byte {
	r.t.Helper()
	return esmlocal.Event(unit, r.stream(stream).arn, recs...)
}

// call invokes fn with event and a deadline functionTimeout away, as Lambda does.
func (r *localRig) call(fn *function, event []byte) (string, error) {
	ctx, cancel := context.WithTimeout(r.t.Context(), functionTimeout)
	defer cancel()
	reply, err := fn.invoke(lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{AwsRequestID: "req-1"}), event)
	return string(reply), err
}

// deliver calls fn with a batch of a stream's records from unit, and fails the test unless
// every record succeeded.
func (r *localRig) deliver(fn *function, stream, unit string, recs ...json.RawMessage) {
	r.t.Helper()
	if reply, err := r.call(fn, r.event(stream, unit, recs...)); reply != success || err != nil {
		r.t.Fatalf("reply %s, %v; want %s", reply, err, success)
	}
}

// deliverAll calls each function with its events in turn, all functions at once, and fails
// the test unless every record succeeded.
func (r *localRig) deliverAll(events map[*function][][]byte) {
	var wg sync.WaitGroup
	for fn, evs := range events {
		wg.Go(func() {
			for _, ev := range evs {
				if reply, err := r.call(fn, ev); reply != success || err != nil {
					r.t.Errorf("reply %s, %v; want %s", reply, err, success)
				}
			}
		})
	}
	wg.Wait()
}

// quota appends a request of account for units, and returns the event's key.
func (r *localRig) quota(account, request string, units uint64) store.Key {
	r.t.Helper()
	return r.append(quotaEvents, expr.String(account), expr.String(request), expr.Uint(units))
}

// pageView appends a view of page on site s1 by user that took ms, with tags.
func (r *localRig) pageView(page, user string, ms uint64, tags ...string) store.Key {
	r.t.Helper()
	ts := make([]expr.Value, len(tags))
	for i, tag := range tags {
		ts[i] = expr.String(tag)
	}
	return r.append(pageViews, expr.String("s1"), expr.String(page), expr.String(user), expr.Uint(ms),
		expr.Decimal(big.NewInt(100)), expr.Float(0.5), expr.String("label"), expr.Array(ts))
}

// append appends an event with vals and event_time now.
func (r *localRig) append(stream string, vals ...expr.Value) store.Key {
	r.t.Helper()
	vals = append(vals, expr.Micros(time.Now().UnixMicro()))
	res, err := r.e.Append(r.t.Context(), rules.cat, stream, [][]expr.Value{vals}, engine.AppendMode{})
	if err == nil {
		err = res[0].Err
	}
	if err != nil {
		r.t.Fatal(err)
	}
	return res[0].Key
}

// overwrite writes over a stream item what change makes of it, as a replica's copy of its
// event would: a MODIFY record.
func (r *localRig) overwrite(stream string, k store.Key, change func(store.Item)) {
	r.t.Helper()
	table := r.naming.StreamTable(stream)
	it, err := r.b.Get(r.t.Context(), table, k, true)
	if err == nil {
		change(it)
		err = r.b.Put(r.t.Context(), table, it, store.Cond{})
	}
	if err != nil {
		r.t.Fatal(err)
	}
}

// remove deletes a stream item: a REMOVE record.
func (r *localRig) remove(stream string, k store.Key) {
	r.t.Helper()
	_, err := r.db.DeleteItem(r.t.Context(), &dynamodb.DeleteItemInput{
		TableName: aws.String(r.naming.StreamTable(stream)),
		Key: map[string]types.AttributeValue{
			catalog.AttrPK: &types.AttributeValueMemberS{Value: k.PK},
			catalog.AttrSK: &types.AttributeValueMemberS{Value: k.SK},
		},
	})
	if err != nil {
		r.t.Fatal(err)
	}
}

// row returns a row of a view version, by its String key values, as its values separated
// by spaces; "" when it doesn't exist.
func (r *localRig) row(version string, key ...string) string {
	r.t.Helper()
	ks := make([]expr.Value, len(key))
	for i, k := range key {
		ks[i] = expr.String(k)
	}
	row, ok, err := r.e.ReadRow(r.t.Context(), rules.cat, version, ks)
	if err != nil {
		r.t.Fatal(err)
	}
	if !ok {
		return ""
	}
	return rowText(row)
}

// checkRow fails the test unless the row of version at key is want.
func (r *localRig) checkRow(want, version string, key ...string) {
	r.t.Helper()
	if got := r.row(version, key...); got != want {
		r.t.Errorf("%s row %q is %q, want %q", version, key, got, want)
	}
}

// item reads the item of the row of version 1 of a view whose one key column is a String.
func (r *localRig) item(view, key string) store.Item {
	r.t.Helper()
	pk, err := keys.Key([]catalog.Type{{Kind: catalog.KindString}}, []expr.Value{expr.String(key)})
	if err != nil {
		r.t.Fatal(err)
	}
	it, err := r.b.Get(r.t.Context(), r.naming.ViewTable(view, 1), store.Key{PK: pk, SK: keys.NoSortKey}, true)
	if err != nil || it == nil {
		r.t.Fatalf("row %s of %s: %v, %v", key, view, it, err)
	}
	return it
}

// marks returns a row item's high-water marks: by unit key, the last sequence number applied.
func marks(t *testing.T, it store.Item) map[string]string {
	t.Helper()
	a, ok := it[catalog.AttrHighWaterMarks]
	h, err := keys.DecodeHWM(a, ok)
	if err != nil {
		t.Fatal(err)
	}
	return h.Units
}

func (r *localRig) unit(stream, unit string) meta.Unit {
	r.t.Helper()
	u, err := r.meta.Unit(r.t.Context(), stream, unit)
	if err != nil {
		r.t.Fatal(err)
	}
	return u
}

// reasons returns the reason of each failed invocation that the functions logged.
func (r *localRig) reasons() []string {
	r.t.Helper()
	var out []string
	for _, l := range r.logs.lines(r.t) {
		if l["msg"] == "batch failed" {
			out = append(out, fmt.Sprint(l["reason"]))
		}
	}
	return out
}

// metrics returns the EMF lines that the functions logged.
func (r *localRig) metrics() []map[string]any {
	r.t.Helper()
	return slices.DeleteFunc(r.logs.lines(r.t), func(l map[string]any) bool { return l["msg"] != "batch metrics" })
}

// logBuffer is the functions' log. Each line is one Write, from any goroutine.
type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	return jsonLines(t, l.b.String())
}

// seqOf returns a record's sequence number as delivered.
func seqOf(t *testing.T, rec json.RawMessage) string {
	t.Helper()
	var r struct {
		Change struct {
			SequenceNumber string `json:"SequenceNumber"`
		} `json:"dynamodb"`
	}
	if err := json.Unmarshal(rec, &r); err != nil || r.Change.SequenceNumber == "" {
		t.Fatalf("record %s has no sequence number: %v", rec, err)
	}
	return r.Change.SequenceNumber
}

// storedSeq returns a record's sequence number in its stored form, 40 digits.
func storedSeq(t *testing.T, rec json.RawMessage) string {
	t.Helper()
	s, err := keys.PadSeq(seqOf(t, rec))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// craft returns a copy of rec with change made to its JSON object and to the object of its
// dynamodb field. Numbers keep their text.
func craft(t *testing.T, rec json.RawMessage, change func(rec, dynamodb map[string]any)) json.RawMessage {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(rec))
	d.UseNumber()
	var m map[string]any
	if err := d.Decode(&m); err != nil {
		t.Fatal(err)
	}
	ddb, ok := m["dynamodb"].(map[string]any)
	if !ok {
		t.Fatalf("record %s has no dynamodb object", rec)
	}
	change(m, ddb)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// field returns a record's top-level field name.
func field(t *testing.T, rec json.RawMessage, name string) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec, &m); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprint(m[name])
}

// fakeFirehose answers PutRecordBatch in process as Firehose does, and keeps every record
// it stores.
type fakeFirehose struct {
	url string

	mu   sync.Mutex
	down bool // every call fails
	// script decides what becomes of each record; every record is accepted while it is nil.
	script   func(rec archived) outcome
	archived []archived
}

// outcome is what the fake does with one record.
type outcome int

const (
	accepted outcome = iota
	// rejected isn't stored, and gets an ErrorCode: the writer sends it again.
	rejected
	// duplicated is stored, but gets an ErrorCode: the record sent again is stored again.
	duplicated
)

// archived is an archive record as Firehose stores it. Numbers keep their text.
type archived map[string]any

type batchRecord struct {
	Data []byte `json:"Data"`
}

type batchResponse struct {
	RecordID  string `json:"RecordId,omitempty"`
	ErrorCode string `json:"ErrorCode,omitempty"`
}

type batchOutput struct {
	FailedPutCount   int             `json:"FailedPutCount"`
	RequestResponses []batchResponse `json:"RequestResponses"`
}

func newFakeFirehose(t *testing.T) *fakeFirehose {
	t.Helper()
	f := &fakeFirehose{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var in struct {
			Records []batchRecord `json:"Records"`
		}
		if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
			t.Errorf("PutRecordBatch request: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		out, ok := f.put(t, in.Records)
		if !ok {
			w.Header().Set("X-Amzn-Errortype", "ResourceNotFoundException")
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"__type":"ResourceNotFoundException","message":"Firehose stream not found"}`)
			return
		}
		if err := json.NewEncoder(w).Encode(out); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(srv.Close)
	f.url = srv.URL
	return f
}

// put answers one call, or reports false when the fake is down.
func (f *fakeFirehose) put(t *testing.T, recs []batchRecord) (batchOutput, bool) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return batchOutput{}, false
	}
	out := batchOutput{RequestResponses: make([]batchResponse, len(recs))}
	for i, r := range recs {
		d := json.NewDecoder(bytes.NewReader(r.Data))
		d.UseNumber()
		var rec archived
		if err := d.Decode(&rec); err != nil {
			t.Errorf("archive record %q: %v", r.Data, err)
		}
		o := accepted
		if f.script != nil {
			o = f.script(rec)
		}
		if o != rejected {
			f.archived = append(f.archived, rec)
		}
		if o == accepted {
			out.RequestResponses[i].RecordID = strconv.Itoa(len(f.archived))
			continue
		}
		out.FailedPutCount++
		out.RequestResponses[i].ErrorCode = "ServiceUnavailableException"
	}
	return out, true
}

func (f *fakeFirehose) setDown(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
}

func (f *fakeFirehose) setScript(script func(rec archived) outcome) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.script = script
}

// records returns the records stored so far, in the order they were.
func (f *fakeFirehose) records() []archived {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.archived)
}

// stored returns how many times each (_shard, _seq) was stored.
func (f *fakeFirehose) stored() map[[2]string]int {
	out := map[[2]string]int{}
	for _, rec := range f.records() {
		out[[2]string{fmt.Sprint(rec[archive.ColumnShard]), fmt.Sprint(rec[archive.ColumnSeq])}]++
	}
	return out
}

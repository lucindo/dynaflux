// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package runtime_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/esmlocal"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/runtime"
)

// clicksSQL is the test store. Both views count or sum, so they need the fenced handoff,
// and the rig sets them live.
const clicksSQL = `
CREATE STREAM clicks (user_id String, page String, ms UInt32)
PRIMARY KEY (user_id);

CREATE MATERIALIZED VIEW page_totals
ORDER BY (page)
AS SELECT page, count() AS views
FROM clicks
GROUP BY page;

-- A click of 0 ms divides by zero, which opens the version's breaker.
CREATE MATERIALIZED VIEW page_rates
ORDER BY (page)
AS SELECT page, sum(intDiv(1000, ms)) AS rate
FROM clicks
GROUP BY page;
`

const (
	region      = "us-east-1"
	shard       = "shardId-00000001761480000000-0123abcd"
	streamTable = "app-stream-clicks"
	streamARN   = "arn:aws:dynamodb:" + region + ":123456789012:table/" + streamTable + "/stream/2026-10-01T00:00:00.000"
	totalsTable = "app-view-page_totals-v1"
	finalReply  = `{"state":{},"batchItemFailures":[]}`
)

// compile returns the test store's catalog, archived in us-east-1 when archived is set.
func compile(t *testing.T, archived bool) *engine.Catalog {
	t.Helper()
	cfg := "store: app\nregions: [" + region + "]\narchive: {enabled: " + strconv.FormatBool(archived) + "}\n"
	cat, err := engine.Compile(catalogtest.MarshalFS(t, fstest.MapFS{
		"dynaflux.yaml":      {Data: []byte(cfg)},
		"dynaflux/store.sql": {Data: []byte(clicksSQL)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

// rig is a projector function of us-east-1 over an in-memory store, whose events are
// appended in us-west-2.
type rig struct {
	cat    *engine.Catalog
	mem    *mem.Mem
	e      *engine.Engine
	b      *backend
	sender *sender
	logs   *bytes.Buffer
	invoke func(context.Context, []byte) ([]byte, error)
}

// newRig returns a rig with every view version live.
func newRig(t *testing.T, cat *engine.Catalog, archiveSchema string) *rig {
	t.Helper()
	r := newPendingRig(t, cat, archiveSchema)
	if err := meta.New(r.mem, catalog.Naming{Store: "app"}.MetaTable()).SetLive(t.Context(), cat.Catalog()); err != nil {
		t.Fatal(err)
	}
	return r
}

// newPendingRig returns a rig with every view version pending its backfill.
func newPendingRig(t *testing.T, cat *engine.Catalog, archiveSchema string) *rig {
	t.Helper()
	m := mem.New()
	r := &rig{cat: cat, mem: m, e: engine.New(m, engine.Options{Region: "us-west-2"}), b: &backend{Backend: m}, sender: &sender{},
		logs: &bytes.Buffer{}}
	r.invoke = runtime.NewProjector(cat, runtime.Env{Region: region, ArchiveSchema: archiveSchema}, r.b, r.sender, r.logs)
	return r
}

// click appends a click and returns its stored item.
func (r *rig) click(t *testing.T, user, page string, ms uint64) store.Item {
	t.Helper()
	res, err := r.e.Append(t.Context(), r.cat, "clicks", [][]expr.Value{{expr.String(user), expr.String(page), expr.Uint(ms), expr.Null()}},
		engine.AppendMode{})
	if err == nil {
		err = res[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	it, err := r.mem.Get(t.Context(), streamTable, res[0].Key, true)
	if err != nil {
		t.Fatal(err)
	}
	return it
}

// call invokes the function with a deadline 30 s away, as Lambda does, and returns the
// reply.
func (r *rig) call(t *testing.T, event []byte) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	reply, err := r.invoke(lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{AwsRequestID: "req-1"}), event)
	return string(reply), err
}

// lines returns the log lines, each decoded.
func (r *rig) lines(t *testing.T) []map[string]any {
	t.Helper()
	return jsonLines(t, r.logs.String())
}

// failure returns the reason of the one log line, which must be a failed batch's.
func (r *rig) failure(t *testing.T) string {
	t.Helper()
	lines := r.lines(t)
	if len(lines) != 1 || lines[0]["level"] != "ERROR" || lines[0]["msg"] != "batch failed" {
		t.Fatalf("log lines %v, want one ERROR line of a failed batch", lines)
	}
	reason, _ := lines[0]["reason"].(string)
	return reason
}

// row returns a page's row of a version as text, "" when it doesn't exist.
func (r *rig) row(t *testing.T, version, page string) string {
	t.Helper()
	row, ok, err := r.e.ReadRow(t.Context(), r.cat, version, []expr.Value{expr.String(page)})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return ""
	}
	return rowText(row)
}

// event renders an invocation's event from the stream table of clicks.
func event(recs ...json.RawMessage) []byte { return esmlocal.Event(shard, streamARN, recs...) }

// record renders a stream record as Lambda delivers it.
func record(op, seq string, newImage, oldImage store.Item) json.RawMessage {
	change := map[string]any{"ApproximateCreationDateTime": 946684800, "SequenceNumber": seq, "StreamViewType": "NEW_AND_OLD_IMAGES"}
	if newImage != nil {
		change["NewImage"] = attrs(newImage)
	}
	if oldImage != nil {
		change["OldImage"] = attrs(oldImage)
	}
	data, err := json.Marshal(map[string]any{"eventName": op, "eventSource": "aws:dynamodb", "awsRegion": region,
		"dynamodb": change, "eventSourceARN": streamARN})
	if err != nil {
		panic(err)
	}
	return data
}

func insert(seq string, it store.Item) json.RawMessage { return record("INSERT", seq, it, nil) }

func attrs(it store.Item) map[string]any {
	m := map[string]any{}
	for k, v := range it {
		m[k] = attrJSON(v)
	}
	return m
}

func attrJSON(v store.Value) map[string]any {
	switch v.Type {
	case store.S:
		return map[string]any{"S": v.Str}
	case store.N:
		return map[string]any{"N": v.Str}
	case store.B:
		return map[string]any{"B": base64.StdEncoding.EncodeToString(v.Bin)}
	case store.BOOL:
		return map[string]any{"BOOL": v.Bool}
	case store.L:
		l := make([]any, len(v.List))
		for i, e := range v.List {
			l[i] = attrJSON(e)
		}
		return map[string]any{"L": l}
	case store.M:
		return map[string]any{"M": attrs(v.Map)}
	}
	return map[string]any{"NULL": true}
}

// seq returns the n-th sequence number as delivered.
func seq(n int) string { return fmt.Sprintf("1%020d", n) }

// backend counts the calls that reach the store, and fails the Puts that fail returns an
// error for.
type backend struct {
	store.Backend

	calls atomic.Int64
	fail  func(table string, it store.Item) error
}

func (b *backend) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	b.calls.Add(1)
	return b.Backend.Get(ctx, table, k, consistent)
}

func (b *backend) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	b.calls.Add(1)
	return b.Backend.BatchGet(ctx, table, ks, consistent)
}

func (b *backend) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	b.calls.Add(1)
	return b.Backend.Query(ctx, table, q)
}

func (b *backend) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	b.calls.Add(1)
	if b.fail != nil {
		if err := b.fail(table, it); err != nil {
			return err
		}
	}
	return b.Backend.Put(ctx, table, it, c)
}

// sender is a Firehose that accepts every record, or fails every call with err.
type sender struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *sender) PutBatch(_ context.Context, _ string, recs [][]byte) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return make([]string, len(recs)), nil
}

func (s *sender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// The window's final invocation has no records, and must succeed without a store call:
// until it does, Lambda stops the shard.
func TestHandleFinalInvocation(t *testing.T) {
	t.Parallel()
	recorded, readErr := os.ReadFile("testdata/final_invocation.json")
	if readErr != nil {
		t.Fatal(readErr)
	}
	r := newRig(t, compile(t, false), "")
	for _, ev := range [][]byte{recorded, event(), []byte(`{"Records":[],"shardId":"","eventSourceARN":""}`)} {
		reply, err := r.call(t, ev)
		if reply != finalReply || err != nil {
			t.Errorf("reply %s, %v; want %s", reply, err, finalReply)
		}
	}
	if n := r.b.calls.Load(); n != 0 || r.logs.Len() != 0 {
		t.Errorf("%d store calls and logs %q, want none", n, r.logs)
	}
}

// refused checks that the event that event renders from a click fails the invocation with
// the error want and reason event, before any store call.
func refused(t *testing.T, event func(it store.Item) []byte, want string) {
	t.Helper()
	r := newRig(t, compile(t, false), "")
	reply, err := r.call(t, event(r.click(t, "u1", "/a", 100)))
	if reply != "" || err == nil || err.Error() != want {
		t.Errorf("reply %q, error %v; want an error %q", reply, err, want)
	}
	if reason := r.failure(t); reason != "event" {
		t.Errorf("reason %q, want event", reason)
	}
	if n := r.b.calls.Load(); n != 0 {
		t.Errorf("%d store calls, want none", n)
	}
}

// Records without shardId come from a mapping without a tumbling window, whose units the
// projector can't name (ADR 0003): the invocation fails, loudly.
func TestHandleMissingShard(t *testing.T) {
	t.Parallel()
	refused(t, func(it store.Item) []byte { return esmlocal.Event("", streamARN, insert(seq(1), it)) },
		"runtime: the event has records but no shardId: the event source mapping needs TumblingWindowInSeconds (ADR 0003)")
}

// The projector consumes the stream tables of its store and prefix, and nothing else.
func TestHandleUnknownTable(t *testing.T) {
	t.Parallel()
	for name, table := range map[string]string{"another table": "app-stream-other", "another prefix": "prod-" + streamTable} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			arn := strings.Replace(streamARN, streamTable, table, 1)
			refused(t, func(it store.Item) []byte { return esmlocal.Event(shard, arn, insert(seq(1), it)) },
				"runtime: "+arn+" isn't the stream of a stream table of store app")
		})
	}
}

func TestHandleUnreadableEvent(t *testing.T) {
	t.Parallel()
	t.Run("unknown eventName", func(t *testing.T) {
		t.Parallel()
		refused(t, func(it store.Item) []byte { return event(record("UPSERT", seq(1), it, nil)) },
			`runtime: record 100000000000000000001 has eventName "UPSERT"`)
	})
	// A stream of another view type delivers INSERTs without a new image, which every
	// version would fail to evaluate.
	t.Run("keys-only stream", func(t *testing.T) {
		t.Parallel()
		refused(t, func(it store.Item) []byte {
			rec, err := json.Marshal(map[string]any{"eventName": "INSERT", "eventSourceARN": streamARN, "dynamodb": map[string]any{
				"SequenceNumber": seq(1), "StreamViewType": "KEYS_ONLY",
				"Keys": attrs(store.Item{catalog.AttrPK: it[catalog.AttrPK], catalog.AttrSK: it[catalog.AttrSK]}),
			}})
			if err != nil {
				t.Fatal(err)
			}
			return event(rec)
		}, `runtime: record 100000000000000000001 has StreamViewType "KEYS_ONLY": the stream table's stream needs NEW_AND_OLD_IMAGES`)
	})
	t.Run("not JSON", func(t *testing.T) {
		t.Parallel()
		refused(t, func(store.Item) []byte { return []byte(`{"Records":`) }, "runtime: decode the event: unexpected end of JSON input")
	})
}

// A projector whose DYNAFLUX_ARCHIVE_SCHEMA doesn't match its catalog fails every batch with
// records before any call (ADR 0008); final invocations still succeed.
func TestHandleArchiveGuard(t *testing.T) {
	t.Parallel()
	archived := compile(t, true)
	fingerprint := archive.SchemaFor(archived.Catalog(), region).Fingerprint()
	tests := []struct {
		name string
		cat  *engine.Catalog
		env  string
	}{
		{"mismatch", archived, "sha256:0000"},
		{"missing", archived, ""},
		{"unexpected", compile(t, false), fingerprint},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := newRig(t, tt.cat, tt.env)
			if reply, err := r.call(t, event()); reply != finalReply || err != nil {
				t.Errorf("final invocation: reply %s, %v; want %s", reply, err, finalReply)
			}
			if reply, err := r.call(t, event(insert(seq(1), r.click(t, "u1", "/a", 100)))); reply != "" || err == nil {
				t.Errorf("reply %q, error %v; want an error", reply, err)
			}
			if reason := r.failure(t); reason != "archive_schema" {
				t.Errorf("reason %q, want archive_schema", reason)
			}
			if n, sent := r.b.calls.Load(), r.sender.count(); n != 0 || sent != 0 {
				t.Errorf("%d store calls and %d Firehose calls, want none", n, sent)
			}
		})
	}
}

// With the fingerprint its catalog gives, the projector archives the batch and applies it.
func TestHandleArchives(t *testing.T) {
	t.Parallel()
	cat := compile(t, true)
	r := newRig(t, cat, archive.SchemaFor(cat.Catalog(), region).Fingerprint())
	reply, err := r.call(t, event(insert(seq(1), r.click(t, "u1", "/a", 100)), insert(seq(2), r.click(t, "u2", "/a", 50))))
	if reply != finalReply || err != nil {
		t.Fatalf("reply %s, %v; want %s", reply, err, finalReply)
	}
	if got, sent := r.row(t, "page_totals@v1", "/a"), r.sender.count(); got != "/a 2" || sent != 1 {
		t.Errorf("page_totals /a is %q after %d Firehose calls, want /a 2 after 1", got, sent)
	}
}

// TTL deletes reach the projector as REMOVE records: a batch of them succeeds without a
// store call.
func TestHandleRemoveOnly(t *testing.T) {
	t.Parallel()
	r := newRig(t, compile(t, false), "")
	it := r.click(t, "u1", "/a", 100)
	ttl, err := json.Marshal(map[string]any{
		"eventName": "REMOVE", "eventSource": "aws:dynamodb", "awsRegion": region, "eventSourceARN": streamARN,
		"userIdentity": map[string]string{"type": "Service", "principalId": "dynamodb.amazonaws.com"},
		"dynamodb":     map[string]any{"SequenceNumber": seq(2), "OldImage": attrs(it), "StreamViewType": "NEW_AND_OLD_IMAGES"},
	})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := r.call(t, event(record("REMOVE", seq(1), nil, it), ttl))
	if reply != finalReply || err != nil {
		t.Errorf("reply %s, %v; want %s", reply, err, finalReply)
	}
	if n := r.b.calls.Load(); n != 0 || r.logs.Len() != 0 {
		t.Errorf("%d store calls and logs %q, want none", n, r.logs)
	}
}

// A row that fails is reported as the lowest sequence number to deliver again, as
// delivered, with a WARN line; the other rows are written.
func TestHandlePartialFailure(t *testing.T) {
	t.Parallel()
	r := newRig(t, compile(t, false), "")
	r.b.fail = func(table string, it store.Item) error {
		if table == totalsTable && it[catalog.AttrPK].Str == "/b" {
			return errors.New("throttled")
		}
		return nil
	}
	long := "9999999999999999999999999999999999999999"
	reply, err := r.call(t, event(insert(seq(1), r.click(t, "u1", "/a", 100)), insert(long, r.click(t, "u2", "/b", 100))))
	if want := `{"state":{},"batchItemFailures":[{"itemIdentifier":"` + long + `"}]}`; reply != want || err != nil {
		t.Fatalf("reply %s, %v; want %s", reply, err, want)
	}
	if got := r.row(t, "page_totals@v1", "/a"); got != "/a 1" {
		t.Errorf("page_totals /a is %q, want /a 1", got)
	}
	lines := r.lines(t)
	if len(lines) != 1 || lines[0]["level"] != "WARN" || lines[0]["failedSeq"] != long ||
		lines[0]["error"] != `projector: write view page_totals@v1 row "/b"/"!": throttled` ||
		lines[0]["requestId"] != "req-1" || lines[0]["stream"] != "clicks" || lines[0]["unit"] != shard {
		t.Errorf("log lines %v, want one WARN line with the failed sequence number", lines)
	}
}

// A failed batch logs why, so an operator can filter on reason.
func TestHandleFailureReasons(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		rig     func(t *testing.T) *rig
		timeout time.Duration
		reason  string
	}{
		{"request DynamoDB rejects", func(t *testing.T) *rig {
			t.Helper()
			r := newRig(t, compile(t, false), "")
			r.b.fail = func(table string, _ store.Item) error {
				if table == totalsTable {
					return fmt.Errorf("no such table: %w", store.ErrInvalidRequest)
				}
				return nil
			}
			return r
		}, time.Minute, "invalid_request"},
		{"Firehose fails", func(t *testing.T) *rig {
			t.Helper()
			cat := compile(t, true)
			r := newRig(t, cat, archive.SchemaFor(cat.Catalog(), region).Fingerprint())
			r.sender.err = errors.New("firehose down")
			return r
		}, time.Minute, "archive"},
		{"longer than the prune window allows", func(t *testing.T) *rig {
			t.Helper()
			return newRig(t, compile(t, false), "")
		}, 901 * time.Second, "lifetime"},
		{"a fence for a version the batch didn't apply", func(t *testing.T) *rig {
			t.Helper()
			r := newPendingRig(t, compile(t, false), "")
			m := meta.New(r.mem, catalog.Naming{Store: "app"}.MetaTable())
			u, err := m.Unit(t.Context(), "clicks", shard)
			if err == nil {
				_, err = m.EnsureFence(t.Context(), u, "page_totals@v1")
			}
			if err != nil {
				t.Fatal(err)
			}
			return r
		}, time.Minute, "fenced"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := tt.rig(t)
			ctx, cancel := context.WithTimeout(t.Context(), tt.timeout)
			defer cancel()
			if _, err := r.invoke(ctx, event(insert(seq(1), r.click(t, "u1", "/a", 100)))); err == nil {
				t.Fatal("no error")
			}
			if reason := r.failure(t); reason != tt.reason {
				t.Errorf("reason %q, want %s", reason, tt.reason)
			}
		})
	}
}

// One EMF line per batch that counted something, at the level of its most severe count, with
// a timestamp Lambda's log-level filter reads; none for a batch that counted nothing.
func TestHandleMetrics(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, compile(t, false), "")
		r.b.fail = func(table string, it store.Item) error {
			if table == totalsTable && it[catalog.AttrPK].Str == "/big" {
				return fmt.Errorf("item is 400,001 bytes: %w", store.ErrTooLarge)
			}
			return nil
		}
		if reply, err := r.call(t, event(insert(seq(1), r.click(t, "u1", "/a", 100)))); reply != finalReply || err != nil {
			t.Fatalf("reply %s, %v; want %s", reply, err, finalReply)
		}
		if r.logs.Len() != 0 {
			t.Fatalf("a batch that counted nothing logged %q", r.logs)
		}
		old := r.click(t, "u2", "/m", 100)
		changed := with(old, "ms", store.Value{Type: store.N, Str: "200"})
		_, err := r.call(t, event(
			insert(seq(2), r.click(t, "u3", "/zero", 0)),
			insert(seq(3), r.click(t, "u4", "/big", 100)),
			record("MODIFY", seq(4), changed, old),
		))
		if err != nil {
			t.Fatal(err)
		}
		const want = `{"timestamp":"2000-01-01T00:00:00Z","level":"ERROR","msg":"batch metrics","BreakerOpened":1,` +
			`"RowTooLarge":1,"DivergentDuplicates":1,"Store":"app","_aws":{"Timestamp":946684800000,"CloudWatchMetrics":` +
			`[{"Namespace":"Dynaflux","Dimensions":[["Store"]],"Metrics":[{"Name":"BreakerOpened","Unit":"Count"},` +
			`{"Name":"RowTooLarge","Unit":"Count"},{"Name":"DivergentDuplicates","Unit":"Count"}]}]},"requestId":"req-1",` +
			`"stream":"clicks","unit":"` + shard + `","broken":["page_rates@v1"]}`
		if got, _, _ := strings.Cut(r.logs.String(), "\n"); got != want {
			t.Errorf("EMF line:\n%s\nwant:\n%s", got, want)
		}
	})
}

// A batch of divergent duplicates alone logs its metric at WARN.
func TestHandleMetricsWarn(t *testing.T) {
	t.Parallel()
	r := newRig(t, compile(t, false), "")
	old := r.click(t, "u1", "/a", 100)
	if _, err := r.call(t, event(record("MODIFY", seq(1), with(old, "ms", store.Value{Type: store.N, Str: "7"}), old))); err != nil {
		t.Fatal(err)
	}
	lines := r.lines(t)
	if len(lines) != 1 || lines[0]["level"] != "WARN" || lines[0]["DivergentDuplicates"] != 1.0 || lines[0]["BreakerOpened"] != nil {
		t.Errorf("log lines %v, want one WARN line with DivergentDuplicates 1", lines)
	}
}

// A breaker opened in a batch that then fails is counted by that batch: its retry finds the
// version already broken.
func TestHandleMetricsOfFailedBatch(t *testing.T) {
	t.Parallel()
	r := newRig(t, compile(t, false), "")
	r.b.fail = func(table string, _ store.Item) error {
		if table == totalsTable {
			return fmt.Errorf("no such table: %w", store.ErrInvalidRequest)
		}
		return nil
	}
	if _, err := r.call(t, event(insert(seq(1), r.click(t, "u1", "/zero", 0)))); !errors.Is(err, store.ErrInvalidRequest) {
		t.Fatalf("err = %v, want ErrInvalidRequest", err)
	}
	lines := r.lines(t)
	if len(lines) != 2 || lines[0]["BreakerOpened"] != 1.0 || lines[1]["reason"] != "invalid_request" {
		t.Errorf("log lines %v, want the metrics, then the failure", lines)
	}
}

// with returns a copy of it with k set to v.
func with(it store.Item, k string, v store.Value) store.Item {
	out := maps.Clone(it)
	out[k] = v
	return out
}

// slow delays every call, and notes when each starts and how many are in flight.
type slow struct {
	store.Backend

	delay    time.Duration
	mu       sync.Mutex
	starts   []time.Time
	inFlight int
}

func (s *slow) wait(ctx context.Context) func() {
	s.mu.Lock()
	s.starts = append(s.starts, time.Now())
	s.inFlight++
	s.mu.Unlock()
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
	}
	return func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}
}

func (s *slow) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	defer s.wait(ctx)()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.Backend.Get(ctx, table, k, consistent)
}

func (s *slow) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	defer s.wait(ctx)()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.Backend.BatchGet(ctx, table, ks, consistent)
}

func (s *slow) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	return func(yield func(store.Item, error) bool) {
		done := s.wait(ctx)
		if err := ctx.Err(); err != nil {
			done()
			yield(nil, err)
			return
		}
		done()
		for it, err := range s.Backend.Query(ctx, table, q) {
			if !yield(it, err) {
				return
			}
		}
	}
}

func (s *slow) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	defer s.wait(ctx)()
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Backend.Put(ctx, table, it, c)
}

// S9 requirement 1 at the adapter: with Lambda's 5 s deadline, nothing is sent after the
// cutoff half a second before it, and nothing is in flight when Invoke returns. The rows
// that didn't finish are delivered again.
func TestHandleNoCallAfterCutoff(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, compile(t, false), "")
		b := &slow{Backend: r.mem, delay: 400 * time.Millisecond}
		invoke := runtime.NewProjector(r.cat, runtime.Env{Region: region}, b, &sender{}, &bytes.Buffer{})
		var recs []json.RawMessage
		for i := range 64 {
			recs = append(recs, insert(seq(i+1), r.click(t, "u", fmt.Sprintf("/%d", i), 100)))
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		deadline, _ := ctx.Deadline()
		cutoff := deadline.Add(-500 * time.Millisecond)
		reply, err := invoke(ctx, event(recs...))
		returned := time.Now()
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.inFlight != 0 {
			t.Errorf("%d calls in flight when Invoke returned", b.inFlight)
		}
		if late := slices.IndexFunc(b.starts, func(at time.Time) bool { return at.After(cutoff) }); late >= 0 {
			t.Errorf("call %d of %d started %v after the cutoff", late, len(b.starts), b.starts[late].Sub(cutoff))
		}
		if !returned.Before(deadline) {
			t.Errorf("Invoke returned %v after the deadline", returned.Sub(deadline))
		}
		var resp struct {
			Failures []struct {
				ID string `json:"itemIdentifier"`
			} `json:"batchItemFailures"`
		}
		if err != nil || json.Unmarshal(reply, &resp) != nil || len(resp.Failures) != 1 {
			t.Errorf("reply %s, %v; want a partial failure", reply, err)
		}
	})
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package fluxruntime_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	fh "github.com/aws/aws-sdk-go-v2/service/firehose"

	"github.com/lucindo/dynaflux/fluxruntime"
	"github.com/lucindo/dynaflux/internal/archive/firehose"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/esmlocal"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// These tests check SYNC appends and waits against the projector that Run serves, on
// DynamoDB Local (ADR 0014, ADR 0015).

// syncStore is the harness's store of SYNC views: quota_usage at versions 1 and 2, and
// endpoint_calls, whose uniqExact puts markers in the append's transaction, are SYNC;
// account_daily is ASYNC.
const (
	syncStore = "../internal/simulation/testdata/sync"
	apiCalls  = "api_calls"
	// otherRegion is where the second engine appends.
	otherRegion = "us-west-2"
)

var loadSyncSuite = sync.OnceValues(func() (*suite, error) { return loadSuite(syncStore) }) //nolint:gochecknoglobals // compiled once

// syncRig runs the projector of emulatortest.Region over DynamoDB Local for the SYNC store.
// Two engines append to one set of tables: a in the projector's region with the store's
// catalog, b in otherRegion with every view ASYNC. Both write to a's view tables, so only
// a's transactions may apply views; b's events reach the projector as another region's do.
type syncRig struct {
	t       *testing.T
	s       *suite
	a, b    *engine.Engine
	bCat    *engine.Catalog
	backend store.Backend
	naming  catalog.Naming
	invoke  esmlocal.Function
	reader  *esmlocal.Reader
}

func newSyncRig(t *testing.T) *syncRig {
	t.Helper()
	s, err := loadSyncSuite()
	if err != nil {
		t.Fatal(err)
	}
	cfg := emulatortest.Config(t, nil)
	db := dynamodb.NewFromConfig(cfg)
	r := &syncRig{t: t, s: s, backend: dynamo.New(db), naming: emulatortest.Tables(t, cfg, s.manifest), bCat: asyncCatalog(t, s.cat)}
	if err = meta.New(r.backend, r.naming.MetaTable()).SetLive(t.Context(), s.cat.Catalog()); err != nil {
		t.Fatal(err)
	}
	r.a = engine.New(r.backend, engine.Options{Region: emulatortest.Region, Prefix: r.naming.Prefix})
	r.b = engine.New(r.backend, engine.Options{Region: otherRegion, Prefix: r.naming.Prefix})
	fake := newFakeFirehose(t)
	sender := firehose.SDK{Client: fh.NewFromConfig(cfg, func(o *fh.Options) { o.BaseEndpoint = &fake.url })}
	env := fluxruntime.Env{Region: emulatortest.Region, Prefix: r.naming.Prefix, ArchiveSchema: s.fingerprint}
	r.invoke = fluxruntime.NewProjector(s.cat, env, r.backend, sender, &logBuffer{})
	out, err := db.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: new(r.naming.StreamTable(apiCalls))})
	if err != nil {
		t.Fatal(err)
	}
	r.reader = esmlocal.NewReader(dynamodbstreams.NewFromConfig(cfg), aws.ToString(out.Table.LatestStreamArn))
	return r
}

// asyncCatalog returns cat with every view version ASYNC.
func asyncCatalog(t *testing.T, cat *engine.Catalog) *engine.Catalog {
	t.Helper()
	data, err := catalog.Marshal(cat.Catalog())
	if err != nil {
		t.Fatal(err)
	}
	c, err := engine.Compile(catalogtest.Async(t, data))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// apiCall is an event of api_calls.
type apiCall struct {
	account, endpoint, request string
	units                      uint64
}

// appendCalls appends calls, each with event_time at, and fails the test unless the call
// returns its results.
func appendCalls(t *testing.T, e *engine.Engine, cat *engine.Catalog, m engine.AppendMode, at time.Time, calls ...apiCall) []engine.Appended {
	t.Helper()
	evs := make([][]expr.Value, len(calls))
	for i, c := range calls {
		evs[i] = []expr.Value{expr.String(c.account), expr.String(c.endpoint), expr.String(c.request), expr.Uint(c.units), expr.Micros(at.UnixMicro())}
	}
	res, err := e.Append(t.Context(), cat, apiCalls, evs, m)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// stored appends calls and fails the test unless each was stored.
func stored(t *testing.T, e *engine.Engine, cat *engine.Catalog, m engine.AppendMode, at time.Time, calls ...apiCall) []engine.Appended {
	t.Helper()
	res := appendCalls(t, e, cat, m, at, calls...)
	for i, r := range res {
		if r.Err != nil {
			t.Fatalf("append %+v: %v", calls[i], r.Err)
		}
	}
	return res
}

// drain applies every record written so far through the projector, and fails the test
// unless each invocation succeeded the first time.
func (r *syncRig) drain() {
	r.t.Helper()
	var failed error
	strict := func(ctx context.Context, event []byte) ([]byte, error) {
		out, err := r.invoke(ctx, event)
		switch {
		case failed != nil:
		case err != nil:
			failed = err
		case string(out) != success:
			failed = fmt.Errorf("reply %s, want %s", out, success)
		}
		return out, err
	}
	err := esmlocal.Drain(r.t.Context(), r.reader, functionTimeout, strict)
	if err = errors.Join(err, failed); err != nil {
		r.t.Fatal(err)
	}
}

// views returns, by view version, the rows of the accounts and endpoints these tests
// append: rowText of the key values, then the aggregates in catalog order.
func (r *syncRig) views() map[string][]string {
	r.t.Helper()
	ctx, e, cat := r.t.Context(), r.a, r.s.cat
	out := map[string][]string{}
	add := func(version string, row []expr.Value, ok bool, err error) {
		if err != nil {
			r.t.Fatal(err)
		}
		if ok {
			out[version] = append(out[version], rowText(row))
		}
	}
	for _, account := range []string{"acct-a", "acct-b", "acct-c"} {
		key := []expr.Value{expr.String(account)}
		for _, version := range []string{"quota_usage@v1", "quota_usage@v2"} {
			row, ok, err := e.ReadRow(ctx, cat, version, key)
			add(version, row, ok, err)
		}
		for row, err := range e.ReadPartition(ctx, cat, "account_daily@v1", key) {
			add("account_daily@v1", row, true, err)
		}
	}
	for _, endpoint := range []string{"/export", "/search"} {
		row, ok, err := e.ReadRow(ctx, cat, "endpoint_calls@v1", []expr.Value{expr.String(endpoint)})
		add("endpoint_calls@v1", row, ok, err)
	}
	return out
}

// A's appends apply the SYNC versions in their transactions and name them in _sync, so A's
// projector skips them there and applies only account_daily; B's events, appended without
// SYNC, it applies to every version. The views end with each event applied once.
func TestSyncAppendsSkippedInTheirRegion(t *testing.T) {
	t.Parallel()
	r := newSyncRig(t)
	at := time.Now().UTC()
	a := stored(t, r.a, r.s.cat, engine.AppendMode{}, at,
		apiCall{"acct-a", "/search", "req-1", 5}, apiCall{"acct-b", "/search", "req-2", 2}, apiCall{"acct-a", "/export", "req-3", 7})
	if dup := appendCalls(t, r.a, r.s.cat, engine.AppendMode{}, at, apiCall{"acct-a", "/search", "req-1", 5}); !errors.Is(dup[0].Err, engine.ErrDuplicate) {
		t.Errorf("append of req-1 again: %v, want ErrDuplicate", dup[0].Err)
	}
	// acct-a on /search again: its uniqExact marker, which A's transaction wrote, holds.
	b := stored(t, r.b, r.bCat, engine.AppendMode{}, at, apiCall{"acct-a", "/search", "req-4", 3}, apiCall{"acct-c", "/export", "req-5", 4})

	// A's event names the versions its transaction applied, sorted, heavy_calls too, whose
	// WHERE drops it; B's names none.
	syncList := store.Value{Type: store.L, List: []store.Value{
		{Type: store.S, Str: "endpoint_calls@v1"}, {Type: store.S, Str: "heavy_calls@v1"},
		{Type: store.S, Str: "quota_usage@v1"}, {Type: store.S, Str: "quota_usage@v2"},
	}}
	if got := r.event(a[0].Key)[catalog.AttrSync]; !reflect.DeepEqual(got, syncList) {
		t.Errorf("A's event has _sync %+v, want %+v", got, syncList)
	}
	if got, ok := r.event(b[0].Key)[catalog.AttrSync]; ok {
		t.Errorf("B's event has _sync %+v, want none", got)
	}
	// No projector has run: the SYNC versions hold A's events, account_daily nothing.
	before := map[string][]string{
		"quota_usage@v1":    {"acct-a 2 12", "acct-b 1 2"},
		"quota_usage@v2":    {"acct-a 2 342 12", "acct-b 1 500 2"},
		"endpoint_calls@v1": {"/export 1 1", "/search 2 2"},
	}
	if got := r.views(); !reflect.DeepEqual(got, before) {
		t.Errorf("views before the projector ran\n%v\nwant\n%v", got, before)
	}
	r.drain()
	// Each of the five events once.
	day := at.Truncate(24 * time.Hour).Format(time.RFC3339Nano)
	after := map[string][]string{
		"quota_usage@v1":    {"acct-a 3 15", "acct-b 1 2", "acct-c 1 4"},
		"quota_usage@v2":    {"acct-a 3 675 15", "acct-b 1 500 2", "acct-c 1 250 4"},
		"account_daily@v1":  {"acct-a " + day + " 3 15", "acct-b " + day + " 1 2", "acct-c " + day + " 1 4"},
		"endpoint_calls@v1": {"/export 2 2", "/search 2 3"},
	}
	if got := r.views(); !reflect.DeepEqual(got, after) {
		t.Errorf("views after the projector ran\n%v\nwant\n%v", got, after)
	}
}

// event reads the stored event of api_calls at k.
func (r *syncRig) event(k store.Key) store.Item {
	r.t.Helper()
	it, err := r.backend.Get(r.t.Context(), r.naming.StreamTable(apiCalls), k, true)
	if err != nil || it == nil {
		r.t.Fatalf("event %v: %v, %v", k, it, err)
	}
	return it
}

// A query that waits for a Waitable append returns once the projector of the append's
// region has written the event's receipt, and its rows hold the event, which only the
// projector applies to the ASYNC account_daily.
func TestWaitForReceiptOfTheProjector(t *testing.T) {
	t.Parallel()
	r := newSyncRig(t)
	at := time.Now().UTC()
	res := stored(t, r.a, r.s.cat, engine.AppendMode{Receipt: true}, at, apiCall{"acct-a", "/search", "req-1", 4})
	ev := res[0].Stored
	tok := r.a.Token(apiCalls, res)
	if err := r.a.Wait(t.Context(), r.s.cat, tok, 100*time.Millisecond); !errors.Is(err, engine.ErrWaitTimeout) {
		t.Fatalf("Wait before the projector ran: %v, want ErrWaitTimeout", err)
	}
	type result struct {
		rows [][]expr.Value
		err  error
	}
	done := make(chan result, 1)
	go func() {
		accounts := []expr.Value{expr.Array([]expr.Value{expr.String("acct-a")})}
		rows, err := r.a.Query(t.Context(), r.s.cat, "AccountDaily", accounts,
			engine.QueryOptions{Wait: []engine.Wait{{Token: tok, Timeout: functionTimeout}}})
		done <- result{rows, err}
	}()
	written := time.Now()
	r.drain()
	got := <-done
	day := at.Truncate(24 * time.Hour).Format(time.RFC3339Nano)
	if want := "acct-a " + day + " 1 4"; got.err != nil || len(got.rows) != 1 || rowText(got.rows[0]) != want {
		t.Errorf("AccountDaily after the wait = %v, %v; want one row %q", got.rows, got.err, want)
	}

	// encoding.md §8.6: the receipt's key names the stored record, and it expires 25 hours
	// after the projector wrote it.
	_, id, err := keys.ParseStreamSK(ev[catalog.AttrSK].Str)
	if err != nil {
		t.Fatal(err)
	}
	key := store.Key{PK: "rcpt#api_calls#" + id + "#" + ev[catalog.AttrIngestTime].Str, SK: "!"}
	rcpt, err := r.backend.Get(t.Context(), r.naming.MetaTable(), key, true)
	if err != nil {
		t.Fatal(err)
	}
	expires, err := strconv.ParseInt(rcpt[catalog.AttrTTL].Str, 10, 64)
	lo, hi := written.Add(25*time.Hour).Unix(), time.Now().Add(25*time.Hour).Unix()+1
	if err != nil || expires < lo || expires > hi {
		t.Errorf("receipt %q has _ttl %v, want from %d to %d", key.PK, rcpt[catalog.AttrTTL], lo, hi)
	}
}

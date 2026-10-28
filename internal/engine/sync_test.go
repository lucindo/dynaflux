// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// quota is the store of the SYNC tests: a quota per account and a set of accounts per
// endpoint, both SYNC, the second failing on calls of no units, and an ASYNC daily count.
const quota = `
CREATE STREAM api_calls (
    account_id String,
    endpoint   String,
    request_id String,
    units      UInt32,
    note       String DEFAULT ''
)
PRIMARY KEY (account_id)
DEDUP BY (request_id);

CREATE MATERIALIZED VIEW quota_usage
MODE SYNC
ORDER BY (account_id)
AS SELECT account_id, count() AS calls, sum(units) AS used
FROM api_calls
GROUP BY account_id;

CREATE MATERIALIZED VIEW endpoint_accounts
MODE SYNC
ORDER BY (endpoint)
AS SELECT endpoint, uniqExact(account_id) AS accounts, sum(intDiv(100, units)) AS shares
FROM api_calls
GROUP BY endpoint;

CREATE MATERIALIZED VIEW account_daily
ORDER BY (account_id, day)
PRIMARY KEY (account_id)
AS SELECT account_id, toStartOfDay(event_time) AS day, count() AS calls
FROM api_calls
GROUP BY account_id, day;

-- name: Used :one
SELECT calls, used FROM quota_usage WHERE account_id = {account:String};

-- name: Accounts :one
SELECT accounts FROM endpoint_accounts WHERE endpoint = {endpoint:String};

-- name: Daily :many
SELECT day, calls FROM account_daily WHERE account_id = {account:String};
`

const (
	callsTable  = "app-stream-api_calls"
	quotaTable  = "app-view-quota_usage-v1"
	quotaUsage  = "quota_usage@v1"
	endpointAcc = "endpoint_accounts@v1"
)

// outcome is how a scripted transaction ends.
type outcome uint8

const (
	applied         outcome = iota
	conflicted              // nothing is written: store.ErrConflict
	throttledBefore         // nothing is written: store.ErrThrottled
	throttledAfter          // written, then store.ErrThrottled, as after a lost response
	resentAfter             // written, then every condition fails, as on a resend without its token
)

// scripted is a store that logs the engine's requests and ends its transactions as
// outcomes says, in turn; after them, transactions apply. before, when set, runs once
// before the next transaction.
type scripted struct {
	store.Backend

	mu       sync.Mutex
	log      []string
	outcomes []outcome
	sent     []time.Time
	before   func()
	failMeta bool // fail reads of the meta table
	// throttledReads is how many more reads of quota_usage's table fail with
	// store.ErrThrottled.
	throttledReads int
}

func (s *scripted) notef(format string, args ...any) {
	s.mu.Lock()
	s.log = append(s.log, fmt.Sprintf(format, args...))
	s.mu.Unlock()
}

func (s *scripted) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	s.notef("get %s consistent=%v", table, consistent)
	return s.Backend.Get(ctx, table, k, consistent)
}

func (s *scripted) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	s.notef("batchget %s consistent=%v", table, consistent)
	s.mu.Lock()
	throttled := table == quotaTable && s.throttledReads > 0
	if throttled {
		s.throttledReads--
	}
	s.mu.Unlock()
	if throttled {
		return nil, store.ErrThrottled
	}
	return s.Backend.BatchGet(ctx, table, ks, consistent)
}

func (s *scripted) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	s.notef("query %s consistent=%v", table, q.Consistent)
	if s.failMeta && table == "app-meta" {
		return func(yield func(store.Item, error) bool) { yield(nil, errors.New("meta is down")) }
	}
	return s.Backend.Query(ctx, table, q)
}

func (s *scripted) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	s.notef("put %s", table)
	return s.Backend.Put(ctx, table, it, c)
}

func (s *scripted) Transact(ctx context.Context, puts []store.TxPut) error {
	s.mu.Lock()
	tables := make([]string, len(puts))
	for i, p := range puts {
		tables[i] = p.Table
	}
	s.log = append(s.log, "transact "+strings.Join(tables, " "))
	s.sent = append(s.sent, time.Now())
	o := applied
	if len(s.outcomes) > 0 {
		o, s.outcomes = s.outcomes[0], s.outcomes[1:]
	}
	before := s.before
	s.before = nil
	s.mu.Unlock()
	if before != nil {
		before()
	}
	switch o {
	case conflicted:
		return store.ErrConflict
	case throttledBefore:
		return store.ErrThrottled
	case applied, throttledAfter, resentAfter:
	}
	if err := s.Backend.Transact(ctx, puts); err != nil {
		return err
	}
	switch o {
	case throttledAfter:
		return store.ErrThrottled
	case resentAfter:
		tx := &store.TxError{Conds: make([]*store.ConditionError, len(puts))}
		for i, p := range puts {
			cur, err := s.Backend.Get(ctx, p.Table, store.Key{PK: p.Item[catalog.AttrPK].Str, SK: p.Item[catalog.AttrSK].Str}, true)
			if err != nil {
				return err
			}
			tx.Conds[i] = &store.ConditionError{Current: cur}
		}
		return tx
	case applied, conflicted, throttledBefore:
	}
	return nil
}

// requests returns the log since the last call, and starts a new one.
func (s *scripted) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.log
	s.log = nil
	return out
}

type syncFixture struct {
	cat  *engine.Catalog
	mem  *mem.Mem
	b    *scripted
	e    *engine.Engine
	meta *meta.Meta
}

// newSyncFixture returns quota's store with every version live, and an engine of us-east-1
// over it.
func newSyncFixture(t *testing.T) *syncFixture {
	t.Helper()
	f := newPendingSync(t)
	if err := f.meta.SetLive(t.Context(), f.cat.Catalog()); err != nil {
		t.Fatal(err)
	}
	return f
}

// newPendingSync is newSyncFixture with every version pending its backfill.
func newPendingSync(t *testing.T) *syncFixture {
	t.Helper()
	m := mem.New()
	b := &scripted{Backend: m}
	return &syncFixture{
		cat:  compile(t, quota),
		mem:  m,
		b:    b,
		e:    engine.New(b, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})}),
		meta: meta.New(m, "app-meta"),
	}
}

// call is an event of api_calls at the bubble's start.
func call(account, endpoint, request string, units uint64) []expr.Value {
	return []expr.Value{expr.String(account), expr.String(endpoint), expr.String(request), expr.Uint(units), expr.String(""), micros(start)}
}

func (f *syncFixture) append(t *testing.T, e *engine.Engine, m engine.AppendMode, events ...[]expr.Value) []engine.Appended {
	t.Helper()
	res, err := e.Append(t.Context(), f.cat, "api_calls", events, m)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// used returns Used's calls and used units of account, as text, or its error.
func (f *syncFixture) used(t *testing.T, account string) string {
	t.Helper()
	rows, err := f.e.Query(t.Context(), f.cat, "Used", []expr.Value{expr.String(account)}, engine.QueryOptions{})
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("%d %d", rows[0][0].AsUint(), rows[0][1].AsUint())
}

// accounts returns Accounts' count of endpoint, as text, or its error.
func (f *syncFixture) accounts(t *testing.T, endpoint string) string {
	t.Helper()
	rows, err := f.e.Query(t.Context(), f.cat, "Accounts", []expr.Value{expr.String(endpoint)}, engine.QueryOptions{})
	if err != nil {
		return err.Error()
	}
	return strconv.FormatUint(rows[0][0].AsUint(), 10)
}

// syncAttr returns the _sync of the event stored at k, "absent" without one.
func (f *syncFixture) syncAttr(t *testing.T, k store.Key) string {
	t.Helper()
	it, err := f.mem.Get(t.Context(), callsTable, k, true)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := it[catalog.AttrSync]
	if !ok {
		return "absent"
	}
	if v.Type != store.L {
		return fmt.Sprintf("type %d", v.Type)
	}
	ids := make([]string, len(v.List))
	for i, id := range v.List {
		ids[i] = id.Str
	}
	return fmt.Sprint(ids)
}

// An append applies the live SYNC versions in its transaction and names them, sorted, in
// _sync; a query sees it at once, and the ASYNC view is left to the projector.
func TestSyncAppendAppliesLiveVersions(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newSyncFixture(t)
		res := f.append(t, f.e, engine.AppendMode{}, call("acme", "/search", "r1", 5))
		if res[0].Err != nil {
			t.Fatal(res[0].Err)
		}
		want := []string{
			"query app-meta consistent=true",
			"batchget app-view-endpoint_accounts-v1 consistent=true",
			"batchget app-view-quota_usage-v1 consistent=true",
			"transact app-stream-api_calls app-view-endpoint_accounts-v1 app-view-endpoint_accounts-v1 app-view-quota_usage-v1",
		}
		got := f.b.requests()
		slices.Sort(got[1:3]) // the versions are read in parallel
		if !slices.Equal(got, want) {
			t.Errorf("requests %q, want %q", got, want)
		}
		if got, want := f.syncAttr(t, res[0].Key), "[endpoint_accounts@v1 quota_usage@v1]"; got != want {
			t.Errorf("_sync %s, want %s", got, want)
		}
		daily, err := f.e.Query(t.Context(), f.cat, "Daily", []expr.Value{expr.String("acme")}, engine.QueryOptions{})
		if u, a := f.used(t, "acme"), f.accounts(t, "/search"); u != "1 5" || a != "1" || err != nil || len(daily) != 0 {
			t.Errorf("Used %s, Accounts %s, Daily %v (%v); want 1 5, 1 and no rows", u, a, daily, err)
		}
	})
}

// Only live SYNC versions are applied; with none, or for an import, the event is a plain
// put without _sync. An import, or a stream without SYNC versions, reads no state.
func TestSyncAppendAppliesOnlyLiveVersions(t *testing.T) {
	t.Parallel()
	activate := func(ctx context.Context, m *meta.Meta, version string) error {
		_, err := m.Activate(ctx, version, "api_calls", meta.SplitFence, meta.DefaultBounds)
		return err
	}
	all := compile(t, quota).Catalog()
	tests := []struct {
		name  string
		set   func(context.Context, *meta.Meta) error
		mode  engine.AppendMode
		write string // the request that writes the event
		sync  string
		meta  bool // whether the append read the states
	}{
		{"one live, one activating", func(ctx context.Context, m *meta.Meta) error {
			if err := activate(ctx, m, quotaUsage); err != nil {
				return err
			}
			if err := m.MakeLive(ctx, quotaUsage, time.Time{}); err != nil {
				return err
			}
			return activate(ctx, m, endpointAcc)
		}, engine.AppendMode{}, "transact app-stream-api_calls app-view-quota_usage-v1", "[quota_usage@v1]", true},
		{"none live", func(context.Context, *meta.Meta) error { return nil },
			engine.AppendMode{}, "put app-stream-api_calls", "absent", true},
		{"one retired, one broken", func(ctx context.Context, m *meta.Meta) error {
			if _, err := m.Retire(ctx, quotaUsage, "api_calls"); err != nil {
				return err
			}
			_, _, err := m.MarkBroken(ctx, endpointAcc, "api_calls", meta.Failure{Cause: meta.CauseRecord, EventID: "01J9ZQ3ZQ8V7W6X5Y4Z3A2B1C0", Error: "boom"})
			return err
		}, engine.AppendMode{}, "put app-stream-api_calls", "absent", true},
		{"an import, every version live", func(ctx context.Context, m *meta.Meta) error {
			return m.SetLive(ctx, all)
		}, engine.AppendMode{Import: true}, "put app-stream-api_calls", "absent", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newPendingSync(t)
				if err := tt.set(t.Context(), f.meta); err != nil {
					t.Fatal(err)
				}
				res := f.append(t, f.e, tt.mode, call("acme", "/search", "r1", 5))
				reqs := f.b.requests()
				if res[0].Err != nil || !slices.Contains(reqs, tt.write) || slices.Contains(reqs, "query app-meta consistent=true") != tt.meta {
					t.Errorf("append: %v, requests %q; want %q among them, and a state read: %v", res[0].Err, reqs, tt.write, tt.meta)
				}
				if got := f.syncAttr(t, res[0].Key); got != tt.sync {
					t.Errorf("_sync %s, want %s", got, tt.sync)
				}
			})
		})
	}
}

// A stream without SYNC versions reads no state.
func TestAppendWithoutSyncVersionsReadsNoState(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		m := mem.New()
		b := &scripted{Backend: m}
		c := compile(t, streams)
		e := engine.New(b, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})})
		appendOne(t, e, c, "page_views", pageView("/a", micros(start)), engine.AppendMode{})
		if got, want := b.requests(), []string{"put app-stream-page_views"}; !slices.Equal(got, want) {
			t.Errorf("requests %q, want %q", got, want)
		}
	})
}

// A transaction that conflicts or is throttled, or a throttled read, is followed by a wait
// drawn below 5 ms·2^n, at most 250 ms, and everything is read again; the read finds an event
// that an earlier attempt stored, which is then the append's. After 10 attempts the append
// fails at once with the last error and writes nothing. A resend without the token finds the
// append's own event.
func TestSyncAppendRetries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		outcomes  []outcome
		throttled int    // reads of quota_usage that are throttled
		err       string // "" for success
		sent      int    // transactions
		reads     int    // reads of quota_usage
		used      string
	}{
		{"conflicts, then applied", []outcome{conflicted, conflicted, conflicted}, 0, "", 4, 4, "1 5"},
		{"throttled, not applied", []outcome{throttledBefore}, 0, "", 2, 2, "1 5"},
		{"applied, then throttled", []outcome{throttledAfter}, 0, "", 2, 2, "1 5"},
		{"applied, then resent without its token", []outcome{resentAfter}, 0, "", 1, 1, "1 5"},
		{"reads throttled, then applied", nil, 2, "", 1, 3, "1 5"},
		{"10 conflicts", slices.Repeat([]outcome{conflicted}, 10), 0,
			"dynaflux: append to api_calls: SYNC versions endpoint_accounts@v1, quota_usage@v1: 10 attempts failed: store: conflict with a concurrent write",
			10, 10, "dynaflux: no row found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newSyncFixture(t)
				f.b.outcomes, f.b.throttledReads = tt.outcomes, tt.throttled
				began := time.Now()
				res := f.append(t, f.e, engine.AppendMode{}, call("acme", "/search", "r1", 5))
				if err := res[0].Err; tt.err == "" && err != nil || tt.err != "" && !errors.Is(err, store.ErrConflict) || err != nil && err.Error() != tt.err {
					t.Errorf("append: %v, want %q", err, tt.err)
				}
				reads := countOf(f.b.requests(), "batchget app-view-quota_usage-v1 consistent=true")
				if len(f.b.sent) != tt.sent || reads != tt.reads {
					t.Fatalf("%d transactions and %d reads of quota_usage, want %d and %d", len(f.b.sent), reads, tt.sent, tt.reads)
				}
				checkWaits(t, began, f.b.sent, tt.throttled > 0, tt.err != "")
				if got := f.used(t, "acme"); got != tt.used {
					t.Errorf("Used %s, want %s", got, tt.used)
				}
			})
		})
	}
}

// A row or marker that another append changed between the read and the transaction is read
// again after a wait drawn below 5 ms, and the transaction sent again: the other append's
// account counts once.
func TestSyncAppendReadsChangedItemsAgain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newSyncFixture(t)
		other := engine.New(f.b, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{1})})
		f.b.before = func() { f.append(t, other, engine.AppendMode{}, call("acme", "/search", "r2", 3)) }
		res := f.append(t, f.e, engine.AppendMode{}, call("acme", "/search", "r1", 5))
		if res[0].Err != nil {
			t.Fatal(res[0].Err)
		}
		reqs := f.b.requests()
		transactions := slices.DeleteFunc(slices.Clone(reqs), func(r string) bool { return !strings.HasPrefix(r, "transact") })
		// The other append's transaction writes the account's marker; the resend follows a
		// read of both versions, and leaves the marker out.
		n := len(reqs)
		tail := slices.Clone(reqs[n-4:])
		slices.Sort(tail[1:3]) // the versions are read in parallel
		want := []string{
			"transact app-stream-api_calls app-view-endpoint_accounts-v1 app-view-endpoint_accounts-v1 app-view-quota_usage-v1",
			"batchget app-view-endpoint_accounts-v1 consistent=true",
			"batchget app-view-quota_usage-v1 consistent=true",
			"transact app-stream-api_calls app-view-endpoint_accounts-v1 app-view-quota_usage-v1",
		}
		if len(transactions) != 3 || !slices.Equal(tail, want) {
			t.Errorf("requests %q, want 3 transactions, ending with %q", reqs, want)
		}
		if wait := f.b.sent[2].Sub(f.b.sent[1]); wait <= 0 || wait >= 5*time.Millisecond {
			t.Errorf("the resend came %v after the other append's transaction, want a wait under 5ms", wait)
		}
		if u, a := f.used(t, "acme"), f.accounts(t, "/search"); u != "2 8" || a != "1" {
			t.Errorf("Used %s, Accounts %s; want 2 8 and 1", u, a)
		}
	})
}

// Six writers that append to one row at once, each event after the one before, all succeed,
// and every event counts once. Scenario 34 of the e2e suite runs four such writers.
func TestSyncAppendsToAHotRow(t *testing.T) {
	t.Parallel()
	const writers, appends = 6, 25
	synctest.Test(t, func(t *testing.T) {
		f := newSyncFixture(t)
		b := remote{Backend: f.mem, rtt: 2 * time.Millisecond}
		var wg sync.WaitGroup
		for w := range writers {
			e := engine.New(b, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{byte(w)})})
			wg.Go(func() {
				for i := range appends {
					res, err := e.Append(t.Context(), f.cat, "api_calls", [][]expr.Value{call("acme", "/search", fmt.Sprintf("w%d-%d", w, i), 1)}, engine.AppendMode{})
					if err == nil {
						err = res[0].Err
					}
					if err != nil {
						t.Errorf("writer %d, append %d: %v", w, i, err)
					}
				}
			})
		}
		wg.Wait()
		want := fmt.Sprintf("%d %d", writers*appends, writers*appends)
		if u, a := f.used(t, "acme"), f.accounts(t, "/search"); u != want || a != "1" {
			t.Errorf("Used %s, Accounts %s; want %s and 1", u, a, want)
		}
	})
}

// remote is a store a round trip away: a BatchGet or Transact reaches it after half of rtt,
// and its response returns after the other half.
type remote struct {
	store.Backend

	rtt time.Duration
}

func (r remote) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	time.Sleep(r.rtt / 2)
	items, err := r.Backend.BatchGet(ctx, table, ks, consistent)
	time.Sleep(r.rtt / 2)
	return items, err
}

func (r remote) Transact(ctx context.Context, puts []store.TxPut) error {
	time.Sleep(r.rtt / 2)
	err := r.Backend.Transact(ctx, puts)
	time.Sleep(r.rtt / 2)
	return err
}

// An event stored before at the key is a duplicate unless it is the call's own: the rows
// keep the first event alone.
func TestSyncAppendDuplicate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		region string
		units  uint64
		same   bool
	}{
		{"another payload", "us-east-1", 7, false},
		{"another region's", "us-west-2", 5, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newSyncFixture(t)
				first := f.append(t, f.e, engine.AppendMode{}, call("acme", "/search", "r1", 5))
				e := engine.New(f.b, engine.Options{Region: tt.region, Entropy: rand.NewChaCha8([32]byte{1})})
				again := f.append(t, e, engine.AppendMode{}, call("acme", "/search", "r1", tt.units))
				if !errors.Is(again[0].Err, engine.ErrDuplicate) || again[0].Key != first[0].Key || again[0].SamePayload != tt.same {
					t.Errorf("again: %+v, want ErrDuplicate at %v with SamePayload %v", again[0], first[0].Key, tt.same)
				}
				if u, a := f.used(t, "acme"), f.accounts(t, "/search"); u != "1 5" || a != "1" {
					t.Errorf("Used %s, Accounts %s; want 1 5 and 1", u, a)
				}
			})
		})
	}
}

// A SYNC view's evaluation error, or a transaction DynamoDB refuses, fails the event alone
// and writes nothing; a state read that fails fails the call.
func TestSyncAppendRefuses(t *testing.T) {
	t.Parallel()
	big := call("acme", "/search", "r2", 5)
	big[4] = expr.String(strings.Repeat("x", 400<<10))
	tests := []struct {
		name  string
		event []expr.Value
		want  string
		is    error
	}{
		{"an evaluation error", call("acme", "/search", "r2", 0),
			"dynaflux: append to api_calls: projector: view endpoint_accounts@v1: evaluation error in intDiv: division by zero", engine.ErrEvaluation},
		{"an event over 400 KB", big,
			"dynaflux: append to api_calls: invalid value: the SYNC transaction: mem: transact: put 0: store: item or key over DynamoDB's size limit: item is up to 409789 bytes, over 409600", engine.ErrInvalidValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newSyncFixture(t)
				res := f.append(t, f.e, engine.AppendMode{}, call("acme", "/search", "r1", 5), tt.event)
				err := res[1].Err
				if res[0].Err != nil || err == nil || err.Error() != tt.want || !errors.Is(err, tt.is) {
					t.Errorf("appends: %v, %v; want the second to fail with %q", res[0].Err, err, tt.want)
				}
				if it, gerr := f.mem.Get(t.Context(), callsTable, res[1].Key, true); it != nil || gerr != nil {
					t.Errorf("the failed event is stored: %v (%v)", it, gerr)
				}
				if u, a := f.used(t, "acme"), f.accounts(t, "/search"); u != "1 5" || a != "1" {
					t.Errorf("Used %s, Accounts %s; want the first event's 1 5 and 1", u, a)
				}
			})
		})
	}
}

// A state read that fails fails the events not yet written, without another read.
func TestSyncAppendFailsWithItsStateRead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newSyncFixture(t)
		f.b.failMeta = true
		res := f.append(t, f.e, engine.AppendMode{}, call("acme", "/search", "r1", 5), call("acme", "/search", "r2", 5))
		want := "dynaflux: append to api_calls: meta: read view states: meta is down"
		for i, r := range res {
			if r.Err == nil || r.Err.Error() != want {
				t.Errorf("event %d: %v, want %q", i, r.Err, want)
			}
		}
		if got := f.b.requests(); !slices.Equal(got, []string{"query app-meta consistent=true"}) {
			t.Errorf("requests %q, want only the state read", got)
		}
	})
}

// Each event of a batch applies the versions live in states read for it: a version retired
// while the batch runs stops being applied once the states are 30 seconds old.
func TestSyncAppendReadsStatesForEachEvent(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newSyncFixture(t)
		f.b.before = func() {
			if _, err := f.meta.Retire(t.Context(), endpointAcc, "api_calls"); err != nil {
				t.Error(err)
			}
			time.Sleep(meta.MaxViewStateAge)
		}
		res := f.append(t, f.e, engine.AppendMode{}, call("acme", "/search", "r1", 5), call("acme", "/search", "r2", 3))
		if res[0].Err != nil || res[1].Err != nil {
			t.Fatal(res[0].Err, res[1].Err)
		}
		first, second := f.syncAttr(t, res[0].Key), f.syncAttr(t, res[1].Key)
		if first != "[endpoint_accounts@v1 quota_usage@v1]" || second != "[quota_usage@v1]" {
			t.Errorf("_sync %s and %s, want [endpoint_accounts@v1 quota_usage@v1] and [quota_usage@v1]", first, second)
		}
	})
}

// countOf returns how many of reqs are req.
func countOf(reqs []string, req string) int {
	n := 0
	for _, r := range reqs {
		if r == req {
			n++
		}
	}
	return n
}

// checkWaits checks the times of an append that began at began and sent its transactions at
// sent: each after a wait, less than 5 ms·2^(n-1) and at most 250 ms, after transaction n; the
// first after a wait when reads were throttled; and, when the append failed, no wait after
// the last.
func checkWaits(t *testing.T, began time.Time, sent []time.Time, throttledReads, failed bool) {
	t.Helper()
	for n := 1; n < len(sent); n++ {
		ceiling := min(250*time.Millisecond, 5*time.Millisecond<<(n-1))
		if wait := sent[n].Sub(sent[n-1]); wait <= 0 || wait >= ceiling {
			t.Errorf("transaction %d came %v after the one before, want a wait under %v", n+1, wait, ceiling)
		}
	}
	if throttledReads && sent[0].Equal(began) {
		t.Errorf("the transaction was sent at once, want it after a wait for each throttled read")
	}
	if last := sent[len(sent)-1]; failed && !time.Now().Equal(last) {
		t.Errorf("the append failed %v after its last transaction, want at once", time.Since(last))
	}
}

// A batch append's events are one transaction each, in input order.
func TestSyncAppendBatchInOrder(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newSyncFixture(t)
		var order []string
		f.b.Backend = &orderLog{Backend: f.mem, order: &order}
		res := f.append(t, f.e, engine.AppendMode{}, call("b", "/x", "r1", 1), call("a", "/x", "r2", 2), call("b", "/y", "r3", 3))
		for _, r := range res {
			if r.Err != nil {
				t.Fatal(r.Err)
			}
		}
		want := []string{res[0].Key.SK, res[1].Key.SK, res[2].Key.SK}
		if !slices.Equal(order, want) {
			t.Errorf("transactions of events %q, want %q", order, want)
		}
		if a, b, x := f.used(t, "a"), f.used(t, "b"), f.accounts(t, "/x"); a != "1 2" || b != "2 4" || x != "2" {
			t.Errorf("Used a %s, b %s, Accounts /x %s; want 1 2, 2 4 and 2", a, b, x)
		}
	})
}

// orderLog records the event of each transaction, by sort key.
type orderLog struct {
	store.Backend

	order *[]string
}

func (o *orderLog) Transact(ctx context.Context, puts []store.TxPut) error {
	*o.order = append(*o.order, puts[0].Item[catalog.AttrSK].Str)
	return o.Backend.Transact(ctx, puts)
}

// An event appended again after its item expired counts again, but its uniqExact value
// doesn't: the marker its first append wrote is present.
func TestSyncAppendAgainAfterExpiry(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newSyncFixture(t)
		first := f.append(t, f.e, engine.AppendMode{}, call("acme", "/search", "r1", 5))
		if err := f.mem.Delete(t.Context(), callsTable, first[0].Key); err != nil {
			t.Fatal(err)
		}
		again := f.append(t, f.e, engine.AppendMode{}, call("acme", "/search", "r1", 5))
		if u, a := f.used(t, "acme"), f.accounts(t, "/search"); again[0].Err != nil || u != "2 10" || a != "1" {
			t.Errorf("again: %v, Used %s, Accounts %s; want success, 2 10 and 1", again[0].Err, u, a)
		}
	})
}

// Every read of a SYNC version is strongly consistent; reads of an ASYNC one aren't.
func TestSyncVersionsReadConsistently(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newSyncFixture(t)
		f.append(t, f.e, engine.AppendMode{}, call("acme", "/search", "r1", 5))
		f.b.requests()
		ctx := t.Context()
		acme := []expr.Value{expr.String("acme")}
		_, qerr := f.e.Query(ctx, f.cat, "Used", acme, engine.QueryOptions{})
		_, derr := f.e.Query(ctx, f.cat, "Daily", acme, engine.QueryOptions{})
		_, _, rerr := f.e.ReadRow(ctx, f.cat, quotaUsage, acme)
		var perr error
		for _, err := range f.e.ReadPartition(ctx, f.cat, quotaUsage, acme) {
			perr = errors.Join(perr, err)
		}
		if err := errors.Join(qerr, derr, rerr, perr); err != nil {
			t.Fatal(err)
		}
		want := []string{
			"batchget app-view-quota_usage-v1 consistent=true",
			"query app-view-account_daily-v1 consistent=false",
			"get app-view-quota_usage-v1 consistent=true",
			"query app-view-quota_usage-v1 consistent=true",
		}
		if got := f.b.requests(); !slices.Equal(got, want) {
			t.Errorf("requests %q, want %q", got, want)
		}
	})
}

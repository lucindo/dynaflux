// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// The fixture's store: totals is additive, so it takes the fenced handoff, and an event whose
// n is 0 fails it; peaks is idempotent; sums is additive and SPLIT; kinds is idempotent but
// counts with markers, so it takes the fenced handoff too.
const (
	storeName = "shop"
	stream    = "events"
	totals    = "totals@v1"
	peaks     = "peaks@v1"
	sums      = "sums@v1"
	kinds     = "kinds@v1"
	storeSQL  = `
CREATE STREAM events (account String, n UInt32) PRIMARY KEY (account) LOG RETENTION 1 DAY;
CREATE MATERIALIZED VIEW totals ORDER BY (account)
AS SELECT account, count() AS events, sum(intDiv(100, n)) AS shares FROM events GROUP BY account;
CREATE MATERIALIZED VIEW peaks ORDER BY (account) AS SELECT account, max(n) AS peak FROM events GROUP BY account;
CREATE MATERIALIZED VIEW sums ORDER BY (account) SPLIT 3
AS SELECT account, count() AS events, sum(n) AS total FROM events GROUP BY account;
CREATE MATERIALIZED VIEW kinds ORDER BY (account) AS SELECT account, uniqExact(n) AS ns FROM events GROUP BY account;
`
	stepTime = 15 * time.Minute
)

// fixture is a region with the store's tables in memory, a backfill environment of fakes and
// an archive that tests fill. Its stream table is two days old, so the job reads the archive.
// Make it in a synctest bubble.
type fixture struct {
	t       *testing.T
	b       *mem.Mem
	m       *meta.Meta
	cat     *engine.Catalog
	env     *backfill.Env
	runs    *backfill.MemRuns
	archive *archiveFake
	units   *unitsFake
	tables  *tablesFake
	witness *witnessFake
	lag     *lagFake
	hash    string // the catalog's
	enc     *archive.Encoder
	engine  *engine.Engine // appends to the stream table; made on first use
	events  int
	// stepTime is each step's time, from its start to its deadline.
	stepTime time.Duration
}

// region is the fixture's, which archives every event of the stream.
const region = "us-east-1"

// streamARN is the ARN of the fixture's stream in its region.
const streamARN = "arn:aws:dynamodb:us-east-1:123456789012:table/shop-stream-events/stream/2000-01-01T00:00:00.000"

func newFixture(t *testing.T) *fixture {
	t.Helper()
	// The default config archives every event of every stream in every region.
	data := catalogtest.Marshal(t, storeName, storeSQL)
	cat, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	naming := catalog.Naming{Store: storeName}
	b := mem.New()
	m := meta.New(b, naming.MetaTable())
	if _, err = m.RegisterCatalog(t.Context(), data, "test"); err != nil {
		t.Fatal(err)
	}
	def, _ := cat.Catalog().Stream(stream)
	f := &fixture{
		t: t, b: b, m: m, cat: cat, runs: backfill.NewMemRuns(),
		archive: &archiveFake{complete: true, files: map[string][]byte{}, errors: map[string][]byte{}, written: map[string]time.Time{}},
		units:   &unitsFake{}, tables: &tablesFake{born: time.Now().Add(-48 * time.Hour)}, lag: &lagFake{},
		witness: &witnessFake{facts: backfill.Facts{ARN: streamARN, Mapping: true}},
		enc:     archive.NewEncoder(def), stepTime: stepTime, hash: catalog.Hash(data),
	}
	f.env = &backfill.Env{
		Naming:  naming,
		Catalog: cat.Catalog(),
		Hash:    catalog.Hash(data),
		Meta:    m,
		Store:   b,
		Core:    projector.New(cat.Projector(), b, nil, projector.Config{Naming: naming}),
		Units:   f.units,
		Tables:  f.tables,
		Archive: f.archive,
		Region:  region,
		Witness: f.witness,
		Runs:    f.runs,
		Lag:     f.lag,
		Bounds:  meta.DefaultBounds,
		Memory:  64 << 20,
		Log:     slog.New(slog.DiscardHandler),
	}
	f.recordOrigin()
	return f
}

// recordOrigin gives the stream the item a deploy writes, and an origin that a backfill -all
// recorded while the stream table, born when the fixture's tables say, was fresh.
func (f *fixture) recordOrigin() {
	f.t.Helper()
	ctx := f.t.Context()
	item, _, err := f.m.Stream(ctx, stream)
	if err == nil {
		err = f.m.ApplyArchiveModes(ctx, nil, map[string]meta.ArchiveMode{stream: meta.ArchiveAll}, true, f.tables.born)
	}
	if err == nil {
		item, _, err = f.m.Stream(ctx, stream)
	}
	if err == nil {
		_, err = f.m.RecordOrigin(ctx, item, meta.Origin{At: f.tables.born, Hash: f.hash, RegisteredAt: f.tables.born, Born: f.tables.born, ARN: streamARN})
	}
	if err != nil {
		f.t.Fatal(err)
	}
}

// seq returns the i-th sequence number, as the archive holds it.
func seq(i int) string { return fmt.Sprintf("%040d", i) }

// record adds to an archive file the record of an event of account with n, at seq i of unit.
func (f *fixture) record(file, unit string, i int, account string, n uint64) {
	f.archive.add(file, f.event(unit, i, account, n))
}

// recordError adds the same to an object of the error prefix.
func (f *fixture) recordError(object, unit string, i int, account string, n uint64) {
	f.archive.addError(object, f.event(unit, i, account, n))
}

// event returns the archive record of a new event of account with n, at seq i of unit, which
// arrived now.
func (f *fixture) event(unit string, i int, account string, n uint64) []byte {
	return f.eventAt(unit, i, account, n, time.Now())
}

// eventAt is event for a record that arrived at arrival.
func (f *fixture) eventAt(unit string, i int, account string, n uint64, arrival time.Time) []byte {
	f.events++
	id := fmt.Sprintf("01JB8Z9X4Q7M3N2P5R6S8%05d", f.events)
	at := arrival.UnixMicro()
	vals := []expr.Value{expr.String(account), expr.Uint(n), expr.String(id), expr.Micros(at), expr.Micros(at), expr.String("us-east-1")}
	return f.enc.AppendRecord(nil, vals, unit, seq(i), arrival)
}

// progress raises a unit's progress to seq i, as a projector's batch does.
func (f *fixture) progress(unit string, i int) {
	f.t.Helper()
	u, err := f.m.Unit(f.t.Context(), stream, unit)
	if err == nil {
		err = f.m.Advance(f.t.Context(), u, seq(i), time.Now(), func(string) bool { return true })
	}
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) start() meta.Job {
	f.t.Helper()
	j, err := backfill.Start(f.t.Context(), f.m, request())
	if err != nil {
		f.t.Fatal(err)
	}
	return j
}

func request() backfill.Request {
	return backfill.Request{Version: totals, Stream: stream, Name: "totals-v1", Execution: backfill.LocalExecution, Split: meta.SplitFence}
}

// step steps attempt 1.
func (f *fixture) step() (time.Duration, bool, error) {
	ctx, cancel := context.WithTimeout(f.t.Context(), f.stepTime)
	defer cancel()
	return backfill.Step(ctx, f.env, totals, 1, "local")
}

// run steps attempt 1 until it is done, waiting as each step says.
func (f *fixture) run() {
	f.t.Helper()
	for range 1000 {
		wait, done, err := f.step()
		if err != nil {
			f.t.Fatal(err)
		}
		if done {
			return
		}
		time.Sleep(wait)
	}
	f.t.Fatal("the job never ended")
}

func (f *fixture) job() meta.Job {
	f.t.Helper()
	j, ok, err := f.m.Job(f.t.Context(), totals)
	if err != nil || !ok {
		f.t.Fatalf("job: %v, %v", ok, err)
	}
	return j
}

func (f *fixture) state() meta.ViewState {
	f.t.Helper()
	states, err := f.m.ViewStates(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	return states[totals]
}

// rows returns totals' rows of accounts, each "account events shares", or "account -" when
// absent.
func (f *fixture) rows(accounts ...string) []string {
	f.t.Helper()
	return rowsIn(f.t, f.b, f.cat, accounts...)
}

func rowsIn(t *testing.T, b store.Backend, cat *engine.Catalog, accounts ...string) []string {
	t.Helper()
	e := engine.New(b, engine.Options{Region: "us-east-1"})
	var out []string
	for _, a := range accounts {
		row, ok, err := e.ReadRow(t.Context(), cat, totals, []expr.Value{expr.String(a)})
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			out = append(out, a+" -")
			continue
		}
		out = append(out, fmt.Sprintf("%s %d %d", a, row[1].AsUint(), row[2].AsUint()))
	}
	return out
}

// archiveFake is an archive whose export files and error objects tests write, and whose
// watermark they set. Every export it runs yields every file, and its queries end at once:
// they succeed unless outcomes, which tests set, give the next export's status. Partitions
// are parts, or one partition of a day on each side of the start without a record count.
// onOpen, when set, runs before a file opens.
type archiveFake struct {
	mu       sync.Mutex
	complete bool
	files    map[string][]byte
	errors   map[string][]byte
	written  map[string]time.Time // when each error object was last written
	parts    []backfill.Partition
	outcomes []backfill.QueryStatus
	throttle int    // how many starts to refuse with ErrThrottled
	loseNext bool   // the next export's start creates its query, but its response is lost
	running  int    // how many reads of an export's status say it runs, before its outcome
	failOpen string // a file whose next open fails
	queries  []fakeQuery
	onOpen   func(file string)
}

// fakeQuery is a query the archive started, its token and its status.
type fakeQuery struct {
	q      backfill.Query
	token  string
	status backfill.QueryStatus
}

func (a *archiveFake) add(file string, rec []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.files[file] = append(append(a.files[file], rec...), '\n')
}

// addError adds to an error object a line of the form Firehose writes a record it couldn't
// deliver in, with rec as its rawData.
func (a *archiveFake) addError(object string, rec []byte) {
	line := `{"attemptsMade":1,"errorCode":"Iceberg.InvalidRecord","rawData":"` + base64.StdEncoding.EncodeToString(rec) + `"}`
	a.mu.Lock()
	defer a.mu.Unlock()
	a.errors[object] = append(a.errors[object], line+"\n"...)
	a.written[object] = time.Now()
}

func (a *archiveFake) setComplete(complete bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.complete = complete
}

// setOutcomes sets the statuses of the next exports, in turn.
func (a *archiveFake) setOutcomes(outcomes ...backfill.QueryStatus) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.outcomes = outcomes
}

// started returns the queries the archive started, in order.
func (a *archiveFake) started() []fakeQuery {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.queries)
}

func (a *archiveFake) Complete(context.Context, string, time.Time) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.complete, nil
}

func (a *archiveFake) Start(_ context.Context, q backfill.Query, token string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.throttle > 0 {
		a.throttle--
		return "", fmt.Errorf("TooManyRequestsException: %w", backfill.ErrThrottled)
	}
	for i, fq := range a.queries {
		if fq.token == token {
			return queryID(i), nil
		}
	}
	st := backfill.QueryStatus{State: backfill.QuerySucceeded, Scanned: 100}
	if q.Kind == backfill.QueryExport && len(a.outcomes) > 0 {
		st, a.outcomes = a.outcomes[0], a.outcomes[1:]
	}
	a.queries = append(a.queries, fakeQuery{q: q, token: token, status: st})
	if a.loseNext && q.Kind == backfill.QueryExport {
		a.loseNext = false
		return "", errors.New("connection reset")
	}
	return queryID(len(a.queries) - 1), nil
}

func queryID(i int) string { return "q" + strconv.Itoa(i) }

func (a *archiveFake) query(id string) (fakeQuery, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i, err := strconv.Atoi(strings.TrimPrefix(id, "q"))
	if err != nil || i < 0 || i >= len(a.queries) {
		return fakeQuery{}, fmt.Errorf("no query %s", id)
	}
	return a.queries[i], nil
}

func (a *archiveFake) Status(_ context.Context, id string) (backfill.QueryStatus, error) {
	fq, err := a.query(id)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err == nil && fq.q.Kind == backfill.QueryExport && a.running > 0 {
		a.running--
		return backfill.QueryStatus{State: backfill.QueryRunning}, nil
	}
	return fq.status, err
}

func (a *archiveFake) Stop(context.Context, string) error { return nil }

func (a *archiveFake) Partitions(_ context.Context, id string) ([]backfill.Partition, error) {
	fq, err := a.query(id)
	switch {
	case err != nil:
		return nil, err
	case fq.q.Kind != backfill.QueryPartitions || fq.status.State != backfill.QuerySucceeded:
		return nil, fmt.Errorf("query %s listed no partitions: %+v", id, fq)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.parts != nil {
		return slices.Clone(a.parts), nil
	}
	day := time.Now().Truncate(24 * time.Hour)
	return []backfill.Partition{{From: day.Add(-24 * time.Hour), To: day.Add(48 * time.Hour), Bytes: 1 << 20}}, nil
}

func (a *archiveFake) Files(_ context.Context, id string) ([]string, error) {
	fq, err := a.query(id)
	switch {
	case err != nil:
		return nil, err
	case fq.q.Kind != backfill.QueryExport || fq.status.State != backfill.QuerySucceeded:
		return nil, fmt.Errorf("query %s exported no files: %+v", id, fq)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Sorted(maps.Keys(a.files)), nil
}

func (a *archiveFake) ErrorObjects(_ context.Context, _, after string, since time.Time) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.DeleteFunc(slices.Sorted(maps.Keys(a.errors)), func(name string) bool {
		return name <= after || a.written[name].Before(since)
	}), nil
}

func (a *archiveFake) Open(_ context.Context, name string) (io.ReadCloser, error) {
	if a.onOpen != nil {
		a.onOpen(name)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if name == a.failOpen {
		a.failOpen = ""
		return nil, errors.New("SlowDown")
	}
	data, ok := a.files[name]
	if !ok {
		data = a.errors[name]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// unitsFake lists the units a test sets, and calls onList, when set, before it returns.
type unitsFake struct {
	mu     sync.Mutex
	listed []string
	onList func()
}

func (u *unitsFake) set(units ...string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.listed = units
}

func (u *unitsFake) Units(context.Context, string) ([]string, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.onList != nil {
		u.onList()
	}
	return slices.Clone(u.listed), nil
}

type tablesFake struct {
	born     time.Time
	restored bool
}

func (t *tablesFake) Born(context.Context, string) (time.Time, bool, error) {
	return t.born, t.restored, nil
}

// witnessFake shows the facts a test sets, whatever the origin.
type witnessFake struct {
	mu    sync.Mutex
	facts backfill.Facts
}

func (w *witnessFake) set(facts backfill.Facts) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.facts = facts
}

func (w *witnessFake) StreamARN(context.Context, string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.facts.ARN, nil
}

func (w *witnessFake) Facts(context.Context, string, meta.StreamItem) (backfill.Facts, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.facts, nil
}

type lagFake struct {
	mu  sync.Mutex
	lag time.Duration
}

func (l *lagFake) set(lag time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lag = lag
}

func (l *lagFake) Lag(context.Context) (time.Duration, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lag, nil
}

// putEvent writes a stream item into the region's replica, as an append would.
func (f *fixture) putEvent() {
	f.t.Helper()
	it := store.Item{
		catalog.AttrPK: {Type: store.S, Str: "a"},
		catalog.AttrSK: {Type: store.S, Str: "x"},
	}
	if err := f.b.Put(f.t.Context(), f.env.Naming.StreamTable(stream), it, store.Cond{}); err != nil {
		f.t.Fatal(err)
	}
}

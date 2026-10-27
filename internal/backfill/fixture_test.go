// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
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
	lag     *lagFake
	enc     *archive.Encoder
	engine  *engine.Engine // appends to the stream table; made on first use
	events  int
	// stepTime is each step's time, from its start to its deadline.
	stepTime time.Duration
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
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
		archive: &archiveFake{complete: true, files: map[string][]byte{}},
		units:   &unitsFake{}, tables: &tablesFake{born: time.Now().Add(-48 * time.Hour)}, lag: &lagFake{},
		enc: archive.NewEncoder(def), stepTime: stepTime,
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
		Runs:    f.runs,
		Lag:     f.lag,
		Bounds:  meta.DefaultBounds,
		Memory:  64 << 20,
		Log:     slog.New(slog.DiscardHandler),
	}
	return f
}

// seq returns the i-th sequence number, as the archive holds it.
func seq(i int) string { return fmt.Sprintf("%040d", i) }

// record adds to an archive file the record of an event of account with n, at seq i of unit.
func (f *fixture) record(file, unit string, i int, account string, n uint64) {
	f.events++
	id := fmt.Sprintf("01JB8Z9X4Q7M3N2P5R6S8%05d", f.events)
	at := time.Now().UnixMicro()
	vals := []expr.Value{expr.String(account), expr.Uint(n), expr.String(id), expr.Micros(at), expr.Micros(at), expr.String("us-east-1")}
	f.archive.add(file, f.enc.AppendRecord(nil, vals, unit, seq(i), time.Time{}))
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

// archiveFake is an archive whose files tests write, and whose watermark they set. onOpen,
// when set, runs before a file opens.
type archiveFake struct {
	mu       sync.Mutex
	complete bool
	files    map[string][]byte
	onOpen   func(file string)
}

func (a *archiveFake) add(file string, rec []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.files[file] = append(append(a.files[file], rec...), '\n')
}

func (a *archiveFake) setComplete(complete bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.complete = complete
}

func (a *archiveFake) Complete(context.Context, string, time.Time) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.complete, nil
}

func (a *archiveFake) Export(context.Context, *meta.Job) ([]string, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Sorted(maps.Keys(a.files)), true, nil
}

func (a *archiveFake) Open(_ context.Context, file string) (io.ReadCloser, error) {
	if a.onOpen != nil {
		a.onOpen(file)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return io.NopCloser(bytes.NewReader(a.files[file])), nil
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

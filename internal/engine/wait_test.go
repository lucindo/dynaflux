// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// waits is the store of the wait tests: a view that an event of no ms fails, and queries
// over it and over the stream.
const waits = `
CREATE STREAM visits (site String, request String, ms UInt32)
PRIMARY KEY (site)
DEDUP BY (request);

CREATE MATERIALIZED VIEW site_views
ORDER BY (site)
AS SELECT site, count() AS views, sum(intDiv(1000, ms)) AS rate
FROM visits
GROUP BY site;

-- name: Views :one
SELECT views FROM site_views WHERE site = {site:String};

-- name: Requests :many
SELECT request FROM visits WHERE site = {site:String};
`

const visitsTable = "app-stream-visits"

// waitStore appends visits in us-east-1, and projects them when a test says so, with that
// region's core.
type waitStore struct {
	cat  *engine.Catalog
	mem  *mem.Mem
	log  *waitLog
	e    *engine.Engine
	core *projector.Core
	seq  int
}

func newWaitStore(t *testing.T) *waitStore {
	t.Helper()
	cat := compile(t, waits)
	m := mem.New()
	naming := catalog.Naming{Store: "app"}
	if err := meta.New(m, naming.MetaTable()).SetLive(t.Context(), cat.Catalog()); err != nil {
		t.Fatal(err)
	}
	log := &waitLog{Backend: m, meta: naming.MetaTable()}
	return &waitStore{
		cat:  cat,
		mem:  m,
		log:  log,
		e:    engine.New(log, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})}),
		core: projector.New(cat.Projector(), m, nil, projector.Config{Naming: naming, Region: "us-east-1"}),
	}
}

// visit returns a visit of site a now.
func visit(request string, ms uint64) []expr.Value {
	return []expr.Value{expr.String("a"), expr.String(request), expr.Uint(ms), micros(time.Now())}
}

// append appends evs with e and returns their token and the stored events.
func (s *waitStore) append(t *testing.T, e *engine.Engine, m engine.AppendMode, evs ...[]expr.Value) (string, []store.Item) {
	t.Helper()
	res, err := e.Append(t.Context(), s.cat, "visits", evs, m)
	if err != nil {
		t.Fatal(err)
	}
	var stored []store.Item
	for _, r := range res {
		if r.Err != nil && !errors.Is(r.Err, engine.ErrDuplicate) {
			t.Fatal(r.Err)
		}
		stored = append(stored, r.Stored)
	}
	return e.Token("visits", stored), stored
}

// project hands stored events to the core as the next records of one unit.
func (s *waitStore) project(t *testing.T, stored ...store.Item) {
	t.Helper()
	var recs []projector.Record
	for _, it := range stored {
		s.seq++
		recs = append(recs, projector.Record{Op: projector.Insert, Seq: fmt.Sprintf("1%020d", s.seq), New: it})
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := s.core.Handle(ctx, projector.Batch{Stream: "visits", Unit: "shardId-1", Records: recs}); err != nil {
		t.Fatal(err)
	}
}

// views runs Views for site a and returns its one count.
func (s *waitStore) views(ctx context.Context, o engine.QueryOptions) (uint64, error) {
	rows, err := s.e.Query(ctx, s.cat, "Views", []expr.Value{expr.String("a")}, o)
	if err != nil {
		return 0, err
	}
	return rows[0][0].AsUint(), nil
}

func waitFor(tok string, timeout time.Duration) engine.QueryOptions {
	return engine.QueryOptions{Wait: []engine.Wait{{Token: tok, Timeout: timeout}}}
}

// waitLog records when each receipt read was sent, and whether each read of another table
// was strongly consistent.
type waitLog struct {
	store.Backend

	meta string

	mu    sync.Mutex
	calls int
	polls []time.Time
	reads []string // "table consistent"
}

func (l *waitLog) note(table string, consistent bool, poll bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	switch {
	case poll:
		l.polls = append(l.polls, time.Now())
	case table != l.meta:
		l.reads = append(l.reads, table+" "+strconv.FormatBool(consistent))
	}
}

func (l *waitLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls, l.polls, l.reads = 0, nil, nil
}

func (l *waitLog) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	l.note(table, consistent, false)
	return l.Backend.Get(ctx, table, k, consistent)
}

func (l *waitLog) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	l.note(table, consistent, table == l.meta)
	return l.Backend.BatchGet(ctx, table, ks, consistent)
}

func (l *waitLog) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	l.note(table, q.Consistent, false)
	return l.Backend.Query(ctx, table, q)
}

func (l *waitLog) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	l.note(table, false, false)
	return l.Backend.Put(ctx, table, it, c)
}

// A wait polls at once, then after 50, 100, 200 and 400 ms, then every 500 ms, each ±20 %,
// and ends at the first poll that finds the receipt; the rows then hold the event.
func TestWaitPollsUntilTheReceipt(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newWaitStore(t)
		tok, stored := s.append(t, s.e, engine.AppendMode{Receipt: true}, visit("r1", 100))
		s.log.reset()
		type result struct {
			views uint64
			err   error
		}
		done := make(chan result)
		go func() {
			n, err := s.views(t.Context(), waitFor(tok, 10*time.Second))
			done <- result{n, err}
		}()
		time.Sleep(2 * time.Second)
		s.project(t, stored...)
		got := <-done
		if got.err != nil || got.views != 1 {
			t.Fatalf("Views = %d, %v; want 1", got.views, got.err)
		}
		polls := s.log.polls
		if !polls[0].Equal(start) || !polls[len(polls)-2].Before(start.Add(2*time.Second)) || polls[len(polls)-1].Before(start.Add(2*time.Second)) {
			t.Errorf("polls at %v; want the first at once and the last the first one after the receipt", polls)
		}
		ms := time.Millisecond
		schedule := []time.Duration{50 * ms, 100 * ms, 200 * ms, 400 * ms, 500 * ms, 500 * ms, 500 * ms, 500 * ms}
		for i := 1; i < len(polls); i++ {
			gap, d := polls[i].Sub(polls[i-1]), schedule[i-1]
			if gap < d*4/5 || gap > d*6/5 {
				t.Errorf("poll %d came %v after the one before, want %v ±20%%", i, gap, d)
			}
		}
	})
}

// A wait ends with ErrWaitTimeout once its timeout passes, after a last poll then, or with
// the context's error when the context ends first.
func TestWaitTimesOut(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newWaitStore(t)
		tok, _ := s.append(t, s.e, engine.AppendMode{Receipt: true}, visit("r1", 100))
		_, err := s.views(t.Context(), waitFor(tok, time.Second))
		want := "dynaflux: query Views: wait for an append timed out: 1 of 1 events of visits have no receipt after 1s"
		if !errors.Is(err, engine.ErrWaitTimeout) || err.Error() != want || time.Since(start) != time.Second {
			t.Errorf("Views after %v: %v; want %q after 1s", time.Since(start), err, want)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		if _, err = s.views(ctx, waitFor(tok, time.Second)); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Views with a context that ends first: %v, want context.DeadlineExceeded", err)
		}
	})
}

// eventID returns the event_id of a stored event.
func eventID(t *testing.T, it store.Item) string {
	t.Helper()
	_, id, err := keys.ParseStreamSK(it[catalog.AttrSK].Str)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// A wait refuses, before any read, a token that it can't wait for, over a view or a stream.
func TestWaitRefuses(t *testing.T) {
	t.Parallel()
	waitable := engine.AppendMode{Receipt: true}
	tests := []struct {
		name string
		// token returns the token to wait for and the error, which follows "query <name>: ".
		token func(t *testing.T, s *waitStore) (string, string)
	}{
		{"another region's", func(t *testing.T, s *waitStore) (string, string) {
			t.Helper()
			tok, _ := s.append(t, engine.New(s.mem, engine.Options{Region: "us-west-2"}), waitable, visit("r1", 100))
			return tok, "the token is of region us-west-2, not us-east-1: wait in the region of the append, whose projector writes its receipts"
		}},
		{"appended without Waitable", func(t *testing.T, s *waitStore) (string, string) {
			t.Helper()
			tok, stored := s.append(t, s.e, engine.AppendMode{}, visit("r1", 100))
			return tok, "event " + eventID(t, stored[0]) + " was appended without a receipt: append with dynaflux.Waitable() to wait for it"
		}},
		{"stored by another region", func(t *testing.T, s *waitStore) (string, string) {
			t.Helper()
			s.append(t, engine.New(s.mem, engine.Options{Region: "us-west-2"}), waitable, visit("r1", 100))
			tok, stored := s.append(t, s.e, waitable, visit("r1", 100))
			return tok, "event " + eventID(t, stored[0]) + " was stored by another region, whose projector writes its receipt"
		}},
		{"over 1000 events", func(t *testing.T, s *waitStore) (string, string) {
			t.Helper()
			var evs [][]expr.Value
			for i := range engine.MaxWaitEvents + 1 {
				evs = append(evs, visit("r"+strconv.Itoa(i), 100))
			}
			tok, _ := s.append(t, s.e, waitable, evs...)
			return tok, "the token names 1001 events, over the 1000 that a wait reads"
		}},
		{"of a stream the catalog doesn't have", func(t *testing.T, s *waitStore) (string, string) {
			t.Helper()
			other := compile(t, "CREATE STREAM clicks (page String) PRIMARY KEY (page);")
			res, err := s.e.Append(t.Context(), other, "clicks", [][]expr.Value{{expr.String("/a"), expr.Null()}}, waitable)
			if err != nil || res[0].Err != nil {
				t.Fatal(err, res[0].Err)
			}
			return s.e.Token("clicks", []store.Item{res[0].Stored}), "the token names stream clicks, which the catalog doesn't have"
		}},
		{"malformed", func(*testing.T, *waitStore) (string, string) {
			return "AQ", "malformed token: not the text of a token that an append returned"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newWaitStore(t)
				tok, want := tt.token(t, s)
				for _, query := range []string{"Views", "Requests"} {
					s.log.reset()
					_, err := s.e.Query(t.Context(), s.cat, query, []expr.Value{expr.String("a")}, waitFor(tok, time.Second))
					if w := "dynaflux: query " + query + ": " + want; err == nil || err.Error() != w || s.log.calls != 0 {
						t.Errorf("%s: %v after %d requests; want %q before any", query, err, s.log.calls, w)
					}
				}
			})
		})
	}
}

// A token holds no PRIMARY KEY value of its events.
func TestTokenLeavesOutPrimaryKeys(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newWaitStore(t)
		const site = "customer-4417@example.com"
		ev := []expr.Value{expr.String(site), expr.String("r1"), expr.Uint(100), micros(time.Now())}
		tok, _ := s.append(t, s.e, engine.AppendMode{Receipt: true}, ev)
		b, err := base64.RawURLEncoding.DecodeString(tok)
		if err != nil || bytes.Contains(b, []byte(site)) {
			t.Errorf("token %q (%v) holds %q", b, err, site)
		}
	})
}

// After a wait, a query reads the version's state again, not the cached one: the waited
// event broke the version, and the projector still wrote its receipt. AllowPartial reads
// the rows anyway.
func TestWaitReadsTheStateAgain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newWaitStore(t)
		_, first := s.append(t, s.e, engine.AppendMode{Receipt: true}, visit("r1", 100))
		s.project(t, first...)
		if n, err := s.views(t.Context(), engine.QueryOptions{}); n != 1 || err != nil {
			t.Fatalf("Views = %d, %v; want 1", n, err)
		}
		tok, poison := s.append(t, s.e, engine.AppendMode{Receipt: true}, visit("r2", 0))
		s.project(t, poison...)
		cached, cerr := s.views(t.Context(), engine.QueryOptions{})
		_, err := s.views(t.Context(), waitFor(tok, time.Second))
		partial := waitFor(tok, time.Second)
		partial.AllowPartial = true
		n, perr := s.views(t.Context(), partial)
		want := "dynaflux: query Views: view version not ready: site_views@v1 is broken in us-east-1: event " +
			eventID(t, poison[0]) + ": evaluation error in intDiv: division by zero"
		if cached != 1 || cerr != nil || err == nil || err.Error() != want || n != 1 || perr != nil {
			t.Errorf("Views from the cache = %d, %v; after the wait %v; with AllowPartial %d, %v; want 1, %q, 1", cached, cerr, err, n, perr, want)
		}
	})
}

// After a wait, every read is strongly consistent; a query over a stream polls nothing. The
// zero token waits for nothing.
func TestWaitReadsConsistently(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, query string
		wait        bool
		polls       int
		reads       []string
	}{
		{"view", "Views", true, 1, []string{"app-view-site_views-v1 true"}},
		{"stream", "Requests", true, 0, []string{visitsTable + " true"}},
		{"view, zero token", "Views", false, 0, []string{"app-view-site_views-v1 false"}},
		{"stream, zero token", "Requests", false, 0, []string{visitsTable + " false"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newWaitStore(t)
				tok, stored := s.append(t, s.e, engine.AppendMode{Receipt: true}, visit("r1", 100))
				s.project(t, stored...)
				if !tt.wait {
					tok = ""
				}
				s.log.reset()
				rows, err := s.e.Query(t.Context(), s.cat, tt.query, []expr.Value{expr.String("a")}, waitFor(tok, time.Second))
				if err != nil || len(rows) != 1 || len(s.log.polls) != tt.polls || !slices.Equal(s.log.reads, tt.reads) {
					t.Errorf("%d rows (%v), %d polls, reads %q; want 1 row, %d polls, reads %q", len(rows), err, len(s.log.polls), s.log.reads, tt.polls, tt.reads)
				}
			})
		})
	}
}

// A DEDUP BY event appended again after its item expired is a new record: the receipt of
// the one before doesn't end a wait for it.
func TestWaitForAnEventAppendedAgain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newWaitStore(t)
		ev := visit("r1", 100)
		_, first := s.append(t, s.e, engine.AppendMode{Receipt: true}, ev)
		s.project(t, first...)
		if err := s.mem.Delete(t.Context(), visitsTable, store.Key{PK: first[0][catalog.AttrPK].Str, SK: first[0][catalog.AttrSK].Str}); err != nil {
			t.Fatal(err)
		}
		tok, again := s.append(t, s.e, engine.AppendMode{Receipt: true}, ev)
		_, err := s.views(t.Context(), waitFor(tok, time.Second))
		s.project(t, again...)
		n, nerr := s.views(t.Context(), waitFor(tok, time.Second))
		if !errors.Is(err, engine.ErrWaitTimeout) || n != 2 || nerr != nil {
			t.Errorf("Views before the projector applied the event again: %v; after: %d, %v; want ErrWaitTimeout, then 2", err, n, nerr)
		}
	})
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/big"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/store/storetest"
)

// streams is the test store: page views as in sql.md §12.1, a DEDUP BY stream and a
// sharded one.
const streams = `
CREATE STREAM page_views (
    user_id     String,
    page        String,
    referrer    Nullable(String),
    tags        Array(String),
    duration_ms UInt32,
    country     LowCardinality(String) DEFAULT ''
)
PRIMARY KEY (user_id)
LOG RETENTION 2 DAY
ACCEPT event_time BETWEEN now() - INTERVAL 30 DAY AND now() + INTERVAL 5 MINUTE;

CREATE STREAM api_calls (
    account_id String,
    request_id UUID,
    units      UInt32,
    price      Decimal(10, 2)
)
PRIMARY KEY (account_id)
DEDUP BY (request_id);

CREATE STREAM clicks (
    user_id String,
    target  String
)
PRIMARY KEY (user_id)
SHARDS 8;
`

// start is where the fake clock of a synctest bubble starts.
var start = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) //nolint:gochecknoglobals // a constant time.Time

func compile(t *testing.T, sql string) *engine.Catalog {
	t.Helper()
	c, err := engine.Compile(catalogtest.Marshal(t, "app", sql))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newEngine(b store.Backend) *engine.Engine {
	return engine.New(b, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})})
}

func micros(t time.Time) expr.Value { return expr.Micros(t.UnixMicro()) }

// pageView is an event of page_views by user u1: user_id, page, referrer, tags,
// duration_ms, country, then event_time.
func pageView(page string, at expr.Value) []expr.Value {
	return []expr.Value{
		expr.String("u1"), expr.String(page), expr.Null(), expr.Array(nil),
		expr.Uint(100), expr.String("US"), at,
	}
}

// apiCall is an event of api_calls: account_id, request_id, units, price, then event_time.
func apiCall(request byte, units uint64, at time.Time) []expr.Value {
	return []expr.Value{
		expr.String("acme"), expr.UUID([16]byte{15: request}), expr.Uint(units),
		expr.Decimal(big.NewInt(1250)), micros(at),
	}
}

func appendOne(t *testing.T, e *engine.Engine, c *engine.Catalog, stream string, ev []expr.Value, m engine.AppendMode) engine.Appended {
	t.Helper()
	res, err := e.Append(t.Context(), c, stream, [][]expr.Value{ev}, m)
	if err != nil {
		t.Fatal(err)
	}
	return res[0]
}

func readAll(t *testing.T, e *engine.Engine, c *engine.Catalog, stream, user string, from, to *int64) [][]expr.Value {
	t.Helper()
	var rows [][]expr.Value
	for row, err := range e.Read(t.Context(), c, stream, []expr.Value{expr.String(user)}, from, to) {
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

// column returns the strings in column i of rows.
func column(rows [][]expr.Value, i int) []string {
	out := make([]string, len(rows))
	for j, r := range rows {
		out[j] = r[i].AsString()
	}
	return out
}

func TestAppendDuplicateReturnsStoredKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		units  uint64        // the first append has 5
		later  time.Duration // between the appends
		region string        // of the second append; "" for the first append's engine
	}{
		{"another payload", 7, 0, "us-east-1"},
		{"another writer, later", 5, time.Millisecond, "us-east-1"},
		{"another region at the same instant", 5, 0, "us-west-2"},
		{"the caller's earlier call at the same instant", 5, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := compile(t, streams)
				b := mem.New()
				e := newEngine(b)
				first := appendOne(t, e, c, "api_calls", apiCall(1, 5, start), engine.AppendMode{})
				if first.Err != nil {
					t.Fatal(first.Err)
				}
				time.Sleep(tt.later)
				other := e
				if tt.region != "" {
					other = engine.New(b, engine.Options{Region: tt.region})
				}
				dup := appendOne(t, other, c, "api_calls", apiCall(1, tt.units, start), engine.AppendMode{})
				if !errors.Is(dup.Err, engine.ErrDuplicate) || dup.Key != first.Key {
					t.Fatalf("got %v at %v, want ErrDuplicate at %v", dup.Err, dup.Key, first.Key)
				}
				rows := readAll(t, other, c, "api_calls", "acme", nil, nil)
				if len(rows) != 1 || rows[0][2].AsUint() != 5 {
					t.Errorf("stored %d events, want the first one, with units 5", len(rows))
				}
			})
		})
	}
}

func TestAppendRetriedBySDKSucceeds(t *testing.T) {
	t.Parallel()
	for _, stream := range []string{"page_views", "api_calls"} {
		t.Run(stream, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := compile(t, streams)
				b := mem.New()
				e := newEngine(storetest.RetriedWrite(b))
				ev := pageView("/home", micros(start))
				if stream == "api_calls" {
					ev = apiCall(1, 5, start)
				}
				if got := appendOne(t, e, c, stream, ev, engine.AppendMode{}); got.Err != nil {
					t.Fatalf("an append that the SDK retried failed: %v", got.Err)
				}
				user := "u1"
				if stream == "api_calls" {
					user = "acme"
				}
				if rows := readAll(t, newEngine(b), c, stream, user, nil, nil); len(rows) != 1 {
					t.Errorf("stored %d events, want 1", len(rows))
				}
			})
		})
	}
}

func TestAppendBatchDuplicateIsNotWritten(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, streams)
		e := newEngine(mem.New())
		res, err := e.Append(t.Context(), c, "api_calls", [][]expr.Value{
			apiCall(1, 5, start), apiCall(2, 6, start), apiCall(1, 5, start),
		}, engine.AppendMode{})
		if err != nil {
			t.Fatal(err)
		}
		if res[0].Err != nil || res[1].Err != nil || !errors.Is(res[2].Err, engine.ErrDuplicate) || res[2].Key != res[0].Key {
			t.Fatalf("got %+v, want events 0 and 1 stored and event 2 a duplicate of event 0", res)
		}
		if rows := readAll(t, e, c, "api_calls", "acme", nil, nil); len(rows) != 2 {
			t.Errorf("stored %d events, want 2", len(rows))
		}
	})
}

func TestAppendBatchDuplicateOfFailedEventFails(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, "CREATE STREAM d (k String, id UInt32, body String) PRIMARY KEY (k) DEDUP BY (id);")
		e := newEngine(mem.New())
		ev := func(body string) []expr.Value {
			return []expr.Value{expr.String("k1"), expr.Uint(1), expr.String(body), micros(start)}
		}
		res, err := e.Append(t.Context(), c, "d", [][]expr.Value{ev(strings.Repeat("x", 401<<10)), ev("small")}, engine.AppendMode{})
		if err != nil {
			t.Fatal(err)
		}
		if !errors.Is(res[0].Err, engine.ErrInvalidValue) || !errors.Is(res[1].Err, res[0].Err) {
			t.Errorf("got %v and %v, want the first event's ErrInvalidValue for both", res[0].Err, res[1].Err)
		}
		if n := len(readAll(t, e, c, "d", "k1", nil, nil)); n != 0 {
			t.Errorf("stored %d events, want 0", n)
		}
	})
}

func TestAppendKeepsOrderWithinOneMillisecond(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, streams)
		e := newEngine(mem.New())
		at := micros(start.Add(-time.Minute))
		pages := []string{"/c", "/a", "/d", "/b"}
		for _, p := range pages[:2] {
			if got := appendOne(t, e, c, "page_views", pageView(p, at), engine.AppendMode{}); got.Err != nil {
				t.Fatal(got.Err)
			}
		}
		if _, err := e.Append(t.Context(), c, "page_views", [][]expr.Value{pageView(pages[2], at), pageView(pages[3], at)}, engine.AppendMode{}); err != nil {
			t.Fatal(err)
		}
		rows := readAll(t, e, c, "page_views", "u1", nil, nil)
		if got := column(rows, 1); !slices.Equal(got, pages) {
			t.Errorf("read %q, want the append order %q", got, pages)
		}
	})
}

func TestAcceptWindowIsInclusive(t *testing.T) {
	t.Parallel()
	past := start.Add(-30 * 24 * time.Hour)
	future := start.Add(5 * time.Minute)
	tests := []struct {
		name string
		at   time.Time
		mode engine.AppendMode
		want error
	}{
		{"lower bound", past, engine.AppendMode{}, nil},
		{"before lower bound", past.Add(-time.Microsecond), engine.AppendMode{}, engine.ErrOutsideWindow},
		{"upper bound", future, engine.AppendMode{}, nil},
		{"after upper bound", future.Add(time.Microsecond), engine.AppendMode{}, engine.ErrOutsideWindow},
		{"import", start.Add(24 * time.Hour), engine.AppendMode{Import: true}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := compile(t, streams)
				got := appendOne(t, newEngine(mem.New()), c, "page_views", pageView("/", micros(tt.at)), tt.mode)
				if !errors.Is(got.Err, tt.want) || (tt.want == nil) != (got.Err == nil) {
					t.Errorf("got %v, want %v", got.Err, tt.want)
				}
			})
		})
	}
}

func TestAppendRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	at := micros(start)
	set := func(ev []expr.Value, i int, v expr.Value) []expr.Value {
		ev = slices.Clone(ev)
		ev[i] = v
		return ev
	}
	view := pageView("/home", at)
	tests := []struct {
		name   string
		stream string
		ev     []expr.Value
		column string
	}{
		{"invalid UTF-8", "page_views", set(view, 1, expr.String("\xff")), "page"},
		{"NULL in a column that isn't Nullable", "page_views", set(view, 1, expr.Null()), "page"},
		{"wrong representation", "page_views", set(view, 1, expr.Int(1)), "page"},
		{"1001 array elements", "page_views", set(view, 3, expr.Array(make([]expr.Value, 1001))), "tags"},
		{"integer out of range", "page_views", set(view, 4, expr.Uint(1<<32)), "duration_ms"},
		{"partition key over 2048 bytes", "page_views", set(view, 0, expr.String(strings.Repeat("u", 2049))), "user_id"},
		{"time past 2106", "page_views", set(view, 6, micros(time.Date(2107, 1, 1, 0, 0, 0, 0, time.UTC))), "event_time"},
		{"Decimal over its precision", "api_calls", set(apiCall(1, 5, start), 3, expr.Decimal(big.NewInt(1e10))), "price"},
		{"DEDUP BY without event_time", "api_calls", set(apiCall(1, 5, start), 4, expr.Null()), "event_time"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := compile(t, streams)
				got := appendOne(t, newEngine(mem.New()), c, tt.stream, tt.ev, engine.AppendMode{Import: true})
				var ve *engine.ValueError
				if !errors.As(got.Err, &ve) || ve.Name != tt.column || !errors.Is(got.Err, engine.ErrInvalidValue) {
					t.Errorf("got %v, want a *ValueError for %s", got.Err, tt.column)
				}
			})
		})
	}
}

func TestAppendRejectsEventOverItemLimit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, streams)
		ev := pageView(strings.Repeat("p", 401<<10), micros(start))
		got := appendOne(t, newEngine(mem.New()), c, "page_views", ev, engine.AppendMode{})
		if !errors.Is(got.Err, engine.ErrInvalidValue) {
			t.Errorf("got %v, want ErrInvalidValue", got.Err)
		}
	})
}

// An event of an archived stream whose archive record could be over Firehose's record
// limit fails alone, and isn't stored: no projector could archive it.
func TestAppendRejectsEventOverArchiveRecordLimit(t *testing.T) {
	t.Parallel()
	const rejected = "dynaflux: append to s: invalid value: the event's archive record can reach 1024001 bytes, over Firehose's record limit of 1024000"
	tests := []struct {
		name    string
		archive string // dynaflux.yaml's archive section
		size    int    // the record's size
		wantErr string
	}{
		{"at the limit", "", 1_024_000, ""},
		{"one byte over", "", 1_024_001, rejected},
		{"one byte over, not archived", "archive:\n  enabled: false\n", 1_024_001, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				got, stored := appendArchiveRecord(t, tt.archive, tt.size)
				if tt.wantErr == "" {
					if got.Err != nil || stored != 1 {
						t.Errorf("got %v and %d events stored, want success", got.Err, stored)
					}
					return
				}
				var ae *engine.ArchiveSizeError
				if !errors.As(got.Err, &ae) || *ae != (engine.ArchiveSizeError{Stream: "s", Size: tt.size}) ||
					!errors.Is(got.Err, engine.ErrInvalidValue) || got.Err.Error() != tt.wantErr || stored != 0 {
					t.Errorf("got %v and %d events stored, want an *ArchiveSizeError for s of %d bytes and none stored", got.Err, stored, tt.size)
				}
			})
		})
	}
}

// appendArchiveRecord appends to stream s, in a store whose dynaflux.yaml ends with
// archive, an event whose archive record has size bytes, and returns the result and how
// many events s holds after.
func appendArchiveRecord(t *testing.T, archive string, size int) (engine.Appended, int) {
	t.Helper()
	c, err := engine.Compile(catalogtest.MarshalFS(t, fstest.MapFS{
		"dynaflux.yaml":      {Data: []byte("store: app\nregions: [us-east-1]\n" + archive)},
		"dynaflux/store.sql": {Data: []byte("CREATE STREAM s (k String, v String) PRIMARY KEY (k);")},
	}))
	if err != nil {
		t.Fatal(err)
	}
	// The record without v's text, with the bubble's 15-digit times and the longest unit,
	// sequence number and arrival time: {"event_id":"<26>","event_time":<15>,
	// "ingest_time":<15>,"origin_region":"us-east-1","k":"u1","v":"","_shard":"<65>",
	// "_seq":"<40>","_arrival":<16>}
	const overhead = 1 + 39 + 29 + 30 + 28 + 9 + 7 + 77 + 50 + 28 + 1
	// \x01 takes 6 bytes in JSON, so the event item stays far below 400 KB.
	n := size - overhead
	v := strings.Repeat("\x01", n/6) + strings.Repeat("x", n%6)
	e := newEngine(mem.New())
	got := appendOne(t, e, c, "s", []expr.Value{expr.String("u1"), expr.String(v), micros(start)}, engine.AppendMode{Import: true})
	return got, len(readAll(t, e, c, "s", "u1", nil, nil))
}

func TestReadMergesShardsInEventOrder(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, streams)
		e := newEngine(mem.New())
		var events [][]expr.Value
		for _, minute := range []int{7, 3, 9, 3, 0, 5, 1, 8, 3, 2} {
			at := micros(start.Add(time.Duration(minute-10) * time.Minute))
			events = append(events, []expr.Value{expr.String("u1"), expr.String(string(rune('0' + minute))), at})
		}
		if _, err := e.Append(t.Context(), c, "clicks", events, engine.AppendMode{}); err != nil {
			t.Fatal(err)
		}
		rows := readAll(t, e, c, "clicks", "u1", nil, nil)
		// The three events at minute 3 share an instant and keep their append order.
		want := []string{"0", "1", "2", "3", "3", "3", "5", "7", "8", "9"}
		if got := column(rows, 1); !slices.Equal(got, want) {
			t.Errorf("read %q, want %q", got, want)
		}
		ids := column(rows, 2)
		if !slices.IsSorted(ids[3:6]) {
			t.Errorf("event_ids at one instant are %q, want them in append order", ids[3:6])
		}
	})
}

func TestReadSinceInclusiveUntilExclusive(t *testing.T) {
	t.Parallel()
	us := func(m int) *int64 { return new(start.Add(time.Duration(m) * time.Minute).UnixMicro()) }
	tests := []struct {
		name     string
		from, to *int64
		want     []string
	}{
		{"open", nil, nil, []string{"-3", "-2", "-1"}},
		{"since", us(-2), nil, []string{"-2", "-1"}},
		{"until", nil, us(-2), []string{"-3"}},
		{"window", us(-2), us(-1), []string{"-2"}},
		{"empty window", us(-2), us(-2), nil},
		{"crossed bounds", us(-1), us(-2), nil},
		{"before 1970", nil, new(int64(-1)), nil},
		{"open past 2106", us(-2), new(int64(1 << 62)), []string{"-2", "-1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := compile(t, streams)
				e := newEngine(mem.New())
				for m := -3; m < 0; m++ {
					ev := pageView(strconv.Itoa(m), micros(start.Add(time.Duration(m)*time.Minute)))
					if got := appendOne(t, e, c, "page_views", ev, engine.AppendMode{}); got.Err != nil {
						t.Fatal(got.Err)
					}
				}
				if got := column(readAll(t, e, c, "page_views", "u1", tt.from, tt.to), 1); !slices.Equal(got, tt.want) {
					t.Errorf("read %q, want %q", got, tt.want)
				}
			})
		})
	}
}

func TestReadBreakStopsEveryShard(t *testing.T) {
	t.Parallel()
	// synctest.Test fails when a goroutine of the bubble outlives the test function.
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, streams)
		e := newEngine(mem.New())
		var events [][]expr.Value
		for i := range 100 {
			events = append(events, []expr.Value{expr.String("u1"), expr.String("x"), micros(start.Add(time.Duration(-i) * time.Second))})
		}
		if _, err := e.Append(t.Context(), c, "clicks", events, engine.AppendMode{}); err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, err := range e.Read(t.Context(), c, "clicks", []expr.Value{expr.String("u1")}, nil, nil) {
			if err != nil {
				t.Fatal(err)
			}
			if n++; n == 3 {
				break
			}
		}
	})
}

func TestReadHidesExpiredEvents(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, streams)
		e := newEngine(mem.New())
		time.Sleep(500 * time.Millisecond)
		if got := appendOne(t, e, c, "page_views", pageView("/", expr.Null()), engine.AppendMode{}); got.Err != nil {
			t.Fatal(got.Err)
		}
		// LOG RETENTION 2 DAY ends at 48:00:00.5, which _ttl rounds up to 48:00:01.
		time.Sleep(48*time.Hour + 500*time.Millisecond - time.Microsecond)
		if n := len(readAll(t, e, c, "page_views", "u1", nil, nil)); n != 1 {
			t.Fatalf("read %d events a microsecond before the rounded-up expiry, want 1", n)
		}
		time.Sleep(time.Microsecond)
		if n := len(readAll(t, e, c, "page_views", "u1", nil, nil)); n != 0 {
			t.Errorf("read %d events at the rounded-up expiry, want 0", n)
		}
	})
}

func TestReadExpiryIsFixedAtAppend(t *testing.T) {
	t.Parallel()
	const day = 24 * time.Hour
	tests := []struct {
		name          string
		before, after string // LOG RETENTION when appending and when reading
		later         time.Duration
		want          int
	}{
		{"shortened", "30 DAY", "1 DAY", 2 * day, 1},
		{"no longer FOREVER", "FOREVER", "1 DAY", 2 * day, 1},
		{"lengthened", "1 DAY", "30 DAY", day, 0},
		{"now FOREVER", "1 DAY", "FOREVER", day, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				stream := func(retention string) *engine.Catalog {
					return compile(t, "CREATE STREAM s (k String) PRIMARY KEY (k) LOG RETENTION "+retention+";")
				}
				b := mem.New()
				ev := []expr.Value{expr.String("k1"), expr.Null()}
				if got := appendOne(t, newEngine(b), stream(tt.before), "s", ev, engine.AppendMode{}); got.Err != nil {
					t.Fatal(got.Err)
				}
				time.Sleep(tt.later)
				if n := len(readAll(t, newEngine(b), stream(tt.after), "s", "k1", nil, nil)); n != tt.want {
					t.Errorf("read %d events, want %d", n, tt.want)
				}
			})
		})
	}
}

func TestReadFillsColumnsAddedLater(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		before := compile(t, "CREATE STREAM s (k String) PRIMARY KEY (k);")
		after := compile(t, `CREATE STREAM s (k String, n Nullable(UInt8), c String DEFAULT 'US', a Array(UInt8) DEFAULT [1, 2])
			PRIMARY KEY (k);`)
		b := mem.New()
		if got := appendOne(t, newEngine(b), before, "s", []expr.Value{expr.String("k1"), expr.Null()}, engine.AppendMode{}); got.Err != nil {
			t.Fatal(got.Err)
		}
		rows := readAll(t, newEngine(b), after, "s", "k1", nil, nil)
		if len(rows) != 1 {
			t.Fatalf("read %d events, want 1", len(rows))
		}
		r := rows[0]
		if !r[1].IsNull() || r[2].AsString() != "US" || len(r[3].AsArray()) != 2 || r[3].AsArray()[1].AsUint() != 2 {
			t.Errorf("read n, c, a = %v, %v, %v; want NULL, US, [1, 2]", r[1], r[2], r[3])
		}
	})
}

func TestReadReturnsImplicitColumns(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, streams)
		e := newEngine(mem.New())
		at := start.Add(-time.Hour)
		if got := appendOne(t, e, c, "api_calls", apiCall(1, 5, at), engine.AppendMode{}); got.Err != nil {
			t.Fatal(got.Err)
		}
		r := readAll(t, e, c, "api_calls", "acme", nil, nil)[0]
		// account_id, request_id, units, price, event_id, event_time, ingest_time, origin_region
		if r[3].AsDecimal().Cmp(big.NewInt(1250)) != 0 || len(r[4].AsString()) != 26 ||
			r[5].AsMicros() != at.UnixMicro() || r[6].AsMicros() != start.UnixMicro() || r[7].AsString() != "us-east-1" {
			t.Errorf("read price %v, event_id %q, event_time %d, ingest_time %d, origin_region %q",
				r[3].AsDecimal(), r[4].AsString(), r[5].AsMicros(), r[6].AsMicros(), r[7].AsString())
		}
	})
}

func TestReadWithoutRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		stream string
		keyLen int
	}{
		{"key over DynamoDB's 2048 bytes", "page_views", 2049},
		{"key over 2048 bytes with the shard suffix #s00", "clicks", 2045},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// The in-memory store rejects an oversized key, so a request would fail the read.
			synctest.Test(t, func(t *testing.T) {
				c := compile(t, streams)
				e := newEngine(mem.New())
				long := strings.Repeat("u", tt.keyLen)
				if rows := readAll(t, e, c, tt.stream, long, nil, nil); len(rows) != 0 {
					t.Errorf("read %d events for a key no event can have, want 0", len(rows))
				}
			})
		})
	}
}

func TestReadRejectsAnInvalidKey(t *testing.T) {
	t.Parallel()
	c := compile(t, streams)
	var got []string
	for _, err := range newEngine(mem.New()).Read(t.Context(), c, "page_views", []expr.Value{expr.String("\xff")}, nil, nil) {
		got = append(got, fmt.Sprint(err))
	}
	if want := []string{"dynaflux: user_id: invalid value: not valid UTF-8"}; !slices.Equal(got, want) {
		t.Errorf("read %q, want %q", got, want)
	}
}

// queryLog records the queries sent to a backend.
type queryLog struct {
	store.Backend

	mu      sync.Mutex
	queries []store.Query
}

func (l *queryLog) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	l.mu.Lock()
	l.queries = append(l.queries, q)
	l.mu.Unlock()
	return l.Backend.Query(ctx, table, q)
}

func TestReadAsksEveryShardForASmallFirstPage(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, streams)
		b := &queryLog{Backend: mem.New()}
		e := newEngine(b)
		if got := appendOne(t, e, c, "clicks", []expr.Value{expr.String("u1"), expr.String("x"), expr.Null()}, engine.AppendMode{}); got.Err != nil {
			t.Fatal(got.Err)
		}
		readAll(t, e, c, "clicks", "u1", nil, nil)
		var sizes []int
		for _, q := range b.queries {
			sizes = append(sizes, q.PageSize)
		}
		if want := slices.Repeat([]int{32}, 8); !slices.Equal(sizes, want) {
			t.Errorf("queried with page sizes %v, want %v", sizes, want)
		}
	})
}

func TestAppendNamesTheColumnsOfAKeyTooLong(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, "CREATE STREAM s (org_id String, user_id String) PRIMARY KEY (org_id, user_id);")
		long := expr.String(strings.Repeat("u", 1100))
		got := appendOne(t, newEngine(mem.New()), c, "s", []expr.Value{long, long, expr.Null()}, engine.AppendMode{})
		var ve *engine.ValueError
		if !errors.As(got.Err, &ve) || ve.Name != "org_id, user_id" {
			t.Errorf("got %v, want a *ValueError for org_id, user_id", got.Err)
		}
	})
}

func TestReadStopsAtUndecodableEvent(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, streams)
		b := mem.New()
		e := newEngine(b)
		res, err := e.Append(t.Context(), c, "page_views", [][]expr.Value{
			pageView("/a", micros(start.Add(-2*time.Minute))),
			pageView("/b", micros(start.Add(-time.Minute))),
		}, engine.AppendMode{})
		if err != nil {
			t.Fatal(err)
		}
		// Another writer stores duration_ms as a string in the first event.
		const table = "app-stream-page_views"
		it, err := b.Get(t.Context(), table, res[0].Key, true)
		if err != nil {
			t.Fatal(err)
		}
		it["duration_ms"] = store.Value{Type: store.S, Str: "100"}
		if err = b.Put(t.Context(), table, it, store.Cond{}); err != nil {
			t.Fatal(err)
		}
		var got []string
		for row, rerr := range e.Read(t.Context(), c, "page_views", []expr.Value{expr.String("u1")}, nil, nil) {
			if rerr != nil {
				got = append(got, "error")
				continue
			}
			got = append(got, row[1].AsString())
		}
		if !slices.Equal(got, []string{"error"}) {
			t.Errorf("read %q, want one error and nothing after it", got)
		}
	})
}

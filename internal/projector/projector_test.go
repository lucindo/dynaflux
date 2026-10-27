// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math/big"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/store/storetest"
)

// views is the test store: one stream and views that each exercise a part of the
// projector.
const views = `
CREATE STREAM clicks (
    user_id String,
    page    String,
    ms      UInt32,
    price   Decimal(10, 2),
    tags    Array(String)
)
PRIMARY KEY (user_id)
ACCEPT event_time BETWEEN now() - INTERVAL 30 DAY AND now() + INTERVAL 5 MINUTE;

CREATE MATERIALIZED VIEW page_totals
ORDER BY (page)
AS SELECT page, count() AS views, sum(ms) AS total_ms, uniqExact(user_id) AS users,
    max(ms) AS longest, min(ms) AS shortest, sum(price) AS revenue
FROM clicks
GROUP BY page;

-- Idempotent only, and reads no ms.
CREATE MATERIALIZED VIEW page_users
ORDER BY (page)
AS SELECT page, uniqExact(user_id) AS users
FROM clicks
GROUP BY page;

CREATE MATERIALIZED VIEW page_rates
ORDER BY (page)
AS SELECT page, sum(intDiv(1000, ms)) AS rate
FROM clicks
GROUP BY page;

CREATE MATERIALIZED VIEW page_keys
ORDER BY (page)
AS SELECT page
FROM clicks
GROUP BY page;

CREATE MATERIALIZED VIEW slow_pages
ORDER BY (page)
AS SELECT page, countIf(ms > 1000000) AS slow
FROM clicks
GROUP BY page;

CREATE MATERIALIZED VIEW user_tags
ORDER BY (user_id)
AS SELECT user_id, uniqExact(tag) AS distinct_tags, count() AS n
FROM clicks
ARRAY JOIN tags AS tag
GROUP BY user_id;

CREATE STREAM readings (
    sensor String,
    label  String,
    x      Float64,
    tags   Array(String)
)
PRIMARY KEY (sensor);

CREATE MATERIALIZED VIEW sensor_stats
ORDER BY (sensor)
AS SELECT sensor,
    sumIf(x, x < 1)         AS small_total,
    avg(x)                  AS mean,
    avgIf(x, x > 1)         AS big_mean,
    argMax(label, x)        AS top,
    argMin(label, x)        AS bottom,
    any(label)              AS first,
    minIf(x, label = 'z')   AS z_min,
    uniq(label)             AS labels,
    quantile(0.5)(x)        AS median,
    quantileDD(0.01, 1)(x)  AS largest
FROM readings
GROUP BY sensor;

-- argMax's val is the same on every row, so the tie order alone picks the tag.
CREATE MATERIALIZED VIEW sensor_tags
ORDER BY (sensor)
AS SELECT sensor, any(tag) AS first_tag, argMax(tag, sensor) AS last_tag, max(tag) AS max_tag
FROM readings
ARRAY JOIN tags AS tag
GROUP BY sensor;

-- A label over 4 KB fails label_max, not label_skips: its -If condition and NULL val skip
-- the row before the cap applies.
CREATE MATERIALIZED VIEW label_max
ORDER BY (sensor)
AS SELECT sensor, max(label) AS top
FROM readings
GROUP BY sensor;

CREATE MATERIALIZED VIEW label_skips
ORDER BY (sensor)
AS SELECT sensor, maxIf(label, x > 100) AS top_big, argMax(label, if(x > 100, x, NULL)) AS at_big
FROM readings
GROUP BY sensor;

CREATE MATERIALIZED VIEW page_days
ORDER BY (page, day)
PRIMARY KEY (page)
TTL day + INTERVAL 1 DAY
SPLIT 2
AS SELECT page, toStartOfDay(event_time) AS day, count() AS views, uniqExact(user_id) AS users
FROM clicks
GROUP BY page, day;
`

const (
	clicksTable     = "app-stream-clicks"
	pageTotalsTable = "app-view-page_totals-v1"
	unit            = "shardId-00000001"
)

// start is where the fake clock of a synctest bubble starts.
var start = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) //nolint:gochecknoglobals // a constant time.Time

type fixture struct {
	cat     *engine.Catalog
	mem     *mem.Mem
	e       *engine.Engine
	meta    *meta.Meta
	archive *archiveLog
	core    *projector.Core
}

// newFixture returns the test store with every view version live, and a core that writes
// through wrap.
func newFixture(t *testing.T, wrap func(store.Backend) store.Backend) *fixture {
	t.Helper()
	f := newPendingFixture(t, wrap)
	if err := f.meta.SetLive(t.Context(), f.cat.Catalog()); err != nil {
		t.Fatal(err)
	}
	return f
}

// newPendingFixture is newFixture with every view version pending its backfill.
func newPendingFixture(t *testing.T, wrap func(store.Backend) store.Backend) *fixture {
	t.Helper()
	cat, err := engine.Compile(catalogtest.Marshal(t, "app", views))
	if err != nil {
		t.Fatal(err)
	}
	m := mem.New()
	naming := catalog.Naming{Store: "app"}
	if wrap == nil {
		wrap = func(b store.Backend) store.Backend { return b }
	}
	a := &archiveLog{}
	return &fixture{
		cat:     cat,
		mem:     m,
		e:       engine.New(m, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})}),
		meta:    meta.New(m, naming.MetaTable()),
		archive: a,
		core:    projector.New(cat.Projector(), wrap(m), a, projector.Config{Naming: naming}),
	}
}

// click appends a click that costs ms cents and returns its stored item.
func (f *fixture) click(t *testing.T, user, page string, ms uint64, tags ...string) store.Item {
	t.Helper()
	tagVals := make([]expr.Value, len(tags))
	for i, tag := range tags {
		tagVals[i] = expr.String(tag)
	}
	price := expr.Decimal(new(big.Int).SetUint64(ms))
	ev := []expr.Value{expr.String(user), expr.String(page), expr.Uint(ms), price, expr.Array(tagVals), expr.Null()}
	res, err := f.e.Append(t.Context(), f.cat, "clicks", [][]expr.Value{ev}, engine.AppendMode{})
	if err == nil {
		err = res[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	it, err := f.mem.Get(t.Context(), clicksTable, res[0].Key, true)
	if err != nil {
		t.Fatal(err)
	}
	return it
}

// seq returns the n-th sequence number as delivered: DynamoDB's have at least 21 digits.
func seq(n int) string { return fmt.Sprintf("1%020d", n) }

func insert(n int, it store.Item) projector.Record {
	return projector.Record{Op: projector.Insert, Seq: seq(n), New: it}
}

func (f *fixture) handle(t *testing.T, recs ...projector.Record) projector.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	res, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: recs})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// row returns a logical row of a version as text, "" when it doesn't exist: the keys, then
// the aggregates in name order, the catalog's. Every Decimal of the test views has scale 2.
func (f *fixture) row(t *testing.T, version string, key ...expr.Value) string {
	t.Helper()
	row, ok, err := f.e.ReadRow(t.Context(), f.cat, version, key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return ""
	}
	parts := make([]string, len(row))
	for i, v := range row {
		switch v.Kind() { //nolint:exhaustive // the kinds of the test views' columns
		case expr.KindString:
			parts[i] = v.AsString()
		case expr.KindUint:
			parts[i] = strconv.FormatUint(v.AsUint(), 10)
		case expr.KindMicros:
			parts[i] = time.UnixMicro(v.AsMicros()).UTC().Format(time.DateOnly)
		case expr.KindDecimal:
			parts[i] = new(big.Rat).SetFrac(v.AsDecimal(), big.NewInt(100)).FloatString(2)
		case expr.KindFloat:
			parts[i] = strconv.FormatFloat(v.AsFloat(), 'g', -1, 64)
		case expr.KindNull:
			parts[i] = "NULL"
		default:
			parts[i] = fmt.Sprintf("kind %d", v.Kind())
		}
	}
	return strings.Join(parts, " ")
}

func page(p string) expr.Value { return expr.String(p) }

// TestHandleWithoutInsertSendsNoRequest: TTL deletes reach the projector as REMOVE records,
// and a batch of them, or of MODIFY records, costs no store call.
func TestHandleWithoutInsertSendsNoRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		recs      func(a, b store.Item) []projector.Record
		divergent int
	}{
		{"no record", func(_, _ store.Item) []projector.Record { return nil }, 0},
		{"REMOVE records", func(a, b store.Item) []projector.Record {
			return []projector.Record{{Op: projector.Remove, Seq: seq(1), Old: a}, {Op: projector.Remove, Seq: seq(2), Old: b}}
		}, 0},
		{"a MODIFY with another payload and a REMOVE", func(a, b store.Item) []projector.Record {
			return []projector.Record{{Op: projector.Modify, Seq: seq(1), Old: a, New: b}, {Op: projector.Remove, Seq: seq(2), Old: b}}
		}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				calls := &callCount{}
				f := newFixture(t, func(b store.Backend) store.Backend { calls.Backend = b; return calls })
				res := f.handle(t, tt.recs(f.click(t, "u1", "/a", 100), f.click(t, "u1", "/a", 200))...)
				if n, archived := calls.n.Load(), f.archive.seqs(); n != 0 || len(archived) > 0 || res.Stats.Divergent != tt.divergent {
					t.Errorf("%d store calls, archived %v, %d divergent; want 0, none, %d", n, archived, res.Stats.Divergent, tt.divergent)
				}
			})
		})
	}
}

func TestHandleAppliesEachRecordOnce(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		a1, a2, a3 := f.click(t, "u1", "/a", 100), f.click(t, "u2", "/a", 300), f.click(t, "u1", "/a", 200)
		// Out of order, with a repeat: the sequence numbers decide.
		f.handle(t, insert(2, a2), insert(1, a1), insert(1, a1))
		// A redelivery that overlaps what the rows hold.
		f.handle(t, insert(1, a1), insert(2, a2), insert(3, a3))
		if got, want := f.row(t, "page_totals@v1", page("/a")), "/a 300 6.00 100 600 2 3"; got != want {
			t.Errorf("page_totals /a is %q, want %q", got, want)
		}
	})
}

func TestHandleRetriesFromLowestFailedSequence(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		fail := &failOnce{table: pageTotalsTable, pk: "/b"}
		f := newFixture(t, func(b store.Backend) store.Backend { fail.Backend = b; return fail })
		a, b1, b2 := f.click(t, "u1", "/a", 100), f.click(t, "u1", "/b", 200), f.click(t, "u2", "/b", 300)
		res := f.handle(t, insert(3, b2), insert(1, a), insert(2, b1))
		if res.FailedSeq != seq(2) || !errors.Is(res.RowErr, errTransient) {
			t.Fatalf("FailedSeq %q (%v), want %q from the failed row", res.FailedSeq, res.RowErr, seq(2))
		}
		// Lambda delivers again from the failed record. The markers written by the failed
		// attempt are the records' own, so their users still count.
		f.handle(t, insert(2, b1), insert(3, b2))
		got := []string{f.row(t, "page_totals@v1", page("/a")), f.row(t, "page_totals@v1", page("/b"))}
		if want := []string{"/a 100 1.00 100 100 1 1", "/b 300 5.00 200 500 2 2"}; !slices.Equal(got, want) {
			t.Errorf("page_totals rows are %q, want %q", got, want)
		}
	})
}

func TestHandleOnlyAppliesInserts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		rec       func(old, other store.Item) projector.Record
		divergent int
	}{
		{"REMOVE of an expired event", func(old, _ store.Item) projector.Record {
			return projector.Record{Op: projector.Remove, Seq: seq(1), Old: old}
		}, 0},
		{"MODIFY with the same payload in another number form", func(old, _ store.Item) projector.Record {
			same := maps.Clone(old)
			same["ms"] = store.Value{Type: store.N, Str: "1E2"}
			same[catalog.AttrOriginRegion] = store.Value{Type: store.S, Str: "us-west-2"}
			return projector.Record{Op: projector.Modify, Seq: seq(1), Old: old, New: same}
		}, 0},
		{"MODIFY with another payload", func(old, other store.Item) projector.Record {
			return projector.Record{Op: projector.Modify, Seq: seq(1), Old: old, New: other}
		}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, nil)
				old, other := f.click(t, "u1", "/a", 100), f.click(t, "u1", "/a", 200)
				res := f.handle(t, tt.rec(old, other))
				if res.Stats.Divergent != tt.divergent {
					t.Errorf("Divergent %d, want %d", res.Stats.Divergent, tt.divergent)
				}
				if got := f.row(t, "page_totals@v1", page("/a")); got != "" {
					t.Errorf("page_totals /a is %q, want no row", got)
				}
			})
		})
	}
}

func TestHandleIgnoresUnknownAttribute(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		// An app with a newer catalog stored a column this catalog doesn't declare.
		it := f.click(t, "u1", "/a", 100)
		it["referrer"] = store.Value{Type: store.N, Str: "7"}
		res := f.handle(t, insert(1, it))
		if got, want := f.row(t, "page_totals@v1", page("/a")), "/a 100 1.00 100 100 1 1"; got != want || len(res.Stats.Broken) > 0 {
			t.Errorf("page_totals /a is %q with broken versions %v, want %q", got, res.Stats.Broken, want)
		}
	})
}

func TestHandleBreaksOnlyFailingVersions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		poison func(it store.Item)
		broken []string
	}{
		{"malformed column", func(it store.Item) {
			it["ms"] = store.Value{Type: store.S, Str: "100"}
		}, []string{"page_rates@v1", "page_totals@v1", "slow_pages@v1"}},
		// The decode error quotes the value, which escapes to 600 KB: over the item limit of
		// the view state that records the failure.
		{"malformed column too long to quote", func(it store.Item) {
			it["ms"] = store.Value{Type: store.N, Str: strings.Repeat("\x01", 150_000)}
		}, []string{"page_rates@v1", "page_totals@v1", "slow_pages@v1"}},
		{"evaluation error", func(it store.Item) {
			it["ms"] = store.Value{Type: store.N, Str: "0"}
		}, []string{"page_rates@v1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, nil)
				good, bad := f.click(t, "u1", "/a", 100), f.click(t, "u2", "/a", 100)
				tt.poison(bad)
				res := f.handle(t, insert(1, good), insert(2, bad))
				if got := f.failedAt(t); !slices.Equal(res.Stats.Broken, tt.broken) || !slices.Equal(got, tt.broken) {
					t.Errorf("broken %v, failed at record 2: %v; want %v", res.Stats.Broken, got, tt.broken)
				}
				// A breaker drops the whole batch for its version; the others take both records.
				if got := f.row(t, "page_users@v1", page("/a")); got != "/a 2" {
					t.Errorf("page_users /a is %q, want %q", got, "/a 2")
				}
				if got := f.row(t, "page_rates@v1", page("/a")); got != "" {
					t.Errorf("page_rates /a is %q, want no row", got)
				}
			})
		})
	}
}

func TestHandleFailsTheBatch(t *testing.T) {
	t.Parallel()
	rejected := fmt.Errorf("too large: %w", store.ErrInvalidRequest)
	oneClick := func(click store.Item) projector.Batch {
		return projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{insert(1, click)}}
	}
	aMinute := func(ctx context.Context) (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, time.Minute)
	}
	tests := []struct {
		name  string
		wrap  func(store.Backend) store.Backend
		batch func(click store.Item) projector.Batch
		ctx   func(context.Context) (context.Context, context.CancelFunc)
		want  error // nil for any error
	}{
		{"a row DynamoDB rejects", failPuts(pageTotalsTable, "", rejected), oneClick, aMinute, store.ErrInvalidRequest},
		{"a breaker that can't be written", failPuts(catalog.Naming{Store: "app"}.MetaTable(), "view", errTransient), oneClick, aMinute, errTransient},
		{"no deadline", nil, oneClick, context.WithCancel, nil},
		{"no delivery unit", nil, func(click store.Item) projector.Batch {
			return projector.Batch{Stream: "clicks", Records: []projector.Record{insert(1, click)}}
		}, aMinute, nil},
		{"an unknown op", nil, func(click store.Item) projector.Batch {
			rec := projector.Record{Op: 9, Seq: seq(1), New: click}
			return projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{rec}}
		}, aMinute, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, tt.wrap)
			ctx, cancel := tt.ctx(t.Context())
			defer cancel()
			// page_rates divides by ms, so a click of 0 ms opens its breaker.
			res, err := f.core.Handle(ctx, tt.batch(f.click(t, "u1", "/a", 0)))
			if err == nil || tt.want != nil && !errors.Is(err, tt.want) || res.FailedSeq != "" {
				t.Errorf("Handle = %+v, %v; want only an error matching %v", res, err, tt.want)
			}
		})
	}
}

func TestHandleReportsOnlyBreakersItOpens(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var f *fixture
		var markErr error
		first := meta.Failure{Unit: "shardId-2", Seq: padSeq(t, seq(9)), EventID: "e", Error: "first"}
		// Another unit's batch opens the breaker after this batch read the view states.
		f = newFixture(t, func(b store.Backend) store.Backend {
			return &afterViewStates{Backend: b, do: func(ctx context.Context) {
				_, markErr = f.meta.MarkBroken(ctx, "page_rates@v1", first)
			}}
		})
		res := f.handle(t, insert(1, f.click(t, "u1", "/a", 0))) // page_rates divides by ms
		if markErr != nil {
			t.Fatal(markErr)
		}
		states, err := f.meta.ViewStates(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if got := states["page_rates@v1"].Failure; len(res.Stats.Broken) > 0 || got == nil || got.Error != "first" {
			t.Errorf("Broken %v with failure %+v, want none reported and the first failure kept", res.Stats.Broken, got)
		}
	})
}

// TestHandleSkipsVersionItFoundBroken: once a batch finds a version broken, whether it opened
// the breaker or another unit's batch had, the core's later batches skip the version before
// the view states it read expire.
func TestHandleSkipsVersionItFoundBroken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		otherOpener bool // another unit opens the breaker after the batch read the view states
	}{
		{"opened by the batch", false},
		{"opened by another unit", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var f *fixture
				var wrap func(store.Backend) store.Backend
				if tt.otherOpener {
					wrap = openBreakerAfterViewStates(t, &f)
				}
				f = newFixture(t, wrap)
				res := f.handle(t, insert(1, f.click(t, "u1", "/a", 0))) // page_rates divides by ms
				if opened := slices.Equal(res.Stats.Broken, []string{"page_rates@v1"}); opened == tt.otherOpener {
					t.Fatalf("Broken %v, want page_rates@v1 reported: %v", res.Stats.Broken, !tt.otherOpener)
				}
				f.handle(t, insert(2, f.click(t, "u2", "/b", 100)))
				if got := f.row(t, "page_rates@v1", page("/b")); got != "" {
					t.Errorf("page_rates /b is %q, want no row: the version is broken", got)
				}
				if got, want := f.row(t, "page_users@v1", page("/b")), "/b 1"; got != want {
					t.Errorf("page_users /b is %q, want %q", got, want)
				}
			})
		})
	}
}

// openBreakerAfterViewStates returns a wrap whose backend opens page_rates' breaker for
// another unit once the core has read the view states.
func openBreakerAfterViewStates(t *testing.T, f **fixture) func(store.Backend) store.Backend {
	t.Helper()
	other := meta.Failure{Unit: "shardId-2", Seq: padSeq(t, seq(9)), EventID: "e", Error: "other"}
	return func(b store.Backend) store.Backend {
		return &afterViewStates{Backend: b, do: func(ctx context.Context) {
			if _, err := (*f).meta.MarkBroken(ctx, "page_rates@v1", other); err != nil {
				t.Error(err)
			}
		}}
	}
}

// TestHandleReportsStatsOfAFailedBatch: a breaker opened by a batch that then fails is
// reported by that batch, since its retry finds the version broken already.
func TestHandleReportsStatsOfAFailedBatch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, failPuts(pageTotalsTable, "", fmt.Errorf("rejected: %w", store.ErrInvalidRequest)))
		a, b := f.click(t, "u1", "/a", 0), f.click(t, "u1", "/a", 100) // page_rates divides by ms
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		res, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{
			insert(1, a), {Op: projector.Modify, Seq: seq(2), Old: a, New: b},
		}})
		want := projector.Stats{Divergent: 1, Broken: []string{"page_rates@v1"}}
		if !errors.Is(err, store.ErrInvalidRequest) || res.FailedSeq != "" || res.RowErr != nil || !reflect.DeepEqual(res.Stats, want) {
			t.Errorf("Handle = %+v, %v; want only Stats %+v, and ErrInvalidRequest", res, err, want)
		}
	})
}

// TestHandleCountsRowsTooLarge: a row whose item would be over DynamoDB's limit fails the
// row, not the batch, and is counted.
func TestHandleCountsRowsTooLarge(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, failPuts(pageTotalsTable, "/b", fmt.Errorf("item is 400,001 bytes: %w", store.ErrTooLarge)))
		res := f.handle(t, insert(1, f.click(t, "u1", "/a", 100)), insert(2, f.click(t, "u1", "/b", 100)), insert(3, f.click(t, "u2", "/b", 100)))
		if res.FailedSeq != seq(2) || res.Stats.TooLarge != 1 || len(res.Stats.Broken) > 0 {
			t.Errorf("FailedSeq %q with %+v, want %q, 1 row too large and no breaker", res.FailedSeq, res.Stats, seq(2))
		}
		if got, want := f.row(t, pageTotals, page("/a")), "/a 100 1.00 100 100 1 1"; got != want {
			t.Errorf("page_totals /a is %q, want %q", got, want)
		}
	})
}

func padSeq(t *testing.T, delivered string) string {
	t.Helper()
	s, err := keys.PadSeq(delivered)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// failedAt returns the broken versions whose failure is the record with sequence number
// seq(2).
func (f *fixture) failedAt(t *testing.T) []string {
	t.Helper()
	states, err := f.meta.ViewStates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for v, st := range states {
		if st.Failure != nil && st.Failure.Seq == padSeq(t, seq(2)) {
			out = append(out, v)
		}
	}
	slices.Sort(out)
	return out
}

func TestHandleWritesRowsWithoutAggregateInput(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		f.handle(t, insert(1, f.click(t, "u1", "/a", 100)))
		got := []string{f.row(t, "page_keys@v1", page("/a")), f.row(t, "slow_pages@v1", page("/a"))}
		if want := []string{"/a", "/a 0"}; !slices.Equal(got, want) {
			t.Errorf("rows %q, want %q", got, want)
		}
	})
}

func TestHandleCountsValueOncePerRecord(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		f.handle(t,
			insert(1, f.click(t, "u1", "/a", 100, "x", "x", "y")),
			insert(2, f.click(t, "u1", "/a", 100, "y", "z")),
			insert(3, f.click(t, "u2", "/a", 100)), // an empty array makes no row
		)
		got := []string{f.row(t, "user_tags@v1", expr.String("u1")), f.row(t, "user_tags@v1", expr.String("u2"))}
		if want := []string{"u1 3 5", ""}; !slices.Equal(got, want) {
			t.Errorf("user_tags rows %q, want %q", got, want)
		}
	})
}

// reading appends a reading and returns its stored item.
func (f *fixture) reading(t *testing.T, label string, x float64, tags ...string) store.Item {
	t.Helper()
	tagVals := make([]expr.Value, len(tags))
	for i, tag := range tags {
		tagVals[i] = expr.String(tag)
	}
	ev := []expr.Value{expr.String("s1"), expr.String(label), expr.Float(x), expr.Array(tagVals), expr.Null()}
	res, err := f.e.Append(t.Context(), f.cat, "readings", [][]expr.Value{ev}, engine.AppendMode{})
	if err == nil {
		err = res[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	it, err := f.mem.Get(t.Context(), "app-stream-readings", res[0].Key, true)
	if err != nil {
		t.Fatal(err)
	}
	return it
}

func TestHandleAppliesEveryAggregate(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		// The first reading, the smallest and the largest have different labels.
		recs := []projector.Record{
			insert(1, f.reading(t, "e", 0.2)),
			insert(2, f.reading(t, "b", 0.1, "t1", "t2")),
			insert(3, f.reading(t, "c", 0.3, "t3", "t0")),
			insert(4, f.reading(t, "d", 2.5)),
		}
		// One record per batch, so each state goes through its stored form. A Float64 sum in
		// arrival order would read 0.6000000000000001.
		for _, r := range recs {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			res, err := f.core.Handle(ctx, projector.Batch{Stream: "readings", Unit: unit, Records: []projector.Record{r}})
			cancel()
			if err != nil || res.FailedSeq != "" || len(res.Stats.Broken) > 0 {
				t.Fatalf("Handle = %+v, %v", res, err)
			}
		}
		s1 := expr.String("s1")
		// Columns in name order. largest and median are sketch values within 1% of 2.5 and 0.2.
		if got, want := f.row(t, "sensor_stats@v1", s1), "s1 2.5 b e 4 2.5114154408817266 0.775 0.1990713958544654 0.6 d 0"; got != want {
			t.Errorf("sensor_stats s1 = %q, want %q", got, want)
		}
		if got, want := f.row(t, "sensor_tags@v1", s1), "s1 t1 t0 t3"; got != want {
			t.Errorf("sensor_tags s1 = %q, want %q", got, want)
		}
	})
}

// TestHandleCapsStringsAfterIfAndNull checks that the 4 KB cap applies only to values that
// pass the -If condition and the NULL rule (sql.md §7.2).
func TestHandleCapsStringsAfterIfAndNull(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		long := f.reading(t, strings.Repeat("a", 4097), 1)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		res, err := f.core.Handle(ctx, projector.Batch{Stream: "readings", Unit: unit, Records: []projector.Record{insert(1, long)}})
		// sensor_stats reads every label in argMax, argMin and any.
		if want := []string{"label_max@v1", "sensor_stats@v1"}; err != nil || !slices.Equal(res.Stats.Broken, want) {
			t.Fatalf("Handle = %+v, %v; want %v broken", res, err, want)
		}
		// No input reached either aggregate: maxIf reads '' and argMax NULL.
		if got, want := f.row(t, "label_skips@v1", expr.String("s1")), "s1 NULL "; got != want {
			t.Errorf("label_skips s1 = %q, want %q", got, want)
		}
	})
}

func TestHandleAppliesAdditiveVersionOnlyOnceActivating(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		f.handle(t, insert(1, f.click(t, "u1", "/a", 100)))
		got := []string{f.row(t, "page_totals@v1", page("/a")), f.row(t, "page_users@v1", page("/a"))}
		if want := []string{"", "/a 1"}; !slices.Equal(got, want) {
			t.Errorf("rows while pending %q, want %q", got, want)
		}
		if err := f.meta.SetViewState(t.Context(), "page_totals@v1", meta.Activating); err != nil {
			t.Fatal(err)
		}
		time.Sleep(meta.MaxViewStateAge) // the core uses the states it read until then
		f.handle(t, insert(2, f.click(t, "u2", "/a", 200)))
		if got, want := f.row(t, "page_totals@v1", page("/a")), "/a 200 2.00 200 200 1 1"; got != want {
			t.Errorf("page_totals /a once activating is %q, want %q", got, want)
		}
	})
}

func TestHandleSendsNothingAfterDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, func(b store.Backend) store.Backend {
			return slowGets{Backend: b, table: "app-view-page_keys-v1", delay: 2 * time.Minute}
		})
		res := f.handle(t, insert(1, f.click(t, "u1", "/a", 100)))
		if res.FailedSeq != seq(1) {
			t.Errorf("FailedSeq %q, want %q", res.FailedSeq, seq(1))
		}
		if got := f.row(t, "page_keys@v1", page("/a")); got != "" {
			t.Errorf("page_keys /a is %q after the deadline, want no row", got)
		}
	})
}

// TestHandleAdvancesNothingAfterDeadline: the last row write lands, but its response comes
// after the deadline, so the progress write that would follow is never sent.
func TestHandleAdvancesNothingAfterDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, func(b store.Backend) store.Backend {
			return slowPuts{Backend: b, table: "app-view-page_rates-v1", delay: 2 * time.Minute}
		})
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		_, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{insert(1, f.click(t, "u1", "/a", 100))}})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Handle: %v, want a timeout", err)
		}
		u, err := f.meta.Unit(t.Context(), "clicks", unit)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := f.row(t, "page_rates@v1", page("/a")), "/a 10"; got != want || u.Progress != "" {
			t.Errorf("page_rates /a is %q and progress %q, want %q and none", got, u.Progress, want)
		}
	})
}

func TestHandleRetriedWritesSucceed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		// Every conditional write is applied, and its response lost: the SDK's retry fails
		// its condition on the write itself.
		f := newFixture(t, storetest.RetriedWrite)
		res := f.handle(t, insert(1, f.click(t, "u1", "/a", 100)), insert(2, f.click(t, "u2", "/a", 200)))
		if got, want := f.row(t, "page_totals@v1", page("/a")), "/a 200 3.00 100 300 2 2"; got != want || res.FailedSeq != "" {
			t.Errorf("page_totals /a is %q with FailedSeq %q, want %q", got, res.FailedSeq, want)
		}
	})
}

func TestHandleSkipsExpiredRow(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		old := f.click(t, "u1", "/a", 100)
		time.Sleep(48 * time.Hour) // page_days rows expire a day after their day
		f.handle(t, insert(1, old), insert(2, f.click(t, "u2", "/a", 100)))
		day := func(t time.Time) expr.Value { return expr.Micros(t.UnixMicro()) }
		got := []string{f.row(t, "page_days@v1", page("/a"), day(start)), f.row(t, "page_days@v1", page("/a"), day(start.Add(48*time.Hour)))}
		if want := []string{"", "/a 2000-01-03 1 1"}; !slices.Equal(got, want) {
			t.Errorf("page_days rows %q, want %q", got, want)
		}
	})
}

func TestRowMergesSplitItems(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		// Two units whose contributions go to different items of a SPLIT 2 row.
		units := []string{unit}
		for i := 0; len(units) < 2; i++ {
			if u := "shardId-" + strconv.Itoa(i); keys.Bucket(u, 2) != keys.Bucket(unit, 2) {
				units = append(units, u)
			}
		}
		for i, u := range units {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			_, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: u, Records: []projector.Record{
				insert(1, f.click(t, "u1", "/a", uint64(100*(i+1)))),
			}})
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
		// u1 counts once: a value's marker belongs to the logical row, not to an item.
		if got, want := f.row(t, "page_days@v1", page("/a"), expr.Micros(start.UnixMicro())), "/a 2000-01-01 1 2"; got != want {
			t.Errorf("page_days row %q, want %q", got, want)
		}
	})
}

// callCount counts the store calls.
type callCount struct {
	store.Backend

	n atomic.Int64
}

func (c *callCount) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	c.n.Add(1)
	return c.Backend.Get(ctx, table, k, consistent)
}

func (c *callCount) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	c.n.Add(1)
	return c.Backend.BatchGet(ctx, table, ks, consistent)
}

func (c *callCount) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	c.n.Add(1)
	return c.Backend.Query(ctx, table, q)
}

func (c *callCount) Put(ctx context.Context, table string, it store.Item, cond store.Cond) error {
	c.n.Add(1)
	return c.Backend.Put(ctx, table, it, cond)
}

var errTransient = errors.New("throttled")

// failOnce fails the first write to one partition of one table with a transient error.
type failOnce struct {
	store.Backend

	table, pk string
	mu        sync.Mutex
	failed    bool
}

func (f *failOnce) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	f.mu.Lock()
	hit := !f.failed && table == f.table && it[catalog.AttrPK].Str == f.pk
	f.failed = f.failed || hit
	f.mu.Unlock()
	if hit {
		return errTransient
	}
	return f.Backend.Put(ctx, table, it, c)
}

// failPuts returns a wrapper that fails with err every write to table, or with pk set, every
// write to that partition of table.
func failPuts(table, pk string, err error) func(store.Backend) store.Backend {
	return func(b store.Backend) store.Backend { return failingPuts{Backend: b, table: table, pk: pk, err: err} }
}

type failingPuts struct {
	store.Backend

	table, pk string
	err       error
}

func (f failingPuts) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if table == f.table && (f.pk == "" || it[catalog.AttrPK].Str == f.pk) {
		return f.err
	}
	return f.Backend.Put(ctx, table, it, c)
}

// afterViewStates runs do once, after the first read of the meta table's view states.
type afterViewStates struct {
	store.Backend

	do   func(context.Context)
	once sync.Once
}

func (a *afterViewStates) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	return func(yield func(store.Item, error) bool) {
		for it, err := range a.Backend.Query(ctx, table, q) {
			if !yield(it, err) {
				return
			}
		}
		if table == (catalog.Naming{Store: "app"}).MetaTable() {
			a.once.Do(func() { a.do(ctx) })
		}
	}
}

// slowGets answers each Get of table only after delay.
type slowGets struct {
	store.Backend

	table string
	delay time.Duration
}

func (s slowGets) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	it, err := s.Backend.Get(ctx, table, k, consistent)
	if table == s.table {
		time.Sleep(s.delay)
	}
	return it, err
}

// slowPuts answers the writes to one table late.
type slowPuts struct {
	store.Backend

	table string
	delay time.Duration
}

func (s slowPuts) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	err := s.Backend.Put(ctx, table, it, c)
	if table == s.table {
		time.Sleep(s.delay)
	}
	return err
}

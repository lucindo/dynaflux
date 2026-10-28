// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package fluxgen_test

import (
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/internal/access"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/store/storetest"
)

const apiCalls = `
CREATE STREAM api_calls (
    account_id String,
    request_id UUID,
    price      Decimal(10, 2),
    ratio      Float64,
    day        Date,
    done_at    Nullable(DateTime64(3)),
    tags       Array(String)
)
PRIMARY KEY (account_id)
DEDUP BY (request_id);

-- name: Calls :many
SELECT request_id, price FROM api_calls
WHERE account_id = {account:String} AND day = {day:Date};

-- name: LastCall :one
SELECT request_id, price FROM api_calls
WHERE account_id = {account:String} AND day = {day:Date}
ORDER BY event_time DESC
LIMIT 1;

CREATE MATERIALIZED VIEW account_calls
ORDER BY (account_id)
AS SELECT account_id, count() AS calls
FROM api_calls
GROUP BY account_id;

-- name: AccountCalls :one
SELECT calls FROM account_calls
WHERE account_id = {account:String};
`

// apiCall and its fields are what dynaflux generate writes for api_calls.
type apiCall struct {
	AccountID string
	RequestID dynaflux.UUID
	Price     dynaflux.Decimal
	Ratio     float64
	Day       time.Time
	DoneAt    *time.Time
	Tags      []string

	EventTime    time.Time
	EventID      string
	IngestTime   time.Time
	OriginRegion string
}

func (e *apiCall) fields() []any {
	return []any{
		&e.AccountID, &e.RequestID, &e.Price, &e.Ratio, &e.Day, &e.DoneAt, &e.Tags,
		&e.EventID, &e.EventTime, &e.IngestTime, &e.OriginRegion,
	}
}

// callsParams and callsRow are what dynaflux generate writes for query Calls.
type callsParams struct {
	Account string
	Day     time.Time
}

func (p *callsParams) fields() []any { return []any{&p.Account, &p.Day} }

type callsRow struct {
	RequestID dynaflux.UUID
	Price     dynaflux.Decimal
}

func (r *callsRow) fields() []any { return []any{&r.RequestID, &r.Price} }

// start is where the fake clock of a synctest bubble starts.
var start = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) //nolint:gochecknoglobals // a constant time.Time

func loadCatalog(t *testing.T) *dynaflux.Catalog {
	t.Helper()
	return fluxgen.LoadCatalog(catalogJSON(t))
}

// catalogJSON is what dynaflux generate embeds for apiCalls.
func catalogJSON(t *testing.T) string {
	t.Helper()
	return string(catalogtest.Marshal(t, "app", apiCalls))
}

func newStore(b store.Backend) *dynaflux.Store {
	s := new(dynaflux.Store)
	access.Get().InitStore(s, engine.New(b, engine.Options{Region: "us-east-1"}))
	return s
}

func call(request byte) apiCall {
	return apiCall{
		AccountID: "acme",
		RequestID: dynaflux.UUID{15: request},
		Price:     mustDecimal("12.50"),
		Day:       start,
		EventTime: start.Add(-time.Hour),
	}
}

func mustDecimal(s string) dynaflux.Decimal {
	d, err := dynaflux.ParseDecimal(s)
	if err != nil {
		panic(err)
	}
	return d
}

func readAll(t *testing.T, s *dynaflux.Store, cat *dynaflux.Catalog, opts ...dynaflux.ReadOption) []apiCall {
	t.Helper()
	key := "acme"
	var out []apiCall
	for ev, err := range fluxgen.ReadEvents(t.Context(), s, cat, "api_calls", []any{&key}, (*apiCall).fields, opts...) {
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ev)
	}
	return out
}

func TestAppendDuplicateReturnsStoredToken(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cat := loadCatalog(t)
		s := newStore(mem.New())
		ev := call(1)
		first, err := fluxgen.AppendEvent(t.Context(), s, cat, "api_calls", ev.fields())
		if err != nil || first.IsZero() {
			t.Fatalf("got %q, %v; want a token", first, err)
		}
		ev.Price = mustDecimal("13")
		again, err := fluxgen.AppendEvent(t.Context(), s, cat, "api_calls", ev.fields())
		if !errors.Is(err, dynaflux.ErrDuplicate) || again != first {
			t.Errorf("got %q, %v; want the first token %q with ErrDuplicate", again, err, first)
		}
	})
}

func TestAppendRetriedBySDKSucceeds(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cat := loadCatalog(t)
		ev := call(1)
		tok, err := fluxgen.AppendEvent(t.Context(), newStore(storetest.RetriedWrite(mem.New())), cat, "api_calls", ev.fields())
		if err != nil || tok.IsZero() {
			t.Errorf("got %q, %v; want a token and no error", tok, err)
		}
	})
}

func TestBatchErrorNeedsTheLoop(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cat := loadCatalog(t)
		s := newStore(mem.New())
		stored := call(1)
		if _, err := fluxgen.AppendEvent(t.Context(), s, cat, "api_calls", stored.fields()); err != nil {
			t.Fatal(err)
		}
		rejected := call(2)
		rejected.Price = mustDecimal("1.234")
		tok, err := fluxgen.AppendEvents(t.Context(), s, cat, "api_calls", []apiCall{stored, rejected, call(3)}, (*apiCall).fields)
		if want := secondCallToken(t, cat, stored, call(3)); tok != want {
			t.Errorf("got token %q, want %q, which names the duplicate and the stored event", tok, want)
		}
		// The single check says "duplicate" although event 1 wasn't stored.
		if !errors.Is(err, dynaflux.ErrDuplicate) {
			t.Fatalf("errors.Is(%v, ErrDuplicate) is false", err)
		}
		var be *dynaflux.BatchError
		if !errors.As(err, &be) {
			t.Fatalf("%v is not a *BatchError", err)
		}
		if none, _ := fluxgen.AppendEvents(t.Context(), s, cat, "api_calls", []apiCall{rejected}, (*apiCall).fields); !none.IsZero() {
			t.Errorf("a batch that stored nothing returned token %q, want the zero Token", none)
		}
		var notStored []int
		for i, e := range be.Errs {
			if e != nil && !errors.Is(e, dynaflux.ErrDuplicate) {
				notStored = append(notStored, i)
			}
		}
		if !slices.Equal(notStored, []int{1}) || len(be.Unwrap()) != 2 {
			t.Errorf("events %v not stored and %d failures, want [1] and 2", notStored, len(be.Unwrap()))
		}
	})
}

// secondCallToken returns the token of a batch of first and next appended to a new store
// after first alone: DEDUP BY event IDs don't depend on the store, and an engine's second
// call gets the same ingest_time in the bubble.
func secondCallToken(t *testing.T, cat *dynaflux.Catalog, first, next apiCall) dynaflux.Token {
	t.Helper()
	s := newStore(mem.New())
	if _, err := fluxgen.AppendEvent(t.Context(), s, cat, "api_calls", first.fields()); err != nil {
		t.Fatal(err)
	}
	tok, err := fluxgen.AppendEvents(t.Context(), s, cat, "api_calls", []apiCall{first, next}, (*apiCall).fields)
	if !errors.Is(err, dynaflux.ErrDuplicate) {
		t.Fatalf("the batch: %v, want only the first event a duplicate", err)
	}
	return tok
}

func TestFieldTypeMismatch(t *testing.T) {
	t.Parallel()
	cat := loadCatalog(t)
	ev := call(1)
	fields := ev.fields()
	var price float64
	fields[2] = &price
	_, err := fluxgen.AppendEvent(t.Context(), newStore(mem.New()), cat, "api_calls", fields)
	want := "dynaflux: stream api_calls: field 2 (price Decimal(10, 2)) is *float64, want *dynaflux.Decimal; regenerate the client"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}
}

// helperErrors returns the error of each helper given s and cat.
func helperErrors(t *testing.T, s *dynaflux.Store, cat *dynaflux.Catalog) map[string]error {
	t.Helper()
	ev := call(1)
	evs := []apiCall{ev}
	_, appendErr := fluxgen.AppendEvent(t.Context(), s, cat, "api_calls", ev.fields())
	_, batchErr := fluxgen.AppendEvents(t.Context(), s, cat, "api_calls", evs, (*apiCall).fields)
	importErr := fluxgen.ImportEvents(t.Context(), s, cat, "api_calls", evs, (*apiCall).fields)
	key := "acme"
	var readErr error
	for _, err := range fluxgen.ReadEvents(t.Context(), s, cat, "api_calls", []any{&key}, (*apiCall).fields) {
		readErr = err
	}
	params := callsParams{Account: "acme", Day: start}
	_, rowsErr := fluxgen.QueryRows(t.Context(), s, cat, "Calls", params.fields(), (*callsRow).fields)
	_, rowErr := fluxgen.QueryRow(t.Context(), s, cat, "Calls", params.fields(), (*callsRow).fields)
	return map[string]error{
		"AppendEvent": appendErr, "AppendEvents": batchErr, "ImportEvents": importErr, "ReadEvents": readErr,
		"QueryRows": rowsErr, "QueryRow": rowErr,
	}
}

func TestLoadCatalogErrorReachesEveryHelper(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		edit      func(string) string
		wantCause string
	}{
		{"newer format", func(string) string { return `{"format":99}` }, "format 99"},
		{"undeclared PRIMARY KEY column", func(s string) string {
			return strings.Replace(s, `"primary_key":["account_id"]`, `"primary_key":["missing"]`, 1)
		}, "primary key missing is not a declared column"},
		{"undeclared DEDUP BY column", func(s string) string {
			return strings.Replace(s, `"dedup_by":["request_id"]`, `"dedup_by":["missing"]`, 1)
		}, "dedup by missing is not a declared column"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cat := fluxgen.LoadCatalog(tt.edit(catalogJSON(t)))
			for name, err := range helperErrors(t, newStore(mem.New()), cat) {
				if err == nil || !strings.Contains(err.Error(), "can't load the generated catalog") || !strings.Contains(err.Error(), tt.wantCause) {
					t.Errorf("%s: got %v, want the catalog's load error, for %s", name, err, tt.wantCause)
				}
			}
		})
	}
}

func TestStoreNotMadeByNewStore(t *testing.T) {
	t.Parallel()
	for name, s := range map[string]*dynaflux.Store{"nil": nil, "zero": new(dynaflux.Store)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const want = "dynaflux: the Store is nil or zero; make one with fluxtest.NewStore"
			for helper, err := range helperErrors(t, s, loadCatalog(t)) {
				if err == nil || err.Error() != want {
					t.Errorf("%s: got %v, want %s", helper, err, want)
				}
			}
		})
	}
}

func TestCatalogNotFromGeneratedPackage(t *testing.T) {
	t.Parallel()
	for name, cat := range map[string]*dynaflux.Catalog{"nil": nil, "zero": new(dynaflux.Catalog)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const want = "dynaflux: the Catalog is nil or zero; use the generated package's Catalog"
			for helper, err := range helperErrors(t, newStore(mem.New()), cat) {
				if err == nil || err.Error() != want {
					t.Errorf("%s: got %v, want %s", helper, err, want)
				}
			}
		})
	}
}

func TestZeroEventTimeInAnyLocationIsUnset(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ev := call(1)
		ev.EventTime = time.Time{}.In(time.FixedZone("Tokyo", 9*60*60))
		_, err := fluxgen.AppendEvent(t.Context(), newStore(mem.New()), loadCatalog(t), "api_calls", ev.fields())
		// api_calls has DEDUP BY, so an unset event_time is an error.
		const want = "dynaflux: event_time: invalid value: a stream with DEDUP BY needs it"
		if err == nil || err.Error() != want {
			t.Errorf("got %v, want %s", err, want)
		}
	})
}

func TestValueErrorNamesTheColumn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(*apiCall)
		column string
	}{
		{"Decimal with more fractional digits than its scale", func(e *apiCall) { e.Price = mustDecimal("1.234") }, "price"},
		{"invalid UTF-8", func(e *apiCall) { e.AccountID = "\xff" }, "account_id"},
		{"NaN", func(e *apiCall) { e.Ratio = math.NaN() }, "ratio"},
		{"Float64 outside DynamoDB's range", func(e *apiCall) { e.Ratio = 1e300 }, "ratio"},
		{"date before 1970", func(e *apiCall) { e.Day = time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC) }, "day"},
		{"1001 array elements", func(e *apiCall) { e.Tags = make([]string, 1001) }, "tags"},
		{"no event_time with DEDUP BY", func(e *apiCall) { e.EventTime = time.Time{} }, "event_time"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ev := call(1)
				tt.change(&ev)
				_, err := fluxgen.AppendEvent(t.Context(), newStore(mem.New()), loadCatalog(t), "api_calls", ev.fields())
				var ve *dynaflux.ValueError
				if !errors.As(err, &ve) || ve.Name != tt.column || !errors.Is(err, dynaflux.ErrInvalidValue) {
					t.Errorf("got %v, want a *dynaflux.ValueError for %s", err, tt.column)
				}
			})
		})
	}
}

// An event whose archive record would be over Firehose's record limit fails with an
// *ArchiveSizeError that names the stream.
func TestArchiveSizeErrorNamesTheStream(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ev := call(1)
		ev.Tags = []string{strings.Repeat("\x01", 180_000)} // 6 bytes each in JSON, 1 in the item
		_, err := fluxgen.AppendEvent(t.Context(), newStore(mem.New()), loadCatalog(t), "api_calls", ev.fields())
		ae, ok := errors.AsType[*dynaflux.ArchiveSizeError](err)
		if !ok || ae.Stream != "api_calls" || ae.Size != 1_080_412 || !errors.Is(err, dynaflux.ErrInvalidValue) {
			t.Errorf("got %v, want a *dynaflux.ArchiveSizeError for api_calls of 1080412 bytes", err)
		}
	})
}

func TestValuesRoundTrip(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cat := loadCatalog(t)
		s := newStore(mem.New())
		tokyo := time.FixedZone("Tokyo", 9*60*60)
		ev := call(1)
		ev.Price = mustDecimal("1.2")
		ev.Day = time.Date(2024, 5, 31, 0, 0, 0, 0, tokyo)
		ev.DoneAt = new(time.Date(2024, 5, 31, 13, 45, 0, 250_999_999, time.UTC))
		ev.Ratio = 0.25
		ev.Tags = []string{}
		if _, err := fluxgen.AppendEvent(t.Context(), s, cat, "api_calls", ev.fields()); err != nil {
			t.Fatal(err)
		}
		got := readAll(t, s, cat)
		want := apiCall{
			AccountID:    "acme",
			RequestID:    dynaflux.UUID{15: 1},
			Price:        mustDecimal("1.20"),
			Ratio:        0.25,
			Day:          time.Date(2024, 5, 30, 0, 0, 0, 0, time.UTC),
			DoneAt:       new(time.Date(2024, 5, 31, 13, 45, 0, 250_000_000, time.UTC)),
			EventTime:    start.Add(-time.Hour),
			EventID:      "24RY764MC2EK6BYZ9NGJMA08KA",
			IngestTime:   start,
			OriginRegion: "us-east-1",
		}
		if !reflect.DeepEqual(got, []apiCall{want}) {
			t.Errorf("read\n%+v\nwant\n%+v", got, []apiCall{want})
		}
	})
}

func TestReadEventsChecksTheKeyFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  []any
		want string
	}{
		{"wrong type", []any{new(int)}, "dynaflux: key of stream api_calls: field 0 (account_id String) is *int, want *string; regenerate the client"},
		{"wrong count", []any{new(string), new(string)}, "dynaflux: key of stream api_calls: 2 fields, want 1; regenerate the client"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var errs []error
			for _, err := range fluxgen.ReadEvents(t.Context(), newStore(mem.New()), loadCatalog(t), "api_calls", tt.key, (*apiCall).fields) {
				errs = append(errs, err)
			}
			if len(errs) != 1 || errs[0] == nil || errs[0].Error() != tt.want {
				t.Errorf("got %v, want one error: %s", errs, tt.want)
			}
		})
	}
}

func TestQueryRowsRoundTrip(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cat := loadCatalog(t)
		s := newStore(mem.New())
		today, yesterday := call(1), call(2)
		yesterday.Day = start.Add(-24 * time.Hour)
		if _, err := fluxgen.AppendEvents(t.Context(), s, cat, "api_calls", []apiCall{today, yesterday}, (*apiCall).fields); err != nil {
			t.Fatal(err)
		}
		// 08:00 in Tokyo on 2000-01-01 is still 1999-12-31 in UTC.
		params := callsParams{Account: "acme", Day: time.Date(2000, 1, 1, 8, 0, 0, 0, time.FixedZone("Tokyo", 9*60*60))}
		got, err := fluxgen.QueryRows(t.Context(), s, cat, "Calls", params.fields(), (*callsRow).fields)
		if want := []callsRow{{RequestID: dynaflux.UUID{15: 2}, Price: mustDecimal("12.50")}}; err != nil || !slices.Equal(got, want) {
			t.Errorf("got %+v, %v; want %+v", got, err, want)
		}
	})
}

func TestQueryRowNotFound(t *testing.T) {
	t.Parallel()
	// Generated code calls QueryRow only for :one queries; a :many query finds no row the
	// same way.
	for _, query := range []string{"LastCall", "Calls"} {
		params := callsParams{Account: "acme", Day: start}
		_, err := fluxgen.QueryRow(t.Context(), newStore(mem.New()), loadCatalog(t), query, params.fields(), (*callsRow).fields)
		if !errors.Is(err, dynaflux.ErrNotFound) {
			t.Errorf("%s: got %v, want ErrNotFound", query, err)
		}
	}
}

func TestQueryOptionsReachTheEngine(t *testing.T) {
	t.Parallel()
	account := "acme"
	query := func(opts ...dynaflux.QueryOption) error {
		_, err := fluxgen.QueryRow(t.Context(), newStore(mem.New()), loadCatalog(t), "AccountCalls", []any{&account},
			func(r *uint64) []any { return []any{r} }, opts...)
		return err
	}
	// No backfill made the view's version live.
	const notReady = "dynaflux: query AccountCalls: view version not ready: account_calls@v1 is pending_backfill in us-east-1"
	if err := query(); !errors.Is(err, dynaflux.ErrViewNotReady) || err.Error() != notReady {
		t.Errorf("without options: %v, want %q", err, notReady)
	}
	if err := query(dynaflux.AllowPartial()); !errors.Is(err, dynaflux.ErrNotFound) {
		t.Errorf("with AllowPartial: %v, want ErrNotFound from the view read", err)
	}
}

func TestQueryChecksTheFields(t *testing.T) {
	t.Parallel()
	params := callsParams{Account: "acme", Day: start}
	tests := []struct {
		name        string
		params, row []any
		want        string
	}{
		{"parameter", []any{new(int), &params.Day}, new(callsRow).fields(),
			"dynaflux: parameters of query Calls: field 0 (account String) is *int, want *string; regenerate the client"},
		{"row", params.fields(), []any{new(dynaflux.UUID), new(float64)},
			"dynaflux: row of query Calls: field 1 (price Decimal(10, 2)) is *float64, want *dynaflux.Decimal; regenerate the client"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := fluxgen.QueryRows(t.Context(), newStore(mem.New()), loadCatalog(t), "Calls", tt.params, func(*callsRow) []any { return tt.row })
			if err == nil || err.Error() != tt.want {
				t.Errorf("got %v, want %s", err, tt.want)
			}
		})
	}
}

func TestReadBoundsWithNanoseconds(t *testing.T) {
	t.Parallel()
	at := start.Add(-time.Hour)
	tests := []struct {
		name string
		opt  dynaflux.ReadOption
		want int
	}{
		{"since a nanosecond before", dynaflux.Since(at.Add(-time.Nanosecond)), 1},
		{"since a nanosecond after", dynaflux.Since(at.Add(time.Nanosecond)), 0},
		{"until the event", dynaflux.Until(at), 0},
		{"until a nanosecond after", dynaflux.Until(at.Add(time.Nanosecond)), 1},
		{"since before 1970", dynaflux.Since(time.Time{}), 1},
		{"until past 2106", dynaflux.Until(time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				cat := loadCatalog(t)
				s := newStore(mem.New())
				ev := call(1)
				if _, err := fluxgen.AppendEvent(t.Context(), s, cat, "api_calls", ev.fields()); err != nil {
					t.Fatal(err)
				}
				if got := len(readAll(t, s, cat, tt.opt)); got != tt.want {
					t.Errorf("read %d events, want %d", got, tt.want)
				}
			})
		})
	}
}

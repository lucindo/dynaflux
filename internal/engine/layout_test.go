// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

func str(s string) store.Value { return store.Value{Type: store.S, Str: s} }

func num(s string) store.Value { return store.Value{Type: store.N, Str: s} }

// TestAppendItemLayout pins the stream items of encoding.md §4 and §6, which every release
// must keep reading and deduplicating against. The event_ids and the shard were computed
// from the spec apart from this code.
func TestAppendItemLayout(t *testing.T) {
	t.Parallel()
	at := start.Add(-time.Minute)
	// Each append is one microsecond after the one before, at 00:00:00.25; LOG RETENTION
	// 2 DAY ends at 946857600.25 s, so _ttl is 946857601 + 3600.
	tests := []struct {
		stream string
		event  []expr.Value
		want   store.Item
	}{
		{"page_views", pageView("/home", micros(at)), store.Item{
			"pk": str("u1"),
			// A ULID of the writer's clock, 946684800250 ms, not of event_time.
			"sk":      str("1999-12-31T23:59:00.000000Z#00VHNCZB7TV63QXKKD6T5AR6KF"),
			"user_id": str("u1"), "page": str("/home"), "tags": {Type: store.L, List: []store.Value{}},
			"duration_ms": num("100"), "country": str("US"),
			"_r": str("us-east-1"), "_it": num("946684800250000"), "_ttl": num("946861201"),
		}},
		{"api_calls", apiCall(1, 5, at), store.Item{
			"pk": str("acme"),
			// base32(SHA-256("acme#1999-12-31T23:59:00.000000Z#00000000-0000-0000-0000-000000000001")[:16])
			"sk":         str("1999-12-31T23:59:00.000000Z#1W65EZ2EZN55V3N4ADGMFCN6Q5"),
			"account_id": str("acme"), "request_id": str("00000000-0000-0000-0000-000000000001"),
			"units": num("5"), "price": num("12.5"),
			"_r": str("us-east-1"), "_it": num("946684800250001"), "_ttl": num("946861201"),
		}},
		{"clicks", []expr.Value{expr.String("u1"), expr.String("x"), micros(at)}, store.Item{
			// Shard bucket(event_id, 8) = 0; the partition key alone would give 2.
			"pk":      str("u1#s00"),
			"sk":      str("1999-12-31T23:59:00.000000Z#00VHNCZB7TV63QXKKD6T5AR6KG"),
			"user_id": str("u1"), "target": str("x"),
			"_r": str("us-east-1"), "_it": num("946684800250002"), "_ttl": num("946861201"),
		}},
	}
	synctest.Test(t, func(t *testing.T) {
		c := compile(t, streams)
		b := mem.New()
		e := newEngine(b)
		time.Sleep(250 * time.Millisecond)
		for _, tt := range tests {
			got := appendOne(t, e, c, tt.stream, tt.event, engine.AppendMode{})
			if got.Err != nil {
				t.Fatal(got.Err)
			}
			it, err := b.Get(t.Context(), "app-stream-"+tt.stream, got.Key, true)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(it, tt.want) {
				t.Errorf("%s: stored\n%v\nwant\n%v", tt.stream, it, tt.want)
			}
		}
	})
}

// TestSyncAppendItemLayout pins what a Waitable append to a stream with SYNC views writes
// in its transaction (encoding.md §6, §7.1 and §7.4): the event with _sync and _rcpt, each
// view's row with _v and the _hwm it read, and the uniqExact marker with an E owner. The
// event_id and the marker ID were computed from the spec apart from this code.
func TestSyncAppendItemLayout(t *testing.T) {
	t.Parallel()
	list := func(vs ...store.Value) store.Value { return store.Value{Type: store.L, List: vs} }
	// base32(SHA-256("acme#2000-01-01T00:00:00.000000Z#r1")[:16])
	const eventID = "46G5DBXPX9H3Z9Q0PX0H158Q1Y"
	noMarks := store.Value{Type: store.M, Map: map[string]store.Value{}}
	tests := []struct {
		table string
		want  []store.Item // in (pk, sk) order
	}{
		{callsTable, []store.Item{{
			"pk": str("acme"), "sk": str("2000-01-01T00:00:00.000000Z#" + eventID),
			"account_id": str("acme"), "endpoint": str("/v1/x"), "request_id": str("r1"), "units": num("5"), "note": str(""),
			"_r": str("us-east-1"), "_it": num("946684800000000"), "_ttl": num("946861200"),
			"_sync": list(str(endpointAcc), str(quotaUsage)),
			"_rcpt": {Type: store.BOOL, Bool: true},
		}}},
		{quotaTable, []store.Item{{
			"pk": str("acme"), "sk": str("!"), "account_id": str("acme"), "calls": num("1"), "used": num("5"),
			"_v": num("1"), "_hwm": noMarks,
		}}},
		{"app-view-endpoint_accounts-v1", []store.Item{
			// The marker of account_id acme in row /v1/x of column accounts.
			{"pk": str("!0XV9VH9M2ZB12MEFEK1XGWXRAW"), "sk": str("!"), "_o": str("E" + eventID)},
			{
				"pk": str("/v1/x"), "sk": str("!"), "endpoint": str("/v1/x"), "accounts": num("1"), "shares": num("20"),
				"_v": num("1"), "_hwm": noMarks,
			},
		}},
	}
	synctest.Test(t, func(t *testing.T) {
		f := newSyncFixture(t)
		if res := f.append(t, f.e, engine.AppendMode{Receipt: true}, call("acme", "/v1/x", "r1", 5)); res[0].Err != nil {
			t.Fatal(res[0].Err)
		}
		for _, tt := range tests {
			got, _, err := f.mem.ScanPage(t.Context(), tt.table, store.Scan{Segments: 1}, nil)
			if err != nil {
				t.Fatal(err)
			}
			slices.SortFunc(got, func(a, b store.Item) int {
				return strings.Compare(a["pk"].Str+"\x00"+a["sk"].Str, b["pk"].Str+"\x00"+b["sk"].Str)
			})
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("%s holds\n%v\nwant\n%v", tt.table, got, tt.want)
			}
		}
	})
}

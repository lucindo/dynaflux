// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine_test

import (
	"reflect"
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

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/lucindo/dynaflux/internal/store"
)

// TestRowItemLayout pins the view items of encoding.md §3.3, §7.1, §7.4 and §7.6 that the
// projector writes, which every release must keep reading: rows with their _v and _hwm, and
// uniqExact markers. The unit key, the SPLIT item, the marker IDs and owners, and the
// high-water mark were computed from the spec apart from this code.
func TestRowItemLayout(t *testing.T) {
	t.Parallel()
	s := func(v string) store.Value { return store.Value{Type: store.S, Str: v} }
	n := func(v string) store.Value { return store.Value{Type: store.N, Str: v} }
	// unitkey("shardId-00000001") is 63WCYVA3A1237; the record of seq(2),
	// 100000000000000000002, is the high-water mark 0x056bc75e2d63100002.
	hwm := store.Value{Type: store.M, Map: map[string]store.Value{
		"63WCYVA3A1237": {Type: store.B, Bin: []byte{0x05, 0x6b, 0xc7, 0x5e, 0x2d, 0x63, 0x10, 0x00, 0x02}},
	}}
	owner := func(seq string) store.Value { return s("L63WCYVA3A1237" + strings.Repeat("0", 19) + seq) }
	tests := []struct {
		table string
		want  []store.Item // in (pk, sk) order
	}{
		{"app-view-page_totals-v1", []store.Item{
			{"pk": s("!5BPV34Q55WZB30KRAQ0TPDRDKT"), "sk": s("!"), "_o": owner("100000000000000000001")}, // u1
			{"pk": s("!7YM27G034MKP8GRDPH6XAFM4R1"), "sk": s("!"), "_o": owner("100000000000000000002")}, // u2
			{
				"pk": s("/a"), "sk": s("!"), "page": s("/a"),
				"views": n("2"), "total_ms": n("400"), "users": n("2"), "longest": n("300"), "shortest": n("100"),
				"revenue": n("4"), // 4.00, as DynamoDB stores it
				"_v":      n("1"), "_hwm": hwm,
			},
		}},
		// TTL day + INTERVAL 1 DAY, and SPLIT 2: bucket(unit, 2) is 1.
		{"app-view-page_days-v1", []store.Item{
			{"pk": s("!015YJ6K24CMXJS01GP7RB8R6C5"), "sk": s("!"), "_o": owner("100000000000000000001"), "_ttl": n("946774800")},
			{"pk": s("!2NQ95XA551SAMTDK6V2T3Q6Q51"), "sk": s("!"), "_o": owner("100000000000000000002"), "_ttl": n("946774800")},
			{
				"pk": s("/a"), "sk": s("2000-01-01T00:00:00Z#p01"), "page": s("/a"), "day": n("946684800000000"),
				"views": n("2"), "users": n("2"),
				"_v": n("1"), "_hwm": hwm, "_ttl": n("946774800"), // 2000-01-02 + 3600 s
			},
		}},
	}
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		f.handle(t, insert(1, f.click(t, "u1", "/a", 100)), insert(2, f.click(t, "u2", "/a", 300)))
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

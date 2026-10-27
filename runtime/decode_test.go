// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package runtime_test

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/runtime"
)

func s(v string) store.Value { return store.Value{Type: store.S, Str: v} }

func n(v string) store.Value { return store.Value{Type: store.N, Str: v} }

// A recorded event decodes with N text as sent, the arrival in whole seconds, NULL and sets
// as the zero Value, empty B, L and M that keep their type, and REMOVEs kept for the core.
func TestRecords(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/window_event.json")
	if err != nil {
		t.Fatal(err)
	}
	got, err := runtime.Records(data)
	if err != nil {
		t.Fatal(err)
	}
	want := []projector.Record{
		{
			Op:      projector.Insert,
			Seq:     "100000000000000000001",
			Arrival: time.Unix(1761480000, 0).UTC(),
			New: store.Item{
				"pk":           s("p#1"),
				"sk":           s("s#1"),
				"price":        n("12.50"),
				"big":          n("1E+2"),
				"long":         n("12345678901234567890123456789012345678"),
				"tiny":         n("-0." + strings.Repeat("0", 126) + "1"),
				"name":         s("café \x00 😀"),
				"blob":         {Type: store.B, Bin: []byte{0, 1, 2}},
				"empty_blob":   {Type: store.B, Bin: []byte{}},
				"flag":         {Type: store.BOOL, Bool: false},
				"tags":         {Type: store.L, List: []store.Value{s("a"), n("1")}},
				"empty_list":   {Type: store.L, List: []store.Value{}},
				"nested":       {Type: store.M, Map: map[string]store.Value{"a": n("1")}},
				"empty_map":    {Type: store.M, Map: map[string]store.Value{}},
				"foreign_null": {},
				"foreign_set":  {},
			},
		},
		{
			Op:      projector.Modify,
			Seq:     "9999999999999999999999999999999999999999",
			Arrival: time.Unix(1761480060, 0).UTC(),
			New:     store.Item{"pk": s("p#1"), "sk": s("s#2"), "price": n("1")},
			Old:     store.Item{"pk": s("p#1"), "sk": s("s#2"), "price": n("2")},
		},
		{
			Op:  projector.Remove,
			Seq: "100000000000000000003",
			Old: store.Item{"pk": s("p#1"), "sk": s("s#0")},
		},
	}
	// DeepEqual tells nil from empty, which is the point for B, L and M.
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records:\n%#v\nwant:\n%#v", got, want)
	}
}

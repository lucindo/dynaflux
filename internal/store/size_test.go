// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"testing"

	"github.com/lucindo/dynaflux/internal/store"
)

func TestItemSize(t *testing.T) {
	t.Parallel()
	str := func(s string) store.Value { return store.Value{Type: store.S, Str: s} }
	num := func(s string) store.Value { return store.Value{Type: store.N, Str: s} }
	tests := []struct {
		name string
		item store.Item
		want int
	}{
		{"empty", store.Item{}, 0},
		// The example of the DynamoDB developer guide's "Items" constraints.
		{"guide example", store.Item{"shirt-color": str("R"), "shirt-size": str("M")}, 23},
		{"utf-8 bytes", store.Item{"é": str("日本")}, 2 + 6},
		{"empty string", store.Item{"s": str("")}, 1},
		{"binary", store.Item{"b": {Type: store.B, Bin: []byte{0, 1, 2}}}, 1 + 3},
		{"bool", store.Item{"t": {Type: store.BOOL, Bool: true}}, 1 + 1},
		{"number with trailing zero", store.Item{"n": num("12.50")}, 1 + 2 + 2},
		{"zero", store.Item{"n": num("0")}, 1 + 0 + 2},
		{"leading and trailing zeros", store.Item{"n": num("-0.000120")}, 1 + 1 + 2},
		{"exponent digits don't count", store.Item{"n": num("1E+40")}, 1 + 1 + 2},
		{"38 digits", store.Item{"n": num("99999999999999999999999999999999999999")}, 1 + 19 + 2},
		{"empty list", store.Item{"l": {Type: store.L}}, 1 + 3},
		{
			"list",
			store.Item{"l": {Type: store.L, List: []store.Value{str("ab"), num("7")}}},
			1 + 3 + (2 + 1) + (1 + 2 + 1),
		},
		{
			"map counts element names",
			store.Item{"m": {Type: store.M, Map: map[string]store.Value{
				"k":  str("v"),
				"kk": {Type: store.BOOL},
			}}},
			1 + 3 + (1 + 1 + 1) + (2 + 1 + 1),
		},
		{
			"nested",
			store.Item{"x": {Type: store.L, List: []store.Value{
				{Type: store.M, Map: map[string]store.Value{"a": {Type: store.L}}},
			}}},
			1 + 3 + (3 + (1 + 3 + 1) + 1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := store.ItemSize(tt.item); got != tt.want {
				t.Errorf("ItemSize(%v) = %d, want %d", tt.item, got, tt.want)
			}
		})
	}
}

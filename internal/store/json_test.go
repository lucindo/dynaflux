// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/lucindo/dynaflux/internal/store"
)

// An item survives DynamoDB's JSON form: N as text, empty B, L and M with their type, and
// the zero Value a foreign writer leaves as {}.
func TestJSONRoundTrip(t *testing.T) {
	t.Parallel()
	it := store.Item{
		"s":  {Type: store.S, Str: "café \x00"},
		"n":  {Type: store.N, Str: "-1.5E-07"},
		"b":  {Type: store.B, Bin: []byte{0, 1}},
		"eb": {Type: store.B, Bin: []byte{}},
		"t":  {Type: store.BOOL, Bool: false},
		"l":  {Type: store.L, List: []store.Value{{Type: store.S, Str: "x"}, {}}},
		"el": {Type: store.L, List: []store.Value{}},
		"m":  {Type: store.M, Map: map[string]store.Value{"k": {Type: store.N, Str: "1"}}},
		"em": {Type: store.M, Map: map[string]store.Value{}},
		"z":  {},
	}
	data, err := json.Marshal(store.EncodeJSON(it))
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"b":{"B":"AAE="},"eb":{"B":""},"el":{"L":[]},"em":{"M":{}},"l":{"L":[{"S":"x"},{}]},` +
		`"m":{"M":{"k":{"N":"1"}}},"n":{"N":"-1.5E-07"},"s":{"S":"café \u0000"},"t":{"BOOL":false},"z":{}}`
	if string(data) != want {
		t.Errorf("encoded\n%s\nwant\n%s", data, want)
	}
	var m map[string]store.JSONValue
	if err = json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	got, err := store.DecodeJSON(m)
	// DeepEqual tells nil from empty, which is the point for B, L and M.
	if err != nil || !reflect.DeepEqual(got, it) {
		t.Errorf("decoded %#v, %v\nwant %#v", got, err, it)
	}
}

func TestDecodeJSONRejectsTwoTypes(t *testing.T) {
	t.Parallel()
	var m map[string]store.JSONValue
	if err := json.Unmarshal([]byte(`{"a":{"L":[{"S":"x","N":"1"}]}}`), &m); err != nil {
		t.Fatal(err)
	}
	if it, err := store.DecodeJSON(m); err == nil || err.Error() != "a: an attribute of two types" {
		t.Errorf("DecodeJSON = %v, %v; want a: an attribute of two types", it, err)
	}
}

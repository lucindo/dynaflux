// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package storetest

import (
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/store"
)

// DynamoDB's number range: magnitudes from 1E-130 to 9.99…E+125.
const (
	minExp = -130
	maxExp = 125
)

// checkNumberForm checks that numbers come back as DynamoDB stores them: leading and
// trailing zeros trimmed (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html).
// The same numbers inside an L and an M need only keep their exact value.
func (s suite) checkNumberForm(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	tiny := "0." + strings.Repeat("0", -minExp-1) + "1"
	huge := "1" + strings.Repeat("0", maxExp)
	in := map[string]string{
		"a": "12.50",
		"b": "007",
		"c": "-0",
		"d": "0.000",
		"e": "1.0",
		"f": "100",
		"g": "-0.50",
		"h": "00.0100",
		"i": "12345678901234567890123456789012345678",
		"j": tiny,
		"k": huge,
	}
	want := map[string]string{
		"a": "12.5",
		"b": "7",
		"c": "0",
		"d": "0",
		"e": "1",
		"f": "100",
		"g": "-0.5",
		"h": "0.01",
		"i": "12345678901234567890123456789012345678",
		"j": tiny,
		"k": huge,
	}
	item := func(nums map[string]string) store.Item {
		it := keyed(part, "a", nil)
		var l []store.Value
		m := make(map[string]store.Value, len(nums))
		for _, name := range slices.Sorted(maps.Keys(nums)) {
			it[name] = num(nums[name])
			l = append(l, num(nums[name]))
			m[name] = num(nums[name])
		}
		it["l"] = list(l...)
		it["m"] = store.Value{Type: store.M, Map: m}
		return it
	}
	mustPut(t, b, item(in), store.Cond{})
	stored := item(want)
	wantStored(t, b, stored)

	err := b.Put(t.Context(), Table, item(in), store.IfAbsent())
	wantConditionFailed(t, err, stored)
	for got, err := range b.Query(t.Context(), Table, store.Query{PK: part}) {
		if err != nil || !equalItem(got, stored) {
			t.Errorf("Query yielded %s, %v; want %s", show(got), err, show(stored))
		}
	}
}

// checkEmptyValues round-trips nil and empty L, M and B, and the empty S, at the top level
// and nested. The SDK sends a nil L or M as invalid JSON, which DynamoDB Local answers
// with a 500 that the SDK retries as transient, so a backend must send empty ones.
func (s suite) checkEmptyValues(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	empty := store.Item{
		"s":    str(""),
		"bNil": {Type: store.B},
		"b":    {Type: store.B, Bin: []byte{}},
		"lNil": {Type: store.L},
		"l":    {Type: store.L, List: []store.Value{}},
		"mNil": {Type: store.M},
		"m":    {Type: store.M, Map: map[string]store.Value{}},
	}
	it := keyed(part, "a", empty)
	it["inList"] = list(slices.Collect(maps.Values(empty))...)
	it["inMap"] = store.Value{Type: store.M, Map: empty}
	mustPut(t, b, it, store.Cond{})
	wantStored(t, b, it)
}

func (s suite) checkDeepCopies(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	original := func() store.Item {
		return keyed(part, "a", store.Item{
			"b": {Type: store.B, Bin: []byte{1, 2}},
			"l": list(str("x")),
			"m": {Type: store.M, Map: map[string]store.Value{"k": str("v")}},
		})
	}
	mutate := func(it store.Item) {
		it["b"].Bin[0] = 9
		it["l"].List[0] = str("changed")
		it["m"].Map["k"] = str("changed")
		it["new"] = str("added")
	}
	in := original()
	mustPut(t, b, in, store.Cond{})
	mutate(in)
	wantStored(t, b, original())

	mutate(mustGet(t, b, keyOf(in)))
	wantStored(t, b, original())

	batch, err := b.BatchGet(t.Context(), Table, []store.Key{keyOf(in)}, true)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	mutate(batch[0])
	wantStored(t, b, original())

	for it, err := range b.Query(t.Context(), Table, store.Query{PK: part}) {
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		mutate(it)
	}
	wantStored(t, b, original())

	scanned, _, err := b.ScanPage(t.Context(), Table, store.Scan{Segments: 1}, nil)
	if err != nil || len(scanned) != 1 {
		t.Fatalf("ScanPage = %d items, %v; want 1", len(scanned), err)
	}
	mutate(scanned[0])
	wantStored(t, b, original())

	var ce *store.ConditionError
	if err = b.Put(t.Context(), Table, original(), store.IfAbsent()); !errors.As(err, &ce) {
		t.Fatalf("Put over an existing item with IfAbsent: %v, want a *store.ConditionError", err)
	}
	mutate(ce.Current)
	wantStored(t, b, original())
}

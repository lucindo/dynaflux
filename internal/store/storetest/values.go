// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package storetest

import (
	"errors"
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
	item := func(nums map[string]string, inList, inMap string) store.Item {
		it := keyed(part, "a", nil)
		for name, n := range nums {
			it[name] = num(n)
		}
		it["l"] = list(num(inList))
		it["m"] = store.Value{Type: store.M, Map: map[string]store.Value{"x": num(inMap)}}
		return it
	}
	mustPut(t, b, item(in, "2.50", "-3.10"), store.Cond{})
	stored := item(want, "2.5", "-3.1")
	wantStored(t, b, stored)

	err := b.Put(t.Context(), Table, item(in, "2.50", "-3.10"), store.IfAbsent())
	wantConditionFailed(t, err, stored)
	for got, err := range b.Query(t.Context(), Table, store.Query{PK: part}) {
		if err != nil || !equalItem(got, stored) {
			t.Errorf("Query yielded %s, %v; want %s", show(got), err, show(stored))
		}
	}
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

	var ce *store.ConditionError
	if err = b.Put(t.Context(), Table, original(), store.IfAbsent()); !errors.As(err, &ce) {
		t.Fatalf("Put over an existing item with IfAbsent: %v, want a *store.ConditionError", err)
	}
	mutate(ce.Current)
	wantStored(t, b, original())
}

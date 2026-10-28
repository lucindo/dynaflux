// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package storetest

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

// The items of a SYNC append's transaction: its event, a row of a view, and a marker of one
// of the row's uniqExact values.
func event(id string) store.Item {
	return keyed(part, "event#"+id, store.Item{"_r": str("r0"), "units": num("3")})
}

func row(version, used string) store.Item {
	return keyed(part, "row", store.Item{catalog.AttrRowVersion: num(version), "used": num(used)})
}

func marker(value, owner string) store.Item {
	return keyed(part, "!marker#"+value, store.Item{catalog.AttrMarkerOwner: str(owner)})
}

func tx(puts ...store.TxPut) []store.TxPut { return puts }

func txPut(it store.Item, c store.Cond) store.TxPut {
	return store.TxPut{Table: Table, Item: it, Cond: c}
}

// wantTxError fails unless err is a *store.TxError matching ErrConditionFailed whose Conds
// hold want: nil where a condition didn't fail, and the stored item where it did ("absent"
// for none).
func wantTxError(t *testing.T, err error, want ...any) {
	t.Helper()
	txErr, ok := errors.AsType[*store.TxError](err)
	if !ok || !errors.Is(err, store.ErrConditionFailed) {
		t.Fatalf("error = %v, want a *store.TxError matching ErrConditionFailed", err)
	}
	if len(txErr.Conds) != len(want) {
		t.Fatalf("%d Conds, want %d", len(txErr.Conds), len(want))
	}
	for i, w := range want {
		got := txErr.Conds[i]
		switch w := w.(type) {
		case nil:
			if got != nil {
				t.Errorf("Conds[%d] = %s, want nil", i, show(got.Current))
			}
		case store.Item:
			if got == nil || !equalItem(got.Current, w) {
				t.Errorf("Conds[%d] = %v, want the stored item %s", i, got, show(w))
			}
		case string: // "absent"
			if got == nil || got.Current != nil {
				t.Errorf("Conds[%d] = %v, want a failure over an absent item", i, got)
			}
		}
	}
}

func mustTransact(t *testing.T, b store.Backend, puts []store.TxPut) {
	t.Helper()
	if err := b.Transact(t.Context(), puts); err != nil {
		t.Fatalf("Transact: %v", err)
	}
}

// wantAbsent fails unless no item is stored under each item's key.
func wantAbsent(t *testing.T, b store.Backend, its ...store.Item) {
	t.Helper()
	for _, it := range its {
		if got := mustGet(t, b, store.KeyOf(it)); got != nil {
			t.Errorf("stored %s, want nothing", show(got))
		}
	}
}

func (s suite) checkTransactWritesAll(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	mustPut(t, b, row("1", "3"), store.IfAbsent())
	plain := keyed(part, "plain", store.Item{"n": num("1.50")})
	mustTransact(t, b, tx(
		txPut(event("1"), store.IfAbsent()),
		txPut(row("2", "6"), store.IfVersion(1)),
		txPut(marker("x", "E1"), store.IfAbsent()),
		txPut(plain, store.Cond{}),
	))
	wantStored(t, b, event("1"))
	wantStored(t, b, row("2", "6"))
	wantStored(t, b, marker("x", "E1"))
	wantStored(t, b, keyed(part, "plain", store.Item{"n": num("1.5")}))
}

// checkTransactConditions: a failed condition cancels every put, and the error holds the
// stored item of each put whose condition failed (DynamoDB Local 3.3.2 experiments E3, E5,
// E6 of the M6 research).
func (s suite) checkTransactConditions(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	mustPut(t, b, row("1", "3"), store.IfAbsent())
	mustPut(t, b, marker("taken", "E0"), store.IfAbsent())

	t.Run("stale row", func(t *testing.T) {
		const stale = 5
		err := b.Transact(t.Context(), tx(
			txPut(event("5"), store.IfAbsent()),
			txPut(row("6", "6"), store.IfVersion(stale)),
			txPut(marker("y", "E5"), store.IfAbsent()),
		))
		wantTxError(t, err, nil, row("1", "3"), nil)
		wantAbsent(t, b, event("5"), marker("y", "E5"))
		wantStored(t, b, row("1", "3"))
	})
	t.Run("present marker", func(t *testing.T) {
		err := b.Transact(t.Context(), tx(
			txPut(event("6"), store.IfAbsent()),
			txPut(row("2", "9"), store.IfVersion(1)),
			txPut(marker("taken", "E6"), store.IfAbsent()),
		))
		wantTxError(t, err, nil, nil, marker("taken", "E0"))
		wantAbsent(t, b, event("6"))
		wantStored(t, b, row("1", "3"))
		wantStored(t, b, marker("taken", "E0"))
	})
	t.Run("absent row", func(t *testing.T) {
		gone := keyed(part, "gone", store.Item{catalog.AttrRowVersion: num("2")})
		err := b.Transact(t.Context(), tx(txPut(event("7"), store.IfAbsent()), txPut(gone, store.IfVersion(1))))
		wantTxError(t, err, nil, "absent")
		wantAbsent(t, b, event("7"), gone)
	})
	t.Run("resent without its token", func(t *testing.T) {
		applied := tx(
			txPut(event("3"), store.IfAbsent()),
			txPut(row("2", "6"), store.IfVersion(1)),
			txPut(marker("z", "E3"), store.IfAbsent()),
		)
		mustTransact(t, b, applied)
		err := b.Transact(t.Context(), applied)
		wantTxError(t, err, event("3"), row("2", "6"), marker("z", "E3"))
	})
}

// checkTransactRefusals: what CheckTransact refuses is never sent.
func (s suite) checkTransactRefusals(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	many := make([]store.TxPut, store.MaxTransactItems+1)
	for i := range many {
		many[i] = txPut(keyed(part, fmt.Sprintf("many#%03d", i), nil), store.IfAbsent())
	}
	big := make([]store.TxPut, store.MaxTransactSize/store.MaxItemSize+1)
	for i := range big {
		pad := strings.Repeat("x", store.MaxItemSize-len("pkpskbig#00x"))
		big[i] = txPut(keyed(part, fmt.Sprintf("big#%02d", i), store.Item{"x": str(pad)}), store.Cond{})
	}
	over := keyed(part, "over", store.Item{"x": str(strings.Repeat("x", store.MaxItemSize))})
	tests := []struct {
		name     string
		puts     []store.TxPut
		tooLarge bool // else invalid
	}{
		{"no puts", nil, false},
		{"over the put limit", many, false},
		{"two puts on one item", tx(
			txPut(event("1"), store.IfAbsent()),
			txPut(event("1"), store.Cond{}),
		), false},
		{"a put that isn't valid", tx(
			txPut(event("2"), store.IfAbsent()),
			txPut(keyed(part, "bad", store.Item{"s": str("\xff")}), store.Cond{}),
		), false},
		{"an item over the limit", tx(txPut(event("3"), store.IfAbsent()), txPut(over, store.Cond{})), true},
		{"puts over the size limit", big, true},
	}
	for _, tt := range tests {
		err := b.Transact(t.Context(), tt.puts)
		if errors.Is(err, store.ErrTooLarge) != tt.tooLarge || errors.Is(err, store.ErrInvalidRequest) == tt.tooLarge {
			t.Errorf("%s: error = %v, want ErrTooLarge %v, else ErrInvalidRequest", tt.name, err, tt.tooLarge)
		}
	}
	if got := readSKs(t, b, store.Query{PK: part}); got != nil {
		t.Errorf("refused transactions stored items %q", got)
	}
}

func (s suite) checkTransactCopies(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	original := func() store.Item {
		return keyed(part, "a", store.Item{"l": list(str("x")), "m": {Type: store.M, Map: map[string]store.Value{"k": str("v")}}})
	}
	mutate := func(it store.Item) {
		it["l"].List[0] = str("changed")
		it["m"].Map["k"] = str("changed")
		it["new"] = str("added")
	}
	puts := tx(txPut(original(), store.IfAbsent()))
	mustTransact(t, b, puts)
	mutate(puts[0].Item)
	wantStored(t, b, original())

	err := b.Transact(t.Context(), tx(txPut(original(), store.IfAbsent())))
	txErr, ok := errors.AsType[*store.TxError](err)
	if !ok || txErr.Conds[0] == nil {
		t.Fatalf("Transact over a stored item = %v, want a *store.TxError", err)
	}
	mutate(txErr.Conds[0].Current)
	wantStored(t, b, original())
}

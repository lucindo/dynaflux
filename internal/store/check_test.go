// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/store"
)

func TestCheckTransact(t *testing.T) {
	t.Parallel()
	str := func(s string) store.Value { return store.Value{Type: store.S, Str: s} }
	// sized returns an item with key sk of exactly size bytes by ItemSize.
	sized := func(sk string, size int) store.Item {
		pad := size - len("pk"+"p"+"sk"+sk+"x")
		return store.Item{"pk": str("p"), "sk": str(sk), "x": str(strings.Repeat("x", pad))}
	}
	puts := func(n int, table func(int) string, item func(int) store.Item) []store.TxPut {
		out := make([]store.TxPut, n)
		for i := range out {
			out[i] = store.TxPut{Table: table(i), Item: item(i), Cond: store.IfAbsent()}
		}
		return out
	}
	oneTable := func(int) string { return "t" }
	distinct := func(i int) store.Item { return sized(fmt.Sprintf("%03d", i), 10) }
	// Ten items at the item limit and one of the rest fill MaxTransactSize exactly.
	filling := func(extra int) func(int) store.Item {
		return func(i int) store.Item {
			if i < 10 {
				return sized(fmt.Sprintf("%03d", i), store.MaxItemSize)
			}
			return sized("rest", store.MaxTransactSize-10*store.MaxItemSize+extra)
		}
	}
	tests := []struct {
		name string
		puts []store.TxPut
		want error // nil for none
	}{
		{"one put", puts(1, oneTable, distinct), nil},
		{"at the put limit", puts(store.MaxTransactItems, oneTable, distinct), nil},
		{"no puts", nil, store.ErrInvalidRequest},
		{"over the put limit", puts(store.MaxTransactItems+1, oneTable, distinct), store.ErrInvalidRequest},
		{"one key in two tables", puts(2, func(i int) string { return fmt.Sprint("t", i) }, func(int) store.Item { return sized("a", 10) }), nil},
		{"one item twice", puts(2, oneTable, func(int) store.Item { return sized("a", 10) }), store.ErrInvalidRequest},
		{"a put that isn't valid", puts(2, oneTable, func(i int) store.Item {
			return store.Item{"pk": str("p"), "sk": str(strconv.Itoa(i)), "s": str("\xff")}
		}), store.ErrInvalidRequest},
		{"an item over the limit", puts(1, oneTable, func(int) store.Item { return sized("a", store.MaxItemSize+1) }), store.ErrTooLarge},
		{"at the size limit", puts(11, oneTable, filling(0)), nil},
		{"over the size limit", puts(11, oneTable, filling(1)), store.ErrTooLarge},
	}
	for _, tt := range tests {
		err := store.CheckTransact(tt.puts)
		if tt.want == nil && err != nil || tt.want != nil && !errors.Is(err, tt.want) {
			t.Errorf("%s: CheckTransact = %v, want %v", tt.name, err, tt.want)
		}
	}
}

func TestTxErrorNamesFailedPuts(t *testing.T) {
	t.Parallel()
	err := &store.TxError{Conds: []*store.ConditionError{nil, {}, nil, {}}}
	if got, want := err.Error(), "store: condition failed: transaction puts [1 3]"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, store.ErrConditionFailed) {
		t.Error("a TxError doesn't match ErrConditionFailed")
	}
}

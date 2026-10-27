// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package storetest

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

type request struct {
	name string
	do   func(context.Context, store.Backend) error
}

func get(k store.Key) func(context.Context, store.Backend) error {
	return func(ctx context.Context, b store.Backend) error {
		_, err := b.Get(ctx, Table, k, true)
		return err
	}
}

func batchGet(ks ...store.Key) func(context.Context, store.Backend) error {
	return func(ctx context.Context, b store.Backend) error {
		_, err := b.BatchGet(ctx, Table, ks, true)
		return err
	}
}

func query(q store.Query) func(context.Context, store.Backend) error {
	return func(ctx context.Context, b store.Backend) error {
		for _, err := range b.Query(ctx, Table, q) {
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func scan(s store.Scan, start *store.Key) func(context.Context, store.Backend) error {
	return func(ctx context.Context, b store.Backend) error {
		_, _, err := b.ScanPage(ctx, Table, s, start)
		return err
	}
}

func put(it store.Item, c store.Cond) func(context.Context, store.Backend) error {
	return func(ctx context.Context, b store.Backend) error { return b.Put(ctx, Table, it, c) }
}

func invalidLookups() []request {
	longPK := strings.Repeat("k", store.MaxPKLen+1)
	longSK := strings.Repeat("k", store.MaxSKLen+1)
	tooMany := make([]store.Key, store.MaxBatchGetKeys+1)
	for i := range tooMany {
		tooMany[i] = store.Key{PK: part, SK: strings.Repeat("k", i+1)}
	}
	return []request{
		{"get with an empty pk", get(store.Key{SK: "a"})},
		{"get with an empty sk", get(store.Key{PK: part})},
		{"get with a pk over the limit", get(store.Key{PK: longPK, SK: "a"})},
		{"get with an sk over the limit", get(store.Key{PK: part, SK: longSK})},
		{"get with a key that isn't UTF-8", get(store.Key{PK: "p\xff", SK: "a"})},
		{"batch get over the key limit", batchGet(tooMany...)},
		{"batch get with an sk over the limit", batchGet(store.Key{PK: part, SK: "a"}, store.Key{PK: part, SK: longSK})},
		{"query with an empty pk", query(store.Query{})},
		{"query with a pk over the limit", query(store.Query{PK: longPK})},
		{"query with a bound over the limit", query(store.Query{PK: part, From: longSK})},
		{"query with a bound that isn't UTF-8", query(store.Query{PK: part, To: "\xc3"})},
		{"query with crossed bounds", query(store.Query{PK: part, From: "b", To: "a"})},
		{"query with an exclusive bound and From", query(store.Query{PK: part, From: "a", To: "b", ToExclusive: true})},
		{"query with an exclusive bound and no To", query(store.Query{PK: part, ToExclusive: true})},
		{"query with a negative limit", query(store.Query{PK: part, Limit: -1})},
		{"query with a negative page size", query(store.Query{PK: part, PageSize: -1})},
		{"scan of no segments", scan(store.Scan{}, nil)},
		{"scan over the segment limit", scan(store.Scan{Segments: store.MaxSegments + 1}, nil)},
		{"scan of a negative segment", scan(store.Scan{Segment: -1, Segments: 1}, nil)},
		{"scan of a segment past the last", scan(store.Scan{Segment: 1, Segments: 1}, nil)},
		{"scan with a negative page size", scan(store.Scan{Segments: 1, PageSize: -1}, nil)},
		{"scan from a key with an empty sk", scan(store.Scan{Segments: 1}, &store.Key{PK: part})},
		{"scan from a key that isn't UTF-8", scan(store.Scan{Segments: 1}, &store.Key{PK: "p\xff", SK: "a"})},
	}
}

func invalidItems() []request {
	bad := func(attrs store.Item) store.Item { return keyed(part, "bad", attrs) }
	badNumber := func(n string) request {
		return request{"number " + n, put(bad(store.Item{"n": num(n)}), store.Cond{})}
	}
	reqs := []request{
		badNumber("123456789012345678901234567890123456789"), // 39 significant digits
		badNumber("0." + strings.Repeat("0", -minExp) + "1"),
		badNumber("1E-131"),
		badNumber("1E+126"),
		badNumber("-1E+126"),
		{"string that isn't UTF-8", put(bad(store.Item{"s": str("\xff")}), store.Cond{})},
		{"list element that isn't UTF-8", put(bad(store.Item{"l": list(str("\xc3"))}), store.Cond{})},
		{"bad number in a map", put(bad(store.Item{"m": {Type: store.M, Map: map[string]store.Value{"n": num("1e999")}}}), store.Cond{})},
		{"NULL", put(bad(store.Item{"z": {}}), store.Cond{})},
		{"unknown type", put(bad(store.Item{"z": {Type: store.M + 1}}), store.Cond{})},
		{"empty attribute name", put(bad(store.Item{"": str("x")}), store.Cond{})},
		{"no pk", put(store.Item{catalog.AttrSK: str("bad")}, store.Cond{})},
		{"no sk", put(store.Item{catalog.AttrPK: str(part)}, store.Cond{})},
		{"numeric pk", put(store.Item{catalog.AttrPK: num("1"), catalog.AttrSK: str("bad")}, store.Cond{})},
		{"empty pk", put(keyed("", "bad", nil), store.Cond{})},
		{"empty sk", put(keyed(part, "", nil), store.Cond{})},
		{"version 0", put(bad(nil), store.IfVersion(0))},
	}
	for _, n := range []string{"", "abc", "1e", "1e+", "--1", "NaN", "Infinity", "0x10", "1 ", "1_000"} {
		reqs = append(reqs, badNumber(n))
	}
	return reqs
}

func (s suite) checkInvalidRequests(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	for _, r := range slices.Concat(invalidLookups(), invalidItems()) {
		err := r.do(t.Context(), b)
		if !errors.Is(err, store.ErrInvalidRequest) || errors.Is(err, store.ErrTooLarge) {
			t.Errorf("%s: error = %v, want ErrInvalidRequest", r.name, err)
		}
	}
	if got := readSKs(t, b, store.Query{PK: part}); got != nil {
		t.Errorf("rejected puts stored items %q", got)
	}
}

func (s suite) checkTooLarge(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	// sized returns an item of exactly size bytes by the documented rules: the names and
	// values of pk, sk and x.
	sized := func(sk string, size int) store.Item {
		pad := size - len(catalog.AttrPK+part) - len(catalog.AttrSK+sk) - len("x")
		return keyed(part, sk, store.Item{"x": str(strings.Repeat("x", pad))})
	}
	tooLarge := []request{
		{"item over the limit", put(sized("over", store.MaxItemSize+1), store.Cond{})},
		{"pk over the limit", put(keyed(strings.Repeat("k", store.MaxPKLen+1), "a", nil), store.Cond{})},
		{"sk over the limit", put(keyed(part, strings.Repeat("k", store.MaxSKLen+1), nil), store.IfAbsent())},
	}
	for _, r := range tooLarge {
		err := r.do(t.Context(), b)
		if !errors.Is(err, store.ErrTooLarge) || errors.Is(err, store.ErrInvalidRequest) {
			t.Errorf("%s: error = %v, want ErrTooLarge", r.name, err)
		}
	}
	if got := readSKs(t, b, store.Query{PK: part}); got != nil {
		t.Errorf("rejected puts stored items %q", got)
	}

	// ItemSize is an upper bound, so an item a little under it fits in every backend.
	const slack = 1 << 10
	fits := sized("fits", store.MaxItemSize-slack)
	mustPut(t, b, fits, store.Cond{})
	wantStored(t, b, fits)

	maxPK, maxSK := strings.Repeat("k", store.MaxPKLen), strings.Repeat("s", store.MaxSKLen)
	atLimits := keyed(maxPK, maxSK, nil)
	mustPut(t, b, atLimits, store.IfAbsent())
	wantStored(t, b, atLimits)
	if got := readSKs(t, b, store.Query{PK: maxPK, From: maxSK, To: maxSK}); !slices.Equal(got, []string{maxSK}) {
		t.Errorf("Query with keys at their limits read %d items, want 1", len(got))
	}
}

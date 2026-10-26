// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:mnd // Query fixtures: counts as in a _test.go file, which mnd skips.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

func putKeys(t *testing.T, b store.Backend, pk string, sks ...string) {
	t.Helper()
	for _, sk := range sks {
		mustPut(t, b, keyed(pk, sk, nil), store.Cond{})
	}
}

// readSKs runs q and returns the sort keys it yields.
func readSKs(t *testing.T, b store.Backend, q store.Query) []string {
	t.Helper()
	var sks []string
	for it, err := range b.Query(t.Context(), Table, q) {
		if err != nil {
			t.Fatalf("Query(%+v): %v", q, err)
		}
		sks = append(sks, it[catalog.AttrSK].Str)
	}
	return sks
}

func (s suite) checkSortKeyByteOrder(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	want := []string{
		"#",
		"%",
		"A",
		"Z",
		"a",
		"a#b",
		"a$",
		"aa",
		"z",
		"\u00bf",     // C2 BF
		"\u00e9",     // C3 A9
		"\uff61",     // EF BD A1
		"\U0001f600", // F0 9F 98 80: above U+FF61 in UTF-8, below it in UTF-16
	}
	shuffled := slices.Clone(want)
	slices.Reverse(shuffled)
	shuffled[0], shuffled[4] = shuffled[4], shuffled[0]
	putKeys(t, b, part, shuffled...)

	if got := readSKs(t, b, store.Query{PK: part}); !slices.Equal(got, want) {
		t.Errorf("ascending order = %q, want %q", got, want)
	}
	slices.Reverse(want)
	if got := readSKs(t, b, store.Query{PK: part, Desc: true}); !slices.Equal(got, want) {
		t.Errorf("descending order = %q, want %q", got, want)
	}
}

func (s suite) checkQueryBounds(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	putKeys(t, b, part, "a", "b", "b#", "c", "d")
	putKeys(t, b, part+part, "a")
	putKeys(t, b, "o", "b")
	tests := []struct {
		name string
		q    store.Query
		want []string
	}{
		{"whole partition", store.Query{}, []string{"a", "b", "b#", "c", "d"}},
		{"from", store.Query{From: "b"}, []string{"b", "b#", "c", "d"}},
		{"to", store.Query{To: "c"}, []string{"a", "b", "b#", "c"}},
		{"between", store.Query{From: "b", To: "c"}, []string{"b", "b#", "c"}},
		{"between one key", store.Query{From: "b", To: "b"}, []string{"b"}},
		{"bounds between keys", store.Query{From: "aa", To: "bz"}, []string{"b", "b#"}},
		{"from past a prefix", store.Query{From: "b$"}, []string{"c", "d"}},
		{"to past a prefix", store.Query{To: "b$"}, []string{"a", "b", "b#"}},
		{"exclusive to", store.Query{To: "c", ToExclusive: true}, []string{"a", "b", "b#"}},
		{"descending between", store.Query{From: "b", To: "c", Desc: true}, []string{"c", "b#", "b"}},
		{"descending exclusive", store.Query{To: "c", ToExclusive: true, Desc: true}, []string{"b#", "b", "a"}},
		{"pages", store.Query{PageSize: 1}, []string{"a", "b", "b#", "c", "d"}},
		{"descending pages", store.Query{PageSize: 1, Desc: true}, []string{"d", "c", "b#", "b", "a"}},
		{"pages between", store.Query{From: "b", To: "c", PageSize: 1}, []string{"b", "b#", "c"}},
		{"descending pages between", store.Query{From: "b", To: "c", PageSize: 1, Desc: true}, []string{"c", "b#", "b"}},
		{"limit", store.Query{Limit: 2}, []string{"a", "b"}},
		{"descending limit", store.Query{Desc: true, Limit: 2}, []string{"d", "c"}},
		{"empty range", store.Query{From: "e"}, nil},
		{"absent partition", store.Query{PK: "q"}, nil},
	}
	for _, tt := range tests {
		if tt.q.PK == "" {
			tt.q.PK = part
		}
		if got := readSKs(t, b, tt.q); !slices.Equal(got, tt.want) {
			t.Errorf("%s: Query(%+v) = %q, want %q", tt.name, tt.q, got, tt.want)
		}
	}
}

// checkQueryPages observes pages through a context canceled while the caller reads: the
// items of the current page still arrive, and the next request fails.
func (s suite) checkQueryPages(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	var sks []string
	for i := range 10 {
		sks = append(sks, fmt.Sprintf("k%02d", i))
	}
	putKeys(t, b, part, sks...)
	tests := []struct {
		name        string
		q           store.Query
		cancelAfter int
		want        []string
		wantErr     bool
	}{
		{"first page has PageSize items", store.Query{PageSize: 1}, 1, sks[:1], true},
		{"second page doubles", store.Query{PageSize: 1}, 2, sks[:3], true},
		{"descending pages", store.Query{PageSize: 2, Desc: true}, 1, []string{"k09", "k08"}, true},
		{"limit counted across pages", store.Query{PageSize: 2, Limit: 5}, 5, sks[:5], false},
		{"limit without page size", store.Query{Limit: 3}, 3, sks[:3], false},
	}
	for _, tt := range tests {
		tt.q.PK = part
		got, err := readUntilCancel(t, b, tt.q, tt.cancelAfter)
		if !slices.Equal(got, tt.want) || (err != nil) != tt.wantErr || err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("%s: read %q and error %v, want %q and an error: %v", tt.name, got, err, tt.want, tt.wantErr)
		}
	}

	n := 0
	for _, err := range b.Query(t.Context(), Table, store.Query{PK: part, PageSize: 1}) {
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		if n++; n == 2 {
			break
		}
	}
	if n != 2 {
		t.Errorf("read %d items before break, want 2", n)
	}
}

func readUntilCancel(t *testing.T, b store.Backend, q store.Query, cancelAfter int) ([]string, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var sks []string
	for it, err := range b.Query(ctx, Table, q) {
		if err != nil {
			return sks, err
		}
		if sks = append(sks, it[catalog.AttrSK].Str); len(sks) == cancelAfter {
			cancel()
		}
	}
	return sks, nil
}

// checkQueryWhileWriting writes while a Query is open: the next page sees the write.
func (s suite) checkQueryWhileWriting(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	putKeys(t, b, part, "a", "c")
	var got []string
	for it, err := range b.Query(t.Context(), Table, store.Query{PK: part, PageSize: 1, Consistent: true}) {
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		if got = append(got, it[catalog.AttrSK].Str); len(got) == 1 {
			putKeys(t, b, part, "b")
		}
	}
	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("read %q, want %q", got, want)
	}
}

func (s suite) checkBatchGet(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	a := keyed(part, "a", store.Item{"n": num("1")})
	bb := keyed(part, "b", store.Item{"n": num("2")})
	mustPut(t, b, a, store.Cond{})
	mustPut(t, b, bb, store.Cond{})
	ks := []store.Key{keyOf(a), {PK: part, SK: "missing"}, keyOf(a), keyOf(bb)}
	got, err := b.BatchGet(t.Context(), Table, ks, true)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	want := []store.Item{a, nil, a, bb}
	if !slices.EqualFunc(got, want, equalItem) {
		t.Fatalf("BatchGet = %v, want %v", got, want)
	}
	got[0]["n"] = str("changed")
	if !equalItem(got[2], a) {
		t.Errorf("results for a repeated key share storage: %s, want %s", show(got[2]), show(a))
	}

	missing := make([]store.Key, store.MaxBatchGetKeys)
	for i := range missing {
		missing[i] = store.Key{PK: "q", SK: strconv.Itoa(i)}
	}
	got, err = b.BatchGet(t.Context(), Table, missing, true)
	if err != nil || len(got) != len(missing) || slices.ContainsFunc(got, func(it store.Item) bool { return it != nil }) {
		t.Errorf("BatchGet of %d missing keys = %v, %v; want %d nils", len(missing), got, err, len(missing))
	}
	if got, err = b.BatchGet(t.Context(), Table, nil, true); err != nil || len(got) != 0 {
		t.Errorf("BatchGet of no keys = %v, %v; want nothing", got, err)
	}
}

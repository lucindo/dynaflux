// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:mnd // Scan fixtures: counts as in a _test.go file, which mnd skips.
package storetest

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/store"
)

// putTable stores two items in each of n partitions and returns them.
func putTable(t *testing.T, b store.Backend, n int) []store.Item {
	t.Helper()
	var items []store.Item
	for i := range n {
		for _, sk := range []string{"a", "b"} {
			it := keyed(fmt.Sprintf("p%02d", i), sk, store.Item{"v": str(fmt.Sprintf("%d%s", i, sk))})
			mustPut(t, b, it, store.Cond{})
			items = append(items, it)
		}
	}
	return items
}

func onceEach(items []store.Item) map[store.Key]int {
	m := make(map[store.Key]int, len(items))
	for _, it := range items {
		m[keyOf(it)] = 1
	}
	return m
}

// scanAll reads s from start to its last page, and returns the items and each page's size.
func scanAll(t *testing.T, b store.Backend, s store.Scan, start *store.Key) ([]store.Item, []int) {
	t.Helper()
	var items []store.Item
	var sizes []int
	for {
		page, next, err := b.ScanPage(t.Context(), Table, s, start)
		if err != nil {
			t.Fatalf("ScanPage(%+v, %v): %v", s, start, err)
		}
		items = append(items, page...)
		sizes = append(sizes, len(page))
		if next == nil {
			return items, sizes
		}
		start = next
	}
}

// byKey sorts items by key, since a scan's order is the backend's.
func byKey(items []store.Item) []store.Item {
	return slices.SortedFunc(slices.Values(items), func(a, b store.Item) int {
		ka, kb := keyOf(a), keyOf(b)
		return cmp.Or(strings.Compare(ka.PK, kb.PK), strings.Compare(ka.SK, kb.SK))
	})
}

func (s suite) checkScan(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	whole := store.Scan{Segments: 1, Consistent: true}
	if items, sizes := scanAll(t, b, whole, nil); len(items) != 0 || !slices.Equal(sizes, []int{0}) {
		t.Errorf("scan of an empty table read %d items in pages %v, want one empty page", len(items), sizes)
	}

	nested := keyed("q", "a", store.Item{"l": list(str("x"), num("2"))})
	mustPut(t, b, nested, store.Cond{})
	want := append(putTable(t, b, 2), nested)
	tests := []struct {
		name      string
		pageSize  int
		wantSizes []int
	}{
		{"one page", 0, []int{5}},
		{"pages of PageSize items", 2, []int{2, 2, 1}},
	}
	for _, tt := range tests {
		s := whole
		s.PageSize = tt.pageSize
		items, sizes := scanAll(t, b, s, nil)
		if !slices.EqualFunc(byKey(items), byKey(want), equalItem) || !slices.Equal(sizes, tt.wantSizes) {
			t.Errorf("%s: scan read %d items in pages %v, want the %d stored in pages %v", tt.name, len(items), sizes, len(want), tt.wantSizes)
		}
	}
}

// checkScanResume resumes a scan from a key an earlier page returned, as a new worker
// does from the key a job stored: twice, as duplicate workers do.
func (s suite) checkScanResume(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	want := putTable(t, b, 3)
	sc := store.Scan{Segments: 1, Consistent: true, PageSize: 2}
	first, next, err := b.ScanPage(t.Context(), Table, sc, nil)
	if err != nil || next == nil {
		t.Fatalf("first ScanPage = %d items, next %v, %v; want a key to continue from", len(first), next, err)
	}
	rest, _ := scanAll(t, b, sc, next)
	again, _ := scanAll(t, b, sc, new(*next))
	if got := slices.Concat(first, rest); !slices.EqualFunc(byKey(got), byKey(want), equalItem) {
		t.Errorf("first page and the pages from its key read %d items, want the %d stored, each once", len(got), len(want))
	}
	if !slices.EqualFunc(again, rest, equalItem) {
		t.Errorf("a second resume from the same key read %d items, want the first resume's %d", len(again), len(rest))
	}
}

// checkScanSegments reads every segment of a scan: each item is in exactly one.
func (s suite) checkScanSegments(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	want := onceEach(putTable(t, b, 40))
	for _, n := range []int{2, 5} {
		seen := map[store.Key]int{}
		for i := range n {
			items, _ := scanAll(t, b, store.Scan{Segment: i, Segments: n, PageSize: 3}, nil)
			for _, it := range items {
				seen[keyOf(it)]++
			}
		}
		if !maps.Equal(seen, want) {
			t.Errorf("%d segments read each key this many times: %v, want each of %d keys once", n, seen, len(want))
		}
	}
}

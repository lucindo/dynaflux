// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package mem_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(*testing.T) store.Backend { return mem.New() })
}

func item(sk string, attrs store.Item) store.Item {
	it := store.Item{
		catalog.AttrPK: {Type: store.S, Str: "p"},
		catalog.AttrSK: {Type: store.S, Str: sk},
	}
	maps.Copy(it, attrs)
	return it
}

// Dynaflux never writes exponents, but DynamoDB accepts them, so mem must too.
func TestExponentsStoredAsPlainDecimals(t *testing.T) {
	t.Parallel()
	tests := []struct{ in, want string }{
		{"1E+2", "100"},
		{"1.5e3", "1500"},
		{"-2.50E-3", "-0.0025"},
		{"125e-2", "1.25"},
		{"0e5", "0"},
		{"1E-130", "0." + strings.Repeat("0", 129) + "1"},
		{"9.9999999999999999999999999999999999999E+125", strings.Repeat("9", 38) + strings.Repeat("0", 88)},
	}
	m := mem.New()
	for _, tt := range tests {
		if err := m.Put(t.Context(), "t", item("a", store.Item{"n": {Type: store.N, Str: tt.in}}), store.Cond{}); err != nil {
			t.Errorf("Put of %s: %v", tt.in, err)
			continue
		}
		got, err := m.Get(t.Context(), "t", store.Key{PK: "p", SK: "a"}, true)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got["n"].Str != tt.want {
			t.Errorf("Put of %s stored %s, want %s", tt.in, got["n"].Str, tt.want)
		}
	}
}

// mem rejects forms DynamoDB may accept but Dynaflux never writes, so such a write fails
// in tests rather than reaching DynamoDB.
func TestRejectsUnusedForms(t *testing.T) {
	t.Parallel()
	attrs := []store.Item{
		{"n": {Type: store.N, Str: ".5"}},
		{"n": {Type: store.N, Str: "5."}},
		{"n": {Type: store.N, Str: "+5"}},
		{"m": {Type: store.M, Map: map[string]store.Value{"": {Type: store.S, Str: "x"}}}},
	}
	m := mem.New()
	for _, a := range attrs {
		if err := m.Put(t.Context(), "t", item("a", a), store.Cond{}); !errors.Is(err, store.ErrInvalidRequest) {
			t.Errorf("Put of %v: error = %v, want ErrInvalidRequest", a, err)
		}
	}
}

func TestItemAtSizeLimitAccepted(t *testing.T) {
	t.Parallel()
	pad := store.MaxItemSize - len("pk"+"p") - len("sk"+"a") - len("x")
	m := mem.New()
	if err := m.Put(t.Context(), "t", item("a", store.Item{"x": {Type: store.S, Str: strings.Repeat("x", pad)}}), store.Cond{}); err != nil {
		t.Errorf("Put of an item of exactly %d bytes: %v", store.MaxItemSize, err)
	}
}

func TestTablesAreSeparate(t *testing.T) {
	t.Parallel()
	m := mem.New()
	if err := m.Put(t.Context(), "t1", item("a", nil), store.IfAbsent()); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := m.Put(t.Context(), "t2", item("a", nil), store.IfAbsent()); err != nil {
		t.Errorf("IfAbsent Put of a key that exists only in another table: %v", err)
	}
	if page, _, err := m.ScanPage(t.Context(), "t3", store.Scan{Segments: 1}, nil); err != nil || len(page) != 0 {
		t.Errorf("ScanPage of a table never written = %d items, %v; want none", len(page), err)
	}
}

func TestDeleteRemovesTheItemFromReadsAndConditions(t *testing.T) {
	t.Parallel()
	m := mem.New()
	for _, sk := range []string{"a", "b"} {
		if err := m.Put(t.Context(), "t", item(sk, nil), store.IfAbsent()); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := m.Delete(t.Context(), "t", store.Key{PK: "p", SK: "a"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := m.Delete(t.Context(), "t", store.Key{PK: "p", SK: "a"}); err != nil {
		t.Errorf("Delete of an absent item: %v", err)
	}
	var sks []string
	for it, err := range m.Query(t.Context(), "t", store.Query{PK: "p"}) {
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		sks = append(sks, it[catalog.AttrSK].Str)
	}
	if len(sks) != 1 || sks[0] != "b" {
		t.Errorf("Query after deleting a read %q, want [b]", sks)
	}
	if err := m.Put(t.Context(), "t", item("a", nil), store.IfAbsent()); err != nil {
		t.Errorf("IfAbsent Put of a deleted key: %v", err)
	}
}

// Without a PageSize, a page ends at the item that brings it to 1 MB.
func TestQueryPagesOfOneMegabyte(t *testing.T) {
	t.Parallel()
	m := mem.New()
	big := strings.Repeat("x", 300<<10)
	for _, sk := range []string{"a", "b", "c", "d", "e", "f"} {
		if err := m.Put(t.Context(), "t", item(sk, store.Item{"x": {Type: store.S, Str: big}}), store.Cond{}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	read := 0
	var err error
	for _, err = range m.Query(ctx, "t", store.Query{PK: "p"}) {
		if err != nil {
			break
		}
		if read++; read == 1 {
			cancel()
		}
	}
	if read != 4 || !errors.Is(err, context.Canceled) {
		t.Errorf("read %d items, then error %v; want 4 items, then context.Canceled", read, err)
	}
}

// sks returns the sort keys of items.
func sks(items []store.Item) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it[catalog.AttrSK].Str
	}
	return out
}

// A scan reads the snapshot its first page took, so it misses every later write, the most
// DynamoDB allows; a stale worker resuming from an older key reads the same snapshot.
func TestScanMissesWritesAfterItsFirstPage(t *testing.T) {
	t.Parallel()
	m := mem.New()
	put := func(sks ...string) {
		for _, sk := range sks {
			if err := m.Put(t.Context(), "t", item(sk, nil), store.Cond{}); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
	}
	put("b", "d", "f")
	s := store.Scan{Segments: 1, PageSize: 1}
	first, next, err := m.ScanPage(t.Context(), "t", s, nil)
	if err != nil || next == nil {
		t.Fatalf("first ScanPage = %q, %v, %v; want a key to continue from", sks(first), next, err)
	}
	put("a", "c", "e", "g")
	readFrom := func(start *store.Key) []string {
		var got []string
		for start != nil {
			var page []store.Item
			if page, start, err = m.ScanPage(t.Context(), "t", s, start); err != nil {
				t.Fatalf("ScanPage: %v", err)
			}
			got = append(got, sks(page)...)
		}
		return got
	}
	if got := append(sks(first), readFrom(next)...); !slices.Equal(got, []string{"b", "d", "f"}) {
		t.Errorf("scan read %q, want [b d f]: the items stored before its first page", got)
	}
	if got := readFrom(next); !slices.Equal(got, []string{"d", "f"}) {
		t.Errorf("a second resume from the first key read %q, want [d f]", got)
	}
}

// Without a PageSize, a scan's page ends at the item that brings it to 1 MB.
func TestScanPagesOfOneMegabyte(t *testing.T) {
	t.Parallel()
	m := mem.New()
	big := strings.Repeat("x", 300<<10)
	for _, sk := range []string{"a", "b", "c", "d", "e", "f"} {
		if err := m.Put(t.Context(), "t", item(sk, store.Item{"x": {Type: store.S, Str: big}}), store.Cond{}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	page, next, err := m.ScanPage(t.Context(), "t", store.Scan{Segments: 1}, nil)
	if err != nil || len(page) != 4 || next == nil {
		t.Errorf("ScanPage = %d items, next %v, %v; want 4 items and a key to continue from", len(page), next, err)
	}
}

// Scans of one segment share one snapshot, so many paged scans hold one table copy, not one
// per page.
func TestScansKeepOneSnapshotPerSegment(t *testing.T) {
	t.Parallel()
	m := mem.New()
	for _, sk := range []string{"a", "b", "c"} {
		if err := m.Put(t.Context(), "t", item(sk, nil), store.Cond{}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	for range 10 {
		var start *store.Key
		for {
			_, next, err := m.ScanPage(t.Context(), "t", store.Scan{Segments: 1, PageSize: 1}, start)
			if err != nil {
				t.Fatalf("ScanPage: %v", err)
			}
			if next == nil {
				break
			}
			start = next
		}
	}
	if n := m.Snapshots(); n != 1 {
		t.Errorf("%d snapshots after 10 scans of one segment, want 1", n)
	}
}

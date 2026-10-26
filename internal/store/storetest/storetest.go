// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package storetest holds the conformance suite that every [store.Backend] passes, and the
// fault of an SDK retry after a lost response.
package storetest

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

// Table is the table the suite uses. newBackend must return a backend in which it exists
// and is empty, with the string keys pk and sk.
const Table = "storetest"

// Run runs the conformance suite, with a new backend for each check.
func Run(t *testing.T, newBackend func(t *testing.T) store.Backend) {
	t.Helper()
	s := suite{newBackend}
	t.Run("PutReplacesItem", s.checkPutReplacesItem)
	t.Run("IfAbsent", s.checkIfAbsent)
	t.Run("IfVersion", s.checkIfVersion)
	t.Run("ConcurrentVersionedWrites", s.checkConcurrentVersionedWrites)
	t.Run("RetriedWrite", s.checkRetriedWrite)
	t.Run("CanceledContext", s.checkCanceledContext)
	t.Run("SortKeyByteOrder", s.checkSortKeyByteOrder)
	t.Run("QueryBounds", s.checkQueryBounds)
	t.Run("QueryPages", s.checkQueryPages)
	t.Run("QueryWhileWriting", s.checkQueryWhileWriting)
	t.Run("BatchGet", s.checkBatchGet)
	t.Run("NumberForm", s.checkNumberForm)
	t.Run("DeepCopies", s.checkDeepCopies)
	t.Run("InvalidRequests", s.checkInvalidRequests)
	t.Run("TooLarge", s.checkTooLarge)
}

// suite gives each check, a parallel subtest, a backend of its own.
type suite struct {
	newBackend func(t *testing.T) store.Backend
}

// RetriedWrite wraps b so that every conditional Put is applied and then fails with a
// *[store.ConditionError] holding the item now stored: what the SDK reports when it
// retries a write whose first attempt succeeded but whose response was lost.
func RetriedWrite(b store.Backend) store.Backend { return retried{b} }

type retried struct{ store.Backend }

func (r retried) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if err := r.Backend.Put(ctx, table, it, c); err != nil || c == (store.Cond{}) {
		return err
	}
	cur, err := r.Backend.Get(ctx, table, keyOf(it), true)
	if err != nil {
		return fmt.Errorf("storetest: read after a retried write: %w", err)
	}
	return &store.ConditionError{Current: cur}
}

const part = "p" // the partition most checks use

func str(s string) store.Value { return store.Value{Type: store.S, Str: s} }

func num(s string) store.Value { return store.Value{Type: store.N, Str: s} }

func list(vs ...store.Value) store.Value { return store.Value{Type: store.L, List: vs} }

// keyed returns an item with the given key and attributes.
func keyed(pk, sk string, attrs store.Item) store.Item {
	it := store.Item{catalog.AttrPK: str(pk), catalog.AttrSK: str(sk)}
	maps.Copy(it, attrs)
	return it
}

func keyOf(it store.Item) store.Key {
	return store.Key{PK: it[catalog.AttrPK].Str, SK: it[catalog.AttrSK].Str}
}

func mustPut(t *testing.T, b store.Backend, it store.Item, c store.Cond) {
	t.Helper()
	if err := b.Put(t.Context(), Table, it, c); err != nil {
		t.Fatalf("Put(%s): %v", show(it), err)
	}
}

func mustGet(t *testing.T, b store.Backend, k store.Key) store.Item {
	t.Helper()
	it, err := b.Get(t.Context(), Table, k, true)
	if err != nil {
		t.Fatalf("Get(%v): %v", k, err)
	}
	return it
}

// wantStored fails unless the item stored under want's key equals want.
func wantStored(t *testing.T, b store.Backend, want store.Item) {
	t.Helper()
	if got := mustGet(t, b, keyOf(want)); !equalItem(got, want) {
		t.Errorf("stored item = %s, want %s", show(got), show(want))
	}
}

// equalItem compares items as DynamoDB values: an empty L, M or B equals a nil one.
func equalItem(a, b store.Item) bool {
	return (a == nil) == (b == nil) && maps.EqualFunc(a, b, equalValue)
}

func equalValue(a, b store.Value) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case store.S, store.N:
		return a.Str == b.Str
	case store.B:
		return bytes.Equal(a.Bin, b.Bin)
	case store.BOOL:
		return a.Bool == b.Bool
	case store.L:
		return slices.EqualFunc(a.List, b.List, equalValue)
	case store.M:
		return maps.EqualFunc(a.Map, b.Map, equalValue)
	}
	return false
}

func show(it store.Item) string {
	if it == nil {
		return "<absent>"
	}
	return showMap(it)
}

func showMap(m map[string]store.Value) string {
	parts := make([]string, 0, len(m))
	for _, name := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, name+":"+showValue(m[name]))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

func showValue(v store.Value) string {
	switch v.Type {
	case store.S:
		return strconv.Quote(v.Str)
	case store.N:
		return "N(" + v.Str + ")"
	case store.B:
		return fmt.Sprintf("B(%x)", v.Bin)
	case store.BOOL:
		return strconv.FormatBool(v.Bool)
	case store.L:
		parts := make([]string, len(v.List))
		for i, e := range v.List {
			parts[i] = showValue(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	case store.M:
		return showMap(v.Map)
	}
	return fmt.Sprintf("Type(%d)", v.Type)
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package storetest

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

func (s suite) checkPutReplacesItem(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	if got := mustGet(t, b, store.Key{PK: part, SK: "a"}); got != nil {
		t.Fatalf("Get of a missing item = %s, want nil", show(got))
	}
	mustPut(t, b, keyed(part, "a", store.Item{"x": str("1"), "y": {Type: store.BOOL, Bool: true}}), store.Cond{})
	replacement := keyed(part, "a", store.Item{"z": num("2")})
	mustPut(t, b, replacement, store.Cond{})
	wantStored(t, b, replacement)
}

// wantConditionFailed fails unless err is a failed condition holding want.
func wantConditionFailed(t *testing.T, err error, want store.Item) {
	t.Helper()
	var ce *store.ConditionError
	if !errors.As(err, &ce) || !errors.Is(err, store.ErrConditionFailed) {
		t.Fatalf("error = %v, want a *store.ConditionError matching ErrConditionFailed", err)
	}
	if !equalItem(ce.Current, want) {
		t.Errorf("ConditionError.Current = %s, want %s", show(ce.Current), show(want))
	}
}

func (s suite) checkIfAbsent(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	first := keyed(part, "a", store.Item{"n": str("first")})
	mustPut(t, b, first, store.IfAbsent())
	err := b.Put(t.Context(), Table, keyed(part, "a", store.Item{"n": str("second")}), store.IfAbsent())
	wantConditionFailed(t, err, first)
	wantStored(t, b, first)
}

func (s suite) checkIfVersion(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	v := func(sk, version string) store.Item {
		return keyed(part, sk, store.Item{catalog.AttrRowVersion: num(version), "n": str("at " + version)})
	}
	mustPut(t, b, v("a", "1"), store.IfAbsent())
	mustPut(t, b, v("a", "2"), store.IfVersion(1))
	err := b.Put(t.Context(), Table, v("a", "3"), store.IfVersion(1))
	wantConditionFailed(t, err, v("a", "2"))
	wantStored(t, b, v("a", "2"))

	err = b.Put(t.Context(), Table, v("absent", "2"), store.IfVersion(1))
	wantConditionFailed(t, err, nil)

	// _v = :v compares numbers: a string "1" is not the version 1.
	text := keyed(part, "text", store.Item{catalog.AttrRowVersion: str("1")})
	mustPut(t, b, text, store.Cond{})
	err = b.Put(t.Context(), Table, v("text", "2"), store.IfVersion(1))
	wantConditionFailed(t, err, text)
}

// checkConcurrentVersionedWrites runs read-modify-write loops that retry from
// ConditionError.Current without another read, as the projector does.
func (s suite) checkConcurrentVersionedWrites(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	const workers, rounds = 8, 25
	key := store.Key{PK: part, SK: "counter"}
	mustPut(t, b, keyed(part, key.SK, store.Item{catalog.AttrRowVersion: num("1")}), store.IfAbsent())
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range rounds {
				if err := increment(t.Context(), b, key); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	wantStored(t, b, keyed(part, key.SK, store.Item{catalog.AttrRowVersion: num(strconv.Itoa(1 + workers*rounds))}))
}

func increment(ctx context.Context, b store.Backend, k store.Key) error {
	cur, err := b.Get(ctx, Table, k, true)
	for err == nil {
		var v int64
		if v, err = strconv.ParseInt(cur[catalog.AttrRowVersion].Str, 10, 64); err != nil {
			return err
		}
		next := keyed(k.PK, k.SK, store.Item{catalog.AttrRowVersion: num(strconv.FormatInt(v+1, 10))})
		err = b.Put(ctx, Table, next, store.IfVersion(v))
		var ce *store.ConditionError
		if !errors.As(err, &ce) {
			return err // nil once the write lands
		}
		cur, err = ce.Current, nil
	}
	return err
}

func (s suite) checkRetriedWrite(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	r := RetriedWrite(b)
	err := r.Put(t.Context(), Table, keyed(part, "a", store.Item{
		catalog.AttrRowVersion: num("1"),
		"n":                    num("1.50"),
	}), store.IfAbsent())
	// The stored item, numbers in DynamoDB's form.
	wantConditionFailed(t, err, keyed(part, "a", store.Item{catalog.AttrRowVersion: num("1"), "n": num("1.5")}))

	second := keyed(part, "a", store.Item{catalog.AttrRowVersion: num("2")})
	wantConditionFailed(t, r.Put(t.Context(), Table, second, store.IfVersion(1)), second)
	wantStored(t, b, second)

	third := keyed(part, "a", store.Item{"n": str("unconditional")})
	if err = r.Put(t.Context(), Table, third, store.Cond{}); err != nil {
		t.Errorf("unconditional Put through RetriedWrite: %v", err)
	}
	wantStored(t, b, third)

	theirs := keyed(part, "b", store.Item{"o": str("theirs")})
	mustPut(t, b, theirs, store.Cond{})
	err = r.Put(t.Context(), Table, keyed(part, "b", store.Item{"o": str("mine")}), store.IfAbsent())
	wantConditionFailed(t, err, theirs)
	wantStored(t, b, theirs)
}

func (s suite) checkStaleReads(t *testing.T) {
	t.Parallel()
	b := StaleReads(s.newBackend(t), s.newBackend(t))
	it := keyed(part, "a", store.Item{"n": num("1")})
	mustPut(t, b, it, store.IfAbsent())
	wantRead(t, b, keyOf(it), true, it)
	wantRead(t, b, keyOf(it), false, nil)
}

// wantRead fails unless Get and BatchGet of k, and a Query of its partition, find want, or
// nothing when want is nil.
func wantRead(t *testing.T, b store.Backend, k store.Key, consistent bool, want store.Item) {
	t.Helper()
	got, err := b.Get(t.Context(), Table, k, consistent)
	if err != nil || !equalItem(got, want) {
		t.Errorf("Get, consistent %v = %s, %v; want %s", consistent, show(got), err, show(want))
	}
	batch, err := b.BatchGet(t.Context(), Table, []store.Key{k}, consistent)
	if err != nil || len(batch) != 1 || !equalItem(batch[0], want) {
		t.Errorf("BatchGet, consistent %v = %d items, %v; want [%s]", consistent, len(batch), err, show(want))
	}
	var queried, wantQueried []store.Item
	for got, err := range b.Query(t.Context(), Table, store.Query{PK: k.PK, Consistent: consistent}) {
		if err != nil {
			t.Fatalf("Query, consistent %v: %v", consistent, err)
		}
		queried = append(queried, got)
	}
	if want != nil {
		wantQueried = []store.Item{want}
	}
	if !slices.EqualFunc(queried, wantQueried, equalItem) {
		t.Errorf("Query, consistent %v = %d items, want %d", consistent, len(queried), len(wantQueried))
	}
}

func (s suite) checkCanceledContext(t *testing.T) {
	t.Parallel()
	b := s.newBackend(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	k := store.Key{PK: part, SK: "a"}
	putErr := b.Put(ctx, Table, keyed(k.PK, k.SK, nil), store.Cond{})
	_, getErr := b.Get(ctx, Table, k, true)
	_, batchErr := b.BatchGet(ctx, Table, []store.Key{k}, true)
	var queryErr error
	for _, err := range b.Query(ctx, Table, store.Query{PK: part}) {
		queryErr = err
		break
	}
	errs := map[string]error{"Put": putErr, "Get": getErr, "BatchGet": batchErr, "Query": queryErr}
	for op, err := range errs {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a canceled context: error = %v, want context.Canceled", op, err)
		}
	}
	if got := mustGet(t, b, k); got != nil {
		t.Errorf("Put with a canceled context stored %s", show(got))
	}
}

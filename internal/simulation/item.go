// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"math/rand/v2"
	"strconv"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

func keyOf(it store.Item) store.Key {
	return store.Key{PK: it[catalog.AttrPK].Str, SK: it[catalog.AttrSK].Str}
}

// recordKey returns the key of a record's item: a REMOVE has only its old image.
func recordKey(rec projector.Record) store.Key {
	if rec.Op == projector.Remove {
		return keyOf(rec.Old)
	}
	return keyOf(rec.New)
}

func keyText(it store.Item) string {
	return it[catalog.AttrPK].Str + " " + it[catalog.AttrSK].Str
}

// keyHash places an item in a delivery unit, as a table partition does.
func keyHash(k store.Key) uint64 {
	return fnv(fnv(fnvOffset, k.PK), k.SK)
}

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

// fnv folds s into the FNV-1a hash h, then a zero byte, so a sequence of strings hashes
// without ambiguity.
func fnv(h uint64, s string) uint64 {
	for i := range len(s) {
		h = (h ^ uint64(s[i])) * fnvPrime
	}
	return h * fnvPrime
}

// newRand returns the generator of one stream of a seed.
func newRand(seed, stream uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, stream)) //nolint:gosec // a seeded generator is what makes a run replayable
}

// integer reads an N that holds an integer; anything else reads as -1.
func integer(v store.Value) int64 {
	n, err := strconv.ParseInt(v.Str, 10, 64)
	if v.Type != store.N || err != nil {
		return -1
	}
	return n
}

// cloneRecord copies a record, so the code under test can't change what the oracle and
// other deliveries read.
func cloneRecord(rec projector.Record) projector.Record {
	rec.New = cloneItem(rec.New)
	rec.Old = cloneItem(rec.Old)
	return rec
}

func cloneItem(it store.Item) store.Item {
	if it == nil {
		return nil
	}
	out := make(store.Item, len(it))
	for name, v := range it {
		out[name] = cloneValue(v)
	}
	return out
}

func cloneValue(v store.Value) store.Value {
	v.Bin = append([]byte(nil), v.Bin...)
	if v.List != nil {
		list := make([]store.Value, len(v.List))
		for i, e := range v.List {
			list[i] = cloneValue(e)
		}
		v.List = list
	}
	if v.Map != nil {
		m := make(map[string]store.Value, len(v.Map))
		for name, e := range v.Map {
			m[name] = cloneValue(e)
		}
		v.Map = m
	}
	return v
}

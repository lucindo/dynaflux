// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg_test

import (
	"math/big"
	"testing"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

func typ(k catalog.Kind) catalog.Type { return catalog.Type{Kind: k} }

func decimal(p, s int) catalog.Type {
	return catalog.Type{Kind: catalog.KindDecimal, Precision: p, Scale: s}
}

func dateTime64(p int) catalog.Type { return catalog.Type{Kind: catalog.KindDateTime64, Precision: p} }

func nullable(t catalog.Type) catalog.Type {
	t.Nullable = true
	return t
}

// spec returns a view aggregate's Spec. Its result type is Nullable when the first argument
// is, as sql.md §7.1 says for sum, min, max, argMax, argMin and any.
func spec(fn catalog.AggFunc, params []float64, args ...catalog.Type) agg.Spec {
	s := agg.Spec{Agg: catalog.Aggregate{Fn: fn, Params: params}, ArgTypes: args}
	s.Agg.Type.Nullable = len(args) > 0 && args[0].Nullable ||
		fn == catalog.AggAvg || fn == catalog.AggQuantile || fn == catalog.AggQuantileDD
	return s
}

func readTime(s agg.Spec) agg.Spec {
	s.ReadTime = true
	return s
}

func compile(t *testing.T, s agg.Spec) *agg.Func {
	t.Helper()
	f, err := agg.Compile(s)
	if err != nil {
		t.Fatalf("Compile(%s): %v", s.Agg.Fn, err)
	}
	return f
}

func input(args ...expr.Value) agg.Input { return agg.Input{Args: args} }

func dec(s string) expr.Value {
	d, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic("bad decimal literal " + s)
	}
	return expr.Decimal(d)
}

// state adds the inputs to a new state of f.
func state(t *testing.T, f *agg.Func, ins ...agg.Input) agg.State {
	t.Helper()
	return add(t, f.New(), ins...)
}

func add(t *testing.T, s agg.State, ins ...agg.Input) agg.State {
	t.Helper()
	for _, in := range ins {
		if err := s.Add(in); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	return s
}

func encode(t *testing.T, s agg.State) store.Value {
	t.Helper()
	v, err := s.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return v
}

func final(t *testing.T, s agg.State) expr.Value {
	t.Helper()
	v, err := s.Final()
	if err != nil {
		t.Fatalf("Final: %v", err)
	}
	return v
}

func decode(t *testing.T, f *agg.Func, a store.Value) agg.State {
	t.Helper()
	s, err := f.Decode(a)
	if err != nil {
		t.Fatalf("Decode(%+v): %v", a, err)
	}
	return s
}

// double merges a state into itself n times: 2^n copies of its inputs.
func double(t *testing.T, f *agg.Func, s agg.State, n int) agg.State {
	t.Helper()
	for range n {
		if err := s.Merge(decode(t, f, encode(t, s))); err != nil {
			t.Fatalf("Merge: %v", err)
		}
	}
	return s
}

func num(s string) store.Value { return store.Value{Type: store.N, Str: s} }

func str(s string) store.Value { return store.Value{Type: store.S, Str: s} }

func bin(b ...byte) store.Value { return store.Value{Type: store.B, Bin: b} }

func list(vs ...store.Value) store.Value { return store.Value{Type: store.L, List: vs} }

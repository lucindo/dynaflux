// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg_test

import (
	"fmt"
	"testing"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

// benchState is an aggregate with the stored state of a row that has taken n inputs, and
// one more input.
type benchState struct {
	name   string
	f      *agg.Func
	stored store.Value
	in     agg.Input
}

// benchStates returns the aggregates of the example's views. The sketches come twice: a
// uniq of 100 values is sparse and one of 100,000 dense, and a quantile's size grows with
// its inputs. A scalar state's size doesn't depend on n.
func benchStates(b *testing.B) []benchState {
	b.Helper()
	type benchCase struct {
		name string
		s    agg.Spec
		n    int
		in   func(i int) agg.Input
	}
	cases := []benchCase{
		{"count", spec(catalog.AggCount, nil), 100, func(int) agg.Input { return agg.Input{} }},
		{"sum_UInt32", spec(catalog.AggSum, nil, typ(catalog.KindUInt32)), 100,
			func(i int) agg.Input { return input(expr.Uint(uint64(i % 5000))) }},
		{"sum_Float64", spec(catalog.AggSum, nil, typ(catalog.KindFloat64)), 100,
			func(i int) agg.Input { return input(expr.Float(float64(i) * 0.37)) }},
		{"max_DateTime64", spec(catalog.AggMax, nil, dateTime64(6)), 100,
			func(i int) agg.Input { return input(expr.Micros(int64(i) * 1_000_000)) }},
		{"argMax_String", spec(catalog.AggArgMax, nil, typ(catalog.KindString), dateTime64(6)), 100,
			func(i int) agg.Input {
				in := input(expr.String(fmt.Sprintf("action-%d", i%7)), expr.Micros(int64(i)))
				in.EventTime, in.EventID = int64(i), fmt.Sprintf("%026d", i)
				return in
			}},
	}
	for _, n := range []int{100, 100_000} {
		cases = append(cases,
			benchCase{fmt.Sprintf("uniq_String/n=%d", n), spec(catalog.AggUniq, nil, typ(catalog.KindString)), n,
				func(i int) agg.Input { return input(expr.String(fmt.Sprintf("user-%d", i))) }},
			benchCase{fmt.Sprintf("quantile_Float64/n=%d", n), spec(catalog.AggQuantile, []float64{0.5}, typ(catalog.KindFloat64)), n,
				func(i int) agg.Input { return input(expr.Float(float64(i%997) + 0.5)) }},
		)
	}
	out := make([]benchState, len(cases))
	for i, c := range cases {
		f, err := agg.Compile(c.s)
		if err != nil {
			b.Fatalf("Compile(%s): %v", c.name, err)
		}
		st := f.New()
		for j := range c.n {
			if err = st.Add(c.in(j)); err != nil {
				b.Fatalf("%s: Add: %v", c.name, err)
			}
		}
		stored, err := st.Encode()
		if err != nil {
			b.Fatalf("%s: Encode: %v", c.name, err)
		}
		out[i] = benchState{name: c.name, f: f, stored: stored, in: c.in(c.n)}
	}
	return out
}

// BenchmarkRowUpdate is the projector's work on one aggregate of a row it writes: decode
// the stored state, add one input, encode it.
func BenchmarkRowUpdate(b *testing.B) {
	for _, c := range benchStates(b) {
		b.Run(c.name, func(b *testing.B) {
			for b.Loop() {
				s, err := c.f.Decode(c.stored)
				if err != nil {
					b.Fatal(err)
				}
				if err = s.Add(c.in); err != nil {
					b.Fatal(err)
				}
				if _, err = s.Encode(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMerge is a query's work on one more row of an aggregate it combines, such as
// TagTotals' uniqMerge: decode the row's state, merge it, and finalize.
func BenchmarkMerge(b *testing.B) {
	for _, c := range benchStates(b) {
		b.Run(c.name, func(b *testing.B) {
			acc, err := c.f.Decode(c.stored)
			if err != nil {
				b.Fatal(err)
			}
			for b.Loop() {
				s, derr := c.f.Decode(c.stored)
				if derr != nil {
					b.Fatal(derr)
				}
				if err = acc.Merge(s); err != nil {
					b.Fatal(err)
				}
				if _, err = acc.Final(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg_test

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

func quantileSpec(alpha, level float64, t catalog.Type) agg.Spec {
	if alpha == 0.01 {
		return spec(catalog.AggQuantile, []float64{level}, t)
	}
	return spec(catalog.AggQuantileDD, []float64{alpha, level}, t)
}

// TestQuantileGolden pins a sketch worked out by hand from encoding.md §7.5 with 35 buckets
// per octave: 1 and −1 are index 0, 2 is 35, and 3 (1.5·2) is 35 + 20, since
// 2^(20/35) ≤ 1.5 < 2^(21/35).
func TestQuantileGolden(t *testing.T) {
	t.Parallel()
	f := compile(t, quantileSpec(0.01, 0.5, typ(catalog.KindInt64)))
	s := state(t, f, input(expr.Int(1)), input(expr.Int(2)), input(expr.Int(3)), input(expr.Int(-1)), input(expr.Int(0)))
	want := bin(0x01, 0x01, 0x03, 0x00, 0x01, 0x23, 0x01, 0x14, 0x01, 0x01, 0x00, 0x01)
	if got := encode(t, s); !reflect.DeepEqual(got, want) {
		t.Fatalf("Encode = %x, want %x", got.Bin, want.Bin)
	}
	// The median of −1, 0, 1, 2, 3 is 1.
	if got := final(t, s).AsFloat(); math.Abs(got-1) > 0.01 {
		t.Fatalf("median = %v, want 1 within 1%%", got)
	}
}

// TestQuantileBucketBounds pins the first value of buckets with 35 buckets per octave: T[j],
// 2^(j/35) rounded to float64 and computed outside Go, starts bucket j, and the float64 below
// it is in bucket j − 1. The smallest and largest float64 are the decoder's bounds: 2^-1074 is
// bucket −1074·35, and (2 − 2^-52)·2^1023 is past T[34]·2^1023.
func TestQuantileBucketBounds(t *testing.T) {
	t.Parallel()
	f := compile(t, quantileSpec(0.01, 0.5, typ(catalog.KindFloat64)))
	cases := []struct {
		x     float64
		index int64
	}{
		{0x1.051ed35254d7bp+0, 1},
		{math.Nextafter(0x1.051ed35254d7bp+0, 0), 0},
		{0x1.6678aee5629f8p+0, 17},
		{math.Nextafter(0x1.6678aee5629f8p+0, 0), 16},
		{0x1.f5f5c20dfe5f2p+5, 5*35 + 34},
		{math.Nextafter(0x1.f5f5c20dfe5f2p+5, 0), 5*35 + 33},
		{math.SmallestNonzeroFloat64, -1074 * 35},
		{math.MaxFloat64, 1023*35 + 34},
	}
	for _, c := range cases {
		got := encode(t, state(t, f, input(expr.Float(c.x))))
		if want := oneBucket(c.index); !reflect.DeepEqual(got, want) {
			t.Fatalf("%x: Encode = %x, want index %d", c.x, got.Bin, c.index)
		}
		decode(t, f, got)
	}
}

// TestQuantileOfOneValue checks every bucket's value against the accuracy bound over
// normal float64 magnitudes of both signs.
func TestQuantileOfOneValue(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(3, 4))
	for _, alpha := range []float64{0.01, 0.002, 0.05, 0.5} {
		f := compile(t, quantileSpec(alpha, 0.5, typ(catalog.KindFloat64)))
		xs := []float64{math.SmallestNonzeroFloat64 * (1 << 52), math.MaxFloat64, 1, 2, 3}
		for range 20000 {
			xs = append(xs, math.Ldexp(1+r.Float64(), r.IntN(2046)-1022))
		}
		for i, x := range xs {
			if i%2 == 1 {
				x = -x
			}
			got := final(t, state(t, f, input(expr.Float(x)))).AsFloat()
			if math.Abs(got-x) > alpha*math.Abs(x) {
				t.Fatalf("rel_acc %v: quantile of %v = %v", alpha, x, got)
			}
		}
	}
}

// TestQuantileAccuracy compares quantiles with the exact value at rank level·(n − 1).
func TestQuantileAccuracy(t *testing.T) {
	t.Parallel()
	gens := map[string]func(r *rand.Rand) float64{
		"latencies": func(r *rand.Rand) float64 { return math.Ldexp(1+r.Float64(), r.IntN(20)) },
		"mixed signs and zeros": func(r *rand.Rand) float64 {
			if r.IntN(10) == 0 {
				return 0
			}
			return float64(r.IntN(2_000_001) - 1_000_000)
		},
	}
	for name, gen := range gens {
		for _, alpha := range []float64{0.01, 0.002} {
			r := rand.New(rand.NewPCG(5, 6))
			xs := make([]float64, 5001)
			for i := range xs {
				xs[i] = gen(r)
			}
			sorted := slices.Sorted(slices.Values(xs))
			for _, level := range []float64{0, 0.01, 0.25, 0.5, 0.9, 0.99, 1} {
				s := state(t, compile(t, quantileSpec(alpha, level, typ(catalog.KindFloat64))), floats(xs)...)
				want := sorted[int(level*float64(len(xs)-1))]
				if got := final(t, s).AsFloat(); math.Abs(got-want) > alpha*math.Abs(want) {
					t.Errorf("%s, rel_acc %v: quantile(%v) = %v, want %v", name, alpha, level, got, want)
				}
			}
		}
	}
}

func floats(xs []float64) []agg.Input {
	in := make([]agg.Input, len(xs))
	for i, x := range xs {
		in[i] = input(expr.Float(x))
	}
	return in
}

// TestQuantileWindow checks the 64-octave cap of each store: values more than 2^64 times
// smaller than the largest of their sign share one bucket, so a stray huge value doesn't
// cost the accuracy of the others.
func TestQuantileWindow(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(13, 14))
	f := compile(t, quantileSpec(0.01, 0.75, typ(catalog.KindFloat64)))
	s := f.New()
	for range 20000 {
		for _, sign := range []float64{1, -1} {
			if err := s.Add(input(expr.Float(sign * (1000 + 1000*r.Float64())))); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, x := range []float64{1.7e15, -1e-15} {
		if err := s.Add(input(expr.Float(x))); err != nil {
			t.Fatal(err)
		}
	}
	if got := final(t, s).AsFloat(); got < 990 || got > 2020 {
		t.Fatalf("quantile(0.75) with outliers = %v, want 1000 to 2000", got)
	}

	tiny := compile(t, quantileSpec(0.01, 0, typ(catalog.KindFloat64)))
	s = state(t, tiny, input(expr.Float(1e-30)), input(expr.Float(1e-31)), input(expr.Float(1e30)))
	// The bottom of the window is 1e30·2^(1/35 − 64), within the bucket's accuracy.
	if got := final(t, s).AsFloat() / 1e30 * 0x1p64; got < 0.99 || got > 1.05 {
		t.Fatalf("quantile(0) = 1e30·2^-64·%v, want the window's lowest bucket", got)
	}
}

// TestQuantileMergeAtAnotherLevel reads a stored sketch with another level, as
// quantileMerge(level) does.
func TestQuantileMergeAtAnotherLevel(t *testing.T) {
	t.Parallel()
	view := compile(t, quantileSpec(0.01, 0.9, typ(catalog.KindUInt64)))
	s := view.New()
	for i := range 100 {
		if err := s.Add(input(expr.Uint(uint64(i + 1)))); err != nil {
			t.Fatal(err)
		}
	}
	query := compile(t, quantileSpec(0.01, 0.5, typ(catalog.KindUInt64)))
	if got := final(t, decode(t, query, encode(t, s))).AsFloat(); math.Abs(got-50) > 0.5 {
		t.Fatalf("median = %v, want 50 within 1%%", got)
	}
}

// oneBucket returns a sketch of one positive bucket with count 1.
func oneBucket(index int64) store.Value {
	return bin(slices.Concat([]byte{0x01, 0x00, 0x01}, binary.AppendVarint(nil, index), []byte{0x01, 0x00})...)
}

func TestQuantileDecodeRejects(t *testing.T) {
	t.Parallel()
	f := compile(t, quantileSpec(0.01, 0.5, typ(catalog.KindFloat64)))
	// One positive bucket at index 0 with count 1: 01, zero 00, pos 01 00 01, neg 00.
	if _, err := f.Decode(bin(0x01, 0x00, 0x01, 0x00, 0x01, 0x00)); err != nil {
		t.Fatalf("Decode of a valid sketch: %v", err)
	}
	cases := []struct {
		name string
		a    store.Value
	}{
		{"as N", num("1")},
		{"unknown version", bin(0x02, 0x00, 0x01, 0x00, 0x01, 0x00)},
		{"no input", bin(0x01, 0x00, 0x00, 0x00)},
		{"zero count", bin(0x01, 0x00, 0x01, 0x00, 0x00, 0x00)},
		{"repeated index", bin(0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x01, 0x00)},
		{"overlong count", bin(0x01, 0x00, 0x01, 0x00, 0x81, 0x00, 0x00)},
		{"trailing bytes", bin(0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00)},
		{"truncated", bin(0x01, 0x00, 0x01, 0x00)},
		{"index below every float", oneBucket(-1074*35 - 1)},
		{"index past every float", oneBucket(1023*35 + 35)},
		{"span past the window", bin(0x01, 0x00, 0x02, 0x00, 0x01, 0xc0, 0x11, 0x01, 0x00)},
		{"more buckets than the window", bin(binary.AppendUvarint([]byte{0x01, 0x00}, 64*35+1)...)},
		{"count past 2^64", bin(0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x01, 0x00, 0x01, 0x00)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if s, err := f.Decode(c.a); err == nil {
				t.Fatalf("Decode(%x) = %+v, want an error", c.a.Bin, s)
			}
		})
	}
}

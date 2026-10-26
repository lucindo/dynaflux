// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg_test

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

func size(v store.Value) int { return store.ItemSize(store.Item{"": v}) }

// maxMicros is the last microsecond before 2106-02-07 06:28:15 UTC.
const maxMicros = math.MaxUint32*1_000_000 - 1

// TestMaxSize checks the bounds of encoding.md §7.8, worked out by hand with the size rules of
// store.ItemSize: an N is (significant digits + 1)/2 + 2 bytes, an L 3 bytes plus 1 per
// element. Each case also builds a state that reaches its bound: 2^n inputs come from n
// merges of a state with itself.
func TestMaxSize(t *testing.T) {
	t.Parallel()
	i64, u64, f64, strT := typ(catalog.KindInt64), typ(catalog.KindUInt64), typ(catalog.KindFloat64), typ(catalog.KindString)
	long := expr.String(strings.Repeat("x", agg.MaxStringBytes))
	worstEvent := agg.Input{EventTime: maxMicros, EventID: strings.Repeat("Z", 26), ArrayIndex: 999}
	cases := []struct {
		name string
		spec agg.Spec
		want int
		// worst builds a state of that size: merges make 2^64 inputs, then then is added.
		worst  []agg.Input
		merges int
		then   []agg.Input
	}{
		// 2^64: 20 digits.
		{"count", spec(catalog.AggCount, nil), 12, []agg.Input{input()}, 64, nil},
		{"uniqExact", spec(catalog.AggUniqExact, nil, strT), 12, []agg.Input{input(expr.String("a"))}, 64, nil},
		// −2^63·2^64: an S of '-' and 39 digits.
		{"sum of Int64", spec(catalog.AggSum, nil, i64), 40, []agg.Input{input(expr.Int(math.MinInt64))}, 64, nil},
		// (2^64 − 1)·2^64: 39 digits.
		{"sum of UInt64", spec(catalog.AggSum, nil, u64), 39, []agg.Input{input(expr.Uint(math.MaxUint64))}, 64, nil},
		// 255·2^64 = 4703919738795935662080: 21 significant digits.
		{"sum of UInt8", spec(catalog.AggSum, nil, typ(catalog.KindUInt8)), 13, []agg.Input{input(expr.Uint(255))}, 64, nil},
		// (10^38 − 1)·2^64: '-', 56 digits, '.', 2 digits.
		{"sum of Decimal(38, 2)", spec(catalog.AggSum, nil, decimal(38, 2)), 60, []agg.Input{input(dec("-" + strings.Repeat("9", 38)))}, 64, nil},
		// (10^10 − 1)·2^64 < 10^30: an N of 30 digits.
		{"sum of Decimal(10, 2)", spec(catalog.AggSum, nil, decimal(10, 2)), 17, []agg.Input{input(dec("-9999999999"))}, 64, nil},
		// MaxFloat64·2^64 plus the smallest subnormal: bits 1087 to −1074 of the mantissa (271
		// bytes), a sign byte and varint(−1074) (2 bytes).
		{"sum of Float64", spec(catalog.AggSum, nil, f64), 274, []agg.Input{input(expr.Float(math.MaxFloat64))}, 64,
			[]agg.Input{input(expr.Float(math.SmallestNonzeroFloat64))}},
		// L of the Int64 sum (40) and the count (12).
		{"avg of Int64", spec(catalog.AggAvg, nil, i64), 3 + 41 + 13, []agg.Input{input(expr.Int(math.MinInt64))}, 64, nil},
		{"min of String", spec(catalog.AggMin, nil, strT), agg.MaxStringBytes, []agg.Input{input(long)}, 0, nil},
		// 1.0000000000000002 has 17 significant digits.
		{"max of Float64", spec(catalog.AggMax, nil, f64), 11, []agg.Input{input(expr.Float(math.Nextafter(1, 2)))}, 0, nil},
		{"max of UInt64", spec(catalog.AggMax, nil, u64), 12, []agg.Input{input(expr.Uint(math.MaxUint64))}, 0, nil},
		{"min of Int8", spec(catalog.AggMin, nil, typ(catalog.KindInt8)), 4, []agg.Input{input(expr.Int(-128))}, 0, nil},
		{"min of Decimal(38, 10)", spec(catalog.AggMin, nil, decimal(38, 10)), 21, []agg.Input{input(dec(strings.Repeat("9", 38)))}, 0, nil},
		{"max of Date", spec(catalog.AggMax, nil, typ(catalog.KindDate)), 10, []agg.Input{input(expr.Micros(0))}, 0, nil},
		{"max of DateTime", spec(catalog.AggMax, nil, typ(catalog.KindDateTime)), 7, []agg.Input{input(expr.Micros(maxMicros - 999_999))}, 0, nil},
		{"max of DateTime64(3)", spec(catalog.AggMax, nil, dateTime64(3)), 9, []agg.Input{input(expr.Micros(maxMicros - 999))}, 0, nil},
		{"max of DateTime64(6)", spec(catalog.AggMax, nil, dateTime64(6)), 10, []agg.Input{input(expr.Micros(maxMicros))}, 0, nil},
		// L of arg (4096), val (16 digits), event_time (16 digits), event_id (26) and
		// array_index (3 digits).
		{"argMax", spec(catalog.AggArgMax, nil, strT, dateTime64(6)), 3 + 4097 + 11 + 11 + 27 + 5,
			[]agg.Input{withArgs(worstEvent, long, expr.Micros(maxMicros))}, 0, nil},
		{"any of UUID", spec(catalog.AggAny, nil, typ(catalog.KindUUID)), 3 + 37 + 11 + 27 + 5,
			[]agg.Input{withArgs(worstEvent, expr.UUID([16]byte{}))}, 0, nil},
		{"uniq", spec(catalog.AggUniq, nil, u64), 2561, distinct(10000), 0, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := compile(t, c.spec)
			if got := f.MaxSize(); got != c.want {
				t.Fatalf("MaxSize = %d, want %d", got, c.want)
			}
			s := add(t, double(t, f, state(t, f, c.worst...), c.merges), c.then...)
			if got := size(encode(t, s)); got != c.want {
				t.Fatalf("worst state is %d bytes, want %d", got, c.want)
			}
		})
	}
}

func distinct(n int) []agg.Input {
	in := make([]agg.Input, n)
	for i := range in {
		in[i] = input(expr.Uint(uint64(i)))
	}
	return in
}

func withArgs(in agg.Input, args ...expr.Value) agg.Input {
	in.Args = args
	return in
}

// TestQuantileMaxSize checks the sketch bound of encoding.md §7.8, worked out by hand for m
// buckets per octave (35 at rel_acc 0.01, 174 at 0.002, 7 at 0.05, 1 at 0.5): a store of 64·m
// buckets with an index varint of 3 bytes (2 for m ≤ 7), a 1-byte step and an 8-byte count
// for each bucket, the zero count, and 256 bytes for counts of 2^56 or more.
func TestQuantileMaxSize(t *testing.T) {
	t.Parallel()
	cases := []struct {
		alpha float64
		want  int
	}{
		{0.01, 1 + 8 + 2*(2+3+8+2239*9) + 256},
		{0.002, 1 + 8 + 2*(2+3+8+11135*9) + 256},
		{0.05, 1 + 8 + 2*(2+2+8+447*9) + 256},
		{0.5, 1 + 8 + 2*(1+2+8+63*9) + 256},
	}
	for _, c := range cases {
		if got := compile(t, quantileSpec(c.alpha, 0.5, typ(catalog.KindFloat64))).MaxSize(); got != c.want {
			t.Fatalf("rel_acc %v: MaxSize = %d, want %d", c.alpha, got, c.want)
		}
	}

	// A full sketch: 2240 buckets of each sign from the lowest index, counts of 2^49 (8
	// bytes), 220 of them 2^56 (9 bytes), under 2^64 in all.
	f := compile(t, quantileSpec(0.01, 0.5, typ(catalog.KindFloat64)))
	b := binary.AppendUvarint([]byte{0x01}, 1<<49)
	big := 220
	for range 2 {
		b = binary.AppendUvarint(b, 2240)
		b = binary.AppendVarint(b, -1074*35)
		for i := range 2240 {
			if i > 0 {
				b = binary.AppendUvarint(b, 1)
			}
			n := uint64(1) << 49
			if big > 0 {
				n, big = 1<<56, big-1
			}
			b = binary.AppendUvarint(b, n)
		}
	}
	got := size(encode(t, decode(t, f, bin(b...))))
	if limit := f.MaxSize(); got > limit || got < limit-40 {
		t.Fatalf("full sketch is %d bytes, want at most and close to %d", got, limit)
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys_test

import (
	"bytes"
	"cmp"
	"math"
	"math/big"
	"math/rand/v2"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// Types used by the property tests: every key type, with Decimal extremes.
func keyTypes() []catalog.Type {
	return []catalog.Type{
		{Kind: catalog.KindString},
		{Kind: catalog.KindUInt8}, {Kind: catalog.KindUInt16}, {Kind: catalog.KindUInt32}, {Kind: catalog.KindUInt64},
		{Kind: catalog.KindInt8}, {Kind: catalog.KindInt16}, {Kind: catalog.KindInt32}, {Kind: catalog.KindInt64},
		{Kind: catalog.KindDecimal, Precision: 1}, {Kind: catalog.KindDecimal, Precision: 5, Scale: 2},
		{Kind: catalog.KindDecimal, Precision: 38}, {Kind: catalog.KindDecimal, Precision: 38, Scale: 10},
		{Kind: catalog.KindDecimal, Precision: 38, Scale: 38},
		{Kind: catalog.KindBool}, {Kind: catalog.KindUUID},
		{Kind: catalog.KindDate}, {Kind: catalog.KindDateTime},
		{Kind: catalog.KindDateTime64, Precision: 3}, {Kind: catalog.KindDateTime64, Precision: 6},
	}
}

// trickyStrings are the string edge cases: every byte up to 0x26 and 0x7F, mutual prefixes
// around the reserved bytes, and characters whose UTF-8 and UTF-16 orders disagree.
func trickyStrings() []string {
	s := []string{"", "a", "a\x00", "a ", "a!", "a#", "a$", "a%", "a&", "ab", "a\x7f", "aé",
		"\uffff", "\U0001F600", "\U0010FFFF", "a\U0010FFFF", "%", "%25", "#", "$", "a#b", "a%41", "a b",
		"New York", "50% #1", "\x7f"}
	for b := range byte(0x27) {
		s = append(s, string([]byte{b}))
	}
	return s
}

const timeLimitMicros = int64(math.MaxUint32) * 1_000_000 // 2106-02-07 06:28:15, excluded

func randString(r *rand.Rand) string {
	if tricky := trickyStrings(); r.IntN(3) > 0 {
		return tricky[r.IntN(len(tricky))]
	}
	var sb strings.Builder
	for range r.IntN(6) {
		switch r.IntN(4) {
		case 0:
			sb.WriteByte(byte(r.IntN(0x30)))
		case 1:
			sb.WriteRune(rune(0x80 + r.IntN(0x10FFFF-0x80)))
		default:
			sb.WriteByte(byte('a' + r.IntN(3)))
		}
	}
	return strings.ToValidUTF8(sb.String(), "x")
}

// randValue returns a valid value of t, half the time one of its edge values.
func randValue(r *rand.Rand, t catalog.Type) expr.Value {
	switch t.Kind {
	case catalog.KindString:
		return expr.String(randString(r))
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64:
		limit := uint64(math.MaxUint64) >> (64 - bitsOf(t.Kind))
		edges := []uint64{0, 1, 9, 10, limit, limit - 1}
		if r.IntN(2) == 0 {
			return expr.Uint(edges[r.IntN(len(edges))])
		}
		return expr.Uint(r.Uint64() & limit >> r.IntN(bitsOf(t.Kind)))
	case catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64:
		lo, hi := int64(math.MinInt64)>>(64-bitsOf(t.Kind)), int64(math.MaxInt64)>>(64-bitsOf(t.Kind))
		edges := []int64{lo, lo + 1, -10, -9, -1, 0, 1, 9, 10, hi}
		if r.IntN(2) == 0 {
			return expr.Int(edges[r.IntN(len(edges))])
		}
		return expr.Int(int64(r.Uint64()) >> (64 - bitsOf(t.Kind) + r.IntN(bitsOf(t.Kind))))
	case catalog.KindDecimal:
		return expr.Decimal(randDecimal(r, t.Precision))
	case catalog.KindBool:
		return expr.Bool(r.IntN(2) == 0)
	case catalog.KindUUID:
		var u [16]byte
		for i := range u {
			u[i] = []byte{0x00, 0x7f, 0xfe, 0xff, byte(r.IntN(256))}[r.IntN(5)]
		}
		return expr.UUID(u)
	case catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		unit := timeUnitOf(t)
		edges := []int64{0, unit, timeLimitMicros - 1, 1_717_163_100_250_123}
		us := r.Int64N(timeLimitMicros)
		if r.IntN(2) == 0 {
			us = edges[r.IntN(len(edges))]
		}
		return expr.Micros(us / unit * unit)
	case catalog.KindFloat64:
		return expr.Float(randFloat(r))
	}
	panic("no generator for " + t.String())
}

func randDecimal(r *rand.Rand, precision int) *big.Int {
	var digits string
	switch r.IntN(4) {
	case 0:
		digits = strings.Repeat("9", precision) // an extreme
	case 1:
		digits = string(rune('0' + r.IntN(3)))
	default:
		var sb strings.Builder
		for range 1 + r.IntN(precision) {
			sb.WriteByte(byte('0' + r.IntN(10)))
		}
		digits = sb.String()
	}
	d, _ := new(big.Int).SetString(digits, 10)
	if r.IntN(2) == 0 {
		d.Neg(d)
	}
	return d
}

func randFloat(r *rand.Rand) float64 {
	largest := math.Nextafter(1e126, 0)
	edges := []float64{0, 1e-130, -1e-130, 0.1, 1, -1, largest, -largest, 1e21, 123.456}
	if r.IntN(2) == 0 {
		return edges[r.IntN(len(edges))]
	}
	return math.Ldexp(r.Float64()+0.5, r.IntN(800)-400) * float64(1-2*r.IntN(2))
}

func bitsOf(k catalog.Kind) int {
	switch k {
	case catalog.KindUInt8, catalog.KindInt8:
		return 8
	case catalog.KindUInt16, catalog.KindInt16:
		return 16
	case catalog.KindUInt32, catalog.KindInt32:
		return 32
	case catalog.KindUInt64, catalog.KindInt64, catalog.KindString, catalog.KindFloat64, catalog.KindDecimal,
		catalog.KindBool, catalog.KindUUID, catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
	}
	return 64
}

func timeUnitOf(t catalog.Type) int64 {
	switch {
	case t.Kind == catalog.KindDate:
		return 86_400_000_000
	case t.Kind == catalog.KindDateTime:
		return 1_000_000
	case t.Precision == 3:
		return 1_000
	default:
		return 1
	}
}

// compareValues is the typed order of sql.md §9.4, written without the encoder: bytes for
// strings, numbers by value, times chronologically, false before true.
func compareValues(t catalog.Type, a, b expr.Value) int {
	switch t.Kind {
	case catalog.KindString:
		return strings.Compare(a.AsString(), b.AsString())
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64:
		return cmp.Compare(a.AsUint(), b.AsUint())
	case catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64:
		return cmp.Compare(a.AsInt(), b.AsInt())
	case catalog.KindDecimal:
		return a.AsDecimal().Cmp(b.AsDecimal())
	case catalog.KindBool:
		x, y := 0, 0
		if a.AsBool() {
			x = 1
		}
		if b.AsBool() {
			y = 1
		}
		return cmp.Compare(x, y)
	case catalog.KindUUID:
		x, y := a.AsUUID(), b.AsUUID()
		return bytes.Compare(x[:], y[:])
	case catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		return cmp.Compare(a.AsMicros(), b.AsMicros())
	case catalog.KindFloat64:
		return cmp.Compare(a.AsFloat(), b.AsFloat())
	}
	panic("no order for " + t.String())
}

func compareTuples(ts []catalog.Type, a, b []expr.Value) int {
	for i, t := range ts {
		if c := compareValues(t, a[i], b[i]); c != 0 {
			return c
		}
	}
	return 0
}

func randSchema(r *rand.Rand) []catalog.Type {
	all := keyTypes()
	ts := make([]catalog.Type, 1+r.IntN(3))
	for i := range ts {
		ts[i] = all[r.IntN(len(all))]
	}
	return ts
}

func randTuple(r *rand.Rand, ts []catalog.Type) []expr.Value {
	vs := make([]expr.Value, len(ts))
	for i, t := range ts {
		vs[i] = randValue(r, t)
	}
	return vs
}

// checkKeyBytes asserts the byte invariants of encoding.md §3.1 and §3.3 on a stored key.
func checkKeyBytes(t *testing.T, k string) {
	t.Helper()
	if k == "" || !utf8.ValidString(k) || k[0] == '!' {
		t.Fatalf("key %q: empty, invalid UTF-8 or starts with '!'", k)
	}
	for i := range len(k) {
		if k[i] < '#' || k[i] == '$' {
			t.Fatalf("key %q: byte %#x at %d", k, k[i], i)
		}
	}
}

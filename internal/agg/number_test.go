// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg_test

import (
	"errors"
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

// TestExactNumbers checks stored sums and counts against encoding.md §5.2 and §7.3 and their
// finalized values against sql.md §7.2.
func TestExactNumbers(t *testing.T) {
	t.Parallel()
	i64, u64, f64 := typ(catalog.KindInt64), typ(catalog.KindUInt64), typ(catalog.KindFloat64)
	nines38 := "99999999999999999999999999999999999999"
	cases := []struct {
		name   string
		spec   agg.Spec
		inputs []agg.Input
		stored store.Value
		final  expr.Value // ignored when evalErr
		// evalErr: the total is outside the result type.
		evalErr bool
	}{
		{"count", spec(catalog.AggCount, nil), []agg.Input{{}, {}, {}}, num("3"), expr.Uint(3), false},
		{"sum of Int64", spec(catalog.AggSum, nil, i64), ins(expr.Int(-5), expr.Int(3)), num("-2"), expr.Int(-2), false},
		{"sum at a scale", spec(catalog.AggSum, nil, decimal(10, 2)), ins(dec("1250"), dec("-5")),
			num("12.45"), dec("1245"), false},
		{"negative sum below 1", spec(catalog.AggSum, nil, decimal(10, 2)), ins(dec("-5")),
			num("-0.05"), dec("-5"), false},
		{"sum that reaches zero", spec(catalog.AggSum, nil, decimal(5, 1)), ins(dec("15"), dec("-15")),
			num("0.0"), dec("0"), false},
		{"Int64 sum past Int64", spec(catalog.AggSum, nil, i64), ins(expr.Int(math.MinInt64), expr.Int(-1)),
			num("-9223372036854775809"), expr.Value{}, true},
		{"UInt64 sum past UInt64", spec(catalog.AggSum, nil, u64), ins(expr.Uint(math.MaxUint64), expr.Uint(1)),
			num("18446744073709551616"), expr.Value{}, true},
		{"Decimal sum past 38 digits", spec(catalog.AggSum, nil, decimal(38, 0)), ins(dec(nines38), dec(nines38)),
			str("199999999999999999999999999999999999998"), expr.Value{}, true},
		{"Float64 sum", spec(catalog.AggSum, nil, f64), ins(expr.Float(0.1), expr.Float(0.2)),
			bin(0x6d, 0x00, 0x26, 0x66, 0x66, 0x66, 0x66, 0x66, 0x67), expr.Float(0.30000000000000004), false},
		{"Float64 sum past Float64", spec(catalog.AggSum, nil, f64), ins(expr.Float(math.MaxFloat64), expr.Float(math.MaxFloat64)),
			bin(0x98, 0x0f, 0x00, 0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff), expr.Value{}, true},
		{"avg", spec(catalog.AggAvg, nil, i64), ins(expr.Int(1), expr.Int(2)),
			list(num("3"), num("2")), expr.Float(1.5), false},
		{"avg of Decimal", spec(catalog.AggAvg, nil, decimal(10, 2)), ins(dec("10"), dec("20")),
			list(num("0.30"), num("2")), expr.Float(0.15), false},
		// The exact sum 0.6000000000000000055… rounds to 0.6, then 0.6/3 rounds down.
		{"avg of Float64", spec(catalog.AggAvg, nil, f64), ins(expr.Float(0.1), expr.Float(0.2), expr.Float(0.3)),
			list(bin(0x6d, 0x00, 0x4c, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcd), num("3")), expr.Float(0.19999999999999998), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := compile(t, c.spec)
			if got := encode(t, state(t, f, c.inputs...)); !reflect.DeepEqual(got, c.stored) {
				t.Fatalf("Encode = %+v, want %+v", got, c.stored)
			}
			got, err := decode(t, f, c.stored).Final()
			switch {
			case c.evalErr && !errors.Is(err, expr.ErrEval):
				t.Fatalf("Final = %+v, %v; want an evaluation error", got, err)
			case !c.evalErr && (err != nil || !sameValue(c.spec, got, c.final)):
				t.Fatalf("Final = %+v, %v; want %+v", got, err, c.final)
			}
		})
	}
}

func ins(vs ...expr.Value) []agg.Input {
	in := make([]agg.Input, len(vs))
	for i, v := range vs {
		in[i] = input(v)
	}
	return in
}

// TestDecodeNumberFormsDynamoDBReturns reads exact numbers in the forms DynamoDB returns:
// trailing zeros dropped, or an exponent.
func TestDecodeNumberFormsDynamoDBReturns(t *testing.T) {
	t.Parallel()
	d := decimal(10, 2)
	f := compile(t, spec(catalog.AggSum, nil, d))
	for _, n := range []string{"12.3", "12.30", "1.23E1"} {
		got := final(t, decode(t, f, num(n)))
		if expr.Compare(got, d, dec("1230"), d) != 0 {
			t.Fatalf("Decode(%s): Final = %+v, want 12.30", n, got)
		}
	}
}

func TestFloatSumIsCorrectlyRounded(t *testing.T) {
	t.Parallel()
	f := compile(t, spec(catalog.AggSum, nil, typ(catalog.KindFloat64)))
	r := rand.New(rand.NewPCG(7, 8))
	for trial := range 300 {
		s := f.New()
		exact := new(big.Rat)
		n := 1 + r.IntN(200)
		prev := 0.0
		for range n {
			x := wideFloat(r)
			if r.IntN(4) == 0 {
				x = -prev // cancellations
			}
			prev = x
			if err := s.Add(input(expr.Float(x))); err != nil {
				t.Fatal(err)
			}
			exact.Add(exact, new(big.Rat).SetFloat64(x))
		}
		want, _ := exact.Float64()
		if got := final(t, s).AsFloat(); got != want {
			t.Fatalf("trial %d: sum = %v, want the correctly rounded %v", trial, got, want)
		}
	}
}

// TestFloatSumZeroIsStored checks that a sum that cancels to zero is a stored, non-empty
// state: an empty B, unlike an absent attribute.
func TestFloatSumZeroIsStored(t *testing.T) {
	t.Parallel()
	f := compile(t, spec(catalog.AggSum, nil, nullable(typ(catalog.KindFloat64))))
	s := state(t, f, input(expr.Float(1.5)), input(expr.Float(-1.5)))
	v := encode(t, s)
	if v.Type != store.B || len(v.Bin) != 0 {
		t.Fatalf("Encode = %+v, want an empty B", v)
	}
	back := decode(t, f, v)
	if back.Empty() || final(t, back).IsNull() || final(t, back).AsFloat() != 0 {
		t.Fatalf("decoded zero sum: Empty %v, Final %+v; want a non-empty 0", back.Empty(), final(t, back))
	}
}

func TestDecodeRejectsMalformedNumbers(t *testing.T) {
	t.Parallel()
	f64 := typ(catalog.KindFloat64)
	cases := []struct {
		name string
		spec agg.Spec
		a    store.Value
	}{
		{"count of 0", spec(catalog.AggCount, nil), num("0")},
		{"negative count", spec(catalog.AggCount, nil), num("-1")},
		{"fractional count", spec(catalog.AggCount, nil), num("1.5")},
		{"count as B", spec(catalog.AggCount, nil), bin(1)},
		{"short number as S", spec(catalog.AggSum, nil, typ(catalog.KindInt64)), str("5")},
		{"even mantissa", spec(catalog.AggSum, nil, f64), bin(0x00, 0x00, 0x02)},
		{"leading zero byte", spec(catalog.AggSum, nil, f64), bin(0x00, 0x00, 0x00, 0x01)},
		{"sign byte 2", spec(catalog.AggSum, nil, f64), bin(0x00, 0x02, 0x01)},
		{"overlong exponent", spec(catalog.AggSum, nil, f64), bin(0x80, 0x00, 0x00, 0x01)},
		{"no mantissa", spec(catalog.AggSum, nil, f64), bin(0x00, 0x00)},
		{"exponent below every float", spec(catalog.AggSum, nil, f64), bin(0xe5, 0x10, 0x00, 0x01)},
		{"sum past 2^64 floats", spec(catalog.AggSum, nil, f64), bin(0x80, 0x11, 0x00, 0x01)},
		{"Float64 sum as N", spec(catalog.AggSum, nil, f64), num("1")},
		{"avg without count", spec(catalog.AggAvg, nil, f64), list(bin(0x00, 0x00, 0x01))},
		{"avg with count 0", spec(catalog.AggAvg, nil, typ(catalog.KindInt32)), list(num("0"), num("0"))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if s, err := compile(t, c.spec).Decode(c.a); err == nil {
				t.Fatalf("Decode(%+v) = %+v, want an error", c.a, s)
			}
		})
	}
}

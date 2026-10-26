// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr_test

import (
	"math"
	"math/big"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// number is a numeric value with its exact value, computed apart from the package.
type number struct {
	typ   string
	text  string
	value expr.Value
	exact *big.Rat
}

func intNumber(i int64) number {
	return number{"Int64", strconv.FormatInt(i, 10), expr.Int(i), new(big.Rat).SetInt64(i)}
}

func uintNumber(u uint64) number {
	return number{"UInt64", strconv.FormatUint(u, 10), expr.Uint(u), new(big.Rat).SetUint64(u)}
}

func floatNumber(f float64) number {
	return number{"Float64", strconv.FormatFloat(f, 'g', -1, 64), expr.Float(f), new(big.Rat).SetFloat64(f)}
}

// decimalNumber reads "-123.45" as a Decimal(38, 2).
func decimalNumber(text string) number {
	_, frac, _ := strings.Cut(text, ".")
	unscaled, ok := new(big.Int).SetString(strings.Replace(text, ".", "", 1), 10)
	exact, ratOK := new(big.Rat).SetString(text)
	if !ok || !ratOK {
		panic("bad decimal " + text)
	}
	return number{"Decimal(38, " + strconv.Itoa(len(frac)) + ")", text, expr.Decimal(unscaled), exact}
}

func randomNumber(rng *rand.Rand) number {
	const kinds = 4
	switch rng.IntN(kinds) {
	case 0:
		return intNumber(int64(rng.Uint64()) >> rng.UintN(64))
	case 1:
		return uintNumber(rng.Uint64() >> rng.UintN(64))
	case 2:
		f := math.Float64frombits(rng.Uint64())
		if math.IsInf(f, 0) || math.IsNaN(f) || rng.IntN(2) == 0 {
			f = float64(int64(rng.Uint64())>>rng.UintN(64)) + float64(rng.IntN(4))/4
		}
		return floatNumber(f)
	}
	digits := strconv.FormatUint(rng.Uint64()>>rng.UintN(64), 10)
	scale := rng.IntN(len(digits) + 1)
	text := strings.TrimLeft(digits[:len(digits)-scale], "0")
	if text == "" {
		text = "0"
	}
	if scale > 0 {
		text += "." + digits[len(digits)-scale:]
	}
	if rng.IntN(2) == 0 && strings.Trim(text, "0.") != "" {
		text = "-" + text
	}
	return decimalNumber(text)
}

func TestCompareIsExactAcrossNumberTypes(t *testing.T) {
	t.Parallel()
	edges := []number{
		intNumber(math.MinInt64), intNumber(-(1 << 53) - 1), intNumber(-1), intNumber(0), intNumber(1),
		intNumber(1<<53 + 1), intNumber(math.MaxInt64),
		uintNumber(0), uintNumber(1 << 53), uintNumber(1<<53 + 1), uintNumber(1<<63 - 1), uintNumber(1 << 63),
		uintNumber(math.MaxUint64),
		floatNumber(0), floatNumber(0.5), floatNumber(-0.5), floatNumber(-1), floatNumber(1 << 53),
		floatNumber(1<<53 + 2), floatNumber(0x1p63), floatNumber(-0x1p63), floatNumber(0x1p64),
		floatNumber(math.Nextafter(0x1p63, 0)), floatNumber(math.Nextafter(0x1p64, 0)),
		floatNumber(math.Nextafter(-0x1p63, 0)), floatNumber(-1e300), floatNumber(1e300),
		floatNumber(math.SmallestNonzeroFloat64), floatNumber(0.1),
		decimalNumber("0.50"), decimalNumber("-0.5"), decimalNumber("0.1"), decimalNumber("1.00"),
		decimalNumber("9007199254740993"), decimalNumber("9223372036854775808"),
		decimalNumber("18446744073709551615"), decimalNumber("-9223372036854775808.01"),
	}
	check := func(a, b number) {
		if got, want := expr.Compare(a.value, ty(a.typ), b.value, ty(b.typ)), a.exact.Cmp(b.exact); got != want {
			t.Errorf("Compare(%s %s, %s %s) = %d, want %d", a.typ, a.text, b.typ, b.text, got, want)
		}
	}
	for _, a := range edges {
		for _, b := range edges {
			check(a, b)
		}
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 100_000 {
		a := randomNumber(rng)
		b := randomNumber(rng)
		if rng.IntN(4) == 0 {
			b = edges[rng.IntN(len(edges))]
		}
		check(a, b)
	}
}

func TestCompareOrdersNullLast(t *testing.T) {
	t.Parallel()
	i, str := ty("Nullable(Int64)"), ty("Nullable(String)")
	cases := []struct {
		name string
		a, b expr.Value
		t    catalog.Type
		want int
	}{
		{"NULL, NULL", expr.Null(), expr.Null(), i, 0},
		{"NULL, max", expr.Null(), expr.Int(math.MaxInt64), i, 1},
		{"max, NULL", expr.Int(math.MaxInt64), expr.Null(), i, -1},
		{"'', NULL", expr.String(""), expr.Null(), str, -1},
		{"NULL, '\\xff'", expr.Null(), expr.String("\xff"), str, 1},
	}
	for _, c := range cases {
		if got := expr.Compare(c.a, c.t, c.b, c.t); got != c.want {
			t.Errorf("Compare(%s) = %d, want %d", c.name, got, c.want)
		}
	}
}

type intType struct {
	name   string
	lo, hi *big.Int
}

func intTypes() []intType {
	var out []intType
	for _, bits := range []int{8, 16, 32, 64} {
		top := new(big.Int).Lsh(big.NewInt(1), uint(bits))
		half := new(big.Int).Rsh(top, 1)
		out = append(out,
			intType{"UInt" + strconv.Itoa(bits), big.NewInt(0), new(big.Int).Sub(top, big.NewInt(1))},
			intType{"Int" + strconv.Itoa(bits), new(big.Int).Neg(half), new(big.Int).Sub(half, big.NewInt(1))})
	}
	return out
}

// TestIntegerOpsWrapTheExactResult checks +, -, *, intDiv and % against math/big: the
// exact result wrapped into the result type, or an evaluation error for a zero divisor and
// for the minimum of a signed dividend's type divided by -1 (sql.md §5.6, §5.8).
func TestIntegerOpsWrapTheExactResult(t *testing.T) {
	t.Parallel()
	types := intTypes()
	rng := rand.New(rand.NewPCG(3, 4))
	pick := func(lo, hi *big.Int) *big.Int {
		switch rng.IntN(6) {
		case 0:
			return lo
		case 1:
			return hi
		case 2:
			return big.NewInt(int64(rng.IntN(5)) - 2) // -2 to 2, clamped below
		}
		span := new(big.Int).Sub(hi, lo)
		r := new(big.Int).SetUint64(rng.Uint64())
		return r.Add(lo, r.Mod(r, span.Add(span, big.NewInt(1))))
	}
	ops := []catalog.Op{catalog.OpPlus, catalog.OpMinus, catalog.OpMultiply, catalog.OpIntDiv, catalog.OpModulo}
	for range 50_000 {
		at, bt, rt := types[rng.IntN(len(types))], types[rng.IntN(len(types))], types[rng.IntN(len(types))]
		a, b := pick(at.lo, at.hi), pick(bt.lo, bt.hi)
		if a.Cmp(at.lo) < 0 || b.Cmp(bt.lo) < 0 {
			continue // a small value below an unsigned range
		}
		op := ops[rng.IntN(len(ops))]
		want := intOracle(op, a, at.lo, b, rt.lo, rt.hi)
		e := call(op, rt.name, lit(at.name, a.String()), lit(bt.name, b.String()))
		p, err := expr.Compile(&e, expr.Scope{})
		if err != nil {
			t.Fatal(err)
		}
		if got := result(e.Type, p, &expr.Frame{}); got != want {
			t.Fatalf("%s(%s %s, %s %s) as %s = %s, want %s", op, at.name, a, bt.name, b, rt.name, got, want)
		}
	}
}

func intOracle(op catalog.Op, a, aLo, b, lo, hi *big.Int) string {
	r := new(big.Int)
	division := op == catalog.OpIntDiv || op == catalog.OpModulo
	switch {
	case op == catalog.OpPlus:
		r.Add(a, b)
	case op == catalog.OpMinus:
		r.Sub(a, b)
	case op == catalog.OpMultiply:
		r.Mul(a, b)
	case division && (b.Sign() == 0 || a.Sign() < 0 && a.Cmp(aLo) == 0 && b.Cmp(big.NewInt(-1)) == 0):
		return "error " + string(op)
	case op == catalog.OpIntDiv:
		r.Quo(a, b)
	default:
		r.Rem(a, b)
	}
	size := new(big.Int).Sub(hi, lo)
	size.Add(size, big.NewInt(1))
	r.Sub(r, lo).Mod(r, size).Add(r, lo)
	return r.String()
}

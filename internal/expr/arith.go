// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr

import (
	"fmt"
	"math"
	"math/big"

	"github.com/lucindo/dynaflux/internal/catalog"
)

const (
	reasonDivByZero = "division by zero"
	reasonMinByNeg1 = "the minimum value of the dividend's type divided by -1"
	reasonNonFinite = "the Float64 result is not finite"
	reasonDecimal   = "the Decimal result overflows its storage"
)

// arith returns the setup of a binary arithmetic op from its forms for integer, Float64
// and Decimal results (sql.md §5.6); nil means the op has no such form.
func arith(integer evalFunc, float func(x, y float64) float64, dec decimalFunc) setup {
	return func(_ *compiler, n *node, _ *catalog.Expr) error {
		k := n.typ.Kind
		switch {
		case k == catalog.KindFloat64 && float != nil:
			n.eval = floatOp(float)
		case k == catalog.KindDecimal && dec != nil:
			n.eval = decimalOp(dec)
		case intBits(k) > 0 && integer != nil:
			n.eval = integer
		default:
			return fmt.Errorf("no %s result", n.typ)
		}
		return nil
	}
}

// The products and quotients are converted explicitly, which forbids the compiler to
// fuse them into a multiply-add: results must not depend on the CPU.
func floatAdd(x, y float64) float64 { return x + y }
func floatSub(x, y float64) float64 { return x - y }
func floatMul(x, y float64) float64 { return float64(x * y) }
func floatDiv(x, y float64) float64 { return float64(x / y) }

func floatOp(fn func(x, y float64) float64) evalFunc {
	return binary(func(n *node, a, b Value) (Value, error) {
		return n.float(fn(toFloat(a), toFloat(b)))
	})
}

func (n *node) float(r float64) (Value, error) {
	if math.IsInf(r, 0) || math.IsNaN(r) {
		return Value{}, n.fail(reasonNonFinite)
	}
	return Float(r), nil
}

// toFloat returns the Float64 nearest to a number that isn't a Decimal.
func toFloat(v Value) float64 {
	if v.kind == kindInt {
		return float64(v.i)
	}
	if v.kind == kindUint {
		return float64(v.n)
	}
	return v.AsFloat()
}

// intOp returns an integer op that wraps (sql.md §5.6). Wrapping commutes with +, - and ×,
// so computing modulo 2^64 and then wrapping into the result type gives the exact result
// wrapped.
func intOp(fn func(x, y uint64) uint64) evalFunc {
	return binary(func(n *node, a, b Value) (Value, error) {
		return wrap(fn(twos(a), twos(b)), n.typ), nil
	})
}

// intDivision returns % or intDiv, which truncate toward zero; the remainder takes the
// sign of the dividend.
func intDivision(remainder bool) evalFunc {
	return binary(func(n *node, a, b Value) (Value, error) {
		am, aNeg := magnitude(a)
		bm, bNeg := magnitude(b)
		switch {
		case bm == 0:
			return Value{}, n.fail(reasonDivByZero)
		case bm == 1 && bNeg && isMinInt(a, n.args[0].typ):
			// The quotient overflows the dividend's type; ClickHouse fails for % too.
			return Value{}, n.fail(reasonMinByNeg1)
		case remainder:
			return wrap(negateIf(am%bm, aNeg), n.typ), nil
		}
		return wrap(negateIf(am/bm, aNeg != bNeg), n.typ), nil
	})
}

func evalNegate(n *node, v Value) (Value, error) {
	if n.typ.Kind == catalog.KindFloat64 {
		return Float(-v.AsFloat()), nil
	}
	if n.typ.Kind == catalog.KindDecimal {
		x := asDecimal(v, n.args[0].typ).at(n.typ.Scale)
		return n.decimal(x.Neg(x))
	}
	return wrap(-twos(v), n.typ), nil
}

// twos returns an integer's two's complement bits.
func twos(v Value) uint64 {
	if v.kind == kindInt {
		return uint64(v.i) //nolint:gosec // reinterpreting the bits is the point
	}
	return v.n
}

func magnitude(v Value) (uint64, bool) {
	if v.kind == kindInt && v.i < 0 {
		return -twos(v), true
	}
	return twos(v), false
}

func negateIf(m uint64, neg bool) uint64 {
	if neg {
		return -m
	}
	return m
}

func isMinInt(v Value, t catalog.Type) bool {
	return isSigned(t.Kind) && v.i == -1<<(intBits(t.Kind)-1)
}

// wrap reduces two's complement bits into the integer type t.
func wrap(x uint64, t catalog.Type) Value {
	if bits := intBits(t.Kind); bits < bits64 {
		x &= 1<<bits - 1
		if isSigned(t.Kind) && x>>(bits-1) == 1 {
			x |= ^uint64(0) << bits
		}
	}
	if isSigned(t.Kind) {
		return Int(int64(x)) //nolint:gosec // reinterpreting the bits is the point
	}
	return Uint(x)
}

const (
	bits8  = 8
	bits16 = 16
	bits32 = 32
	bits64 = 64
)

// intBits returns the width of an integer kind, and 0 for any other kind.
func intBits(k catalog.Kind) uint {
	switch k {
	case catalog.KindUInt8, catalog.KindInt8:
		return bits8
	case catalog.KindUInt16, catalog.KindInt16:
		return bits16
	case catalog.KindUInt32, catalog.KindInt32:
		return bits32
	case catalog.KindUInt64, catalog.KindInt64:
		return bits64
	case catalog.KindString, catalog.KindFloat64, catalog.KindDecimal, catalog.KindBool, catalog.KindUUID,
		catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
	}
	return 0
}

func isSigned(k catalog.Kind) bool {
	return k >= catalog.KindInt8 && k <= catalog.KindInt64
}

// decimal is an exact number: unscaled × 10^-scale.
type decimal struct {
	unscaled *big.Int
	scale    int
}

// asDecimal returns a Decimal or integer v of type t; an integer has scale 0.
func asDecimal(v Value, t catalog.Type) decimal {
	if v.kind == kindDecimal {
		return decimal{v.d, t.Scale}
	}
	if v.kind == kindInt {
		return decimal{big.NewInt(v.i), 0}
	}
	return decimal{new(big.Int).SetUint64(v.n), 0}
}

// at returns a new unscaled value of d at scale, which is at least d's: no Decimal op
// lowers a scale (sql.md §4.5, §5.6).
func (d decimal) at(scale int) *big.Int {
	return new(big.Int).Mul(d.unscaled, pow10(scale-d.scale))
}

// pow10 returns 10^n for n up to 2 × 38, the most decimalDiv needs. The result is shared:
// callers must not modify it.
func pow10(n int) *big.Int { return pow10s[n] }

//nolint:gochecknoglobals // a read-only table that saves an Exp on every Decimal op
var pow10s = func() []*big.Int {
	t := make([]*big.Int, 2*catalog.MaxDecimalPrecision+1)
	t[0] = big.NewInt(1)
	for i := 1; i < len(t); i++ {
		t[i] = new(big.Int).Mul(t[i-1], big.NewInt(decimalBase))
	}
	return t
}()

const decimalBase = 10

// decimalFunc computes an op exactly at the result's scale, or returns a reason it fails.
type decimalFunc func(x, y decimal, scale int) (*big.Int, string)

func decimalAdd(x, y decimal, scale int) (*big.Int, string) {
	r := x.at(scale)
	return r.Add(r, y.at(scale)), ""
}

func decimalSub(x, y decimal, scale int) (*big.Int, string) {
	r := x.at(scale)
	return r.Sub(r, y.at(scale)), ""
}

func decimalMul(x, y decimal, scale int) (*big.Int, string) {
	p := decimal{new(big.Int).Mul(x.unscaled, y.unscaled), x.scale + y.scale}
	return p.at(scale), ""
}

// decimalDiv truncates the exact quotient toward zero: x/y at scale s is
// x.unscaled × 10^(s + y.scale) / (y.unscaled × 10^x.scale).
func decimalDiv(x, y decimal, scale int) (*big.Int, string) {
	if y.unscaled.Sign() == 0 {
		return nil, reasonDivByZero
	}
	num := decimal{x.unscaled, 0}.at(scale + y.scale)
	return num.Quo(num, decimal{y.unscaled, 0}.at(x.scale)), ""
}

func decimalOp(fn decimalFunc) evalFunc {
	return binary(func(n *node, a, b Value) (Value, error) {
		r, reason := fn(asDecimal(a, n.args[0].typ), asDecimal(b, n.args[1].typ), n.typ.Scale)
		if reason != "" {
			return Value{}, n.fail(reason)
		}
		return n.decimal(r)
	})
}

// decimal returns the unscaled x as a value of n's Decimal type, which must fit the
// storage of the type's precision class, not its precision (sql.md §5.6).
func (n *node) decimal(x *big.Int) (Value, error) {
	if x.Cmp(n.lo) < 0 || x.Cmp(n.hi) > 0 {
		return Value{}, n.fail(reasonDecimal)
	}
	return Decimal(x), nil
}

const (
	class9  = 9
	class18 = 18
)

// decimalRange returns the smallest and the largest unscaled value that the storage of
// Decimal(precision, S) holds: 32 bits, 64 bits or 38 digits.
func decimalRange(precision int) (*big.Int, *big.Int) {
	switch {
	case precision <= class9:
		return big.NewInt(math.MinInt32), big.NewInt(math.MaxInt32)
	case precision <= class18:
		return big.NewInt(math.MinInt64), big.NewInt(math.MaxInt64)
	}
	hi := new(big.Int).Sub(pow10(catalog.MaxDecimalPrecision), big.NewInt(1))
	return new(big.Int).Neg(hi), hi
}

// converted returns a, converted to the number type t when its own type differs;
// op names the conditional the conversion is for.
func converted(a node, t catalog.Type, op catalog.Op) node {
	numeric := t.Kind == catalog.KindFloat64 || t.Kind == catalog.KindDecimal || intBits(t.Kind) > 0
	if !numeric || a.typ.Kind == t.Kind && a.typ.Scale == t.Scale {
		return a
	}
	n := node{eval: unary(evalConvert), op: op, typ: t, args: []node{a}}
	if t.Kind == catalog.KindDecimal {
		n.lo, n.hi = decimalRange(t.Precision)
	}
	return n
}

func evalConvert(n *node, v Value) (Value, error) {
	if n.typ.Kind == catalog.KindFloat64 {
		return Float(toFloat(v)), nil
	}
	if n.typ.Kind == catalog.KindDecimal {
		return n.decimal(asDecimal(v, n.args[0].typ).at(n.typ.Scale))
	}
	return wrap(twos(v), n.typ), nil
}

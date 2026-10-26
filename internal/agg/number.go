// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// counterType is the type of a count: count, uniqExact and the count inside avg.
func counterType() catalog.Type { return catalog.Type{Kind: catalog.KindUInt64} }

// Limits of an exact Float64 sum m·2^e of at most 2^64 finite values.
const (
	mantissaBits = 53
	// minSumExp is the exponent of the smallest subnormal: every finite value is a multiple of it.
	minSumExp = -1074
	// maxSumBits bounds e + bits(m): 2^64 values below 2^1024.
	maxSumBits = 1024 + 64
)

// total is an exact sum (encoding.md §7.3). A Float64 sum is m·2^e with m odd, or m = e = 0,
// so each value has one form. Other sums are m at the argument's scale, and e is 0.
type total struct {
	m big.Int
	e int
}

func (t *total) add(at catalog.Type, v expr.Value) {
	var x big.Int
	switch {
	case at.Kind == catalog.KindFloat64:
		t.addFloat(v.AsFloat())
		return
	case at.Kind == catalog.KindDecimal:
		t.addScaled(v.AsDecimal(), 0, false)
		return
	case isSigned(at.Kind):
		x.SetInt64(v.AsInt())
	default:
		x.SetUint64(v.AsUint())
	}
	t.addScaled(&x, 0, false)
}

func (t *total) inc() {
	var one big.Int
	t.addScaled(one.SetInt64(1), 0, false)
}

// addFloat adds x exactly: Frexp and Ldexp split it into an integer and an exponent.
func (t *total) addFloat(x float64) {
	if x == 0 {
		return
	}
	frac, exp := math.Frexp(x)
	var m big.Int
	m.SetInt64(int64(math.Ldexp(frac, mantissaBits)))
	t.addScaled(&m, exp-mantissaBits, true)
}

// addScaled adds v·2^e, leaving v unchanged.
func (t *total) addScaled(v *big.Int, e int, dyadic bool) {
	var shifted big.Int
	switch {
	case v.Sign() == 0:
		return
	case t.m.Sign() == 0:
		t.m.Set(v)
		t.e = e
	case e < t.e:
		t.m.Lsh(&t.m, uint(t.e-e))
		t.e = e
		t.m.Add(&t.m, v)
	default:
		t.m.Add(&t.m, shifted.Lsh(v, uint(e-t.e)))
	}
	if !dyadic {
		return
	}
	if t.m.Sign() == 0 {
		t.e = 0
		return
	}
	tz := t.m.TrailingZeroBits()
	t.m.Rsh(&t.m, tz)
	t.e += int(tz)
}

func (t *total) merge(o *total, at catalog.Type) {
	t.addScaled(&o.m, o.e, at.Kind == catalog.KindFloat64)
}

func (t *total) encode(at catalog.Type) store.Value {
	if at.Kind == catalog.KindFloat64 {
		return store.Value{Type: store.B, Bin: t.dyadic()}
	}
	return keys.EncodeExact(&t.m, at.Scale)
}

// dyadic returns the bytes of a Float64 sum: varint(e), a sign byte, then |m| big-endian.
// The exact zero is no bytes.
func (t *total) dyadic() []byte {
	if t.m.Sign() == 0 {
		return []byte{}
	}
	b := binary.AppendVarint(nil, int64(t.e))
	var sign byte
	if t.m.Sign() < 0 {
		sign = 1
	}
	return append(append(b, sign), t.m.Bytes()...)
}

func (t *total) decode(at catalog.Type, a store.Value) error {
	if at.Kind != catalog.KindFloat64 {
		m, err := keys.DecodeExact(a, at.Scale)
		if err != nil {
			return err
		}
		t.m.Set(m)
		return nil
	}
	if a.Type != store.B {
		return errors.New("a Float64 sum must be a DynamoDB B")
	}
	b := a.Bin
	if len(b) == 0 {
		return nil
	}
	e, n := binary.Varint(b)
	if n <= 0 || len(b) < n+2 || b[n] > 1 || e < minSumExp || e > maxSumBits {
		return fmt.Errorf("Float64 sum %x is malformed", b)
	}
	t.m.SetBytes(b[n+1:])
	if b[n] == 1 {
		t.m.Neg(&t.m)
	}
	t.e = int(e)
	if t.m.Bit(0) == 0 || t.e+t.m.BitLen() > maxSumBits || !bytes.Equal(t.dyadic(), b) {
		return fmt.Errorf("Float64 sum %x is not in the form Dynaflux writes", b)
	}
	return nil
}

// float returns the float64 nearest to the total, an evaluation error when that is infinite.
func (t *total) float(at catalog.Type) (float64, error) {
	num := new(big.Int).Set(&t.m)
	den := big.NewInt(1)
	if t.e >= 0 {
		num.Lsh(num, uint(t.e))
	} else {
		den.Lsh(den, uint(-t.e))
	}
	if at.Scale > 0 {
		den.Mul(den, new(big.Int).Exp(big.NewInt(decimalBase), big.NewInt(int64(at.Scale)), nil))
	}
	x, _ := new(big.Rat).SetFrac(num, den).Float64()
	if math.IsInf(x, 0) {
		return 0, evalError("a sum is outside Float64's range")
	}
	return x, nil
}

// final returns the sum as a value of the sum's result type (sql.md §7.1).
func (t *total) final(at catalog.Type) (expr.Value, error) {
	switch {
	case at.Kind == catalog.KindFloat64:
		x, err := t.float(at)
		return expr.Float(x), err
	case at.Kind == catalog.KindDecimal:
		if len(new(big.Int).Abs(&t.m).String()) > catalog.MaxDecimalPrecision {
			return expr.Value{}, evalError("sum %s is outside Decimal(38, %d)", t.m.String(), at.Scale)
		}
		return expr.Decimal(new(big.Int).Set(&t.m)), nil
	case isSigned(at.Kind):
		if !t.m.IsInt64() {
			return expr.Value{}, evalError("sum %s is outside Int64", t.m.String())
		}
		return expr.Int(t.m.Int64()), nil
	}
	if !t.m.IsUint64() {
		return expr.Value{}, evalError("total %s is outside UInt64", t.m.String())
	}
	return expr.Uint(t.m.Uint64()), nil
}

// sum is the state of count, sum and a view's uniqExact: an exact total. A uniqExact counts
// the values whose markers its records own.
type sum struct {
	f   *Func
	t   total
	has bool
}

// acc is the type the total accumulates.
func (s *sum) acc() catalog.Type {
	if s.f.fn == catalog.AggSum {
		return s.f.args[0]
	}
	return counterType()
}

func (s *sum) Add(in Input) error {
	if err := s.f.check(in); err != nil {
		return err
	}
	if s.f.fn == catalog.AggSum {
		s.t.add(s.f.args[0], in.Args[0])
	} else {
		s.t.inc()
	}
	s.has = true
	return nil
}

func (s *sum) Merge(o State) error {
	x, ok := o.(*sum)
	if !ok || x.f != s.f {
		return errMerge(s.f)
	}
	if x.has {
		s.t.merge(&x.t, s.acc())
		s.has = true
	}
	return nil
}

func (s *sum) Empty() bool { return !s.has }

func (s *sum) Encode() (store.Value, error) {
	if !s.has {
		return store.Value{}, errEmpty
	}
	return s.t.encode(s.acc()), nil
}

func (s *sum) Final() (expr.Value, error) {
	if !s.has {
		return s.f.none(zero(s.acc().Kind)), nil
	}
	return s.t.final(s.acc())
}

func (s *sum) decode(a store.Value) error {
	if err := s.t.decode(s.acc(), a); err != nil {
		return err
	}
	if s.f.fn != catalog.AggSum && s.t.m.Sign() <= 0 {
		return fmt.Errorf("count %s is not positive", s.t.m.String())
	}
	s.has = true
	return nil
}

func (s *sum) maxSize() int {
	if s.f.fn == catalog.AggSum {
		return size(worstSum(s.f.args[0]))
	}
	return size(worstCount())
}

// avg is the state of avg: an exact sum and a count.
type avg struct {
	f          *Func
	sum, count total
	has        bool
}

func (s *avg) Add(in Input) error {
	if err := s.f.check(in); err != nil {
		return err
	}
	s.sum.add(s.f.args[0], in.Args[0])
	s.count.inc()
	s.has = true
	return nil
}

func (s *avg) Merge(o State) error {
	x, ok := o.(*avg)
	if !ok || x.f != s.f {
		return errMerge(s.f)
	}
	if x.has {
		s.sum.merge(&x.sum, s.f.args[0])
		s.count.merge(&x.count, counterType())
		s.has = true
	}
	return nil
}

func (s *avg) Empty() bool { return !s.has }

func (s *avg) Encode() (store.Value, error) {
	if !s.has {
		return store.Value{}, errEmpty
	}
	return store.Value{Type: store.L, List: []store.Value{s.sum.encode(s.f.args[0]), s.count.encode(counterType())}}, nil
}

// Final is the exact sum rounded to the nearest float64, divided by the count as a float64.
func (s *avg) Final() (expr.Value, error) {
	if !s.has {
		return s.f.none(expr.Float(0)), nil
	}
	x, err := s.sum.float(s.f.args[0])
	if err != nil {
		return expr.Value{}, err
	}
	n, err := s.count.float(counterType())
	if err != nil {
		return expr.Value{}, err
	}
	return expr.Float(float64(x / n)), nil
}

func (s *avg) decode(a store.Value) error {
	if a.Type != store.L || len(a.List) != 2 {
		return errors.New("an avg state must be a DynamoDB L of a sum and a count")
	}
	if err := s.sum.decode(s.f.args[0], a.List[0]); err != nil {
		return fmt.Errorf("sum: %w", err)
	}
	if err := s.count.decode(counterType(), a.List[1]); err != nil {
		return fmt.Errorf("count: %w", err)
	}
	if s.count.m.Sign() <= 0 {
		return fmt.Errorf("count %s is not positive", s.count.m.String())
	}
	s.has = true
	return nil
}

func (s *avg) maxSize() int {
	return size(store.Value{Type: store.L, List: []store.Value{worstSum(s.f.args[0]), worstCount()}})
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"fmt"
	"math"
	"math/big"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
)

// operand is a compiled expression over a query's parameters, with its type.
type operand struct {
	p *expr.Program
	t catalog.Type
}

// binding is a key column bound by equality or a set.
type binding struct {
	col    int // the column's index in the query's source row
	typ    catalog.Type
	values []operand
}

// keyRange bounds the key column after the bound ones.
type keyRange struct {
	col          int
	typ          catalog.Type
	lower, upper *bound
}

type bound struct {
	operand

	inclusive bool
}

// typedBound is a range bound's value with its own type, for the exact re-check of rows.
type typedBound struct {
	v         expr.Value
	t         catalog.Type
	inclusive bool
}

// read is what one call of a query reads: every combination of the bound columns' values,
// in key order, and the range of the next key column, converted to its type.
type read struct {
	combos [][]expr.Value
	lo, hi *keys.Bound
	// loV and hiV are the bounds as given, which rows are checked against.
	loV, hiV *typedBound
	empty    bool // no row can match: the read sends no request
}

// planRead evaluates the key plan of q for params.
func (q *query) planRead(params []expr.Value) (read, error) {
	var r read
	sets := make([][]expr.Value, len(q.bound))
	for i := range q.bound {
		var err error
		if sets[i], err = q.bound[i].eval(params); err != nil {
			return read{}, err
		}
	}
	if slices.ContainsFunc(sets, func(vs []expr.Value) bool { return len(vs) == 0 }) {
		return read{empty: true}, nil
	}
	n := 1
	for _, vs := range sets {
		// Checked per set, so the product never overflows.
		if n *= len(vs); n > catalog.MaxKeyCombinations {
			return read{}, fmt.Errorf("%w: more than %d", ErrTooManyKeys, catalog.MaxKeyCombinations)
		}
	}
	r.combos = product(sets)
	if q.rng == nil {
		return r, nil
	}
	var err error
	kt := keyType(q.rng.typ)
	if r.lo, r.loV, r.empty, err = kt.convertBound(q.rng.lower, params, true); err != nil || r.empty {
		return r, err
	}
	r.hi, r.hiV, r.empty, err = kt.convertBound(q.rng.upper, params, false)
	return r, err
}

// eval returns the distinct values of a binding that the key type can hold exactly; an
// Array value contributes its elements.
func (b *binding) eval(params []expr.Value) ([]expr.Value, error) {
	var out []expr.Value
	seen := map[string]bool{}
	for _, o := range b.values {
		v, err := o.p.Eval(&expr.Frame{Params: params})
		if err != nil {
			return nil, err
		}
		elems, t := []expr.Value{v}, o.t
		if t.Array {
			elems, t = v.AsArray(), catalog.Type{Kind: t.Kind, Precision: t.Precision, Scale: t.Scale}
		}
		for _, x := range elems {
			if x.IsNull() {
				continue
			}
			c := keyType(b.typ).convert(x, t, 0)
			if !c.fits() {
				continue
			}
			f, ferr := keys.AppendField(nil, b.typ, c.v)
			if ferr != nil {
				return nil, ferr
			}
			if !seen[string(f)] {
				seen[string(f)] = true
				out = append(out, c.v)
			}
		}
	}
	return out, nil
}

// product returns every combination of one value from each set, the first set outermost.
func product(sets [][]expr.Value) [][]expr.Value {
	combos := [][]expr.Value{nil}
	for _, set := range sets {
		next := make([][]expr.Value, 0, len(combos)*len(set))
		for _, c := range combos {
			for _, v := range set {
				next = append(next, append(slices.Clip(c), v))
			}
		}
		combos = next
	}
	return combos
}

// keyType is a key column's type, which bound values convert to.
type keyType catalog.Type

// converted is a value converted to a key type.
type converted struct {
	v     expr.Value
	exact bool
	// under and over report a value below or above every value of the key type.
	under, over bool
}

func (c converted) fits() bool { return c.exact && !c.under && !c.over }

// convertBound converts a range bound inward: a lower bound to the smallest key value at or
// above it, an upper bound to the largest at or below it. It returns nil for a bound that
// every key value passes, and empty for one that none does.
func (kt keyType) convertBound(b *bound, params []expr.Value, lower bool) (*keys.Bound, *typedBound, bool, error) {
	if b == nil {
		return nil, nil, false, nil
	}
	v, err := b.p.Eval(&expr.Frame{Params: params})
	if err != nil || v.IsNull() {
		return nil, nil, v.IsNull(), err
	}
	dir := -1
	if lower {
		dir = 1
	}
	c := kt.convert(v, b.t, dir)
	tb := &typedBound{v: v, t: b.t, inclusive: b.inclusive}
	switch {
	case lower && c.over, !lower && c.under:
		return nil, nil, true, nil
	case lower && c.under, !lower && c.over:
		return nil, tb, false, nil
	}
	return &keys.Bound{Value: c.v, Inclusive: b.inclusive || !c.exact}, tb, false, nil
}

// convert converts v, of type vt, to the key type: exactly for dir 0, else to the nearest
// key value below (dir < 0) or above (dir > 0).
func (kt keyType) convert(v expr.Value, vt catalog.Type, dir int) converted {
	t := catalog.Type(kt)
	switch t.Kind {
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64,
		catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64, catalog.KindDecimal:
		return kt.convertNumber(v, vt, dir)
	case catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		return kt.convertTime(v.AsMicros(), dir)
	case catalog.KindString, catalog.KindFloat64, catalog.KindBool, catalog.KindUUID:
	}
	return converted{v: v, exact: true}
}

func (kt keyType) convertNumber(v expr.Value, vt catalog.Type, dir int) converted {
	scale := 0
	if kt.Kind == catalog.KindDecimal {
		scale = kt.Scale
	}
	r := exactRat(v, vt)
	r.Mul(r, new(big.Rat).SetInt(expr.Pow10(scale)))
	n := new(big.Int).Div(r.Num(), r.Denom()) // the floor: Denom is positive
	exact := r.IsInt()
	if !exact && dir > 0 {
		n.Add(n, big.NewInt(1))
	}
	lo, hi := kt.numberRange()
	c := converted{exact: exact, under: n.Cmp(lo) < 0, over: n.Cmp(hi) > 0}
	switch {
	case c.under || c.over:
	case kt.Kind == catalog.KindDecimal:
		c.v = expr.Decimal(n)
	case kindOf(kt.Kind) == expr.KindInt:
		c.v = expr.Int(n.Int64())
	default:
		c.v = expr.Uint(n.Uint64())
	}
	return c
}

// exactRat returns the exact value of a number of type t.
func exactRat(v expr.Value, t catalog.Type) *big.Rat {
	switch v.Kind() { //nolint:exhaustive // a bound on a number key is a number
	case expr.KindInt:
		return new(big.Rat).SetInt64(v.AsInt())
	case expr.KindUint:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(v.AsUint()))
	case expr.KindFloat:
		return new(big.Rat).SetFloat64(v.AsFloat())
	case expr.KindDecimal:
		return new(big.Rat).SetFrac(new(big.Int).Set(v.AsDecimal()), expr.Pow10(t.Scale))
	}
	return new(big.Rat)
}

// numberRange returns the smallest and largest values of a number key type, in units of
// its scale.
func (kt keyType) numberRange() (*big.Int, *big.Int) {
	switch kt.Kind { //nolint:exhaustive // only number key types
	case catalog.KindDecimal:
		hi := new(big.Int).Sub(expr.Pow10(kt.Precision), big.NewInt(1))
		return new(big.Int).Neg(hi), hi
	case catalog.KindUInt8:
		return big.NewInt(0), big.NewInt(math.MaxUint8)
	case catalog.KindUInt16:
		return big.NewInt(0), big.NewInt(math.MaxUint16)
	case catalog.KindUInt32:
		return big.NewInt(0), big.NewInt(math.MaxUint32)
	case catalog.KindUInt64:
		return big.NewInt(0), new(big.Int).SetUint64(math.MaxUint64)
	case catalog.KindInt8:
		return big.NewInt(math.MinInt8), big.NewInt(math.MaxInt8)
	case catalog.KindInt16:
		return big.NewInt(math.MinInt16), big.NewInt(math.MaxInt16)
	case catalog.KindInt32:
		return big.NewInt(math.MinInt32), big.NewInt(math.MaxInt32)
	}
	return big.NewInt(math.MinInt64), big.NewInt(math.MaxInt64)
}

// convertTime rounds a time to a whole unit of the key type: a day, a second, a
// millisecond or a microsecond.
func (kt keyType) convertTime(us int64, dir int) converted {
	const (
		micro  = 1
		milli  = 1_000
		second = 1_000_000
		day    = 86_400 * second
	)
	unit := int64(micro)
	switch {
	case kt.Kind == catalog.KindDate:
		unit = day
	case kt.Kind == catalog.KindDateTime:
		unit = second
	case kt.Precision == catalog.DateTime64Millis:
		unit = milli
	}
	n := us / unit
	if us%unit != 0 && us < 0 {
		n-- // the floor, below zero too
	}
	n *= unit
	exact := n == us
	if !exact && dir > 0 {
		n += unit
	}
	last := (catalog.TimeLimitMicros - 1) / unit * unit
	return converted{v: expr.Micros(n), exact: exact, under: n < 0, over: n > last}
}

// inRange reports whether a row's value v of the range column passes the bounds as given.
func (r *read) inRange(v expr.Value, t catalog.Type) bool {
	if lo := r.loV; lo != nil {
		if c := expr.Compare(v, t, lo.v, lo.t); c < 0 || c == 0 && !lo.inclusive {
			return false
		}
	}
	if hi := r.hiV; hi != nil {
		if c := expr.Compare(v, t, hi.v, hi.t); c > 0 || c == 0 && !hi.inclusive {
			return false
		}
	}
	return true
}

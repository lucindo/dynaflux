// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr

import (
	"cmp"
	"math"
	"math/big"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Compare returns -1, 0 or +1 as a is less than, equal to or greater than b, for types
// that sql.md §5.7 lets compare. Numbers compare by exact value whatever their types: the
// UInt64 9007199254740993 is greater than the Float64 9007199254740992. Temporal values
// compare as instants, strings byte by byte, Bool false before true, and UUIDs by their
// bytes. NULL equals NULL and comes after every other value, as in the default NULLS LAST
// of sql.md §10.8; a sort for DESC or NULLS FIRST must place NULLs itself, not negate.
func Compare(a Value, at catalog.Type, b Value, bt catalog.Type) int {
	switch {
	case a.IsNull() && b.IsNull():
		return 0
	case a.IsNull():
		return 1
	case b.IsNull():
		return -1
	case (a.kind == KindDecimal || b.kind == KindDecimal) && a.kind != KindFloat && b.kind != KindFloat:
		return compareDecimal(asDecimal(a, at), asDecimal(b, bt))
	case a.kind == KindDecimal || b.kind == KindDecimal:
		return exact(a, at).Cmp(exact(b, bt))
	case a.kind == KindFloat && b.kind == KindFloat:
		return cmp.Compare(a.AsFloat(), b.AsFloat())
	case a.kind == KindFloat:
		return compareFloat(a.AsFloat(), b)
	case b.kind == KindFloat:
		return -compareFloat(b.AsFloat(), a)
	case a.kind == KindInt && b.kind == KindUint:
		return compareIntUint(a.i, b.n)
	case a.kind == KindUint && b.kind == KindInt:
		return -compareIntUint(b.i, a.n)
	case a.kind == KindInt || a.kind == KindMicros:
		return cmp.Compare(a.i, b.i)
	case a.kind == KindUint || a.kind == KindBool:
		return cmp.Compare(a.n, b.n)
	}
	return strings.Compare(a.s, b.s)
}

func compareIntUint(i int64, u uint64) int {
	if i < 0 {
		return -1
	}
	return cmp.Compare(uint64(i), u)
}

// compareFloat compares f with an integer exactly: the integer parts first, in integer
// arithmetic, then f's fraction.
func compareFloat(f float64, v Value) int {
	const two63, two64 = 0x1p63, 0x1p64
	t := math.Trunc(f)
	if v.kind == KindUint {
		switch {
		case f < 0:
			return -1
		case f >= two64:
			return 1
		}
		if c := cmp.Compare(uint64(t), v.n); c != 0 {
			return c
		}
		return cmp.Compare(f, t)
	}
	switch {
	case f < -two63:
		return -1
	case f >= two63:
		return 1
	}
	if c := cmp.Compare(int64(t), v.i); c != 0 {
		return c
	}
	return cmp.Compare(f, t)
}

func compareDecimal(x, y decimal) int {
	switch {
	case x.scale < y.scale:
		return x.at(y.scale).Cmp(y.unscaled)
	case x.scale > y.scale:
		return x.unscaled.Cmp(y.at(x.scale))
	}
	return x.unscaled.Cmp(y.unscaled)
}

// exact returns a Decimal of type t or a Float64 as an exact rational.
func exact(v Value, t catalog.Type) *big.Rat {
	if v.kind == KindFloat {
		return new(big.Rat).SetFloat64(v.AsFloat())
	}
	return new(big.Rat).SetFrac(v.d, Pow10(t.Scale))
}

func evalCompare(test func(c int) bool) evalFunc {
	return binary(func(n *node, a, b Value) (Value, error) {
		return Bool(test(Compare(a, n.args[0].typ, b, n.args[1].typ))), nil
	})
}

// evalIn tests Args[0] against each later arg, a scalar or the elements of an Array. A
// NULL item never matches.
func evalIn(n *node, f *Frame) (Value, error) {
	x, err := n.arg(0, f)
	if err != nil || x.IsNull() {
		return x, err
	}
	xt := n.args[0].typ
	for i := 1; i < len(n.args); i++ {
		item, itemErr := n.arg(i, f)
		if itemErr != nil {
			return Value{}, itemErr
		}
		t := n.args[i].typ
		items := []Value{item}
		if t.Array {
			t.Array = false
			items = item.AsArray()
		}
		for _, v := range items {
			if Compare(x, xt, v, t) == 0 {
				return Bool(true), nil
			}
		}
	}
	return Bool(false), nil
}

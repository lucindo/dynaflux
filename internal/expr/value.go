// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package expr evaluates the catalog's expressions over SQL values.
package expr

import (
	"math"
	"math/big"
)

// Value is one SQL value. It holds a representation only, which [Value.Kind] names: the
// catalog type of the expression that made it says which constructor built it, and only
// that constructor's accessor reads it meaningfully. The zero Value is NULL.
type Value struct {
	kind Kind
	n    uint64   // Bool, Uint, Float bits
	i    int64    // Int, Micros
	s    string   // String, UUID bytes
	d    *big.Int // Decimal
	a    []Value  // Array
}

// Kind is the representation of a [Value]: the constructor that built it.
type Kind uint8

// Kinds, one per constructor: [Null] makes KindNull, [Bool] makes KindBool, and so on.
const (
	KindNull Kind = iota
	KindBool
	KindInt
	KindUint
	KindFloat
	KindDecimal
	KindString
	KindUUID
	KindMicros
	KindArray
)

// Null returns NULL.
func Null() Value { return Value{} }

// Bool returns a Bool.
func Bool(b bool) Value {
	v := Value{kind: KindBool}
	if b {
		v.n = 1
	}
	return v
}

// Int returns a value of Int8 to Int64.
func Int(i int64) Value { return Value{kind: KindInt, i: i} }

// Uint returns a value of UInt8 to UInt64.
func Uint(u uint64) Value { return Value{kind: KindUint, n: u} }

// Float returns a Float64. f must be finite; −0 becomes 0, the only zero Float64 has.
func Float(f float64) Value {
	if f == 0 {
		return Value{kind: KindFloat}
	}
	return Value{kind: KindFloat, n: math.Float64bits(f)}
}

// Decimal returns a Decimal(P, S) from its unscaled value; the type gives S. The Value
// takes ownership of unscaled.
func Decimal(unscaled *big.Int) Value { return Value{kind: KindDecimal, d: unscaled} }

// String returns a String.
func String(s string) Value { return Value{kind: KindString, s: s} }

// UUID returns a UUID.
func UUID(b [16]byte) Value { return Value{kind: KindUUID, s: string(b[:])} }

// Micros returns a Date, DateTime or DateTime64: microseconds since 1970-01-01 UTC.
func Micros(us int64) Value { return Value{kind: KindMicros, i: us} }

// Array returns an Array, never NULL even when vs is empty. The Value takes ownership of vs.
func Array(vs []Value) Value { return Value{kind: KindArray, a: vs} }

// Kind returns v's representation.
func (v Value) Kind() Kind { return v.kind }

// IsNull reports whether v is NULL.
func (v Value) IsNull() bool { return v.kind == KindNull }

// AsBool returns the value of a [Bool].
func (v Value) AsBool() bool { return v.n != 0 }

// IsTrue reports whether a condition's value v holds: NULL counts as false.
func (v Value) IsTrue() bool { return !v.IsNull() && v.AsBool() }

// AsInt returns the value of an [Int].
func (v Value) AsInt() int64 { return v.i }

// AsUint returns the value of a [Uint].
func (v Value) AsUint() uint64 { return v.n }

// AsFloat returns the value of a [Float].
func (v Value) AsFloat() float64 { return math.Float64frombits(v.n) }

// AsDecimal returns the unscaled value of a [Decimal]. The caller must not modify it.
func (v Value) AsDecimal() *big.Int { return v.d }

// AsString returns the value of a [String].
func (v Value) AsString() string { return v.s }

// AsUUID returns the value of a [UUID].
func (v Value) AsUUID() [16]byte {
	var b [16]byte
	copy(b[:], v.s)
	return b
}

// AsMicros returns the value of a [Micros].
func (v Value) AsMicros() int64 { return v.i }

// AsArray returns the elements of an [Array]. The caller must not modify them.
func (v Value) AsArray() []Value { return v.a }

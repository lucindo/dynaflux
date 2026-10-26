// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr_test

import (
	"math"
	"math/big"
	"testing"

	"github.com/lucindo/dynaflux/internal/expr"
)

func TestOnlyNullIsNull(t *testing.T) {
	t.Parallel()
	if !expr.Null().IsNull() || !(expr.Value{}).IsNull() {
		t.Error("Null() and the zero Value must be NULL")
	}
	zeros := map[string]expr.Value{
		"Bool(false)":  expr.Bool(false),
		"Int(0)":       expr.Int(0),
		"Uint(0)":      expr.Uint(0),
		"Float(0)":     expr.Float(0),
		"Decimal(0)":   expr.Decimal(new(big.Int)),
		"String(\"\")": expr.String(""),
		"UUID(zero)":   expr.UUID([16]byte{}),
		"Micros(0)":    expr.Micros(0),
		"Array(nil)":   expr.Array(nil),
	}
	for name, v := range zeros {
		if v.IsNull() {
			t.Errorf("%s is NULL", name)
		}
	}
}

func TestKindNamesTheConstructor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		v    expr.Value
		want expr.Kind
	}{
		{"zero Value", expr.Value{}, expr.KindNull},
		{"Null", expr.Null(), expr.KindNull},
		{"Bool", expr.Bool(false), expr.KindBool},
		{"Int", expr.Int(0), expr.KindInt},
		{"Uint", expr.Uint(0), expr.KindUint},
		{"Float", expr.Float(0), expr.KindFloat},
		{"Decimal", expr.Decimal(new(big.Int)), expr.KindDecimal},
		{"String", expr.String(""), expr.KindString},
		{"UUID", expr.UUID([16]byte{}), expr.KindUUID},
		{"Micros", expr.Micros(0), expr.KindMicros},
		{"Array", expr.Array(nil), expr.KindArray},
	}
	for _, c := range cases {
		if got := c.v.Kind(); got != c.want {
			t.Errorf("%s: Kind = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestAccessorsReturnConstructorInput(t *testing.T) {
	t.Parallel()
	id := [16]byte{0x61, 0xf0, 0xc4, 0x04, 0x5c, 0xb3, 0x11, 0xe7, 0x90, 0x7b, 0xa6, 0x00, 0x6a, 0xd3, 0xdb, 0xa0}
	checks := map[string]bool{
		"Bool":               expr.Bool(true).AsBool(),
		"Int min":            expr.Int(math.MinInt64).AsInt() == math.MinInt64,
		"Int negative":       expr.Int(-42).AsInt() == -42,
		"Uint max":           expr.Uint(math.MaxUint64).AsUint() == math.MaxUint64,
		"Float":              expr.Float(-1.5).AsFloat() == -1.5,
		"Float smallest":     expr.Float(math.SmallestNonzeroFloat64).AsFloat() == math.SmallestNonzeroFloat64,
		"Decimal":            expr.Decimal(big.NewInt(-1234)).AsDecimal().Cmp(big.NewInt(-1234)) == 0,
		"String":             expr.String("héllo").AsString() == "héllo",
		"UUID":               expr.UUID(id).AsUUID() == id,
		"Micros":             expr.Micros(1717163100250123).AsMicros() == 1717163100250123,
		"Micros before 1970": expr.Micros(-1).AsMicros() == -1,
	}
	for name, ok := range checks {
		if !ok {
			t.Errorf("%s: accessor doesn't return the constructor's input", name)
		}
	}
	arr := expr.Array([]expr.Value{expr.String("a"), expr.Null()}).AsArray()
	if len(arr) != 2 || arr[0].AsString() != "a" || !arr[1].IsNull() {
		t.Errorf("Array elements = %v, want [String(a) NULL]", arr)
	}
}

func TestFloatNegativeZeroIsZero(t *testing.T) {
	t.Parallel()
	if f := expr.Float(math.Copysign(0, -1)).AsFloat(); f != 0 || math.Signbit(f) {
		t.Errorf("Float(-0) = %v with sign bit %v, want +0", f, math.Signbit(f))
	}
}

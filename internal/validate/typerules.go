// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"fmt"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Integer widths and Decimal precision classes (sql.md §4.5, §5.6).
const (
	bits8  = 8
	bits16 = 16
	bits32 = 32
	bits64 = 64

	class9  = 9
	class18 = 18
	class38 = 38
)

// typeError is a type rule that an expression breaks.
type typeError struct {
	code, hint, msg string
}

func newTypeError(code, hint, format string, args ...any) *typeError {
	return &typeError{code: code, hint: hint, msg: fmt.Sprintf(format, args...)}
}

func scalarOf(t catalog.Type) catalog.Type {
	t.Nullable = false
	return t
}

func isInteger(t catalog.Type) bool {
	return !t.Array && t.Kind >= catalog.KindUInt8 && t.Kind <= catalog.KindInt64
}

func isNumeric(t catalog.Type) bool {
	return isInteger(t) || !t.Array && (t.Kind == catalog.KindFloat64 || t.Kind == catalog.KindDecimal)
}

func isFloat(t catalog.Type) bool   { return !t.Array && t.Kind == catalog.KindFloat64 }
func isDecimal(t catalog.Type) bool { return !t.Array && t.Kind == catalog.KindDecimal }

func isTemporal(t catalog.Type) bool {
	return !t.Array && (t.Kind == catalog.KindDate || t.Kind == catalog.KindDateTime || t.Kind == catalog.KindDateTime64)
}

func isOrderable(t catalog.Type) bool {
	return isNumeric(t) || isTemporal(t) || !t.Array && (t.Kind == catalog.KindString || t.Kind == catalog.KindBool)
}

// isKeyType reports whether t can be part of a stream or view key (sql.md §4.4).
func isKeyType(t catalog.Type) bool {
	if t.Nullable || t.Array || t.Kind == catalog.KindFloat64 {
		return false
	}
	return isOrderable(t) || t.Kind == catalog.KindUUID
}

func isBool(t catalog.Type) bool { return !t.Array && t.Kind == catalog.KindBool }

// intBits returns the width of an integer kind.
func intBits(k catalog.Kind) int {
	switch k {
	case catalog.KindUInt8, catalog.KindInt8:
		return bits8
	case catalog.KindUInt16, catalog.KindInt16:
		return bits16
	case catalog.KindUInt32, catalog.KindInt32:
		return bits32
	case catalog.KindUInt64, catalog.KindInt64, catalog.KindString, catalog.KindFloat64, catalog.KindDecimal,
		catalog.KindBool, catalog.KindUUID, catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
	}
	return bits64
}

// wider returns the next integer width after bits, at most 64.
func wider(bits int) int {
	const step = 2 // each width doubles the one before
	return min(bits64, step*bits)
}

func isSigned(k catalog.Kind) bool {
	return k >= catalog.KindInt8 && k <= catalog.KindInt64
}

func intType(bits int, signed bool) catalog.Type {
	kinds := []catalog.Kind{catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64}
	if signed {
		kinds = []catalog.Kind{catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64}
	}
	for _, k := range kinds {
		if intBits(k) >= bits {
			return catalog.Type{Kind: k}
		}
	}
	return catalog.Type{Kind: kinds[len(kinds)-1]}
}

func decimalClass(precision int) int {
	switch {
	case precision <= class9:
		return class9
	case precision <= class18:
		return class18
	}
	return class38
}

func decimalType(precision, scale int) catalog.Type {
	return catalog.Type{Kind: catalog.KindDecimal, Precision: precision, Scale: scale}
}

// commonType returns the common type of if, multiIf and coalesce values (sql.md §4.5).
// It is Nullable when nullable is set or any type is.
func commonType(types []catalog.Type, nullable bool) (catalog.Type, *typeError) {
	bases := make([]catalog.Type, len(types))
	for i, t := range types {
		nullable = nullable || t.Nullable
		bases[i] = scalarOf(t)
	}
	t, err := commonBase(bases)
	if err != nil {
		return catalog.Type{}, err
	}
	if nullable && t.Array {
		return catalog.Type{}, newTypeError("DF5308", "for Decimals, use one scale", "%s and NULL have no common type", t)
	}
	t.Nullable = nullable
	return t, nil
}

func commonBase(types []catalog.Type) (catalog.Type, *typeError) {
	first := types[0]
	if !slices.ContainsFunc(types, func(t catalog.Type) bool { return t != first }) {
		return first, nil
	}
	noCommon := newTypeError("DF5308", "for Decimals, use one scale", "%s have no common type", typeList(types))
	switch {
	case !slices.ContainsFunc(types, func(t catalog.Type) bool { return !isTemporal(t) }):
		return commonTemporal(types), nil
	case slices.ContainsFunc(types, func(t catalog.Type) bool { return !isNumeric(t) }):
		return catalog.Type{}, noCommon
	case slices.ContainsFunc(types, isDecimal) && slices.ContainsFunc(types, isFloat):
		return catalog.Type{}, decimalFloatError()
	case slices.ContainsFunc(types, isDecimal):
		return commonDecimal(types, noCommon)
	case slices.ContainsFunc(types, isFloat):
		if slices.ContainsFunc(types, func(t catalog.Type) bool { return isInteger(t) && intBits(t.Kind) == bits64 }) {
			return catalog.Type{}, noCommon
		}
		return catalog.Type{Kind: catalog.KindFloat64}, nil
	}
	return commonInteger(types, noCommon)
}

func commonTemporal(types []catalog.Type) catalog.Type {
	out := catalog.Type{Kind: catalog.KindDate}
	for _, t := range types {
		switch {
		case t.Kind == catalog.KindDateTime64:
			out.Kind, out.Precision = t.Kind, max(out.Precision, t.Precision)
		case t.Kind == catalog.KindDateTime && out.Kind == catalog.KindDate:
			out.Kind = t.Kind
		}
	}
	return out
}

func commonInteger(types []catalog.Type, noCommon *typeError) (catalog.Type, *typeError) {
	unsigned, signed := 0, 0
	for _, t := range types {
		if isSigned(t.Kind) {
			signed = max(signed, intBits(t.Kind))
		} else {
			unsigned = max(unsigned, intBits(t.Kind))
		}
	}
	switch {
	case signed == 0:
		return intType(unsigned, false), nil
	case unsigned == 0:
		return intType(signed, true), nil
	case unsigned == bits64:
		return catalog.Type{}, noCommon
	}
	return intType(max(signed, wider(unsigned)), true), nil
}

// commonDecimal combines Decimals of one scale with integers.
func commonDecimal(types []catalog.Type, noCommon *typeError) (catalog.Type, *typeError) {
	scale, precision := -1, 0
	for _, t := range types {
		if !isDecimal(t) {
			continue
		}
		if scale >= 0 && t.Scale != scale {
			return catalog.Type{}, noCommon
		}
		scale, precision = t.Scale, max(precision, decimalClass(t.Precision))
	}
	for _, t := range types {
		if isInteger(t) {
			precision = max(precision, scale+intDigits(t.Kind, scale))
		}
	}
	if precision > catalog.MaxDecimalPrecision {
		return catalog.Type{}, noCommon
	}
	return decimalType(decimalClass(precision), scale), nil
}

// intDigits is the digit count an integer type adds to a Decimal of the given scale.
func intDigits(k catalog.Kind, scale int) int {
	const (
		digits8, digits16, digits32, digitsInt64, digitsUInt64 = 3, 5, 10, 19, 20
		// As in ClickHouse, a scale of 0 counts Int32 and Int64 a digit short.
		digitsInt32Scale0, digitsInt64Scale0 = 9, 18
	)
	switch {
	case k == catalog.KindInt32 && scale == 0:
		return digitsInt32Scale0
	case k == catalog.KindInt64 && scale == 0:
		return digitsInt64Scale0
	case k == catalog.KindUInt64:
		return digitsUInt64
	case k == catalog.KindInt64:
		return digitsInt64
	}
	switch intBits(k) {
	case bits8:
		return digits8
	case bits16:
		return digits16
	}
	return digits32
}

func typeList(types []catalog.Type) string {
	var names []string
	for _, t := range types {
		if s := t.String(); !slices.Contains(names, s) {
			names = append(names, s)
		}
	}
	if len(names) == 1 {
		return names[0] + " and " + names[0]
	}
	return strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
}

func decimalFloatError() *typeError {
	return newTypeError("DF5309", "use Decimal values, and scale with integers: x * 11 / 10", "can't combine Decimal and Float64")
}

// Arithmetic operators, as diagnostics name them.
const (
	opPlus   = "+"
	opMinus  = "-"
	opTimes  = "*"
	opDivide = "/"
	opModulo = "%"
	opIntDiv = "intDiv"
)

// arithType returns the result type of a binary arithmetic operator (sql.md §5.6).
func arithType(op string, a, b catalog.Type) (catalog.Type, *typeError) {
	integral := op == opModulo || op == opIntDiv
	for _, t := range []catalog.Type{a, b} {
		switch {
		case !isNumeric(t):
			return catalog.Type{}, numericError(op, integral, t)
		case integral && !isInteger(t):
			return catalog.Type{}, numericError(op, true, t)
		}
	}
	nullable := a.Nullable || b.Nullable
	a, b = scalarOf(a), scalarOf(b)
	var t catalog.Type
	var err *typeError
	switch {
	case isDecimal(a) && isFloat(b) || isFloat(a) && isDecimal(b):
		return catalog.Type{}, decimalFloatError()
	case isDecimal(a) || isDecimal(b):
		t, err = decimalArith(op, a, b)
	case isFloat(a) || isFloat(b) || op == opDivide:
		t = catalog.Type{Kind: catalog.KindFloat64}
	default:
		t = intArith(op, a.Kind, b.Kind)
	}
	t.Nullable = nullable
	return t, err
}

func numericError(op string, integral bool, t catalog.Type) *typeError {
	want := "numeric"
	if integral {
		want = "integer"
	}
	return newTypeError("DF5312", "", "%s needs %s operands, not %s", op, want, t)
}

func intArith(op string, a, b catalog.Kind) catalog.Type {
	signed := isSigned(a) || isSigned(b)
	widest := wider(max(intBits(a), intBits(b)))
	switch op {
	case opMinus:
		return intType(widest, true)
	case opModulo:
		if !isSigned(a) {
			return intType(intBits(b), false)
		}
		return intType(wider(intBits(b)), true)
	case opIntDiv:
		return intType(intBits(a), signed)
	}
	return intType(widest, signed)
}

func decimalArith(op string, a, b catalog.Type) (catalog.Type, *typeError) {
	if !isDecimal(a) {
		return b, nil
	}
	if !isDecimal(b) {
		return a, nil
	}
	class := max(decimalClass(a.Precision), decimalClass(b.Precision))
	scale := max(a.Scale, b.Scale)
	switch op {
	case opTimes:
		scale = a.Scale + b.Scale
	case opDivide:
		scale = a.Scale
		if a.Scale < b.Scale {
			return catalog.Type{}, newTypeError("DF5310", "", "Decimal scale %d is out of range for %s / %s", a.Scale, a, b)
		}
	}
	if scale > class {
		return catalog.Type{}, newTypeError("DF5310", "", "Decimal scale %d is out of range for %s %s %s", scale, a, op, b)
	}
	return decimalType(class, scale), nil
}

// negateType returns the type of -x (sql.md §5.6).
func negateType(t catalog.Type) (catalog.Type, *typeError) {
	if !isNumeric(t) {
		return catalog.Type{}, numericError(opMinus, false, t)
	}
	if isInteger(t) && !isSigned(t.Kind) {
		out := intType(wider(intBits(t.Kind)), true)
		out.Nullable = t.Nullable
		return out, nil
	}
	return t, nil
}

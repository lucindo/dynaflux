// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"fmt"
	"strconv"
	"strings"
)

// Kind is the scalar kind of a [Type].
type Kind uint8

// Scalar kinds. DateTime is only a result type: the bucket functions return it.
const (
	KindString Kind = iota + 1
	KindUInt8
	KindUInt16
	KindUInt32
	KindUInt64
	KindInt8
	KindInt16
	KindInt32
	KindInt64
	KindFloat64
	KindDecimal
	KindBool
	KindUUID
	KindDate
	KindDateTime
	KindDateTime64
)

// kindLast is the last Kind: a Kind added after it must become kindLast.
const kindLast = KindDateTime64

// Limits of the parameterized types.
const (
	// MaxDecimalPrecision is the largest P of Decimal(P, S): a DynamoDB number holds 38 digits.
	MaxDecimalPrecision = 38
	// MaxArrayLen is the most elements an Array value holds. An append with a longer array
	// fails, which bounds the rows that ARRAY JOIN makes from one event.
	MaxArrayLen = 1000
	// DateTime64Millis is the precision of DateTime64(3).
	DateTime64Millis = 3
	// DateTime64Micros is the precision of DateTime64(6).
	DateTime64Micros = 6
)

// String returns the ClickHouse name of k, such as "UInt32" or "Decimal".
func (k Kind) String() string {
	switch k {
	case KindString:
		return "String"
	case KindUInt8:
		return "UInt8"
	case KindUInt16:
		return "UInt16"
	case KindUInt32:
		return "UInt32"
	case KindUInt64:
		return "UInt64"
	case KindInt8:
		return "Int8"
	case KindInt16:
		return "Int16"
	case KindInt32:
		return "Int32"
	case KindInt64:
		return "Int64"
	case KindFloat64:
		return "Float64"
	case KindDecimal:
		return "Decimal"
	case KindBool:
		return "Bool"
	case KindUUID:
		return "UUID"
	case KindDate:
		return "Date"
	case KindDateTime:
		return "DateTime"
	case KindDateTime64:
		return "DateTime64"
	}
	return "Kind(" + strconv.Itoa(int(k)) + ")"
}

// Type is a resolved ClickHouse type. Its text form is the ClickHouse type name, such as
// Nullable(String) or Decimal(10, 2), and [ParseType] reads it back.
//
// LowCardinality isn't represented: LowCardinality(String) means exactly String, and the
// compiler resolves it so.
type Type struct {
	Kind Kind
	// Precision is P of Decimal(P, S), or the fractional digits of DateTime64.
	Precision int
	// Scale is S of Decimal(P, S).
	Scale int
	// Nullable makes the type Nullable(T).
	Nullable bool
	// Array makes the type Array(T). An Array is never Nullable.
	Array bool
}

// ParseType parses a type in the exact form [Type.String] writes. Other spellings, such as
// "Decimal(10,2)" or LowCardinality(String), are errors.
func ParseType(s string) (Type, error) {
	t, ok := parseType(s)
	if !ok || t.String() != s {
		return Type{}, fmt.Errorf("invalid type %q", s)
	}
	if err := t.check(); err != nil {
		return Type{}, err
	}
	return t, nil
}

// String returns the ClickHouse type name.
func (t Type) String() string {
	s := t.scalar()
	if t.Array {
		return "Array(" + s + ")"
	}
	if t.Nullable {
		return "Nullable(" + s + ")"
	}
	return s
}

// MarshalText implements [encoding.TextMarshaler]. It fails for an invalid type.
func (t Type) MarshalText() ([]byte, error) {
	if err := t.check(); err != nil {
		return nil, err
	}
	return []byte(t.String()), nil
}

// UnmarshalText implements [encoding.TextUnmarshaler] with [ParseType].
func (t *Type) UnmarshalText(text []byte) error {
	parsed, err := ParseType(string(text))
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}

func (t Type) scalar() string {
	if t.Kind == KindDecimal {
		return "Decimal(" + strconv.Itoa(t.Precision) + ", " + strconv.Itoa(t.Scale) + ")"
	}
	if t.Kind == KindDateTime64 {
		return "DateTime64(" + strconv.Itoa(t.Precision) + ")"
	}
	return t.Kind.String()
}

func (t Type) check() error {
	switch {
	case t.Kind < KindString || t.Kind > kindLast:
		return fmt.Errorf("invalid type kind %d", t.Kind)
	case t.Array && t.Nullable:
		return fmt.Errorf("invalid type %s: an Array can't be Nullable", t)
	case t.Kind == KindDecimal:
		if t.Precision < 1 || t.Precision > MaxDecimalPrecision || t.Scale < 0 || t.Scale > t.Precision {
			return fmt.Errorf("invalid type %s: Decimal(P, S) needs 1 <= P <= %d and 0 <= S <= P", t, MaxDecimalPrecision)
		}
	case t.Kind == KindDateTime64:
		if t.Precision != DateTime64Millis && t.Precision != DateTime64Micros || t.Scale != 0 {
			return fmt.Errorf("invalid type %s: DateTime64 precision is 3 or 6", t)
		}
	case t.Precision != 0 || t.Scale != 0:
		return fmt.Errorf("invalid type %s: it takes no precision or scale", t.Kind)
	}
	return nil
}

// parseType reads the structure only; [ParseType] then requires the canonical spelling.
func parseType(s string) (Type, bool) {
	if inner, found := unwrap(s, "Array"); found {
		t, ok := parseScalar(inner)
		t.Array = true
		return t, ok
	}
	if inner, found := unwrap(s, "Nullable"); found {
		t, ok := parseScalar(inner)
		t.Nullable = true
		return t, ok
	}
	return parseScalar(s)
}

func parseScalar(s string) (Type, bool) {
	if args, found := unwrap(s, "Decimal"); found {
		p, sc, cut := strings.Cut(args, ", ")
		precision, perr := strconv.Atoi(p)
		scale, serr := strconv.Atoi(sc)
		return Type{Kind: KindDecimal, Precision: precision, Scale: scale}, cut && perr == nil && serr == nil
	}
	if args, found := unwrap(s, "DateTime64"); found {
		precision, err := strconv.Atoi(args)
		return Type{Kind: KindDateTime64, Precision: precision}, err == nil
	}
	for k := KindString; k <= kindLast; k++ {
		if k != KindDecimal && k != KindDateTime64 && k.String() == s {
			return Type{Kind: k}, true
		}
	}
	return Type{}, false
}

// unwrap returns the text inside name(...).
func unwrap(s, name string) (string, bool) {
	rest, found := strings.CutPrefix(s, name+"(")
	if !found {
		return "", false
	}
	return strings.CutSuffix(rest, ")")
}

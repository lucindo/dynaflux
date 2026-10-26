// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"math"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

const (
	typeNullable       = "Nullable"
	typeArray          = "Array"
	typeLowCardinality = "LowCardinality"
	typeString         = "String"

	hintTypeArgs = "write them as in Decimal(10, 2), Array(String) or DateTime64(6)"
	hintDecimal  = "Decimal(P, S) needs 1 ≤ P ≤ 38 and 0 ≤ S ≤ P"
)

// resolveType resolves a declared type, of a column or a parameter (sql.md §4.1, §4.2).
// It reports DF5001 to DF5009, and returns false when the type is invalid.
func resolveType(st *stmt, t *sql.Type) (catalog.Type, bool) {
	switch t.Name.Name {
	case typeNullable:
		inner, ok := typeParam(st, t)
		if !ok {
			return catalog.Type{}, false
		}
		if isWrapper(inner) {
			st.errorf(inner.Pos, "DF5006", "write LowCardinality(Nullable(String)); for arrays, use [] instead of NULL",
				"%s can't be inside Nullable", st.text(inner))
			return catalog.Type{}, false
		}
		s, ok := resolveScalar(st, inner)
		s.Nullable = true
		return s, ok
	case typeArray:
		return resolveArray(st, t)
	case typeLowCardinality:
		return resolveLowCardinality(st, t)
	}
	return resolveScalar(st, t)
}

func resolveArray(st *stmt, t *sql.Type) (catalog.Type, bool) {
	inner, ok := typeParam(st, t)
	if !ok {
		return catalog.Type{}, false
	}
	var elem catalog.Type
	switch inner.Name.Name {
	case typeNullable, typeArray:
	case typeLowCardinality:
		if elem, ok = resolveLowCardinality(st, inner); !ok {
			return catalog.Type{}, false
		}
	default:
		if elem, ok = resolveScalar(st, inner); !ok {
			return catalog.Type{}, false
		}
	}
	if elem.Kind == 0 || elem.Nullable {
		st.errorf(inner.Pos, "DF5007", "elements are non-Nullable scalars", "Array element type %s is not supported", st.text(inner))
		return catalog.Type{}, false
	}
	elem.Array = true
	return elem, true
}

// resolveLowCardinality accepts LowCardinality(String) and LowCardinality(Nullable(String)),
// which mean String and Nullable(String).
func resolveLowCardinality(st *stmt, t *sql.Type) (catalog.Type, bool) {
	inner, ok := typeParam(st, t)
	if !ok {
		return catalog.Type{}, false
	}
	resolved, ok := resolveType(st, inner)
	if !ok {
		return catalog.Type{}, false
	}
	if resolved.Kind != catalog.KindString || resolved.Array || inner.Name.Name == typeLowCardinality {
		st.errorf(t.Pos, "DF5008", "LowCardinality works only with String", "LowCardinality(%s) is not supported", st.text(inner))
		return catalog.Type{}, false
	}
	return resolved, true
}

// typeParam returns the one type argument of Nullable, Array or LowCardinality.
func typeParam(st *stmt, t *sql.Type) (*sql.Type, bool) {
	if len(t.Args) == 1 {
		if inner, ok := t.Args[0].(*sql.Type); ok {
			return inner, true
		}
	}
	st.errorf(t.Pos, "DF5009", hintTypeArgs, "invalid type arguments for %s", st.text(t))
	return nil, false
}

func isWrapper(t *sql.Type) bool {
	switch t.Name.Name {
	case typeNullable, typeArray, typeLowCardinality:
		return true
	}
	return false
}

func resolveScalar(st *stmt, t *sql.Type) (catalog.Type, bool) {
	name := t.Name.Name
	kind, ok := scalarKind(name)
	switch {
	case ok && kind == catalog.KindDecimal:
		return resolveDecimal(st, t)
	case ok && kind == catalog.KindDateTime64:
		return resolveDateTime64(st, t)
	case ok && t.Parens:
		st.errorf(t.Pos, "DF5009", hintTypeArgs, "invalid type arguments for %s", st.text(t))
		return catalog.Type{}, false
	case ok:
		return catalog.Type{Kind: kind}, true
	}
	if hint, unsupported := unsupportedType(name); unsupported {
		st.errorf(t.Pos, "DF5002", hint, "type %s is not supported in v1", name)
		return catalog.Type{}, false
	}
	st.errorf(t.Pos, "DF5001", didYouMean(name, knownTypeNames()), "unknown type %s", name)
	return catalog.Type{}, false
}

// scalarKind returns the kind a type name spells, with ClickHouse's letter-case rules.
func scalarKind(name string) (catalog.Kind, bool) {
	for _, k := range []catalog.Kind{
		catalog.KindString, catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64,
		catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64, catalog.KindFloat64, catalog.KindUUID,
	} {
		if name == k.String() {
			return k, true
		}
	}
	for _, k := range []catalog.Kind{catalog.KindDate, catalog.KindDateTime64, catalog.KindDecimal, catalog.KindBool} {
		if strings.EqualFold(name, k.String()) {
			return k, true
		}
	}
	return 0, false
}

func knownTypeNames() []string {
	return []string{
		"String", "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64", "Float64",
		"Decimal", "Bool", "UUID", "Date", "DateTime64", typeArray, typeNullable, typeLowCardinality,
	}
}

func resolveDecimal(st *stmt, t *sql.Type) (catalog.Type, bool) {
	const maxArgs = 2
	if len(t.Args) > maxArgs {
		st.errorf(t.Pos, "DF5009", hintTypeArgs, "invalid type arguments for %s", st.text(t))
		return catalog.Type{}, false
	}
	nums := make([]int, len(t.Args))
	for i, a := range t.Args {
		n, ok := intTypeArg(a)
		if !ok {
			st.errorf(t.Pos, "DF5009", hintTypeArgs, "invalid type arguments for %s", st.text(t))
			return catalog.Type{}, false
		}
		nums[i] = n
	}
	typ := catalog.Type{Kind: catalog.KindDecimal}
	if len(nums) > 0 {
		typ.Precision = nums[0]
	}
	if len(nums) > 1 {
		typ.Scale = nums[1]
	}
	if typ.Precision < 1 || typ.Precision > catalog.MaxDecimalPrecision || typ.Scale > typ.Precision {
		st.errorf(t.Pos, "DF5005", hintDecimal, "%s is out of range", st.text(t))
		return catalog.Type{}, false
	}
	return typ, true
}

func resolveDateTime64(st *stmt, t *sql.Type) (catalog.Type, bool) {
	const maxArgs = 2
	if len(t.Args) > maxArgs {
		st.errorf(t.Pos, "DF5009", hintTypeArgs, "invalid type arguments for %s", st.text(t))
		return catalog.Type{}, false
	}
	var zone *sql.StringLit
	if len(t.Args) == maxArgs {
		z, isString := t.Args[1].(*sql.StringLit)
		if !isString {
			st.errorf(t.Pos, "DF5009", hintTypeArgs, "invalid type arguments for %s", st.text(t))
			return catalog.Type{}, false
		}
		zone = z
	}
	precision := 0
	if len(t.Args) > 0 {
		p, ok := intTypeArg(t.Args[0])
		if !ok {
			st.errorf(t.Pos, "DF5009", hintTypeArgs, "invalid type arguments for %s", st.text(t))
			return catalog.Type{}, false
		}
		precision = p
	}
	ok := true
	if precision != catalog.DateTime64Millis && precision != catalog.DateTime64Micros {
		st.errorf(t.Pos, "DF5003", "for DateTime64(9): Python, JSON and Athena truncate nanoseconds; use DateTime64(6)",
			"DateTime64 needs precision 3 or 6")
		ok = false
	}
	if zone != nil && zone.Value != "UTC" {
		st.errorf(zone.Pos, "DF5004", "all times are UTC; omit the zone", "time zone %s is not supported", st.text(zone))
		ok = false
	}
	return catalog.Type{Kind: catalog.KindDateTime64, Precision: precision}, ok
}

// intTypeArg returns the value of an integer type argument, capped so that it fits an int.
func intTypeArg(a sql.TypeArg) (int, bool) {
	n, ok := a.(*sql.NumberLit)
	if !ok || n.IsFloat || n.Neg {
		return 0, false
	}
	return int(min(n.Int, math.MaxInt32)), true
}

// unsupportedType returns the replacement hint of a ClickHouse type that v1 lacks.
func unsupportedType(name string) (string, bool) {
	switch name {
	case "Float32":
		return "use Float64", true
	case "Date32":
		return "use Date", true
	case "Int128", "Int256", "UInt128", "UInt256":
		return "use Int64, UInt64 or Decimal(38, 0)", true
	case "Decimal32", "Decimal64", "Decimal128", "Decimal256":
		return "use Decimal(P, S)", true
	case "FixedString":
		return "use String", true
	case "Enum", "Enum8", "Enum16":
		return "use LowCardinality(String)", true
	case "Map", "Tuple", "JSON", "Variant", "Dynamic", "IPv4", "IPv6", "Object", "Nested":
		return "use separate columns of supported types", true
	}
	switch strings.ToUpper(name) {
	case "DATETIME":
		return "use DateTime64(3) or DateTime64(6)", true
	case "TIMESTAMP":
		return "use the ClickHouse type DateTime64(3) or DateTime64(6)", true
	case "INT", "INTEGER", "MEDIUMINT":
		return "use the ClickHouse type Int32", true
	case "TINYINT":
		return "use the ClickHouse type Int8", true
	case "SMALLINT":
		return "use the ClickHouse type Int16", true
	case "BIGINT":
		return "use the ClickHouse type Int64", true
	case "FLOAT", "REAL", "DOUBLE":
		return "use the ClickHouse type Float64", true
	case "VARCHAR", "CHAR", "TEXT", "BLOB", "NVARCHAR":
		return "use the ClickHouse type String", true
	case "BOOLEAN":
		return "use the ClickHouse type Bool", true
	case "NUMERIC", "DEC":
		return "use the ClickHouse type Decimal(P, S)", true
	}
	return "", false
}

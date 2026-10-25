// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

func TestParseTypeRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		text string
		want catalog.Type
	}{
		{"String", catalog.Type{Kind: catalog.KindString}},
		{"UInt8", catalog.Type{Kind: catalog.KindUInt8}},
		{"UInt16", catalog.Type{Kind: catalog.KindUInt16}},
		{"UInt32", catalog.Type{Kind: catalog.KindUInt32}},
		{"UInt64", catalog.Type{Kind: catalog.KindUInt64}},
		{"Int8", catalog.Type{Kind: catalog.KindInt8}},
		{"Int16", catalog.Type{Kind: catalog.KindInt16}},
		{"Int32", catalog.Type{Kind: catalog.KindInt32}},
		{"Int64", catalog.Type{Kind: catalog.KindInt64}},
		{"Float64", catalog.Type{Kind: catalog.KindFloat64}},
		{"Decimal(1, 0)", catalog.Type{Kind: catalog.KindDecimal, Precision: 1}},
		{"Decimal(10, 2)", catalog.Type{Kind: catalog.KindDecimal, Precision: 10, Scale: 2}},
		{"Decimal(38, 38)", catalog.Type{Kind: catalog.KindDecimal, Precision: 38, Scale: 38}},
		{"Bool", catalog.Type{Kind: catalog.KindBool}},
		{"UUID", catalog.Type{Kind: catalog.KindUUID}},
		{"Date", catalog.Type{Kind: catalog.KindDate}},
		{"DateTime", catalog.Type{Kind: catalog.KindDateTime}},
		{"DateTime64(3)", catalog.Type{Kind: catalog.KindDateTime64, Precision: 3}},
		{"DateTime64(6)", catalog.Type{Kind: catalog.KindDateTime64, Precision: 6}},
		{"Nullable(String)", catalog.Type{Kind: catalog.KindString, Nullable: true}},
		{"Nullable(Int64)", catalog.Type{Kind: catalog.KindInt64, Nullable: true}},
		{"Nullable(Float64)", catalog.Type{Kind: catalog.KindFloat64, Nullable: true}},
		{"Nullable(Decimal(9, 4))", catalog.Type{Kind: catalog.KindDecimal, Precision: 9, Scale: 4, Nullable: true}},
		{"Nullable(Bool)", catalog.Type{Kind: catalog.KindBool, Nullable: true}},
		{"Nullable(UUID)", catalog.Type{Kind: catalog.KindUUID, Nullable: true}},
		{"Nullable(Date)", catalog.Type{Kind: catalog.KindDate, Nullable: true}},
		{"Nullable(DateTime)", catalog.Type{Kind: catalog.KindDateTime, Nullable: true}},
		{"Nullable(DateTime64(6))", catalog.Type{Kind: catalog.KindDateTime64, Precision: 6, Nullable: true}},
		{"Array(String)", catalog.Type{Kind: catalog.KindString, Array: true}},
		{"Array(UInt8)", catalog.Type{Kind: catalog.KindUInt8, Array: true}},
		{"Array(Decimal(18, 2))", catalog.Type{Kind: catalog.KindDecimal, Precision: 18, Scale: 2, Array: true}},
		{"Array(Date)", catalog.Type{Kind: catalog.KindDate, Array: true}},
		{"Array(DateTime64(3))", catalog.Type{Kind: catalog.KindDateTime64, Precision: 3, Array: true}},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			t.Parallel()
			got, err := catalog.ParseType(tt.text)
			if err != nil {
				t.Fatalf("ParseType(%q): %v", tt.text, err)
			}
			if got != tt.want {
				t.Errorf("ParseType(%q) = %#v, want %#v", tt.text, got, tt.want)
			}
			text, err := tt.want.MarshalText()
			if err != nil {
				t.Fatalf("MarshalText(%#v): %v", tt.want, err)
			}
			if string(text) != tt.text {
				t.Errorf("MarshalText(%#v) = %q, want %q", tt.want, text, tt.text)
			}
		})
	}
}

func TestParseTypeRejects(t *testing.T) {
	t.Parallel()
	for _, text := range []string{
		"",
		"string",
		"Float32",
		"DateTime64",
		"DateTime64(9)",
		"DateTime64(6, 'UTC')",
		"Decimal",
		"Decimal(10)",
		"Decimal(10,2)",
		"Decimal(0, 0)",
		"Decimal(39, 0)",
		"Decimal(10, 11)",
		"Decimal(+5, 2)",
		"Decimal(05, 2)",
		"LowCardinality(String)",
		"Nullable(String",
		"Nullable(Nullable(String))",
		"Nullable(Array(String))",
		"Array(Nullable(String))",
		"Array(Array(String))",
		" String",
	} {
		t.Run(text, func(t *testing.T) {
			t.Parallel()
			if got, err := catalog.ParseType(text); err == nil {
				t.Errorf("ParseType(%q) = %v, want an error", text, got)
			}
		})
	}
}

func TestMarshalTextRejectsInvalidType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		typ  catalog.Type
	}{
		{"zero", catalog.Type{}},
		{"Nullable Array", catalog.Type{Kind: catalog.KindString, Nullable: true, Array: true}},
		{"String with precision", catalog.Type{Kind: catalog.KindString, Precision: 3}},
		{"negative Decimal scale", catalog.Type{Kind: catalog.KindDecimal, Precision: 10, Scale: -1}},
		{"DateTime64 without precision", catalog.Type{Kind: catalog.KindDateTime64}},
		{"kind after the last", catalog.Type{Kind: catalog.KindDateTime64 + 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if text, err := tt.typ.MarshalText(); err == nil {
				t.Errorf("MarshalText(%#v) = %q, want an error", tt.typ, text)
			}
		})
	}
}

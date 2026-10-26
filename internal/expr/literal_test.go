// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr_test

import (
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/expr"
)

func TestLiteralReadsCanonicalText(t *testing.T) {
	t.Parallel()
	cases := []struct{ typ, text string }{
		{"UInt8", "255"},
		{"UInt64", "18446744073709551615"},
		{"Int8", "-128"},
		{"Int64", "-9223372036854775808"},
		{"Int32", "0"},
		{"Float64", "0.1"},
		{"Float64", "-1.5"},
		{"Float64", "1e+06"},
		{"Float64", "5e-324"},
		{"Float64", "0"},
		{"Decimal(5, 2)", "-0.05"},
		{"Decimal(5, 2)", "999.99"},
		{"Decimal(3, 0)", "0"},
		{"Decimal(38, 0)", strings.Repeat("9", 38)},
		{"Decimal(38, 38)", "0." + strings.Repeat("9", 38)},
		{"Bool", "true"},
		{"Bool", "false"},
		{"UUID", "61f0c404-5cb3-11e7-907b-a6006ad3dba0"},
		{"Date", "1970-01-01"},
		{"Date", "2024-02-29"},
		{"Date", "2106-02-07"},
		{"DateTime", "2106-02-07 06:28:14"},
		{"DateTime64(3)", "2024-05-31 13:45:00.250"},
		{"DateTime64(6)", "2106-02-07 06:28:14.999999"},
		{"String", "héllo"},
		{"String", ""},
		{"Nullable(String)", "x"},
	}
	for _, c := range cases {
		v, err := expr.Literal(ty(c.typ), c.text)
		if err != nil {
			t.Errorf("Literal(%s, %q): %v", c.typ, c.text, err)
			continue
		}
		if got := render(ty(c.typ), v); got != c.text && got != `"`+c.text+`"` {
			t.Errorf("Literal(%s, %q) = %s", c.typ, c.text, got)
		}
	}
}

func TestLiteralRejectsOtherText(t *testing.T) {
	t.Parallel()
	cases := []struct{ typ, text string }{
		{"UInt8", "256"},
		{"UInt8", "-1"},
		{"UInt8", "+1"},
		{"UInt8", "01"},
		{"UInt8", " 1"},
		{"UInt8", ""},
		{"Int8", "-129"},
		{"Int8", "-0"},
		{"Float64", "1000000"},
		{"Float64", "0.10"},
		{"Float64", "-0"},
		{"Float64", "NaN"},
		{"Float64", "+Inf"},
		{"Float64", "1e400"},
		{"Float64", ".5"},
		{"Decimal(5, 2)", "1.5"},
		{"Decimal(5, 2)", "1.500"},
		{"Decimal(5, 2)", "-0.00"},
		{"Decimal(5, 2)", "01.50"},
		{"Decimal(5, 2)", "1000.00"},
		{"Decimal(5, 2)", "+1.50"},
		{"Decimal(5, 2)", "1,50"},
		{"Decimal(5, 2)", ".50"},
		{"Decimal(3, 0)", "1.0"},
		{"Decimal(3, 0)", "1."},
		{"Decimal(3, 0)", "-"},
		{"Bool", "TRUE"},
		{"Bool", "1"},
		{"UUID", "61F0C404-5CB3-11E7-907B-A6006AD3DBA0"},
		{"UUID", "61f0c4045cb311e7907ba6006ad3dba0"},
		{"UUID", "61f0c404-5cb3-11e7-907b-a6006ad3dba"},
		{"UUID", "61f0c404-5cb3-11e7-907b-a6006ad3dbag"},
		{"Date", "2024-02-30"},
		{"Date", "2024-1-01"},
		{"Date", "1969-12-31"},
		{"Date", "2106-02-08"},
		{"Date", "2024-05-31 00:00:00"},
		{"DateTime", "2106-02-07 06:28:15"},
		{"DateTime", "2024-05-31 13:45:00.250"},
		{"DateTime", "2024-05-31"},
		{"DateTime", "2024-05-31T13:45:00"},
		{"DateTime64(3)", "2024-05-31 13:45:00.25"},
		{"DateTime64(3)", "2024-05-31 13:45:00.2500"},
		{"DateTime64(3)", "2024-05-31 13:45:00"},
		{"String", "\xff"},
		{"Array(String)", "a"},
	}
	for _, c := range cases {
		if v, err := expr.Literal(ty(c.typ), c.text); err == nil {
			t.Errorf("Literal(%s, %q) = %s, want an error", c.typ, c.text, render(ty(c.typ), v))
		}
	}
}

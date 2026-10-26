// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux_test

import (
	"errors"
	"math/big"
	"testing"

	"github.com/lucindo/dynaflux"
)

func TestParseDecimal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, want string // want "" for an error
	}{
		{"12.50", "12.50"},
		{"-0.05", "-0.05"},
		{"7", "7"},
		{"-0.00", "0.00"},
		{"007.10", "7.10"},
		{"99999999999999999999999999999999999999", "99999999999999999999999999999999999999"},
		{"0.00000000000000000000000000000000000001", "0.00000000000000000000000000000000000001"},
		{"-1234567890123456789012345678.9012345678", "-1234567890123456789012345678.9012345678"},
		{"100000000000000000000000000000000000000", ""},
		{"0.000000000000000000000000000000000000001", ""},
		{"", ""},
		{"-", ""},
		{".5", ""},
		{"5.", ""},
		{"+5", ""},
		{"1e5", ""},
		{"1.2.3", ""},
		{" 1", ""},
	}
	for _, tt := range tests {
		d, err := dynaflux.ParseDecimal(tt.in)
		switch {
		case tt.want == "" && err == nil:
			t.Errorf("ParseDecimal(%q) = %s, want an error", tt.in, d)
		case tt.want != "" && (err != nil || d.String() != tt.want):
			t.Errorf("ParseDecimal(%q) = %s, %v; want %s", tt.in, d, err, tt.want)
		}
	}
}

func TestDecimalCompareAcrossScales(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a, b string
		want int
	}{
		{"12.5", "12.50", 0},
		{"12.5", "12.49", 1},
		{"-1", "-0.999", -1},
		{"0", "-0.00", 0},
		{"99999999999999999999999999999999999999", "0.00000000000000000000000000000000000001", 1},
	}
	for _, tt := range tests {
		a, b := mustParse(t, tt.a), mustParse(t, tt.b)
		if got := a.Compare(b); got != tt.want {
			t.Errorf("%s.Compare(%s) = %d, want %d", a, b, got, tt.want)
		}
		if got := a.Equal(b); got != (tt.want == 0) {
			t.Errorf("%s.Equal(%s) = %v", a, b, got)
		}
	}
	if mustParse(t, "12.5") == mustParse(t, "12.50") {
		t.Error("12.5 == 12.50, want == to see the scale")
	}
}

func TestDecimalFromBig(t *testing.T) {
	t.Parallel()
	d, err := dynaflux.DecimalFromBig(big.NewInt(-1250), 2)
	if err != nil || d.String() != "-12.50" {
		t.Fatalf("got %s, %v; want -12.50", d, err)
	}
	if u, scale := d.Big(); u.Cmp(big.NewInt(-1250)) != 0 || scale != 2 {
		t.Errorf("Big() = %v, %d; want -1250, 2", u, scale)
	}
	tooBig, _ := new(big.Int).SetString("100000000000000000000000000000000000000", 10)
	for _, c := range []struct {
		u     *big.Int
		scale int
	}{{tooBig, 0}, {big.NewInt(1), -1}, {big.NewInt(1), 39}} {
		if got, ferr := dynaflux.DecimalFromBig(c.u, c.scale); ferr == nil {
			t.Errorf("DecimalFromBig(%v, %d) = %s, want an error", c.u, c.scale, got)
		}
	}
}

func TestDecimalText(t *testing.T) {
	t.Parallel()
	var d dynaflux.Decimal
	if err := d.UnmarshalText([]byte("-3.140")); err != nil {
		t.Fatal(err)
	}
	if b, err := d.MarshalText(); err != nil || string(b) != "-3.140" {
		t.Errorf("MarshalText() = %q, %v; want -3.140", b, err)
	}
	if err := d.UnmarshalText([]byte("x")); err == nil {
		t.Error("UnmarshalText(x) succeeded")
	}
	if got := (dynaflux.Decimal{}).String(); got != "0" {
		t.Errorf("the zero Decimal is %s, want 0", got)
	}
}

func mustParse(t *testing.T, s string) dynaflux.Decimal {
	t.Helper()
	d, err := dynaflux.ParseDecimal(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// The inputs of Go 1.27's uuid tests.
func TestParseUUID(t *testing.T) {
	t.Parallel()
	u1 := dynaflux.UUID{0xf8, 0x1d, 0x4f, 0xae, 0x7d, 0xec, 0x11, 0xd0, 0xa7, 0x65, 0x00, 0xa0, 0xc9, 0x1e, 0x6b, 0xf6}
	var allOnes dynaflux.UUID
	for i := range allOnes {
		allOnes[i] = 0xff
	}
	for s, want := range map[string]dynaflux.UUID{
		"00000000-0000-0000-0000-000000000000":          {},
		"ffffffff-ffff-ffff-ffff-ffffffffffff":          allOnes,
		"f81d4fae-7dec-11d0-a765-00a0c91e6bf6":          u1,
		"F81D4FAE-7DEC-11D0-A765-00A0C91E6BF6":          u1,
		"f81d4fae7dec11d0a76500a0c91e6bf6":              u1,
		"{f81d4fae-7dec-11d0-a765-00a0c91e6bf6}":        u1,
		"urn:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6": u1,
	} {
		if got, err := dynaflux.ParseUUID(s); err != nil || got != want {
			t.Errorf("ParseUUID(%q) = %v, %v; want %v", s, got, err, want)
		}
	}
	for _, s := range []string{
		"",
		"0000000000000-0000-0000-000000000000",
		"00000000-000000000-0000-000000000000",
		"00000000-0000-000000000-000000000000",
		"00000000-0000-0000-00000000000000000",
		"00000000-0000-0000-0000-00000000000",
		"x0000000-0000-0000-0000-000000000000",
		"00000000-x000-0000-0000-000000000000",
		"00000000-0000-x000-0000-000000000000",
		"00000000-0000-0000-x000-000000000000",
		"00000000-0000-0000-0000-x00000000000",
		"{x0000000-0000-0000-0000-000000000000}",
		"urn:uuid:x000000-0000-0000-0000-000000000000",
		"x0000000000000000000000000000000",
		"0000-0000-0000-0000-0000-0000-0000-0000",
		"{00000000000000000000000000000000}",
		"{urn:uuid:00000000-0000-0000-0000-000000000000}",
		"urn:uuid:00000000000000000000000000000000",
		// Beyond Go's inputs: the wrong wrapping characters.
		"(f81d4fae-7dec-11d0-a765-00a0c91e6bf6)",
		"URN:UUID:f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
	} {
		if got, err := dynaflux.ParseUUID(s); err == nil {
			t.Errorf("ParseUUID(%q) = %v, want an error", s, got)
		}
	}
}

func TestUUIDText(t *testing.T) {
	t.Parallel()
	u := dynaflux.UUID{0xf8, 0x1d, 0x4f, 0xae, 0x7d, 0xec, 0x11, 0xd0, 0xa7, 0x65, 0x00, 0xa0, 0xc9, 0x1e, 0x6b, 0xf6}
	const want = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
	if got := u.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if got, err := u.AppendText([]byte("urn:uuid:")); err != nil || string(got) != "urn:uuid:"+want {
		t.Errorf("AppendText = %q, %v", got, err)
	}
	same := u
	if u.Compare(dynaflux.UUID{}) != 1 || (dynaflux.UUID{}).Compare(u) != -1 || u.Compare(same) != 0 {
		t.Error("Compare doesn't order by bytes")
	}
}

func TestBatchErrorUnwrapsOnlyFailures(t *testing.T) {
	t.Parallel()
	be := &dynaflux.BatchError{Errs: []error{nil, dynaflux.ErrDuplicate, nil, dynaflux.ErrOutsideWindow}}
	if got := be.Unwrap(); len(got) != 2 || !errors.Is(got[0], dynaflux.ErrDuplicate) || !errors.Is(got[1], dynaflux.ErrOutsideWindow) {
		t.Errorf("Unwrap() = %v, want the two failures", got)
	}
	if !errors.Is(be, dynaflux.ErrOutsideWindow) || errors.Is(be, dynaflux.ErrInvalidValue) {
		t.Error("errors.Is doesn't see exactly the failures")
	}
	const want = "dynaflux: 2 of 4 events failed; event 1: duplicate event; event 3: event_time outside the ACCEPT window"
	if be.Error() != want {
		t.Errorf("Error() = %q, want %q", be.Error(), want)
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

const (
	// maxDecimalDigits is the most digits a Decimal holds, the precision of a DynamoDB number.
	maxDecimalDigits = 38
	wordBits         = 64
	decimalBase      = 10
)

// Decimal is an exact decimal number of at most 38 digits that keeps its scale, the digits
// after its point: 12.50 has scale 2. The zero value is 0.
//
// == compares the representation, scale included, so 12.5 and 12.50 differ under == and are
// [Decimal.Equal]. Values read from one Decimal(P, S) column all have scale S.
type Decimal struct {
	hi, lo uint64 // the unscaled value's magnitude
	neg    bool   // never set for zero
	scale  uint8
}

// ParseDecimal parses an optional '-', digits, and optionally '.' and more digits, such as
// "-12.50". The scale is the number of digits after the point.
func ParseDecimal(s string) (Decimal, error) {
	digits, neg := strings.CutPrefix(s, "-")
	whole, frac, point := strings.Cut(digits, ".")
	if !isDigits(whole) || point && !isDigits(frac) {
		return Decimal{}, fmt.Errorf("dynaflux: %q is not a decimal number", s)
	}
	u, _ := new(big.Int).SetString(whole+frac, decimalBase)
	if neg {
		u.Neg(u)
	}
	d, err := DecimalFromBig(u, len(frac))
	if err != nil {
		return Decimal{}, fmt.Errorf("dynaflux: decimal %q: %w", s, err)
	}
	return d, nil
}

func isDigits(s string) bool {
	return s != "" && strings.Trim(s, "0123456789") == ""
}

// DecimalFromBig returns unscaled × 10^−scale. It fails when unscaled has more than 38
// digits or scale is outside 0 to 38.
func DecimalFromBig(unscaled *big.Int, scale int) (Decimal, error) {
	if scale < 0 || scale > maxDecimalDigits {
		return Decimal{}, fmt.Errorf("scale %d is outside 0 to %d", scale, maxDecimalDigits)
	}
	mag := new(big.Int).Abs(unscaled)
	if mag.Cmp(pow10(maxDecimalDigits)) >= 0 {
		return Decimal{}, errors.New("more than 38 digits")
	}
	lo := mag.Uint64()
	return Decimal{
		hi:    mag.Rsh(mag, wordBits).Uint64(),
		lo:    lo,
		neg:   unscaled.Sign() < 0,
		scale: uint8(scale),
	}, nil
}

// Big returns d's unscaled value and scale: d is unscaled × 10^−scale.
func (d Decimal) Big() (*big.Int, int) {
	u := new(big.Int).SetUint64(d.hi)
	u.Lsh(u, wordBits).Or(u, new(big.Int).SetUint64(d.lo))
	if d.neg {
		u.Neg(u)
	}
	return u, int(d.scale)
}

// Compare returns -1, 0 or +1 as d is less than, equal to or greater than e, by value.
func (d Decimal) Compare(e Decimal) int {
	a, as := d.Big()
	b, bs := e.Big()
	if as < bs {
		a.Mul(a, pow10(bs-as))
	} else {
		b.Mul(b, pow10(as-bs))
	}
	return a.Cmp(b)
}

// Equal reports whether d and e have the same value, whatever their scales.
func (d Decimal) Equal(e Decimal) bool { return d.Compare(e) == 0 }

// String returns d with exactly its scale's digits after the point, such as "-0.05".
func (d Decimal) String() string {
	u, scale := d.Big()
	digits := u.Abs(u).String()
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	point := len(digits) - scale
	s := digits[:point]
	if scale > 0 {
		s += "." + digits[point:]
	}
	if d.neg {
		s = "-" + s
	}
	return s
}

// MarshalText implements [encoding.TextMarshaler] with [Decimal.String].
func (d Decimal) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// UnmarshalText implements [encoding.TextUnmarshaler] with [ParseDecimal].
func (d *Decimal) UnmarshalText(b []byte) error {
	parsed, err := ParseDecimal(string(b))
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(decimalBase), big.NewInt(int64(n)), nil)
}

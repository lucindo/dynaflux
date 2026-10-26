// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr

import (
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Literal returns the value of an OpLiteral: the canonical text of a value of the scalar
// type t, the only form [catalog.Expr] allows. Any other text is an error, and so is a
// temporal value outside the range of sql.md §4.1.
func Literal(t catalog.Type, canonical string) (Value, error) {
	v, ok := parseLiteral(t, canonical)
	if !ok {
		return Value{}, fmt.Errorf("invalid %s literal %q", t, canonical)
	}
	return v, nil
}

func parseLiteral(t catalog.Type, s string) (Value, bool) {
	k := t.Kind
	switch {
	case t.Array:
		return Value{}, false
	case k == catalog.KindString:
		return String(s), utf8.ValidString(s)
	case k == catalog.KindBool:
		return Bool(s == "true"), s == "true" || s == "false"
	case k == catalog.KindFloat64:
		f, err := strconv.ParseFloat(s, 64)
		ok := err == nil && !math.IsInf(f, 0) && !math.IsNaN(f) && s != "-0" && strconv.FormatFloat(f, 'g', -1, 64) == s
		return Float(f), ok
	case k == catalog.KindDecimal:
		return parseDecimal(s, t)
	case k == catalog.KindUUID:
		return parseUUID(s)
	case k == catalog.KindDate || k == catalog.KindDateTime || k == catalog.KindDateTime64:
		return parseTime(s, t)
	case isSigned(k):
		i, err := strconv.ParseInt(s, 10, int(intBits(k)))
		return Int(i), err == nil && strconv.FormatInt(i, 10) == s
	}
	u, err := strconv.ParseUint(s, 10, int(intBits(k)))
	return Uint(u), err == nil && intBits(k) > 0 && strconv.FormatUint(u, 10) == s
}

// parseDecimal reads an optional "-", the integer part without leading zeros, and when
// the scale is S > 0, a point and exactly S digits. The value has at most P digits, and
// zero is never negative.
func parseDecimal(s string, t catalog.Type) (Value, bool) {
	body, neg := strings.CutPrefix(s, "-")
	whole, frac, point := strings.Cut(body, ".")
	digits := whole + frac
	ok := isDigits(whole) && (whole == "0" || whole[0] != '0') &&
		point == (t.Scale > 0) && len(frac) == t.Scale && (frac == "" || isDigits(frac)) &&
		len(strings.TrimLeft(digits, "0")) <= t.Precision
	if !ok {
		return Value{}, false
	}
	x, _ := new(big.Int).SetString(digits, decimalBase) // only digits, checked above
	if neg {
		x.Neg(x)
	}
	return Decimal(x), !neg || x.Sign() != 0
}

func isDigits(s string) bool {
	return s != "" && strings.Trim(s, "0123456789") == ""
}

// parseUUID reads lowercase 8-4-4-4-12 hex digits.
func parseUUID(s string) (Value, bool) {
	const form = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
	if len(s) != len(form) {
		return Value{}, false
	}
	for i := range len(s) {
		if form[i] == '-' && s[i] != '-' || form[i] != '-' && !strings.ContainsRune("0123456789abcdef", rune(s[i])) {
			return Value{}, false
		}
	}
	var b [16]byte
	_, err := hex.Decode(b[:], []byte(strings.ReplaceAll(s, "-", "")))
	return UUID(b), err == nil
}

// timeLimit bounds temporal values (sql.md §4.1): times are before 2106-02-07 06:28:15
// UTC, so dates are through 2106-02-07.
const timeLimit = math.MaxUint32 * microsPerSecond

// parseTime reads "YYYY-MM-DD" for a Date, "YYYY-MM-DD hh:mm:ss" for a DateTime, and the
// latter with exactly p fractional digits for a DateTime64(p).
func parseTime(s string, t catalog.Type) (Value, bool) {
	layout := "2006-01-02"
	if t.Kind == catalog.KindDateTime {
		layout += " 15:04:05"
	}
	if t.Kind == catalog.KindDateTime64 {
		layout += " 15:04:05." + strings.Repeat("0", t.Precision)
	}
	tm, err := time.Parse(layout, s)
	// Formatting back rejects what Parse allows beyond the layout, such as a fraction
	// after the seconds of a layout without one.
	if err != nil || tm.Format(layout) != s {
		return Value{}, false
	}
	us := tm.UnixMicro()
	return Micros(us), us >= 0 && us < timeLimit
}

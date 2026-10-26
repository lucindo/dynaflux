// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// numberType returns the type of a number literal (sql.md §3.5): the smallest unsigned
// type that holds an integer, the smallest signed one for a negative integer, and
// Float64 for a float. -0 is the UInt8 0.
func numberType(n *sql.NumberLit) catalog.Type {
	switch {
	case n.IsFloat:
		return catalog.Type{Kind: catalog.KindFloat64}
	case !n.Neg || n.Int == 0:
		for _, k := range []catalog.Kind{catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32} {
			if n.Int <= uint64(1)<<intBits(k)-1 {
				return catalog.Type{Kind: k}
			}
		}
		return catalog.Type{Kind: catalog.KindUInt64}
	}
	for _, k := range []catalog.Kind{catalog.KindInt8, catalog.KindInt16, catalog.KindInt32} {
		if n.Int <= uint64(1)<<(intBits(k)-1) {
			return catalog.Type{Kind: k}
		}
	}
	return catalog.Type{Kind: catalog.KindInt64}
}

// numberLiteral returns a number literal with its own type.
func numberLiteral(n *sql.NumberLit) catalog.Expr {
	t := numberType(n)
	if n.IsFloat {
		f := n.Float
		if n.Neg {
			f = -f
		}
		return literal(t, formatFloat(f))
	}
	return literal(t, exactValue(n).Num().String())
}

func literal(t catalog.Type, value string) catalog.Expr {
	return catalog.Expr{Op: catalog.OpLiteral, Type: t, Value: value}
}

// exactValue returns the value a number literal's text stands for.
func exactValue(n *sql.NumberLit) *big.Rat {
	// The parser leaves only decimal digits, a point and an exponent, which SetString reads.
	r, _ := new(big.Rat).SetString(clampExponent(n.Text))
	if n.Neg {
		r.Neg(r)
	}
	return r
}

// exponentSlack bounds exponents to ±(len(text)+400), which changes no result: a non-zero
// value there or beyond is 0 or infinite as a Float64, and needs more than 38 digits.
const exponentSlack = 400

// clampExponent bounds the exponent of a float literal's text, which can overflow an
// int64 or make [big.Rat] compute a power of ten with millions of digits. A positive
// exponent past the bound passes the lexer only with a zero mantissa, as any other
// value would overflow Float64.
func clampExponent(text string) string {
	mantissa, exp, found := strings.Cut(strings.ToLower(text), "e")
	if !found {
		return text
	}
	bound := int64(len(text) + exponentSlack)
	e, err := strconv.ParseInt(exp, 10, 64)
	if err != nil { // out of int64 range: the lexer allows only digits and a sign
		e = bound
		if strings.HasPrefix(exp, "-") {
			e = -bound
		}
	}
	return mantissa + "e" + strconv.FormatInt(min(max(e, -bound), bound), 10)
}

func formatFloat(f float64) string {
	if f == 0 {
		return "0"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// decimalLiteral returns a number literal as the Decimal(P, S) that holds its exact value
// (catalog.Expr, sql.md Dev 7), and false when that needs more than 38 digits.
func decimalLiteral(n *sql.NumberLit) (catalog.Expr, bool) {
	r := exactValue(n)
	scale := 0
	scaled := new(big.Rat).Set(r)
	for !scaled.IsInt() {
		scaled.Mul(scaled, new(big.Rat).SetInt64(decimalBase))
		scale++
	}
	m := scaled.Num()
	precision := max(len(new(big.Int).Abs(m).String()), scale)
	if m.Sign() == 0 {
		precision, scale = 1, 0
	}
	if precision > catalog.MaxDecimalPrecision {
		return catalog.Expr{}, false
	}
	return literal(decimalType(precision, scale), r.FloatString(scale)), true
}

// Bounds of temporal values (sql.md §4.1): dates through 2106-02-07, and times before
// 2106-02-07 06:28:15.
func timeLimit() time.Time {
	return time.Unix(math.MaxUint32, 0).UTC()
}

func lastDate() time.Time {
	return time.Date(2106, 2, 7, 0, 0, 0, 0, time.UTC)
}

const (
	dateLayout     = "2006-01-02"
	dateTimeLayout = "2006-01-02 15:04:05"
	dateLen        = len(dateLayout)
	nanoDigits     = 9 // fractional digits of a nanosecond
)

// parseTime parses s strictly as a value of the temporal type t (sql.md §4.6): a real
// calendar date, a time for DateTime and DateTime64, and up to p fractional digits for
// DateTime64(p), within the range of §4.1.
func parseTime(s string, t catalog.Type) (time.Time, bool) {
	layout := dateLayout
	if t.Kind != catalog.KindDate && len(s) > dateLen {
		layout = dateTimeLayout
	}
	s, frac, hasFrac := strings.Cut(s, ".")
	tm, err := time.Parse(layout, s)
	// Formatting back rejects what Parse allows beyond the layout, such as a one-digit hour.
	if err != nil || tm.Format(layout) != s {
		return time.Time{}, false
	}
	if hasFrac {
		v, ok := digitsValue(frac)
		if !ok || len(frac) > t.Precision {
			return time.Time{}, false
		}
		tm = tm.Add(time.Duration(v * int(math.Pow10(nanoDigits-len(frac)))))
	}
	return tm, inRange(tm, t)
}

func digitsValue(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	v := 0
	for i := range len(s) {
		if !isDigitASCII(s[i]) {
			return 0, false
		}
		v = v*decimalBase + int(s[i]-'0')
	}
	return v, true
}

func inRange(tm time.Time, t catalog.Type) bool {
	if tm.Before(time.Unix(0, 0)) {
		return false
	}
	if t.Kind == catalog.KindDate {
		return !tm.After(lastDate())
	}
	return tm.Before(timeLimit())
}

// formatTime writes a temporal value of type t in canonical form.
func formatTime(tm time.Time, t catalog.Type) string {
	if t.Kind == catalog.KindDate {
		return tm.Format(dateLayout)
	}
	if t.Kind == catalog.KindDateTime64 {
		return tm.Format(dateTimeLayout + "." + strings.Repeat("0", t.Precision))
	}
	return tm.Format(dateTimeLayout)
}

// parseUUID parses 8-4-4-4-12 hex digits, in either case, and returns them in lowercase.
func parseUUID(s string) (string, bool) {
	const form = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
	if len(s) != len(form) {
		return "", false
	}
	for i := range len(s) {
		if form[i] == '-' && s[i] != '-' || form[i] != '-' && !strings.ContainsRune("0123456789abcdefABCDEF", rune(s[i])) {
			return "", false
		}
	}
	return strings.ToLower(s), true
}

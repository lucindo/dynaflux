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

// exponentSlack is how far past the literal's length an exponent can go before it's
// clamped: a non-zero value past it is 0 as a Float64 and needs more than 38 digits,
// as it does at the bound.
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
	return literal(decimalType(precision, scale), formatDecimal(m, scale)), true
}

// formatDecimal writes the Decimal whose value is m / 10^scale in canonical form.
func formatDecimal(m *big.Int, scale int) string {
	digits := new(big.Int).Abs(m).String()
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	sign := ""
	if m.Sign() < 0 {
		sign = "-"
	}
	if scale == 0 {
		return sign + digits
	}
	cut := len(digits) - scale
	return sign + digits[:cut] + "." + digits[cut:]
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
	dateTimeLen    = len(dateTimeLayout)
	nanoDigits     = 9 // fractional digits of a nanosecond
)

// parseTime parses s strictly as a value of the temporal type t (sql.md §4.6): a real
// calendar date, a time for DateTime and DateTime64, and up to p fractional digits for
// DateTime64(p), within the range of §4.1.
func parseTime(s string, t catalog.Type) (time.Time, bool) {
	date, rest := s, ""
	if len(s) > dateLen {
		date, rest = s[:dateLen], s[dateLen:]
	}
	tm, ok := parseDate(date)
	if !ok {
		return time.Time{}, false
	}
	if rest != "" {
		if t.Kind == catalog.KindDate {
			return time.Time{}, false
		}
		if tm, ok = parseClock(tm, rest, t.Precision); !ok {
			return time.Time{}, false
		}
	}
	return tm, inRange(tm, t)
}

func parseDate(s string) (time.Time, bool) {
	if len(s) != dateLen || s[4] != '-' || s[7] != '-' {
		return time.Time{}, false
	}
	y, ok1 := digitsValue(s[0:4])
	m, ok2 := digitsValue(s[5:7])
	d, ok3 := digitsValue(s[8:10])
	if !ok1 || !ok2 || !ok3 {
		return time.Time{}, false
	}
	tm := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
	return tm, tm.Year() == y && int(tm.Month()) == m && tm.Day() == d
}

// parseClock adds " hh:mm:ss[.f]" to a date; precision is the most fractional digits.
func parseClock(date time.Time, s string, precision int) (time.Time, bool) {
	const hours, minutes = 24, 60
	clock, frac, hasFrac := strings.Cut(s, ".")
	if len(clock) != dateTimeLen-dateLen || clock[0] != ' ' || clock[3] != ':' || clock[6] != ':' {
		return time.Time{}, false
	}
	h, ok1 := digitsValue(clock[1:3])
	m, ok2 := digitsValue(clock[4:6])
	sec, ok3 := digitsValue(clock[7:9])
	if !ok1 || !ok2 || !ok3 || h >= hours || m >= minutes || sec >= minutes {
		return time.Time{}, false
	}
	nanos := 0
	if hasFrac {
		v, ok := digitsValue(frac)
		if !ok || len(frac) > precision {
			return time.Time{}, false
		}
		nanos = v * int(math.Pow10(nanoDigits-len(frac)))
	}
	return date.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute +
		time.Duration(sec)*time.Second + time.Duration(nanos)), true
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

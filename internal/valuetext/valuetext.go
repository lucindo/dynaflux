// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package valuetext reads and writes values of catalog types outside DynamoDB: as text, for
// command-line flags and tables, and as JSON, for -json output, import and the HTTP query
// endpoints.
//
// Both forms write integers and decimals in plain notation, a Decimal(P, S) with S digits
// after the point, Float64 as encoding/json does, Bool as true or false, UUIDs as lowercase
// 8-4-4-4-12, a Date as YYYY-MM-DD, and DateTime and DateTime64(p) as RFC 3339 in UTC with p
// fractional digits. In JSON, Int64, UInt64, Decimal, UUID and temporal values are strings,
// arrays are JSON arrays and NULL is null. In text, a String is its bytes unquoted, NULL is
// NULL, and an array is a ClickHouse literal such as ['/a','/b'] or [1,2].
//
// Parsing accepts more than formatting writes: times with any UTC offset and up to 6
// fractional digits, truncated to the type's precision (sql.md §4.3); UUIDs in either case;
// integers and decimals as JSON numbers when exact, or as strings; array elements quoted or
// bare. Text has no form for NULL: parameters and keys are never Nullable. A parsed value
// fits its type as DynamoDB stores it.
package valuetext

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
)

const (
	decimalBase = 10
	bits64      = 64
	// maxFraction is the most fractional digits of a time: microseconds.
	maxFraction = 6
	// encoding/json writes a Float64 outside [floatPlainMin, floatPlainMax) with an exponent.
	floatPlainMin = 1e-6
	floatPlainMax = 1e21
	uuidForm      = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
)

// AppendText appends the text form of v, a value of type t.
func AppendText(dst []byte, t catalog.Type, v expr.Value) []byte {
	if v.IsNull() {
		return append(dst, "NULL"...)
	}
	if !t.Array {
		return appendScalar(dst, t, v)
	}
	elem := scalar(t)
	dst = append(dst, '[')
	for i, e := range v.AsArray() {
		if i > 0 {
			dst = append(dst, ',')
		}
		if quoted(t.Kind) {
			dst = appendQuoted(dst, string(appendScalar(nil, elem, e)))
		} else {
			dst = appendScalar(dst, elem, e)
		}
	}
	return append(dst, ']')
}

// ParseText parses s, the text form of a value of type t.
func ParseText(t catalog.Type, s string) (expr.Value, error) {
	var v expr.Value
	var err error
	if t.Array {
		v, err = parseArray(scalar(t), s)
	} else {
		v, err = parseScalar(t, s, false)
	}
	if err == nil {
		err = fits(t, v)
	}
	if err != nil {
		return expr.Value{}, fmt.Errorf("invalid %s %q: %w", t, s, err)
	}
	return v, nil
}

// scalar returns the type of t's elements, or t without Nullable.
func scalar(t catalog.Type) catalog.Type {
	return catalog.Type{Kind: t.Kind, Precision: t.Precision, Scale: t.Scale}
}

// fits checks v against the rules DynamoDB storage applies to type t: integer ranges,
// decimal digits, the Float64 number range and the temporal ranges.
func fits(t catalog.Type, v expr.Value) error {
	_, _, err := keys.EncodeValue(t, v)
	return err
}

// quoted reports whether an array literal quotes elements of kind k, as ClickHouse does.
func quoted(k catalog.Kind) bool {
	switch k {
	case catalog.KindString, catalog.KindUUID, catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		return true
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64, catalog.KindInt8,
		catalog.KindInt16, catalog.KindInt32, catalog.KindInt64, catalog.KindFloat64, catalog.KindDecimal, catalog.KindBool:
	}
	return false
}

func appendScalar(dst []byte, t catalog.Type, v expr.Value) []byte {
	switch t.Kind {
	case catalog.KindString:
		return append(dst, v.AsString()...)
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64:
		return strconv.AppendUint(dst, v.AsUint(), decimalBase)
	case catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64:
		return strconv.AppendInt(dst, v.AsInt(), decimalBase)
	case catalog.KindFloat64:
		return appendFloat(dst, v.AsFloat())
	case catalog.KindDecimal:
		return appendDecimal(dst, v.AsDecimal(), t.Scale)
	case catalog.KindBool:
		return strconv.AppendBool(dst, v.AsBool())
	case catalog.KindUUID:
		return appendUUID(dst, v.AsUUID())
	case catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		return time.UnixMicro(v.AsMicros()).UTC().AppendFormat(dst, timeLayout(t))
	}
	return dst
}

// timeLayout writes a Date as YYYY-MM-DD and a time as RFC 3339 with the type's digits.
func timeLayout(t catalog.Type) string {
	if t.Kind == catalog.KindDate {
		return time.DateOnly
	}
	if t.Kind == catalog.KindDateTime64 {
		return "2006-01-02T15:04:05." + strings.Repeat("0", t.Precision) + "Z07:00"
	}
	return time.RFC3339
}

// appendFloat writes f as encoding/json does: the shortest text that reads back as f.
func appendFloat(dst []byte, f float64) []byte {
	format := byte('f')
	if a := math.Abs(f); a != 0 && (a < floatPlainMin || a >= floatPlainMax) {
		format = 'e'
	}
	dst = strconv.AppendFloat(dst, f, format, -1, bits64)
	if n := len(dst); format == 'e' && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
		// e-07 becomes e-7.
		dst[n-2] = dst[n-1]
		dst = dst[:n-1]
	}
	return dst
}

// appendDecimal writes a decimal in plain notation with exactly scale fractional digits.
func appendDecimal(dst []byte, unscaled *big.Int, scale int) []byte {
	if unscaled.Sign() < 0 {
		dst = append(dst, '-')
	}
	digits := new(big.Int).Abs(unscaled).String()
	digits = strings.Repeat("0", max(scale+1-len(digits), 0)) + digits
	point := len(digits) - scale
	dst = append(dst, digits[:point]...)
	if scale > 0 {
		dst = append(append(dst, '.'), digits[point:]...)
	}
	return dst
}

func appendUUID(dst []byte, u [16]byte) []byte {
	h := hex.EncodeToString(u[:])
	j := 0
	for i := range len(uuidForm) {
		if uuidForm[i] == '-' {
			dst = append(dst, '-')
			continue
		}
		dst = append(dst, h[j])
		j++
	}
	return dst
}

// appendQuoted writes s in single quotes, escaping \ and ' with a backslash.
func appendQuoted(dst []byte, s string) []byte {
	dst = append(dst, '\'')
	for i := range len(s) {
		if s[i] == '\\' || s[i] == '\'' {
			dst = append(dst, '\\')
		}
		dst = append(dst, s[i])
	}
	return append(dst, '\'')
}

// parseScalar parses the text form of a scalar of type t. With exp, a decimal or integer may
// have an exponent, as a JSON number may.
func parseScalar(t catalog.Type, s string, exp bool) (expr.Value, error) {
	switch t.Kind {
	case catalog.KindString:
		if !utf8.ValidString(s) {
			return expr.Value{}, errors.New("not valid UTF-8")
		}
		return expr.String(s), nil
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64,
		catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64:
		return parseInt(t, s, exp)
	case catalog.KindFloat64:
		f, err := strconv.ParseFloat(s, bits64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return expr.Value{}, errors.New("want a finite number")
		}
		return expr.Float(f), nil
	case catalog.KindDecimal:
		d, err := parseDecimal(s, t.Scale, exp)
		if err != nil {
			return expr.Value{}, err
		}
		return expr.Decimal(d), nil
	case catalog.KindBool:
		if s != "true" && s != "false" {
			return expr.Value{}, errors.New("want true or false")
		}
		return expr.Bool(s == "true"), nil
	case catalog.KindUUID:
		return parseUUID(s)
	case catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		return parseTime(t, s)
	}
	return expr.Value{}, fmt.Errorf("invalid type %s", t)
}

// parseInt reads an integer in base 10, or with exp any exact decimal number.
func parseInt(t catalog.Type, s string, exp bool) (expr.Value, error) {
	digits := s
	if exp {
		d, err := parseDecimal(s, 0, true)
		if err != nil {
			return expr.Value{}, errors.New("want an integer")
		}
		digits = d.String()
	}
	if expr.KindOf(t.Kind) == expr.KindUint {
		u, err := strconv.ParseUint(digits, decimalBase, bits64)
		if err != nil {
			return expr.Value{}, intError(err)
		}
		return expr.Uint(u), nil
	}
	i, err := strconv.ParseInt(digits, decimalBase, bits64)
	if err != nil {
		return expr.Value{}, intError(err)
	}
	return expr.Int(i), nil
}

func intError(err error) error {
	if errors.Is(err, strconv.ErrRange) {
		return errors.New("out of range")
	}
	return errors.New("want an integer")
}

// parseDecimal reads "-", digits, ".", digits and, with exp, an exponent, and returns the
// unscaled value at scale. It fails when the number has nonzero digits past the scale.
func parseDecimal(s string, scale int, exp bool) (*big.Int, error) {
	syntax := errors.New("want a decimal number, such as -12.50")
	body, neg := strings.CutPrefix(s, "-")
	mantissa, power := body, 0
	if exp {
		if i := strings.IndexAny(body, "eE"); i >= 0 {
			p, err := strconv.Atoi(strings.TrimPrefix(body[i+1:], "+"))
			if err != nil || strings.HasPrefix(body[i+1:], "+-") {
				return nil, syntax
			}
			// Clamped so that shift below can't overflow; past these bounds it fails either way.
			limit := len(body) + catalog.MaxDecimalPrecision + 1
			mantissa, power = body[:i], min(max(p, -limit), limit)
		}
	}
	whole, frac, point := strings.Cut(mantissa, ".")
	if !isDigits(whole) || point && !isDigits(frac) {
		return nil, syntax
	}
	digits := strings.TrimLeft(whole+frac, "0")
	if digits == "" {
		return new(big.Int), nil
	}
	// The value is digits × 10^shift at the scale; a shift past 38 digits can't fit any type.
	shift := power - len(frac) + scale
	switch {
	case shift > catalog.MaxDecimalPrecision:
		return nil, fmt.Errorf("more than %d digits", catalog.MaxDecimalPrecision)
	case shift >= 0:
		digits += strings.Repeat("0", shift)
	case shift < -len(digits) || strings.Trim(digits[len(digits)+shift:], "0") != "":
		return nil, fmt.Errorf("more than %d digits after the point", scale)
	default:
		digits = digits[:len(digits)+shift]
	}
	// No type holds more, and SetString takes time quadratic in the digits.
	if len(digits) > catalog.MaxDecimalPrecision {
		return nil, fmt.Errorf("more than %d digits", catalog.MaxDecimalPrecision)
	}
	d, _ := new(big.Int).SetString(digits, decimalBase) // only digits, checked above
	if neg {
		d.Neg(d)
	}
	return d, nil
}

func isDigits(s string) bool {
	return s != "" && strings.Trim(s, "0123456789") == ""
}

// parseUUID reads 8-4-4-4-12 hex digits in either case.
func parseUUID(s string) (expr.Value, error) {
	ok := len(s) == len(uuidForm)
	var h []byte
	for i := 0; ok && i < len(s); i++ {
		if uuidForm[i] == '-' {
			ok = s[i] == '-'
		} else {
			h = append(h, s[i])
		}
	}
	var u [16]byte
	if !ok {
		return expr.Value{}, errors.New("want 8-4-4-4-12 hex digits")
	}
	if _, err := hex.Decode(u[:], h); err != nil {
		return expr.Value{}, errors.New("want 8-4-4-4-12 hex digits")
	}
	return expr.UUID(u), nil
}

// parseTime reads a Date as YYYY-MM-DD and a time as RFC 3339 with up to 6 fractional digits,
// truncated to the type's precision.
func parseTime(t catalog.Type, s string) (expr.Value, error) {
	if t.Kind == catalog.KindDate {
		tm, err := time.Parse(time.DateOnly, s)
		if err != nil {
			return expr.Value{}, errors.New("want YYYY-MM-DD")
		}
		return expr.Micros(tm.UnixMicro()), nil
	}
	tm, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || fractionDigits(s) > maxFraction {
		return expr.Value{}, errors.New("want RFC 3339 with at most 6 fractional digits, such as 2026-10-26T13:45:00.250Z")
	}
	precision := 0
	if t.Kind == catalog.KindDateTime64 {
		precision = t.Precision
	}
	unit := int64(1)
	for range maxFraction - precision {
		unit *= decimalBase
	}
	us := tm.UnixMicro()
	// Truncation rounds toward the past, so a time before 1970 stays out of range.
	us -= (us%unit + unit) % unit
	return expr.Micros(us), nil
}

// fractionDigits counts the digits after the seconds' point, which [time.Parse] also takes as
// a comma.
func fractionDigits(s string) int {
	i := strings.IndexAny(s, ".,")
	if i < 0 {
		return 0
	}
	n := 0
	for _, c := range s[i+1:] {
		if c < '0' || c > '9' {
			break
		}
		n++
	}
	return n
}

// parseArray reads a ClickHouse array literal: elements between [ and ], separated by commas,
// each in single quotes (with \' and \\ escapes) or bare.
func parseArray(elem catalog.Type, s string) (expr.Value, error) {
	body, ok := strings.CutPrefix(strings.TrimSpace(s), "[")
	body, closed := strings.CutSuffix(body, "]")
	if !ok || !closed {
		return expr.Value{}, errors.New("want an array literal, such as ['a','b'] or [1,2]")
	}
	vals := []expr.Value{}
	rest := strings.TrimSpace(body)
	for i := 1; rest != ""; i++ {
		var text string
		if rest[0] == '\'' {
			var err error
			if text, rest, err = cutQuoted(rest); err != nil {
				return expr.Value{}, fmt.Errorf("element %d: %w", i, err)
			}
		} else {
			end := strings.IndexByte(rest, ',')
			if end < 0 {
				end = len(rest)
			}
			text, rest = strings.TrimSpace(rest[:end]), rest[end:]
		}
		v, err := parseScalar(elem, text, false)
		if err != nil {
			return expr.Value{}, fmt.Errorf("element %d: %w", i, err)
		}
		vals = append(vals, v)
		rest = strings.TrimSpace(rest)
		if rest == "" {
			break
		}
		next, comma := strings.CutPrefix(rest, ",")
		if rest = strings.TrimSpace(next); !comma || rest == "" {
			return expr.Value{}, fmt.Errorf("element %d: want a comma and another element, or ]", i)
		}
	}
	return expr.Array(vals), nil
}

// cutQuoted reads the single-quoted string that starts s and returns it and the rest of s.
func cutQuoted(s string) (string, string, error) {
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\'':
			return b.String(), s[i+1:], nil
		case '\\':
			if i+1 == len(s) || s[i+1] != '\'' && s[i+1] != '\\' {
				return "", "", errors.New(`only \' and \\ are escapes`)
			}
			i++
		}
		b.WriteByte(s[i])
	}
	return "", "", errors.New("no closing quote")
}

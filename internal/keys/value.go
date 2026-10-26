// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

const (
	decimalBase = 10
	uuidLen     = 16
	// numberDigits is the precision of a DynamoDB number.
	numberDigits = 38
	// maxExactDigits bounds an exact count or sum: 2^64 inputs of 38 digits stay below 10^58
	// (encoding.md §7.8). The bound keeps a stray exponent from allocating without limit.
	maxExactDigits = 60
	// maxExponent bounds a parsed exponent, far past any number in range, so that exponent
	// arithmetic can't overflow.
	maxExponent = 1 << 20
	// uint64Digits is the most digits an integer value or a time in microseconds has.
	uint64Digits = 20
	// minSeqDigits is the shortest sequence number DynamoDB delivers.
	minSeqDigits = 21
	// ttlMargin is added to every _ttl (encoding.md §5.4).
	ttlMargin = 3600
)

// DynamoDB's number range for Float64 values (sql.md §4.3): 0, or a magnitude from 1e-130 up
// to but not including 1e126.
const (
	minFloat = 1e-130
	maxFloat = 1e126
)

// EncodeValue returns the attribute value of v, a value of the column type t (encoding.md
// §5.1). present is false for NULL, which is stored as an absent attribute. It fails for a
// value outside its type, which no attribute can hold.
func EncodeValue(t catalog.Type, v expr.Value) (store.Value, bool, error) {
	if v.IsNull() {
		if !t.Nullable {
			return store.Value{}, false, fmt.Errorf("NULL is not a %s value", t)
		}
		return store.Value{}, false, nil
	}
	if !t.Array {
		a, err := encodeScalar(t, v)
		if err != nil {
			return store.Value{}, false, err
		}
		return a, true, nil
	}
	elem := catalog.Type{Kind: t.Kind, Precision: t.Precision, Scale: t.Scale}
	elems := v.AsArray()
	list := make([]store.Value, len(elems))
	for i, e := range elems {
		if e.IsNull() {
			return store.Value{}, false, fmt.Errorf("element %d is NULL", i+1)
		}
		a, err := encodeScalar(elem, e)
		if err != nil {
			return store.Value{}, false, fmt.Errorf("element %d: %w", i+1, err)
		}
		list[i] = a
	}
	return store.Value{Type: store.L, List: list}, true, nil
}

func encodeScalar(t catalog.Type, v expr.Value) (store.Value, error) {
	switch t.Kind {
	case catalog.KindString:
		return store.Value{Type: store.S, Str: v.AsString()}, nil
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64,
		catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64:
		if err := checkInt(t.Kind, v); err != nil {
			return store.Value{}, err
		}
		if _, signed := intBits(t.Kind); signed {
			return number(strconv.FormatInt(v.AsInt(), 10)), nil
		}
		return number(strconv.FormatUint(v.AsUint(), 10)), nil
	case catalog.KindFloat64:
		f := v.AsFloat()
		if a := math.Abs(f); f != 0 && !(a >= minFloat && a < maxFloat) {
			return store.Value{}, fmt.Errorf("%s value %g is outside DynamoDB's number range", t, f)
		}
		return number(strconv.FormatFloat(f, 'f', -1, 64)), nil
	case catalog.KindDecimal:
		if err := checkDecimal(t, v.AsDecimal()); err != nil {
			return store.Value{}, err
		}
		return number(formatDecimal(v.AsDecimal(), t.Scale)), nil
	case catalog.KindBool:
		return store.Value{Type: store.BOOL, Bool: v.AsBool()}, nil
	case catalog.KindUUID:
		return store.Value{Type: store.S, Str: string(appendUUID(nil, v.AsUUID()))}, nil
	case catalog.KindDate:
		b, err := appendTime(nil, t, v.AsMicros())
		if err != nil {
			return store.Value{}, err
		}
		return store.Value{Type: store.S, Str: string(b)}, nil
	case catalog.KindDateTime, catalog.KindDateTime64:
		if err := checkTime(t, v.AsMicros()); err != nil {
			return store.Value{}, err
		}
		return number(strconv.FormatInt(v.AsMicros(), 10)), nil
	}
	return store.Value{}, fmt.Errorf("invalid type %s", t)
}

func number(text string) store.Value { return store.Value{Type: store.N, Str: text} }

// DecodeValue returns the value of the column type t that an attribute holds (encoding.md
// §5.1), or NULL for an absent attribute of a Nullable type. Numbers are read exactly, in any
// form DynamoDB returns, and must fit t. A missing non-Nullable column is an error: its
// DEFAULT belongs to the caller.
func DecodeValue(t catalog.Type, a store.Value, present bool) (expr.Value, error) {
	if !present {
		if t.Nullable {
			return expr.Null(), nil
		}
		return expr.Value{}, fmt.Errorf("a %s value is missing", t)
	}
	if !t.Array {
		return decodeScalar(t, a)
	}
	if a.Type != store.L {
		return expr.Value{}, wrongType(t, store.L)
	}
	elem := catalog.Type{Kind: t.Kind, Precision: t.Precision, Scale: t.Scale}
	vs := make([]expr.Value, len(a.List))
	for i, e := range a.List {
		v, err := decodeScalar(elem, e)
		if err != nil {
			return expr.Value{}, fmt.Errorf("element %d: %w", i+1, err)
		}
		vs[i] = v
	}
	return expr.Array(vs), nil
}

func decodeScalar(t catalog.Type, a store.Value) (expr.Value, error) {
	if want := attrType(t.Kind); a.Type != want {
		return expr.Value{}, wrongType(t, want)
	}
	switch t.Kind {
	case catalog.KindString:
		return expr.String(a.Str), nil
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64,
		catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64:
		return decodeInt(t, a.Str)
	case catalog.KindFloat64:
		if _, ok := parseNumber(a.Str); !ok {
			return expr.Value{}, fmt.Errorf("%q is not a number", a.Str)
		}
		f, err := strconv.ParseFloat(a.Str, 64)
		if err != nil {
			return expr.Value{}, fmt.Errorf("%s value %q: %w", t, a.Str, err)
		}
		return expr.Float(f), nil
	case catalog.KindDecimal:
		d, err := scaledNumber(t, a.Str, t.Scale, t.Precision)
		if err != nil {
			return expr.Value{}, err
		}
		return expr.Decimal(d), nil
	case catalog.KindBool:
		return expr.Bool(a.Bool), nil
	case catalog.KindUUID:
		return parseUUID(t, a.Str)
	case catalog.KindDate:
		tm, err := time.Parse(layoutDate, a.Str)
		if err != nil || tm.Format(layoutDate) != a.Str {
			return expr.Value{}, fmt.Errorf("%s value %q is not YYYY-MM-DD", t, a.Str)
		}
		return micros(t, tm.UnixMicro())
	case catalog.KindDateTime, catalog.KindDateTime64:
		d, err := scaledNumber(t, a.Str, 0, uint64Digits)
		if err != nil {
			return expr.Value{}, err
		}
		if !d.IsInt64() {
			return expr.Value{}, fmt.Errorf("%s value %s (microseconds) is outside 1970 to 2106", t, d)
		}
		return micros(t, d.Int64())
	}
	return expr.Value{}, fmt.Errorf("invalid type %s", t)
}

func attrType(k catalog.Kind) store.Type {
	switch k {
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64,
		catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64,
		catalog.KindFloat64, catalog.KindDecimal, catalog.KindDateTime, catalog.KindDateTime64:
		return store.N
	case catalog.KindBool:
		return store.BOOL
	case catalog.KindString, catalog.KindUUID, catalog.KindDate:
	}
	return store.S
}

func wrongType(t catalog.Type, want store.Type) error {
	names := [...]string{store.S: "S", store.N: "N", store.B: "B", store.BOOL: "BOOL", store.L: "L", store.M: "M"}
	return fmt.Errorf("a %s value must be a DynamoDB %s", t, names[want])
}

func micros(t catalog.Type, us int64) (expr.Value, error) {
	if err := checkTime(t, us); err != nil {
		return expr.Value{}, err
	}
	return expr.Micros(us), nil
}

func decodeInt(t catalog.Type, text string) (expr.Value, error) {
	d, err := scaledNumber(t, text, 0, uint64Digits)
	if err != nil {
		return expr.Value{}, err
	}
	var v expr.Value
	_, signed := intBits(t.Kind)
	switch {
	case signed && d.IsInt64():
		v = expr.Int(d.Int64())
	case !signed && d.IsUint64():
		v = expr.Uint(d.Uint64())
	default:
		return expr.Value{}, fmt.Errorf("%s value %s is out of range", t, d)
	}
	if rangeErr := checkInt(t.Kind, v); rangeErr != nil {
		return expr.Value{}, rangeErr
	}
	return v, nil
}

// scaledNumber reads number text as an integer count of 10^−scale units with at most
// maxDigits digits.
func scaledNumber(t catalog.Type, text string, scale, maxDigits int) (*big.Int, error) {
	n, ok := parseNumber(text)
	if !ok {
		return nil, fmt.Errorf("%q is not a number", text)
	}
	d, ok := n.scaled(scale, maxDigits)
	if !ok {
		return nil, fmt.Errorf("%s can't hold %s exactly", t, text)
	}
	return d, nil
}

// parseUUID accepts only the lowercase 8-4-4-4-12 form [appendUUID] writes.
func parseUUID(t catalog.Type, s string) (expr.Value, error) {
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != uuidLen || string(appendUUID(nil, [uuidLen]byte(b))) != s {
		return expr.Value{}, fmt.Errorf("%s value %q is not lowercase 8-4-4-4-12 hex", t, s)
	}
	return expr.UUID([uuidLen]byte(b)), nil
}

// formatDecimal writes an unscaled value at scale as decimal text: an optional '-', at least
// one integer digit, then '.' and exactly scale digits when scale > 0. Zero is never negative.
func formatDecimal(unscaled *big.Int, scale int) string {
	digits := padded(new(big.Int).Abs(unscaled).String(), scale+1)
	var b strings.Builder
	if unscaled.Sign() < 0 {
		b.WriteByte('-')
	}
	point := len(digits) - scale
	b.WriteString(digits[:point])
	if scale > 0 {
		b.WriteByte('.')
		b.WriteString(digits[point:])
	}
	return b.String()
}

// EncodeExact returns the attribute value of an exact count or sum (encoding.md §5.2): the
// decimal text of unscaled at scale, an N when unscaled has at most 38 digits and an S
// otherwise.
func EncodeExact(unscaled *big.Int, scale int) store.Value {
	text := formatDecimal(unscaled, scale)
	if len(new(big.Int).Abs(unscaled).String()) > numberDigits {
		return store.Value{Type: store.S, Str: text}
	}
	return number(text)
}

// DecodeExact reads an exact count or sum written by [EncodeExact] as an unscaled value at
// scale. The attribute type must be the one EncodeExact picks for the value. An N may be in any
// form DynamoDB returns; an S, which DynamoDB keeps as written, must be EncodeExact's text.
func DecodeExact(a store.Value, scale int) (*big.Int, error) {
	if a.Type != store.N && a.Type != store.S {
		return nil, errors.New("an exact number must be a DynamoDB N or S")
	}
	n, ok := parseNumber(a.Str)
	if !ok {
		return nil, fmt.Errorf("%q is not a number", a.Str)
	}
	d, ok := n.scaled(scale, maxExactDigits)
	if !ok {
		return nil, fmt.Errorf("%s is not a whole number at scale %d", a.Str, scale)
	}
	if w := EncodeExact(d, scale); w.Type != a.Type || a.Type == store.S && w.Str != a.Str {
		return nil, fmt.Errorf("exact number %q at scale %d is not in the form Dynaflux writes", a.Str, scale)
	}
	return d, nil
}

// PadSeq returns a delivered sequence number in its stored form: 40 digits, zero-padded, so
// that string order is numeric order (encoding.md §5.3). DynamoDB delivers 21 to 40 digits.
func PadSeq(delivered string) (string, error) {
	if len(delivered) < minSeqDigits || len(delivered) > catalog.SequenceWidth ||
		!isDigits(delivered) || strings.Trim(delivered, "0") == "" {
		return "", fmt.Errorf("sequence number %q is not 21 to 40 decimal digits above zero", delivered)
	}
	return padded(delivered, catalog.SequenceWidth), nil
}

// TTL returns the _ttl attribute of an item that expires at expiryMicros (encoding.md §5.4):
// Unix seconds, rounded up, plus an hour for readers whose clock is slow.
func TTL(expiryMicros int64) store.Value {
	s := expiryMicros / microsPerSecond
	if expiryMicros%microsPerSecond > 0 {
		s++
	}
	return number(strconv.FormatInt(s+ttlMargin, 10))
}

// TTLExpiry returns the expiry that a _ttl attribute stands for, in microseconds: the one
// given to [TTL], rounded up to a whole second.
func TTLExpiry(a store.Value) (int64, error) {
	v, err := DecodeValue(catalog.Type{Kind: catalog.KindInt64}, a, true)
	if err != nil {
		return 0, err
	}
	s := v.AsInt() - ttlMargin
	if s < 0 || s > math.MaxInt64/microsPerSecond {
		return 0, fmt.Errorf("_ttl %s is not an expiry Dynaflux writes", a.Str)
	}
	return s * microsPerSecond, nil
}

func isDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}

// decimal is a parsed number: int(digits) × 10^exp, with digits free of leading and trailing
// zeros, and empty for zero.
type decimal struct {
	neg    bool
	digits string
	exp    int
}

// parseNumber reads -?D+(.D+)?([eE][+-]?D+)?, the syntax of every number Dynaflux reads.
func parseNumber(s string) (decimal, bool) {
	rest, neg := strings.CutPrefix(s, "-")
	intPart, rest := digitRun(rest)
	frac, ok := "", intPart != ""
	if after, found := strings.CutPrefix(rest, "."); found {
		frac, rest = digitRun(after)
		ok = ok && frac != ""
	}
	e10 := 0
	if rest != "" {
		if rest[0] != 'e' && rest[0] != 'E' {
			return decimal{}, false
		}
		e, err := strconv.Atoi(rest[1:])
		if err != nil || e < -maxExponent || e > maxExponent {
			return decimal{}, false
		}
		e10 = e
	}
	if !ok {
		return decimal{}, false
	}
	mant := strings.TrimLeft(intPart+frac, "0")
	trimmed := strings.TrimRight(mant, "0")
	if trimmed == "" {
		return decimal{}, true
	}
	return decimal{neg: neg, digits: trimmed, exp: e10 - len(frac) + len(mant) - len(trimmed)}, true
}

func digitRun(s string) (string, string) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return s[:i], s[i:]
}

// scaled returns d × 10^scale when it is an integer of at most maxDigits digits.
func (d decimal) scaled(scale, maxDigits int) (*big.Int, bool) {
	if d.digits == "" {
		return new(big.Int), true
	}
	// digits has no trailing zero, so d × 10^scale is an integer only when exp + scale ≥ 0.
	zeros := d.exp + scale
	if zeros < 0 || len(d.digits)+zeros > maxDigits {
		return nil, false
	}
	v, _ := new(big.Int).SetString(d.digits+strings.Repeat("0", zeros), decimalBase)
	if d.neg {
		v.Neg(v)
	}
	return v, true
}

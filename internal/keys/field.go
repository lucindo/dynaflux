// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// NoSortKey is the sort key of an item without sort fields: a view row whose ORDER BY has
// none after its PRIMARY KEY, and a uniqExact marker.
const NoSortKey = "!"

// Reserved key bytes (encoding.md §3.1). Inside a field every byte is escape or above.
const (
	sep    = '#' // between fields
	above  = '$' // range bounds only: never stored
	escape = '%' // starts an escape inside a String field
)

const (
	upperHex = "0123456789ABCDEF"
	lowerHex = "0123456789abcdef"
)

// Suffix letters and the count of digits after them (encoding.md §3.3).
const (
	shardLetter  = 's'
	splitLetter  = 'p'
	suffixDigits = 2
)

// Time text (encoding.md §3.2) and the range of temporal values (sql.md §4.1).
const (
	layoutDate        = "2006-01-02"
	layoutDateTime    = "2006-01-02T15:04:05Z"
	layoutDateTime64m = "2006-01-02T15:04:05.000Z"
	layoutDateTime64u = "2006-01-02T15:04:05.000000Z"

	microsPerMilli  = 1_000
	microsPerSecond = 1_000_000
	microsPerDay    = 86_400 * microsPerSecond
	// timeLimit is 2106-02-07 06:28:15 UTC, the first instant past the range.
	timeLimit = math.MaxUint32 * microsPerSecond
)

// streamSKLen is the length of every stream sort key: event_time, '#', event_id.
const streamSKLen = len(layoutDateTime64u) + 1 + eventIDLen

// AppendField appends the key field of v, a value of the key type t, to dst (encoding.md
// §3.2). The field's byte order is the type's order.
func AppendField(dst []byte, t catalog.Type, v expr.Value) ([]byte, error) {
	if t.Nullable || t.Array {
		return dst, fmt.Errorf("%s is not a key type", t)
	}
	if v.IsNull() {
		return dst, errors.New("a key value is NULL")
	}
	switch t.Kind {
	case catalog.KindString:
		return appendString(dst, v.AsString()), nil
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64,
		catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64:
		return appendInt(dst, t.Kind, v)
	case catalog.KindDecimal:
		return appendDecimalField(dst, t, v.AsDecimal())
	case catalog.KindBool:
		return strconv.AppendBool(dst, v.AsBool()), nil
	case catalog.KindUUID:
		return appendUUID(dst, v.AsUUID()), nil
	case catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		return appendTime(dst, t, v.AsMicros())
	case catalog.KindFloat64:
	}
	return dst, fmt.Errorf("%s is not a key type", t)
}

// Key returns the key of the values vs of the key types ts: their fields joined by '#'
// (encoding.md §3.3). Callers check the result against DynamoDB's key size limits.
func Key(ts []catalog.Type, vs []expr.Value) (string, error) {
	if len(ts) == 0 || len(ts) != len(vs) {
		return "", fmt.Errorf("a key needs one value per field: %d types, %d values", len(ts), len(vs))
	}
	var b []byte
	for i, t := range ts {
		if i > 0 {
			b = append(b, sep)
		}
		var err error
		if b, err = AppendField(b, t, vs[i]); err != nil {
			return "", fmt.Errorf("key field %d: %w", i+1, err)
		}
	}
	return string(b), nil
}

// ShardKey returns the partition key of shard i of a stream with SHARDS: pk, '#', then
// s00 to s63. i is a [Bucket] below the stream's SHARDS count, at most [catalog.MaxShards].
func ShardKey(pk string, i int) string {
	return pk + string(sep) + suffix(shardLetter, i)
}

// SplitKey returns the sort key of item i of a SPLIT logical row whose sort fields encode to
// lsk: lsk, '#', then p00 to p15, or the suffix alone when the view has no sort fields and
// lsk is "". i is a [Bucket] below the view's SPLIT count, at most [catalog.MaxSplit].
func SplitKey(lsk string, i int) string {
	if lsk == "" {
		return suffix(splitLetter, i)
	}
	return lsk + string(sep) + suffix(splitLetter, i)
}

func suffix(letter byte, i int) string {
	return string(fmt.Appendf([]byte{letter}, "%0*d", suffixDigits, i))
}

// StreamSK returns a stream item's sort key (encoding.md §6): event_time in microseconds as a
// DateTime64(6) field, '#', then the 26-character event_id. It is always 54 bytes.
func StreamSK(eventTime int64, eventID string) (string, error) {
	if _, ok := decodeBase32(eventID, eventHashLen); !ok {
		return "", fmt.Errorf("event_id %q is not 26 characters of base32", eventID)
	}
	t := catalog.Type{Kind: catalog.KindDateTime64, Precision: catalog.DateTime64Micros}
	b, err := appendTime(make([]byte, 0, streamSKLen), t, eventTime)
	if err != nil {
		return "", fmt.Errorf("event_time: %w", err)
	}
	return string(append(append(b, sep), eventID...)), nil
}

// ParseStreamSK returns the event_time, in microseconds, and the event_id of a stream item's
// sort key. It accepts only what [StreamSK] returns.
func ParseStreamSK(sk string) (int64, string, error) {
	timeLen := len(layoutDateTime64u)
	if len(sk) != streamSKLen || sk[timeLen] != sep {
		return 0, "", fmt.Errorf("stream sort key %q is not event_time#event_id", sk)
	}
	tm, err := time.Parse(layoutDateTime64u, sk[:timeLen])
	if err != nil || tm.Format(layoutDateTime64u) != sk[:timeLen] {
		return 0, "", fmt.Errorf("stream sort key %q: event_time is not RFC 3339 with 6 fractional digits", sk)
	}
	us := tm.UnixMicro()
	if us < 0 || us >= timeLimit {
		return 0, "", fmt.Errorf("stream sort key %q: event_time is outside 1970 to 2106", sk)
	}
	id := sk[timeLen+1:]
	if _, ok := decodeBase32(id, eventHashLen); !ok {
		return 0, "", fmt.Errorf("stream sort key %q: event_id is not 26 characters of base32", sk)
	}
	return us, id, nil
}

// appendString escapes each byte up to escape as '%' and two uppercase hex digits. Escapes
// sort below every kept byte, and the empty string's '%' is a prefix of every escape.
func appendString(dst []byte, s string) []byte {
	if s == "" {
		return append(dst, escape)
	}
	for i := range len(s) {
		if c := s[i]; c <= escape {
			dst = append(dst, escape, upperHex[c>>4], upperHex[c&0xF])
		} else {
			dst = append(dst, c)
		}
	}
	return dst
}

// appendInt writes an integer zero-padded to the digits of its type's largest magnitude; a
// negative one is '-' and the nines' complement of its padded magnitude, so that it sorts
// below zero and in reverse order of magnitude.
func appendInt(dst []byte, k catalog.Kind, v expr.Value) ([]byte, error) {
	if err := checkInt(k, v); err != nil {
		return dst, err
	}
	bits, signed := intBits(k)
	shift := wordBits - bits
	if !signed {
		width := len(strconv.FormatUint(math.MaxUint64>>shift, 10))
		return appendPadded(dst, strconv.FormatUint(v.AsUint(), 10), width), nil
	}
	width := len(strconv.FormatInt(math.MinInt64>>shift, 10)) - 1 // without the sign
	i := v.AsInt()
	if i >= 0 {
		return appendPadded(dst, strconv.FormatInt(i, 10), width), nil
	}
	magnitude := strconv.FormatInt(i, 10)[1:]
	return appendComplement(append(dst, '-'), padded(magnitude, width)), nil
}

func checkInt(k catalog.Kind, v expr.Value) error {
	bits, signed := intBits(k)
	shift := wordBits - bits
	if signed {
		if i := v.AsInt(); i < math.MinInt64>>shift || i > math.MaxInt64>>shift {
			return fmt.Errorf("%s value %d is out of range", k, i)
		}
		return nil
	}
	if u := v.AsUint(); u > math.MaxUint64>>shift {
		return fmt.Errorf("%s value %d is out of range", k, u)
	}
	return nil
}

const wordBits = 64

// intBits returns the width of an integer kind and whether it is signed; 0 for other kinds.
func intBits(k catalog.Kind) (int, bool) {
	const bits8, bits16, bits32, bits64 = 8, 16, 32, 64
	switch k {
	case catalog.KindUInt8:
		return bits8, false
	case catalog.KindUInt16:
		return bits16, false
	case catalog.KindUInt32:
		return bits32, false
	case catalog.KindUInt64:
		return bits64, false
	case catalog.KindInt8:
		return bits8, true
	case catalog.KindInt16:
		return bits16, true
	case catalog.KindInt32:
		return bits32, true
	case catalog.KindInt64:
		return bits64, true
	case catalog.KindString, catalog.KindFloat64, catalog.KindDecimal, catalog.KindBool, catalog.KindUUID,
		catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
	}
	return 0, false
}

// appendDecimalField writes the unscaled value zero-padded to P digits, '.' before the last S,
// and a negative value as '-' and the nines' complement of those digits.
func appendDecimalField(dst []byte, t catalog.Type, unscaled *big.Int) ([]byte, error) {
	if err := checkDecimal(t, unscaled); err != nil {
		return dst, err
	}
	digits := padded(new(big.Int).Abs(unscaled).String(), t.Precision)
	if unscaled.Sign() < 0 {
		dst = append(dst, '-')
		digits = string(appendComplement(nil, digits))
	}
	point := t.Precision - t.Scale
	dst = append(dst, digits[:point]...)
	if t.Scale > 0 {
		dst = append(append(dst, '.'), digits[point:]...)
	}
	return dst, nil
}

func checkDecimal(t catalog.Type, unscaled *big.Int) error {
	if len(new(big.Int).Abs(unscaled).String()) > t.Precision {
		return fmt.Errorf("%s value has more than %d digits", t, t.Precision)
	}
	return nil
}

func padded(digits string, width int) string {
	return strings.Repeat("0", max(width-len(digits), 0)) + digits
}

func appendPadded(dst []byte, digits string, width int) []byte {
	return append(dst, padded(digits, width)...)
}

func appendComplement(dst []byte, digits string) []byte {
	for i := range len(digits) {
		dst = append(dst, '9'-(digits[i]-'0'))
	}
	return dst
}

// appendUUID writes the lowercase 8-4-4-4-12 hex form.
func appendUUID(dst []byte, u [16]byte) []byte {
	b := u[:]
	for i, n := range [...]int{4, 2, 2, 2, 6} {
		if i > 0 {
			dst = append(dst, '-')
		}
		for _, x := range b[:n] {
			dst = append(dst, lowerHex[x>>4], lowerHex[x&0xF])
		}
		b = b[n:]
	}
	return dst
}

// appendTime writes RFC 3339 text in UTC with the type's fractional digits. Years are 1970 to
// 2106, so the text has a fixed width and sorts in time order.
func appendTime(dst []byte, t catalog.Type, us int64) ([]byte, error) {
	if err := checkTime(t, us); err != nil {
		return dst, err
	}
	return time.UnixMicro(us).UTC().AppendFormat(dst, timeLayout(t)), nil
}

// checkTime checks that us is in the range of sql.md §4.1 and a whole unit of the type.
func checkTime(t catalog.Type, us int64) error {
	if us < 0 || us >= timeLimit {
		return fmt.Errorf("%s value %d (microseconds) is outside 1970 to 2106", t, us)
	}
	if us%timeUnit(t) != 0 {
		return fmt.Errorf("%s value %d (microseconds) has more precision than the type", t, us)
	}
	return nil
}

func timeUnit(t catalog.Type) int64 {
	switch {
	case t.Kind == catalog.KindDate:
		return microsPerDay
	case t.Kind == catalog.KindDateTime:
		return microsPerSecond
	case t.Precision == catalog.DateTime64Millis:
		return microsPerMilli
	}
	return 1
}

func timeLayout(t catalog.Type) string {
	switch {
	case t.Kind == catalog.KindDate:
		return layoutDate
	case t.Kind == catalog.KindDateTime:
		return layoutDateTime
	case t.Precision == catalog.DateTime64Millis:
		return layoutDateTime64m
	}
	return layoutDateTime64u
}

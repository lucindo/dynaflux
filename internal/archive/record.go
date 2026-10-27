// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"bytes"
	"encoding/hex"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// MaxRecordSize is Firehose's limit on one record: 1,000 KiB before base64.
const MaxRecordSize = 1_024_000

// The longest bookkeeping values a stream record can give: a DynamoDB Streams shard ID has
// at most 65 characters, and an arrival time has 16 digits in microseconds until 2286.
const (
	maxUnitLen       = 65
	maxArrivalMicros = 9_999_999_999_000_000
)

const (
	decimalBase = 10
	float64Bits = 64
	// encoding/json writes a float64 in exponent form outside [floatPlainMin, floatPlainMax).
	floatPlainMin = 1e-6
	floatPlainMax = 1e21
)

// Encoder writes the archive records of a stream: each one compact JSON object, with the
// keys in the order of the stream's [Table] columns and NULL values left out. Equal inputs
// give equal bytes, so the copies of a record that Firehose receives more than once are
// identical. It is safe for concurrent use.
type Encoder struct {
	fields []field // the implicit and declared columns, in table order
}

type field struct {
	key []byte // the column's name as a JSON string, and ':'
	typ catalog.Type
	val int // the column's index in AppendRecord's vals
}

// NewEncoder returns the encoder of s's records.
func NewEncoder(s *catalog.Stream) *Encoder {
	implicit, declared := len(catalog.ImplicitColumns()), len(s.Columns)
	e := &Encoder{}
	for i, c := range columns(s)[:implicit+declared] {
		val := declared + i // the implicit values follow the declared ones
		if i >= implicit {
			val = i - implicit
		}
		e.fields = append(e.fields, field{key: append(appendString(nil, c.Name), ':'), typ: c.Type, val: val})
	}
	return e
}

// AppendRecord appends the record of an event to dst. vals are the event's values as
// projector.Stream.Decode returns them: the declared columns' in catalog order, then the
// implicit columns' in [catalog.ImplicitColumns] order. unit is the delivery unit's ID,
// paddedSeq the record's sequence number in [catalog.SequenceWidth] digits, and arrival
// the record's arrival in the stream, left out when zero.
func (e *Encoder) AppendRecord(dst []byte, vals []expr.Value, unit, paddedSeq string, arrival time.Time) []byte {
	sep := byte('{')
	for _, f := range e.fields {
		v := vals[f.val]
		if v.IsNull() {
			continue
		}
		dst = append(append(dst, sep), f.key...)
		dst = appendValue(dst, f.typ, v)
		sep = ','
	}
	dst = append(append(dst, sep), `"`+ColumnShard+`":`...)
	dst = appendString(dst, unit)
	dst = append(dst, `,"`+ColumnSeq+`":`...)
	dst = appendString(dst, paddedSeq)
	if !arrival.IsZero() {
		dst = append(dst, `,"`+ColumnArrival+`":`...)
		dst = strconv.AppendInt(dst, arrival.UnixMicro(), decimalBase)
	}
	return append(dst, '}')
}

// MaxSize returns the size of the largest record that vals can make, whatever delivery
// unit, sequence number and arrival time the stream record gives it.
func (e *Encoder) MaxSize(vals []expr.Value) int {
	unit := strings.Repeat("u", maxUnitLen)
	seq := strings.Repeat("9", catalog.SequenceWidth)
	return len(e.AppendRecord(nil, vals, unit, seq, time.UnixMicro(maxArrivalMicros)))
}

func appendValue(dst []byte, t catalog.Type, v expr.Value) []byte {
	if !t.Array {
		return appendScalar(dst, t, v)
	}
	dst = append(dst, '[')
	for i, el := range v.AsArray() {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendScalar(dst, t, el)
	}
	return append(dst, ']')
}

// appendScalar writes the JSON that Firehose parses into the column type of t (infra.md
// §7.3). Integers that a double can't hold exactly, and decimals, are strings; times are
// numbers of microseconds.
func appendScalar(dst []byte, t catalog.Type, v expr.Value) []byte {
	switch t.Kind {
	case catalog.KindString:
		return appendString(dst, v.AsString())
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32:
		return strconv.AppendUint(dst, v.AsUint(), decimalBase)
	case catalog.KindInt8, catalog.KindInt16, catalog.KindInt32:
		return strconv.AppendInt(dst, v.AsInt(), decimalBase)
	case catalog.KindUInt64:
		return append(strconv.AppendUint(append(dst, '"'), v.AsUint(), decimalBase), '"')
	case catalog.KindInt64:
		return append(strconv.AppendInt(append(dst, '"'), v.AsInt(), decimalBase), '"')
	case catalog.KindDecimal:
		return append(appendDecimal(append(dst, '"'), v.AsDecimal(), t.Scale), '"')
	case catalog.KindFloat64:
		return appendFloat(dst, v.AsFloat())
	case catalog.KindBool:
		return strconv.AppendBool(dst, v.AsBool())
	case catalog.KindUUID:
		return append(appendUUID(append(dst, '"'), v.AsUUID()), '"')
	case catalog.KindDate:
		return append(time.UnixMicro(v.AsMicros()).UTC().AppendFormat(append(dst, '"'), time.DateOnly), '"')
	case catalog.KindDateTime, catalog.KindDateTime64:
		return strconv.AppendInt(dst, v.AsMicros(), decimalBase)
	}
	return dst
}

// appendDecimal writes a decimal in plain notation with exactly scale fractional digits.
func appendDecimal(dst []byte, unscaled *big.Int, scale int) []byte {
	if unscaled.Sign() < 0 {
		dst = append(dst, '-')
	}
	digits := new(big.Int).Abs(unscaled).Append(nil, decimalBase)
	if pad := scale + 1 - len(digits); pad > 0 {
		digits = append(bytes.Repeat([]byte{'0'}, pad), digits...)
	}
	point := len(digits) - scale
	dst = append(dst, digits[:point]...)
	if scale > 0 {
		dst = append(append(dst, '.'), digits[point:]...)
	}
	return dst
}

// appendFloat writes f as encoding/json does: the shortest text that reads back as f.
func appendFloat(dst []byte, f float64) []byte {
	format := byte('f')
	if a := math.Abs(f); a != 0 && (a < floatPlainMin || a >= floatPlainMax) {
		format = 'e'
	}
	dst = strconv.AppendFloat(dst, f, format, -1, float64Bits)
	if n := len(dst); format == 'e' && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
		// e-07 becomes e-7.
		dst[n-2] = dst[n-1]
		dst = dst[:n-1]
	}
	return dst
}

// appendUUID writes the lowercase 8-4-4-4-12 form.
func appendUUID(dst []byte, u [16]byte) []byte {
	b := u[:]
	for i, n := range [...]int{4, 2, 2, 2, 6} {
		if i > 0 {
			dst = append(dst, '-')
		}
		dst = hex.AppendEncode(dst, b[:n])
		b = b[n:]
	}
	return dst
}

// appendString writes s as a JSON string, as encoding/json does with HTML escaping off: <,
// > and & would take six bytes each.
func appendString(dst []byte, s string) []byte {
	const hexDigits = "0123456789abcdef"
	dst = append(dst, '"')
	for i := 0; i < len(s); {
		b := s[i]
		if b >= utf8.RuneSelf {
			r, n := utf8.DecodeRuneInString(s[i:])
			switch {
			case r == utf8.RuneError && n == 1:
				dst = append(dst, `\ufffd`...)
			case r == '\u2028' || r == '\u2029':
				dst = append(dst, `\u202`...)
				dst = append(dst, hexDigits[r&0xF])
			default:
				dst = append(dst, s[i:i+n]...)
			}
			i += n
			continue
		}
		switch b {
		case '"', '\\':
			dst = append(dst, '\\', b)
		case '\b':
			dst = append(dst, `\b`...)
		case '\f':
			dst = append(dst, `\f`...)
		case '\n':
			dst = append(dst, `\n`...)
		case '\r':
			dst = append(dst, `\r`...)
		case '\t':
			dst = append(dst, `\t`...)
		default:
			if b < ' ' {
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xF])
			} else {
				dst = append(dst, b)
			}
		}
		i++
	}
	return append(dst, '"')
}

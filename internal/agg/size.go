// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg

import (
	"encoding/binary"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

const (
	decimalBase = 10
	wordBits    = 64
	byteBits    = 8
)

// Sizes of the values a worst-case state holds (encoding.md §5.1, §7.2).
const (
	// numberDigits is the precision of a DynamoDB number: an exact number with more digits is
	// stored as an S.
	numberDigits = 38
	// inputBits bounds the inputs of one row: 2^64.
	inputBits = 64
	// eventIDLen is the length of every event_id: a ULID or a DEDUP BY hash in base32.
	eventIDLen = 26
	// floatDigits is the most significant digits in the shortest text of a float64.
	floatDigits     = 17
	uuidLen         = len("01234567-89ab-cdef-0123-456789abcdef")
	dateLen         = len("2006-01-02")
	microsPerSecond = 1_000_000
	microsPerMilli  = 1_000
)

// size is the size [store.ItemSize] counts for an attribute value.
func size(v store.Value) int { return store.ItemSize(store.Item{"": v}) }

// nines is a number with d significant digits.
func nines(d int) store.Value { return store.Value{Type: store.N, Str: strings.Repeat("9", d)} }

func text(n int) store.Value { return store.Value{Type: store.S, Str: strings.Repeat("x", n)} }

func digits(n int) int { return len(strconv.Itoa(n)) }

// worstExact returns a stored exact number at scale as large as any of magnitude at most
// maxAbs (encoding.md §5.2): an N of up to 38 digits, or an S past them.
func worstExact(maxAbs *big.Int, scale int, signed bool) store.Value {
	d := len(maxAbs.String())
	if d <= numberDigits {
		return nines(d)
	}
	if scale > 0 {
		d++
	}
	if signed {
		d++
	}
	return text(d)
}

func worstCount() store.Value {
	return worstExact(new(big.Int).Lsh(big.NewInt(1), inputBits), 0, false)
}

// worstSum returns the largest stored sum of 2^64 values of type t.
func worstSum(t catalog.Type) store.Value {
	if t.Kind == catalog.KindFloat64 {
		return store.Value{Type: store.B, Bin: make([]byte, maxDyadicLen())}
	}
	m := new(big.Int).SetUint64(maxMagnitude(t.Kind))
	if t.Kind == catalog.KindDecimal {
		m.Exp(big.NewInt(decimalBase), big.NewInt(int64(t.Precision)), nil).Sub(m, big.NewInt(1))
	}
	return worstExact(m.Lsh(m, inputBits), t.Scale, t.Kind == catalog.KindDecimal || isSigned(t.Kind))
}

// maxDyadicLen bounds the bytes of an exact Float64 sum of 2^64 values: a 2-byte exponent,
// the sign and a mantissa from bit −1074 to bit 1087.
func maxDyadicLen() int {
	var buf [binary.MaxVarintLen64]byte
	e := max(binary.PutVarint(buf[:], minSumExp), binary.PutVarint(buf[:], maxSumBits))
	return e + 1 + (maxSumBits-minSumExp+byteBits-1)/byteBits
}

// MaxValueSize returns the largest attribute value of a value of t that may reach a view's
// state, as [store.ItemSize] counts it (encoding.md §5.1): a String counts at
// [MaxStringBytes].
func MaxValueSize(t catalog.Type) int { return size(worstValue(t)) }

// worstValue returns the largest attribute value of a value of t that may reach a view's
// state (encoding.md §5.1).
func worstValue(t catalog.Type) store.Value {
	switch k := t.Kind; {
	case k == catalog.KindString:
		return text(MaxStringBytes)
	case k == catalog.KindFloat64:
		return nines(floatDigits)
	case k == catalog.KindDecimal:
		return nines(t.Precision)
	case k == catalog.KindBool:
		return store.Value{Type: store.BOOL}
	case k == catalog.KindUUID:
		return text(uuidLen)
	case k == catalog.KindDate:
		return text(dateLen)
	case k == catalog.KindDateTime:
		return nines(digits((catalog.TimeLimitMicros - 1) / microsPerSecond))
	case k == catalog.KindDateTime64 && t.Precision == catalog.DateTime64Millis:
		return nines(digits((catalog.TimeLimitMicros - 1) / microsPerMilli))
	case k == catalog.KindDateTime64:
		return nines(digits(catalog.TimeLimitMicros - 1))
	}
	return nines(len(strconv.FormatUint(maxMagnitude(t.Kind), decimalBase)))
}

// maxMagnitude returns the largest magnitude of a value of an integer kind.
func maxMagnitude(k catalog.Kind) uint64 {
	bits, signed := intBits(k)
	if signed {
		return 1 << (bits - 1)
	}
	return math.MaxUint64 >> (wordBits - bits)
}

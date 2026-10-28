// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

// crockford is Crockford's base32 alphabet. It is in ASCII order, so base32 text sorts like
// the bytes it encodes.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

const (
	base32Bits = 5
	base32Mask = 1<<base32Bits - 1
	byteBits   = 8

	unitHashLen  = 8  // bytes of H(u) in a unit key and a backfill mark
	eventHashLen = 16 // bytes of an event_id, a ULID or a marker identity
	unitKeyLen   = 13 // base32 of unitHashLen bytes
	eventIDLen   = 26 // base32 of eventHashLen bytes

	markerPrefix = "!"
)

// Owner kinds: the first byte of a uniqExact marker's owner (encoding.md §7.4).
const (
	OwnerLive     byte = 'L'
	OwnerBackfill byte = 'B'
)

// UnitKey names the delivery unit unit in high-water marks, marker owners and the unit's
// meta item: 13 characters of base32 (encoding.md §4.2).
func UnitKey(unit string) string {
	h := sha256.Sum256([]byte(unit))
	return string(appendBase32(make([]byte, 0, unitKeyLen), h[:unitHashLen]))
}

// Canonical appends the canonical bytes of v to dst (encoding.md §4.4). They identify values
// for uniq, uniqExact and the per-record deduplication of values. t is the aggregate
// argument's scalar type, and v is not NULL; a Nullable(T) value has T's bytes.
func Canonical(dst []byte, t catalog.Type, v expr.Value) []byte {
	switch t.Kind {
	case catalog.KindString:
		return append(dst, v.AsString()...)
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64:
		return binary.BigEndian.AppendUint64(dst, v.AsUint())
	case catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64:
		return binary.BigEndian.AppendUint64(dst, uint64(v.AsInt())) //nolint:gosec // two's complement bits, as specified
	case catalog.KindFloat64:
		return binary.BigEndian.AppendUint64(dst, math.Float64bits(v.AsFloat()))
	case catalog.KindDecimal:
		return appendInt128(dst, v.AsDecimal())
	case catalog.KindBool:
		if v.AsBool() {
			return append(dst, 1)
		}
		return append(dst, 0)
	case catalog.KindUUID:
		u := v.AsUUID()
		return append(dst, u[:]...)
	case catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		return binary.BigEndian.AppendUint64(dst, uint64(v.AsMicros())) //nolint:gosec // two's complement bits, as specified
	}
	return dst
}

// appendInt128 appends x as 16 bytes of big-endian two's complement. A Decimal's unscaled
// value is below 10^38 < 2^127 in magnitude, so it fits.
func appendInt128(dst []byte, x *big.Int) []byte {
	var b [16]byte
	if x.Sign() >= 0 {
		x.FillBytes(b[:])
		return append(dst, b[:]...)
	}
	// −x in two's complement is the bitwise complement of |x| − 1.
	m := new(big.Int).Neg(x)
	m.Sub(m, big.NewInt(1)).FillBytes(b[:])
	for i := range b {
		b[i] = ^b[i]
	}
	return append(dst, b[:]...)
}

// DedupEventID returns the event_id of an event of a stream with DEDUP BY (encoding.md §4.1).
// key is the [Key] of the PRIMARY KEY values, event_time as DateTime64(6) and the DEDUP BY
// values, in that order.
func DedupEventID(key string) string {
	h := sha256.Sum256([]byte(key))
	return string(appendBase32(make([]byte, 0, eventIDLen), h[:eventHashLen]))
}

// Bucket returns BE64(H(x)[0:8]) mod n (encoding.md §4.3): the shard of an event_id with
// SHARDS n, or the item of a delivery unit's contributions with SPLIT n. n is at least 1, as
// the catalog checks.
func Bucket(x string, n int) int {
	h := sha256.Sum256([]byte(x))
	return int(binary.BigEndian.Uint64(h[:8]) % uint64(n)) //nolint:gosec // n ≥ 1, so the result is below n
}

// MarkerKey returns the key of the uniqExact marker for a value of column in a logical row
// (encoding.md §7.4). pk is the row's partition key, lsk its sort fields without a SPLIT
// suffix ("" when the view has none), and canon the value's [Canonical] bytes.
func MarkerKey(pk, lsk, column string, canon []byte) store.Key {
	b := make([]byte, 0, len(pk)+len(lsk)+len(column)+len(canon)+3*binary.MaxVarintLen64)
	for _, s := range [...]string{pk, lsk, column} {
		b = binary.AppendUvarint(b, uint64(len(s)))
		b = append(b, s...)
	}
	b = append(b, canon...)
	h := sha256.Sum256(b)
	id := appendBase32(append(make([]byte, 0, len(markerPrefix)+eventIDLen), markerPrefix...), h[:eventHashLen])
	return store.Key{PK: string(id), SK: NoSortKey}
}

// Owner returns a uniqExact marker's owner (encoding.md §7.4): kind ([OwnerLive] or
// [OwnerBackfill]), the record's 13-character [UnitKey], then its sequence number in
// [PadSeq]'s form. It fails for any other input, such as a unit ID in place of its key.
func Owner(kind byte, unitKey, seq string) (string, error) {
	if kind != OwnerLive && kind != OwnerBackfill {
		return "", fmt.Errorf("marker owner kind %q is not %q or %q", kind, OwnerLive, OwnerBackfill)
	}
	if _, ok := decodeBase32(unitKey, unitHashLen); !ok || !validSeq(seq) {
		return "", fmt.Errorf("marker owner: %q, %q is not a unit key and a sequence number", unitKey, seq)
	}
	return string(kind) + unitKey + seq, nil
}

// OwnerEvent returns the owner of the uniqExact markers that a SYNC append's transaction
// writes (encoding.md §7.4): E, then the event's 26-character event_id. It fails for any
// other input.
func OwnerEvent(eventID string) (string, error) {
	if _, ok := decodeBase32(eventID, eventHashLen); !ok {
		return "", fmt.Errorf("marker owner: %q is not an event_id", eventID)
	}
	return "E" + eventID, nil
}

// appendBase32 appends base32(b) (encoding.md §4): b read as one big-endian number,
// left-padded with zero bits to a multiple of 5.
func appendBase32(dst, b []byte) []byte {
	chars := (len(b)*byteBits + base32Bits - 1) / base32Bits
	acc, bits := uint(0), chars*base32Bits-len(b)*byteBits // the padding bits are zeros in acc
	for _, x := range b {
		acc = acc<<byteBits | uint(x)
		bits += byteBits
		for bits >= base32Bits {
			bits -= base32Bits
			dst = append(dst, crockford[acc>>bits&base32Mask])
		}
	}
	return dst
}

// decodeBase32 decodes the base32 text of n bytes. It accepts only what appendBase32 writes:
// the exact length, uppercase alphabet characters and zero padding bits.
func decodeBase32(s string, n int) ([]byte, bool) {
	chars := (n*byteBits + base32Bits - 1) / base32Bits
	if len(s) != chars {
		return nil, false
	}
	out := make([]byte, 0, n)
	acc, bits := uint(0), -(chars*base32Bits - n*byteBits) // bits of acc not yet output, padding excluded
	for i := range len(s) {
		d := strings.IndexByte(crockford, s[i])
		if d < 0 || i == 0 && d>>(base32Bits+bits) != 0 {
			return nil, false
		}
		acc = acc<<base32Bits | uint(d)
		bits += base32Bits
		if bits >= byteBits {
			bits -= byteBits
			out = append(out, byte(acc>>bits&math.MaxUint8))
		}
	}
	return out, true
}

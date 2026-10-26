// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

import (
	"math"
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
)

// Row sizes follow DynamoDB's item size rules: each attribute costs the bytes of its name
// plus those of its value. Keys, Strings and sketches take typical sizes, so row sizes are
// estimates.
const (
	bytesPerKB = 1024
	// unknownLenBytes is the assumed size of a String or Array value.
	unknownLenBytes = 32
	// Sizes of fixed-width values.
	bytes8   = 1
	bytes16  = 2
	bytes32  = 4
	bytes64  = 8
	bytes128 = 16
	// Digits of the largest values that aren't plain integers.
	float64Digits     = 17
	unixSecondsDigits = 10
	// Estimated states: a count, a sum, and an avg's sum and count. uniqExact's counter is a
	// count.
	countBytes = 8
	sumBytes   = 12
	avgBytes   = 20
	// expiryBytes is the value of _ttl, epoch seconds; mapHeaderBytes is the fixed part of
	// a map such as _hwm.
	expiryBytes    = 6
	mapHeaderBytes = 3
	bitsPerByte    = 8
	// hwmEntryBytes is the size of a high-water mark: a 13-byte unit key, a binary sequence
	// number of about 10 bytes, and 1 byte of map overhead (encoding.md §7.6).
	hwmEntryBytes = 24
	// A typical quantile sketch: values over a million-fold range (20 octaves), M buckets
	// per octave for its rel_acc (35 at 0.01), about 2 bytes each (encoding.md §7.5).
	quantileOctaves     = 20
	quantileBucketBytes = 2
	quantileRelAcc      = 0.01
)

// attribute is an attribute of a view row.
type attribute struct {
	name     string
	what     string // what the value holds, for the report
	bytes    int    // name and value; without the sketch of a uniq
	state    stateKind
	maxBytes int // a quantile's largest name and value
}

type stateKind uint8

const (
	plainState    stateKind = iota
	hllState                // uniq: the model adds the HyperLogLog
	quantileState           // quantile, quantileDD: bytes holds a typical sketch, maxBytes the largest
)

// rowLayout returns the attributes of a row of vv, with _hwm's map header only: the model
// estimates how many high-water marks it holds.
func rowLayout(vv *catalog.ViewVersion) []attribute {
	pk, sk := vv.Keys[:vv.PartitionKeyLen], vv.Keys[vv.PartitionKeyLen:]
	skWhat, skBytes := "a constant", bytes8
	if len(sk) > 0 {
		skWhat, skBytes = keyNames(sk), keyBytes(sk)
	}
	if vv.Split > 1 {
		skWhat += " and the item number"
		skBytes += bytes8
	}
	attrs := []attribute{
		{name: catalog.AttrPK, what: keyNames(pk), bytes: len(catalog.AttrPK) + keyBytes(pk)},
		{name: catalog.AttrSK, what: skWhat, bytes: len(catalog.AttrSK) + skBytes},
	}
	for _, k := range vv.Keys {
		attrs = append(attrs, attribute{name: k.Name, what: "key column", bytes: len(k.Name) + valueBytes(k.Expr.Type)})
	}
	for _, a := range vv.Aggregates {
		attrs = append(attrs, aggregateAttribute(a))
	}
	attrs = append(attrs, attribute{name: catalog.AttrRowVersion, what: "row version", bytes: len(catalog.AttrRowVersion) + countBytes})
	if vv.TTL != nil {
		attrs = append(attrs, attribute{name: catalog.AttrTTL, what: "expiry", bytes: len(catalog.AttrTTL) + expiryBytes})
	}
	return append(attrs, attribute{name: catalog.AttrHighWaterMarks, what: "high-water marks", bytes: len(catalog.AttrHighWaterMarks) + mapHeaderBytes})
}

func rowBytes(layout []attribute) int {
	n := 0
	for _, a := range layout {
		n += a.bytes
	}
	return n
}

func aggregateAttribute(a catalog.ViewAggregate) attribute {
	attr := attribute{name: a.Name, what: aggregateText(a.Aggregate), bytes: len(a.Name)}
	switch a.Fn {
	case catalog.AggCount, catalog.AggUniqExact:
		attr.bytes += countBytes
	case catalog.AggSum:
		attr.bytes += sumBytes
	case catalog.AggAvg:
		attr.bytes += avgBytes
	case catalog.AggMin, catalog.AggMax, catalog.AggAny:
		attr.bytes += valueBytes(a.Args[0].Type)
	case catalog.AggArgMax, catalog.AggArgMin:
		attr.bytes += valueBytes(a.Args[0].Type) + valueBytes(a.Args[1].Type)
	case catalog.AggUniq:
		attr.state = hllState
	case catalog.AggQuantile, catalog.AggQuantileDD:
		attr.state = quantileState
		relAcc := quantileRelAcc
		if a.Fn == catalog.AggQuantileDD {
			relAcc = a.Params[0]
		}
		perOctave := math.Ceil(math.Ln2 / math.Log((1+relAcc)/(1-relAcc)))
		attr.bytes += int(perOctave) * quantileOctaves * quantileBucketBytes
		if f, err := agg.Compile(agg.Spec{Agg: a.Aggregate, ArgTypes: []catalog.Type{a.Args[0].Type}}); err == nil {
			attr.maxBytes = len(a.Name) + f.MaxSize()
		}
	}
	return attr
}

// aggregateText returns a short form of a, such as sum(duration_ms) or countIf(…).
func aggregateText(a catalog.Aggregate) string {
	fn := string(a.Fn)
	args := make([]string, 0, len(a.Args)+1)
	for _, arg := range a.Args {
		args = append(args, exprText(arg))
	}
	if a.Cond != nil {
		fn += "If"
		args = append(args, "…")
	}
	return fn + "(" + strings.Join(args, ", ") + ")"
}

// exprText returns a column's name and a parameter's placeholder, and an ellipsis for
// anything else.
func exprText(e catalog.Expr) string {
	switch e.Op { //nolint:exhaustive // every other op prints as an ellipsis
	case catalog.OpColumn:
		return e.Name
	case catalog.OpParam:
		return "{" + e.Name + "}"
	}
	return "…"
}

func keyNames(keys []catalog.NamedExpr) string {
	return strings.Join(keyNamesOf(keys), ", ")
}

// keyBytes returns the size of the encoded key columns: fixed-width types take their width.
func keyBytes(keys []catalog.NamedExpr) int {
	n := 0
	for _, k := range keys {
		if w := width(k.Expr.Type); w > 0 {
			n += w
		} else {
			n += unknownLenBytes
		}
	}
	return n
}

// width returns the bytes of a fixed-width type, and 0 for a String.
func width(t catalog.Type) int {
	switch t.Kind {
	case catalog.KindUInt8, catalog.KindInt8, catalog.KindBool:
		return bytes8
	case catalog.KindUInt16, catalog.KindInt16, catalog.KindDate:
		return bytes16
	case catalog.KindUInt32, catalog.KindInt32, catalog.KindDateTime:
		return bytes32
	case catalog.KindUInt64, catalog.KindInt64, catalog.KindFloat64, catalog.KindDateTime64:
		return bytes64
	case catalog.KindDecimal, catalog.KindUUID:
		return bytes128
	case catalog.KindString:
	}
	return 0
}

// valueBytes returns the size of a value of type t as a native DynamoDB attribute value,
// at most: NULL takes 1 byte.
func valueBytes(t catalog.Type) int {
	if t.Array {
		return unknownLenBytes
	}
	switch t.Kind {
	case catalog.KindString:
		return unknownLenBytes
	case catalog.KindBool:
		return bytes8
	case catalog.KindUUID:
		return bytes128
	case catalog.KindFloat64:
		return numberBytes(float64Digits)
	case catalog.KindDecimal:
		return numberBytes(t.Precision)
	case catalog.KindDateTime64:
		return numberBytes(unixSecondsDigits + t.Precision)
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64, catalog.KindInt8,
		catalog.KindInt16, catalog.KindInt32, catalog.KindInt64, catalog.KindDate, catalog.KindDateTime:
	}
	// Integers, and Date and DateTime, which are days and seconds: as many digits as the
	// largest unsigned value of their width.
	largest := uint64(math.MaxUint64) >> (bitsPerByte * (bytes64 - width(t)))
	return numberBytes(len(strconv.FormatUint(largest, 10)))
}

// numberBytes is the size of a DynamoDB number of digits significant digits.
func numberBytes(digits int) int {
	return (digits+1)/2 + 1
}

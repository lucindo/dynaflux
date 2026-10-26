// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"math/big"
	"strings"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

const (
	bytesPerKB = 1024
	// maxItemKB is DynamoDB's item limit. hwmReserveKB is the part of it that a row item
	// keeps for _hwm, _v, _ttl and slack (encoding.md §7.8).
	maxItemKB    = 400
	hwmReserveKB = 32
)

// checkItemSizes rejects a view whose row item can pass DynamoDB's item limit before its
// high-water marks (DF3307): then whether a write fits would depend on the region and the
// order of events.
func (p *project) checkItemSizes() {
	const limit = (maxItemKB - hwmReserveKB) * bytesPerKB
	for _, d := range p.decls {
		v := d.view
		if v == nil || v.stream == nil || !v.keysOK {
			continue
		}
		if n := maxItemSize(&v.version); n > limit {
			d.st.errorf(d.pos, "DF3307",
				"move some aggregates to another view, or use quantileDD with a larger rel_acc",
				"a row can reach %.1f KB before its high-water marks; DynamoDB's %d KB item limit leaves %d KB "+
					"after the %d KB reserved for them", float64(n)/bytesPerKB, maxItemKB, maxItemKB-hwmReserveKB, hwmReserveKB)
		}
	}
}

// maxItemSize returns the largest size of a row item of vv without _hwm, _v and _ttl: its
// key, key columns and states, attribute names included. String key fields count at their
// key's DynamoDB limit.
func maxItemSize(vv *catalog.ViewVersion) int {
	n := vv.PartitionKeyLen
	pk, pkStrings := keySize(vv.Keys[:n])
	if pkStrings {
		pk = store.MaxPKLen
	}
	sk, skStrings := keySize(vv.Keys[n:])
	switch {
	case skStrings:
		sk = store.MaxSKLen
	case vv.Split > 1:
		sk = len(keys.SplitKey(strings.Repeat("x", sk), vv.Split-1))
	case sk == 0:
		sk = len(keys.NoSortKey)
	}
	size := len(catalog.AttrPK) + pk + len(catalog.AttrSK) + sk
	for i, k := range vv.Keys {
		v := agg.MaxValueSize(k.Expr.Type)
		switch {
		case k.Expr.Type.Kind != catalog.KindString:
		case i < n:
			v = store.MaxPKLen
		default:
			v = store.MaxSKLen
		}
		size += len(k.Name) + v
	}
	for _, a := range vv.Aggregates {
		spec := agg.Spec{Agg: a.Aggregate}
		for _, arg := range a.Args {
			spec.ArgTypes = append(spec.ArgTypes, arg.Type)
		}
		// Only an aggregate that validate rejected doesn't compile; engine's tests check that.
		if f, err := agg.Compile(spec); err == nil {
			size += len(a.Name) + f.MaxSize()
		}
	}
	return size
}

// keySize returns the encoded size of the key fields of ks without their String fields, and
// whether ks has one.
func keySize(ks []catalog.NamedExpr) (int, bool) {
	size, hasString := 0, false
	for i, k := range ks {
		t := k.Expr.Type
		if t.Kind == catalog.KindString {
			hasString = true
			continue
		}
		f, err := keys.AppendField(nil, t, widest(t))
		if err == nil {
			size += len(f)
		}
		if i > 0 {
			size++ // the '#' between fields
		}
	}
	return size, hasString
}

// widest returns a value of the fixed-width key type t whose field is the longest: a
// negative number has a sign, and false is longer than true.
func widest(t catalog.Type) expr.Value {
	switch t.Kind { //nolint:exhaustive // every other key type has one width
	case catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64:
		return expr.Int(-1)
	case catalog.KindDecimal:
		return expr.Decimal(big.NewInt(-1))
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64:
		return expr.Uint(0)
	case catalog.KindBool:
		return expr.Bool(false)
	case catalog.KindUUID:
		return expr.UUID([16]byte{})
	}
	return expr.Micros(0)
}

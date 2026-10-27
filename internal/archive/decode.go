// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// Decoder reads archive records back into stream items: the inverse of [Encoder], for an
// UNLOAD JSON row or the rawData of a record Firehose couldn't deliver. It is safe for
// concurrent use.
type Decoder struct {
	s        *catalog.Stream
	types    map[string]catalog.Type // every column of the stream's table
	declared map[string]bool
	key      []catalog.Column // the PRIMARY KEY columns
}

// NewDecoder returns the decoder of s's records.
func NewDecoder(s *catalog.Stream) *Decoder {
	d := &Decoder{s: s, types: map[string]catalog.Type{}, declared: map[string]bool{}}
	for _, c := range columns(s) {
		d.types[c.Name] = c.Type
	}
	for _, c := range s.Columns {
		d.declared[c.Name] = true
	}
	for _, name := range s.PrimaryKey {
		c, _ := s.Column(name)
		d.key = append(d.key, c)
	}
	return d
}

// Decode returns the delivery unit, the sequence number in [catalog.SequenceWidth] digits and
// the stream item of rec, an archive record. The item has no _ttl, which the archive doesn't
// hold. A key that isn't a column of the stream's table fails, and a null value is NULL.
func (d *Decoder) Decode(rec []byte) (string, string, store.Item, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec, &raw); err != nil {
		return "", "", nil, fmt.Errorf("archive: record of %s: %w", d.s.Name, err)
	}
	it := store.Item{}
	vals := map[string]expr.Value{} // the implicit, bookkeeping and key columns
	for _, name := range slices.Sorted(maps.Keys(raw)) {
		t, ok := d.types[name]
		if !ok {
			return "", "", nil, fmt.Errorf("archive: record of %s: %q isn't a column", d.s.Name, name)
		}
		if bytes.Equal(raw[name], []byte("null")) {
			continue
		}
		a, err := attribute(t, raw[name])
		var v expr.Value
		if err == nil {
			v, err = keys.DecodeValue(t, a, true)
		}
		if err != nil {
			return "", "", nil, fmt.Errorf("archive: record of %s: %s: %w", d.s.Name, name, err)
		}
		vals[name] = v
		if d.declared[name] {
			// Stored in the form the engine writes, whatever form the record had.
			if it[name], _, err = keys.EncodeValue(t, v); err != nil {
				return "", "", nil, fmt.Errorf("archive: record of %s: %s: %w", d.s.Name, name, err)
			}
		}
	}
	unit, seq, err := d.bookkeeping(vals)
	if err == nil {
		err = d.keys(it, vals)
	}
	if err != nil {
		return "", "", nil, fmt.Errorf("archive: record of %s: %w", d.s.Name, err)
	}
	return unit, seq, it, nil
}

// bookkeeping returns a record's unit and padded sequence number.
func (d *Decoder) bookkeeping(vals map[string]expr.Value) (string, string, error) {
	unit, ok := vals[ColumnShard]
	if !ok || unit.AsString() == "" {
		return "", "", fmt.Errorf("no %s", ColumnShard)
	}
	seq, ok := vals[ColumnSeq]
	if !ok {
		return "", "", fmt.Errorf("no %s", ColumnSeq)
	}
	padded, err := keys.PadSeq(seq.AsString())
	if err != nil {
		return "", "", fmt.Errorf("%s: %w", ColumnSeq, err)
	}
	return unit.AsString(), padded, nil
}

// keys sets the item's key and the implicit columns it stores: pk, sk, _r and _it.
func (d *Decoder) keys(it store.Item, vals map[string]expr.Value) error {
	for _, name := range []string{catalog.ColumnEventID, catalog.ColumnEventTime, catalog.ColumnIngestTime, catalog.ColumnOriginRegion} {
		if _, ok := vals[name]; !ok {
			return fmt.Errorf("no %s", name)
		}
	}
	id, et := vals[catalog.ColumnEventID].AsString(), vals[catalog.ColumnEventTime].AsMicros()
	sk, err := keys.StreamSK(et, id)
	if err != nil {
		return err
	}
	ts := make([]catalog.Type, len(d.key))
	kv := make([]expr.Value, len(d.key))
	for i, c := range d.key {
		ts[i], kv[i] = c.Type, vals[c.Name]
		if kv[i].IsNull() {
			return fmt.Errorf("no %s, a PRIMARY KEY column", c.Name)
		}
	}
	pk, err := keys.Key(ts, kv)
	if err != nil {
		return err
	}
	if d.s.Shards > 1 {
		pk = keys.ShardKey(pk, keys.Bucket(id, d.s.Shards))
	}
	it[catalog.AttrPK] = store.Value{Type: store.S, Str: pk}
	it[catalog.AttrSK] = store.Value{Type: store.S, Str: sk}
	it[catalog.AttrOriginRegion] = store.Value{Type: store.S, Str: vals[catalog.ColumnOriginRegion].AsString()}
	it[catalog.AttrIngestTime], _, err = keys.EncodeValue(catalog.EventTimeType(), vals[catalog.ColumnIngestTime])
	return err
}

// attribute returns the stored attribute that a record's JSON value raw stands for, unchecked:
// [keys.DecodeValue] checks it against t.
func attribute(t catalog.Type, raw json.RawMessage) (store.Value, error) {
	if !t.Array {
		return scalarAttribute(t.Kind, raw)
	}
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return store.Value{}, err
	}
	list := make([]store.Value, len(elems))
	for i, e := range elems {
		a, err := scalarAttribute(t.Kind, e)
		if err != nil {
			return store.Value{}, fmt.Errorf("element %d: %w", i+1, err)
		}
		list[i] = a
	}
	return store.Value{Type: store.L, List: list}, nil
}

// scalarAttribute reads a JSON string, number or bool as the attribute type of k. A number may
// be quoted: [Encoder] writes 64-bit integers and decimals that way.
func scalarAttribute(k catalog.Kind, raw json.RawMessage) (store.Value, error) {
	switch k {
	case catalog.KindBool:
		var b bool
		err := json.Unmarshal(raw, &b)
		return store.Value{Type: store.BOOL, Bool: b}, err
	case catalog.KindString, catalog.KindUUID, catalog.KindDate:
		var s string
		err := json.Unmarshal(raw, &s)
		return store.Value{Type: store.S, Str: s}, err
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64,
		catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64,
		catalog.KindFloat64, catalog.KindDecimal, catalog.KindDateTime, catalog.KindDateTime64:
	}
	var n json.Number
	err := json.Unmarshal(raw, &n)
	return store.Value{Type: store.N, Str: string(n)}, err
}

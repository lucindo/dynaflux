// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// Decoder reads archive records back into stream items: the inverse of [Encoder], for the
// rawData of a record Firehose couldn't deliver, or a row of an [Unload] export, which
// writes some values in forms of its own. It is safe for concurrent use.
type Decoder struct {
	s        *catalog.Stream
	types    map[string]catalog.Type // every column of the stream's table
	declared map[string]bool
	key      []catalog.Column // the PRIMARY KEY columns
	event    []string         // the columns of an event's values, in Encoder.AppendRecord's order
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
	for _, c := range eventColumns(s) {
		d.event = append(d.event, c.Name)
	}
	return d
}

// Record is an archive record read back.
type Record struct {
	Unit string // its delivery unit
	Seq  string // its sequence number in [catalog.SequenceWidth] digits
	// Item is its stream item, without _ttl, which the archive doesn't hold.
	Item store.Item
	// Arrival is when it entered the change stream; zero when the record lacks it.
	Arrival time.Time
}

// Decode reads rec, an archive record. A key that isn't a column of the stream's table fails,
// and a null value or a missing key is NULL. An event_id that its values don't give fails
// ([CheckEventID]).
//
// Besides the encoder's forms, it reads an integer as a decimal string, a time as text
// ("YYYY-MM-DD HH:MM:SS" and up to 6 fractional digits), and a decimal with fewer fractional
// digits than its scale. It reads numbers from their text, never through a float64.
func (d *Decoder) Decode(rec []byte) (Record, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec, &raw); err != nil {
		return Record{}, fmt.Errorf("archive: record of %s: %w", d.s.Name, err)
	}
	it := store.Item{}
	vals := map[string]expr.Value{} // the implicit, bookkeeping and key columns
	for _, name := range slices.Sorted(maps.Keys(raw)) {
		t, ok := d.types[name]
		if !ok {
			return Record{}, fmt.Errorf("archive: record of %s: %q isn't a column", d.s.Name, name)
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
			return Record{}, fmt.Errorf("archive: record of %s: %s: %w", d.s.Name, name, err)
		}
		vals[name] = v
		if d.declared[name] {
			// Stored in the form the engine writes, whatever form the record had.
			if it[name], _, err = keys.EncodeValue(t, v); err != nil {
				return Record{}, fmt.Errorf("archive: record of %s: %s: %w", d.s.Name, name, err)
			}
		}
	}
	unit, seq, err := d.bookkeeping(vals)
	if err == nil {
		err = d.keys(it, vals)
	}
	if err == nil {
		err = d.checkEventID(vals)
	}
	if err != nil {
		return Record{}, fmt.Errorf("archive: record of %s: %w", d.s.Name, err)
	}
	r := Record{Unit: unit, Seq: seq, Item: it}
	if a, ok := vals[ColumnArrival]; ok {
		r.Arrival = time.UnixMicro(a.AsMicros()).UTC()
	}
	return r, nil
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

// checkEventID checks a record's event_id against its values, by column name.
func (d *Decoder) checkEventID(vals map[string]expr.Value) error {
	if len(d.s.DedupBy) == 0 {
		return nil
	}
	event := make([]expr.Value, len(d.event))
	for i, name := range d.event {
		event[i] = vals[name] // NULL when absent
	}
	return CheckEventID(d.s, event, vals[catalog.ColumnEventID].AsString())
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
		return scalarAttribute(t, raw)
	}
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return store.Value{}, err
	}
	list := make([]store.Value, len(elems))
	for i, e := range elems {
		a, err := scalarAttribute(t, e)
		if err != nil {
			return store.Value{}, fmt.Errorf("element %d: %w", i+1, err)
		}
		list[i] = a
	}
	return store.Value{Type: store.L, List: list}, nil
}

// scalarAttribute reads a JSON string, number or bool as the attribute of a scalar value of
// type t, or of its elements for an array. A number may be quoted: [Encoder] writes 64-bit
// integers and decimals that way, and an export every bigint and decimal. A quoted time is the
// text an export writes.
func scalarAttribute(t catalog.Type, raw json.RawMessage) (store.Value, error) {
	switch t.Kind {
	case catalog.KindBool:
		var b bool
		err := json.Unmarshal(raw, &b)
		return store.Value{Type: store.BOOL, Bool: b}, err
	case catalog.KindString, catalog.KindUUID, catalog.KindDate:
		var s string
		err := json.Unmarshal(raw, &s)
		return store.Value{Type: store.S, Str: s}, err
	case catalog.KindDateTime, catalog.KindDateTime64:
		if len(raw) > 0 && raw[0] == '"' {
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return store.Value{}, err
			}
			return textMicros(s)
		}
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64,
		catalog.KindInt8, catalog.KindInt16, catalog.KindInt32, catalog.KindInt64,
		catalog.KindFloat64, catalog.KindDecimal:
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return store.Value{}, err
	}
	// An export writes a decimal in plain notation at its column's scale: more digits mean the
	// archive's scale isn't the catalog's, even where the value fits.
	if t.Kind == catalog.KindDecimal && fractionDigits(string(n)) > t.Scale {
		return store.Value{}, fmt.Errorf("%s has more fractional digits than %s", n, t)
	}
	return store.Value{Type: store.N, Str: string(n)}, nil
}

// maxFractionDigits is the precision of an archive timestamp: microseconds.
const maxFractionDigits = 6

// textMicros reads a time in the text an export writes, "YYYY-MM-DD HH:MM:SS" and 0 to 6
// fractional digits in UTC, as microseconds.
func textMicros(s string) (store.Value, error) {
	sec, frac, dotted := strings.Cut(s, ".")
	tm, err := time.Parse(time.DateTime, sec)
	if err != nil || tm.Format(time.DateTime) != sec ||
		dotted && (frac == "" || len(frac) > maxFractionDigits || strings.Trim(frac, "0123456789") != "") {
		return store.Value{}, fmt.Errorf("%q is not YYYY-MM-DD HH:MM:SS with 0 to %d fractional digits", s, maxFractionDigits)
	}
	us, unit := tm.UnixMicro(), int64(time.Second/time.Microsecond)
	for _, digit := range []byte(frac) {
		unit /= decimalBase
		us += int64(digit-'0') * unit
	}
	return store.Value{Type: store.N, Str: strconv.FormatInt(us, decimalBase)}, nil
}

// fractionDigits returns how many digits follow the point of number text n.
func fractionDigits(n string) int {
	_, frac, _ := strings.Cut(n, ".")
	if i := strings.IndexAny(frac, "eE"); i >= 0 {
		frac = frac[:i]
	}
	return len(frac)
}

// CheckEventID fails unless id is the event_id that s's DEDUP BY gives vals, an event's values
// in [Encoder.AppendRecord]'s order: the hash of its PRIMARY KEY values, event_time and DEDUP BY
// values (encoding.md §4.1). It catches a time or key value that changed on its way through the
// archive. Without DEDUP BY, event_ids are random, and it checks nothing.
func CheckEventID(s *catalog.Stream, vals []expr.Value, id string) error {
	if len(s.DedupBy) == 0 {
		return nil
	}
	cols := eventColumns(s)
	if len(vals) != len(cols) {
		return fmt.Errorf("event_id: %d values for the %d columns of %s", len(vals), len(cols), s.Name)
	}
	names := slices.Concat(s.PrimaryKey, []string{catalog.ColumnEventTime}, s.DedupBy)
	ts := make([]catalog.Type, len(names))
	vs := make([]expr.Value, len(names))
	for i, name := range names {
		j := slices.IndexFunc(cols, func(c catalog.Column) bool { return c.Name == name })
		ts[i], vs[i] = cols[j].Type, vals[j]
	}
	k, err := keys.Key(ts, vs)
	if err != nil {
		return fmt.Errorf("event_id: %w", err)
	}
	if want := keys.DedupEventID(k); id != want {
		return fmt.Errorf("event_id %s isn't %s, the hash of its PRIMARY KEY, event_time and DEDUP BY values", id, want)
	}
	return nil
}

// eventColumns returns the columns of an event's values, in [Encoder.AppendRecord]'s order.
func eventColumns(s *catalog.Stream) []catalog.Column {
	return slices.Concat(s.Columns, catalog.ImplicitColumns())
}

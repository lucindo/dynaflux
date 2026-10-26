// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"fmt"
	"reflect"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// Stream is a compiled stream: how its items decode.
type Stream struct {
	Def *catalog.Stream
	// Columns are the declared columns, in catalog order.
	Columns []Column
}

// Column is a declared stream column.
type Column struct {
	Name string
	Type catalog.Type
	// Missing is what an event stored before the column existed reads as: its DEFAULT, or
	// NULL. CanMiss is false for a column without either, which every event has.
	Missing expr.Value
	CanMiss bool
}

// The implicit columns follow the declared ones in a decoded event, in this order.
const (
	implicitEventID = iota
	implicitEventTime
	implicitIngestTime
	implicitOriginRegion
	implicitCount
)

func compileStream(def *catalog.Stream) (*Stream, error) {
	s := &Stream{Def: def}
	for _, c := range def.Columns {
		col := Column{Name: c.Name, Type: c.Type, CanMiss: c.Type.Nullable || c.Default != nil}
		if c.Default != nil {
			p, err := expr.Compile(c.Default, expr.Scope{})
			if err == nil {
				col.Missing, err = p.Eval(&expr.Frame{})
			}
			if err != nil {
				return nil, fmt.Errorf("column %s: DEFAULT: %w", c.Name, err)
			}
		}
		s.Columns = append(s.Columns, col)
	}
	return s, nil
}

// names returns the names of a decoded event's values.
func (s *Stream) names() []string {
	names := make([]string, 0, len(s.Columns)+implicitCount)
	for _, c := range s.Columns {
		names = append(names, c.Name)
	}
	for _, c := range catalog.ImplicitColumns() {
		names = append(names, c.Name)
	}
	return names
}

// Decode returns the event an item holds: the declared columns' values in catalog order,
// then event_id, event_time, ingest_time and origin_region. Attributes that are neither
// declared nor reserved are ignored. bad is nil when every value decodes; otherwise bad[i]
// says why value i doesn't, and the value is NULL.
func (s *Stream) Decode(it store.Item) ([]expr.Value, []error) {
	n := len(s.Columns)
	vals := make([]expr.Value, n+implicitCount)
	var bad []error
	fail := func(i int, name string, err error) {
		if bad == nil {
			bad = make([]error, len(vals))
		}
		bad[i] = fmt.Errorf("%s: %w", name, err)
	}
	for i, c := range s.Columns {
		if _, ok := it[c.Name]; !ok && c.CanMiss {
			vals[i] = c.Missing
			continue
		}
		v, err := decodeAttr(it, c.Name, c.Type)
		if err != nil {
			fail(i, c.Name, err)
		}
		vals[i] = v
	}
	if et, id, err := keys.ParseStreamSK(it[catalog.AttrSK].Str); err != nil {
		fail(n+implicitEventID, catalog.ColumnEventID, err)
		fail(n+implicitEventTime, catalog.ColumnEventTime, err)
	} else {
		vals[n+implicitEventID], vals[n+implicitEventTime] = expr.String(id), expr.Micros(et)
	}
	var err error
	if vals[n+implicitIngestTime], err = decodeAttr(it, catalog.AttrIngestTime, catalog.EventTimeType()); err != nil {
		fail(n+implicitIngestTime, catalog.ColumnIngestTime, err)
	}
	if vals[n+implicitOriginRegion], err = decodeAttr(it, catalog.AttrOriginRegion, catalog.Type{Kind: catalog.KindString}); err != nil {
		fail(n+implicitOriginRegion, catalog.ColumnOriginRegion, err)
	}
	return vals, bad
}

func decodeAttr(it store.Item, name string, t catalog.Type) (expr.Value, error) {
	a, ok := it[name]
	v, err := keys.DecodeValue(t, a, ok)
	if err != nil {
		return expr.Null(), err
	}
	return v, nil
}

// SamePayload reports whether two items of the stream hold the same declared values.
func (s *Stream) SamePayload(a, b store.Item) bool {
	for _, c := range s.Columns {
		if !SameValue(c.Type, a, b, c.Name) {
			return false
		}
	}
	return true
}

// SameValue reports whether attribute name holds the same value of type t in a and b, or is
// absent from both. DynamoDB rewrites numbers, so values are decoded and encoded again
// before the comparison.
func SameValue(t catalog.Type, a, b store.Item, name string) bool {
	x, inA := a[name]
	y, inB := b[name]
	if !inA || !inB {
		return inA == inB
	}
	nx, okX := normalize(t, x)
	ny, okY := normalize(t, y)
	return okX && okY && reflect.DeepEqual(nx, ny)
}

func normalize(t catalog.Type, a store.Value) (store.Value, bool) {
	v, err := keys.DecodeValue(t, a, true)
	if err != nil {
		return store.Value{}, false
	}
	n, _, err := keys.EncodeValue(t, v)
	return n, err == nil
}

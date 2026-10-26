// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"fmt"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// Row returns the logical row that items, the stored items of one row of a view version
// (one per SPLIT item, nil where absent), hold: the key values in ORDER BY order, and each
// aggregate's merged state in catalog order. An item without an aggregate's attribute holds
// its empty state, so a row that no aggregate input reached has the "No input rows" values.
// ok is false when no item exists or the row has expired at now.
func (c *Compiled) Row(version string, items []store.Item, now time.Time) ([]expr.Value, []agg.State, bool, error) {
	v, ok := c.views[version]
	if !ok {
		return nil, nil, false, fmt.Errorf("projector: no view version %s in the catalog", version)
	}
	var merged *item
	for _, it := range items {
		if it == nil {
			continue
		}
		r, err := v.decodeItem(it)
		if err != nil {
			return nil, nil, false, err
		}
		if merged == nil {
			merged = r
			continue
		}
		for i, s := range r.states {
			if err = merged.states[i].Merge(s); err != nil {
				return nil, nil, false, err
			}
		}
	}
	if merged == nil {
		return nil, nil, false, nil
	}
	if exp, has, err := v.expiry(merged.keys); err != nil || has && now.UnixMicro() >= exp {
		return nil, nil, false, err
	}
	return merged.keys, merged.states, true, nil
}

// Funcs returns the Func of each aggregate of a view version, in catalog order, which makes
// the states that Row returns; nil for a version not in the catalog.
func (c *Compiled) Funcs(version string) []*agg.Func {
	v, ok := c.views[version]
	if !ok {
		return nil
	}
	fs := make([]*agg.Func, len(v.aggs))
	for i, a := range v.aggs {
		fs[i] = a.fn
	}
	return fs
}

// item is a decoded row item.
type item struct {
	version int64 // _v; 0 for an item that doesn't exist
	hwm     keys.HWM
	keys    []expr.Value
	states  []agg.State
}

// decodeItem decodes a stored row item, rejecting anything Dynaflux doesn't write.
func (v *view) decodeItem(it store.Item) (*item, error) {
	r := &item{}
	known := map[string]bool{catalog.AttrPK: true, catalog.AttrSK: true, catalog.AttrTTL: true}
	fail := func(err error) (*item, error) {
		return nil, fmt.Errorf("view %s: item %q/%q: %w", v.id, it[catalog.AttrPK].Str, it[catalog.AttrSK].Str, err)
	}
	for i, k := range v.def.Keys {
		known[k.Name] = true
		kv, err := decodeAttr(it, k.Name, v.keyTypes[i])
		if err != nil {
			return fail(fmt.Errorf("%s: %w", k.Name, err))
		}
		r.keys = append(r.keys, kv)
	}
	for _, a := range v.aggs {
		known[a.name] = true
		s := a.fn.New()
		if attr, ok := it[a.name]; ok {
			var err error
			if s, err = a.fn.Decode(attr); err != nil {
				return fail(fmt.Errorf("%s: %w", a.name, err))
			}
		}
		r.states = append(r.states, s)
	}
	known[catalog.AttrRowVersion], known[catalog.AttrHighWaterMarks] = true, true
	ver, err := strconv.ParseInt(it[catalog.AttrRowVersion].Str, 10, 64)
	if it[catalog.AttrRowVersion].Type != store.N || err != nil || ver < 1 {
		return fail(fmt.Errorf("%s is not a version", catalog.AttrRowVersion))
	}
	r.version = ver
	hwm, hok := it[catalog.AttrHighWaterMarks]
	if r.hwm, err = keys.DecodeHWM(hwm, hok); err != nil {
		return fail(err)
	}
	for name := range it {
		if !known[name] {
			return fail(fmt.Errorf("unexpected attribute %s", name))
		}
	}
	return r, nil
}

// expiry returns when the row with the key values ks expires, in microseconds, and false
// for a version without TTL.
func (v *view) expiry(ks []expr.Value) (int64, bool, error) {
	if v.ttlKey < 0 {
		return 0, false, nil
	}
	at := ks[v.ttlKey].AsMicros()
	if p := v.def.TTL.Plus; p != nil {
		var err error
		if at, err = expr.AddInterval(at, *p); err != nil {
			return 0, false, fmt.Errorf("view %s: TTL: %w", v.id, err)
		}
	}
	return at, true, nil
}

// encodeItem returns the item to store for g's row: its key, key columns, non-empty states,
// _v, _hwm and, with TTL, _ttl.
func (g *rowGroup) encodeItem(r *item) (store.Item, error) {
	v := g.v
	it := store.Item{
		catalog.AttrPK:         {Type: store.S, Str: g.key.PK},
		catalog.AttrSK:         {Type: store.S, Str: g.key.SK},
		catalog.AttrRowVersion: {Type: store.N, Str: strconv.FormatInt(r.version, 10)},
	}
	for i, kv := range r.keys {
		a, _, err := keys.EncodeValue(v.keyTypes[i], kv)
		if err != nil {
			return nil, fmt.Errorf("key %s: %w", v.def.Keys[i].Name, err)
		}
		it[v.def.Keys[i].Name] = a
	}
	for i, s := range r.states {
		if s.Empty() {
			continue
		}
		a, err := s.Encode()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", v.aggs[i].name, err)
		}
		it[v.aggs[i].name] = a
	}
	hwm, err := r.hwm.Encode()
	if err != nil {
		return nil, err
	}
	it[catalog.AttrHighWaterMarks] = hwm
	if g.hasExp {
		it[catalog.AttrTTL] = keys.TTL(g.exp)
	}
	return it, nil
}

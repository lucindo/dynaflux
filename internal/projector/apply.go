// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// maxAttempts bounds a row's read-modify-write. Few writers share a row item, so a
// condition that keeps failing means the backend disagrees with the _v it returns.
const maxAttempts = 100

// applyRow writes the contributions of g that the item doesn't hold yet, by its high-water
// mark for the unit, in one conditional write. A row that has expired is skipped.
func (c *Core) applyRow(ctx context.Context, unitKey string, g *rowGroup) error {
	v := g.v
	if g.hasExp && time.Now().UnixMicro() >= g.exp {
		return nil
	}
	table := c.naming.ViewTable(v.name, v.def.Number)
	stored, err := c.b.Get(ctx, table, g.key, true)
	if err != nil {
		return fmt.Errorf("projector: read view %s row %q/%q: %w", v.id, g.key.PK, g.key.SK, err)
	}
	for range maxAttempts {
		next, prev, nerr := c.nextItem(ctx, table, unitKey, g, stored)
		if nerr != nil || next == nil {
			return nerr
		}
		cond := store.IfAbsent()
		if prev > 0 {
			cond = store.IfVersion(prev)
		}
		err = c.b.Put(ctx, table, next, cond)
		var ce *store.ConditionError
		if !errors.As(err, &ce) {
			if err != nil {
				return fmt.Errorf("projector: write view %s row %q/%q: %w", v.id, g.key.PK, g.key.SK, err)
			}
			return nil
		}
		stored = ce.Current
	}
	return fmt.Errorf("projector: write view %s row %q/%q: condition failed %d times in a row", v.id, g.key.PK, g.key.SK, maxAttempts)
}

// nextItem returns the item to write over stored, which has version prev (0 when absent),
// or nil when stored already holds every contribution.
func (c *Core) nextItem(ctx context.Context, table, unitKey string, g *rowGroup, stored store.Item) (store.Item, int64, error) {
	cur, err := g.current(stored)
	if err != nil {
		return nil, 0, err
	}
	last := cur.hwm.Units[unitKey]
	pending := slices.DeleteFunc(slices.Clone(g.contribs), func(ct *contribution) bool { return ct.seq <= last })
	if len(pending) == 0 {
		return nil, 0, nil
	}
	counted, err := c.countValues(ctx, table, unitKey, g, pending)
	if err == nil {
		err = cur.add(pending, counted)
	}
	if err != nil {
		return nil, 0, err
	}
	prev := cur.version
	cur.version++
	cur.hwm.Units[unitKey] = pending[len(pending)-1].seq
	next, err := g.encodeItem(cur)
	return next, prev, err
}

// current decodes the stored item, or returns an empty one when there is none.
func (g *rowGroup) current(stored store.Item) (*item, error) {
	if stored != nil {
		return g.v.decodeItem(stored)
	}
	cur := &item{hwm: keys.HWM{Units: map[string]string{}}, keys: g.keys}
	for _, a := range g.v.aggs {
		cur.states = append(cur.states, a.fn.New())
	}
	return cur, nil
}

// add merges the states of the pending records and their counted uniqExact values.
func (r *item) add(pending []*contribution, counted [][]expr.Value) error {
	for _, ct := range pending {
		for i, s := range ct.states {
			if s == nil {
				continue
			}
			if err := r.states[i].Merge(s); err != nil {
				return err
			}
		}
	}
	for i, vals := range counted {
		for _, val := range vals {
			if err := r.states[i].Add(agg.Input{Args: []expr.Value{val}}); err != nil {
				return err
			}
		}
	}
	return nil
}

// marked is a pending record's uniqExact value with the key of its marker.
type marked struct {
	ct  *contribution
	agg int
	val expr.Value
	key store.Key
}

// marked lists the pending records' uniqExact values, in sequence order.
func (g *rowGroup) marked(pending []*contribution) []marked {
	var out []marked
	for _, ct := range pending {
		for i, vals := range ct.values {
			for _, uv := range vals {
				k := keys.MarkerKey(g.key.PK, g.lsk, g.v.aggs[i].name, []byte(uv.canon))
				out = append(out, marked{ct: ct, agg: i, val: uv.value, key: k})
			}
		}
	}
	return out
}

// countValues returns, by aggregate, the uniqExact values that the pending records own. The
// first record that has a value creates its marker unless one exists, and a value counts
// for the record that owns the marker, including a marker the record wrote in an earlier
// attempt.
func (c *Core) countValues(ctx context.Context, table, unitKey string, g *rowGroup, pending []*contribution) ([][]expr.Value, error) {
	ms := g.marked(pending)
	if err := c.readMarkers(ctx, table, g, ms); err != nil {
		return nil, err
	}
	counted := make([][]expr.Value, len(g.v.aggs))
	for _, m := range ms {
		owner, err := keys.Owner(keys.OwnerLive, unitKey, m.ct.seq)
		if err != nil {
			return nil, err
		}
		o, ok := g.owners[m.key]
		if !ok {
			if o, err = c.putMarker(ctx, table, g, m.key, owner); err != nil {
				return nil, err
			}
			g.owners[m.key] = o
		}
		if o == owner {
			counted[m.agg] = append(counted[m.agg], m.val)
		}
	}
	return counted, nil
}

// readMarkers caches the owners of the stored markers of ms.
func (c *Core) readMarkers(ctx context.Context, table string, g *rowGroup, ms []marked) error {
	var ks []store.Key
	asked := map[store.Key]bool{}
	for _, m := range ms {
		if _, ok := g.owners[m.key]; !ok && !asked[m.key] {
			asked[m.key] = true
			ks = append(ks, m.key)
		}
	}
	for chunk := range slices.Chunk(ks, store.MaxBatchGetKeys) {
		its, err := c.b.BatchGet(ctx, table, chunk, true)
		if err != nil {
			return fmt.Errorf("projector: read view %s markers: %w", g.v.id, err)
		}
		for i, it := range its {
			if it == nil {
				continue
			}
			o, oerr := markerOwner(it)
			if oerr != nil {
				return fmt.Errorf("projector: view %s: %w", g.v.id, oerr)
			}
			g.owners[chunk[i]] = o
		}
	}
	return nil
}

// putMarker creates the marker k owned by owner unless one exists, and returns the stored
// owner.
func (c *Core) putMarker(ctx context.Context, table string, g *rowGroup, k store.Key, owner string) (string, error) {
	it := store.Item{
		catalog.AttrPK:          {Type: store.S, Str: k.PK},
		catalog.AttrSK:          {Type: store.S, Str: k.SK},
		catalog.AttrMarkerOwner: {Type: store.S, Str: owner},
	}
	if g.hasExp {
		it[catalog.AttrTTL] = keys.TTL(g.exp)
	}
	err := c.b.Put(ctx, table, it, store.IfAbsent())
	var ce *store.ConditionError
	switch {
	case errors.As(err, &ce):
		o, oerr := markerOwner(ce.Current)
		if oerr != nil {
			return "", fmt.Errorf("projector: view %s: %w", g.v.id, oerr)
		}
		return o, nil
	case err != nil:
		return "", fmt.Errorf("projector: write view %s marker %q: %w", g.v.id, k.PK, err)
	}
	return owner, nil
}

func markerOwner(it store.Item) (string, error) {
	o, ok := it[catalog.AttrMarkerOwner]
	if !ok || o.Type != store.S {
		return "", fmt.Errorf("marker %q has no owner", it[catalog.AttrPK].Str)
	}
	return o.Str, nil
}

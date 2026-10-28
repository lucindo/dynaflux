// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
)

// maxAttempts bounds a row's read-modify-write. Few writers share a row item, so a
// condition that keeps failing means the backend disagrees with the _v it returns.
const maxAttempts = 100

// A row write that meets a transaction (store.ErrConflict) reads the row again and retries
// up to conflictRetries times, each after a wait from conflictWait to 10 times as long.
const (
	conflictRetries = 4
	conflictWait    = 5 * time.Millisecond
)

// applyRows writes the rows of unit own's records, "" for the backfill's, parallel at a
// time, and returns each one's error. Every write has finished when it returns. It
// reads every row before it writes any: pruning reads the items of the other units whose
// high-water marks the rows hold, once for all of them. A row that has expired is skipped.
func (c *Core) applyRows(ctx context.Context, stream, own string, groups []*rowGroup, parallel int) []error {
	errs := make([]error, len(groups))
	items := make([]*item, len(groups))
	now := time.Now().UnixMicro()
	fanOut(len(groups), parallel, func(i int) {
		if g := groups[i]; !g.hasExp || now < g.exp {
			items[i], errs[i] = c.readRow(ctx, g)
		}
	})
	units, err := c.units(ctx, stream, own, items)
	// The items of a SPLIT row share its uniqExact markers, so they are written in turn: two
	// identical marker requests in flight would race.
	rows := logicalRows(groups)
	fanOut(len(rows), parallel, func(r int) {
		for _, i := range rows[r] {
			switch {
			case items[i] == nil: // expired, or not read
			case err != nil:
				errs[i] = err
			default:
				errs[i] = c.writeRow(ctx, groups[i], items[i], units)
			}
		}
	})
	return errs
}

// fanOut calls f for each index below n, parallel at a time, and returns once every call
// has.
func fanOut(n, parallel int, f func(int)) {
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for i := range n {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			f(i)
		})
	}
	wg.Wait()
}

// logicalRows returns the indexes of groups by logical row: a SPLIT row's items together.
func logicalRows(groups []*rowGroup) [][]int {
	type row struct {
		v       *view
		pk, lsk string
	}
	at := map[row]int{}
	var out [][]int
	for i, g := range groups {
		k := row{g.v, g.key.PK, g.lsk}
		j, ok := at[k]
		if !ok {
			j = len(out)
			at[k] = j
			out = append(out, nil)
		}
		out[j] = append(out[j], i)
	}
	return out
}

func (c *Core) readRow(ctx context.Context, g *rowGroup) (*item, error) {
	stored, err := c.b.Get(ctx, c.naming.ViewTable(g.v.name, g.v.def.Number), g.key, true)
	if err != nil {
		return nil, fmt.Errorf("projector: read view %s row %q/%q: %w", g.v.id, g.key.PK, g.key.SK, err)
	}
	return g.current(stored)
}

// units reads, by unit key, the items of the units other than own whose high-water marks
// items hold.
func (c *Core) units(ctx context.Context, stream, own string, items []*item) (map[string]meta.Unit, error) {
	var ks []string
	for _, it := range items {
		if it != nil {
			ks = slices.AppendSeq(ks, maps.Keys(it.hwm.Units))
		}
	}
	slices.Sort(ks)
	ks = slices.DeleteFunc(slices.Compact(ks), func(k string) bool { return k == own })
	units, err := c.meta.Units(ctx, stream, ks, false)
	if err != nil {
		return nil, fmt.Errorf("projector: %w", err)
	}
	return units, nil
}

// writeRow writes the contributions of g that cur, its stored item, doesn't hold yet. A write
// that meets a transaction may have been applied, so the row is read again before a retry.
// Retrying in place keeps a hot row from failing the batch in the region whose appends
// write SYNC rows too (ADR 0014 §7).
func (c *Core) writeRow(ctx context.Context, g *rowGroup, cur *item, units map[string]meta.Unit) error {
	for n := 1; ; n++ {
		err := c.putRow(ctx, g, cur, units)
		if !errors.Is(err, store.ErrConflict) || n > conflictRetries {
			return err
		}
		t := time.NewTimer(g.conflictWait(n))
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
		if cur, err = c.readRow(ctx, g); err != nil {
			return err
		}
	}
}

// conflictWait returns the wait before g's retry n after a conflict. It is a hash of the row
// item, the first record and n, which spreads the writers that met one transaction and
// keeps a simulation run replayable.
func (g *rowGroup) conflictWait(n int) time.Duration {
	x := fmt.Sprintf("%s\x00%s\x00%s\x00%d", g.key.PK, g.key.SK, g.contribs[0].seq, n)
	spread := int(9 * conflictWait / time.Microsecond)
	return conflictWait + time.Duration(keys.Bucket(x, spread+1))*time.Microsecond
}

// putRow writes the contributions of g that cur, its stored item, doesn't hold yet, by its
// high-water marks, in one conditional write.
func (c *Core) putRow(ctx context.Context, g *rowGroup, cur *item, units map[string]meta.Unit) error {
	v := g.v
	table := c.naming.ViewTable(v.name, v.def.Number)
	for range maxAttempts {
		next, prev, err := c.nextItem(ctx, table, g, cur, units)
		if err != nil || next == nil {
			return err
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
		if cur, err = g.current(ce.Current); err != nil {
			return err
		}
	}
	return fmt.Errorf("projector: write view %s row %q/%q: condition failed %d times in a row", v.id, g.key.PK, g.key.SK, maxAttempts)
}

// nextItem returns the item to write over cur, which has version prev (0 when absent), or
// nil when cur already holds every contribution.
func (c *Core) nextItem(ctx context.Context, table string, g *rowGroup, cur *item, units map[string]meta.Unit) (store.Item, int64, error) {
	pending := g.pending(&cur.hwm)
	if len(pending) == 0 {
		return nil, 0, nil
	}
	c.prune(&cur.hwm, units)
	counted, err := c.countValues(ctx, table, g, pending)
	if err == nil {
		err = cur.add(pending, counted)
	}
	if err != nil {
		return nil, 0, err
	}
	prev := cur.version
	cur.version++
	next, err := g.encodeItem(cur)
	return next, prev, err
}

// pending returns the contributions that h doesn't hold yet, and moves h past them. Live, a
// record counts above its unit's entry. The backfill applies records in one order, by unit
// key and sequence number, so a record counts above the mark B of the last one applied.
func (g *rowGroup) pending(h *keys.HWM) []*contribution {
	if g.owner == keys.OwnerBackfill {
		var last keys.Mark
		if h.Backfill != nil {
			last = *h.Backfill
		}
		out := slices.DeleteFunc(slices.Clone(g.contribs), func(ct *contribution) bool { return ct.mark().Compare(last) <= 0 })
		if len(out) > 0 {
			m := out[len(out)-1].mark()
			h.Backfill = &m
		}
		return out
	}
	out := slices.DeleteFunc(slices.Clone(g.contribs), func(ct *contribution) bool {
		return ct.seq <= h.Units[ct.unitKey]
	})
	for _, ct := range out {
		h.Units[ct.unitKey] = ct.seq
	}
	return out
}

// prune drops each unit's mark that the unit's progress covers, once progress rose more than
// W ago (S9 rule 3): an invocation that writes the row later started after the rise, so it
// skips every record the mark stood for. The backfill's mark B stays.
func (c *Core) prune(h *keys.HWM, units map[string]meta.Unit) {
	for key, seq := range h.Units {
		if u := units[key]; u.Progress >= seq && time.Since(u.ProgressAt) > c.window {
			delete(h.Units, key)
		}
	}
}

func (ct *contribution) mark() keys.Mark { return keys.Mark{UnitKey: ct.unitKey, Seq: ct.seq} }

func (r *record) mark() keys.Mark { return keys.Mark{UnitKey: r.unitKey, Seq: r.seq} }

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
func (c *Core) countValues(ctx context.Context, table string, g *rowGroup, pending []*contribution) ([][]expr.Value, error) {
	ms := g.marked(pending)
	if err := c.readMarkers(ctx, table, g, ms); err != nil {
		return nil, err
	}
	counted := make([][]expr.Value, len(g.v.aggs))
	for _, m := range ms {
		owner, err := keys.Owner(g.owner, m.ct.unitKey, m.ct.seq)
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
	err := c.b.Put(ctx, table, g.marker(k, owner), store.IfAbsent())
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

// marker returns g's marker item k, owned by owner, which expires with the row.
func (g *rowGroup) marker(k store.Key, owner string) store.Item {
	it := store.Item{
		catalog.AttrPK:          {Type: store.S, Str: k.PK},
		catalog.AttrSK:          {Type: store.S, Str: k.SK},
		catalog.AttrMarkerOwner: {Type: store.S, Str: owner},
	}
	if g.hasExp {
		it[catalog.AttrTTL] = keys.TTL(g.exp)
	}
	return it
}

func markerOwner(it store.Item) (string, error) {
	o, ok := it[catalog.AttrMarkerOwner]
	if !ok || o.Type != store.S {
		return "", fmt.Errorf("marker %q has no owner", it[catalog.AttrPK].Str)
	}
	return o.Str, nil
}

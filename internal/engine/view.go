// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"slices"
	"time"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// ReadRow returns the logical row of view version (such as page_daily@v2) whose key
// values, in ORDER BY order, are key: the key values, then each aggregate's finalized value
// in catalog order. ok is false when the row doesn't exist or has expired. The items of a
// SPLIT row are one Query.
func (e *Engine) ReadRow(ctx context.Context, cat *Catalog, version string, key []expr.Value) ([]expr.Value, bool, error) {
	name, vv, ok := cat.p.Version(version)
	if !ok {
		return nil, false, fmt.Errorf("dynaflux: no view version %s in the catalog", version)
	}
	if len(key) != len(vv.Keys) {
		return nil, false, fmt.Errorf("dynaflux: %d values for the %d keys of view %s", len(key), len(vv.Keys), version)
	}
	for i, k := range vv.Keys {
		if _, _, err := encode(k.Name, k.Expr.Type, key[i]); err != nil {
			return nil, false, err
		}
	}
	sq, ok, err := rowQuery(vv, key)
	if err != nil || !ok {
		return nil, false, err
	}
	table := e.naming(cat).ViewTable(name, vv.Number)
	var items []store.Item
	if vv.Split == 1 {
		it, gerr := e.b.Get(ctx, table, store.Key{PK: sq.PK, SK: sq.From}, false)
		if gerr != nil {
			return nil, false, fmt.Errorf("dynaflux: read view %s: %w", version, gerr)
		}
		items = append(items, it)
	} else {
		for it, qerr := range e.b.Query(ctx, table, sq) {
			if qerr != nil {
				return nil, false, fmt.Errorf("dynaflux: read view %s: %w", version, qerr)
			}
			items = append(items, it)
		}
	}
	ks, states, ok, err := cat.p.Row(version, items, time.Now())
	if err != nil {
		return nil, false, fmt.Errorf("dynaflux: %w", err)
	}
	if !ok {
		return nil, false, nil
	}
	all := make([]int, len(states))
	for i := range all {
		all[i] = i
	}
	row, err := finalize(version, vv, ks, states, all)
	if err != nil {
		return nil, false, fmt.Errorf("dynaflux: %w", err)
	}
	return row, true, nil
}

// finalize returns the columns of a row of view version id from its key values ks and
// merged states: the value of each aggregate in finals, and NULL for the others.
func finalize(id string, vv *catalog.ViewVersion, ks []expr.Value, states []agg.State, finals []int) ([]expr.Value, error) {
	row := make([]expr.Value, len(ks)+len(states))
	copy(row, ks)
	for _, i := range finals {
		v, err := states[i].Final()
		if err != nil {
			return nil, fmt.Errorf("view %s column %s: %w", id, vv.Aggregates[i].Name, err)
		}
		row[len(ks)+i] = v
	}
	return row, nil
}

// rowQuery returns the read of every item of the logical row with key values kvs: a
// Query whose From is the item's key for a view without SPLIT. It returns false when the
// key is too long for any row to have it.
func rowQuery(vv *catalog.ViewVersion, kvs []expr.Value) (store.Query, bool, error) {
	types := keyTypes(vv)
	n := vv.PartitionKeyLen
	pk, err := keys.Key(types[:n], kvs[:n])
	if err != nil {
		return store.Query{}, false, err
	}
	lsk := ""
	if len(kvs) > n {
		if lsk, err = keys.Key(types[n:], kvs[n:]); err != nil {
			return store.Query{}, false, err
		}
	}
	q := store.Query{PK: pk, From: cmp.Or(lsk, keys.NoSortKey)}
	if vv.Split > 1 {
		q.From, q.To = keys.SplitKey(lsk, 0), keys.SplitKey(lsk, vv.Split-1)
	}
	return q, len(pk) <= store.MaxPKLen && len(q.From) <= store.MaxSKLen && len(q.To) <= store.MaxSKLen, nil
}

func keyTypes(vv *catalog.ViewVersion) []catalog.Type {
	ts := make([]catalog.Type, len(vv.Keys))
	for i, k := range vv.Keys {
		ts[i] = k.Expr.Type
	}
	return ts
}

// source is a row that a query reads: its column values and, for a view row that a -Merge
// reads, each aggregate's merged state.
type source struct {
	vals   []expr.Value
	states []agg.State
}

// viewRow returns the row that items, those of one row of the query's view, hold; false
// when there is none.
func (q *query) viewRow(cat *Catalog, items []store.Item, now time.Time) (source, bool, error) {
	ks, states, ok, err := cat.p.Row(q.viewID, items, now)
	if err != nil || !ok {
		return source{}, false, err
	}
	vals, err := finalize(q.viewID, q.view, ks, states, q.finals)
	if !q.merges {
		states = nil
	}
	return source{vals: vals, states: states}, err == nil, err
}

// readRows passes the rows of every key combination to keep, which reports whether it kept
// the row. With perCombo > 0, the read of a combination stops after perCombo kept rows.
func (e *Engine) readRows(ctx context.Context, cat *Catalog, q *query, r *read, perCombo int, keep func(source) (bool, error)) error {
	if q.view != nil && q.view.Split == 1 && len(q.bound) == len(q.view.Keys) {
		return e.pointRows(ctx, cat, q, r, keep)
	}
	for _, combo := range r.combos {
		kept := 0
		for row, err := range e.comboRows(ctx, cat, q, r, combo, perCombo) {
			ok := false
			if err == nil {
				ok, err = keep(row)
			}
			if err != nil {
				return err
			}
			if ok {
				kept++
			}
			if perCombo > 0 && kept == perCombo {
				break
			}
		}
	}
	return nil
}

// pointRows reads the rows of a view without SPLIT whose every key column is bound, in one
// BatchGet per 100 rows.
func (e *Engine) pointRows(ctx context.Context, cat *Catalog, q *query, r *read, keep func(source) (bool, error)) error {
	var ks []store.Key
	for _, combo := range r.combos {
		sq, ok, err := rowQuery(q.view, combo)
		if err != nil {
			return err
		}
		if ok {
			ks = append(ks, store.Key{PK: sq.PK, SK: sq.From})
		}
	}
	now := time.Now()
	table := e.naming(cat).ViewTable(q.def.Source.Name, q.view.Number)
	for chunk := range slices.Chunk(ks, store.MaxBatchGetKeys) {
		its, err := e.b.BatchGet(ctx, table, chunk, false)
		if err != nil {
			return err
		}
		for _, it := range its {
			src, ok, rerr := q.viewRow(cat, []store.Item{it}, now)
			if rerr == nil && ok {
				_, rerr = keep(src)
			}
			if rerr != nil {
				return rerr
			}
		}
	}
	return nil
}

// comboRows yields the rows of one key combination in the read's key order.
func (e *Engine) comboRows(ctx context.Context, cat *Catalog, q *query, r *read, combo []expr.Value, perCombo int) iter.Seq2[source, error] {
	if q.stream != nil {
		from, to := r.eventRange()
		return func(yield func(source, error) bool) {
			for row, err := range e.events(ctx, cat, q.stream, combo, from, to, q.desc) {
				if !yield(source{vals: row}, err) {
					return
				}
			}
		}
	}
	sq, ok, err := q.viewQuery(r, combo)
	if err != nil || !ok {
		return func(yield func(source, error) bool) {
			if err != nil {
				yield(source{}, err)
			}
		}
	}
	if perCombo > 0 {
		// One item more than the rows ends the last row, or is the item an exclusive bound
		// may let in.
		sq.PageSize = perCombo*q.view.Split + 1
	}
	return e.viewRows(ctx, cat, q, sq)
}

// viewQuery returns the Query of one combination of the bound key values, false when no
// row can match.
func (q *query) viewQuery(r *read, combo []expr.Value) (store.Query, bool, error) {
	vv := q.view
	if len(combo) == len(vv.Keys) {
		return rowQuery(vv, combo)
	}
	types := keyTypes(vv)
	n := vv.PartitionKeyLen
	pk, err := keys.Key(types[:n], combo[:n])
	if err != nil || len(pk) > store.MaxPKLen {
		return store.Query{}, false, err
	}
	prefix := ""
	if len(combo) > n {
		if prefix, err = keys.Key(types[n:len(combo)], combo[n:]); err != nil {
			return store.Query{}, false, err
		}
	}
	rg, err := keys.Range(prefix, types[len(combo)], r.lo, r.hi)
	if err != nil || rg.Empty {
		return store.Query{}, false, err
	}
	return store.Query{PK: pk, From: rg.From, To: rg.To, ToExclusive: rg.ToExclusive, Desc: q.desc}, true, nil
}

// viewRows yields the logical rows of a view Query's items.
func (e *Engine) viewRows(ctx context.Context, cat *Catalog, q *query, sq store.Query) iter.Seq2[source, error] {
	table := e.naming(cat).ViewTable(q.def.Source.Name, q.view.Number)
	return func(yield func(source, error) bool) {
		now := time.Now()
		for items, err := range rowItems(e.b.Query(ctx, table, sq), q.view.Split > 1) {
			var src source
			ok := false
			if err == nil {
				src, ok, err = q.viewRow(cat, items, now)
			}
			if err != nil {
				yield(source{}, err)
				return
			}
			if ok && !yield(src, nil) {
				return
			}
		}
	}
}

// rowItems groups the items of a view Query by logical row. The items of a SPLIT row are
// adjacent in key order, since a row's sort fields are a prefix of its items' keys.
func rowItems(items iter.Seq2[store.Item, error], split bool) iter.Seq2[[]store.Item, error] {
	return func(yield func([]store.Item, error) bool) {
		var group []store.Item
		lsk := ""
		for it, err := range items {
			if err != nil {
				yield(nil, err)
				return
			}
			k := it[catalog.AttrSK].Str
			if split {
				k = keys.SplitPrefix(k)
			}
			if len(group) > 0 && k != lsk {
				if !yield(group, nil) {
					return
				}
				group = nil
			}
			lsk, group = k, append(group, it)
		}
		if len(group) > 0 {
			yield(group, nil)
		}
	}
}

// eventRange returns the event_time range of a stream read, [from, to) in microseconds;
// nil for an open end.
func (r *read) eventRange() (*int64, *int64) {
	var from, to *int64
	if r.lo != nil {
		f := r.lo.Value.AsMicros()
		if !r.lo.Inclusive {
			f++
		}
		from = &f
	}
	if r.hi != nil {
		t := r.hi.Value.AsMicros()
		if r.hi.Inclusive {
			t++
		}
		to = &t
	}
	return from, to
}

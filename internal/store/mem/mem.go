// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package mem is an in-memory [store.Backend] with the DynamoDB behavior Dynaflux depends
// on: sort keys in byte order, conditions that return the stored item, size limits,
// numbers stored in DynamoDB's form, and the requests DynamoDB rejects. Reads are always
// consistent.
package mem

import (
	"cmp"
	"context"
	"fmt"
	"hash/fnv"
	"iter"
	"math/bits"
	"slices"
	"strings"
	"sync"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

// pageBytes is DynamoDB's cap on what one Query request reads.
const pageBytes = 1 << 20

// Mem is a [store.Backend]; it is safe for concurrent use. Every table exists, empty
// until written.
type Mem struct {
	mu     sync.Mutex
	tables map[string]map[string]*partition // table, then pk
	// scans holds the snapshot that the latest scan of each segment took at its first page.
	// A scan resumes from it, and so does a stale worker that resumes from an older key.
	scans map[scanSegment][]store.Item
}

type scanSegment struct {
	table             string
	segment, segments int
}

type partition struct {
	sks   []string // sorted by bytes
	items map[string]store.Item
}

// New returns an empty store. Put stores each number in DynamoDB's form, without leading
// zeros or trailing fraction zeros, so readers that rescale are exercised in every test.
func New() *Mem {
	return &Mem{tables: make(map[string]map[string]*partition), scans: make(map[scanSegment][]store.Item)}
}

// Get implements [store.Backend].
func (m *Mem) Get(ctx context.Context, table string, k store.Key, _ bool) (store.Item, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("mem: get: %w", err)
	}
	if err := store.CheckKey(k); err != nil {
		return nil, fmt.Errorf("mem: get: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneItem(m.lookup(table, k)), nil
}

// BatchGet implements [store.Backend].
func (m *Mem) BatchGet(ctx context.Context, table string, ks []store.Key, _ bool) ([]store.Item, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("mem: batch get: %w", err)
	}
	if err := store.CheckKeys(ks); err != nil {
		return nil, fmt.Errorf("mem: batch get: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]store.Item, len(ks))
	for i, k := range ks {
		out[i] = cloneItem(m.lookup(table, k))
	}
	return out, nil
}

// Query implements [store.Backend]. Each page is read under the lock and yielded without
// it, so the caller may write while it iterates and sees writes made between pages.
func (m *Mem) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	return func(yield func(store.Item, error) bool) {
		if err := store.CheckQuery(q); err != nil {
			yield(nil, fmt.Errorf("mem: query: %w", err))
			return
		}
		c := cursor{size: q.PageSize, left: q.Limit}
		for !c.done {
			if err := ctx.Err(); err != nil {
				yield(nil, fmt.Errorf("mem: query: %w", err))
				return
			}
			for _, it := range m.nextPage(table, q, &c) {
				if !yield(it, nil) {
					return
				}
			}
		}
	}
}

// cursor is where a Query is between pages.
type cursor struct {
	after string // sk of the last item read; "" before the first page
	size  int    // item limit of the next page; 0 for none
	left  int    // items left under the Query's Limit
	done  bool
}

// nextPage reads the page after c.after and advances c.
func (m *Mem) nextPage(table string, q store.Query, c *cursor) []store.Item {
	limit := c.size
	if q.Limit > 0 && (limit == 0 || c.left < limit) {
		limit = c.left
	}
	page, more := m.page(table, q, c.after, limit)
	c.left -= len(page)
	c.done = !more || q.Limit > 0 && c.left == 0
	if !c.done {
		c.after = page[len(page)-1][catalog.AttrSK].Str
	}
	if c.size > 0 && c.size <= pageBytes {
		// Past pageBytes items, pages are bounded by bytes, and doubling could overflow.
		c.size += c.size
	}
	return page
}

// ScanPage implements [store.Backend]. A scan's first page snapshots its segment, and a page
// that continues from a key reads the segment's latest snapshot after that key, so no write
// made after that snapshot appears: the most a DynamoDB scan may miss. Segments split the
// range of a hash of pk, and items come in the order of that hash, not of their keys.
func (m *Mem) ScanPage(ctx context.Context, table string, s store.Scan, start *store.Key) ([]store.Item, *store.Key, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("mem: scan: %w", err)
	}
	if err := store.CheckScan(s, start); err != nil {
		return nil, nil, fmt.Errorf("mem: scan: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	seg := scanSegment{table, s.Segment, s.Segments}
	rest, ok := m.scans[seg]
	if start == nil || !ok {
		rest = m.snapshot(table, s)
		m.scans[seg] = rest
	}
	if start != nil {
		from := scanPos{hashPK(start.PK), start.PK, start.SK}
		i, found := slices.BinarySearchFunc(rest, from, func(it store.Item, p scanPos) int { return positionOf(it).compare(p) })
		if found {
			i++
		}
		rest = rest[i:]
	}
	page, more := firstPage(rest, s.PageSize)
	out := make([]store.Item, len(page))
	for i, it := range page {
		out[i] = cloneItem(it)
	}
	if !more {
		return out, nil, nil
	}
	last := page[len(page)-1]
	next := store.KeyOf(last)
	return out, &next, nil
}

// scanPos is an item's place in scan order.
type scanPos struct {
	hash   uint64
	pk, sk string
}

func positionOf(it store.Item) scanPos {
	pk := it[catalog.AttrPK].Str
	return scanPos{hashPK(pk), pk, it[catalog.AttrSK].Str}
}

func (a scanPos) compare(b scanPos) int {
	return cmp.Or(cmp.Compare(a.hash, b.hash), strings.Compare(a.pk, b.pk), strings.Compare(a.sk, b.sk))
}

func hashPK(pk string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(pk)) //nolint:gosec // a hash.Hash never fails to write
	return h.Sum64()
}

// snapshot returns the items of s's segment in scan order. Stored items are never changed
// in place, so it shares them.
func (m *Mem) snapshot(table string, s store.Scan) []store.Item {
	var pos []scanPos
	for pk, p := range m.tables[table] {
		h := hashPK(pk)
		if seg, _ := bits.Mul64(h, uint64(s.Segments)); seg != uint64(s.Segment) { //nolint:gosec // CheckScan made both positive
			continue
		}
		for _, sk := range p.sks {
			pos = append(pos, scanPos{h, pk, sk})
		}
	}
	slices.SortFunc(pos, scanPos.compare)
	items := make([]store.Item, len(pos))
	for i, at := range pos {
		items[i] = m.tables[table][at.pk].items[at.sk]
	}
	return items
}

// firstPage returns the first page of items: up to limit items (0 for none) and pageBytes.
// more reports whether the page stopped at a limit.
func firstPage(items []store.Item, limit int) ([]store.Item, bool) {
	read := 0
	for n, it := range items {
		read += store.ItemSize(it)
		if n+1 == limit || read >= pageBytes {
			return items[:n+1], true
		}
	}
	return items, false
}

// Put implements [store.Backend].
func (m *Mem) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mem: put: %w", err)
	}
	stored, err := store.Stored(it, c)
	if err != nil {
		return fmt.Errorf("mem: put: %w", err)
	}
	k := store.KeyOf(stored)
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur := m.lookup(table, k); !c.Holds(cur) {
		return &store.ConditionError{Current: cloneItem(cur)}
	}
	m.insert(table, k, stored)
	return nil
}

// Transact implements [store.Backend]: it checks every condition, then writes every put, all
// under the lock, and reports every condition that failed.
func (m *Mem) Transact(ctx context.Context, puts []store.TxPut) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mem: transact: %w", err)
	}
	stored, err := store.StoredTransact(puts)
	if err != nil {
		return fmt.Errorf("mem: transact: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var failed *store.TxError
	for i, p := range puts {
		if cur := m.lookup(p.Table, store.KeyOf(stored[i])); !p.Cond.Holds(cur) {
			if failed == nil {
				failed = &store.TxError{Conds: make([]*store.ConditionError, len(puts))}
			}
			failed.Conds[i] = &store.ConditionError{Current: cloneItem(cur)}
		}
	}
	if failed != nil {
		return failed
	}
	for i, p := range puts {
		m.insert(p.Table, store.KeyOf(stored[i]), stored[i])
	}
	return nil
}

// Delete removes the item with key k, if there is one. It isn't part of [store.Backend]:
// only the simulation's TTL deletes use it.
func (m *Mem) Delete(ctx context.Context, table string, k store.Key) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mem: delete: %w", err)
	}
	if err := store.CheckKey(k); err != nil {
		return fmt.Errorf("mem: delete: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.tables[table][k.PK]
	if p == nil {
		return nil
	}
	if i, found := slices.BinarySearch(p.sks, k.SK); found {
		p.sks = slices.Delete(p.sks, i, i+1)
		delete(p.items, k.SK)
	}
	return nil
}

func (m *Mem) lookup(table string, k store.Key) store.Item {
	p := m.tables[table][k.PK]
	if p == nil {
		return nil
	}
	return p.items[k.SK]
}

func (m *Mem) insert(table string, k store.Key, it store.Item) {
	parts := m.tables[table]
	if parts == nil {
		parts = make(map[string]*partition)
		m.tables[table] = parts
	}
	p := parts[k.PK]
	if p == nil {
		p = &partition{items: make(map[string]store.Item)}
		parts[k.PK] = p
	}
	if i, found := slices.BinarySearch(p.sks, k.SK); !found {
		p.sks = slices.Insert(p.sks, i, k.SK)
	}
	p.items[k.SK] = it
}

// page reads the next page after the sort key after ("" for the first page), up to limit
// items (0 for none) and pageBytes. more reports whether the page stopped at a limit.
func (m *Mem) page(table string, q store.Query, after string, limit int) ([]store.Item, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.tables[table][q.PK]
	if p == nil {
		return nil, false
	}
	lo, hi := p.span(q, after)
	var out []store.Item
	read := 0
	for n := range hi - lo {
		i := lo + n
		if q.Desc {
			i = hi - 1 - n
		}
		it := p.items[p.sks[i]]
		out = append(out, cloneItem(it))
		read += store.ItemSize(it)
		if len(out) == limit || read >= pageBytes {
			return out, true
		}
	}
	return out, false
}

// span returns the index range [lo, hi) of the sort keys within q's bounds and past after.
func (p *partition) span(q store.Query, after string) (int, int) {
	lo, hi := 0, len(p.sks)
	if q.From != "" {
		lo = firstAtLeast(p.sks, q.From)
	}
	if q.To != "" {
		if q.ToExclusive {
			hi = firstAtLeast(p.sks, q.To)
		} else {
			hi = firstAbove(p.sks, q.To)
		}
	}
	if after != "" {
		if q.Desc {
			hi = min(hi, firstAtLeast(p.sks, after))
		} else {
			lo = max(lo, firstAbove(p.sks, after))
		}
	}
	return lo, hi
}

func firstAtLeast(sks []string, x string) int {
	i, _ := slices.BinarySearch(sks, x)
	return i
}

func firstAbove(sks []string, x string) int {
	i, found := slices.BinarySearch(sks, x)
	if found {
		i++
	}
	return i
}

func cloneItem(it store.Item) store.Item {
	if it == nil {
		return nil
	}
	out := make(store.Item, len(it))
	for name, v := range it {
		out[name] = cloneValue(v)
	}
	return out
}

// cloneValue copies a stored value, whose unused fields are already empty.
func cloneValue(v store.Value) store.Value {
	switch v.Type {
	case store.B:
		v.Bin = append([]byte{}, v.Bin...)
	case store.L:
		list := make([]store.Value, len(v.List))
		for i, e := range v.List {
			list[i] = cloneValue(e)
		}
		v.List = list
	case store.M:
		m := make(map[string]store.Value, len(v.Map))
		for name, e := range v.Map {
			m[name] = cloneValue(e)
		}
		v.Map = m
	case store.S, store.N, store.BOOL:
	}
	return v
}

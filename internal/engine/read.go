// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

const (
	// firstPage is the item limit of each shard's first Query page, so a read that stops
	// early reads little; later pages double.
	firstPage = 32
)

// Read yields the events of stream whose PRIMARY KEY values are key and whose event_time
// is in [from, to), in (event_time, event_id) order; a nil bound is open. Each event holds
// the declared columns' values in catalog order, then event_id, event_time, ingest_time and
// origin_region. Events are left out from their expiry, rounded up to a whole second.
//
// Read yields at most one error and then stops. Each range reads again, and breaking out
// stops the reads of every shard.
func (e *Engine) Read(ctx context.Context, cat *Catalog, stream string, key []expr.Value, from, to *int64) iter.Seq2[[]expr.Value, error] {
	return func(yield func([]expr.Value, error) bool) {
		s, err := cat.stream(stream)
		if err == nil {
			err = s.checkKey(key)
		}
		if err != nil {
			yield(nil, err)
			return
		}
		for row, rerr := range e.events(ctx, cat, s, key, from, to, false, false) {
			if rerr != nil {
				yield(nil, fmt.Errorf("dynaflux: read %s: %w", stream, rerr))
				return
			}
			if !yield(row, nil) {
				return
			}
		}
	}
}

// events yields the live events of s whose PRIMARY KEY values are key and whose event_time
// is in [from, to), merged from every shard in (event_time, event_id) order, or in the
// reverse order with desc. consistent makes the reads strongly consistent.
func (e *Engine) events(ctx context.Context, cat *Catalog, s *stream, key []expr.Value, from, to *int64, desc, consistent bool) iter.Seq2[[]expr.Value, error] {
	return func(yield func([]expr.Value, error) bool) {
		pk, err := s.lookupKey(key)
		lo, hi, ok := "", "", false
		if err == nil {
			lo, hi, ok, err = skRange(from, to)
		}
		if err != nil {
			yield(nil, err)
			return
		}
		if pk == "" || !ok {
			return
		}
		now := time.Now().UnixMicro()
		q := store.Query{PK: pk, From: lo, To: hi, Desc: desc, PageSize: firstPage, Consistent: consistent}
		for it, qerr := range merge(e.shards(ctx, e.table(cat, s), s, q), desc) {
			if qerr != nil {
				yield(nil, qerr)
				return
			}
			row, live, derr := s.decode(it, now)
			if derr != nil {
				yield(nil, fmt.Errorf("item %q/%q: %w", it[catalog.AttrPK].Str, it[catalog.AttrSK].Str, derr))
				return
			}
			if live && !yield(row, nil) {
				return
			}
		}
	}
}

// checkKey checks the PRIMARY KEY values of a read.
func (s *stream) checkKey(key []expr.Value) error {
	if len(key) != len(s.key) {
		return fmt.Errorf("dynaflux: %d values for the %d PRIMARY KEY columns of stream %s", len(key), len(s.key), s.Def.Name)
	}
	for i, c := range s.key {
		if _, _, err := encode(s.Columns[c].Name, s.Columns[c].Type, key[i]); err != nil {
			return err
		}
	}
	return nil
}

// lookupKey returns the partition key of the PRIMARY KEY values key, without a shard
// suffix, or "" when it is too long to match any event.
func (s *stream) lookupKey(key []expr.Value) (string, error) {
	pk, err := keys.Key(s.keyTypes(), key)
	if err != nil {
		return "", fmt.Errorf("partition key: %w", err)
	}
	if s.Def.Shards > 1 && len(keys.ShardKey(pk, 0)) > store.MaxPKLen || len(pk) > store.MaxPKLen {
		return "", nil
	}
	return pk, nil
}

// skRange returns the sort-key bounds of event_time in [from, to), "" when open, and false
// when no event_time is in the range.
func skRange(from, to *int64) (string, string, bool, error) {
	var lo, hi []byte
	var err error
	if from != nil && *from > 0 {
		if *from >= catalog.TimeLimitMicros {
			return "", "", false, nil
		}
		lo, err = keys.AppendField(nil, catalog.EventTimeType(), expr.Micros(*from))
	}
	if to != nil && *to < catalog.TimeLimitMicros && err == nil {
		if *to <= 0 || from != nil && *from >= *to {
			return "", "", false, nil
		}
		// Every event at to sorts after this bare time field, so the bound leaves them out.
		hi, err = keys.AppendField(nil, catalog.EventTimeType(), expr.Micros(*to))
	}
	if err != nil {
		return "", "", false, fmt.Errorf("event_time bound: %w", err)
	}
	return string(lo), string(hi), true, nil
}

// shards returns q's read of each shard of s: q itself without SHARDS.
func (e *Engine) shards(ctx context.Context, table string, s *stream, q store.Query) []iter.Seq2[store.Item, error] {
	if s.Def.Shards == 1 {
		return []iter.Seq2[store.Item, error]{e.b.Query(ctx, table, q)}
	}
	seqs := make([]iter.Seq2[store.Item, error], s.Def.Shards)
	pk := q.PK
	for i := range seqs {
		q.PK = keys.ShardKey(pk, i)
		seqs[i] = e.b.Query(ctx, table, q)
	}
	return seqs
}

// merge merges seqs, each in sort-key order, or in reverse order with desc, into one
// sequence in that order. It stops at the first error.
func merge(seqs []iter.Seq2[store.Item, error], desc bool) iter.Seq2[store.Item, error] {
	if len(seqs) == 1 {
		return seqs[0]
	}
	return func(yield func(store.Item, error) bool) {
		m := merger{heads: make([]store.Item, len(seqs)), desc: desc}
		for _, seq := range seqs {
			next, stop := iter.Pull2(seq)
			defer stop()
			m.next = append(m.next, next)
		}
		for i := range seqs {
			if err := m.pull(i); err != nil {
				yield(nil, err)
				return
			}
		}
		for i := m.first(); i >= 0; i = m.first() {
			if !yield(m.heads[i], nil) {
				return
			}
			if err := m.pull(i); err != nil {
				yield(nil, err)
				return
			}
		}
	}
}

// merger holds the next item of each merged sequence, nil once it has ended.
type merger struct {
	next  []func() (store.Item, error, bool)
	heads []store.Item
	desc  bool
}

func (m *merger) pull(i int) error {
	it, err, _ := m.next[i]()
	m.heads[i] = it
	return err
}

// first returns the sequence whose next item comes first, or -1 when every one has ended.
func (m *merger) first() int {
	first := -1
	for i, it := range m.heads {
		if it == nil {
			continue
		}
		if first < 0 || (it[catalog.AttrSK].Str < m.heads[first][catalog.AttrSK].Str) != m.desc {
			first = i
		}
	}
	return first
}

// decode returns the event in it, and whether it is still live at now.
func (s *stream) decode(it store.Item, now int64) ([]expr.Value, bool, error) {
	// The expiry was fixed at append; the catalog's LOG RETENTION may have changed since.
	if a, ok := it[catalog.AttrTTL]; ok {
		expiry, err := keys.TTLExpiry(a)
		if err != nil {
			return nil, false, fmt.Errorf("%s: %w", catalog.AttrTTL, err)
		}
		if now >= expiry {
			return nil, false, nil
		}
	}
	row, bad := s.Decode(it)
	for _, err := range bad {
		if err != nil {
			return nil, false, err
		}
	}
	return row, true, nil
}

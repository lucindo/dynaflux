// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// AppendMode changes how Append checks and writes events.
type AppendMode struct {
	// Import skips the ACCEPT window, to load past events, and never applies SYNC versions:
	// the projector applies the events.
	Import bool
	// Receipt asks the projector of the engine's region for a receipt of each event once it
	// has handled it, which [Engine.Wait] polls (ADR 0015).
	Receipt bool
}

// Appended is the result of appending one event.
type Appended struct {
	// Key is the stored event's key, also with ErrDuplicate: the event stored before.
	Key store.Key
	Err error
	// SamePayload reports, with ErrDuplicate, that the event stored before has this event's
	// declared values.
	SamePayload bool
	// Stored is the event stored at Key, which [Engine.Token] names: this call's, or with
	// ErrDuplicate the one stored before. Nil with any other error.
	Stored store.Item
}

// Append appends events to stream, each with its own conditional write, and returns their
// results in order. An event holds the declared columns' values in catalog order, then
// event_time, which NULL sets to the writer's clock. A failed check fails only its event.
//
// The stream's SYNC versions that are live in the engine's region, by the view states that
// queries read, take each event in the event's own transaction, one event after another
// (ADR 0014). The states are read for each event, so a long call follows their changes. The
// event names the versions in _sync, so the region's projector skips them. An evaluation
// error in one fails the event, which matches ErrEvaluation, and writes nothing. A state read
// that fails fails the events not yet written.
//
// An event whose event_id an earlier event of the call has isn't written: it gets
// ErrDuplicate, or that event's error when that event wasn't stored. When the write finds an
// event stored at its key, that is the call's own write when its origin_region, ingest_time
// and payload are the ones sent, as after the SDK retried a write whose response was lost:
// success. Any other stored event is ErrDuplicate, even one of an earlier call that this one
// retries. So another engine of the region that stores the same event in the same
// microsecond counts as this call.
func (e *Engine) Append(ctx context.Context, cat *Catalog, stream string, events [][]expr.Value, m AppendMode) ([]Appended, error) {
	s, err := cat.stream(stream)
	if err != nil {
		return nil, err
	}
	now := e.now()
	w, err := s.window(now, m)
	if err != nil {
		return nil, fmt.Errorf("dynaflux: append to %s: %w", stream, err)
	}
	out := make([]Appended, len(events))
	items := make([]store.Item, len(events))
	first := make(map[store.Key]int, len(events)) // the index of the event that claims each key
	for i, vals := range events {
		items[i], out[i].Key, out[i].Err = e.event(s, vals, now, w)
		if _, dup := first[out[i].Key]; !dup && out[i].Err == nil {
			first[out[i].Key] = i
		}
		if items[i] != nil && m.Receipt {
			items[i][catalog.AttrReceipt] = store.Value{Type: store.BOOL, Bool: true}
		}
	}
	table := e.table(cat, s)
	stored := make([]store.Item, len(events)) // the event at each key written, after the write
	var stateErr error
	for i, it := range items {
		if it == nil || first[out[i].Key] != i {
			continue
		}
		var live []string
		if stateErr == nil {
			live, stateErr = e.liveSync(ctx, cat, s, m)
		}
		switch {
		case stateErr != nil:
			out[i].Err = fmt.Errorf("dynaflux: append to %s: %w", stream, stateErr)
		case len(live) > 0:
			it[catalog.AttrSync] = syncList(live)
			stored[i], out[i].Err = e.putSync(ctx, cat, table, s, it, live, now)
		default:
			stored[i], out[i].Err = e.put(ctx, table, s, it)
		}
	}
	s.results(out, first, items, stored)
	return out, nil
}

// results completes out, the results of an Append whose events are items: the event that
// first claimed each key is first's, and stored[j] the event stored at event j's key after
// its write.
func (s *stream) results(out []Appended, first map[store.Key]int, items, stored []store.Item) {
	for i := range out {
		j, ok := first[out[i].Key]
		if ok && j != i && out[i].Err == nil {
			// A copy of a stored event is a duplicate; a copy of one that failed fails with it.
			out[i].Err = cmp.Or(out[j].Err, ErrDuplicate)
		}
		dup := errors.Is(out[i].Err, ErrDuplicate)
		out[i].SamePayload = ok && dup && s.SamePayload(stored[j], items[i])
		if ok && (out[i].Err == nil || dup) {
			out[i].Stored = stored[j]
		}
	}
}

// window is the ACCEPT window: event_time from lo to hi, both inclusive.
type window struct{ lo, hi int64 }

func (s *stream) window(now int64, m AppendMode) (window, error) {
	if m.Import {
		return window{math.MinInt64, math.MaxInt64}, nil
	}
	a := s.Def.Accept
	hi, err := expr.AddInterval(now, a.Future)
	if err != nil {
		return window{}, err
	}
	w := window{math.MinInt64, hi}
	if a.Past != nil {
		w.lo, err = expr.AddInterval(now, catalog.Interval{N: -a.Past.N, Unit: a.Past.Unit})
	}
	return w, err
}

func (w window) check(et int64) error {
	switch {
	case et < w.lo:
		return fmt.Errorf("%w: %s is before %s", ErrOutsideWindow, rfc3339(et), rfc3339(w.lo))
	case et > w.hi:
		return fmt.Errorf("%w: %s is after %s", ErrOutsideWindow, rfc3339(et), rfc3339(w.hi))
	}
	return nil
}

// event builds the item of one event.
func (e *Engine) event(s *stream, vals []expr.Value, now int64, w window) (store.Item, store.Key, error) {
	if len(vals) != len(s.Columns)+1 {
		return nil, store.Key{}, fmt.Errorf("dynaflux: %d values for the %d columns and event_time of stream %s", len(vals), len(s.Columns), s.Def.Name)
	}
	it := make(store.Item)
	for i, c := range s.Columns {
		a, present, err := encode(c.Name, c.Type, vals[i])
		if err != nil {
			return nil, store.Key{}, err
		}
		if present {
			it[c.Name] = a
		}
	}
	et, err := s.eventTime(vals[len(s.Columns)], now)
	if err != nil {
		return nil, store.Key{}, err
	}
	if err = w.check(et); err != nil {
		return nil, store.Key{}, err
	}
	k, err := e.key(s, vals, et, now)
	if err != nil {
		return nil, store.Key{}, err
	}
	it[catalog.AttrPK] = store.Value{Type: store.S, Str: k.PK}
	it[catalog.AttrSK] = store.Value{Type: store.S, Str: k.SK}
	it[catalog.AttrOriginRegion] = store.Value{Type: store.S, Str: e.region}
	it[catalog.AttrIngestTime], _, err = keys.EncodeValue(catalog.EventTimeType(), expr.Micros(now))
	if err != nil {
		return nil, store.Key{}, fmt.Errorf("dynaflux: ingest_time: %w", err)
	}
	if p, ok := s.Def.LogRetention.Period(); ok {
		expiry, aerr := expr.AddInterval(now, p)
		if aerr != nil {
			return nil, store.Key{}, fmt.Errorf("dynaflux: expiry: %w", aerr)
		}
		it[catalog.AttrTTL] = keys.TTL(expiry)
	}
	if s.archive != nil {
		// The projector archives the values it decodes from the item.
		decoded, bad := s.Decode(it)
		if err = errors.Join(bad...); err != nil {
			return nil, store.Key{}, fmt.Errorf("dynaflux: append to %s: the event doesn't decode: %w", s.Def.Name, err)
		}
		if size := s.archive.MaxSize(decoded); size > archive.MaxRecordSize {
			return nil, store.Key{}, &ArchiveSizeError{Stream: s.Def.Name, Size: size}
		}
	}
	return it, k, nil
}

func (s *stream) eventTime(v expr.Value, now int64) (int64, error) {
	if v.IsNull() {
		if len(s.dedup) > 0 {
			return 0, invalid(catalog.ColumnEventTime, errors.New("a stream with DEDUP BY needs it"))
		}
		return now, nil
	}
	if _, _, err := encode(catalog.ColumnEventTime, catalog.EventTimeType(), v); err != nil {
		return 0, err
	}
	return v.AsMicros(), nil
}

// key returns the key of an event at event_time et.
func (e *Engine) key(s *stream, vals []expr.Value, et, now int64) (store.Key, error) {
	var id string
	var err error
	if len(s.dedup) == 0 {
		id, err = e.ulids.Next(time.UnixMicro(now).UnixMilli())
	} else {
		var k string
		k, err = keys.Key(s.idTypes, s.idValues(vals, et))
		id = keys.DedupEventID(k)
	}
	if err != nil {
		return store.Key{}, fmt.Errorf("dynaflux: event_id: %w", err)
	}
	pk, err := keys.Key(s.keyTypes(), pick(vals, s.key))
	if err != nil {
		return store.Key{}, fmt.Errorf("dynaflux: partition key: %w", err)
	}
	if s.Def.Shards > 1 {
		pk = keys.ShardKey(pk, keys.Bucket(id, s.Def.Shards))
	}
	if len(pk) > store.MaxPKLen {
		return store.Key{}, invalid(strings.Join(s.Def.PrimaryKey, ", "),
			fmt.Errorf("the partition key is %d bytes, over %d", len(pk), store.MaxPKLen))
	}
	sk, err := keys.StreamSK(et, id)
	if err != nil {
		return store.Key{}, fmt.Errorf("dynaflux: sort key: %w", err)
	}
	return store.Key{PK: pk, SK: sk}, nil
}

func (s *stream) keyTypes() []catalog.Type {
	ts := make([]catalog.Type, len(s.key))
	for i, c := range s.key {
		ts[i] = s.Columns[c].Type
	}
	return ts
}

func (s *stream) idValues(vals []expr.Value, et int64) []expr.Value {
	vs := append(pick(vals, s.key), expr.Micros(et))
	return append(vs, pick(vals, s.dedup)...)
}

func pick(vals []expr.Value, idx []int) []expr.Value {
	out := make([]expr.Value, len(idx))
	for i, j := range idx {
		out[i] = vals[j]
	}
	return out
}

// put writes it unless an event is stored at its key, and returns the event stored there.
func (e *Engine) put(ctx context.Context, table string, s *stream, it store.Item) (store.Item, error) {
	err := e.b.Put(ctx, table, it, store.IfAbsent())
	var ce *store.ConditionError
	switch {
	case err == nil:
		return it, nil
	case errors.As(err, &ce):
		return s.claimed(ce.Current, it)
	case errors.Is(err, store.ErrTooLarge):
		return nil, fmt.Errorf("dynaflux: %w: the event is over DynamoDB's 400 KB item limit", ErrInvalidValue)
	}
	return nil, fmt.Errorf("dynaflux: append to %s: %w", s.Def.Name, err)
}

// claimed decides an append that found stored at its event's key: when stored holds what
// sent does, the same origin_region, ingest_time and payload, an earlier attempt of the call
// wrote it, which is success; any other event is ErrDuplicate.
func (s *stream) claimed(stored, sent store.Item) (store.Item, error) {
	if projector.SameValue(catalog.Type{Kind: catalog.KindString}, stored, sent, catalog.AttrOriginRegion) &&
		projector.SameValue(catalog.EventTimeType(), stored, sent, catalog.AttrIngestTime) &&
		s.SamePayload(stored, sent) {
		return sent, nil
	}
	return stored, ErrDuplicate
}

func rfc3339(us int64) string { return time.UnixMicro(us).UTC().Format(time.RFC3339Nano) }

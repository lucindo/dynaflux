// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// Stream item attributes (encoding.md §8.5), besides the deployed catalog's hash and
// registered_at and the view-state item's cause and at.
const (
	streamPrefix       = "stream#"
	attrMode           = "mode"
	attrModeAt         = "mode_at"
	attrOrigin         = "origin"
	attrBorn           = "born"
	attrARN            = "arn"
	attrHoles          = "holes"
	attrFrom           = "from"
	attrTo             = "to"
	attrObject         = "object"
	attrCheckedThrough = "checked_through"
	attrUnchecked      = "unchecked_before"
)

// ErrStreamChanged is a stream item written by someone else since it was read.
var ErrStreamChanged = errors.New("meta: the stream item changed since it was read")

// ArchiveMode is a region's archive mode for a stream, as the last deploy set it.
type ArchiveMode string

// Archive modes. A region's archive of a stream can be complete only under ArchiveAll.
const (
	ArchiveAll   ArchiveMode = ArchiveMode(catalog.ArchiveEventsAll)
	ArchiveLocal ArchiveMode = ArchiveMode(catalog.ArchiveEventsLocal)
	ArchiveOff   ArchiveMode = "off"
)

func (m ArchiveMode) known() bool {
	return m == ArchiveAll || m == ArchiveLocal || m == ArchiveOff
}

// HoleCause is why a hole was recorded.
type HoleCause string

// Hole causes. HoleUpdate and HoleCreate are deploys that changed the mode to or from
// ArchiveAll; HoleLag a projector that handled a record almost 24 hours old; HoleErrors a job
// that found an object in the error prefix that isn't Dynaflux records.
const (
	HoleUpdate HoleCause = "update"
	HoleCreate HoleCause = "create"
	HoleLag    HoleCause = "lag"
	HoleErrors HoleCause = "errors"
)

// StreamItem is a stream's history in a region's archive (ADR 0017): the stream item.
type StreamItem struct {
	Stream string
	Mode   ArchiveMode
	ModeAt time.Time
	Origin *Origin // nil before one is recorded
	// Holes are in the order they were written; none is ever removed.
	Holes          []Hole
	CheckedThrough time.Time // zero before the first passing check of projector activity
	// Unchecked is set when the hours since the origin before it went unchecked: CloudWatch
	// kept no hourly data that old when a passing check started.
	Unchecked time.Time

	version int64 // _v; 0 when the item doesn't exist
}

// Origin is when a region's archive of a stream began to hold every event.
type Origin struct {
	At           time.Time // the writer's clock
	Hash         string    // the region's current catalog then
	RegisteredAt time.Time // that catalog's registration
	Born         time.Time // the earliest CreationDateTime of the stream table's replicas
	ARN          string    // the stream's ARN in the region
	// Holes is how many holes the item held when the origin was written: only later ones
	// count against it.
	Holes int
}

// Void reports whether arn, the ARN of the stream table's stream in the region, isn't the one
// the origin was recorded for: the table, its replica in the region or its stream was
// re-created, and the history of the region's archive starts over. Replicas in other regions
// don't matter: the region's stream carries every event either way.
func (o Origin) Void(arn string) bool {
	return o.ARN != arn
}

// Hole is a time from which the region's archive of a stream may miss events.
type Hole struct {
	At       time.Time // the writer's clock, for display only
	Cause    HoleCause
	From, To ArchiveMode // HoleUpdate and HoleCreate
	Object   string      // HoleErrors
}

// After returns the holes that count against the origin: those written after it, or every
// hole without one. Their order, not their times, says so: the times come from the clocks
// of different hosts.
func (s StreamItem) After() []Hole {
	if s.Origin == nil {
		return s.Holes
	}
	return s.Holes[s.Origin.Holes:]
}

// Stream reads a stream's item with a consistent read, and reports whether it exists.
func (m *Meta) Stream(ctx context.Context, stream string) (StreamItem, bool, error) {
	it, err := m.b.Get(ctx, m.table, streamItemKey(stream), true)
	if err != nil {
		return StreamItem{}, false, fmt.Errorf("meta: read the stream item of %s: %w", stream, err)
	}
	if it == nil {
		return StreamItem{Stream: stream}, false, nil
	}
	s, err := m.decodeStream(stream, it)
	return s, err == nil, err
}

// RecordOrigin writes o, the origin of read's stream, into read, conditional on read's _v,
// and returns the stored item. o.Holes becomes the count of read's holes. It fails with
// ErrTransition when read's mode isn't ArchiveAll, or when read has an origin with o's ARN,
// which only a re-created stream voids; with ErrStreamChanged when read is no longer the
// stored item.
func (m *Meta) RecordOrigin(ctx context.Context, read StreamItem, o Origin) (StreamItem, error) {
	switch {
	case read.Mode != ArchiveAll:
		return StreamItem{}, fmt.Errorf("meta: record the origin of %s: its archive mode is %q: %w", read.Stream, read.Mode, ErrTransition)
	case read.Origin != nil && !read.Origin.Void(o.ARN):
		return StreamItem{}, fmt.Errorf("meta: record the origin of %s: it has one: %w", read.Stream, ErrTransition)
	}
	o.At, o.RegisteredAt, o.Born, o.Holes = stamp(o.At), stamp(o.RegisteredAt), stamp(o.Born), len(read.Holes)
	next := read.clone()
	next.Origin = &o
	stored, ok, err := m.putStream(ctx, read, next)
	switch {
	case err != nil:
		return StreamItem{}, err
	case !ok:
		return stored, fmt.Errorf("meta: record the origin of %s: %w", read.Stream, ErrStreamChanged)
	}
	return stored, nil
}

// ApplyArchiveModes records one deploy's archive modes, from old to next, stream by stream:
// a stream without an item gets one with its mode and no hole; a stream whose mode changes
// to or from ArchiveAll gets its new mode and a hole in one write; another change of mode
// only the mode. A stream missing from old or next counts as ArchiveOff; create says the
// deploy created the resource, whose old modes are then empty. at is the writer's clock.
func (m *Meta) ApplyArchiveModes(ctx context.Context, old, next map[string]ArchiveMode, create bool, at time.Time) error {
	cause := HoleUpdate
	if create {
		cause, old = HoleCreate, nil
	}
	streams := slices.Collect(maps.Keys(next))
	for _, modes := range []map[string]ArchiveMode{old, next} {
		for s, mode := range modes {
			if !mode.known() {
				return fmt.Errorf("meta: archive mode %q of %s isn't all, local or off", mode, s)
			}
			if !slices.Contains(streams, s) {
				streams = append(streams, s)
			}
		}
	}
	slices.Sort(streams)
	at = stamp(at)
	for _, s := range streams {
		from, to := modeOf(old, s), modeOf(next, s)
		err := m.updateStream(ctx, s, func(it *StreamItem) (bool, error) {
			switch {
			case it.version == 0:
				_, listed := next[s]
				it.Mode, it.ModeAt = to, at
				return listed, nil
			case (from == ArchiveAll) != (to == ArchiveAll):
				it.Holes = append(it.Holes, Hole{At: at, Cause: cause, From: from, To: to})
			case it.Mode == to:
				return false, nil
			}
			it.Mode, it.ModeAt = to, at
			return true, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func modeOf(modes map[string]ArchiveMode, stream string) ArchiveMode {
	if mode, ok := modes[stream]; ok {
		return mode
	}
	return ArchiveOff
}

// MarkLag records that a projector handled a record of stream almost 24 hours old, at by its
// clock, unless a lag hole already counts against the origin. A stream without an item has
// no origin for a hole to count against: nothing is written.
func (m *Meta) MarkLag(ctx context.Context, stream string, at time.Time) error {
	return m.markHole(ctx, stream, Hole{At: at, Cause: HoleLag})
}

// MarkErrors records that a job found object in stream's error prefix, which isn't Dynaflux
// records, as MarkLag records a lag.
func (m *Meta) MarkErrors(ctx context.Context, stream, object string, at time.Time) error {
	if object == "" {
		return fmt.Errorf("meta: mark errors of %s: no object", stream)
	}
	return m.markHole(ctx, stream, Hole{At: at, Cause: HoleErrors, Object: object})
}

func (m *Meta) markHole(ctx context.Context, stream string, h Hole) error {
	h.At = stamp(h.At)
	return m.updateStream(ctx, stream, func(it *StreamItem) (bool, error) {
		if it.version == 0 || slices.ContainsFunc(it.After(), func(o Hole) bool { return o.Cause == h.Cause }) {
			return false, nil
		}
		it.Holes = append(it.Holes, h)
		return true, nil
	})
}

// SetChecked records that a check of projector activity passed through through, and when
// unchecked is set, that the hours before it went unchecked. A stream without an item, or one
// checked further, is left as it is; Unchecked only moves later.
func (m *Meta) SetChecked(ctx context.Context, stream string, through, unchecked time.Time) error {
	through, unchecked = stamp(through), stamp(unchecked)
	return m.updateStream(ctx, stream, func(it *StreamItem) (bool, error) {
		if it.version == 0 || !through.After(it.CheckedThrough) {
			return false, nil
		}
		it.CheckedThrough = through
		if unchecked.After(it.Unchecked) {
			it.Unchecked = unchecked
		}
		return true, nil
	})
}

// updateStream applies change to stream's item until the conditional write holds. change
// reports whether there is anything to write.
func (m *Meta) updateStream(ctx context.Context, stream string, change func(*StreamItem) (bool, error)) error {
	cur, _, err := m.Stream(ctx, stream)
	if err != nil {
		return err
	}
	for range maxAttempts {
		next := cur.clone()
		write, cerr := change(&next)
		if cerr != nil || !write {
			return cerr
		}
		stored, ok, perr := m.putStream(ctx, cur, next)
		if perr != nil || ok {
			return perr
		}
		cur = stored
	}
	return m.errAttempts(streamItemKey(stream))
}

// putStream writes next in place of prev. When prev is no longer the stored item, it
// returns the stored one and false. A stored item equal to next is next, written by this
// call and retried by the SDK.
func (m *Meta) putStream(ctx context.Context, prev, next StreamItem) (StreamItem, bool, error) {
	next.version = prev.version + 1
	it := encodeStream(next)
	if _, err := m.decodeStream(next.Stream, it); err != nil {
		return StreamItem{}, false, err
	}
	err := m.b.Put(ctx, m.table, it, cond(prev.version))
	ce, failed := errors.AsType[*store.ConditionError](err)
	switch {
	case err != nil && !failed:
		return StreamItem{}, false, fmt.Errorf("meta: write the stream item of %s: %w", next.Stream, err)
	case !failed:
		return next, true, nil
	case ce.Current == nil:
		return StreamItem{Stream: next.Stream}, false, nil
	}
	stored, err := m.decodeStream(next.Stream, ce.Current)
	if err != nil {
		return StreamItem{}, false, err
	}
	return stored, reflect.DeepEqual(stored, next), nil
}

func (s StreamItem) clone() StreamItem {
	out := s
	out.Holes = slices.Clone(s.Holes)
	if s.Origin != nil {
		o := *s.Origin
		out.Origin = &o
	}
	return out
}

func streamItemKey(stream string) store.Key {
	return store.Key{PK: streamPrefix + stream, SK: keys.NoSortKey}
}

func encodeStream(s StreamItem) store.Item {
	k := streamItemKey(s.Stream)
	it := store.Item{
		catalog.AttrPK:         str(k.PK),
		catalog.AttrSK:         str(k.SK),
		attrMode:               str(string(s.Mode)),
		attrModeAt:             num(micros(s.ModeAt)),
		catalog.AttrRowVersion: num(s.version),
	}
	if o := s.Origin; o != nil {
		it[attrOrigin] = store.Value{Type: store.M, Map: map[string]store.Value{
			attrAt:           num(micros(o.At)),
			attrHash:         str(o.Hash),
			attrRegisteredAt: num(micros(o.RegisteredAt)),
			attrBorn:         num(micros(o.Born)),
			attrARN:          str(o.ARN),
			attrHoles:        num(int64(o.Holes)),
		}}
	}
	if len(s.Holes) > 0 {
		holes := make([]store.Value, len(s.Holes))
		for i, h := range s.Holes {
			m := map[string]store.Value{attrAt: num(micros(h.At)), attrCause: str(string(h.Cause))}
			if h.From != "" {
				m[attrFrom], m[attrTo] = str(string(h.From)), str(string(h.To))
			}
			if h.Object != "" {
				m[attrObject] = str(h.Object)
			}
			holes[i] = store.Value{Type: store.M, Map: m}
		}
		it[attrHoles] = store.Value{Type: store.L, List: holes}
	}
	putTime(it, attrCheckedThrough, s.CheckedThrough)
	putTime(it, attrUnchecked, s.Unchecked)
	return it
}

func (m *Meta) decodeStream(stream string, it store.Item) (StreamItem, error) {
	r := reader{attrs: it}
	r.str(catalog.AttrPK)
	r.str(catalog.AttrSK)
	s := StreamItem{
		Stream:         stream,
		Mode:           ArchiveMode(r.str(attrMode)),
		ModeAt:         r.instant(attrModeAt, true),
		CheckedThrough: r.instant(attrCheckedThrough, false),
		Unchecked:      r.instant(attrUnchecked, false),
	}
	if !s.Mode.known() {
		r.failf("%s is %q", attrMode, s.Mode)
	}
	r.nested(attrOrigin, false, func(or *reader) {
		s.Origin = &Origin{
			At: or.instant(attrAt, true), Hash: or.str(attrHash), RegisteredAt: or.instant(attrRegisteredAt, true),
			Born: or.instant(attrBorn, true), ARN: or.text(attrARN), Holes: int(or.count(attrHoles)),
		}
	})
	r.each(attrHoles, false, func(hr *reader) { s.Holes = append(s.Holes, decodeHole(hr)) })
	if s.Origin != nil && s.Origin.Holes > len(s.Holes) {
		r.failf("the origin follows %d holes of %d", s.Origin.Holes, len(s.Holes))
	}
	s.version = r.version()
	if err := r.close(); err != nil {
		return StreamItem{}, m.decodeError(it, err)
	}
	return s, nil
}

func decodeHole(r *reader) Hole {
	h := Hole{At: r.instant(attrAt, true), Cause: HoleCause(r.str(attrCause))}
	switch h.Cause {
	case HoleUpdate, HoleCreate:
		h.From, h.To = ArchiveMode(r.str(attrFrom)), ArchiveMode(r.str(attrTo))
		if !h.From.known() || !h.To.known() {
			r.failf("modes %q to %q", h.From, h.To)
		}
	case HoleErrors:
		h.Object = r.text(attrObject)
	case HoleLag:
	default:
		r.failf("%s is %q", attrCause, h.Cause)
	}
	return h
}

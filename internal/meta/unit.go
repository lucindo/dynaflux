// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// unitLifetime is how long a unit item outlives its last write: past every record of the
// unit that can still be delivered, and long enough that a row touched about once a year
// still finds the item to prune its entry for the unit.
const unitLifetime = 400 * 24 * time.Hour

// Unit item attributes.
const (
	unitPrefix     = "unit#"
	attrID         = "id"
	attrProgress   = "progress"
	attrProgressAt = "progress_at"
	attrFences     = "fences"
)

// Unit is a delivery unit's item as read. EnsureFence and Advance write it only while it is
// still the stored item.
type Unit struct {
	Stream string
	ID     string // the delivery unit's ID
	Key    string // its [keys.UnitKey]
	// Progress is the last sequence number fully applied and archived, 40 digits; "" before
	// the first advance.
	Progress   string
	ProgressAt time.Time         // the deadline of the invocation that last raised Progress
	Fences     map[string]string // view version ID → fence, 40 digits

	version int64 // _v; 0 when the item doesn't exist
}

// Unit reads a unit's item with a consistent read, so the skip by progress uses a value read
// after the invocation started.
func (m *Meta) Unit(ctx context.Context, stream, unitID string) (Unit, error) {
	u := Unit{Stream: stream, ID: unitID, Key: keys.UnitKey(unitID)}
	it, err := m.b.Get(ctx, m.table, unitItemKey(stream, u.Key), true)
	if err != nil {
		return Unit{}, fmt.Errorf("meta: read unit %s of %s: %w", unitID, stream, err)
	}
	if it == nil {
		return u, nil
	}
	return m.decodeUnit(stream, u.Key, it)
}

// Units reads the items of a stream's units by unit key, leaving out units without one.
// Pruning reads eventually consistent: progress only rises, so a stale item still proves what
// it shows. The backfill reads its fences consistently.
func (m *Meta) Units(ctx context.Context, stream string, unitKeys []string, consistent bool) (map[string]Unit, error) {
	out := make(map[string]Unit, len(unitKeys))
	for chunk := range slices.Chunk(unitKeys, store.MaxBatchGetKeys) {
		ks := make([]store.Key, len(chunk))
		for i, key := range chunk {
			ks[i] = unitItemKey(stream, key)
		}
		its, err := m.b.BatchGet(ctx, m.table, ks, consistent)
		if err != nil {
			return nil, fmt.Errorf("meta: read units of %s: %w", stream, err)
		}
		for i, it := range its {
			if it == nil {
				continue
			}
			u, derr := m.decodeUnit(stream, chunk[i], it)
			if derr != nil {
				return nil, derr
			}
			out[chunk[i]] = u
		}
	}
	return out, nil
}

// EnsureFence returns u with a fence for version. Without one, it fences the unit at its
// stored progress in the same conditional write, so every record at or below the fence was
// archived before the fence existed. An existing fence is kept.
func (m *Meta) EnsureFence(ctx context.Context, u Unit, version string) (Unit, error) {
	for range maxAttempts {
		if _, ok := u.Fences[version]; ok {
			return u, nil
		}
		next := u
		next.Fences = make(map[string]string, len(u.Fences)+1)
		maps.Copy(next.Fences, u.Fences)
		next.Fences[version] = cmp.Or(u.Progress, zeroFence())
		stored, ok, err := m.put(ctx, u, next)
		if err != nil || ok {
			return stored, err
		}
		u = stored
	}
	return Unit{}, m.errAttempts(unitItemKey(u.Stream, u.Key))
}

// Advance raises the unit's progress to upTo, a sequence number in its stored form, and
// stamps it with at, the invocation's deadline. A stored progress at or above upTo is
// success. A fence for a version that covered rejects fails with [ErrFenced]: this batch
// didn't apply the version to the records above the fence, and the backfill won't either.
func (m *Meta) Advance(ctx context.Context, u Unit, upTo string, at time.Time, covered func(version string) bool) error {
	if !isSeq(upTo) {
		return fmt.Errorf("meta: advance unit %s of %s to %q: not a sequence number in its stored form", u.ID, u.Stream, upTo)
	}
	if at.IsZero() {
		return fmt.Errorf("meta: advance unit %s of %s: no deadline", u.ID, u.Stream)
	}
	for range maxAttempts {
		if u.Progress >= upTo {
			return nil
		}
		for _, v := range slices.Sorted(maps.Keys(u.Fences)) {
			if !covered(v) {
				return fmt.Errorf("meta: advance unit %s of %s: fence of %s: %w", u.ID, u.Stream, v, ErrFenced)
			}
		}
		next := u
		next.Progress, next.ProgressAt = upTo, at
		stored, ok, err := m.put(ctx, u, next)
		if err != nil || ok {
			return err
		}
		u = stored
	}
	return m.errAttempts(unitItemKey(u.Stream, u.Key))
}

// FenceUnits fences each listed unit of a stream for version, as [Meta.EnsureFence] does.
// version must be [Activating], or the call fails with [ErrNotActivating]. listedAt, when the
// units were listed, must be at least [ListingDelay] after its activation: by then every
// projector knows the version is activating, so a unit missing from the listing gets its
// fence from its first batch. A listedAt ahead of the clock is refused. Fences already made
// are kept, so a failed call can be repeated.
func (m *Meta) FenceUnits(ctx context.Context, stream, version string, unitIDs []string, listedAt time.Time) error {
	vs, _, err := m.viewState(ctx, version)
	if err != nil {
		return err
	}
	if vs.State != Activating {
		return fmt.Errorf("meta: fence units of %s for %s: the version is %s: %w", stream, version, vs.State, ErrNotActivating)
	}
	if now := time.Now(); listedAt.After(now) {
		return fmt.Errorf("meta: fence units of %s for %s: listed at %s, ahead of the clock (%s)",
			stream, version, listedAt.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano))
	}
	if earliest := vs.ActivatedAt.Add(ListingDelay); listedAt.Before(earliest) {
		return fmt.Errorf("meta: fence units of %s for %s: listed at %s, before %s: %w",
			stream, version, listedAt.UTC().Format(time.RFC3339Nano), earliest.Format(time.RFC3339Nano), ErrListedTooEarly)
	}
	for _, id := range unitIDs {
		u, uerr := m.Unit(ctx, stream, id)
		if uerr == nil {
			_, uerr = m.EnsureFence(ctx, u, version)
		}
		if uerr != nil {
			return uerr
		}
	}
	return nil
}

// put writes next in place of prev. When prev is no longer the stored item, it returns the
// stored one and false, for the caller to check again.
func (m *Meta) put(ctx context.Context, prev, next Unit) (Unit, bool, error) {
	next.version = prev.version + 1
	err := m.b.Put(ctx, m.table, encodeUnit(next), cond(prev.version))
	var ce *store.ConditionError
	if !errors.As(err, &ce) {
		if err != nil {
			return Unit{}, false, fmt.Errorf("meta: write unit %s of %s: %w", prev.ID, prev.Stream, err)
		}
		return next, true, nil
	}
	if ce.Current == nil {
		return Unit{Stream: prev.Stream, ID: prev.ID, Key: prev.Key}, false, nil
	}
	stored, err := m.decodeUnit(prev.Stream, prev.Key, ce.Current)
	return stored, false, err
}

func unitItemKey(stream, unitKey string) store.Key {
	return store.Key{PK: unitPrefix + stream + "#" + unitKey, SK: keys.NoSortKey}
}

func encodeUnit(u Unit) store.Item {
	k := unitItemKey(u.Stream, u.Key)
	it := store.Item{
		catalog.AttrPK:         str(k.PK),
		catalog.AttrSK:         str(k.SK),
		attrID:                 str(u.ID),
		catalog.AttrRowVersion: num(u.version),
		catalog.AttrTTL:        keys.TTL(micros(time.Now().Add(unitLifetime))),
	}
	if u.Progress != "" {
		it[attrProgress] = str(u.Progress)
		it[attrProgressAt] = num(micros(u.ProgressAt))
	}
	if len(u.Fences) > 0 {
		fences := make(map[string]store.Value, len(u.Fences))
		for v, f := range u.Fences {
			fences[v] = str(f)
		}
		it[attrFences] = store.Value{Type: store.M, Map: fences}
	}
	return it
}

func (m *Meta) decodeUnit(stream, unitKey string, it store.Item) (Unit, error) {
	r := reader{attrs: it}
	r.str(catalog.AttrPK)
	r.str(catalog.AttrSK)
	u := Unit{Stream: stream, ID: r.str(attrID), Key: unitKey, Progress: r.seq(attrProgress, false, isSeq)}
	if u.Progress != "" {
		u.ProgressAt = r.instant(attrProgressAt, true)
	}
	if fences, ok := r.value(attrFences, store.M, false); ok {
		fr := reader{attrs: fences.Map}
		u.Fences = make(map[string]string, len(fences.Map))
		for _, v := range slices.Sorted(maps.Keys(fences.Map)) {
			u.Fences[v] = fr.seq(v, true, isFence)
		}
		if err := fr.close(); err != nil {
			r.failf("%s: %w", attrFences, err)
		}
	}
	r.integer(catalog.AttrTTL, true)
	u.version = r.version()
	if err := r.close(); err != nil {
		return Unit{}, m.decodeError(it, err)
	}
	return u, nil
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lucindo/dynaflux/internal/meta"
)

// OriginWait is how long after the current catalog's registration, and after the stream
// item's last mode hole, a scan for an origin starts once the item holds one: the longest
// invocation plus a minute for clocks, so no invocation of a build that didn't archive still
// runs. A rollback writes its hole and registers no catalog.
const OriginWait = 16 * time.Minute

// originAttempts bounds how often RecordOrigin starts over after a write that met another.
const originAttempts = 10

// OriginState is what RecordOrigin did, or why it recorded no origin.
type OriginState int

// Origin states.
const (
	// OriginRecorded is an origin RecordOrigin recorded.
	OriginRecorded OriginState = iota + 1
	// OriginHeld is a valid origin that was there.
	OriginHeld
	// OriginPending is a stream item's mode or a deployed catalog that doesn't archive every
	// event of the stream in the region: a deploy is under way, or the region doesn't.
	OriginPending
	// OriginWaiting is a deploy that changed the archive mode less than OriginWait ago.
	OriginWaiting
	// OriginTooLate is a stream table that holds an event, is 23 hours old or was restored: no
	// origin can be proven any more.
	OriginTooLate
)

// OriginStatus is what RecordOrigin did, and with OriginWaiting, when by the host's clock
// it may scan.
type OriginStatus struct {
	State OriginState
	Until time.Time
}

// RecordOrigin records the origin of stream's archive in env's region when ADR 0017 §3
// allows it now: the stream item's mode is all, the deployed catalog archives every event of
// the stream there, the stream is fresh by a scan sent after both were read, and with a mode
// hole, the scan starts OriginWait after the catalog's registration and the last mode hole.
// The write is conditional on the item read; when it meets another write, RecordOrigin starts
// over. A valid origin stays.
func RecordOrigin(ctx context.Context, env *Env, stream string) (OriginStatus, error) {
	if env.Witness == nil || env.Region == "" {
		return OriginStatus{}, errors.New("backfill: recording an origin needs the region and its witness")
	}
	for range originAttempts {
		item, o, st, err := originNow(ctx, env, stream)
		if err != nil || st.State != OriginRecorded {
			return st, err
		}
		_, err = env.Meta.RecordOrigin(ctx, item, o)
		switch {
		case err == nil:
			return st, nil
		case errors.Is(err, meta.ErrTransition):
			// The mode isn't all: a deploy changed it.
			return OriginStatus{State: OriginPending}, nil
		case !errors.Is(err, meta.ErrStreamChanged):
			return OriginStatus{}, err
		}
	}
	return OriginStatus{}, fmt.Errorf("backfill: record the origin of %s in %s: its stream item kept changing", stream, env.Region)
}

// originNow reads stream's item, and returns it with the origin that RecordOrigin may write
// over it now, with OriginRecorded, or else why it may write none.
func originNow(ctx context.Context, env *Env, stream string) (meta.StreamItem, meta.Origin, OriginStatus, error) {
	none := func(st OriginState, err error) (meta.StreamItem, meta.Origin, OriginStatus, error) {
		return meta.StreamItem{}, meta.Origin{}, OriginStatus{State: st}, err
	}
	table := env.Naming.StreamTable(stream)
	item, _, err := env.Meta.Stream(ctx, stream)
	if err != nil {
		return none(0, err)
	}
	arn, err := env.Witness.StreamARN(ctx, table)
	if err != nil {
		return none(0, fmt.Errorf("backfill: the stream ARN of %s: %w", table, err))
	}
	if item.Origin != nil && !item.Origin.Void(arn) {
		return none(OriginHeld, nil)
	}
	reg, cat, ok, err := deployedCatalog(ctx, env)
	switch {
	case err != nil:
		return none(0, err)
	case !ok || !cat.ExactHandoff(env.Region, stream):
		return none(OriginPending, nil)
	}
	// A mode other than all fails the write, with OriginPending.
	if until := scanAfter(item, reg); item.Mode == meta.ArchiveAll && env.now().Before(until) {
		return meta.StreamItem{}, meta.Origin{}, OriginStatus{State: OriginWaiting, Until: until}, nil
	}
	fresh, born, err := CheckStream(ctx, env.Tables, env.Store, table)
	switch {
	case err != nil:
		return none(0, err)
	case !fresh:
		return none(OriginTooLate, nil)
	}
	o := meta.Origin{At: env.now(), Hash: reg.Hash, RegisteredAt: reg.At, Born: born, ARN: arn}
	return item, o, OriginStatus{State: OriginRecorded}, nil
}

// scanAfter returns when a scan for an origin may start, zero without a mode hole:
// OriginWait after the later of the catalog's registration and the last mode hole's time.
func scanAfter(item meta.StreamItem, reg meta.Registered) time.Time {
	var last time.Time
	for _, h := range item.Holes {
		if h.Cause == meta.HoleUpdate || h.Cause == meta.HoleCreate {
			last = h.At
		}
	}
	if last.IsZero() {
		return time.Time{}
	}
	return later(last, reg.At).Add(OriginWait)
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

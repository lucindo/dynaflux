// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

// View-state item attributes.
const (
	viewPK          = "view"
	attrState       = "state"
	attrActivatedAt = "activated_at"
	attrBroken      = "broken"
	attrUnit        = "unit"
	attrSeq         = "seq"
	attrEventID     = "event_id"
	attrError       = "error"
	attrAt          = "at"
)

// State is a view version's state in a region.
type State string

// States. A version without an item is PendingBackfill.
const (
	PendingBackfill State = "pending_backfill"
	Activating      State = "activating"
	Live            State = "live"
	Broken          State = "broken"
)

// ViewState is a view version's state item.
type ViewState struct {
	Version     string
	State       State
	ActivatedAt time.Time // when State became Activating; zero before
	Failure     *Failure  // when State is Broken
}

// Failure is the record whose evaluation broke a version.
type Failure struct {
	Unit    string // the delivery unit's full ID
	Seq     string // 40 digits
	EventID string
	Error   string
	At      time.Time
}

// ViewStates reads every view version's state with consistent reads. A version missing from
// the result is [PendingBackfill].
func (m *Meta) ViewStates(ctx context.Context) (map[string]ViewState, error) {
	out := make(map[string]ViewState)
	for it, err := range m.b.Query(ctx, m.table, store.Query{PK: viewPK, Consistent: true}) {
		if err != nil {
			return nil, fmt.Errorf("meta: read view states: %w", err)
		}
		vs, _, derr := m.decodeView(it)
		if derr != nil {
			return nil, derr
		}
		out[vs.Version] = vs
	}
	return out, nil
}

// SetViewState moves a version from [PendingBackfill] to [Activating], stamping ActivatedAt,
// or from Activating to [Live]. Setting the state a version already has changes nothing, so
// a repeated call succeeds. Any other move fails with [ErrTransition]: in particular,
// [Broken] is final, so a finished backfill never turns a broken version live.
func (m *Meta) SetViewState(ctx context.Context, version string, s State) error {
	if s != Activating && s != Live {
		return fmt.Errorf("meta: set %s to %s: %w", version, s, ErrTransition)
	}
	return m.updateView(ctx, version, func(vs *ViewState) (bool, error) {
		switch {
		case vs.State == s:
			return false, nil
		case s == Activating && vs.State == PendingBackfill:
			vs.ActivatedAt = time.Now()
		case s == Live && vs.State == Activating:
		default:
			return false, fmt.Errorf("meta: set %s from %s to %s: %w", version, vs.State, s, ErrTransition)
		}
		vs.State = s
		return true, nil
	})
}

// MarkBroken moves a version to [Broken] from any state, recording f. A broken version keeps
// the first failure.
func (m *Meta) MarkBroken(ctx context.Context, version string, f Failure) error {
	if !isSeq(f.Seq) {
		return fmt.Errorf("meta: mark %s broken: %q is not a sequence number in its stored form", version, f.Seq)
	}
	return m.updateView(ctx, version, func(vs *ViewState) (bool, error) {
		if vs.State == Broken {
			return false, nil
		}
		vs.State, vs.Failure = Broken, &f
		return true, nil
	})
}

// viewState reads a version's state and its item's version with a consistent read.
func (m *Meta) viewState(ctx context.Context, version string) (ViewState, int64, error) {
	it, err := m.b.Get(ctx, m.table, store.Key{PK: viewPK, SK: version}, true)
	if err != nil {
		return ViewState{}, 0, fmt.Errorf("meta: read the state of %s: %w", version, err)
	}
	if it == nil {
		return ViewState{Version: version, State: PendingBackfill}, 0, nil
	}
	return m.decodeView(it)
}

// updateView applies change to a version's state until the conditional write holds. change
// reports whether there is anything to write.
func (m *Meta) updateView(ctx context.Context, version string, change func(*ViewState) (bool, error)) error {
	vs, v, err := m.viewState(ctx, version)
	if err != nil {
		return err
	}
	for range maxAttempts {
		write, cerr := change(&vs)
		if cerr != nil || !write {
			return cerr
		}
		err = m.b.Put(ctx, m.table, encodeView(vs, v+1), cond(v))
		var ce *store.ConditionError
		if !errors.As(err, &ce) {
			if err != nil {
				return fmt.Errorf("meta: write the state of %s: %w", version, err)
			}
			return nil
		}
		if ce.Current == nil {
			vs, v = ViewState{Version: version, State: PendingBackfill}, 0
		} else if vs, v, err = m.decodeView(ce.Current); err != nil {
			return err
		}
	}
	return m.errAttempts(store.Key{PK: viewPK, SK: version})
}

func encodeView(vs ViewState, v int64) store.Item {
	it := store.Item{
		catalog.AttrPK:         str(viewPK),
		catalog.AttrSK:         str(vs.Version),
		attrState:              str(string(vs.State)),
		catalog.AttrRowVersion: num(v),
	}
	if !vs.ActivatedAt.IsZero() {
		it[attrActivatedAt] = num(micros(vs.ActivatedAt))
	}
	if f := vs.Failure; f != nil {
		it[attrBroken] = store.Value{Type: store.M, Map: map[string]store.Value{
			attrUnit:    str(f.Unit),
			attrSeq:     str(f.Seq),
			attrEventID: str(f.EventID),
			attrError:   str(f.Error),
			attrAt:      num(micros(f.At)),
		}}
	}
	return it
}

func (m *Meta) decodeView(it store.Item) (ViewState, int64, error) {
	r := reader{attrs: it}
	r.str(catalog.AttrPK)
	vs := ViewState{Version: r.str(catalog.AttrSK), State: State(r.str(attrState))}
	switch vs.State {
	case PendingBackfill:
	case Activating, Live:
		vs.ActivatedAt = r.instant(attrActivatedAt, true)
	case Broken:
		vs.ActivatedAt = r.instant(attrActivatedAt, false)
		if b, ok := r.value(attrBroken, store.M, true); ok {
			br := reader{attrs: b.Map}
			vs.Failure = &Failure{
				Unit:    br.str(attrUnit),
				Seq:     br.seq(attrSeq, true, isSeq),
				EventID: br.str(attrEventID),
				Error:   br.str(attrError),
				At:      br.instant(attrAt, true),
			}
			if err := br.close(); err != nil {
				r.failf("%s: %w", attrBroken, err)
			}
		}
	default:
		r.failf("%s %q is unknown", attrState, vs.State)
	}
	v := r.version()
	if err := r.close(); err != nil {
		return ViewState{}, 0, m.decodeError(it, err)
	}
	return vs, v, nil
}

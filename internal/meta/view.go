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
	viewPK           = "view"
	attrState        = "state"
	attrStream       = "stream"
	attrActivatedAt  = "activated_at"
	attrSplit        = "split"
	attrSplitAt      = "split_at"
	attrSplitLate    = "split_late"
	attrPartialSince = "partial_since"
	attrRetiredAt    = "retired_at"
	attrBroken       = "broken"
	attrCause        = "cause"
	attrUnit         = "unit"
	attrSeq          = "seq"
	attrEventID      = "event_id"
	attrError        = "error"
	attrAt           = "at"
)

// State is a view version's state in a region (docs/spec/lifecycle.md §3).
type State string

// States. A version without an item is PendingBackfill. Nothing leaves Retired.
const (
	PendingBackfill State = "pending_backfill"
	Activating      State = "activating"
	Live            State = "live"
	Broken          State = "broken"
	Retired         State = "retired"
)

// Split is how a version's events are divided between its backfill and live projection
// (docs/spec/lifecycle.md §5.1). It also fixes the source of every job of the version.
type Split string

// Splits.
const (
	SplitFence Split = "fence" // the archive source, split at each unit's fence
	SplitTime  Split = "time"  // the table source for an additive version, split at SplitAt
	SplitNone  Split = "none"  // the table source for an idempotent version
)

func checkSplit(s Split) error {
	switch s {
	case SplitFence, SplitTime, SplitNone:
		return nil
	}
	return fmt.Errorf("split %q is unknown", s)
}

// Cause is why a version broke.
type Cause string

// Causes.
const (
	CauseRecord Cause = "record" // an event failed to evaluate
	CauseSplit  Cause = "split"  // an event that the time split can't place
)

// ViewState is a view version's state item.
type ViewState struct {
	Version, Stream string
	State           State
	Split           Split     // set with ActivatedAt
	ActivatedAt     time.Time // when State became Activating; zero before
	// SplitAt is the split time B, and SplitLate the time from which a projector breaks the
	// version on an INSERT at or before B. Both are zero unless Split is SplitTime.
	SplitAt, SplitLate time.Time
	PartialSince       time.Time // the earliest ingest_time the version is complete from; zero when complete
	RetiredAt          time.Time // when State became Retired
	Failure            *Failure  // once the version broke
}

// Failure is the record that broke a version.
type Failure struct {
	Cause Cause
	// Unit is the delivery unit's full ID and Seq the record's 40-digit sequence number. Both
	// are empty for a record read from the stream table, which EventID identifies.
	Unit, Seq string
	EventID   string
	Error     string
	At        time.Time
}

func (f *Failure) check() error {
	switch {
	case f.Cause != CauseRecord && f.Cause != CauseSplit:
		return fmt.Errorf("cause %q is unknown", f.Cause)
	case f.Unit == "" && f.Seq == "":
		if f.EventID == "" {
			return errors.New("a failure without a unit names no event_id")
		}
	case f.Unit == "" || !isSeq(f.Seq):
		return fmt.Errorf("unit %q and sequence number %q aren't a unit and a sequence number in its stored form", f.Unit, f.Seq)
	}
	return nil
}

// ViewStates reads every view version's state with consistent reads. A version missing from
// the result is [PendingBackfill].
func (m *Meta) ViewStates(ctx context.Context) (map[string]ViewState, error) {
	out := make(map[string]ViewState)
	for it, err := range m.b.Query(ctx, m.table, store.Query{PK: viewPK, Consistent: true}) {
		if err != nil {
			if accessDenied(err) {
				return nil, fmt.Errorf("meta: read view states: the role needs dynamodb:Query on table %s: %w", m.table, err)
			}
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

// accessDenied reports whether err is DynamoDB's AccessDeniedException. It matches the
// SDK's API errors by their method, since meta doesn't import the SDK (ADR 0004).
func accessDenied(err error) bool {
	var api interface{ ErrorCode() string }
	return errors.As(err, &api) && api.ErrorCode() == "AccessDeniedException"
}

// Activate moves a version of stream from [PendingBackfill] to [Activating], stamping
// ActivatedAt and fixing its split, and returns the stored state. With [SplitTime] it also
// sets SplitAt to ActivatedAt + b.Listing + b.Skew, and SplitLate to SplitAt + b.Skew +
// b.Landing. A version already activating with the same split is returned unchanged, so a
// repeated call succeeds. Any other state, or another split, fails with [ErrTransition].
func (m *Meta) Activate(ctx context.Context, version, stream string, split Split, b Bounds) (ViewState, error) {
	if err := checkSplit(split); err != nil {
		return ViewState{}, fmt.Errorf("meta: activate %s: %w", version, err)
	}
	if err := b.check(); err != nil {
		return ViewState{}, err
	}
	return m.updateView(ctx, version, stream, func(vs *ViewState) (bool, error) {
		switch {
		case vs.State == Activating && vs.Split == split:
			return false, nil
		case vs.State == Activating:
			return false, fmt.Errorf("meta: activate %s with the %s split: it is activating with the %s split: %w", version, split, vs.Split, ErrTransition)
		case vs.State != PendingBackfill:
			return false, fmt.Errorf("meta: activate %s: it is %s: %w", version, vs.State, ErrTransition)
		}
		vs.State, vs.Split, vs.ActivatedAt = Activating, split, stamp(time.Now())
		if split == SplitTime {
			vs.SplitAt = stamp(vs.ActivatedAt.Add(b.Listing + b.Skew))
			vs.SplitLate = stamp(vs.SplitAt.Add(b.Skew + b.Landing))
		}
		return true, nil
	})
}

// MakeLive moves a version from [Activating] to [Live]. A non-zero partialSince records that
// the version holds every event only from that ingest_time on. A live version is left as it
// is, so a repeated call succeeds. Any other state fails with [ErrTransition]: in
// particular, a finished backfill never turns a broken or retired version live.
func (m *Meta) MakeLive(ctx context.Context, version string, partialSince time.Time) error {
	_, err := m.updateView(ctx, version, "", func(vs *ViewState) (bool, error) {
		if vs.State == Live {
			return false, nil
		}
		if vs.State != Activating {
			return false, fmt.Errorf("meta: make %s live: it is %s: %w", version, vs.State, ErrTransition)
		}
		vs.State = Live
		if !partialSince.IsZero() {
			vs.PartialSince = stamp(partialSince)
		}
		return true, nil
	})
	return err
}

// Retire moves a version of stream from any state to [Retired], stamping RetiredAt and
// keeping every other attribute, and returns the stored state. A retired version is left
// as it is.
func (m *Meta) Retire(ctx context.Context, version, stream string) (ViewState, error) {
	return m.updateView(ctx, version, stream, func(vs *ViewState) (bool, error) {
		if vs.State == Retired {
			return false, nil
		}
		vs.State, vs.RetiredAt = Retired, stamp(time.Now())
		return true, nil
	})
}

// SetLive activates every view version of cat with the fence split and makes it live, for a
// store with no events to backfill, such as a test's.
func (m *Meta) SetLive(ctx context.Context, cat *catalog.Catalog) error {
	for _, v := range cat.Views {
		for _, vv := range v.Versions {
			id := catalog.VersionID(v.Name, vv.Number)
			if _, err := m.Activate(ctx, id, vv.Stream, SplitFence, DefaultBounds); err != nil {
				return err
			}
			if err := m.MakeLive(ctx, id, time.Time{}); err != nil {
				return err
			}
		}
	}
	return nil
}

// MarkBroken moves a version of stream to [Broken] from any state but [Retired], recording f
// with at most the first 1 KB of its error, and returns the stored state and whether this
// call opened the breaker. A broken version keeps the first failure, and a retired one
// ignores f.
func (m *Meta) MarkBroken(ctx context.Context, version, stream string, f Failure) (ViewState, bool, error) {
	if err := f.check(); err != nil {
		return ViewState{}, false, fmt.Errorf("meta: mark %s broken: %w", version, err)
	}
	f.Error = cutError(f.Error)
	opened := false // set once this call writes
	vs, err := m.updateView(ctx, version, stream, func(vs *ViewState) (bool, error) {
		switch vs.State {
		case PendingBackfill, Activating, Live:
			vs.State, vs.Failure, opened = Broken, &f, true
			return true, nil
		case Broken, Retired:
			// After a write, f in the stored state is that write, retried by the SDK; a retire
			// since then keeps it.
			sf := vs.Failure
			opened = opened && sf != nil && sf.Unit == f.Unit && sf.Seq == f.Seq && sf.EventID == f.EventID
		}
		return false, nil
	})
	return vs, opened && err == nil, err
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

// updateView applies change to the state of a version of stream until the conditional write
// holds, and returns the stored state. change reports whether there is anything to write. A
// version stored with another stream fails; an empty stream matches any.
func (m *Meta) updateView(ctx context.Context, version, stream string, change func(*ViewState) (bool, error)) (ViewState, error) {
	vs, v, err := m.viewState(ctx, version)
	if err != nil {
		return ViewState{}, err
	}
	for range maxAttempts {
		if v == 0 {
			vs.Stream = stream
		}
		if stream != "" && vs.Stream != stream {
			return ViewState{}, fmt.Errorf("meta: %s is a version of stream %s, not %s", version, vs.Stream, stream)
		}
		write, cerr := change(&vs)
		if cerr != nil {
			return ViewState{}, cerr
		}
		if !write {
			return vs, nil
		}
		err = m.putView(ctx, vs, v)
		var ce *store.ConditionError
		if !errors.As(err, &ce) {
			if err != nil {
				return ViewState{}, fmt.Errorf("meta: write the state of %s: %w", version, err)
			}
			return vs, nil
		}
		if ce.Current == nil {
			vs, v = ViewState{Version: version, State: PendingBackfill}, 0
		} else if vs, v, err = m.decodeView(ce.Current); err != nil {
			return ViewState{}, err
		}
	}
	return ViewState{}, m.errAttempts(store.Key{PK: viewPK, SK: version})
}

// putView writes vs over the item whose version is v, 0 for none. An item that doesn't
// decode, such as one without its stream, would fail every read of the view states, so it
// is never written.
func (m *Meta) putView(ctx context.Context, vs ViewState, v int64) error {
	it := encodeView(vs, v+1)
	if _, _, err := m.decodeView(it); err != nil {
		return err
	}
	return m.b.Put(ctx, m.table, it, cond(v))
}

func encodeView(vs ViewState, v int64) store.Item {
	it := store.Item{
		catalog.AttrPK:         str(viewPK),
		catalog.AttrSK:         str(vs.Version),
		attrState:              str(string(vs.State)),
		attrStream:             str(vs.Stream),
		catalog.AttrRowVersion: num(v),
	}
	if vs.Split != "" {
		it[attrSplit] = str(string(vs.Split))
	}
	putTime(it, attrActivatedAt, vs.ActivatedAt)
	putTime(it, attrSplitAt, vs.SplitAt)
	putTime(it, attrSplitLate, vs.SplitLate)
	putTime(it, attrPartialSince, vs.PartialSince)
	putTime(it, attrRetiredAt, vs.RetiredAt)
	if f := vs.Failure; f != nil {
		it[attrBroken] = store.Value{Type: store.M, Map: map[string]store.Value{
			attrCause:   str(string(f.Cause)),
			attrUnit:    str(f.Unit),
			attrSeq:     str(f.Seq),
			attrEventID: str(f.EventID),
			attrError:   str(f.Error),
			attrAt:      num(micros(f.At)),
		}}
	}
	return it
}

// presence is whether an item in a given state holds an attribute.
type presence uint8

const (
	absent presence = iota
	optional
	required
)

// viewLayout is what an item in a given state holds besides the attributes of every
// view-state item.
type viewLayout struct {
	activation presence // activated_at, with split and, for the time split, split_at and split_late
	failure    presence // broken
	partial    bool     // partial_since may be present once activated
	retired    bool     // retired_at
}

// layoutOf returns the layout of state s, and false for a state Dynaflux doesn't write. A
// move to Broken or Retired keeps what the item held.
func layoutOf(s State) (viewLayout, bool) {
	switch s {
	case PendingBackfill:
		return viewLayout{}, true
	case Activating:
		return viewLayout{activation: required}, true
	case Live:
		return viewLayout{activation: required, partial: true}, true
	case Broken:
		return viewLayout{activation: optional, failure: required, partial: true}, true
	case Retired:
		return viewLayout{activation: optional, failure: optional, partial: true, retired: true}, true
	}
	return viewLayout{}, false
}

func (m *Meta) decodeView(it store.Item) (ViewState, int64, error) {
	r := reader{attrs: it}
	r.str(catalog.AttrPK)
	vs := ViewState{Version: r.str(catalog.AttrSK), Stream: r.text(attrStream), State: State(r.str(attrState))}
	l, ok := layoutOf(vs.State)
	if !ok {
		r.failf("%s %q is unknown", attrState, vs.State)
	}
	if l.activation != absent {
		vs.ActivatedAt = r.instant(attrActivatedAt, l.activation == required)
	}
	if !vs.ActivatedAt.IsZero() {
		vs.Split = Split(r.str(attrSplit))
		if err := checkSplit(vs.Split); err != nil {
			r.failf("%w", err)
		}
		if vs.Split == SplitTime {
			vs.SplitAt = r.instant(attrSplitAt, true)
			vs.SplitLate = r.instant(attrSplitLate, true)
		}
		if l.partial {
			vs.PartialSince = r.instant(attrPartialSince, false)
		}
	}
	if l.failure != absent {
		vs.Failure = r.failure(l.failure == required)
	}
	if l.retired {
		vs.RetiredAt = r.instant(attrRetiredAt, true)
	}
	v := r.version()
	if err := r.close(); err != nil {
		return ViewState{}, 0, m.decodeError(it, err)
	}
	return vs, v, nil
}

// failure reads a view-state item's broken map; nil when it is absent.
func (r *reader) failure(required bool) *Failure {
	var f *Failure
	r.nested(attrBroken, required, func(br *reader) {
		f = &Failure{
			Cause:   Cause(br.str(attrCause)),
			Unit:    br.str(attrUnit),
			Seq:     br.str(attrSeq),
			EventID: br.str(attrEventID),
			Error:   br.str(attrError),
			At:      br.instant(attrAt, true),
		}
		if err := f.check(); err != nil {
			br.failf("%w", err)
		}
	})
	return f
}

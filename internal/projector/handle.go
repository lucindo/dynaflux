// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
)

// Op is the kind of a change-stream record.
type Op uint8

// Record kinds.
const (
	Insert Op = iota + 1
	Modify
	Remove
)

// Record is one change-stream record.
type Record struct {
	Op Op
	// Seq is the sequence number as delivered: 21 to 40 decimal digits.
	Seq string
	// Arrival is when the record entered the stream, in whole seconds: the archive's
	// _arrival. Zero when unknown.
	Arrival  time.Time
	New, Old store.Item
}

// Batch is one invocation's records: one delivery unit's.
type Batch struct {
	Stream string // the stream's name
	Unit   string // the delivery unit's ID
	// Records may come in any order, and repeat.
	Records []Record
}

// Result is the outcome of a batch. A batch that failed as a whole sets only Stats.
type Result struct {
	// FailedSeq is the lowest sequence number to deliver again, as delivered; "" when every
	// row was written.
	FailedSeq string
	// RowErr says why rows failed when FailedSeq is set.
	RowErr error
	Stats  Stats
}

// Stats count what a batch met besides its writes.
type Stats struct {
	// Divergent counts MODIFY records whose old and new payloads differ: one DEDUP BY event
	// appended in two regions with different values.
	Divergent int
	// Broken lists the view versions whose breaker this batch opened.
	Broken []string
	// TooLarge counts the rows that failed with [store.ErrTooLarge].
	TooLarge int
}

// Archiver writes a batch's INSERT records to the stream's archive, from which a backfill
// reads them.
type Archiver interface {
	Archive(ctx context.Context, stream, unit string, recs []Record) error
}

// ErrArchive fails a batch whose INSERT records the [Archiver] didn't write.
var ErrArchive = errors.New("projector: archive")

// Config configures a [Core].
type Config struct {
	// Naming names the store's tables.
	Naming catalog.Naming
}

// ErrLifetime fails a batch whose deadline is further away than the prune window allows: a
// row could drop the unit's high-water mark while the invocation still writes rows.
var ErrLifetime = errors.New("projector: the invocation can outlive the prune window")

// Core applies batches of change-stream records to the view versions of a store. It is
// safe for concurrent use.
type Core struct {
	c      *Compiled
	b      store.Backend
	a      Archiver
	meta   *meta.Meta
	naming catalog.Naming
	// window is W, the prune window. lifetime is the longest invocation it allows: W less the
	// time a write sent before the deadline may take to land.
	window, lifetime time.Duration

	mu       sync.Mutex
	states   map[string]meta.ViewState // nil until read
	statesAt time.Time                 // when the read that returned states was sent
}

// rowParallelism bounds the rows a batch writes at once.
const rowParallelism = 16

// Handle stops sending a reserve before the deadline: maxReserve, or the time left divided
// by reserveShare when that is less. Requests in flight end at the cutoff, and the reserve
// leaves time to close their connections and reply before Lambda stops the invocation.
const (
	maxReserve   = 2 * time.Second
	reserveShare = 10
)

// New returns a Core that writes c's views to b and archives what it applies to a, nil
// when this region doesn't archive.
func New(c *Compiled, b store.Backend, a Archiver, cfg Config) *Core {
	w := time.Duration(c.def.PruneWindowSeconds) * time.Second
	return &Core{
		c:        c,
		b:        b,
		a:        a,
		meta:     meta.New(b, cfg.Naming.MetaTable()),
		naming:   cfg.Naming,
		window:   w,
		lifetime: w - catalog.LifetimeMarginSeconds*time.Second,
	}
}

// record is a Record with its delivery unit and its sequence number in PadSeq's form.
type record struct {
	Record

	seq     string
	unit    string // the delivery unit's ID
	unitKey string
}

// Handle applies a batch to every view version of its stream that the batch reaches,
// archives its INSERT records and advances the unit's progress past what it applied; a batch
// without an INSERT sends nothing. ctx's deadline is the invocation's: at most the
// prune window less its margin away, or Handle fails with [ErrLifetime]. Handle sends
// nothing after the deadline less a reserve of min(2 s, a tenth of the time left), and
// stamps progress with the deadline itself. Handle returns an error only to fail the whole
// batch, with the batch's Stats either way. A row that fails sets FailedSeq, and every row
// is finished before Handle returns.
func (c *Core) Handle(ctx context.Context, b Batch) (Result, error) {
	if len(b.Records) == 0 {
		return Result{}, nil
	}
	deadline, ok := ctx.Deadline()
	if !ok || b.Unit == "" {
		return Result{}, errors.New("projector: a batch needs a delivery unit and a context with a deadline")
	}
	left := time.Until(deadline)
	if left > c.lifetime {
		return Result{}, fmt.Errorf("%w: the deadline is %v away, and a prune window of %v allows %v", ErrLifetime, left, c.window, c.lifetime)
	}
	s, ok := c.c.streams[b.Stream]
	if !ok {
		return Result{}, fmt.Errorf("projector: no stream %s in the catalog", b.Stream)
	}
	recs, err := sortRecords(b.Records, b.Unit)
	if err != nil {
		return Result{}, err
	}
	var res Result
	inserts := 0
	for _, r := range recs {
		switch r.Op {
		case Insert:
			inserts++
		case Modify:
			if !s.SamePayload(r.Old, r.New) {
				res.Stats.Divergent++
			}
		case Remove:
		}
	}
	// Only an INSERT changes a view or the archive, and progress may stay below the rest.
	if inserts == 0 {
		return res, nil
	}
	reserve := min(maxReserve, left/reserveShare)
	ctx, cancel := context.WithDeadline(ctx, deadline.Add(-reserve))
	defer cancel()
	// All but the progress write stop half a reserve earlier, so a row that never answers
	// still leaves time to checkpoint the rows written below it.
	rowCtx, cancelRows := context.WithDeadline(ctx, deadline.Add(-reserve-reserve/2))
	defer cancelRows()
	if err = c.apply(rowCtx, ctx, b, s, recs, deadline, &res); err != nil {
		return Result{Stats: res.Stats}, err
	}
	return res, nil
}

// apply is Handle's work on a batch with an INSERT, in recs. It sends with ctx, but writes
// progress with progressCtx and stamps it with deadline.
func (c *Core) apply(ctx, progressCtx context.Context, b Batch, s *Stream, recs []record, deadline time.Time, res *Result) error {
	rc, err := c.reach(ctx, b.Stream, b.Unit)
	if err != nil {
		return fmt.Errorf("projector: %w", err)
	}
	// Records at or below progress, read after the invocation started, were applied, and a row
	// may have dropped their marks since (S9 rule 1).
	recs = slices.DeleteFunc(recs, func(r record) bool { return r.seq <= rc.unit.Progress })
	if err = c.archive(ctx, b, recs); err != nil {
		return err
	}
	rs := &rows{owner: keys.OwnerLive, groups: map[groupKey]*rowGroup{}}
	broken := map[*view]bool{}
	for i := range recs {
		if r := &recs[i]; r.Op == Insert {
			if err = c.evaluate(ctx, rs, s, r, rc, broken, &res.Stats); err != nil {
				return err
			}
		}
	}
	groups := slices.DeleteFunc(rs.order, func(g *rowGroup) bool { return broken[g.v] })
	failed, err := c.finish(ctx, b.Stream, rc.unit.Key, groups, res)
	if err != nil {
		return err
	}
	return c.advance(progressCtx, rc, recs, failed, deadline)
}

// sortRecords pads sequence numbers, sorts the records by them and drops repeats: Lambda
// may redeliver out of order, and resumes from the lowest failed sequence number.
func sortRecords(in []Record, unit string) ([]record, error) {
	out := make([]record, len(in))
	for i, r := range in {
		if r.Op < Insert || r.Op > Remove {
			return nil, fmt.Errorf("projector: record %s has unknown op %d", r.Seq, r.Op)
		}
		var err error
		if out[i], err = newRecord(r, unit); err != nil {
			return nil, err
		}
	}
	slices.SortStableFunc(out, func(a, b record) int { return strings.Compare(a.seq, b.seq) })
	return slices.CompactFunc(out, func(a, b record) bool { return a.seq == b.seq }), nil
}

func newRecord(r Record, unit string) (record, error) {
	seq, err := keys.PadSeq(r.Seq)
	if err != nil {
		return record{}, fmt.Errorf("projector: unit %s: %w", unit, err)
	}
	return record{Record: r, seq: seq, unit: unit, unitKey: keys.UnitKey(unit)}, nil
}

// reach is what a batch applies: the view versions its records reach, and each fenced
// version's fence on the unit, at or below which the version's records belong to its
// backfill.
type reach struct {
	states map[string]meta.ViewState
	views  []*view
	fences map[*view]string
	unit   meta.Unit
}

// reach reads the view states, cached, unless the stream has no views, and the unit's item,
// consistently, and fences the unit for each version that needs the fenced handoff and
// records reach. A fence for a version the cached states call pending_backfill means they
// were read before its activation, so they are read again.
func (c *Core) reach(ctx context.Context, stream, unitID string) (*reach, error) {
	var states map[string]meta.ViewState
	var err error
	if len(c.c.byStream[stream]) > 0 {
		if states, err = c.viewStates(ctx, false); err != nil {
			return nil, err
		}
	}
	u, err := c.meta.Unit(ctx, stream, unitID)
	if err != nil {
		return nil, err
	}
	if c.stale(stream, states, u) {
		if states, err = c.viewStates(ctx, true); err != nil {
			return nil, err
		}
	}
	rc := &reach{states: states, views: c.active(stream, states), fences: map[*view]string{}}
	for _, v := range rc.views {
		if !v.def.FencedHandoff() {
			continue
		}
		if u, err = c.meta.EnsureFence(ctx, u, v.id); err != nil {
			return nil, err
		}
		rc.fences[v] = u.Fences[v.id]
	}
	rc.unit = u
	return rc, nil
}

// viewStates returns the view states, read at most meta.MaxViewStateAge ago unless fresh is
// set. The age is on the wall clock, since the monotonic one may stop while Lambda freezes
// the environment; a negative age, from a clock that stepped back, counts as expired.
func (c *Core) viewStates(ctx context.Context, fresh bool) (map[string]meta.ViewState, error) {
	c.mu.Lock()
	states, at := c.states, c.statesAt
	c.mu.Unlock()
	if age := time.Since(at); !fresh && states != nil && age >= 0 && age < meta.MaxViewStateAge {
		return states, nil
	}
	sent := time.Now().Round(0)
	states, err := c.meta.ViewStates(ctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.states, c.statesAt = states, sent
	c.mu.Unlock()
	return states, nil
}

// stale reports whether u holds a fence for a version of the stream that states call
// pending_backfill.
func (c *Core) stale(stream string, states map[string]meta.ViewState, u meta.Unit) bool {
	return slices.ContainsFunc(c.c.byStream[stream], func(v *view) bool {
		_, fenced := u.Fences[v.id]
		return fenced && cmp.Or(states[v.id].State, meta.PendingBackfill) == meta.PendingBackfill
	})
}

// active returns the view versions of stream that records reach: none that is broken, and
// a version that needs the fenced handoff only once it is activating or live.
func (c *Core) active(stream string, states map[string]meta.ViewState) []*view {
	var out []*view
	for _, v := range c.c.byStream[stream] {
		switch cmp.Or(states[v.id].State, meta.PendingBackfill) {
		case meta.Broken:
		case meta.Activating, meta.Live:
			out = append(out, v)
		case meta.PendingBackfill:
			if !v.def.FencedHandoff() {
				out = append(out, v)
			}
		}
	}
	return out
}

// archive writes the batch's INSERT records to the archive. A MODIFY is never archived:
// the backfill tells records apart by unit and sequence number, so its new image would
// count its event twice.
func (c *Core) archive(ctx context.Context, b Batch, recs []record) error {
	if c.a == nil {
		return nil
	}
	var ins []Record
	for i := range recs {
		if recs[i].Op == Insert {
			ins = append(ins, recs[i].Record)
		}
	}
	if len(ins) == 0 {
		return nil
	}
	if err := c.a.Archive(ctx, b.Stream, b.Unit, ins); err != nil {
		return fmt.Errorf("%w: %w", ErrArchive, err)
	}
	return nil
}

// evaluate adds an INSERT record's contributions to rs: to each version it reaches, and to
// a fenced version only above the fence. A version whose evaluation fails is marked broken,
// and gets nothing more from the batch.
func (c *Core) evaluate(ctx context.Context, rs *rows, s *Stream, r *record, rc *reach, broken map[*view]bool, st *Stats) error {
	vals, bad := s.Decode(r.New)
	for _, v := range rc.views {
		if f, fenced := rc.fences[v]; broken[v] || fenced && r.seq <= f {
			continue
		}
		err := rs.add(v, r, vals, bad)
		if err == nil {
			continue
		}
		opened, err := c.breakVersion(ctx, v, r, vals, err)
		if err != nil {
			return err
		}
		if opened {
			st.Broken = append(st.Broken, v.id)
		}
		broken[v] = true
	}
	return nil
}

// breakVersion opens v's breaker with the record whose evaluation failed with cause, marks v
// broken in the cached view states, and reports whether this call opened the breaker.
func (c *Core) breakVersion(ctx context.Context, v *view, r *record, vals []expr.Value, cause error) (bool, error) {
	f := meta.Failure{
		Unit:    r.unit,
		Seq:     r.seq,
		EventID: vals[len(v.stream.Columns)+implicitEventID].AsString(),
		Error:   cause.Error(),
		At:      time.Now(),
	}
	opened, err := c.meta.MarkBroken(ctx, v.id, f)
	if err != nil {
		return false, fmt.Errorf("projector: %w", err)
	}
	c.cacheBroken(v.id)
	return opened, nil
}

// cacheBroken marks version broken in the cached view states, so the core's next batches
// skip it before the states expire. Broken is final, so the mark can't go stale.
func (c *Core) cacheBroken(version string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.states == nil { // a backfill reads no states, and the first read finds the version broken
		return
	}
	// Batches in flight hold the old map.
	states := maps.Clone(c.states)
	vs := states[version]
	vs.Version, vs.State = version, meta.Broken
	states[version] = vs
	c.states = states
}

// finish writes the rows of unit own's records, sets FailedSeq to the lowest sequence number
// that a failed row holds and returns it in PadSeq's form. A request DynamoDB would reject on
// every attempt fails the batch.
func (c *Core) finish(ctx context.Context, stream, own string, groups []*rowGroup, res *Result) (string, error) {
	errs := c.applyRows(ctx, stream, own, groups)
	lowest := ""
	var failed []error
	var invalid error
	for i, err := range errs {
		switch {
		case err == nil:
			continue
		case errors.Is(err, store.ErrInvalidRequest):
			invalid = cmp.Or(invalid, err)
			continue
		case errors.Is(err, store.ErrTooLarge):
			res.Stats.TooLarge++
		}
		failed = append(failed, err)
		for _, ct := range groups[i].contribs {
			if lowest == "" || ct.seq < lowest {
				lowest, res.FailedSeq = ct.seq, ct.delivered
			}
		}
	}
	if invalid != nil {
		return "", invalid
	}
	res.RowErr = errors.Join(failed...)
	return lowest, nil
}

// advance raises the unit's progress to the last record below failed, every record when
// failed is "", stamped with the deadline. A fence for a version the batch didn't apply
// fails the batch; the next batch of the unit finds the fence and reads the view states
// again. A broken version is covered, and so is every version records could reach, whether
// or not one did.
func (c *Core) advance(ctx context.Context, rc *reach, recs []record, failed string, deadline time.Time) error {
	upTo := ""
	for i := range recs {
		if failed != "" && recs[i].seq >= failed {
			break
		}
		upTo = recs[i].seq
	}
	if upTo == "" {
		return nil
	}
	covered := func(version string) bool {
		return rc.states[version].State == meta.Broken ||
			slices.ContainsFunc(rc.views, func(v *view) bool { return v.id == version })
	}
	if err := c.meta.Advance(ctx, rc.unit, upTo, deadline, covered); err != nil {
		return fmt.Errorf("projector: %w", err)
	}
	return nil
}

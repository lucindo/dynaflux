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
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
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
	// States is the meta table's state cache, which queries in the same process may share;
	// nil gives the core one of its own.
	States *meta.StateCache
	// Now is the host's clock, which the time split's late check reads when a batch starts;
	// nil means time.Now.
	Now func() time.Time
	// BackfillRows is how many rows ApplyBackfill writes at once; 0 means as many as a live
	// batch does, 16.
	BackfillRows int
}

// ErrUnknownVersion fails a batch whose stream has an activating or live version that the
// catalog neither defines nor lists as retired (G3): this build would consume records the
// version needs.
var ErrUnknownVersion = errors.New("projector: an active view version isn't in this catalog")

// ErrSplit fails a batch whose stream has an activating or live additive version split
// neither at fences nor by time: this build would count the backfill's events again.
var ErrSplit = errors.New("projector: an active view version has a split this build doesn't apply")

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
	states *meta.StateCache
	naming catalog.Naming
	// window is W, the prune window. lifetime is the longest invocation it allows: W less the
	// time a write sent before the deadline may take to land.
	window, lifetime time.Duration
	now              func() time.Time
	backfillRows     int
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
	m := meta.New(b, cfg.Naming.MetaTable())
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Core{
		c:            c,
		b:            b,
		a:            a,
		meta:         m,
		states:       cmp.Or(cfg.States, meta.NewStateCache(m)),
		naming:       cfg.Naming,
		window:       w,
		lifetime:     w - catalog.LifetimeMarginSeconds*time.Second,
		now:          now,
		backfillRows: cmp.Or(cfg.BackfillRows, rowParallelism),
	}
}

// record is a Record with its delivery unit and its sequence number in PadSeq's form. An item
// a backfill read from the stream table has no unit: its time mark stands in for the unit
// key and the sequence number.
type record struct {
	Record

	seq     string
	unit    string // the delivery unit's ID; "" for a table item
	unitKey string
}

// eventID returns the event_id in the sort key of r's new image; "" when it doesn't parse.
func (r *record) eventID() string {
	_, id, err := keys.ParseStreamSK(r.New[catalog.AttrSK].Str)
	if err != nil {
		return ""
	}
	return id
}

// name names r in an error: by its unit and sequence number, or a table item by its event_id.
func (r *record) name() string {
	if r.unit == "" {
		return "event " + r.eventID()
	}
	return "unit " + r.unit + " record " + r.Seq
}

// Handle applies a batch to every view version of its stream that the batch reaches,
// archives its INSERT records and advances the unit's progress past what it applied; a batch
// without an INSERT only reads the view states for its MODIFY records, if any. A version
// split by time takes an INSERT whose ingest_time is after the split time. Its breaker opens
// on an INSERT at or before it when the batch started at or after the split's late bound,
// and on a MODIFY whose images straddle it. Handle fails with [ErrUnknownVersion] while the
// stream has an active version this catalog lacks (G3), and with [ErrSplit] while it has an
// additive one split neither at fences nor by time. ctx's deadline is the invocation's: at
// most the prune window less its margin away, or Handle fails with [ErrLifetime]. Handle sends
// nothing after the deadline less a reserve of min(2 s, a tenth of the time left), and
// stamps progress with the deadline itself. Handle returns an error only to fail the whole
// batch, with the batch's Stats either way. A row that fails sets FailedSeq, and every row
// is finished before Handle returns.
func (c *Core) Handle(ctx context.Context, b Batch) (Result, error) {
	start := c.now()
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
	inserts, modifies := 0, 0
	for _, r := range recs {
		switch r.Op {
		case Insert:
			inserts++
		case Modify:
			modifies++
			if !s.SamePayload(r.Old, r.New) {
				res.Stats.Divergent++
			}
		case Remove:
		}
	}
	reserve := min(maxReserve, left/reserveShare)
	ctx, cancel := context.WithDeadline(ctx, deadline.Add(-reserve))
	defer cancel()
	// All but the progress write stop half a reserve earlier, so a row that never answers
	// still leaves time to checkpoint the rows written below it.
	rowCtx, cancelRows := context.WithDeadline(ctx, deadline.Add(-reserve-reserve/2))
	defer cancelRows()
	// Only an INSERT changes a view or the archive, and progress may stay below the rest. A
	// MODIFY still meets G3, and the time split: older code must not consume one.
	if inserts == 0 {
		if modifies > 0 {
			err = c.modifies(rowCtx, s, recs, &res.Stats)
		}
		return res, err
	}
	if err = c.apply(rowCtx, ctx, b, s, recs, start, deadline, &res); err != nil {
		return Result{Stats: res.Stats}, err
	}
	return res, nil
}

// apply is Handle's work on a batch with an INSERT, in recs, which started at start. It sends
// with ctx, but writes progress with progressCtx and stamps it with deadline.
func (c *Core) apply(ctx, progressCtx context.Context, b Batch, s *Stream, recs []record, start, deadline time.Time, res *Result) error {
	rc, err := c.reach(ctx, b.Stream, b.Unit)
	if err != nil {
		return err
	}
	rc.start = start
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
		} else if r.Op == Modify {
			if err = c.straddle(ctx, r, rc.views, rc.states, broken, &res.Stats); err != nil {
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
// backfill. start is when the batch started, which the time split's late check reads.
type reach struct {
	states map[string]meta.ViewState
	views  []*view
	fences map[*view]string
	unit   meta.Unit
	start  time.Time
}

// reach reads the view states, cached, and the unit's item, consistently, and fences the
// unit for each version that needs the fenced handoff, is split at fences and records reach.
// A fence for a version the cached states call pending_backfill means they were read before
// its activation, so they are read again.
func (c *Core) reach(ctx context.Context, stream, unitID string) (*reach, error) {
	states, err := c.viewStates(ctx, stream, false)
	if err != nil {
		return nil, err
	}
	u, err := c.meta.Unit(ctx, stream, unitID)
	if err != nil {
		return nil, fmt.Errorf("projector: %w", err)
	}
	if stale(states, u) {
		if states, err = c.viewStates(ctx, stream, true); err != nil {
			return nil, err
		}
	}
	rc := &reach{states: states, views: c.active(stream, states), fences: map[*view]string{}}
	for _, v := range rc.views {
		if !v.def.FencedHandoff() || states[v.id].Split != meta.SplitFence {
			continue
		}
		if u, err = c.meta.EnsureFence(ctx, u, v.id); err != nil {
			return nil, fmt.Errorf("projector: %w", err)
		}
		rc.fences[v] = u.Fences[v.id]
	}
	rc.unit = u
	return rc, nil
}

// viewStates returns the view states, read at most meta.MaxViewStateAge ago unless fresh is
// set, for a batch of stream. It fails with ErrUnknownVersion when they hold an activating or
// live version of stream that the catalog neither defines nor lists as retired (G3), and with
// ErrSplit for an additive one split neither at fences nor by time. It retires those the
// catalog lists as retired, so that no build skips a version that isn't retired in the region.
func (c *Core) viewStates(ctx context.Context, stream string, fresh bool) (map[string]meta.ViewState, error) {
	states, err := c.states.States(ctx, fresh)
	if err != nil {
		return nil, fmt.Errorf("projector: %w", err)
	}
	unknown, split := "", ""
	var retired []string
	for id, vs := range states {
		if vs.Stream != stream || vs.State != meta.Activating && vs.State != meta.Live {
			continue
		}
		v := c.c.views[id]
		switch {
		case v != nil:
			if v.def.FencedHandoff() && vs.Split != meta.SplitFence && vs.Split != meta.SplitTime && (split == "" || id < split) {
				split = id
			}
		case slices.Contains(c.c.def.Retired, id):
			retired = append(retired, id)
		case unknown == "" || id < unknown:
			unknown = id
		}
	}
	if unknown != "" {
		return nil, fmt.Errorf("%w: %s of stream %s is %s; deploy a build whose catalog defines it",
			ErrUnknownVersion, unknown, stream, states[unknown].State)
	}
	if split != "" {
		return nil, fmt.Errorf("%w: %s of stream %s is %s with the %s split; deploy a build that applies it",
			ErrSplit, split, stream, states[split].State, states[split].Split)
	}
	if len(retired) == 0 {
		return states, nil
	}
	slices.Sort(retired) // in a fixed order, which the harness replays
	return c.retire(ctx, states, retired)
}

// retire writes the tombstone of each version in ids before a batch skips them, and returns
// states with the stored ones.
func (c *Core) retire(ctx context.Context, states map[string]meta.ViewState, ids []string) (map[string]meta.ViewState, error) {
	states = maps.Clone(states) // the cache shares its map
	for _, id := range ids {
		vs, err := c.meta.Retire(ctx, id, states[id].Stream)
		if err != nil {
			return nil, fmt.Errorf("projector: %w", err)
		}
		c.states.Set(vs)
		states[id] = vs
	}
	return states, nil
}

// stale reports whether u holds a fence for a version that states call pending_backfill.
func stale(states map[string]meta.ViewState, u meta.Unit) bool {
	for v := range u.Fences {
		if cmp.Or(states[v].State, meta.PendingBackfill) == meta.PendingBackfill {
			return true
		}
	}
	return false
}

// active returns the view versions of stream that records reach: none that is broken or
// retired, and a version that needs the fenced handoff only once it is activating or live.
func (c *Core) active(stream string, states map[string]meta.ViewState) []*view {
	var out []*view
	for _, v := range c.c.byStream[stream] {
		switch cmp.Or(states[v.id].State, meta.PendingBackfill) {
		case meta.Broken, meta.Retired:
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

// evaluate adds an INSERT record's contributions to rs: to each version it reaches, to a
// fenced version only above the fence, and to a version split by time only after the split
// time. A version whose evaluation fails, or whose time split can't place the record, is
// marked broken, and gets nothing more from the batch.
func (c *Core) evaluate(ctx context.Context, rs *rows, s *Stream, r *record, rc *reach, broken map[*view]bool, st *Stats) error {
	vals, bad := s.Decode(r.New)
	for _, v := range rc.views {
		if f, fenced := rc.fences[v]; broken[v] || fenced && r.seq <= f {
			continue
		}
		live, err := byTime(rc.states[v.id], rc.start, r)
		if err != nil {
			if err = c.open(ctx, v, r, meta.CauseSplit, err, broken, st); err != nil {
				return err
			}
			continue
		}
		if !live {
			continue
		}
		if err = rs.add(v, r, vals, bad); err != nil {
			if err = c.open(ctx, v, r, meta.CauseRecord, err, broken, st); err != nil {
				return err
			}
		}
	}
	return nil
}

// byTime reports whether live projection applies r, an INSERT, to a version in state vs.
// Split by time, it does exactly when r's ingest_time is after the split time; an INSERT at
// or before it is the backfill's, unless the batch started at or after the late bound.
func byTime(vs meta.ViewState, start time.Time, r *record) (bool, error) {
	if vs.Split != meta.SplitTime {
		return true, nil
	}
	at, err := ingestTime(r.New)
	switch {
	case err != nil:
		return false, err
	case at > vs.SplitAt.UnixMicro():
		return true, nil
	case !start.Before(vs.SplitLate):
		// The backfill's read began after the late bound, so it may have missed the event.
		return false, fmt.Errorf("ingest_time %s is at or before the split time %s, but the batch started at %s, at or after the late bound %s",
			rfc3339(at), rfc3339(vs.SplitAt.UnixMicro()), rfc3339(start.UnixMicro()), rfc3339(vs.SplitLate.UnixMicro()))
	}
	return false, nil
}

// modifies checks a batch's MODIFY records, which change no view: G3, then the time split of
// each version they reach.
func (c *Core) modifies(ctx context.Context, s *Stream, recs []record, st *Stats) error {
	states, err := c.viewStates(ctx, s.Def.Name, false)
	if err != nil {
		return err
	}
	views, broken := c.active(s.Def.Name, states), map[*view]bool{}
	for i := range recs {
		if r := &recs[i]; r.Op == Modify {
			if err = c.straddle(ctx, r, views, states, broken, st); err != nil {
				return err
			}
		}
	}
	return nil
}

// straddle opens the breaker of each version of views split by time whose split time r, a
// MODIFY, straddles: one DEDUP BY event appended in two regions, whose INSERT live projection
// judged by one ingest_time while the backfill judges the table's copy by the other.
func (c *Core) straddle(ctx context.Context, r *record, views []*view, states map[string]meta.ViewState, broken map[*view]bool, st *Stats) error {
	from, ferr := ingestTime(r.Old)
	to, terr := ingestTime(r.New)
	for _, v := range views {
		vs := states[v.id]
		if broken[v] || vs.Split != meta.SplitTime {
			continue
		}
		b, err := vs.SplitAt.UnixMicro(), errors.Join(ferr, terr)
		if err == nil && (from <= b) == (to <= b) {
			continue
		}
		if err == nil {
			err = fmt.Errorf("a MODIFY moves ingest_time from %s to %s, across the split time %s", rfc3339(from), rfc3339(to), rfc3339(b))
		}
		if err = c.open(ctx, v, r, meta.CauseSplit, err, broken, st); err != nil {
			return err
		}
	}
	return nil
}

// ingestTime returns the ingest_time an item holds, in microseconds.
func ingestTime(it store.Item) (int64, error) {
	v, err := decodeAttr(it, catalog.AttrIngestTime, catalog.EventTimeType())
	if err != nil {
		return 0, fmt.Errorf("%s: %w", catalog.ColumnIngestTime, err)
	}
	return v.AsMicros(), nil
}

func rfc3339(us int64) string { return time.UnixMicro(us).UTC().Format(time.RFC3339Nano) }

// open opens v's breaker with r, which failed with err for cause, and keeps v from the rest of
// the batch.
func (c *Core) open(ctx context.Context, v *view, r *record, cause meta.Cause, err error, broken map[*view]bool, st *Stats) error {
	opened, err := c.breakVersion(ctx, v, r, cause, err)
	if err != nil {
		return err
	}
	if opened {
		st.Broken = append(st.Broken, v.id)
	}
	broken[v] = true
	return nil
}

// breakVersion opens v's breaker with r, which failed with failure for cause, puts the stored
// state in the cached view states, and reports whether this call opened the breaker. A table
// item, without a unit, is named by its event_id alone.
func (c *Core) breakVersion(ctx context.Context, v *view, r *record, cause meta.Cause, failure error) (bool, error) {
	f := meta.Failure{Cause: cause, Unit: r.unit, EventID: r.eventID(), Error: failure.Error(), At: time.Now()}
	if r.unit != "" {
		f.Seq = r.seq
	}
	vs, opened, err := c.meta.MarkBroken(ctx, v.id, v.def.Stream, f)
	if err != nil {
		return false, fmt.Errorf("projector: %w", err)
	}
	c.states.Set(vs)
	return opened, nil
}

// finish writes the rows of unit own's records, sets FailedSeq to the lowest sequence number
// that a failed row holds and returns it in PadSeq's form. A request DynamoDB would reject on
// every attempt fails the batch.
func (c *Core) finish(ctx context.Context, stream, own string, groups []*rowGroup, res *Result) (string, error) {
	errs := c.applyRows(ctx, stream, own, groups, rowParallelism)
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
// again. A broken or retired version is covered, and so is every version records could reach,
// whether or not one did. A version the catalog lists as retired is retired by then.
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
		state := rc.states[version].State
		return state == meta.Broken || state == meta.Retired ||
			slices.ContainsFunc(rc.views, func(v *view) bool { return v.id == version })
	}
	if err := c.meta.Advance(ctx, rc.unit, upTo, deadline, covered); err != nil {
		return fmt.Errorf("projector: %w", err)
	}
	return nil
}

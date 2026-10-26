// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
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
	Seq      string
	New, Old store.Item
}

// Batch is one invocation's records: one delivery unit's.
type Batch struct {
	Stream string // the stream's name
	Unit   string // the delivery unit's ID
	// Records may come in any order, and repeat.
	Records []Record
}

// Result is the outcome of a batch that didn't fail as a whole.
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
}

// Config configures a [Core].
type Config struct {
	// Naming names the store's tables.
	Naming catalog.Naming
}

// Core applies batches of change-stream records to the view versions of a store. It is
// safe for concurrent use.
type Core struct {
	c      *Compiled
	b      store.Backend
	meta   *meta.Meta
	naming catalog.Naming
}

// rowParallelism bounds the rows a batch writes at once.
const rowParallelism = 16

// New returns a Core that writes c's views to b.
func New(c *Compiled, b store.Backend, cfg Config) *Core {
	return &Core{c: c, b: b, meta: meta.New(b, cfg.Naming.MetaTable()), naming: cfg.Naming}
}

// record is a Record with its sequence number in PadSeq's form.
type record struct {
	Record

	seq string
}

// Handle applies a batch to every view version of its stream that the batch reaches. ctx
// must have a deadline; nothing is sent after it. Handle returns an error only to fail the
// whole batch. A row that fails sets FailedSeq, and every row is finished before Handle
// returns.
func (c *Core) Handle(ctx context.Context, b Batch) (Result, error) {
	if len(b.Records) == 0 {
		return Result{}, nil
	}
	if _, ok := ctx.Deadline(); !ok || b.Unit == "" {
		return Result{}, errors.New("projector: a batch needs a delivery unit and a context with a deadline")
	}
	s, ok := c.c.streams[b.Stream]
	if !ok {
		return Result{}, fmt.Errorf("projector: no stream %s in the catalog", b.Stream)
	}
	recs, err := sortRecords(b.Records)
	if err != nil {
		return Result{}, err
	}
	active, err := c.active(ctx, b.Stream)
	if err != nil {
		return Result{}, err
	}
	var res Result
	rs := &rows{unit: b.Unit, groups: map[groupKey]*rowGroup{}}
	broken := map[*view]bool{}
	for i := range recs {
		r := &recs[i]
		switch r.Op {
		case Insert:
			if err = c.evaluate(ctx, rs, s, r, active, broken, &res.Stats); err != nil {
				return Result{}, err
			}
		case Modify:
			if !s.SamePayload(r.Old, r.New) {
				res.Stats.Divergent++
			}
		case Remove:
		default:
			return Result{}, fmt.Errorf("projector: record %s has unknown op %d", r.Seq, r.Op)
		}
	}
	groups := slices.DeleteFunc(rs.order, func(g *rowGroup) bool { return broken[g.v] })
	if err = c.finish(ctx, keys.UnitKey(b.Unit), groups, &res); err != nil {
		return Result{}, err
	}
	return res, nil
}

// sortRecords pads sequence numbers, sorts the records by them and drops repeats: Lambda
// may redeliver out of order, and resumes from the lowest failed sequence number.
func sortRecords(in []Record) ([]record, error) {
	out := make([]record, len(in))
	for i, r := range in {
		seq, err := keys.PadSeq(r.Seq)
		if err != nil {
			return nil, fmt.Errorf("projector: %w", err)
		}
		out[i] = record{Record: r, seq: seq}
	}
	slices.SortStableFunc(out, func(a, b record) int { return strings.Compare(a.seq, b.seq) })
	return slices.CompactFunc(out, func(a, b record) bool { return a.seq == b.seq }), nil
}

// active returns the view versions of stream that records reach: none that is broken, and
// a version that needs the fenced handoff only once it is activating or live.
func (c *Core) active(ctx context.Context, stream string) ([]*view, error) {
	views := c.c.byStream[stream]
	if len(views) == 0 {
		return nil, nil
	}
	states, err := c.meta.ViewStates(ctx)
	if err != nil {
		return nil, fmt.Errorf("projector: %w", err)
	}
	var out []*view
	for _, v := range views {
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
	return out, nil
}

// evaluate adds an INSERT record's contributions to rs. A version whose evaluation fails is
// marked broken, and gets nothing more from the batch.
func (c *Core) evaluate(ctx context.Context, rs *rows, s *Stream, r *record, active []*view, broken map[*view]bool, st *Stats) error {
	vals, bad := s.Decode(r.New)
	for _, v := range active {
		if broken[v] {
			continue
		}
		err := rs.add(v, r, vals, bad)
		if err == nil {
			continue
		}
		f := meta.Failure{
			Unit:    rs.unit,
			Seq:     r.seq,
			EventID: vals[len(s.Columns)+implicitEventID].AsString(),
			Error:   err.Error(),
			At:      time.Now(),
		}
		opened, err := c.meta.MarkBroken(ctx, v.id, f)
		if err != nil {
			return fmt.Errorf("projector: %w", err)
		}
		if opened {
			st.Broken = append(st.Broken, v.id)
		}
		broken[v] = true
	}
	return nil
}

// finish writes the rows, with bounded parallelism, and sets FailedSeq to the lowest
// sequence number that a failed row holds. A request DynamoDB would reject on every attempt
// fails the batch.
func (c *Core) finish(ctx context.Context, unitKey string, groups []*rowGroup, res *Result) error {
	errs := make([]error, len(groups))
	sem := make(chan struct{}, rowParallelism)
	var wg sync.WaitGroup
	for i, g := range groups {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			errs[i] = c.applyRow(ctx, unitKey, g)
		})
	}
	wg.Wait()
	lowest := ""
	var failed []error
	for i, err := range errs {
		if err == nil {
			continue
		}
		if errors.Is(err, store.ErrInvalidRequest) {
			return err
		}
		failed = append(failed, err)
		for _, ct := range groups[i].contribs {
			if lowest == "" || ct.seq < lowest {
				lowest, res.FailedSeq = ct.seq, ct.delivered
			}
		}
	}
	res.RowErr = errors.Join(failed...)
	return nil
}

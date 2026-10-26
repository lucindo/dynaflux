// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
)

const (
	watermarkPoll = 10 * time.Second // between a coordinator's reads of the archive's watermark
	stepRetry     = time.Second      // before a coordinator repeats a step that failed
)

// coordinator backfills one view version in one region, as the backfill command will
// (S1b rule 2, S9 rule 4): it activates the version and fences the stream's units, unless
// the version is idempotent-only; waits until the archive holds every record put before the
// cut; selects the archived records by the fences on the units' items, read after the
// archive; applies them chunk by chunk, each chunk in a worker invocation of its own; and
// makes the version live. A failed step is repeated; one that finds the version broken ends
// the backfill.
type coordinator struct {
	rg      *region
	id      string
	version string
	stream  *stream
	fenced  bool // whether the version needs the fenced handoff
	ctx     context.Context
	rng     *rand.Rand
	meta    *meta.Meta
	chunks  int      // worker invocations so far
	listed  []string // the units of the last listing
}

// coordinators returns the region's coordinators, one per version the run backfills.
func (rg *region) coordinators() []*coordinator {
	r := rg.run
	var out []*coordinator
	for _, v := range r.cfg.Catalog.Catalog().Views {
		for i := range v.Versions {
			vv := &v.Versions[i]
			id := catalog.VersionID(v.Name, vv.Number)
			if !r.backfills(id) {
				continue
			}
			actor := rg.name + " backfill " + id
			out = append(out, &coordinator{
				rg:      rg,
				id:      actor,
				version: id,
				stream:  rg.streams[r.cfg.Naming.StreamTable(vv.Stream)],
				fenced:  vv.FencedHandoff(),
				ctx:     withInvocation(context.Background(), &invocation{id: actor, stallAt: -1}),
				rng:     r.actorRNG(actor),
				meta:    meta.New(rg.store, r.meta),
			})
		}
	}
	return out
}

func (c *coordinator) run() {
	r := c.rg.run
	defer func() {
		r.sched.gate(c.id, "done")
		r.settle(1)
	}()
	time.Sleep(between(c.rng, 0, r.sc.Backfill.Within))
	if !c.do("activate", func() error { return c.meta.SetViewState(c.ctx, c.version, meta.Activating) }) {
		return
	}
	cut := time.Now()
	if c.fenced && !c.do("fence", func() error {
		var err error
		cut, err = c.meta.FenceUnits(c.ctx, c.stream.name, c.version, c.list)
		return err
	}) {
		return
	}
	r.sched.gate(c.id, "archive")
	for !c.rg.archive.completeThrough(cut, time.Now()) {
		time.Sleep(watermarkPoll)
		r.sched.gate(c.id, "watermark")
	}
	recs := c.read()
	var sel []projector.BackfillRecord
	if !c.do("select", func() error {
		fences, err := c.fences(unitsOf(recs))
		if err == nil {
			sel, err = projector.SelectBackfill(recs, fences)
		}
		return err
	}) {
		return
	}
	for chunk := range slices.Chunk(sel, r.sc.Backfill.Chunk) {
		if !c.do("apply", func() error { return c.apply(chunk) }) {
			return
		}
	}
	if c.do("live", func() error { return c.meta.SetViewState(c.ctx, c.version, meta.Live) }) {
		r.count(&r.stats.Backfills)
	}
}

// do repeats f until it succeeds, and reports whether it did. It gives up when the run
// stops, or when f finds the version broken.
func (c *coordinator) do(step string, f func() error) bool {
	r := c.rg.run
	for !r.stopped() {
		err := f()
		switch {
		case err == nil:
			return true
		case errors.Is(err, meta.ErrTransition), errors.Is(err, meta.ErrNotActivating), errors.Is(err, projector.ErrBroken):
			return false
		case !errors.Is(err, errTransient) && !errors.Is(err, errAmbiguous) && !errors.Is(err, errTimeout) && !errors.Is(err, errCrashed):
			r.violatef("backfill: %s: %s: %v", c.id, step, err)
			return false
		}
		time.Sleep(stepRetry)
	}
	return false
}

// list returns the IDs of the stream's units that DescribeStream returns: all but those
// that every mapping finished delivering, which stand for units past the stream's
// retention.
func (c *coordinator) list(ctx context.Context) ([]string, error) {
	lat, f := c.rg.enter(ctx, kindRead, "list "+c.stream.name)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	c.stream.listed = true
	c.listed = nil
	for _, u := range c.stream.units {
		if u.finished.IsZero() {
			c.listed = append(c.listed, u.id)
		}
	}
	return c.listed, nil
}

// read returns the stream's records in the archive, delivered or in the error prefix.
func (c *coordinator) read() []projector.BackfillRecord {
	c.rg.run.sched.gate(c.id, "read")
	var out []projector.BackfillRecord
	for _, e := range c.rg.archive.arrived(time.Now()) {
		if e.stream == c.stream.name {
			out = append(out, projector.BackfillRecord{Unit: e.unit, Seq: e.rec.Seq, Item: e.rec.New})
		}
	}
	return out
}

// unitsOf returns the units of recs.
func unitsOf(recs []projector.BackfillRecord) []string {
	var ids []string
	for _, rec := range recs {
		ids = append(ids, rec.Unit)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// fences reads the items of units, consistently, and returns their fences for the version.
func (c *coordinator) fences(units []string) (map[string]string, error) {
	unitKeys := make([]string, len(units))
	for i, id := range units {
		unitKeys[i] = keys.UnitKey(id)
	}
	items, err := c.meta.Units(c.ctx, c.stream.name, unitKeys, true)
	if err != nil {
		return nil, err
	}
	fences := map[string]string{}
	for _, u := range items {
		if f, ok := u.Fences[c.version]; ok {
			fences[u.ID] = f
		}
	}
	return fences, nil
}

// apply applies a chunk in one worker invocation.
func (c *coordinator) apply(chunk []projector.BackfillRecord) error {
	r := c.rg.run
	c.chunks++
	r.count(&r.stats.Chunks)
	id := c.id + " #" + strconv.Itoa(c.chunks)
	return c.rg.invoke(id, c.rng, len(chunk), func(ctx context.Context, core Core) (projector.Result, error) {
		return projector.Result{}, core.ApplyBackfill(ctx, c.version, chunk)
	}).err
}

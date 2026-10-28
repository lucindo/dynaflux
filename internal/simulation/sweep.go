// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"slices"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

// sweeper is a region's TTL: it deletes an item of a stream table or of the meta table up
// to Sweep.Delay after its _ttl. A sweep whose item was written again since is dropped: the
// later write has its own. Only gated code calls its methods but run.
type sweeper struct {
	rg    *region
	id    string
	queue dueQueue[sweep]
}

type sweep struct {
	due   time.Time
	table string
	key   store.Key
	ttl   string // the item's _ttl as written; another value means another write replaced it
}

func (sw sweep) dueAt() time.Time { return sw.due }

// expire schedules the delete of an item just written. A run waits for the deletes of
// stream items; a unit item outlives its run unless the core gives it too short a _ttl.
func (w *sweeper) expire(table string, it store.Item) {
	r := w.rg.run
	ttl, ok := it[catalog.AttrTTL]
	if !ok || r.sc.Sweep.Delay == 0 {
		return
	}
	sec, err := strconv.ParseInt(ttl.Str, 10, 64)
	if err != nil {
		r.violatef("harness: %s: %s %s has _ttl %q", w.rg.name, table, keyText(it), ttl.Str)
		return
	}
	if w.rg.streams[table] != nil {
		r.pending++
	}
	w.queue.add(sweep{due: time.Unix(sec, 0).Add(between(r.topo, 0, r.sc.Sweep.Delay)), table: table, key: store.KeyOf(it), ttl: ttl.Str})
}

func (w *sweeper) run() {
	r := w.rg.run
	for {
		r.sched.gate(w.id, "sweep")
		n := 0
		for n < len(w.queue.jobs) && !w.queue.jobs[n].due.After(time.Now()) {
			w.sweep(w.queue.jobs[n])
			n++
		}
		w.queue.jobs = slices.Delete(w.queue.jobs, 0, n)
		var next time.Time
		if len(w.queue.jobs) > 0 {
			next = w.queue.jobs[0].due
		}
		if !w.queue.wait(next, r.stop) {
			return
		}
	}
}

// sweep deletes the item of sw unless another write replaced it. A stream item's delete
// replicates.
func (w *sweeper) sweep(sw sweep) {
	rg, r := w.rg, w.rg.run
	ctx := context.Background()
	s := rg.streams[sw.table]
	if s != nil {
		defer r.settle(1)
	}
	cur, err := rg.mem.Get(ctx, sw.table, sw.key, true)
	switch {
	case err != nil:
	case cur == nil || cur[catalog.AttrTTL].Str != sw.ttl:
	case s != nil:
		s.remove(sw.key)
		for _, l := range rg.links {
			l.ship(shipment{table: sw.table, item: cur, remove: true})
		}
	default:
		err = rg.mem.Delete(ctx, sw.table, sw.key)
	}
	if err != nil {
		r.violatef("harness: %s: sweep of %s %s: %v", rg.name, sw.key.PK, sw.key.SK, err)
	}
}

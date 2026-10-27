// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

const (
	maxSeqGap    = 8                      // most a region's sequence number grows per record
	shortestLink = 200 * time.Millisecond // replication delay bounds
	longestLink  = 5 * time.Second
	partShift    = 40                       // key-hash bits above the ones that pick units choose a PF lane
	firstWideSeq = "1000000000000000000000" // 10^21, the first 22-digit sequence number
)

// stream is one region's replica of a stream table and its change stream, cut into
// delivery units. Only gated code changes it.
type stream struct {
	rg          *region
	name, table string
	seq         *big.Int
	units       []*unit // in creation order
	open        []*unit // one per key range; together they cover every key
	listed      bool    // whether a backfill listed the units
	// deleted holds the items deleted here. A delete is newer than any write of its item, so
	// a write that arrives after it is dropped.
	deleted map[store.Key]bool
}

func newStream(rg *region, name, table string) *stream {
	// Every region gets each of the stream's n events, and each adds at least 1, so a
	// start more than maxSeqGap and at most n below 10^21 gives sequence numbers of 21
	// and of 22 digits, which compare wrongly unpadded, whenever n > maxSeqGap.
	n := int64(rg.run.counts[name])
	lo := min(maxSeqGap+1, n)
	seq, _ := new(big.Int).SetString(firstWideSeq, 0)
	seq.Sub(seq, big.NewInt(lo+rg.run.topo.Int64N(n-lo+1)))
	return &stream{rg: rg, name: name, table: table, seq: seq, deleted: map[store.Key]bool{}}
}

// append writes an append to the replica and its change stream, then ships it to every
// other region. Records carry the item as stored, with numbers in DynamoDB's form.
func (s *stream) append(ctx context.Context, it store.Item, c store.Cond) error {
	if err := s.rg.mem.Put(ctx, s.table, it, c); err != nil {
		return err
	}
	stored, err := s.rg.mem.Get(ctx, s.table, keyOf(it), true)
	if err != nil {
		return err
	}
	s.rg.run.applied[tableKey{s.table, keyOf(stored)}]++
	s.emit(projector.Record{Op: projector.Insert, New: stored})
	s.rg.sweeper.expire(s.table, stored)
	for _, l := range s.rg.links {
		l.ship(shipment{table: s.table, item: stored})
	}
	return nil
}

// replicate applies a write shipped from another region as Global Tables do: the last
// writer wins. An item not present gives an INSERT. One present, from an append of the
// same DEDUP BY event in this region or a third, is replaced with a MODIFY by a later write,
// by ingest_time then origin_region; an earlier write is dropped, and so is any write of an
// item deleted here.
func (s *stream) replicate(it store.Item) {
	r := s.rg.run
	if s.deleted[keyOf(it)] {
		return
	}
	ctx := context.Background()
	cur, err := s.rg.mem.Get(ctx, s.table, keyOf(it), true)
	if err == nil && (cur == nil || later(it, cur)) {
		err = s.rg.mem.Put(ctx, s.table, it, store.Cond{})
	}
	switch {
	case err != nil:
		r.violatef("harness: %s: replication of %s: %v", s.rg.name, keyText(it), err)
		return
	case cur == nil:
		s.emit(projector.Record{Op: projector.Insert, New: it})
	case later(it, cur):
		r.count(&r.stats.Modifies)
		if r.isDivergent(tableKey{s.table, keyOf(it)}) {
			r.count(&r.stats.Divergent)
		}
		s.emit(projector.Record{Op: projector.Modify, Old: cur, New: it})
	default:
		return
	}
	s.rg.sweeper.expire(s.table, it)
}

// remove deletes an item, by a TTL delete here or one replicated from another region. It
// emits the item's REMOVE when the item is here, and keeps any later write of it out.
func (s *stream) remove(k store.Key) {
	r := s.rg.run
	if s.deleted[k] {
		return
	}
	s.deleted[k] = true
	ctx := context.Background()
	cur, err := s.rg.mem.Get(ctx, s.table, k, true)
	if err == nil && cur != nil {
		err = s.rg.mem.Delete(ctx, s.table, k)
	}
	switch {
	case err != nil:
		r.violatef("harness: %s: delete of %s %s: %v", s.rg.name, k.PK, k.SK, err)
	case cur != nil:
		r.count(&r.stats.Removes)
		r.expired[tableKey{s.table, k}] = true
		s.emit(projector.Record{Op: projector.Remove, Old: cur})
	}
}

// later reports whether a was written after b.
func later(a, b store.Item) bool {
	ta, tb := integer(a[catalog.AttrIngestTime]), integer(b[catalog.AttrIngestTime])
	return ta > tb || ta == tb && a[catalog.AttrOriginRegion].Str > b[catalog.AttrOriginRegion].Str
}

// emit gives rec the stream's next sequence number and adds it to the unit that holds its
// item's key.
func (s *stream) emit(rec projector.Record) {
	h := keyHash(recordKey(rec))
	u := s.unitFor(h)
	closeOnNext := s.rg.run.sc.Topology.CloseOnNext
	if closeOnNext && u.left == 0 {
		s.rollover(u)
		u = s.unitFor(h)
	}
	s.seq.Add(s.seq, big.NewInt(1+s.rg.run.topo.Int64N(maxSeqGap)))
	rec.Seq = s.seq.String()
	u.add(rec, h)
	if u.left--; u.left == 0 && !closeOnNext {
		s.rollover(u)
	}
}

func (s *stream) unitFor(h uint64) *unit {
	for _, u := range s.open {
		if u.covers(h) {
			return u
		}
	}
	panic("simulation: no open unit covers a key")
}

// rollover closes u and opens its child, or two children with half its keys each.
func (s *stream) rollover(u *unit) {
	u.close()
	s.open = slices.DeleteFunc(s.open, func(o *unit) bool { return o == u })
	topo := s.rg.run.sc.Topology
	if u.depth < topo.MaxDepth && chance(s.rg.run.topo, topo.Split) {
		s.newUnit(u.root, u.depth+1, u.prefix, u)
		s.newUnit(u.root, u.depth+1, u.prefix|1<<u.depth, u)
		return
	}
	s.newUnit(u.root, u.depth, u.prefix, u)
}

// newUnit opens a unit for the keys whose hash h has h mod Partitions = root and whose
// next depth bits are prefix, and starts a poller for each of its lanes.
func (s *stream) newUnit(root uint64, depth int, prefix uint64, parent *unit) {
	r := s.rg.run
	u := &unit{
		s:      s,
		id:     s.rg.unitID(),
		root:   root,
		depth:  depth,
		prefix: prefix,
		left:   1 + r.topo.IntN(r.sc.Topology.UnitRecords),
	}
	r.count(&r.stats.Units)
	s.rg.units[keys.UnitKey(u.id)] = u
	if s.listed {
		r.count(&r.stats.Unlisted)
	}
	for _, m := range s.rg.mappings {
		for part := range r.sc.ESM.PF {
			l := &lane{
				id:      fmt.Sprintf("%s %s %s/%d", s.rg.name, m.name, u.id, part),
				unit:    u,
				m:       m,
				part:    part,
				changed: make(chan struct{}),
				done:    make(chan struct{}),
			}
			if parent != nil {
				for _, p := range parent.lanes {
					if p.m == m {
						l.parents = append(l.parents, p)
					}
				}
			}
			u.lanes = append(u.lanes, l)
			r.sched.goActor(func() { m.poll(l) })
		}
	}
	s.units = append(s.units, u)
	s.open = append(s.open, u)
}

// unitID returns a new unit ID in the form DynamoDB Streams uses for shards.
func (rg *region) unitID() string {
	for {
		id := fmt.Sprintf("shardId-%020d-%08x", time.Now().UnixMilli(), rg.run.topo.Uint32())
		if !rg.unitIDs[id] {
			rg.unitIDs[id] = true
			return id
		}
	}
}

// unit is a delivery unit: a shard of the change stream.
type unit struct {
	s        *stream
	id       string
	root     uint64
	depth    int
	prefix   uint64
	left     int // records until it closes
	closed   bool
	recs     []projector.Record
	lanes    []*lane   // one per mapping and PF lane
	finished time.Time // when every lane delivered the closed unit
	quiet    time.Time // no lane takes a batch before it
}

func (u *unit) covers(h uint64) bool {
	parts := uint64(u.s.rg.run.sc.Topology.Partitions) //nolint:gosec // Scenario.check keeps it positive
	return h%parts == u.root && (h/parts)&(1<<u.depth-1) == u.prefix
}

func (u *unit) add(rec projector.Record, h uint64) {
	u.recs = append(u.recs, rec)
	pf := uint64(u.s.rg.run.sc.ESM.PF) //nolint:gosec // Scenario.check keeps it positive
	part := int((h >> partShift) % pf) //nolint:gosec // below PF
	for _, l := range u.lanes {
		if l.part == part {
			l.recs = append(l.recs, rec)
			l.at = append(l.at, time.Now())
			l.signal()
			u.s.rg.run.pending++
		}
	}
}

func (u *unit) close() {
	u.closed = true
	for _, l := range u.lanes {
		l.signal()
		l.finishIfDone()
	}
}

// lane is what one poller of a mapping delivers from a unit: all its records, or with a
// ParallelizationFactor above 1, those of one key hash class.
type lane struct {
	id          string
	unit        *unit
	m           *mapping
	part        int
	parents     []*lane // the parent unit's lanes of the same mapping
	recs        []projector.Record
	at          []time.Time // when each record entered the stream
	acked       int
	invocations int           // only the poller touches it
	changed     chan struct{} // closed and replaced when a record arrives or the unit closes
	done        chan struct{} // closed once the unit is closed and every record acknowledged
}

func (l *lane) signal() {
	close(l.changed)
	l.changed = make(chan struct{})
}

// wait returns what the lane's poller waits for: a parent to finish, or a record. It
// returns nil when the poller can deliver, or is done.
func (l *lane) wait() <-chan struct{} {
	for _, p := range l.parents {
		if !isClosed(p.done) {
			return p.done
		}
	}
	if l.acked == len(l.recs) && !l.unit.closed {
		return l.changed
	}
	return nil
}

func (l *lane) ack(n int) {
	l.acked += n
	l.unit.s.rg.run.settle(n)
	l.finishIfDone()
}

func (l *lane) finishIfDone() {
	if !l.unit.closed || l.acked < len(l.recs) || isClosed(l.done) {
		return
	}
	close(l.done)
	for _, o := range l.unit.lanes {
		if !isClosed(o.done) {
			return
		}
	}
	l.unit.finished = time.Now()
}

// link replicates one region's stream writes and deletes to another after a random delay,
// so replicas receive them in different orders. A cut holds them until it ends.
type link struct {
	id       string
	from, to *region
	queue    dueQueue[shipment]
	cuts     []cut
}

type shipment struct {
	due    time.Time
	table  string
	item   store.Item
	remove bool // a delete of item
}

func (sh shipment) dueAt() time.Time { return sh.due }

// cut is a time the link is down: from, inclusive, to to.
type cut struct{ from, to time.Time }

func newLink(from, to *region) *link {
	return &link{id: "link " + from.name + ">" + to.name, from: from, to: to, queue: newDueQueue[shipment]()}
}

// ship queues a write or a delete; only gated code calls it.
func (l *link) ship(sh shipment) {
	sh.due = time.Now().Add(l.delay(sh))
	l.queue.add(sh)
	l.from.run.pending++
}

// delay draws how long sh takes to land: as TimeSplit says, up to its landing bound after
// the write's ingest_time, or past it.
func (l *link) delay(sh shipment) time.Duration {
	r := l.from.run
	ts := r.sc.TimeSplit
	if ts.Landing == 0 || sh.remove {
		return between(r.topo, shortestLink, longestLink)
	}
	by := time.Until(time.UnixMicro(integer(sh.item[catalog.AttrIngestTime])).Add(ts.Landing))
	switch {
	case chance(r.topo, ts.Late):
		r.count(&r.stats.LateLandings)
		return max(shortestLink, by) + between(r.topo, 1, ts.LateMax)
	case ts.Spread:
		return between(r.topo, shortestLink, max(shortestLink, by))
	}
	return between(r.topo, shortestLink, longestLink)
}

func (l *link) run() {
	r := l.from.run
	for {
		r.sched.gate(l.id, "deliver")
		up := l.down(time.Now())
		if up.IsZero() {
			l.deliverDue()
		}
		var next time.Time
		if len(l.queue.jobs) > 0 {
			next = l.queue.jobs[0].due
			if up.After(next) {
				next = up
			}
		}
		if !l.queue.wait(next, r.stop) {
			return
		}
	}
}

// deliverDue applies the shipments due by now, the link being up.
func (l *link) deliverDue() {
	r, now, n := l.from.run, time.Now(), 0
	for ; n < len(l.queue.jobs) && !l.queue.jobs[n].due.After(now); n++ {
		sh := l.queue.jobs[n]
		if !l.down(sh.due).IsZero() {
			r.count(&r.stats.Held)
		} else if !sh.due.Equal(now) {
			r.violatef("harness: %s: a shipment arrived %v late, with the link up", l.id, now.Sub(sh.due))
		}
		if s := l.to.streams[sh.table]; sh.remove {
			s.remove(keyOf(sh.item))
		} else {
			s.replicate(sh.item)
		}
	}
	if n > 0 {
		l.queue.jobs = slices.Delete(l.queue.jobs, 0, n)
		r.settle(n)
	}
}

// down returns when the cut the link is in at now ends; zero when the link is up.
func (l *link) down(now time.Time) time.Time {
	for _, c := range l.cuts {
		if !now.Before(c.from) && now.Before(c.to) {
			return c.to
		}
	}
	return time.Time{}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

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
}

func newStream(rg *region, s Stream) *stream {
	// Every region gets each of the stream's n events, and each adds at least 1, so a
	// start more than maxSeqGap and at most n below 10^21 gives sequence numbers of 21
	// and of 22 digits, which compare wrongly unpadded, whenever n > maxSeqGap.
	n := int64(rg.run.counts[s.Table])
	lo := min(maxSeqGap+1, n)
	seq, _ := new(big.Int).SetString(firstWideSeq, 0)
	seq.Sub(seq, big.NewInt(lo+rg.run.topo.Int64N(n-lo+1)))
	return &stream{rg: rg, name: s.Name, table: s.Table, seq: seq}
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
	s.rg.run.applied[tableKey{s.table, keyOf(stored)}] = true
	s.emit(stored)
	for _, l := range s.rg.links {
		l.ship(s.table, stored)
	}
	return nil
}

// replicate applies a write shipped from another region. An item already present gives
// no record.
func (s *stream) replicate(it store.Item) {
	switch err := s.rg.mem.Put(context.Background(), s.table, it, store.IfAbsent()); {
	case err == nil:
		s.emit(it)
	case !errors.Is(err, store.ErrConditionFailed):
		s.rg.run.violatef("harness: %s: replication of %s: %v", s.rg.name, keyText(it), err)
	}
}

func (s *stream) emit(it store.Item) {
	h := keyHash(keyOf(it))
	u := s.unitFor(h)
	s.seq.Add(s.seq, big.NewInt(1+s.rg.run.topo.Int64N(maxSeqGap)))
	u.add(Record{Op: Insert, Seq: s.seq.String(), New: it}, h)
	if u.left--; u.left == 0 {
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
	recs     []Record
	lanes    []*lane   // one per mapping and PF lane
	finished time.Time // when every lane delivered the closed unit
}

func (u *unit) covers(h uint64) bool {
	parts := uint64(u.s.rg.run.sc.Topology.Partitions) //nolint:gosec // Scenario.check keeps it positive
	return h%parts == u.root && (h/parts)&(1<<u.depth-1) == u.prefix
}

func (u *unit) add(rec Record, h uint64) {
	u.recs = append(u.recs, rec)
	pf := uint64(u.s.rg.run.sc.ESM.PF) //nolint:gosec // Scenario.check keeps it positive
	part := int((h >> partShift) % pf) //nolint:gosec // below PF
	for _, l := range u.lanes {
		if l.part == part {
			l.recs = append(l.recs, rec)
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
	recs        []Record
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

// link replicates one region's stream writes to another after a random delay, so
// replicas receive them in different orders.
type link struct {
	id       string
	from, to *region
	queue    []shipment // by due time
	wake     chan struct{}
}

type shipment struct {
	due   time.Time
	table string
	item  store.Item
}

func newLink(from, to *region) *link {
	return &link{id: "link " + from.name + ">" + to.name, from: from, to: to, wake: make(chan struct{}, 1)}
}

// ship queues a write; only gated code calls it.
func (l *link) ship(table string, it store.Item) {
	due := time.Now().Add(between(l.from.run.topo, shortestLink, longestLink))
	i, _ := slices.BinarySearchFunc(l.queue, due, func(s shipment, t time.Time) int {
		if s.due.After(t) {
			return 1
		}
		return -1 // after every shipment due at the same time
	})
	l.queue = slices.Insert(l.queue, i, shipment{due, table, it})
	l.from.run.pending++
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *link) run() {
	r := l.from.run
	for {
		r.sched.gate(l.id, "deliver")
		n := 0
		for n < len(l.queue) && !l.queue[n].due.After(time.Now()) {
			l.to.streams[l.queue[n].table].replicate(l.queue[n].item)
			n++
		}
		if n > 0 {
			l.queue = slices.Delete(l.queue, 0, n)
			r.settle(n)
		}
		if len(l.queue) > 0 {
			time.Sleep(time.Until(l.queue[0].due))
			continue
		}
		if r.stopped() {
			return
		}
		select {
		case <-l.wake:
		case <-r.stop:
		}
	}
}

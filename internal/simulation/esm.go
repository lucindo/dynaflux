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
)

const (
	retryBackoff  = time.Second // before the mapping retries a failed batch
	stallSpread   = 4           // a slow invocation stalls one of its first stallSpread requests per record
	stallFraction = 4           // a stalled response returns up to Timeout/stallFraction before the deadline
)

var (
	errTimeout = errors.New("simulation: invocation timed out")
	errCrashed = errors.New("simulation: invocation crashed")
)

// mapping is one event source mapping of a region: a poller per lane, one batch at a
// time, retrying a batch until it succeeds.
type mapping struct {
	rg    *region
	name  string
	start time.Time
}

// poll delivers a lane: after its parents, batch by batch, until the unit is closed and
// delivered or the run stops.
func (m *mapping) poll(l *lane) {
	r := m.rg.run
	rng := r.actorRNG(l.id)
	time.Sleep(time.Until(m.start))
	for {
		r.sched.gate(l.id, "poll")
		if r.stopped() {
			return
		}
		if wait := l.wait(); wait != nil {
			select {
			case <-wait:
			case <-r.stop:
			}
			continue
		}
		if l.acked == len(l.recs) {
			return
		}
		time.Sleep(between(rng, 0, r.sc.ESM.Window))
		r.sched.gate(l.id, "take")
		if r.stopped() {
			return
		}
		end := min(l.acked+1+rng.IntN(r.sc.ESM.MaxBatch), len(l.recs))
		if n := m.deliver(l, rng, l.recs[l.acked:end]); n > 0 {
			r.sched.gate(l.id, "ack")
			l.ack(n)
			continue
		}
		time.Sleep(retryBackoff)
	}
}

// deliver invokes a batch, with the extra deliveries the scenario draws, and returns how
// many of its records the mapping may checkpoint.
func (m *mapping) deliver(l *lane, rng *rand.Rand, recs []Record) int {
	r := m.rg.run
	e := r.sc.ESM
	if chance(rng, e.Stale) {
		r.count(&r.stats.Stale)
		id := l.nextID() + " stale"
		r.sched.goActor(func() { m.invoke(l, id, r.actorRNG(id), recs) })
	}
	if chance(rng, e.Empty) {
		r.count(&r.stats.Empty)
		m.invoke(l, l.nextID(), rng, nil)
	}
	o := m.invoke(l, l.nextID(), rng, recs)
	switch {
	case o.err != nil:
		return 0
	case o.res.FailedSeq == "":
		if chance(rng, e.Dup) {
			r.count(&r.stats.Dups)
			m.invoke(l, l.nextID(), rng, recs)
		}
		return len(recs)
	}
	i := slices.IndexFunc(recs, func(rec Record) bool { return rec.Seq == o.res.FailedSeq })
	if i < 0 {
		r.violatef("ESM: %s returned FailedSeq %s, which isn't in its batch", l.id, o.res.FailedSeq)
		return 0
	}
	return i
}

func (l *lane) nextID() string {
	l.invocations++
	return l.id + " #" + strconv.Itoa(l.invocations)
}

type outcome struct {
	res Result
	err error
}

// call is one Handle in flight; out is set before finished closes.
type call struct {
	finished chan struct{}
	out      outcome
}

// invoke runs one invocation on a warm container and waits for its result or its
// deadline, whichever comes first.
func (m *mapping) invoke(l *lane, id string, rng *rand.Rand, recs []Record) outcome {
	rg := m.rg
	r := rg.run
	e := r.sc.ESM
	core := rg.acquire(id, rng)
	inv := &invocation{id: id, deadline: time.Now().Add(e.Timeout), empty: len(recs) == 0, stallAt: -1}
	if len(recs) > 0 && chance(rng, e.Slow) {
		inv.stallAt = rng.IntN(stallSpread * len(recs))
		inv.stall = between(rng, -time.Millisecond, e.Timeout/stallFraction)
	}
	b := Batch{Stream: l.unit.s.name, Unit: l.unit.id, Records: make([]Record, len(recs))}
	for i, rec := range recs {
		b.Records[i] = cloneRecord(rec)
	}
	ctx, cancel := context.WithDeadline(withInvocation(context.Background(), inv), inv.deadline)
	defer cancel()
	c := &call{finished: make(chan struct{})}
	r.count(&r.stats.Invocations)
	r.sched.goActor(func() { r.handle(ctx, inv, core, b, c) })
	timer := time.NewTimer(time.Until(inv.deadline))
	defer timer.Stop()
	select {
	case <-c.finished:
	case <-timer.C:
	}
	// Both ways meet at this gate, so a result in the deadline's instant has an order the
	// seed fixes.
	r.sched.gate(id, "end")
	if !isClosed(c.finished) {
		// The invocation is dead: the gates refuse whatever it sends from now on.
		r.count(&r.stats.Timeouts)
		return outcome{err: errTimeout}
	}
	if !errors.Is(c.out.err, errCrashed) {
		rg.pool = append(rg.pool, core)
	}
	return c.out
}

// handle runs Handle as one invocation. A crash, a kill or a panic leaves errCrashed; a
// panic is also a bug (P8). A crash or a kill on a goroutine Handle started ends only that
// goroutine, so Handle may still return; the invocation died all the same.
func (r *run) handle(ctx context.Context, inv *invocation, core Core, b Batch, c *call) {
	c.out.err = errCrashed
	killed := true // until Handle returns or panics: the gates ended its goroutine
	defer func() {
		if p := recover(); p != nil {
			killed = false
			r.violatef("P8: %s panicked: %v", inv.id, p)
		}
		if killed {
			// The whole invocation died, so its other goroutines send nothing more.
			inv.kill()
		} else {
			inv.finish()
		}
		close(c.finished)
	}()
	res, err := core.Handle(ctx, b)
	killed = false
	if !inv.isDead() {
		c.out = outcome{res: res, err: err}
	}
}

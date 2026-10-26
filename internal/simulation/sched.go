// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing/synctest"
)

// RNG streams. Actors draw from streams of their own, keyed by their names, so adding an
// actor shifts no other actor's draws.
const (
	streamEvents uint64 = iota
	streamTopology
	streamFaults
	streamSched
	streamArchive
)

// sched releases gated goroutines one at a time, in an order drawn from a seeded RNG,
// only once every other goroutine of the bubble is durably blocked. A released goroutine
// therefore runs alone until it blocks again, and state that only gated code touches is
// changed in an order the seed fixes.
type sched struct {
	arrive chan ticket
	done   chan struct{} // closed when the last actor exits
	closed chan struct{} // closed when run returns: a later gate ends its goroutine
	rng    *rand.Rand
	ready  []ticket
	steps  int64
	trace  uint64
	report func(format string, args ...any) // records a violation

	mu   sync.Mutex
	live int
}

type ticket struct {
	actor, op string
	resume    chan struct{}
}

func newSched(seed uint64, report func(format string, args ...any)) *sched {
	return &sched{
		arrive: make(chan ticket),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
		rng:    newRand(seed, streamSched),
		trace:  fnvOffset,
		report: report,
	}
}

// gate parks the caller until the scheduler releases it, and reports whether it did.
// Tickets of one instant are ordered by actor and op before the draw, so their arrival
// order doesn't matter. Once the run is over gate returns false at once: only goroutines
// the code under test started outlive the actors, and they reach gates through enter.
func (s *sched) gate(actor, op string) bool {
	t := ticket{actor: actor, op: op, resume: make(chan struct{})}
	select {
	case s.arrive <- t:
		<-t.resume
		return true
	case <-s.closed:
		return false
	}
}

// goActor starts an actor; run returns once every actor has exited.
func (s *sched) goActor(f func()) {
	s.mu.Lock()
	s.live++
	s.mu.Unlock()
	go func() {
		defer s.exit()
		f()
	}()
}

func (s *sched) exit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.live--; s.live == 0 {
		close(s.done)
	}
}

// run must be called from the bubble's root goroutine, after the first actor started.
func (s *sched) run() {
	defer close(s.closed)
	for {
		synctest.Wait()
		s.drain()
		if len(s.ready) > 0 {
			s.release()
			continue
		}
		if isClosed(s.done) {
			return
		}
		// Everyone sleeps or waits: block, so the fake clock moves to the next timer.
		select {
		case t := <-s.arrive:
			s.add(t)
		case <-s.done:
		}
	}
}

func (s *sched) drain() {
	for {
		select {
		case t := <-s.arrive:
			s.add(t)
		default:
			return
		}
	}
}

// add queues an arrived ticket. Of two equal tickets the runtime, not the seed, picks the
// goroutine the draw releases, so a run with them can't replay.
func (s *sched) add(t ticket) {
	if slices.ContainsFunc(s.ready, func(o ticket) bool { return o.actor == t.actor && o.op == t.op }) {
		s.report("D3: %s has two identical requests in flight: %s", t.actor, t.op)
	}
	s.ready = append(s.ready, t)
}

func (s *sched) release() {
	slices.SortFunc(s.ready, func(a, b ticket) int {
		return cmp.Or(strings.Compare(a.actor, b.actor), strings.Compare(a.op, b.op))
	})
	i := s.rng.IntN(len(s.ready))
	t := s.ready[i]
	s.ready = slices.Delete(s.ready, i, i+1)
	s.steps++
	s.trace = fnv(fnv(s.trace, t.actor), t.op)
	close(t.resume)
}

func isClosed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

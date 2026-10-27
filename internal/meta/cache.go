// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"
)

// StateCache holds a meta table's view states, for the projector and for queries. It is safe
// for concurrent use.
type StateCache struct {
	mu      sync.Mutex
	m       *Meta
	states  map[string]ViewState // nil until read
	at      time.Time            // when the read that returned states was sent
	reading *stateRead           // the read in flight that callers share; nil when none
}

// stateRead is one read of the view states; done closes when it ends.
type stateRead struct {
	done   chan struct{}
	states map[string]ViewState
	err    error
}

// NewStateCache returns an empty cache of m's view states.
func NewStateCache(m *Meta) *StateCache { return &StateCache{m: m} }

// States returns the view states, read at most [MaxViewStateAge] ago, or read now when fresh
// is set. The age is on the wall clock, since the monotonic one may stop while Lambda freezes
// the environment; a negative age, from a clock that stepped back, counts as expired.
// Callers that find the states expired share one read, and its error when it fails. The map
// is shared: callers don't change it.
func (c *StateCache) States(ctx context.Context, fresh bool) (map[string]ViewState, error) {
	for {
		c.mu.Lock()
		if age := time.Since(c.at); !fresh && c.states != nil && age >= 0 && age < MaxViewStateAge {
			states := c.states
			c.mu.Unlock()
			return states, nil
		}
		rd := c.reading
		if rd == nil || fresh { // a fresh caller can't use a read sent before it called
			rd = &stateRead{done: make(chan struct{})}
			if c.reading == nil {
				c.reading = rd
			}
			c.mu.Unlock()
			c.read(ctx, rd)
			return rd.states, rd.err
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-rd.done:
		}
		// A read that its own caller cancelled says nothing about the states: read again.
		if ctx.Err() == nil && (errors.Is(rd.err, context.Canceled) || errors.Is(rd.err, context.DeadlineExceeded)) {
			continue
		}
		return rd.states, rd.err
	}
}

func (c *StateCache) read(ctx context.Context, rd *stateRead) {
	sent := time.Now().Round(0)
	rd.states, rd.err = c.m.ViewStates(ctx)
	c.mu.Lock()
	if rd.err == nil {
		// A breaker opened, or a version retired, while the read was in flight stays so.
		for version, vs := range c.states {
			if finality(rd.states[version].State) < finality(vs.State) {
				rd.states[version] = vs
			}
		}
		c.states, c.at = rd.states, sent
	}
	if c.reading == rd {
		c.reading = nil
	}
	c.mu.Unlock()
	close(rd.done)
}

// Set puts vs, a state that a write just stored, in the cached states, so that callers act
// on it before the states expire: a broken or retired version never applies again. A
// cached state that vs can't follow is kept. Before the first read it does nothing: that
// read finds the stored state.
func (c *StateCache) Set(vs ViewState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.states == nil || finality(vs.State) < finality(c.states[vs.Version].State) {
		return
	}
	states := maps.Clone(c.states) // callers hold the old map
	states[vs.Version] = vs
	c.states = states
}

// finality orders the states that a version leaves only for a later one: Broken only for
// Retired, and Retired never. Other states rank 0.
func finality(s State) int { return slices.Index([]State{Broken, Retired}, s) + 1 }

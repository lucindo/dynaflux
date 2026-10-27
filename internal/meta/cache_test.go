// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// slowQueries counts queries, each of which takes delay and fails with err when it is set.
type slowQueries struct {
	store.Backend

	delay   time.Duration
	mu      sync.Mutex
	queries int
	err     error
}

func (b *slowQueries) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	b.mu.Lock()
	b.queries++
	err := b.err
	b.mu.Unlock()
	return func(yield func(store.Item, error) bool) {
		select {
		case <-ctx.Done():
			yield(nil, ctx.Err())
			return
		case <-time.After(b.delay):
		}
		if err != nil {
			yield(nil, err)
			return
		}
		for it, err := range b.Backend.Query(ctx, table, q) {
			if !yield(it, err) {
				return
			}
		}
	}
}

func (b *slowQueries) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.queries
}

func (b *slowQueries) fail(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = err
}

// newCache returns a cache over b, and b's meta table to change states behind the cache.
func newCache(b *slowQueries) (*meta.StateCache, *meta.Meta) {
	return meta.NewStateCache(meta.New(b, table)), meta.New(b.Backend, table)
}

func cachedState(t *testing.T, c *meta.StateCache, fresh bool) meta.State {
	t.Helper()
	states, err := c.States(t.Context(), fresh)
	if err != nil {
		t.Fatalf("States: %v", err)
	}
	return states[version].State
}

func TestStateCacheKeepsStatesForMaxAge(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := &slowQueries{Backend: mem.New()}
		c, m := newCache(b)
		cachedState(t, c, false)
		activate(t, m)
		time.Sleep(meta.MaxViewStateAge - time.Microsecond)
		if got := cachedState(t, c, false); got != "" || b.count() != 1 {
			t.Errorf("before the states expire: %q after %d reads, want the cached absent state after 1", got, b.count())
		}
		time.Sleep(time.Microsecond)
		if got := cachedState(t, c, false); got != meta.Activating || b.count() != 2 {
			t.Errorf("once they expire: %q after %d reads, want activating after 2", got, b.count())
		}
	})
}

func TestStateCacheFreshReadsAgain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := &slowQueries{Backend: mem.New()}
		c, m := newCache(b)
		cachedState(t, c, false)
		activate(t, m)
		if got := cachedState(t, c, true); got != meta.Activating || b.count() != 2 {
			t.Errorf("fresh: %q after %d reads, want activating after 2", got, b.count())
		}
		if got := cachedState(t, c, false); got != meta.Activating || b.count() != 2 {
			t.Errorf("cached after a fresh read: %q after %d reads, want activating after 2", got, b.count())
		}
	})
}

// callers calls c.States from n goroutines at once and returns their results.
func callers(ctx context.Context, c *meta.StateCache, n int) ([]meta.State, []error) {
	states, errs := make([]meta.State, n), make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			got, err := c.States(ctx, false)
			states[i], errs[i] = got[version].State, err
		})
	}
	wg.Wait()
	return states, errs
}

func TestStateCacheSharesOneRead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := &slowQueries{Backend: mem.New(), delay: time.Second}
		c, m := newCache(b)
		activate(t, m)
		states, errs := callers(t.Context(), c, 5)
		for i := range states {
			if states[i] != meta.Activating || errs[i] != nil {
				t.Errorf("caller %d: %q, %v; want activating", i, states[i], errs[i])
			}
		}
		if b.count() != 1 {
			t.Errorf("%d reads, want 1", b.count())
		}
	})
}

var errRead = errors.New("read failed")

// A failed read fails every caller that waited for it, and isn't cached.
func TestStateCacheSharesAFailedRead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := &slowQueries{Backend: mem.New(), delay: time.Second, err: errRead}
		c, m := newCache(b)
		activate(t, m)
		_, errs := callers(t.Context(), c, 3)
		for i, err := range errs {
			if !errors.Is(err, errRead) {
				t.Errorf("caller %d: %v, want the read's error", i, err)
			}
		}
		b.fail(nil)
		if got := cachedState(t, c, false); got != meta.Activating || b.count() != 2 {
			t.Errorf("after the failure: %q after %d reads, want activating after 2", got, b.count())
		}
	})
}

// A caller whose own context ended doesn't fail the callers that waited for its read.
func TestStateCacheWaiterReadsAgainWhenTheReaderGaveUp(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := &slowQueries{Backend: mem.New(), delay: time.Second}
		c, m := newCache(b)
		activate(t, m)
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		var readerErr error
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, readerErr = c.States(ctx, false)
		}()
		time.Sleep(100 * time.Millisecond)
		got := cachedState(t, c, false)
		<-done
		if !errors.Is(readerErr, context.DeadlineExceeded) || got != meta.Activating || b.count() != 2 {
			t.Errorf("reader %v, waiter %q after %d reads; want the deadline, and activating after 2", readerErr, got, b.count())
		}
	})
}

// Set puts a stored state in the cache, failure and all, but never moves a version back out of
// Broken or Retired.
func TestStateCacheSet(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := &slowQueries{Backend: mem.New()}
		c, m := newCache(b)
		activate(t, m)
		broken, _, err := m.MarkBroken(t.Context(), version, stream, failure())
		if err != nil {
			t.Fatal(err)
		}
		c.Set(broken) // before the first read: that read finds the stored state
		before, err := c.States(t.Context(), false)
		if err != nil || before[version].State != meta.Broken {
			t.Fatalf("first read: %v, %v; want broken", before, err)
		}
		retired, err := m.Retire(t.Context(), version, stream)
		if err != nil {
			t.Fatal(err)
		}
		c.Set(retired)
		c.Set(broken) // a breaker that a later write found retired
		c.Set(meta.ViewState{Version: version, State: meta.Live})
		after, err := c.States(t.Context(), false)
		if got := after[version]; err != nil || got.State != meta.Retired || got.Failure == nil || got.Failure.Error != "boom" || b.count() != 1 {
			t.Errorf("after Set: %+v, %v after %d reads; want retired with the failure after 1", got, err, b.count())
		}
		if before[version].State != meta.Broken {
			t.Errorf("Set changed a map a caller holds: %v", before)
		}
	})
}

// apiError is an SDK API error as meta sees it: an error with an error code.
type apiError struct{ code string }

func (e apiError) Error() string     { return "api error " + e.code }
func (e apiError) ErrorCode() string { return e.code }

func TestStateCacheNamesTheMissingGrant(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code  string
		grant bool
	}{
		{"AccessDeniedException", true},
		{"ThrottlingException", false},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			t.Parallel()
			b := &slowQueries{Backend: mem.New(), err: fmt.Errorf("operation error: %w", apiError{tt.code})}
			c, _ := newCache(b)
			_, err := c.States(t.Context(), false)
			if !errors.As(err, new(apiError)) {
				t.Fatalf("States = %v, want the API error", err)
			}
			if named := strings.Contains(err.Error(), "dynamodb:Query on table "+table); named != tt.grant {
				t.Errorf("States = %v; grant named %t, want %t", err, named, tt.grant)
			}
		})
	}
}

// A read that misses a breaker opened or a version retired while it was in flight keeps the
// version so: nothing leaves Retired, and Broken leaves only for Retired.
func TestStateCacheKeepsAStateSetDuringARead(t *testing.T) {
	t.Parallel()
	f := failure()
	tests := []struct {
		name          string
		retiredBefore bool // the store holds the version retired when the read is sent
		set           meta.ViewState
		want          meta.State
	}{
		{"broken during the read", false, meta.ViewState{Version: version, Stream: stream, State: meta.Broken, Failure: &f}, meta.Broken},
		{"retired during the read", false, meta.ViewState{Version: version, Stream: stream, State: meta.Retired}, meta.Retired},
		{"broken, then a read finds it retired", true, meta.ViewState{Version: version, Stream: stream, State: meta.Broken, Failure: &f}, meta.Retired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				b := &slowQueries{Backend: mem.New(), delay: time.Second}
				c, m := newCache(b)
				activate(t, m)
				cachedState(t, c, false)
				if tt.retiredBefore {
					retire(t, m)
				}
				time.Sleep(meta.MaxViewStateAge)
				var reader meta.State
				done := make(chan struct{})
				go func() {
					defer close(done)
					reader = cachedState(t, c, false)
				}()
				time.Sleep(time.Second / 2)
				c.Set(tt.set) // stored by a write that the read in flight doesn't see
				<-done
				if got := cachedState(t, c, false); reader != tt.want || got != tt.want || b.count() != 2 {
					t.Errorf("reader %q, then cached %q after %d reads; want %q after 2", reader, got, b.count(), tt.want)
				}
			})
		})
	}
}

// The cached states' age is on the wall clock: states stamped an hour ahead, as after the
// clock stepped back, are read again. The stamp keeps no monotonic reading, which stops while
// Lambda freezes the environment. The test runs outside synctest, whose clock has no
// monotonic reading.
func TestStateCacheRereadsStatesStampedAhead(t *testing.T) {
	t.Parallel()
	b := &slowQueries{Backend: mem.New()}
	c, m := newCache(b)
	cachedState(t, c, false)
	activate(t, m)
	c.SetAt(time.Now().Add(time.Hour).Round(0))
	if got := cachedState(t, c, false); got != meta.Activating || b.count() != 2 {
		t.Errorf("states stamped ahead: %q after %d reads, want activating after 2", got, b.count())
	}
	if at := c.At(); at != at.Round(0) {
		t.Errorf("the states are stamped %v, with a monotonic reading", at)
	}
}

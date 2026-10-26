// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys_test

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/lucindo/dynaflux/internal/keys"
)

func nextID(t *testing.T, g *keys.ULIDs, ms int64) string {
	t.Helper()
	id, err := g.Next(ms)
	if err != nil {
		t.Fatalf("Next(%d): %v", ms, err)
	}
	return id
}

// Within one millisecond each ULID adds 1 to the previous one and reads no entropy.
func TestULIDsMonotonicWithinAMillisecond(t *testing.T) {
	t.Parallel()
	g := keys.NewULIDs(bytes.NewReader([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}))
	const ms = 1_717_163_100_250
	for _, want := range []string{"01HZ7EB22T000G40R40M30E209", "01HZ7EB22T000G40R40M30E20A"} {
		if got := nextID(t, g, ms); got != want {
			t.Errorf("Next(%d) = %q, want %q", ms, got, want)
		}
	}
}

// A carry out of the random part moves into the time part, which keeps the order.
func TestULIDsCarryIntoTime(t *testing.T) {
	t.Parallel()
	g := keys.NewULIDs(bytes.NewReader(bytes.Repeat([]byte{0xFF}, 10)))
	a, b := nextID(t, g, 1), nextID(t, g, 1)
	if a != "0000000001ZZZZZZZZZZZZZZZZ" || b != "00000000020000000000000000" {
		t.Errorf("IDs = %q, %q", a, b)
	}
}

func TestULIDsKeepOrderWhenTheClockStepsBack(t *testing.T) {
	t.Parallel()
	g := keys.NewULIDs(nil)
	a := nextID(t, g, 5_000)
	b := nextID(t, g, 4_000)
	c := nextID(t, g, 6_000)
	if a >= b || b >= c {
		t.Errorf("IDs out of order: %q, %q, %q", a, b, c)
	}
	if c[:10] != "00000005VG" { // 6000 ms
		t.Errorf("a later millisecond takes its own time: %q", c)
	}
}

func TestULIDsEntropyFailureKeepsState(t *testing.T) {
	t.Parallel()
	g := keys.NewULIDs(io.MultiReader(bytes.NewReader(make([]byte, 10)), strings.NewReader("abc")))
	first := nextID(t, g, 1)
	if _, err := g.Next(2); err == nil {
		t.Fatal("Next with short entropy: want an error")
	}
	if got := nextID(t, g, 1); got != "00000000010000000000000001" || got <= first {
		t.Errorf("after the failure Next(1) = %q, want the successor of %q", got, first)
	}
}

func TestULIDsRejectTimesOutsideTheRange(t *testing.T) {
	t.Parallel()
	g := keys.NewULIDs(nil)
	for _, ms := range []int64{-1, timeLimitMicros / 1000} {
		if id, err := g.Next(ms); err == nil {
			t.Errorf("Next(%d) = %q, want an error", ms, id)
		}
	}
}

func TestULIDsConcurrentCallsAreDistinct(t *testing.T) {
	t.Parallel()
	g := keys.NewULIDs(nil)
	const workers, each = 8, 500
	ids := make(chan string, workers*each)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range each {
				id, err := g.Next(1_717_163_100_250)
				if err != nil {
					t.Error(err)
					return
				}
				ids <- id
			}
		})
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate ID %q", id)
		}
		seen[id] = true
	}
	if len(seen) != workers*each {
		t.Errorf("%d IDs, want %d", len(seen), workers*each)
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/lucindo/dynaflux/internal/catalog"
)

const (
	ulidTimeBytes   = 6  // 48-bit Unix milliseconds
	ulidRandomBytes = 10 // 80 random bits
	timeBitsInWord  = 16 // the bits of a uint64 below a ULID's 48-bit time
	ulidTimeLimit   = catalog.TimeLimitMicros / microsPerMilli
)

// ULIDs makes the event_ids of one writer's appends to streams without DEDUP BY: ULIDs,
// monotonic as the ULID spec describes (encoding.md §4.1). It is safe for concurrent use.
type ULIDs struct {
	mu      sync.Mutex
	entropy io.Reader
	last    [eventHashLen]byte // big-endian: the time, then the random part
	started bool
}

// NewULIDs returns a ULID maker that reads its random bits from entropy, or from
// [crypto/rand.Reader] when entropy is nil.
func NewULIDs(entropy io.Reader) *ULIDs {
	if entropy == nil {
		entropy = rand.Reader
	}
	return &ULIDs{entropy: entropy}
}

// Next returns a new event_id at the writer's clock reading ms, in Unix milliseconds. Each
// one sorts after the one before: within one millisecond it adds 1 to the previous ID.
func (g *ULIDs) Next(ms int64) (string, error) {
	if ms < 0 || ms >= ulidTimeLimit {
		return "", fmt.Errorf("ULID time %d ms is outside 1970 to 2106", ms)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	lastMS := int64(binary.BigEndian.Uint64(g.last[:8]) >> timeBitsInWord)
	// A clock that steps back also increments, so one writer's IDs keep their order.
	if g.started && ms <= lastMS {
		increment(g.last[:])
		return string(appendBase32(make([]byte, 0, eventIDLen), g.last[:])), nil
	}
	var random [ulidRandomBytes]byte
	if _, err := io.ReadFull(g.entropy, random[:]); err != nil {
		return "", fmt.Errorf("ULID entropy: %w", err)
	}
	binary.BigEndian.PutUint64(g.last[:8], uint64(ms)<<timeBitsInWord)
	copy(g.last[ulidTimeBytes:], random[:])
	g.started = true
	return string(appendBase32(make([]byte, 0, eventIDLen), g.last[:])), nil
}

// increment adds 1 to b, a big-endian number. Where the ULID spec fails on a carry out of the
// random part, the carry moves into the time part and keeps the order.
func increment(b []byte) {
	for i := len(b) - 1; i >= 0; i-- {
		b[i]++
		if b[i] != 0 {
			return
		}
	}
}

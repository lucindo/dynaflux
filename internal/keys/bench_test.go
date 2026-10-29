// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// BenchmarkKey encodes the key of a page_daily row: a String and a DateTime, the shape of
// the example's views.
func BenchmarkKey(b *testing.B) {
	ts := []catalog.Type{tString, tDT}
	vs := []expr.Value{expr.String("/pricing/team plan"), expr.Micros(1_717_160_400_000_000)}
	for b.Loop() {
		if _, err := keys.Key(ts, vs); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStreamSK encodes a stream item's sort key, once per append.
func BenchmarkStreamSK(b *testing.B) {
	for b.Loop() {
		if _, err := keys.StreamSK(may31at, "01HZ3V8Q9W5N7X2K4M6P8R0T1Y"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEventID makes one event_id, once per append to a stream without DEDUP BY.
func BenchmarkEventID(b *testing.B) {
	g := keys.NewULIDs(rand.NewChaCha8([32]byte{}))
	ms := may31at / 1000
	for b.Loop() {
		if _, err := g.Next(ms); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHWM decodes a row's high-water marks, advances one unit and encodes them again:
// what each row write of the projector does. The sizes bracket explain's ~20 shards at
// 1,000 events/s and 2 days of LOG RETENTION.
func BenchmarkHWM(b *testing.B) {
	for _, n := range []int{1, 20, 100} {
		b.Run(fmt.Sprintf("units=%d", n), func(b *testing.B) {
			stored := benchHWM(b, n)
			unit := keys.UnitKey("shardId-00000000")
			next, err := keys.PadSeq("4421584599999999999999999999")
			if err != nil {
				b.Fatal(err)
			}
			for b.Loop() {
				h, derr := keys.DecodeHWM(stored, true)
				if derr != nil {
					b.Fatal(derr)
				}
				h.Units[unit] = next
				if _, err = h.Encode(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// benchHWM returns the _hwm attribute of a row with marks of n units.
func benchHWM(b *testing.B, n int) store.Value {
	b.Helper()
	h := keys.HWM{Units: make(map[string]string, n)}
	for i := range n {
		seq, err := keys.PadSeq(fmt.Sprintf("4421584500000000%012d", i))
		if err != nil {
			b.Fatal(err)
		}
		h.Units[keys.UnitKey(fmt.Sprintf("shardId-%08d", i))] = seq
	}
	stored, err := h.Encode()
	if err != nil {
		b.Fatal(err)
	}
	return stored
}

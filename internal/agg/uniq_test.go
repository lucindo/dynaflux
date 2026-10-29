// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

const registers = 4096

// addHash sets the register of a 64-bit hash as encoding.md §7.5 describes.
func addHash(r *[registers]uint8, h uint64) {
	i := h >> 52
	rank := uint8(min(bits.LeadingZeros64(h<<12)+1, 31))
	r[i] = max(r[i], rank)
}

// sketchBytes encodes registers as encoding.md §7.5 describes, bit by bit.
func sketchBytes(r *[registers]uint8) []byte {
	sparse := []byte{0x01}
	var prev uint64
	for i, x := range r {
		if x != 0 {
			v := uint64(i)<<5 | uint64(x)
			sparse = binary.AppendUvarint(sparse, v-prev)
			prev = v
		}
	}
	const denseLen = 2561
	if len(sparse) < denseLen {
		return sparse
	}
	dense := make([]byte, denseLen)
	dense[0] = 0x02
	for i, x := range r {
		for b := range 5 {
			if x>>b&1 == 1 {
				j := 5*i + b
				dense[1+j/8] |= 1 << (j % 8)
			}
		}
	}
	return dense
}

// TestUniqMatchesSpec checks stored sketches against an implementation of encoding.md
// §4.4 and §7.5 written from the text: the hash of the canonical bytes, the registers, and
// the sparse and dense forms.
func TestUniqMatchesSpec(t *testing.T) {
	t.Parallel()
	f := compile(t, spec(catalog.AggUniq, nil, typ(catalog.KindInt64)))
	for _, n := range []int{1, 10, 500, 2000, 4000, 20000} {
		s := f.New()
		var want [registers]uint8
		for i := range n {
			v := int64(i)*7919 - 5000
			if err := s.Add(input(expr.Int(v))); err != nil {
				t.Fatal(err)
			}
			h := sha256.Sum256(binary.BigEndian.AppendUint64(nil, uint64(v)))
			addHash(&want, binary.BigEndian.Uint64(h[:8]))
		}
		if got := encode(t, s); !bytes.Equal(got.Bin, sketchBytes(&want)) {
			t.Fatalf("n = %d: Encode = %x…, want %x…", n, got.Bin[:min(16, len(got.Bin))], sketchBytes(&want)[:16])
		}
	}
}

// TestUniqGolden pins sketches computed outside Go from the spec.
func TestUniqGolden(t *testing.T) {
	t.Parallel()
	cases := []struct {
		t      catalog.Type
		values []expr.Value
		want   store.Value
	}{
		{typ(catalog.KindUInt64), []expr.Value{expr.Uint(1)}, bin(0x01, 0xc2, 0xb4, 0x06)},
		{typ(catalog.KindInt64), []expr.Value{expr.Int(-1)}, bin(0x01, 0xc3, 0x4a)},
		{typ(catalog.KindString), []expr.Value{expr.String("a"), expr.String("b"), expr.String("c"), expr.String("a")},
			bin(0x01, 0xe1, 0xb9, 0x01, 0xe2, 0x3e, 0xdf, 0xb1, 0x04)},
	}
	for _, c := range cases {
		f := compile(t, spec(catalog.AggUniq, nil, c.t))
		s := f.New()
		for _, v := range c.values {
			if err := s.Add(input(v)); err != nil {
				t.Fatal(err)
			}
		}
		if got := encode(t, s); !reflect.DeepEqual(got, c.want) {
			t.Fatalf("uniq of %v: Encode = %x, want %x", c.t, got.Bin, c.want.Bin)
		}
	}
}

// TestUniqFormSwitch checks the switch from sparse to dense at its boundary. Registers 0 to
// k − 1 at rank 1 take 1 + k sparse bytes (deltas 1, then 32), so k = 2,559 stays sparse, and
// k = 2,560 is as long as the dense form, so it is stored dense.
func TestUniqFormSwitch(t *testing.T) {
	t.Parallel()
	f := compile(t, spec(catalog.AggUniq, nil, typ(catalog.KindUInt64)))
	var regs [registers]uint8
	for i := range 2559 {
		regs[i] = 1
	}
	s := decode(t, f, bin(sketchBytes(&regs)...))
	if got := encode(t, s).Bin; got[0] != 0x01 || len(got) != 2560 {
		t.Fatalf("2,559 registers: form %#x of %d bytes, want 0x01 of 2,560", got[0], len(got))
	}
	var last [registers]uint8
	last[2559] = 1
	if err := s.Merge(decode(t, f, bin(sketchBytes(&last)...))); err != nil {
		t.Fatal(err)
	}
	regs[2559] = 1
	got := encode(t, s)
	if got.Bin[0] != 0x02 || len(got.Bin) != 2561 || !bytes.Equal(got.Bin, sketchBytes(&regs)) {
		t.Fatalf("2,560 registers: form %#x of %d bytes, want the dense form", got.Bin[0], len(got.Bin))
	}
	decode(t, f, got)
}

// TestUniqAccuracy measures the estimator over sketches of n uniform hashes: bias at most
// 0.5% and RMS error at most 2% (the standard error is about 1.6%).
func TestUniqAccuracy(t *testing.T) {
	t.Parallel()
	f := compile(t, spec(catalog.AggUniq, nil, typ(catalog.KindUInt64)))
	const runs = 40
	for _, n := range []int{100, 1_000, 10_000, 100_000, 1_000_000} {
		var sum, sumSq float64
		for run := range runs {
			r := rand.New(rand.NewPCG(uint64(n), uint64(run)))
			var regs [registers]uint8
			for range n {
				addHash(&regs, r.Uint64())
			}
			got := float64(final(t, decode(t, f, bin(sketchBytes(&regs)...))).AsUint())
			e := (got - float64(n)) / float64(n)
			sum += e
			sumSq += e * e
		}
		bias, rms := sum/runs, math.Sqrt(sumSq/runs)
		if math.Abs(bias) > 0.005 || rms > 0.02 {
			t.Errorf("n = %d: bias %.4f, RMS error %.4f", n, bias, rms)
		}
	}
}

// TestUniqSmallCountsAreExact checks that a few distinct values, hashed as stored, are counted
// exactly, also when added twice.
func TestUniqSmallCountsAreExact(t *testing.T) {
	t.Parallel()
	f := compile(t, spec(catalog.AggUniq, nil, typ(catalog.KindString)))
	s := f.New()
	for i := range 20 {
		for range 2 {
			if err := s.Add(input(expr.String(string(rune('a' + i))))); err != nil {
				t.Fatal(err)
			}
		}
		if got := final(t, s).AsUint(); got != uint64(i+1) {
			t.Fatalf("after %d values: estimate %d", i+1, got)
		}
	}
}

// TestUniqEstimateNeverFalls raises registers of sketches at every fill and checks that the
// estimate never falls. The throughput benchmark relies on it: a view that applied only some
// of the appends whose outcome is unknown holds registers between those of the sketch of the
// confirmed appends and those of the sketch of all of them, so its estimate lies between
// theirs.
func TestUniqEstimateNeverFalls(t *testing.T) {
	t.Parallel()
	f := compile(t, spec(catalog.AggUniq, nil, typ(catalog.KindUInt64)))
	estimate := func(regs *[registers]uint8) uint64 {
		return final(t, decode(t, f, bin(sketchBytes(regs)...))).AsUint()
	}
	r := rand.New(rand.NewPCG(7, 11))
	// A slice, not a map: the draws from r follow the order, so a failure reproduces.
	type sketch struct {
		name string
		regs *[registers]uint8
	}
	var sketches []sketch
	for _, n := range []int{1, 100, 3_000, 30_000, 1_000_000} {
		var regs [registers]uint8
		for range n {
			addHash(&regs, r.Uint64())
		}
		sketches = append(sketches, sketch{fmt.Sprintf("%d hashes", n), &regs})
	}
	var uniform [registers]uint8
	for i := range uniform {
		uniform[i] = uint8(1 + r.IntN(31))
	}
	sketches = append(sketches, sketch{"uniform ranks", &uniform})
	for _, sk := range sketches {
		name, regs := sk.name, sk.regs
		before := estimate(regs)
		for range 200 {
			i := r.IntN(registers)
			if regs[i] == 31 {
				continue
			}
			raised := *regs
			raised[i] += uint8(1 + r.IntN(int(31-regs[i])))
			if got := estimate(&raised); got < before {
				t.Fatalf("%s: raising register %d from %d to %d lowers the estimate from %d to %d",
					name, i, regs[i], raised[i], before, got)
			}
		}
	}
}

func TestUniqSaturates(t *testing.T) {
	t.Parallel()
	var regs [registers]uint8
	for i := range regs {
		regs[i] = 31
	}
	f := compile(t, spec(catalog.AggUniq, nil, typ(catalog.KindUInt64)))
	if got := final(t, decode(t, f, bin(sketchBytes(&regs)...))).AsUint(); got != math.MaxUint64 {
		t.Fatalf("estimate of a full sketch = %d, want %d", got, uint64(math.MaxUint64))
	}
}

func TestUniqDecodeRejects(t *testing.T) {
	t.Parallel()
	f := compile(t, spec(catalog.AggUniq, nil, typ(catalog.KindUInt64)))
	var one [registers]uint8
	one[7] = 3
	denseOfOne := make([]byte, 2561)
	denseOfOne[0] = 0x02
	denseOfOne[1+7*5/8] = 3 << (7 * 5 % 8)
	cases := []struct {
		name string
		a    store.Value
	}{
		{"as N", num("1")},
		{"no bytes", bin()},
		{"unknown form", bin(0x03, 0x01)},
		{"empty sparse", bin(0x01)},
		{"zero delta", bin(0x01, 0x23, 0x00)},
		{"rank 0", bin(0x01, 0x20)},
		{"index past 4095", bin(0x01, 0x81, 0x80, 0x08)},
		{"overlong varint", bin(0x01, 0xe3, 0x80, 0x00)},
		{"truncated varint", bin(0x01, 0xe3)},
		{"two entries for one register", bin(0x01, 0x22, 0x01)},
		{"dense of one register", bin(denseOfOne...)},
		{"short dense", bin(0x02, 0x00)},
	}
	if !bytes.Equal(sketchBytes(&one), []byte{0x01, 0xe3, 0x01}) {
		t.Fatalf("sparse of one register = %x", sketchBytes(&one))
	}
	if _, err := f.Decode(bin(0x01, 0xe3, 0x01)); err != nil {
		t.Fatalf("Decode of a valid sparse sketch: %v", err)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if s, err := f.Decode(c.a); err == nil {
				t.Fatalf("Decode(%x) = %+v, want an error", c.a.Bin, s)
			}
		})
	}
}

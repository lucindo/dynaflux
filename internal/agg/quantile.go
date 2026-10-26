// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"slices"
	"sort"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

const (
	// minRelAcc is the smallest rel_acc: below it the tables and the largest sketch grow
	// without bound.
	minRelAcc = 0.002
	// octaves is the span of each store, as a power of two below its largest value.
	octaves     = 64
	octaveRatio = 2
	// bigPrec is the precision of the table computations.
	bigPrec = 256
	// newtonSteps bounds Newton's method, which converges in about ten steps.
	newtonSteps = 100
	ddVersion   = 0x01
)

// mapping is DDSketch's log-spaced bucket mapping with γ = 2^(1/m), computed only with exact
// operations: Frexp and comparisons with a table built once with math/big.
type mapping struct {
	m   int       // buckets per octave
	t   []float64 // t[j] = 2^(j/m) rounded, j = 0…m; t[0] = 1 and t[m] = 2
	rep []float64 // rep[j]·2^k is the value of every x with t[j] ≤ x/2^k < t[j+1]
	// lo and hi are the indexes of the smallest and largest finite magnitudes.
	lo, hi int
}

func newFloat(x float64) *big.Float { return new(big.Float).SetPrec(bigPrec).SetFloat64(x) }

// newMapping builds the tables for relative accuracy alpha (encoding.md §7.5).
func newMapping(alpha float64) *mapping {
	m := bucketsPerOctave(alpha)
	g := octaveRoot(m)
	mp := &mapping{m: m, t: make([]float64, m+1), rep: make([]float64, m)}
	p := newFloat(1)
	for j := range m {
		mp.t[j], _ = p.Float64()
		p.Mul(p, g)
	}
	mp.t[m] = octaveRatio
	for j := range m {
		lo, hi := new(big.Rat).SetFloat64(mp.t[j]), new(big.Rat).SetFloat64(mp.t[j+1])
		r := new(big.Rat).Mul(lo, hi)
		r.Quo(r.Add(r, r), lo.Add(lo, hi))
		mp.rep[j], _ = r.Float64()
	}
	mp.lo, mp.hi = mp.index(math.SmallestNonzeroFloat64), mp.index(math.MaxFloat64)
	return mp
}

// bucketsPerOctave returns the smallest m ≥ 1 with 2^(1/m) ≤ γ = (1 + α)/(1 − α), that is
// γ^m ≥ 2: then every bucket's value is within relative error α of the bucket's inputs.
func bucketsPerOctave(alpha float64) int {
	one, a := newFloat(1), newFloat(alpha)
	gamma := newFloat(0).Quo(newFloat(0).Add(one, a), newFloat(0).Sub(one, a))
	m := 1
	for p := newFloat(0).Set(gamma); p.Cmp(newFloat(octaveRatio)) < 0; p.Mul(p, gamma) {
		m++
	}
	return m
}

// octaveRoot returns 2^(1/m) by Newton's method from above: y ← ((m − 1)·y + 2/y^(m−1))/m.
func octaveRoot(m int) *big.Float {
	y := newFloat(0).Quo(newFloat(float64(m+1)), newFloat(float64(m)))
	for range newtonSteps {
		p := newFloat(1)
		for range m - 1 {
			p.Mul(p, y)
		}
		next := newFloat(0).Quo(newFloat(octaveRatio), p)
		next.Add(next, newFloat(0).Mul(newFloat(float64(m-1)), y))
		next.Quo(next, newFloat(float64(m)))
		if next.Cmp(y) >= 0 {
			break
		}
		y = next
	}
	return y
}

// index returns the bucket of x > 0: with x = u·2^(e−1) and u in [1, 2), (e − 1)·m plus the
// count of t[1] … t[m−1] that are at most u.
func (mp *mapping) index(x float64) int {
	f, e := math.Frexp(x)
	u := octaveRatio * f
	return (e-1)*mp.m + sort.Search(mp.m-1, func(i int) bool { return mp.t[i+1] > u })
}

// value returns the value of bucket i: rep[i mod m]·2^⌊i/m⌋.
func (mp *mapping) value(i int) float64 {
	e, j := i/mp.m, i%mp.m
	if j < 0 {
		e, j = e-1, j+mp.m
	}
	return math.Ldexp(mp.rep[j], e)
}

// capped folds the counts below the store's window, the octaves·m indexes up to its
// largest, into the window's lowest index. The bound only rises, so the result depends only
// on the inputs, not on their order or how they were merged.
func (mp *mapping) capped(bs []bucket) []bucket {
	if len(bs) == 0 {
		return bs
	}
	lo := bs[len(bs)-1].index - octaves*mp.m + 1
	k := sort.Search(len(bs), func(i int) bool { return bs[i].index >= lo })
	if k == 0 {
		return bs
	}
	var n uint64
	for _, b := range bs[:k] {
		n += b.n
	}
	if bs[k].index == lo {
		bs[k].n += n
		return bs[k:]
	}
	bs[k-1] = bucket{index: lo, n: n}
	return bs[k-1:]
}

// maxBytes bounds an encoded sketch of at most 2^64 inputs. A full store has octaves·m
// buckets, each after the first a 1-byte index step and a count. A count below 2^56 takes at
// most 8 bytes; with fewer than 2^64 inputs, at most 255 counts reach 2^56 and take a ninth
// byte, and one reaches 2^63 and takes a tenth.
func (mp *mapping) maxBytes() int {
	const countBytes, extraCountBytes = 8, 256
	n := octaves * mp.m
	var buf [binary.MaxVarintLen64]byte
	index := max(binary.PutVarint(buf[:], int64(mp.lo)), binary.PutVarint(buf[:], int64(mp.hi)))
	perStore := binary.PutUvarint(buf[:], uint64(n)) + index + countBytes + (n-1)*(1+countBytes) //nolint:gosec // n > 0
	return 1 + countBytes + 2*perStore + extraCountBytes
}

// bucket is a non-zero count at an index.
type bucket struct {
	index int
	n     uint64
}

// sketch is the state of quantile and quantileDD: a DDSketch with a zero count, a store for
// positive values and one for the magnitudes of negative values, each in index order.
type sketch struct {
	f        *Func
	zero     uint64
	pos, neg []bucket
}

func (s *sketch) Add(in Input) error {
	if err := s.f.check(in); err != nil {
		return err
	}
	var x float64
	switch v := in.Args[0]; {
	case s.f.args[0].Kind == catalog.KindFloat64:
		x = v.AsFloat()
	case isSigned(s.f.args[0].Kind):
		x = float64(v.AsInt())
	default:
		x = float64(v.AsUint())
	}
	mp := s.f.dd
	switch {
	case x > 0:
		s.pos = mp.capped(addCount(s.pos, mp.index(x), 1))
	case x < 0:
		s.neg = mp.capped(addCount(s.neg, mp.index(-x), 1))
	default:
		s.zero++
	}
	return nil
}

func addCount(bs []bucket, index int, n uint64) []bucket {
	i, found := slices.BinarySearchFunc(bs, index, func(b bucket, t int) int { return cmp.Compare(b.index, t) })
	if found {
		bs[i].n += n
		return bs
	}
	return slices.Insert(bs, i, bucket{index: index, n: n})
}

func (s *sketch) Merge(o State) error {
	x, ok := o.(*sketch)
	if !ok || x.f != s.f {
		return errMerge(s.f)
	}
	s.zero += x.zero
	s.pos = s.f.dd.capped(mergeBuckets(s.pos, x.pos))
	s.neg = s.f.dd.capped(mergeBuckets(s.neg, x.neg))
	return nil
}

func mergeBuckets(a, b []bucket) []bucket {
	out := make([]bucket, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		switch {
		case a[0].index < b[0].index:
			out, a = append(out, a[0]), a[1:]
		case a[0].index > b[0].index:
			out, b = append(out, b[0]), b[1:]
		default:
			out = append(out, bucket{index: a[0].index, n: a[0].n + b[0].n})
			a, b = a[1:], b[1:]
		}
	}
	return append(append(out, a...), b...)
}

func (s *sketch) Empty() bool { return s.zero == 0 && len(s.pos) == 0 && len(s.neg) == 0 }

func (s *sketch) Encode() (store.Value, error) {
	if s.Empty() {
		return store.Value{}, errEmpty
	}
	return store.Value{Type: store.B, Bin: s.bytes()}, nil
}

// bytes returns 0x01, uvarint(zero), then each store as uvarint(len), varint(first index),
// uvarint(count), then uvarint(index step) and uvarint(count) for each later bucket.
func (s *sketch) bytes() []byte {
	b := binary.AppendUvarint([]byte{ddVersion}, s.zero)
	for _, bs := range [2][]bucket{s.pos, s.neg} {
		b = binary.AppendUvarint(b, uint64(len(bs)))
		for i, x := range bs {
			if i == 0 {
				b = binary.AppendVarint(b, int64(x.index))
			} else {
				b = binary.AppendUvarint(b, uint64(x.index-bs[i-1].index)) //nolint:gosec // indexes ascend
			}
			b = binary.AppendUvarint(b, x.n)
		}
	}
	return b
}

// Final returns the value of the first bucket, from the most negative value up, whose
// running count passes rank = level·(n − 1). DDSketch doesn't interpolate.
func (s *sketch) Final() (expr.Value, error) {
	if s.Empty() {
		return s.f.none(expr.Float(0)), nil
	}
	n := s.zero
	for _, b := range slices.Concat(s.pos, s.neg) {
		n += b.n
	}
	rank := float64(s.f.level * float64(n-1))
	var run uint64
	last := 0.0
	for _, b := range slices.Backward(s.neg) {
		run += b.n
		if last = -s.f.dd.value(b.index); float64(run) > rank {
			return expr.Float(last), nil
		}
	}
	if s.zero > 0 {
		run += s.zero
		if last = 0; float64(run) > rank {
			return expr.Float(last), nil
		}
	}
	for _, b := range s.pos {
		run += b.n
		if last = s.f.dd.value(b.index); float64(run) > rank {
			return expr.Float(last), nil
		}
	}
	// float64(n) can round down to rank when n passes 2^53.
	return expr.Float(last), nil
}

func (s *sketch) decode(a store.Value) error {
	if a.Type != store.B {
		return errors.New("a quantile state must be a DynamoDB B")
	}
	b := a.Bin
	if len(b) == 0 || b[0] != ddVersion {
		return fmt.Errorf("quantile state %x has no version byte", b)
	}
	r := reader{b: b[1:]}
	s.zero = r.uvarint()
	s.pos = s.decodeStore(&r)
	s.neg = s.decodeStore(&r)
	if r.err == nil && len(r.b) > 0 {
		r.err = errors.New("trailing bytes")
	}
	if r.err == nil {
		r.err = s.checkTotal()
	}
	if r.err != nil {
		return fmt.Errorf("quantile state %x: %w", b, r.err)
	}
	if !bytes.Equal(s.bytes(), b) {
		return fmt.Errorf("quantile state %x is not in the form Dynaflux writes", b)
	}
	return nil
}

func (s *sketch) decodeStore(r *reader) []bucket {
	mp := s.f.dd
	n := r.uvarint()
	if r.err != nil || n > uint64(octaves*mp.m) { //nolint:gosec // positive
		r.fail("a store has more buckets than its window")
		return nil
	}
	bs := make([]bucket, 0, n)
	for i := range n {
		var index int
		if i == 0 {
			first := r.varint()
			if first < int64(mp.lo) || first > int64(mp.hi) {
				r.fail("an index is outside every finite value")
			}
			index = int(first)
		} else {
			step := r.uvarint()
			last := bs[len(bs)-1].index
			if step == 0 || step > uint64(mp.hi-last) { //nolint:gosec // last ≤ hi
				r.fail("indexes don't ascend within the finite values")
			}
			index = last + int(step) //nolint:gosec // step ≤ hi − last
		}
		count := r.uvarint()
		if count == 0 {
			r.fail("a bucket has no count")
		}
		if r.err != nil {
			return nil
		}
		bs = append(bs, bucket{index: index, n: count})
	}
	if len(bs) > 0 && bs[0].index <= bs[len(bs)-1].index-octaves*mp.m {
		r.fail("a store spans more than its window")
	}
	return bs
}

// checkTotal requires at least one input and a total count that fits a uint64.
func (s *sketch) checkTotal() error {
	n := s.zero
	var carry uint64
	for _, b := range slices.Concat(s.pos, s.neg) {
		var c uint64
		n, c = bits.Add64(n, b.n, 0)
		carry |= c
	}
	if n == 0 || carry != 0 {
		return errors.New("the total count is 0 or past 2^64")
	}
	return nil
}

func (s *sketch) maxSize() int { return s.f.dd.maxBytes() }

// reader reads varints until the first error.
type reader struct {
	b   []byte
	err error
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail("a varint is truncated or too long")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) varint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.fail("a varint is truncated or too long")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) fail(msg string) {
	if r.err == nil {
		r.err = errors.New(msg)
	}
}

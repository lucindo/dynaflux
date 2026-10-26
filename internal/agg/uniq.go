// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"

	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// HyperLogLog parameters (encoding.md §7.5): 2^12 registers of 5 bits, each holding a rank
// of 1 to q + 1 = 31, or 0 when empty. This is Ertl's model with q = 30.
const (
	hllIndexBits = 12
	hllRegisters = 1 << hllIndexBits
	hllRankBits  = 5
	hllMaxRank   = 1<<hllRankBits - 1
	hllGroup     = 8 // registers per hllRankBits bytes of the dense form
	hllDenseLen  = 1 + hllRegisters/hllGroup*hllRankBits

	hllSparse = 0x01
	hllDense  = 0x02
)

// hll is the state of uniq.
type hll struct {
	f   *Func
	r   [hllRegisters]uint8
	has bool
}

func (s *hll) Add(in Input) error {
	if err := s.f.check(in); err != nil {
		return err
	}
	h := sha256.Sum256(keys.Canonical(nil, s.f.args[0], in.Args[0]))
	x := binary.BigEndian.Uint64(h[:8])
	i := x >> (wordBits - hllIndexBits)
	rank := uint8(min(bits.LeadingZeros64(x<<hllIndexBits)+1, hllMaxRank)) //nolint:gosec // 1 to 31
	s.r[i] = max(s.r[i], rank)
	s.has = true
	return nil
}

func (s *hll) Merge(o State) error {
	x, ok := o.(*hll)
	if !ok || x.f != s.f {
		return errMerge(s.f)
	}
	for i, r := range x.r {
		s.r[i] = max(s.r[i], r)
	}
	s.has = s.has || x.has
	return nil
}

func (s *hll) Empty() bool { return !s.has }

func (s *hll) Encode() (store.Value, error) {
	if !s.has {
		return store.Value{}, errEmpty
	}
	return store.Value{Type: store.B, Bin: s.bytes()}, nil
}

// bytes returns the sparse form when it is shorter than the dense one: both stand for the
// same registers, so equal registers give equal bytes.
func (s *hll) bytes() []byte {
	b := []byte{hllSparse}
	var prev uint64
	for i, r := range s.r {
		if r == 0 {
			continue
		}
		v := uint64(i)<<hllRankBits | uint64(r)
		b = binary.AppendUvarint(b, v-prev)
		prev = v
		if len(b) >= hllDenseLen {
			return s.dense()
		}
	}
	return b
}

// dense packs register i into bits 5i to 5i + 4, least significant bit first: each group of
// 8 registers fills 5 bytes.
func (s *hll) dense() []byte {
	b := make([]byte, hllDenseLen)
	b[0] = hllDense
	var buf [8]byte
	for g := range hllRegisters / hllGroup {
		var v uint64
		for k, r := range s.r[g*hllGroup : (g+1)*hllGroup] {
			v |= uint64(r) << (k * hllRankBits)
		}
		binary.LittleEndian.PutUint64(buf[:], v)
		copy(b[1+g*hllRankBits:], buf[:hllRankBits])
	}
	return b
}

func (s *hll) Final() (expr.Value, error) {
	return expr.Uint(s.estimate()), nil
}

func (s *hll) decode(a store.Value) error {
	if a.Type != store.B || len(a.Bin) == 0 {
		return errors.New("a uniq state must be a non-empty DynamoDB B")
	}
	b := a.Bin
	switch b[0] {
	case hllSparse:
		if err := s.decodeSparse(b[1:]); err != nil {
			return err
		}
	case hllDense:
		if len(b) != hllDenseLen {
			return fmt.Errorf("a dense uniq state has %d bytes, not %d", len(b), hllDenseLen)
		}
		var buf [8]byte
		for g := range hllRegisters / hllGroup {
			copy(buf[:], b[1+g*hllRankBits:1+(g+1)*hllRankBits])
			v := binary.LittleEndian.Uint64(buf[:])
			for k := range hllGroup {
				s.r[g*hllGroup+k] = uint8(v >> (k * hllRankBits) & hllMaxRank)
			}
		}
	default:
		return fmt.Errorf("unknown uniq state form %#x", b[0])
	}
	if s.r == [hllRegisters]uint8{} || !bytes.Equal(s.bytes(), b) {
		return fmt.Errorf("uniq state %x is not in the form Dynaflux writes", b)
	}
	s.has = true
	return nil
}

func (s *hll) decodeSparse(b []byte) error {
	const limit = hllRegisters << hllRankBits
	var prev uint64
	for len(b) > 0 {
		d, n := binary.Uvarint(b)
		if n <= 0 || d == 0 || d >= limit-prev {
			return errors.New("a sparse uniq state has a malformed entry")
		}
		b = b[n:]
		prev += d
		s.r[prev>>hllRankBits] = uint8(prev & hllMaxRank)
	}
	return nil
}

func (s *hll) maxSize() int { return hllDenseLen }

// Constants of the estimator: m registers, ranks up to q + 1, and α∞·m², a constant
// expression rounded once.
const (
	hllM       = float64(hllRegisters)
	hllQ       = hllMaxRank - 1
	hllAlphaM2 = hllM * hllM / (2 * math.Ln2) //nolint:mnd // α∞ = 1/(2 ln 2)
)

// estimate is Ertl's improved raw estimator (arXiv:1702.01284, Algorithm 6). It uses only
// + − × ÷ and Sqrt, and converts every product and quotient with float64(…), which forbids
// fusing it into a multiply-add, so every CPU computes the same bits.
func (s *hll) estimate() uint64 {
	var c [hllMaxRank + 1]int
	for _, r := range s.r {
		c[r]++
	}
	z := float64(hllM * tau(float64(float64(hllM-float64(c[hllQ+1]))/hllM)))
	for k := hllQ; k >= 1; k-- {
		z = float64(0.5 * (z + float64(c[k]))) //nolint:mnd // Algorithm 6
	}
	z += float64(hllM * sigma(float64(float64(c[0])/hllM)))
	e := math.Round(float64(hllAlphaM2 / z))
	if e >= math.MaxUint64 {
		// An estimate past UInt64, as when every register is 31, saturates.
		return math.MaxUint64
	}
	return uint64(e)
}

func sigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	y, z := 1.0, x
	for {
		x = float64(x * x)
		prev := z
		z += float64(x * y)
		y += y
		if z == prev {
			return z
		}
	}
}

func tau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	y, z := 1.0, 1-x
	for {
		x = math.Sqrt(x)
		prev := z
		y = float64(y * 0.5) //nolint:mnd // Algorithm 6
		t := 1 - x
		z -= float64(float64(t*t) * y)
		if z == prev {
			return float64(z / 3) //nolint:mnd // Algorithm 6
		}
	}
}

// set is the state of a read-time uniqExact: the canonical bytes of the values seen.
type set struct {
	f    *Func
	vals map[string]struct{}
}

func (s *set) Add(in Input) error {
	if err := s.f.check(in); err != nil {
		return err
	}
	if s.vals == nil {
		s.vals = make(map[string]struct{})
	}
	s.vals[string(keys.Canonical(nil, s.f.args[0], in.Args[0]))] = struct{}{}
	return nil
}

func (s *set) Merge(o State) error {
	x, ok := o.(*set)
	if !ok || x.f != s.f {
		return errMerge(s.f)
	}
	if s.vals == nil && len(x.vals) > 0 {
		s.vals = make(map[string]struct{}, len(x.vals))
	}
	for v := range x.vals {
		s.vals[v] = struct{}{}
	}
	return nil
}

func (s *set) Empty() bool { return len(s.vals) == 0 }

func (s *set) Encode() (store.Value, error) { return store.Value{}, errNotStored }

func (s *set) Final() (expr.Value, error) { return expr.Uint(uint64(len(s.vals))), nil }

func (s *set) decode(store.Value) error { return errNotStored }

// maxSize is 0: a read-time state is never stored.
func (s *set) maxSize() int { return 0 }

var errNotStored = errors.New("a read-time uniqExact is never stored")

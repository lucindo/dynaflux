// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math"
	"math/bits"
	"math/rand/v2"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"testing"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

// crossArchDigest pins TestCrossArchitectureDigest's outputs. A code change that changes it
// changes the storage format, which then needs a new catalog.StorageFormatVersion.
const crossArchDigest = "a38b90806b53994a637183f65ae0fed7f019481c6926959ce2eea9c70311e496"

// TestCrossArchitectureDigest hashes states and finalized values over inputs that reach
// every float path: exact sums over the whole exponent range, sketch buckets of normal and
// subnormal values, conversions of large integers, and estimates of random sketches. Every
// CPU must give the same digest: run it with GOARCH=amd64 too, which Rosetta runs on arm64.
func TestCrossArchitectureDigest(t *testing.T) {
	t.Parallel()
	h := sha256.New()
	for _, c := range orderCases() {
		f := compile(t, c.spec)
		writeState(t, h, c.spec, addAll(t, f.New(), c.gen, upTo(1500)))
	}
	r := rand.New(rand.NewPCG(9, 10))
	for _, alpha := range []float64{0.01, 0.002, 0.05} {
		qs := quantileSpec(alpha, 0.5, typ(catalog.KindFloat64))
		f := compile(t, qs)
		for range 20000 {
			if x := math.Float64frombits(r.Uint64() &^ (1 << 63)); finite(x) && x != 0 {
				writeState(t, h, qs, state(t, f, input(expr.Float(x))))
			}
		}
	}
	us := quantileSpec(0.01, 0.5, typ(catalog.KindUInt64))
	u := compile(t, us)
	for range 2000 {
		writeState(t, h, us, state(t, u, input(expr.Uint(r.Uint64()))))
	}
	as := spec(catalog.AggAvg, nil, typ(catalog.KindFloat64))
	avg := compile(t, as)
	for range 300 {
		s := avg.New()
		for range 1 + r.IntN(50) {
			if x := math.Float64frombits(r.Uint64()); finite(x) {
				addAll(t, s, func(int) agg.Input { return input(expr.Float(x)) }, []int{0})
			}
		}
		writeState(t, h, as, s)
	}
	uqs := spec(catalog.AggUniq, nil, typ(catalog.KindUInt64))
	uniq := compile(t, uqs)
	for range 1000 {
		regs := randomRegisters(r)
		writeState(t, h, uqs, decode(t, uniq, bin(sketchBytes(&regs)...)))
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != crossArchDigest {
		t.Fatalf("digest %s, want %s", got, crossArchDigest)
	}
}

// randomRegisters returns registers of a random fill and ranks, cheaper to make than from
// hashes.
func randomRegisters(r *rand.Rand) [registers]uint8 {
	var regs [registers]uint8
	empty := r.IntN(registers)
	for i := range regs {
		if r.IntN(registers) >= empty {
			regs[i] = uint8(min(bits.LeadingZeros64(r.Uint64()>>r.IntN(32))+1, 31))
		}
	}
	regs[0] = max(regs[0], 1)
	return regs
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

// writeState hashes what the storage format fixes: the attribute value, the finalized
// value, and which errors are evaluation errors. Error text may change freely.
func writeState(t *testing.T, h hash.Hash, s agg.Spec, st agg.State) {
	t.Helper()
	if !s.ReadTime {
		v, err := st.Encode()
		if err == nil {
			writeAttr(t, h, v)
		}
		fmt.Fprintf(h, " %s\n", errClass(err))
	}
	x, err := st.Final()
	fmt.Fprintf(h, "%s %s\n", render(s, x), errClass(err))
}

func writeAttr(t *testing.T, h hash.Hash, v store.Value) {
	t.Helper()
	switch v.Type { //nolint:exhaustive // no state is an M, so default fails
	case store.S:
		fmt.Fprintf(h, "S%q", v.Str)
	case store.N:
		fmt.Fprintf(h, "N%q", v.Str)
	case store.B:
		fmt.Fprintf(h, "B%x.", v.Bin)
	case store.BOOL:
		fmt.Fprintf(h, "BOOL%t", v.Bool)
	case store.L:
		fmt.Fprintf(h, "L%d[", len(v.List))
		for _, x := range v.List {
			writeAttr(t, h, x)
		}
		fmt.Fprint(h, "]")
	default:
		t.Fatalf("a state encoded as attribute type %d", v.Type)
	}
}

func errClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, expr.ErrEval):
		return "eval"
	}
	return "error"
}

// render writes a finalized value exactly.
func render(s agg.Spec, v expr.Value) string {
	switch {
	case v.IsNull():
		return "NULL"
	case s.Agg.Fn == catalog.AggCount || s.Agg.Fn == catalog.AggUniq || s.Agg.Fn == catalog.AggUniqExact:
		return strconv.FormatUint(v.AsUint(), 10)
	case s.Agg.Fn == catalog.AggAvg || s.Agg.Fn == catalog.AggQuantile || s.Agg.Fn == catalog.AggQuantileDD ||
		s.ArgTypes[0].Kind == catalog.KindFloat64:
		return strconv.FormatUint(math.Float64bits(v.AsFloat()), 16)
	case s.ArgTypes[0].Kind == catalog.KindDecimal:
		return v.AsDecimal().String()
	case s.ArgTypes[0].Kind == catalog.KindString:
		return strconv.Quote(v.AsString())
	}
	return fmt.Sprint(v.AsInt(), v.AsUint())
}

// TestNoFusedMultiplyAdd compiles the package for arm64 and for amd64 with FMA (v3), the
// targets where Go may fuse x*y + z, and fails on any fused instruction: a fused result is
// rounded once instead of twice, so it would differ from a CPU without FMA.
func TestNoFusedMultiplyAdd(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("compiles the package twice")
	}
	fused := regexp.MustCompile(`\b(FMADDD|FMSUBD|FNMADDD|FNMSUBD|VFMADD\w*|VFMSUB\w*|VFNMADD\w*|VFNMSUB\w*)\b`)
	for _, env := range [][]string{{"GOARCH=arm64"}, {"GOARCH=amd64", "GOAMD64=v3"}} {
		cmd := exec.CommandContext(t.Context(), "go", "build", "-gcflags=-S", "github.com/lucindo/dynaflux/internal/agg")
		cmd.Env = append(os.Environ(), env...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v go build: %v\n%s", env, err, out)
		}
		if !regexp.MustCompile(`\bTEXT\b`).Match(out) {
			t.Fatalf("%v: go build printed no assembly", env)
		}
		if m := fused.FindAll(out, -1); len(m) > 0 {
			t.Errorf("%v: %d fused multiply-add instructions, such as %s", env, len(m), m[0])
		}
	}
}

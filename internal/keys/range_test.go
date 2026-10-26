// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys_test

import (
	"math/rand/v2"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

func incl(v expr.Value) *keys.Bound { return &keys.Bound{Value: v, Inclusive: true} }
func excl(v expr.Value) *keys.Bound { return &keys.Bound{Value: v} }

// Each row of the table in encoding.md §3.5, on a UInt8 field.
func TestRangeShapes(t *testing.T) {
	t.Parallel()
	u := expr.Uint
	tests := []struct {
		name   string
		prefix string
		lo, hi *keys.Bound
		want   keys.SKRange
	}{
		{"field >= lo", "a", incl(u(5)), nil, keys.SKRange{From: "a#005", To: "a$"}},
		{"field > lo", "a", excl(u(5)), nil, keys.SKRange{From: "a#005$", To: "a$"}},
		{"field <= hi", "a", nil, incl(u(9)), keys.SKRange{From: "a#", To: "a#009$"}},
		{"field < hi", "a", nil, excl(u(9)), keys.SKRange{From: "a#", To: "a#009"}},
		{"field < hi, no prefix", "", nil, excl(u(9)), keys.SKRange{To: "009", ToExclusive: true}},
		{"lo <= field < hi, no prefix", "", incl(u(5)), excl(u(9)), keys.SKRange{From: "005", To: "009"}},
		{"field > lo, no prefix", "", excl(u(5)), nil, keys.SKRange{From: "005$"}},
		{"field <= hi, no prefix", "", nil, incl(u(9)), keys.SKRange{To: "009$"}},
		{"prefix only", "a", nil, nil, keys.SKRange{From: "a#", To: "a$"}},
		{"partition", "", nil, nil, keys.SKRange{}},
		{"lo = hi, both inclusive", "", incl(u(5)), incl(u(5)), keys.SKRange{From: "005", To: "005$"}},
		{"crossed", "a", incl(u(9)), incl(u(5)), keys.SKRange{Empty: true}},
		{"lo = hi, lo exclusive", "a", excl(u(5)), incl(u(5)), keys.SKRange{Empty: true}},
		{"lo = hi, hi exclusive", "", incl(u(5)), excl(u(5)), keys.SKRange{Empty: true}},
	}
	for _, tt := range tests {
		got, err := keys.Range(tt.prefix, tUInt8, tt.lo, tt.hi)
		if err != nil || got != tt.want {
			t.Errorf("%s: Range = %+v, %v; want %+v", tt.name, got, err, tt.want)
		}
	}
}

// A stream read of [since, until) is sk ≥ enc(since) and sk ≤ enc(until) (encoding.md §3.5):
// every event at until sorts above the bound.
func TestRangeOverStreamTime(t *testing.T) {
	t.Parallel()
	got, err := keys.Range("", tDT64u, incl(expr.Micros(may31)), excl(expr.Micros(may31at)))
	want := keys.SKRange{From: "2024-05-31T00:00:00.000000Z", To: "2024-05-31T13:45:00.250123Z"}
	if err != nil || got != want {
		t.Fatalf("Range = %+v, %v; want %+v", got, err, want)
	}
	atUntil, err := keys.StreamSK(may31at, "00000000000000000000000000")
	if err != nil || atUntil <= got.To {
		t.Errorf("an event at until (%q) is not above To %q: %v", atUntil, got.To, err)
	}
}

func TestRangeLeavesOutOversizedBounds(t *testing.T) {
	t.Parallel()
	long := expr.String(strings.Repeat("z", store.MaxSKLen))
	got, err := keys.Range("a", tString, incl(long), excl(long))
	if err != nil || !got.Empty {
		t.Fatalf("crossed long bounds: %+v, %v", got, err)
	}
	got, err = keys.Range("a", tString, incl(expr.String("m")), incl(long))
	if want := (keys.SKRange{From: "a#m", To: "a$", Loose: true}); err != nil || got != want {
		t.Errorf("long upper bound: %+v, %v; want %+v", got, err, want)
	}
	got, err = keys.Range("", tString, excl(long), excl(expr.String("{")))
	if want := (keys.SKRange{To: "{", ToExclusive: true, Loose: true}); err != nil || got != want {
		t.Errorf("long lower bound: %+v, %v; want %+v", got, err, want)
	}
	// A bound of exactly 1024 bytes is kept; one byte more is left out.
	z := func(n int) expr.Value { return expr.String(strings.Repeat("z", n)) }
	for _, tt := range []struct {
		name   string
		lo, hi *keys.Bound
		want   keys.SKRange
	}{
		{"From of 1024 bytes", incl(z(store.MaxSKLen)), nil, keys.SKRange{From: strings.Repeat("z", store.MaxSKLen)}},
		{"From of 1025 bytes", incl(z(store.MaxSKLen + 1)), nil, keys.SKRange{Loose: true}},
		{"To of 1024 bytes", nil, incl(z(store.MaxSKLen - 1)), keys.SKRange{To: strings.Repeat("z", store.MaxSKLen-1) + "$"}},
		{"To of 1025 bytes", nil, incl(z(store.MaxSKLen)), keys.SKRange{Loose: true}},
	} {
		if r, rErr := keys.Range("", tString, tt.lo, tt.hi); rErr != nil || r != tt.want {
			t.Errorf("%s: %+v, %v; want %+v", tt.name, r, rErr, tt.want)
		}
	}
	// No stored key can start with a prefix this long.
	got, err = keys.Range(strings.Repeat("p", store.MaxSKLen-1), tString, nil, nil)
	if err != nil || !got.Empty {
		t.Errorf("long prefix: %+v, %v; want Empty", got, err)
	}
}

func TestRangeRejectsOutOfRangeAndNonKeyBounds(t *testing.T) {
	t.Parallel()
	if _, err := keys.Range("", tUInt8, incl(expr.Uint(300)), nil); err == nil {
		t.Error("UInt8 bound 300: want an error")
	}
	// Even when no key can start with the prefix.
	if _, err := keys.Range(strings.Repeat("p", store.MaxSKLen-1), tUInt8, incl(expr.Uint(300)), nil); err == nil {
		t.Error("UInt8 bound 300 under a long prefix: want an error")
	}
	if _, err := keys.Range("", tFloat, nil, incl(expr.Float(1))); err == nil {
		t.Error("Float64 bound: want an error")
	}
}

// selects is the item selection DynamoDB makes for a Query with r's sort-key condition.
func selects(r keys.SKRange, sk string) bool {
	switch {
	case r.Empty:
		return false
	case r.From != "" && sk < r.From:
		return false
	case r.To != "" && r.ToExclusive:
		return sk < r.To
	case r.To != "":
		return sk <= r.To
	}
	return true
}

// Ranges against the typed predicate: every item in range is read, and the only item read
// outside it is the documented boundary item.
func TestRangeMatchesTypedPredicate(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(3, 4))
	var drops, empties int
	for scenario := range 20_000 {
		sc := newRangeScenario(r)
		got, err := keys.Range(sc.prefix, sc.ts[sc.j], sc.lo, sc.hi)
		if err != nil {
			t.Fatalf("scenario %d: Range: %v", scenario, err)
		}
		checkQueryValid(t, got)
		if got.Empty {
			empties++
			if !boundsCross(sc.ts[sc.j], sc.lo, sc.hi) {
				t.Fatalf("scenario %d: Empty for bounds %v, %v that hold values", scenario, sc.lo, sc.hi)
			}
		}
		for i, sk := range sc.sks {
			want, read := sc.inRange(i), selects(got, sk)
			switch {
			case want && !read:
				t.Fatalf("scenario %d %+v: item %q in range is not read", scenario, got, sk)
			case read && !want && sc.isBoundaryItem(got, sk):
				drops++
			case read && !want:
				t.Fatalf("scenario %d %+v: item %q outside the range is read", scenario, got, sk)
			}
		}
	}
	if drops == 0 || empties == 0 {
		t.Errorf("the scenarios never reached a boundary item (%d) or crossed bounds (%d)", drops, empties)
	}
}

type rangeScenario struct {
	ts     []catalog.Type
	split  bool
	j      int // the bounded field; fields before it equal base
	base   []expr.Value
	prefix string
	lo, hi *keys.Bound
	sks    []string
	vals   [][]expr.Value
}

func newRangeScenario(r *rand.Rand) rangeScenario {
	sc := rangeScenario{ts: randSchema(r), split: r.IntN(2) == 0}
	sc.base = randTuple(r, sc.ts)
	seen := map[string]bool{}
	for range 40 {
		v := randTuple(r, sc.ts)
		copy(v[:r.IntN(len(sc.ts)+1)], sc.base)
		k, _ := keys.Key(sc.ts, v)
		if seen[k] {
			continue
		}
		seen[k] = true
		if !sc.split {
			sc.sks, sc.vals = append(sc.sks, k), append(sc.vals, v)
			continue
		}
		for p := range 2 {
			sc.sks, sc.vals = append(sc.sks, keys.SplitKey(k, p)), append(sc.vals, v)
		}
	}
	sc.j = r.IntN(len(sc.ts))
	if sc.j > 0 {
		sc.prefix, _ = keys.Key(sc.ts[:sc.j], sc.base[:sc.j])
	}
	sc.lo, sc.hi = sc.randBound(r), sc.randBound(r)
	return sc
}

// randBound is absent a quarter of the time, and otherwise often a value that some key holds.
func (sc rangeScenario) randBound(r *rand.Rand) *keys.Bound {
	if r.IntN(4) == 0 {
		return nil
	}
	v := randValue(r, sc.ts[sc.j])
	switch r.IntN(3) {
	case 0:
		v = sc.base[sc.j]
	case 1:
		v = sc.vals[r.IntN(len(sc.vals))][sc.j]
	}
	return &keys.Bound{Value: v, Inclusive: r.IntN(2) == 0}
}

func (sc rangeScenario) inRange(i int) bool {
	v, t := sc.vals[i], sc.ts[sc.j]
	if compareTuples(sc.ts[:sc.j], v[:sc.j], sc.base[:sc.j]) != 0 {
		return false
	}
	if sc.lo != nil {
		if c := compareValues(t, v[sc.j], sc.lo.Value); c < 0 || c == 0 && !sc.lo.Inclusive {
			return false
		}
	}
	if sc.hi != nil {
		if c := compareValues(t, v[sc.j], sc.hi.Value); c > 0 || c == 0 && !sc.hi.Inclusive {
			return false
		}
	}
	return true
}

// isBoundaryItem reports the one item encoding.md §3.5 allows a read outside the range: the
// item equal to an exclusive upper bound on the last sort field of a key without SPLIT.
func (sc rangeScenario) isBoundaryItem(r keys.SKRange, sk string) bool {
	return sc.hi != nil && !sc.hi.Inclusive && !r.ToExclusive && sk == r.To &&
		sc.j == len(sc.ts)-1 && !sc.split
}

func boundsCross(t catalog.Type, lo, hi *keys.Bound) bool {
	if lo == nil || hi == nil {
		return false
	}
	c := compareValues(t, lo.Value, hi.Value)
	return c > 0 || c == 0 && (!lo.Inclusive || !hi.Inclusive)
}

// checkQueryValid asserts what DynamoDB requires of a sort-key condition.
func checkQueryValid(t *testing.T, r keys.SKRange) {
	t.Helper()
	switch {
	case r.Empty:
		return
	case len(r.From) > store.MaxSKLen || len(r.To) > store.MaxSKLen:
		t.Fatalf("%+v: a bound is over %d bytes", r, store.MaxSKLen)
	case !utf8.ValidString(r.From) || !utf8.ValidString(r.To):
		t.Fatalf("%+v: a bound is not valid UTF-8", r)
	case r.From != "" && r.To != "" && r.From > r.To:
		t.Fatalf("%+v: an inverted BETWEEN", r)
	case r.ToExclusive && r.From != "":
		t.Fatalf("%+v: an exclusive upper bound with a lower bound", r)
	}
}

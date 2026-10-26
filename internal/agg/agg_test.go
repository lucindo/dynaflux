// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg_test

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// orderCase is an aggregate with a generator of its inputs. Input i is the same on every
// call with the same i, so an idempotent aggregate can see it twice.
type orderCase struct {
	name string
	spec agg.Spec
	gen  func(i int) agg.Input
}

// wideFloat returns a float64 of either sign over 2^±150 or 0, built without math library
// functions so it is the same on every CPU.
func wideFloat(r *rand.Rand) float64 {
	if r.IntN(50) == 0 {
		return 0
	}
	x := math.Ldexp(float64(r.Uint64()>>11|1), r.IntN(300)-150-53)
	if r.IntN(3) == 0 {
		x = -x
	}
	return x
}

func rng(i int) *rand.Rand { return rand.New(rand.NewPCG(uint64(i), 0x5eed)) }

func randString(r *rand.Rand) string {
	b := make([]byte, r.IntN(4))
	for i := range b {
		b[i] = "aAzé\x00"[r.IntN(6)]
	}
	return string(b)
}

// event gives input i a unique (event_id, array_index) and a few ties in event_time.
func event(i int, args ...expr.Value) agg.Input {
	return agg.Input{Args: args, EventTime: int64(i % 7), EventID: eventID(i / 3), ArrayIndex: i % 3}
}

// eventID returns an event_id of 26 characters that sorts as n.
func eventID(n int) string { return fmt.Sprintf("%026d", n) }

func orderCases() []orderCase {
	u64, i64, f64, strT := typ(catalog.KindUInt64), typ(catalog.KindInt64), typ(catalog.KindFloat64), typ(catalog.KindString)
	d384 := decimal(38, 4)
	wideDecimal := func(i int) expr.Value {
		r := rng(i)
		d := new(big.Int).SetUint64(r.Uint64())
		d.Lsh(d, 64).Add(d, new(big.Int).SetUint64(r.Uint64()))
		d.Mod(d, new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil))
		if r.IntN(2) == 0 {
			d.Neg(d)
		}
		return expr.Decimal(d)
	}
	return []orderCase{
		{"count", spec(catalog.AggCount, nil), func(int) agg.Input { return input() }},
		{"sum UInt64", spec(catalog.AggSum, nil, u64), func(i int) agg.Input { return input(expr.Uint(rng(i).Uint64())) }},
		{"sum Int64", spec(catalog.AggSum, nil, i64), func(i int) agg.Input { return input(expr.Int(rng(i).Int64() - math.MaxInt64/2)) }},
		{"sum Decimal", spec(catalog.AggSum, nil, d384), func(i int) agg.Input { return input(wideDecimal(i)) }},
		{"sum Float64", spec(catalog.AggSum, nil, f64), func(i int) agg.Input { return input(expr.Float(wideFloat(rng(i)))) }},
		{"avg Float64", spec(catalog.AggAvg, nil, f64), func(i int) agg.Input { return input(expr.Float(wideFloat(rng(i)))) }},
		{"avg Decimal", spec(catalog.AggAvg, nil, d384), func(i int) agg.Input { return input(wideDecimal(i)) }},
		{"min String", spec(catalog.AggMin, nil, strT), func(i int) agg.Input { return input(expr.String(randString(rng(i)))) }},
		{"max Float64", spec(catalog.AggMax, nil, f64), func(i int) agg.Input { return input(expr.Float(float64(rng(i).IntN(100)) / 8)) }},
		{"max Decimal", spec(catalog.AggMax, nil, d384), func(i int) agg.Input { return input(wideDecimal(i)) }},
		{"argMax", spec(catalog.AggArgMax, nil, strT, i64), func(i int) agg.Input {
			r := rng(i)
			return event(i, expr.String(randString(r)), expr.Int(int64(r.IntN(3))))
		}},
		{"argMin", spec(catalog.AggArgMin, nil, i64, strT), func(i int) agg.Input {
			return event(i, expr.Int(int64(i)), expr.String(randString(rng(i))))
		}},
		{"any", spec(catalog.AggAny, nil, strT), func(i int) agg.Input { return event(i, expr.String(randString(rng(i)))) }},
		{"uniq", spec(catalog.AggUniq, nil, u64), func(i int) agg.Input { return input(expr.Uint(uint64(rng(i).IntN(5000)))) }},
		{"uniqExact", spec(catalog.AggUniqExact, nil, strT), func(i int) agg.Input { return input(expr.String(randString(rng(i)))) }},
		{"read-time uniqExact", readTime(spec(catalog.AggUniqExact, nil, u64)), func(i int) agg.Input {
			return input(expr.Uint(uint64(rng(i).IntN(300))))
		}},
		{"quantile", spec(catalog.AggQuantile, []float64{0.9}, f64), func(i int) agg.Input { return input(expr.Float(wideFloat(rng(i)))) }},
		{"quantileDD", spec(catalog.AggQuantileDD, []float64{0.002, 0.25}, i64), func(i int) agg.Input {
			return input(expr.Int(rng(i).Int64N(1<<40) - 1<<20))
		}},
	}
}

// split shuffles the first n inputs into three parts. An idempotent aggregate also gets
// some inputs in two parts.
func split(r *rand.Rand, n int, idempotent bool) [3][]int {
	var parts [3][]int
	for _, i := range r.Perm(n) {
		p := r.IntN(3)
		parts[p] = append(parts[p], i)
		if idempotent && r.IntN(4) == 0 {
			parts[(p+1)%3] = append(parts[(p+1)%3], i)
		}
	}
	return parts
}

// TestStateDependsOnlyOnInputs checks that every state, and its finalized value, is the same
// whatever the order of its inputs, how they are split over states, and the merge tree, with
// every partial state stored and read back.
func TestStateDependsOnlyOnInputs(t *testing.T) {
	t.Parallel()
	const n = 1500
	all := upTo(n)
	for _, c := range orderCases() {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := compile(t, c.spec)
			want := addAll(t, f.New(), c.gen, all)
			r := rand.New(rand.NewPCG(1, 2))
			for trial := range 4 {
				var states [3]agg.State
				for p, idx := range split(r, n, c.spec.Agg.Fn.Idempotent() && c.spec.Agg.Fn != catalog.AggUniqExact) {
					states[p] = stored(t, f, c.spec, addAll(t, f.New(), c.gen, idx))
				}
				if diff := compareStates(c.spec, mergeTree(t, f, c.spec, states, trial), want); diff != "" {
					t.Fatalf("trial %d: %s", trial, diff)
				}
			}
		})
	}
}

func upTo(n int) []int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	return idx
}

// addAll adds the inputs gen makes for idx to s.
func addAll(t *testing.T, s agg.State, gen func(int) agg.Input, idx []int) agg.State {
	t.Helper()
	for _, i := range idx {
		if err := s.Add(gen(i)); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	return s
}

// stored returns s after a round trip through its attribute value, or s for a state that is
// never stored.
func stored(t *testing.T, f *agg.Func, s agg.Spec, st agg.State) agg.State {
	t.Helper()
	if s.ReadTime || st.Empty() {
		return st
	}
	back := decode(t, f, encode(t, st))
	if diff := compareStates(s, back, st); diff != "" {
		t.Fatalf("round trip: %s", diff)
	}
	return back
}

func mergeTree(t *testing.T, f *agg.Func, s agg.Spec, st [3]agg.State, shape int) agg.State {
	t.Helper()
	merge := func(a, b agg.State) agg.State {
		if err := a.Merge(b); err != nil {
			t.Fatal(err)
		}
		return stored(t, f, s, a)
	}
	switch shape % 3 {
	case 0:
		return merge(merge(st[0], st[1]), st[2])
	case 1:
		return merge(st[2], merge(st[1], st[0]))
	}
	return merge(merge(merge(f.New(), st[1]), st[2]), st[0])
}

// compareStates describes how two states differ, in their attribute values or their
// finalized values, or returns "".
func compareStates(s agg.Spec, a, b agg.State) string {
	if a.Empty() != b.Empty() {
		return fmt.Sprintf("Empty %v, want %v", a.Empty(), b.Empty())
	}
	if !s.ReadTime && !a.Empty() {
		av, aerr := a.Encode()
		bv, berr := b.Encode()
		if aerr != nil || berr != nil || !reflect.DeepEqual(av, bv) {
			return fmt.Sprintf("Encode %+v (%v), want %+v (%v)", av, aerr, bv, berr)
		}
	}
	av, aerr := a.Final()
	bv, berr := b.Final()
	if fmt.Sprint(aerr) != fmt.Sprint(berr) || av.IsNull() != bv.IsNull() || !sameValue(s, av, bv) {
		return fmt.Sprintf("Final %+v (%v), want %+v (%v)", av, aerr, bv, berr)
	}
	return ""
}

func sameValue(s agg.Spec, a, b expr.Value) bool {
	if a.IsNull() || b.IsNull() {
		return a.IsNull() == b.IsNull()
	}
	var t catalog.Type
	switch s.Agg.Fn {
	case catalog.AggCount, catalog.AggUniq, catalog.AggUniqExact:
		t = typ(catalog.KindUInt64)
	case catalog.AggAvg, catalog.AggQuantile, catalog.AggQuantileDD:
		t = typ(catalog.KindFloat64)
	case catalog.AggSum, catalog.AggMin, catalog.AggMax, catalog.AggArgMax, catalog.AggArgMin, catalog.AggAny:
		t = s.ArgTypes[0]
	}
	return expr.Compare(a, t, b, t) == 0
}

func TestCompileRejects(t *testing.T) {
	t.Parallel()
	u64, strT := typ(catalog.KindUInt64), typ(catalog.KindString)
	cases := []struct {
		name string
		spec agg.Spec
		want string
	}{
		{"unknown function", spec("median", nil, u64), "unknown aggregate function"},
		{"count with an argument", spec(catalog.AggCount, nil, u64), "takes 0 arguments"},
		{"argMax with one argument", spec(catalog.AggArgMax, nil, u64), "takes 2 arguments"},
		{"quantile without level", spec(catalog.AggQuantile, nil, u64), "1 parameters"},
		{"array argument", spec(catalog.AggUniq, nil, catalog.Type{Kind: catalog.KindUInt64, Array: true}), "not a scalar"},
		{"sum of String", spec(catalog.AggSum, nil, strT), "doesn't take String"},
		{"sum of Bool", spec(catalog.AggSum, nil, typ(catalog.KindBool)), "doesn't take Bool"},
		{"quantile of Decimal", spec(catalog.AggQuantile, []float64{0.5}, decimal(10, 2)), "doesn't take Decimal(10, 2)"},
		{"max of UUID", spec(catalog.AggMax, nil, typ(catalog.KindUUID)), "doesn't take UUID"},
		{"argMin by UUID", spec(catalog.AggArgMin, nil, strT, typ(catalog.KindUUID)), "argument 2"},
		{"rel_acc below the floor", spec(catalog.AggQuantileDD, []float64{0.001, 0.5}, u64), "rel_acc 0.001"},
		{"rel_acc of 1", spec(catalog.AggQuantileDD, []float64{1, 0.5}, u64), "rel_acc 1"},
		{"level above 1", spec(catalog.AggQuantile, []float64{1.5}, u64), "level 1.5"},
		{"NaN level", spec(catalog.AggQuantile, []float64{math.NaN()}, u64), "level NaN"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			_, err := agg.Compile(c.spec)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Compile error %v, want one containing %q", err, c.want)
			}
		})
	}
}

func TestAddRejectsBadInput(t *testing.T) {
	t.Parallel()
	strT := typ(catalog.KindString)
	cases := []struct {
		name string
		spec agg.Spec
		in   agg.Input
	}{
		{"NULL argument", spec(catalog.AggMax, nil, nullable(strT)), input(expr.Null())},
		{"missing argument", spec(catalog.AggSum, nil, typ(catalog.KindInt64)), input()},
		{"extra argument", spec(catalog.AggCount, nil), input(expr.Int(1))},
		{"array index past the longest array", spec(catalog.AggAny, nil, strT),
			agg.Input{Args: []expr.Value{expr.String("x")}, EventID: eventID(1), ArrayIndex: catalog.MaxArrayLen + 1}},
		{"event_time before 1970", spec(catalog.AggAny, nil, strT),
			agg.Input{Args: []expr.Value{expr.String("x")}, EventTime: -1, EventID: eventID(1)}},
		// 2106-02-07 06:28:15 UTC, the first instant past DateTime64(6).
		{"event_time past 2106", spec(catalog.AggArgMax, nil, strT, strT),
			agg.Input{Args: []expr.Value{expr.String("x"), expr.String("y")}, EventTime: 4_294_967_295_000_000, EventID: eventID(1)}},
		{"event_id of 25 characters", spec(catalog.AggArgMin, nil, strT, strT),
			agg.Input{Args: []expr.Value{expr.String("x"), expr.String("y")}, EventID: eventID(1)[1:]}},
		{"no event_id", spec(catalog.AggAny, nil, strT), input(expr.String("x"))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := compile(t, c.spec).New().Add(c.in)
			if err == nil || errors.Is(err, expr.ErrEval) {
				t.Fatalf("Add error %v, want a non-evaluation error", err)
			}
		})
	}
}

func TestMergeRejectsAnotherAggregate(t *testing.T) {
	t.Parallel()
	a := compile(t, spec(catalog.AggCount, nil))
	b := compile(t, spec(catalog.AggCount, nil))
	if err := a.New().Merge(b.New()); err == nil {
		t.Fatal("Merge of states of two Funcs succeeded")
	}
	if err := a.New().Merge(compile(t, spec(catalog.AggUniq, nil, typ(catalog.KindString))).New()); err == nil {
		t.Fatal("Merge of a uniq state into a count succeeded")
	}
}

func TestEmptyState(t *testing.T) {
	t.Parallel()
	u32, strT := typ(catalog.KindUInt32), typ(catalog.KindString)
	cases := []struct {
		name string
		spec agg.Spec
		want expr.Value
	}{
		{"count", spec(catalog.AggCount, nil), expr.Uint(0)},
		{"sum", spec(catalog.AggSum, nil, u32), expr.Uint(0)},
		{"sum of Nullable", spec(catalog.AggSum, nil, nullable(u32)), expr.Null()},
		{"sum of Float64", spec(catalog.AggSum, nil, typ(catalog.KindFloat64)), expr.Float(0)},
		{"min of String", spec(catalog.AggMin, nil, strT), expr.String("")},
		{"max of Date", spec(catalog.AggMax, nil, typ(catalog.KindDate)), expr.Micros(0)},
		{"argMax", spec(catalog.AggArgMax, nil, typ(catalog.KindInt8), strT), expr.Int(0)},
		{"any of Nullable", spec(catalog.AggAny, nil, nullable(strT)), expr.Null()},
		{"avg", spec(catalog.AggAvg, nil, u32), expr.Null()},
		{"uniq", spec(catalog.AggUniq, nil, strT), expr.Uint(0)},
		{"uniqExact", spec(catalog.AggUniqExact, nil, strT), expr.Uint(0)},
		{"quantile", spec(catalog.AggQuantile, []float64{0.5}, u32), expr.Null()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := compile(t, c.spec)
			checkEmpty(t, c.spec, f.New(), c.want)
			s := f.New()
			if err := s.Merge(f.New()); err != nil {
				t.Fatal(err)
			}
			checkEmpty(t, c.spec, s, c.want)
		})
	}
}

// checkEmpty checks that s is empty, has no attribute value and finalizes to want.
func checkEmpty(t *testing.T, sp agg.Spec, s agg.State, want expr.Value) {
	t.Helper()
	if !s.Empty() {
		t.Fatal("the state isn't empty")
	}
	if _, err := s.Encode(); err == nil {
		t.Fatal("Encode of an empty state succeeded")
	}
	got := final(t, s)
	if got.IsNull() != want.IsNull() || !want.IsNull() && !sameValue(sp, got, want) {
		t.Fatalf("Final = %+v, want %+v", got, want)
	}
}

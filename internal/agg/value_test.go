// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg_test

import (
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

func TestMinMax(t *testing.T) {
	t.Parallel()
	strT, d := typ(catalog.KindString), decimal(10, 2)
	cases := []struct {
		name   string
		spec   agg.Spec
		inputs []expr.Value
		stored store.Value
	}{
		// Strings compare byte by byte: 'B' < 'a' < 'z' < "é" (0xC3 0xA9).
		{"max of String", spec(catalog.AggMax, nil, strT),
			[]expr.Value{expr.String("a"), expr.String("é"), expr.String("B"), expr.String("z")}, str("é")},
		{"min of String", spec(catalog.AggMin, nil, strT),
			[]expr.Value{expr.String("a"), expr.String("é"), expr.String("B"), expr.String("z")}, str("B")},
		{"min of Decimal", spec(catalog.AggMin, nil, d), []expr.Value{dec("150"), dec("-5"), dec("0")}, num("-0.05")},
		{"max of Bool", spec(catalog.AggMax, nil, typ(catalog.KindBool)),
			[]expr.Value{expr.Bool(false), expr.Bool(true), expr.Bool(false)}, store.Value{Type: store.BOOL, Bool: true}},
		{"min of Date", spec(catalog.AggMin, nil, typ(catalog.KindDate)),
			[]expr.Value{expr.Micros(86_400_000_000), expr.Micros(0)}, str("1970-01-01")},
		{"max of Float64", spec(catalog.AggMax, nil, typ(catalog.KindFloat64)),
			[]expr.Value{expr.Float(-2.5), expr.Float(1e125), expr.Float(0)}, num("100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := compile(t, c.spec)
			s := f.New()
			for _, v := range c.inputs {
				if err := s.Add(input(v)); err != nil {
					t.Fatal(err)
				}
			}
			if got := encode(t, s); !reflect.DeepEqual(got, c.stored) {
				t.Fatalf("Encode = %+v, want %+v", got, c.stored)
			}
		})
	}
}

// TestPickTieBreaks checks argMax, argMin and any against sql.md §7.2: ties on val go to the
// larger (argMax) or smaller (argMin) (event_time, event_id, array_index); any keeps the
// smallest of these.
func TestPickTieBreaks(t *testing.T) {
	t.Parallel()
	strT, i64 := typ(catalog.KindString), typ(catalog.KindInt64)
	row := func(arg string, val, time int64, id, index int) agg.Input {
		return agg.Input{Args: []expr.Value{expr.String(arg), expr.Int(val)}, EventTime: time, EventID: eventID(id), ArrayIndex: index}
	}
	rows := []agg.Input{
		row("a", 5, 10, 1, 0),
		row("b", 5, 20, 1, 0),
		row("c", 5, 20, 2, 0),
		row("d", 5, 20, 2, 1),
		row("e", 1, 30, 3, 0),
		row("f", 5, 20, 1, 2),
	}
	var anyRows []agg.Input
	for _, r := range rows[1:] {
		r.Args = r.Args[:1]
		anyRows = append(anyRows, r)
	}
	cases := []struct {
		spec   agg.Spec
		rows   []agg.Input
		stored store.Value
	}{
		{spec(catalog.AggArgMax, nil, strT, i64), rows, list(str("d"), num("5"), num("20"), str(eventID(2)), num("1"))},
		{spec(catalog.AggArgMin, nil, strT, i64), rows, list(str("e"), num("1"), num("30"), str(eventID(3)), num("0"))},
		{spec(catalog.AggArgMin, nil, strT, i64), rows[:4], list(str("a"), num("5"), num("10"), str(eventID(1)), num("0"))},
		{spec(catalog.AggAny, nil, strT), anyRows, list(str("b"), num("20"), str(eventID(1)), num("0"))},
	}
	for _, c := range cases {
		t.Run(string(c.spec.Agg.Fn), func(t *testing.T) {
			t.Parallel()
			f := compile(t, c.spec)
			// Every order gives the same state.
			for shift := range c.rows {
				st := state(t, f, slices.Concat(c.rows[shift:], c.rows[:shift])...)
				if got := encode(t, st); !reflect.DeepEqual(got, c.stored) {
					t.Fatalf("Encode = %+v, want %+v", got, c.stored)
				}
			}
			want := c.stored.List[0].Str
			if got := final(t, decode(t, f, c.stored)); got.AsString() != want {
				t.Fatalf("Final = %q, want %q", got.AsString(), want)
			}
		})
	}
}

// TestViewValueLimits checks the limits of sql.md §5.8 on values that reach min, max,
// argMax, argMin or any, on every value, kept or not: a String over 4 KB is left out, and a
// Float64 outside DynamoDB's range is an evaluation error. Neither changes the state.
// Read-time aggregates have no such limit.
func TestViewValueLimits(t *testing.T) {
	t.Parallel()
	strT, f64, i64 := typ(catalog.KindString), typ(catalog.KindFloat64), typ(catalog.KindInt64)
	// long sorts last, so a max or an argMax by it would keep it.
	long := expr.String(strings.Repeat("~", agg.MaxStringBytes+1))
	full := expr.String(strings.Repeat("z", agg.MaxStringBytes))
	cases := []struct {
		name string
		spec agg.Spec
		// first is added before in, which then fails in a view with want.
		first, in agg.Input
		want      error
	}{
		{"max of a long String", spec(catalog.AggMax, nil, strT), input(full), input(long), agg.ErrTooLong},
		{"min of a long String it wouldn't keep", spec(catalog.AggMin, nil, strT), input(full), input(long), agg.ErrTooLong},
		{"argMax of a long arg", spec(catalog.AggArgMax, nil, strT, i64),
			input(full, expr.Int(1)), input(long, expr.Int(9)), agg.ErrTooLong},
		{"argMax by a long val", spec(catalog.AggArgMax, nil, i64, strT),
			input(expr.Int(1), full), input(expr.Int(2), long), agg.ErrTooLong},
		{"any of a long String", spec(catalog.AggAny, nil, strT), input(full), input(long), agg.ErrTooLong},
		{"max of a tiny Float64", spec(catalog.AggMax, nil, f64), input(expr.Float(1)), input(expr.Float(1e-131)), expr.ErrEval},
		{"min of a huge Float64", spec(catalog.AggMin, nil, f64), input(expr.Float(math.Nextafter(1e126, 0))), input(expr.Float(1e126)),
			expr.ErrEval},
		{"argMin by a huge Float64", spec(catalog.AggArgMin, nil, strT, f64),
			input(full, expr.Float(-1e125)), input(full, expr.Float(-1e300)), expr.ErrEval},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.first.EventID, c.in.EventID = eventID(1), eventID(2)
			f := compile(t, c.spec)
			s := f.New()
			if err := s.Add(c.first); err != nil {
				t.Fatalf("Add of a value at the limit: %v", err)
			}
			s = decode(t, f, encode(t, s))
			before := encode(t, s)
			if err := s.Add(c.in); !errors.Is(err, c.want) {
				t.Fatalf("Add = %v, want %v", err, c.want)
			}
			if after := encode(t, s); !reflect.DeepEqual(after, before) {
				t.Errorf("Add that failed changed the state from %v to %v", before, after)
			}
			rt := compile(t, readTime(c.spec)).New()
			if err := rt.Add(c.in); err != nil {
				t.Fatalf("read-time Add = %v, want no limit", err)
			}
		})
	}
}

func TestDecodeRejectsMalformedValues(t *testing.T) {
	t.Parallel()
	strT, i64 := typ(catalog.KindString), typ(catalog.KindInt64)
	argMax := spec(catalog.AggArgMax, nil, strT, i64)
	id, long := str(eventID(1)), str(strings.Repeat("a", agg.MaxStringBytes+1))
	cases := []struct {
		name string
		spec agg.Spec
		a    store.Value
	}{
		{"max of String as N", spec(catalog.AggMax, nil, strT), num("1")},
		{"Int8 out of range", spec(catalog.AggMin, nil, typ(catalog.KindInt8)), num("128")},
		{"max of a String past the limit", spec(catalog.AggMax, nil, strT), long},
		{"argMax without array_index", argMax, list(str("a"), num("1"), num("2"), id)},
		{"argMax with event_id as N", argMax, list(str("a"), num("1"), num("2"), num("3"), num("0"))},
		{"argMax with event_id of 25 characters", argMax, list(str("a"), num("1"), num("2"), str(eventID(1)[1:]), num("0"))},
		{"argMax with event_time past 2106", argMax, list(str("a"), num("1"), num("4294967295000000"), id, num("0"))},
		{"argMax with array_index past the longest array", argMax, list(str("a"), num("1"), num("2"), id, num("1001"))},
		{"argMax with an arg past the limit", argMax, list(long, num("1"), num("2"), id, num("0"))},
		{"argMin with a val past the limit", spec(catalog.AggArgMin, nil, i64, strT), list(num("1"), long, num("2"), id, num("0"))},
		{"any of a String past the limit", spec(catalog.AggAny, nil, strT), list(long, num("2"), id, num("0"))},
		{"any with a val", spec(catalog.AggAny, nil, strT), list(str("a"), num("1"), num("2"), id, num("0"))},
	}
	if _, err := compile(t, argMax).Decode(list(str("a"), num("1"), num("4294967294999999"), id, num("1000"))); err != nil {
		t.Fatalf("Decode of a valid argMax state: %v", err)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if _, err := compile(t, c.spec).Decode(c.a); err == nil {
				t.Fatal("Decode succeeded, want an error")
			}
		})
	}
}

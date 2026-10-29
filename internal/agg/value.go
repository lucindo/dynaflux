// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package agg

import (
	"cmp"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// pickFields counts event_time, event_id and array_index in an argMax, argMin or any state.
const pickFields = 3

// checkValue applies a view's limits to a value that reaches min, max, argMax, argMin or
// any: a String of at most [MaxStringBytes], and a Float64 that a DynamoDB number can hold.
// Every value is checked, not only the one the state keeps, so what a view keeps doesn't
// depend on the order of events or on how they are spread over items.
func (f *Func) checkValue(i int, v expr.Value) error {
	t := f.args[i]
	switch {
	case f.readTime:
	case t.Kind == catalog.KindString && len(v.AsString()) > MaxStringBytes:
		return fmt.Errorf("%w: %s argument %d is a String of %d bytes, longer than %d",
			ErrTooLong, f.fn, i+1, len(v.AsString()), MaxStringBytes)
	case t.Kind == catalog.KindFloat64:
		if _, _, err := keys.EncodeValue(t, v); err != nil {
			return evalError("%s argument %d: %v", f.fn, i+1, err)
		}
	}
	return nil
}

// decodeValue decodes stored argument i, which a view never writes past [MaxStringBytes].
func (f *Func) decodeValue(i int, a store.Value) (expr.Value, error) {
	v, err := keys.DecodeValue(f.args[i], a, true)
	if err == nil && f.args[i].Kind == catalog.KindString && len(v.AsString()) > MaxStringBytes {
		return expr.Value{}, fmt.Errorf("a String of %d bytes is longer than %d", len(v.AsString()), MaxStringBytes)
	}
	return v, err
}

// extreme is the state of min and max: the smallest or largest value.
type extreme struct {
	f   *Func
	v   expr.Value
	has bool
}

func (s *extreme) Add(in Input) error {
	if err := s.f.check(in); err != nil {
		return err
	}
	if err := s.f.checkValue(0, in.Args[0]); err != nil {
		return err
	}
	s.keep(in.Args[0])
	return nil
}

func (s *extreme) keep(v expr.Value) {
	if s.has {
		c := expr.Compare(v, s.f.args[0], s.v, s.f.args[0])
		if s.f.fn == catalog.AggMax && c <= 0 || s.f.fn == catalog.AggMin && c >= 0 {
			return
		}
	}
	s.v, s.has = v, true
}

func (s *extreme) Merge(o State) error {
	x, ok := o.(*extreme)
	if !ok || x.f != s.f {
		return errMerge(s.f)
	}
	if x.has {
		s.keep(x.v)
	}
	return nil
}

func (s *extreme) Empty() bool { return !s.has }

func (s *extreme) Encode() (store.Value, error) {
	if !s.has {
		return store.Value{}, errEmpty
	}
	a, _, err := keys.EncodeValue(s.f.args[0], s.v)
	return a, err
}

func (s *extreme) Final() (expr.Value, error) {
	if !s.has {
		return s.f.none(zero(s.f.args[0].Kind)), nil
	}
	return s.v, nil
}

func (s *extreme) decode(a store.Value) error {
	v, err := s.f.decodeValue(0, a)
	if err != nil {
		return err
	}
	s.v, s.has = v, true
	return nil
}

func (s *extreme) maxSize() int { return size(worstValue(s.f.args[0])) }

// entry is the input row an argMax, argMin or any state keeps. any has no val.
type entry struct {
	arg, val expr.Value
	time     int64
	id       string
	index    int
}

// pick is the state of argMax, argMin and any: the input whose (val, event_time, event_id,
// array_index) is the largest for argMax and the smallest for argMin, and whose
// (event_time, event_id, array_index) is the smallest for any (sql.md Dev 8).
type pick struct {
	f   *Func
	e   entry
	has bool
}

func (s *pick) Add(in Input) error {
	if err := s.f.check(in); err != nil {
		return err
	}
	for i, v := range in.Args {
		if err := s.f.checkValue(i, v); err != nil {
			return err
		}
	}
	if !s.f.readTime {
		if err := checkEvent(in); err != nil {
			return fmt.Errorf("%s: %w", s.f.fn, err)
		}
	}
	e := entry{arg: in.Args[0], time: in.EventTime, id: in.EventID, index: in.ArrayIndex}
	if s.f.fn != catalog.AggAny {
		e.val = in.Args[1]
	}
	s.keep(e)
	return nil
}

// checkEvent checks the fields of a view's input row that a state stores beside its values.
func checkEvent(in Input) error {
	switch {
	case in.EventTime < 0 || in.EventTime >= catalog.TimeLimitMicros:
		return fmt.Errorf("event_time %d is outside 1970 to 2106", in.EventTime)
	case len(in.EventID) != eventIDLen:
		return fmt.Errorf("event_id %q is not %d characters", in.EventID, eventIDLen)
	case in.ArrayIndex < 0 || in.ArrayIndex > catalog.MaxArrayLen:
		return fmt.Errorf("array index %d is outside 0 to %d", in.ArrayIndex, catalog.MaxArrayLen)
	}
	return nil
}

func (s *pick) keep(e entry) {
	if s.has {
		c := s.compare(e, s.e)
		if s.f.fn == catalog.AggArgMax && c <= 0 || s.f.fn != catalog.AggArgMax && c >= 0 {
			return
		}
	}
	s.e, s.has = e, true
}

func (s *pick) compare(a, b entry) int {
	c := 0
	if s.f.fn != catalog.AggAny {
		c = expr.Compare(a.val, s.f.args[1], b.val, s.f.args[1])
	}
	return cmp.Or(c, cmp.Compare(a.time, b.time), strings.Compare(a.id, b.id), cmp.Compare(a.index, b.index))
}

func (s *pick) Merge(o State) error {
	x, ok := o.(*pick)
	if !ok || x.f != s.f {
		return errMerge(s.f)
	}
	if x.has {
		s.keep(x.e)
	}
	return nil
}

func (s *pick) Empty() bool { return !s.has }

// Encode returns [arg, val, event_time, event_id, array_index], without val for any.
func (s *pick) Encode() (store.Value, error) {
	if !s.has {
		return store.Value{}, errEmpty
	}
	list := make([]store.Value, 0, len(s.f.args)+pickFields)
	for i, t := range s.f.args {
		v := s.e.arg
		if i == 1 {
			v = s.e.val
		}
		a, _, err := keys.EncodeValue(t, v)
		if err != nil {
			return store.Value{}, err
		}
		list = append(list, a)
	}
	t, _, err := keys.EncodeValue(catalog.EventTimeType(), expr.Micros(s.e.time))
	if err != nil {
		return store.Value{}, fmt.Errorf("event_time: %w", err)
	}
	list = append(list, t, store.Value{Type: store.S, Str: s.e.id},
		store.Value{Type: store.N, Str: strconv.Itoa(s.e.index)})
	return store.Value{Type: store.L, List: list}, nil
}

func (s *pick) Final() (expr.Value, error) {
	if !s.has {
		return s.f.none(zero(s.f.args[0].Kind)), nil
	}
	return s.e.arg, nil
}

func (s *pick) decode(a store.Value) error {
	want := len(s.f.args) + pickFields
	if a.Type != store.L || len(a.List) != want {
		return fmt.Errorf("a %s state must be a DynamoDB L of %d values", s.f.fn, want)
	}
	l := a.List
	var err error
	if s.e.arg, err = s.f.decodeValue(0, l[0]); err != nil {
		return fmt.Errorf("arg: %w", err)
	}
	if s.f.fn != catalog.AggAny {
		if s.e.val, err = s.f.decodeValue(1, l[1]); err != nil {
			return fmt.Errorf("val: %w", err)
		}
		l = l[1:]
	}
	t, err := keys.DecodeValue(catalog.EventTimeType(), l[1], true)
	if err != nil {
		return fmt.Errorf("event_time: %w", err)
	}
	if l[2].Type != store.S || len(l[2].Str) != eventIDLen {
		return fmt.Errorf("event_id must be a DynamoDB S of %d characters", eventIDLen)
	}
	i, err := keys.DecodeExact(l[3], 0)
	if err != nil || i.Sign() < 0 || i.Cmp(big.NewInt(catalog.MaxArrayLen)) > 0 {
		return fmt.Errorf("array_index %q is not an ARRAY JOIN position", l[3].Str)
	}
	s.e.time, s.e.id, s.e.index = t.AsMicros(), l[2].Str, int(i.Int64())
	s.has = true
	return nil
}

func (s *pick) maxSize() int {
	list := []store.Value{worstValue(s.f.args[0])}
	if s.f.fn != catalog.AggAny {
		list = append(list, worstValue(s.f.args[1]))
	}
	list = append(list, worstValue(catalog.EventTimeType()), text(eventIDLen), nines(digits(catalog.MaxArrayLen)))
	return size(store.Value{Type: store.L, List: list})
}

// zero returns the default value of a kind (sql.md §7.1).
func zero(k catalog.Kind) expr.Value {
	switch {
	case k == catalog.KindString:
		return expr.String("")
	case k == catalog.KindFloat64:
		return expr.Float(0)
	case k == catalog.KindDecimal:
		return expr.Decimal(new(big.Int))
	case k == catalog.KindBool:
		return expr.Bool(false)
	case k == catalog.KindUUID:
		return expr.UUID([16]byte{})
	case k == catalog.KindDate || k == catalog.KindDateTime || k == catalog.KindDateTime64:
		return expr.Micros(0)
	case isSigned(k):
		return expr.Int(0)
	}
	return expr.Uint(0)
}

// intBits returns the width of an integer kind and whether it is signed; 0 for other kinds.
func intBits(k catalog.Kind) (int, bool) {
	const bits8, bits16, bits32, bits64 = 8, 16, 32, 64
	switch k {
	case catalog.KindUInt8:
		return bits8, false
	case catalog.KindUInt16:
		return bits16, false
	case catalog.KindUInt32:
		return bits32, false
	case catalog.KindUInt64:
		return bits64, false
	case catalog.KindInt8:
		return bits8, true
	case catalog.KindInt16:
		return bits16, true
	case catalog.KindInt32:
		return bits32, true
	case catalog.KindInt64:
		return bits64, true
	case catalog.KindString, catalog.KindFloat64, catalog.KindDecimal, catalog.KindBool, catalog.KindUUID,
		catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
	}
	return 0, false
}

func isInteger(k catalog.Kind) bool {
	b, _ := intBits(k)
	return b > 0
}

func isSigned(k catalog.Kind) bool {
	_, signed := intBits(k)
	return signed
}

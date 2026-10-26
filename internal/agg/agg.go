// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package agg implements the aggregate states of docs/spec/encoding.md §7.2–§7.5: what a
// view stores for an aggregate column, how inputs and other states merge into it, its
// finalized value, and the largest attribute it can need (§7.8).
//
// A state depends only on the multiset of inputs that reached it: not on their order, on how
// they were split over items and merged, or on the CPU (§9). Float code therefore uses only
// correctly rounded operations, wraps every product and quotient in float64(…) so the
// compiler can't fuse it into a multiply-add, and builds its tables with math/big.
package agg

import (
	"errors"
	"fmt"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

// MaxStringBytes is the longest String that may reach min, max, argMax, argMin or any in a
// view. A longer one is an evaluation error even when the state wouldn't keep it, so that a
// row's size has a bound and whether a view breaks doesn't depend on the order of events.
const MaxStringBytes = 4096

// Spec describes an aggregate state.
type Spec struct {
	// Agg is the aggregate. Its Fn and Params define the state. Of its Type only Nullable
	// is read: the value of an empty state is then NULL. Args and Cond are for the caller
	// to evaluate.
	Agg catalog.Aggregate
	// ArgTypes are the types of the arguments the state aggregates. For a -Merge, they are
	// the arguments of the view column's aggregate.
	ArgTypes []catalog.Type
	// ReadTime marks a query aggregate over fetched rows. Its state is never stored, the
	// view limits on values don't apply, and uniqExact keeps a set of values.
	ReadTime bool
}

// Input is one input row.
type Input struct {
	// Args are the argument values, in ArgTypes order. None is NULL: the caller skips rows
	// with a NULL argument.
	Args []expr.Value
	// EventTime (microseconds), EventID and ArrayIndex (the 1-based position in the ARRAY
	// JOIN array, 0 without one) break ties in argMax, argMin and any. Each input row of a
	// view has its own (EventID, ArrayIndex). A query gives its rows' positions in key
	// order instead.
	EventTime  int64
	EventID    string
	ArrayIndex int
}

// State is an aggregate state. It is not safe for concurrent use.
type State interface {
	// Add applies one input row. For a view's uniqExact, the caller adds each value whose
	// marker the record owns, once (encoding.md §7.4).
	Add(in Input) error
	// Merge merges o, a state of the same [Func], into this state.
	Merge(o State) error
	// Empty reports whether no input reached the state. An empty state is stored as an
	// absent attribute.
	Empty() bool
	// Encode returns the attribute value of a non-empty state.
	Encode() (store.Value, error)
	// Final returns the finalized value: the "No input rows" value of sql.md §7.1 for an
	// empty state. A result outside its type is an evaluation error.
	Final() (expr.Value, error)
}

// Func is a compiled [Spec]: it makes, decodes and bounds states. It never changes, so it
// is safe for concurrent use.
type Func struct {
	fn       catalog.AggFunc
	args     []catalog.Type // without Nullable: a NULL never reaches a state
	nullable bool
	readTime bool
	level    float64  // quantile, quantileDD
	dd       *mapping // quantile, quantileDD
	make     func() state
}

// state is what a Func needs of every State it makes.
type state interface {
	State
	// decode sets a new state from its attribute value. It rejects any value Encode
	// wouldn't write.
	decode(a store.Value) error
	// maxSize is the largest attribute value a view state can have, as [store.ItemSize]
	// counts it.
	maxSize() int
}

var errEmpty = errors.New("an empty state has no attribute value")

// Compile checks s and returns its Func. Building a quantile's bucket tables takes about a
// millisecond, so compile each aggregate once.
func Compile(s Spec) (*Func, error) {
	f := &Func{fn: s.Agg.Fn, nullable: s.Agg.Type.Nullable, readTime: s.ReadTime}
	args, params, err := f.setClass()
	if err != nil {
		return nil, err
	}
	if len(s.ArgTypes) != args || len(s.Agg.Params) != params {
		return nil, fmt.Errorf("%s takes %d arguments and %d parameters, not %d and %d",
			f.fn, args, params, len(s.ArgTypes), len(s.Agg.Params))
	}
	for i, t := range s.ArgTypes {
		if argErr := checkArg(f.fn, i, t); argErr != nil {
			return nil, fmt.Errorf("%s argument %d: %w", f.fn, i+1, argErr)
		}
		t.Nullable = false
		f.args = append(f.args, t)
	}
	if err = f.setParams(s.Agg.Params); err != nil {
		return nil, fmt.Errorf("%s: %w", f.fn, err)
	}
	return f, nil
}

// New returns an empty state.
func (f *Func) New() State { return f.make() }

// Decode returns the state an attribute value holds. A missing attribute is the empty
// state, from [Func.New].
func (f *Func) Decode(a store.Value) (State, error) {
	s := f.make()
	if err := s.decode(a); err != nil {
		return nil, fmt.Errorf("decode %s state: %w", f.fn, err)
	}
	return s, nil
}

// MaxSize returns the largest attribute value a view state can have, as [store.ItemSize]
// counts it, for at most 2^64 inputs, event IDs of 26 characters and ARRAY JOIN positions up
// to [catalog.MaxArrayLen] (encoding.md §7.8).
func (f *Func) MaxSize() int { return f.make().maxSize() }

// setClass picks the state of f's function and returns its argument and parameter counts.
func (f *Func) setClass() (int, int, error) {
	args, params := 1, 0
	switch f.fn {
	case catalog.AggCount:
		f.make, args = func() state { return &sum{f: f} }, 0
	case catalog.AggSum:
		f.make = func() state { return &sum{f: f} }
	case catalog.AggUniqExact:
		f.make = func() state { return &sum{f: f} }
		if f.readTime {
			f.make = func() state { return &set{f: f} }
		}
	case catalog.AggAvg:
		f.make = func() state { return &avg{f: f} }
	case catalog.AggMin, catalog.AggMax:
		f.make = func() state { return &extreme{f: f} }
	case catalog.AggAny:
		f.make = func() state { return &pick{f: f} }
	case catalog.AggArgMax, catalog.AggArgMin:
		f.make, args = func() state { return &pick{f: f} }, 2
	case catalog.AggUniq:
		f.make = func() state { return &hll{f: f} }
	case catalog.AggQuantile:
		f.make, params = func() state { return &sketch{f: f} }, 1
	case catalog.AggQuantileDD:
		f.make, params = func() state { return &sketch{f: f} }, 2
	default:
		return 0, 0, fmt.Errorf("unknown aggregate function %q", f.fn)
	}
	return args, params, nil
}

// checkArg reports an argument type that fn can't aggregate (sql.md §7.1).
func checkArg(fn catalog.AggFunc, i int, t catalog.Type) error {
	k := t.Kind
	var ok bool
	switch {
	case t.Array || k < catalog.KindString || k > catalog.KindDateTime64:
		return fmt.Errorf("%s is not a scalar type", t)
	case fn == catalog.AggSum || fn == catalog.AggAvg:
		ok = isInteger(k) || k == catalog.KindFloat64 || k == catalog.KindDecimal
	case fn == catalog.AggQuantile || fn == catalog.AggQuantileDD:
		ok = isInteger(k) || k == catalog.KindFloat64
	case fn == catalog.AggMin || fn == catalog.AggMax || i == 1:
		ok = k != catalog.KindUUID
	default:
		ok = true
	}
	if !ok {
		return fmt.Errorf("%s doesn't take %s", fn, t)
	}
	return nil
}

func (f *Func) setParams(ps []float64) error {
	const quantileAccuracy = 0.01
	alpha := quantileAccuracy
	switch f.fn { //nolint:exhaustive // only the parametric functions take parameters
	case catalog.AggQuantile:
		f.level = ps[0]
	case catalog.AggQuantileDD:
		alpha, f.level = ps[0], ps[1]
		if !(alpha >= minRelAcc && alpha < 1) {
			return fmt.Errorf("rel_acc %v is outside [%v, 1)", alpha, minRelAcc)
		}
	default:
		return nil
	}
	if !(f.level >= 0 && f.level <= 1) {
		return fmt.Errorf("level %v is outside [0, 1]", f.level)
	}
	f.dd = newMapping(alpha)
	return nil
}

// check checks the preconditions of an input.
func (f *Func) check(in Input) error {
	if len(in.Args) != len(f.args) {
		return fmt.Errorf("%s takes %d arguments, not %d", f.fn, len(f.args), len(in.Args))
	}
	for i, v := range in.Args {
		if v.IsNull() {
			return fmt.Errorf("%s argument %d is NULL", f.fn, i+1)
		}
	}
	return nil
}

// none returns the value of a state no input reached: NULL for a Nullable result, else def.
func (f *Func) none(def expr.Value) expr.Value {
	if f.nullable {
		return expr.Null()
	}
	return def
}

func errMerge(f *Func) error {
	return fmt.Errorf("merge %s state: the other state is of another aggregate", f.fn)
}

func evalError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", expr.ErrEval, fmt.Sprintf(format, args...))
}

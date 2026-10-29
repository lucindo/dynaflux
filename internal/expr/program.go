// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr

import (
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// ErrEval is an evaluation error (sql.md §5.8). It depends only on the values evaluated,
// so every region fails alike. Every [*Error] matches it.
var ErrEval = errors.New("evaluation error")

// Error is an evaluation error of one operation.
type Error struct {
	Op     catalog.Op
	Reason string
}

func (e *Error) Error() string {
	return "evaluation error in " + string(e.Op) + ": " + e.Reason
}

// Is reports whether target is [ErrEval].
func (e *Error) Is(target error) bool { return target == ErrEval }

// Scope names the slots of a [Frame]: Columns and Params give the names of OpColumn and
// OpParam nodes in slot order, and Aggs is the number of OpAggregate values.
type Scope struct {
	Columns []string
	Params  []string
	Aggs    int
}

// Frame holds the values a [Program] reads, in the slot order of its [Scope].
type Frame struct{ Cols, Params, Aggs []Value }

// Program is a compiled expression. It never changes, so it is safe for concurrent use
// as long as callers don't modify the values it returns.
type Program struct {
	root               node
	cols, params, aggs int
}

// Compile compiles an expression of a catalog that [catalog.Unmarshal] accepted, whose
// types follow sql.md §4.5 and §5.6. It fails on a name that s doesn't hold and on a
// literal that isn't in canonical form.
func Compile(e *catalog.Expr, s Scope) (*Program, error) {
	if e == nil {
		return nil, errors.New("compile expression: nil expression")
	}
	c := compiler{scope: s, setups: setups()}
	root, err := c.compile(e)
	if err != nil {
		return nil, fmt.Errorf("compile expression: %w", err)
	}
	return &Program{root: root, cols: len(s.Columns), params: len(s.Params), aggs: s.Aggs}, nil
}

// Eval evaluates the program on f. A failure of the expression itself is an [*Error]. The
// result may share storage with p and f, such as a Decimal literal's [Value.AsDecimal]: the
// caller must not modify it.
func (p *Program) Eval(f *Frame) (Value, error) {
	if f == nil || len(f.Cols) < p.cols || len(f.Params) < p.params || len(f.Aggs) < p.aggs {
		return Value{}, errors.New("evaluate expression: the frame is smaller than the scope")
	}
	return p.root.eval(&p.root, f)
}

type evalFunc func(n *node, f *Frame) (Value, error)

// node is a compiled operation. Its args are evaluated by eval, which may skip some.
type node struct {
	eval evalFunc
	op   catalog.Op
	typ  catalog.Type
	args []node
	slot int      // the Frame slot of a column, parameter or aggregate
	val  Value    // a literal
	lo   *big.Int // the storage range of a Decimal result
	hi   *big.Int
}

func (n *node) arg(i int, f *Frame) (Value, error) {
	a := &n.args[i]
	return a.eval(a, f)
}

func (n *node) fail(reason string) *Error {
	return &Error{Op: n.op, Reason: reason}
}

// unary returns an op of one operand that gives NULL for NULL (sql.md §5.5).
func unary(fn func(n *node, v Value) (Value, error)) evalFunc {
	return func(n *node, f *Frame) (Value, error) {
		v, err := n.arg(0, f)
		if err != nil || v.IsNull() {
			return Null(), err
		}
		return fn(n, v)
	}
}

// binary returns an op of two operands that gives NULL when either is NULL. It evaluates
// both first, so a NULL doesn't hide the other's error: only the ops of sql.md §5.8 skip
// operands.
func binary(fn func(n *node, a, b Value) (Value, error)) evalFunc {
	return func(n *node, f *Frame) (Value, error) {
		a, err := n.arg(0, f)
		if err != nil {
			return Null(), err
		}
		b, err := n.arg(1, f)
		if err != nil || a.IsNull() || b.IsNull() {
			return Null(), err
		}
		return fn(n, a, b)
	}
}

type compiler struct {
	scope  Scope
	setups map[catalog.Op]setup
}

// setup finishes a node whose args are compiled: it picks eval and checks what the op needs.
type setup func(c *compiler, n *node, e *catalog.Expr) error

func (c *compiler) compile(e *catalog.Expr) (node, error) {
	n := node{op: e.Op, typ: e.Type, args: make([]node, len(e.Args))}
	for i := range e.Args {
		a, err := c.compile(&e.Args[i])
		if err != nil {
			return node{}, err
		}
		n.args[i] = a
	}
	s, ok := c.setups[e.Op]
	if !ok {
		return node{}, fmt.Errorf("unknown op %q", e.Op)
	}
	if err := s(c, &n, e); err != nil {
		return node{}, fmt.Errorf("%s: %w", e.Op, err)
	}
	if n.typ.Kind == catalog.KindDecimal {
		n.lo, n.hi = decimalRange(n.typ.Precision)
	}
	return n, nil
}

// setups returns the setup of every op.
func setups() map[catalog.Op]setup {
	return map[catalog.Op]setup{
		catalog.OpLiteral:   setupLiteral,
		catalog.OpNull:      is(evalNull),
		catalog.OpArray:     is(evalArray),
		catalog.OpColumn:    setupColumn,
		catalog.OpParam:     setupParam,
		catalog.OpAggregate: setupAggregate,

		catalog.OpAnd:             is(evalLogic(false)),
		catalog.OpOr:              is(evalLogic(true)),
		catalog.OpNot:             is(unary(evalNot)),
		catalog.OpIsNull:          is(evalIsNull),
		catalog.OpEquals:          is(evalCompare(func(c int) bool { return c == 0 })),
		catalog.OpNotEquals:       is(evalCompare(func(c int) bool { return c != 0 })),
		catalog.OpLess:            is(evalCompare(func(c int) bool { return c < 0 })),
		catalog.OpLessOrEquals:    is(evalCompare(func(c int) bool { return c <= 0 })),
		catalog.OpGreater:         is(evalCompare(func(c int) bool { return c > 0 })),
		catalog.OpGreaterOrEquals: is(evalCompare(func(c int) bool { return c >= 0 })),
		catalog.OpIn:              is(evalIn),

		catalog.OpPlus:     arith(intOp(func(x, y uint64) uint64 { return x + y }), floatAdd, decimalAdd),
		catalog.OpMinus:    arith(intOp(func(x, y uint64) uint64 { return x - y }), floatSub, decimalSub),
		catalog.OpMultiply: arith(intOp(func(x, y uint64) uint64 { return x * y }), floatMul, decimalMul),
		catalog.OpDivide:   arith(nil, floatDiv, decimalDiv),
		catalog.OpModulo:   arith(intDivision(true), nil, nil),
		catalog.OpIntDiv:   arith(intDivision(false), nil, nil),
		catalog.OpNegate:   is(unary(evalNegate)),

		catalog.OpIf:       choice(evalIf, func(i, _ int) bool { return i > 0 }),
		catalog.OpMultiIf:  choice(evalMultiIf, func(i, n int) bool { return i%2 == 1 || i == n-1 }),
		catalog.OpCoalesce: choice(evalCoalesce, func(int, int) bool { return true }),

		catalog.OpLower:  is(stringOp(LowerASCII)),
		catalog.OpUpper:  is(stringOp(UpperASCII)),
		catalog.OpConcat: is(evalConcat),
		catalog.OpLength: is(unary(evalLength)),

		catalog.OpToDate:               is(floorTime(microsPerDay)),
		catalog.OpToStartOfMinute:      is(floorTime(microsPerMinute)),
		catalog.OpToStartOfFiveMinutes: is(floorTime(fiveMinutes * microsPerMinute)),
		catalog.OpToStartOfHour:        is(floorTime(microsPerHour)),
		catalog.OpToStartOfDay:         is(floorTime(microsPerDay)),
		catalog.OpToStartOfWeek:        setupStartOfWeek,
		catalog.OpToStartOfMonth:       is(unary(evalStartOfMonth)),
		catalog.OpToYYYYMM:             is(unary(evalYYYYMM)),
	}
}

// is returns a setup that only sets eval.
func is(eval evalFunc) setup {
	return func(_ *compiler, n *node, _ *catalog.Expr) error {
		n.eval = eval
		return nil
	}
}

func setupLiteral(_ *compiler, n *node, e *catalog.Expr) error {
	v, err := Literal(e.Type, e.Value)
	n.val, n.eval = v, evalLiteral
	return err
}

func setupColumn(c *compiler, n *node, e *catalog.Expr) error {
	return n.setSlot(c.scope.Columns, e.Name, evalColumn)
}

func setupParam(c *compiler, n *node, e *catalog.Expr) error {
	return n.setSlot(c.scope.Params, e.Name, evalParam)
}

func (n *node) setSlot(names []string, name string, eval evalFunc) error {
	n.slot, n.eval = slices.Index(names, name), eval
	if n.slot < 0 {
		return fmt.Errorf("%q is not in scope", name)
	}
	return nil
}

func setupAggregate(c *compiler, n *node, e *catalog.Expr) error {
	n.slot, n.eval = e.Index, evalAggregate
	if e.Index >= c.scope.Aggs {
		return fmt.Errorf("index %d is not in scope", e.Index)
	}
	return nil
}

func evalLiteral(n *node, _ *Frame) (Value, error)   { return n.val, nil }
func evalNull(*node, *Frame) (Value, error)          { return Null(), nil }
func evalColumn(n *node, f *Frame) (Value, error)    { return f.Cols[n.slot], nil }
func evalParam(n *node, f *Frame) (Value, error)     { return f.Params[n.slot], nil }
func evalAggregate(n *node, f *Frame) (Value, error) { return f.Aggs[n.slot], nil }

func evalArray(n *node, f *Frame) (Value, error) {
	vs := make([]Value, len(n.args))
	for i := range n.args {
		v, err := n.arg(i, f)
		if err != nil {
			return Value{}, err
		}
		vs[i] = v
	}
	return Array(vs), nil
}

// evalLogic returns AND, for which false decides the result, or OR, for which true does.
// It stops at the first operand that decides it; otherwise any NULL operand makes it NULL.
func evalLogic(decisive bool) evalFunc {
	return func(n *node, f *Frame) (Value, error) {
		null := false
		for i := range n.args {
			v, err := n.arg(i, f)
			switch {
			case err != nil:
				return Value{}, err
			case v.IsNull():
				null = true
			case v.AsBool() == decisive:
				return v, nil
			}
		}
		if null {
			return Null(), nil
		}
		return Bool(!decisive), nil
	}
}

func evalNot(_ *node, v Value) (Value, error) { return Bool(!v.AsBool()), nil }

func evalIsNull(n *node, f *Frame) (Value, error) {
	v, err := n.arg(0, f)
	return Bool(v.IsNull()), err
}

// choice returns the setup of a conditional, which converts each value it returns, the
// args where isValue holds, to its own type (sql.md §4.5).
func choice(eval evalFunc, isValue func(i, n int) bool) setup {
	return func(_ *compiler, n *node, _ *catalog.Expr) error {
		for i := range n.args {
			if isValue(i, len(n.args)) {
				n.args[i] = converted(n.args[i], n.typ, n.op)
			}
		}
		n.eval = eval
		return nil
	}
}

func evalIf(n *node, f *Frame) (Value, error) {
	c, err := n.arg(0, f)
	if err != nil {
		return Value{}, err
	}
	if c.IsTrue() {
		return n.arg(1, f)
	}
	return n.arg(len(n.args)-1, f)
}

func evalMultiIf(n *node, f *Frame) (Value, error) {
	last := len(n.args) - 1
	for i := 0; i < last; i += 2 {
		c, err := n.arg(i, f)
		if err != nil {
			return Value{}, err
		}
		if c.IsTrue() {
			return n.arg(i+1, f)
		}
	}
	return n.arg(last, f)
}

// evalCoalesce evaluates every argument, even after the one it returns (sql.md §5.8).
func evalCoalesce(n *node, f *Frame) (Value, error) {
	out := Null()
	for i := range n.args {
		v, err := n.arg(i, f)
		if err != nil {
			return Value{}, err
		}
		if out.IsNull() {
			out = v
		}
	}
	return out, nil
}

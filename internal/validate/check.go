// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// operand is a checked expression.
type operand struct {
	ir catalog.Expr
	// node is the source, without the parentheses around it.
	node sql.Expr
	// pred marks a predicate: a comparison, IN, BETWEEN, IS NULL, AND, OR or NOT.
	pred bool
	// null marks the NULL literal, which has no type until its context gives it one.
	null bool
	// bad marks an expression whose error is already reported, so that its parents
	// report nothing more.
	bad bool
}

func (o operand) typ() catalog.Type { return o.ir.Type }

func badOperand() operand { return operand{bad: true} }

// paramRole is where a parameter appears, for the rules of sql.md §10.3.
type paramRole uint8

const (
	paramValue   paramRole = iota + 1 // anywhere a value can be
	paramInList                       // an item of an IN list
	paramAfterIn                      // right after IN
)

// env is what an expression may use where it appears (sql.md §5.9).
type env struct {
	// place names the clause, such as WHERE or DEFAULT, for DF5204.
	place string
	// column resolves a column reference.
	column func(ref *sql.ColumnRef) operand
	// param resolves a parameter; nil means parameters aren't allowed (DF4403).
	param func(p *sql.Param, role paramRole) operand
	// aggregate checks an aggregate call; nil means aggregates aren't allowed (DF5204).
	aggregate func(c *checker, call *sql.Call, fn aggregateFunc) operand
}

// checker types an expression, lowers it to the catalog IR, and reports what it breaks
// (sql.md §4 to §6).
type checker struct {
	st  *stmt
	env env
	// guards are the divisors known to be non-zero where the expression runs (DF5501).
	guards []catalog.Expr
}

func (c *checker) expr(x sql.Expr) operand {
	switch x := x.(type) {
	case *sql.NumberLit:
		return operand{ir: numberLiteral(x), node: x}
	case *sql.StringLit:
		return operand{ir: literal(catalog.Type{Kind: catalog.KindString}, x.Value), node: x}
	case *sql.BoolLit:
		return operand{ir: boolLiteral(x.Value), node: x}
	case *sql.NullLit:
		return operand{ir: catalog.Expr{Op: catalog.OpNull}, node: x, null: true}
	case *sql.Param:
		return c.param(x, paramValue)
	case *sql.ColumnRef:
		return c.column(x)
	case *sql.Call:
		return c.call(x)
	case *sql.Interval:
		c.st.errorf(x.Pos, "DF5403", "", "INTERVAL is only allowed in TTL and ACCEPT")
	case *sql.ArrayLit:
		c.st.errorf(x.Pos, "DF1102", "write the array as the whole DEFAULT value",
			"array literal inside an expression is not supported")
	case *sql.ParenExpr:
		return c.expr(x.X)
	case *sql.UnaryExpr:
		return c.unary(x)
	case *sql.BinaryExpr:
		return c.binary(x)
	case *sql.InExpr:
		return c.in(x)
	case *sql.BetweenExpr:
		return c.between(x)
	case *sql.IsNullExpr:
		return c.isNull(x)
	}
	return badOperand()
}

func boolLiteral(v bool) catalog.Expr {
	if v {
		return literal(catalog.Type{Kind: catalog.KindBool}, "true")
	}
	return literal(catalog.Type{Kind: catalog.KindBool}, "false")
}

func boolType(nullable bool) catalog.Type {
	return catalog.Type{Kind: catalog.KindBool, Nullable: nullable}
}

func predicate(ir catalog.Expr, x sql.Expr) operand {
	return operand{ir: ir, node: x, pred: true}
}

func (c *checker) column(ref *sql.ColumnRef) operand {
	if len(ref.Qualifier) > 0 {
		c.st.errorf(ref.Pos, "DF5102", "use the column name alone", "qualified name %s is not supported", c.st.text(ref))
		return badOperand()
	}
	return c.env.column(ref)
}

func (c *checker) param(p *sql.Param, role paramRole) operand {
	if c.env.param == nil {
		c.st.errorf(p.Pos, "DF4403", "", "parameters are only allowed in named queries")
		return badOperand()
	}
	return c.env.param(p, role)
}

// value checks x where a value is needed: not a predicate (DF5303), and not NULL
// (DF5314).
func (c *checker) value(x sql.Expr) operand {
	o := c.valueOrNull(x)
	if o.null {
		c.untypedNull(o.node)
		return badOperand()
	}
	return o
}

// valueOrNull checks x where a value or a NULL literal is needed.
func (c *checker) valueOrNull(x sql.Expr) operand {
	o := c.expr(x)
	if o.pred && !o.bad {
		text := c.st.text(x)
		c.st.errorf(x.Extent().Pos, "DF5303", "if("+text+", 1, 0)", "condition %s can't be used as a value", text)
		return badOperand()
	}
	return o
}

// condition checks x where a condition is needed (sql.md §5.4).
func (c *checker) condition(x sql.Expr) operand {
	o := c.expr(x)
	switch {
	case o.bad || isBool(o.typ()) && !o.null:
		return o
	case o.null:
		c.untypedNull(o.node)
	default:
		c.st.errorf(x.Extent().Pos, "DF5302", "use a comparison, IN, IS NULL, AND, OR, NOT or a Bool value",
			"%s is not a condition", c.st.text(x))
	}
	return badOperand()
}

func (c *checker) untypedNull(x sql.Node) {
	c.st.errorf(x.Extent().Pos, "DF5314", "use a typed value, or IS NULL", "NULL has no type here")
}

func (c *checker) typeError(at sql.Node, err *typeError) operand {
	c.st.errorf(at.Extent().Pos, err.code, err.hint, "%s", err.msg)
	return badOperand()
}

// guarded runs check with extra divisors known to be non-zero.
func (c *checker) guarded(extra []catalog.Expr, check func() operand) operand {
	saved := c.guards
	c.guards = append(slices.Clip(saved), extra...)
	defer func() { c.guards = saved }()
	return check()
}

// nonZeroIfTrue returns the divisors that a condition rules out zero for when it is
// true: the d of each top-level AND term d != 0, d <> 0 or d > 0, written either way
// round (sql.md §5.6).
func nonZeroIfTrue(cond catalog.Expr) []catalog.Expr {
	terms := []catalog.Expr{cond}
	if cond.Op == catalog.OpAnd {
		terms = cond.Args
	}
	var out []catalog.Expr
	for _, t := range terms {
		if len(t.Args) != argsOfBinaryFun {
			continue
		}
		l, r := t.Args[0], t.Args[1]
		switch {
		case t.Op == catalog.OpNotEquals && isZeroLiteral(r), t.Op == catalog.OpGreater && isZeroLiteral(r):
			out = append(out, l)
		case t.Op == catalog.OpNotEquals && isZeroLiteral(l), t.Op == catalog.OpLess && isZeroLiteral(l):
			out = append(out, r)
		}
	}
	return out
}

// nonZeroIfFalse returns the divisor that a condition d = 0 rules out zero for when it
// is false.
func nonZeroIfFalse(cond catalog.Expr) []catalog.Expr {
	if cond.Op != catalog.OpEquals {
		return nil
	}
	switch {
	case isZeroLiteral(cond.Args[1]):
		return cond.Args[:1]
	case isZeroLiteral(cond.Args[0]):
		return cond.Args[1:]
	}
	return nil
}

func isZeroLiteral(e catalog.Expr) bool {
	return e.Op == catalog.OpLiteral && isNumeric(e.Type) && e.Value == "0"
}

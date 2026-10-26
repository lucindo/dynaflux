// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"reflect"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
)

const (
	opIn          = "IN"
	hintTimeForms = "the forms are 'YYYY-MM-DD', 'YYYY-MM-DD hh:mm:ss[.ffffff]' and 8-4-4-4-12 hex digits for UUID"
)

func (c *checker) unary(x *sql.UnaryExpr) operand {
	if x.Op == sql.OpNot {
		o := c.condition(x.X)
		if o.bad {
			return o
		}
		return predicate(catalog.Expr{Op: catalog.OpNot, Type: boolType(o.typ().Nullable), Args: []catalog.Expr{o.ir}}, x)
	}
	o := c.value(x.X)
	if o.bad {
		return o
	}
	t, err := negateType(o.typ())
	if err != nil {
		return c.typeError(x, err)
	}
	return operand{ir: catalog.Expr{Op: catalog.OpNegate, Type: t, Args: []catalog.Expr{o.ir}}, node: x}
}

func (c *checker) binary(x *sql.BinaryExpr) operand {
	switch x.Op {
	case sql.OpOr, sql.OpAnd:
		return c.logic(x)
	case sql.OpEq:
		return c.compare(x, catalog.OpEquals)
	case sql.OpNe:
		return c.compare(x, catalog.OpNotEquals)
	case sql.OpLt:
		return c.compare(x, catalog.OpLess)
	case sql.OpLe:
		return c.compare(x, catalog.OpLessOrEquals)
	case sql.OpGt:
		return c.compare(x, catalog.OpGreater)
	case sql.OpGe:
		return c.compare(x, catalog.OpGreaterOrEquals)
	case sql.OpAdd:
		return c.arith(x, catalog.OpPlus)
	case sql.OpSub:
		return c.arith(x, catalog.OpMinus)
	case sql.OpMul:
		return c.arith(x, catalog.OpMultiply)
	case sql.OpDiv:
		return c.arith(x, catalog.OpDivide)
	case sql.OpMod:
	}
	return c.arith(x, catalog.OpModulo)
}

func (c *checker) logic(x *sql.BinaryExpr) operand {
	op := catalog.OpOr
	l := c.condition(x.X)
	var r operand
	if x.Op == sql.OpAnd {
		op = catalog.OpAnd
		var guards []catalog.Expr
		if !l.bad {
			guards = nonZeroIfTrue(l.ir)
		}
		r = c.guarded(guards, func() operand { return c.condition(x.Y) })
	} else {
		r = c.condition(x.Y)
	}
	if l.bad || r.bad {
		return badOperand()
	}
	var args []catalog.Expr
	for _, o := range []operand{l, r} {
		if o.ir.Op == op {
			args = append(args, o.ir.Args...)
		} else {
			args = append(args, o.ir)
		}
	}
	return predicate(catalog.Expr{Op: op, Type: boolType(l.typ().Nullable || r.typ().Nullable), Args: args}, x)
}

// chained reports whether x is a comparison, IN, BETWEEN or IS NULL that isn't in
// parentheses, which makes the predicate around it a chain (DF5304).
func (c *checker) chained(x sql.Expr, opPos diag.Pos) bool {
	b, isBinary := x.(*sql.BinaryExpr)
	_, isIn := x.(*sql.InExpr)
	_, isBetween := x.(*sql.BetweenExpr)
	_, isIsNull := x.(*sql.IsNullExpr)
	predicate := isBinary && isComparison(b.Op) || isIn || isBetween || isIsNull
	if !predicate {
		return false
	}
	c.st.errorf(opPos, "DF5304", "combine them with AND, or add parentheses", "comparisons can't be chained")
	return true
}

func isComparison(op sql.BinaryOp) bool {
	switch op {
	case sql.OpEq, sql.OpNe, sql.OpLt, sql.OpLe, sql.OpGt, sql.OpGe:
		return true
	case sql.OpOr, sql.OpAnd, sql.OpAdd, sql.OpSub, sql.OpMul, sql.OpDiv, sql.OpMod:
	}
	return false
}

func (c *checker) compare(x *sql.BinaryExpr, op catalog.Op) operand {
	if c.chained(x.X, x.OpPos) {
		return badOperand()
	}
	l, r := c.valueOrNull(x.X), c.valueOrNull(x.Y)
	if l.bad || r.bad || c.comparedWithNull(x, l, r) {
		return badOperand()
	}
	li, ri, ok := c.pair(x, x.Op.String(), l, r)
	if !ok {
		return badOperand()
	}
	t := boolType(l.typ().Nullable || r.typ().Nullable)
	return predicate(catalog.Expr{Op: op, Type: t, Args: []catalog.Expr{li, ri}}, x)
}

// comparedWithNull reports a NULL literal operand of a comparison or BETWEEN (DF5305).
func (c *checker) comparedWithNull(at sql.Node, operands ...operand) bool {
	other := "x"
	null := false
	for _, o := range operands {
		if o.null {
			null = true
		} else if other == "x" {
			other = c.st.text(o.node)
		}
	}
	if null {
		c.st.errorf(at.Extent().Pos, "DF5305", "for = and !=, use "+other+" IS [NOT] NULL", "a comparison with NULL is never true")
	}
	return null
}

// pair checks that a comparison, IN or BETWEEN can compare a with b (sql.md §5.7), and
// returns their IR with literals converted to the other side's type (§4.6).
func (c *checker) pair(at sql.Node, op string, a, b operand) (catalog.Expr, catalog.Expr, bool) {
	ai, aok := c.convertLiteral(a, b)
	bi, bok := c.convertLiteral(b, a)
	if !aok || !bok {
		return catalog.Expr{}, catalog.Expr{}, false
	}
	if err := comparePair(op, ai.Type, bi.Type); err != nil {
		c.typeError(at, err)
		return catalog.Expr{}, catalog.Expr{}, false
	}
	return ai, bi, true
}

// convertLiteral converts lit to the type of other when lit is a literal that converts
// to it: a string to a temporal or UUID value, or a number to a Decimal.
func (c *checker) convertLiteral(lit, other operand) (catalog.Expr, bool) {
	target := scalarOf(other.typ())
	if s, ok := lit.node.(*sql.StringLit); ok {
		return c.convertString(s, lit, target)
	}
	n, ok := lit.node.(*sql.NumberLit)
	if !ok || !isDecimal(target) {
		return lit.ir, true
	}
	if d, fits := decimalLiteral(n); fits {
		return d, true
	}
	c.st.errorf(n.Pos, "DF5406", "Decimal values have at most 38 digits; use a literal that fits Decimal(38, S)",
		"%s needs more than 38 digits, so it can't be compared with %s", c.st.text(n), other.typ())
	return catalog.Expr{}, false
}

// convertString parses a string literal compared with a temporal or UUID value
// strictly (DF5401); it never converts to a number or a Bool (DF5402).
func (c *checker) convertString(s *sql.StringLit, lit operand, target catalog.Type) (catalog.Expr, bool) {
	switch {
	case isTemporal(target):
		if tm, ok := parseTime(s.Value, target); ok {
			return literal(target, formatTime(tm, target)), true
		}
	case target.Kind == catalog.KindUUID && !target.Array:
		if id, ok := parseUUID(s.Value); ok {
			return literal(target, id), true
		}
	case isNumeric(target) || isBool(target):
		c.st.errorf(s.Pos, "DF5402", "", "can't compare a string literal with %s", target)
		return catalog.Expr{}, false
	default:
		return lit.ir, true
	}
	c.st.errorf(s.Pos, "DF5401", hintTimeForms, "'%s' is not a valid %s", s.Value, target)
	return catalog.Expr{}, false
}

// comparePair checks the operand types of a comparison, IN or BETWEEN, after literal
// conversion.
func comparePair(op string, a, b catalog.Type) *typeError {
	sa, sb := scalarOf(a), scalarOf(b)
	switch {
	case isNumeric(sa) && isNumeric(sb):
		if isDecimal(sa) && isFloat(sb) || isFloat(sa) && isDecimal(sb) {
			return decimalFloatError()
		}
	case isTemporal(sa) && isTemporal(sb):
	case sa.Array || sb.Array || sa.Kind != sb.Kind:
		return newTypeError("DF5301", "", "can't apply %s to %s and %s", op, a, b)
	case sa.Kind == catalog.KindUUID && op != "=" && op != "!=" && op != opIn:
		return newTypeError("DF5311", "compare UUIDs with = or IN", "UUID values can't be ordered")
	}
	return nil
}

func (c *checker) in(x *sql.InExpr) operand {
	if c.chained(x.X, x.OpPos) {
		return badOperand()
	}
	l := c.value(x.X)
	if l.bad {
		return l
	}
	args := []catalog.Expr{l.ir}
	ok := true
	if x.Param != nil {
		arg, argOK := c.inParam(x, l)
		args, ok = append(args, arg), argOK
	}
	for _, item := range x.List {
		arg, argOK := c.inItem(x, l, item)
		args, ok = append(args, arg), ok && argOK
	}
	if !ok {
		return badOperand()
	}
	ir := catalog.Expr{Op: catalog.OpIn, Type: boolType(l.typ().Nullable), Args: args}
	if x.Not {
		ir = catalog.Expr{Op: catalog.OpNot, Type: ir.Type, Args: []catalog.Expr{ir}}
	}
	return predicate(ir, x)
}

// inParam checks x IN {p:Array(T)}: T pairs with x.
func (c *checker) inParam(x *sql.InExpr, l operand) (catalog.Expr, bool) {
	p := c.param(x.Param, paramAfterIn)
	if p.bad {
		return catalog.Expr{}, false
	}
	elem := p
	elem.ir.Type.Array = false
	if _, _, ok := c.pair(x, opIn, l, elem); !ok {
		return catalog.Expr{}, false
	}
	return p.ir, true
}

func (c *checker) inItem(x *sql.InExpr, l operand, item sql.Expr) (catalog.Expr, bool) {
	if null, ok := item.(*sql.NullLit); ok {
		c.st.errorf(null.Pos, "DF5307", "", "NULL in an IN list never matches")
		return catalog.Expr{}, false
	}
	var o operand
	if p, ok := item.(*sql.Param); ok {
		o = c.param(p, paramInList)
	} else {
		o = c.expr(item)
	}
	if o.bad {
		return catalog.Expr{}, false
	}
	_, ir, ok := c.pair(x, opIn, l, o)
	return ir, ok
}

func (c *checker) between(x *sql.BetweenExpr) operand {
	if c.chained(x.X, x.OpPos) {
		return badOperand()
	}
	v, lo, hi := c.valueOrNull(x.X), c.valueOrNull(x.Lo), c.valueOrNull(x.Hi)
	if v.bad || lo.bad || hi.bad || c.comparedWithNull(x, v, lo, hi) {
		return badOperand()
	}
	v1, loIR, ok := c.pair(x, ">=", v, lo)
	if !ok {
		return badOperand()
	}
	v2, hiIR, ok := c.pair(x, "<=", v, hi)
	if !ok {
		return badOperand()
	}
	t := boolType(v.typ().Nullable || lo.typ().Nullable || hi.typ().Nullable)
	ir := catalog.Expr{Op: catalog.OpAnd, Type: t, Args: []catalog.Expr{
		{Op: catalog.OpGreaterOrEquals, Type: boolType(v.typ().Nullable || lo.typ().Nullable), Args: []catalog.Expr{v1, loIR}},
		{Op: catalog.OpLessOrEquals, Type: boolType(v.typ().Nullable || hi.typ().Nullable), Args: []catalog.Expr{v2, hiIR}},
	}}
	if x.Not {
		ir = catalog.Expr{Op: catalog.OpNot, Type: t, Args: []catalog.Expr{ir}}
	}
	return predicate(ir, x)
}

func (c *checker) isNull(x *sql.IsNullExpr) operand {
	if c.chained(x.X, x.OpPos) {
		return badOperand()
	}
	o := c.value(x.X)
	if o.bad {
		return o
	}
	if !o.typ().Nullable {
		result := "IS NULL is always false"
		if x.Not {
			result = "IS NOT NULL is always true"
		}
		c.st.errorf(x.X.Extent().Pos, "DF5306", "", "%s is not Nullable, so %s", c.st.text(x.X), result)
		return badOperand()
	}
	ir := catalog.Expr{Op: catalog.OpIsNull, Type: boolType(false), Args: []catalog.Expr{o.ir}}
	if x.Not {
		ir = catalog.Expr{Op: catalog.OpNot, Type: ir.Type, Args: []catalog.Expr{ir}}
	}
	return predicate(ir, x)
}

func (c *checker) arith(x *sql.BinaryExpr, op catalog.Op) operand {
	l, r := c.value(x.X), c.value(x.Y)
	if l.bad || r.bad {
		return badOperand()
	}
	t, err := arithType(arithSymbol(op), l.typ(), r.typ())
	if err != nil {
		return c.typeError(x, err)
	}
	if (op == catalog.OpDivide || op == catalog.OpModulo) && !c.divisor(x, x.X, x.Y, r) {
		return badOperand()
	}
	return operand{ir: catalog.Expr{Op: op, Type: t, Args: []catalog.Expr{l.ir, r.ir}}, node: x}
}

// divisor checks the divisor d of /, % or intDiv (sql.md §5.6): a literal 0 is DF5313,
// and any other divisor that isn't a number literal or ruled out zero by a guard gets
// DF5501. a and b are the dividend and the divisor as written. It reports whether the
// division is valid.
func (c *checker) divisor(at sql.Node, a, b sql.Expr, d operand) bool {
	if n, ok := d.node.(*sql.NumberLit); ok {
		if n.Int == 0 && n.Float == 0 {
			c.st.errorf(at.Extent().Pos, "DF5313", "", "division by zero")
			return false
		}
		return true
	}
	if slices.ContainsFunc(c.guards, func(g catalog.Expr) bool { return reflect.DeepEqual(g, d.ir) }) {
		return true
	}
	dividend, divisor := c.st.text(a), c.st.text(b)
	c.st.warnf(at.Extent().Pos, "DF5501",
		"if("+divisor+" = 0, NULL, "+dividend+" / "+divisor+"), or if("+divisor+" = 0, 0, intDiv("+dividend+", "+divisor+"))",
		"division by %s fails when it is 0", divisor)
	return true
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"math"
	"strconv"
	"strings"
)

// Expressions are parsed by recursive descent, one function per precedence
// level, from OR (lowest) to prefix - (highest).

func (p *parser) expr() Expr {
	if !p.nest() {
		return nil
	}
	defer p.unnest()
	return p.or()
}

func (p *parser) or() Expr {
	start := p.tok
	x := p.and()
	for op := p.tok; p.acceptKw("OR"); op = p.tok {
		y := p.and()
		x = &BinaryExpr{Op: OpOr, OpPos: op.pos, X: x, Y: y, Span: p.span(start)}
	}
	return x
}

func (p *parser) and() Expr {
	start := p.tok
	x := p.not()
	for op := p.tok; p.acceptKw(kwAnd); op = p.tok {
		y := p.not()
		x = &BinaryExpr{Op: OpAnd, OpPos: op.pos, X: x, Y: y, Span: p.span(start)}
	}
	return x
}

func (p *parser) not() Expr {
	start := p.tok
	if !p.acceptKw(kwNot) {
		return p.predicate()
	}
	if !p.nest() {
		return nil
	}
	defer p.unnest()
	x := p.not()
	return &UnaryExpr{Op: OpNot, X: x, Span: p.span(start)}
}

// predicate parses a sum and the comparisons, IN, BETWEEN and IS NULL after
// it. A second one is a chain, which the compiler rejects (DF5304).
func (p *parser) predicate() Expr {
	start := p.tok
	x := p.sum()
	for {
		op := p.tok
		if cmp, ok := cmpOp(op); ok {
			p.advance()
			y := p.sum()
			x = &BinaryExpr{Op: cmp, OpPos: op.pos, X: x, Y: y, Span: p.span(start)}
			continue
		}
		not := false
		if p.isKw(kwNot) {
			switch next := p.peek(); {
			case isKw(next, "IN"), isKw(next, "BETWEEN"):
				p.advance()
				not = true
			case isKw(next, "LIKE"), isKw(next, "ILIKE"), isKw(next, "REGEXP"):
				p.unsupported("NOT "+strings.ToUpper(next.text), hintCompare)
				return x
			default:
				return x
			}
		}
		switch {
		case p.acceptKw("IN"):
			x = p.in(start, op, x, not)
		case p.acceptKw("BETWEEN"):
			lo := p.sum()
			p.expectKw(kwAnd)
			hi := p.sum()
			x = &BetweenExpr{X: x, Not: not, OpPos: op.pos, Lo: lo, Hi: hi, Span: p.span(start)}
		case p.acceptKw("IS"):
			x = p.isNull(start, op, x)
		default:
			return x
		}
	}
}

func cmpOp(t token) (BinaryOp, bool) {
	if t.kind != tokOp {
		return 0, false
	}
	switch t.text {
	case "=", "==":
		return OpEq, true
	case "!=", "<>":
		return OpNe, true
	case "<":
		return OpLt, true
	case "<=":
		return OpLe, true
	case ">":
		return OpGt, true
	case ">=":
		return OpGe, true
	}
	return 0, false
}

func (p *parser) in(start, op token, x Expr, not bool) Expr {
	in := &InExpr{X: x, Not: not, OpPos: op.pos}
	switch {
	case p.isOp("{"):
		in.Param = p.param()
	case p.isOp("("):
		paren := p.tok
		p.advance()
		if p.isKw("SELECT") || p.isKw("WITH") {
			p.unsupportedAt(paren.pos, "subquery", hintOneSource)
		}
		for {
			if p.isOp("{") {
				in.List = append(in.List, p.param())
			} else {
				in.List = append(in.List, p.signedLiteral("a literal or a parameter"))
			}
			if !p.acceptOp(",") {
				break
			}
		}
		p.expectOp(")")
	default:
		p.expected("( or a parameter")
	}
	in.Span = p.span(start)
	return in
}

func (p *parser) isNull(start, op token, x Expr) Expr {
	not := p.acceptKw(kwNot)
	if p.isKw("DISTINCT") {
		feature := "IS DISTINCT FROM"
		if not {
			feature = "IS NOT DISTINCT FROM"
		}
		p.unsupportedAt(op.pos, feature, hintNullSafe)
	}
	p.expectKw(kwNull)
	return &IsNullExpr{X: x, Not: not, OpPos: op.pos, Span: p.span(start)}
}

func (p *parser) sum() Expr {
	start := p.tok
	x := p.product()
	for {
		var op BinaryOp
		switch {
		case p.isOp("+"):
			op = OpAdd
		case p.isOp("-"):
			op = OpSub
		default:
			return x
		}
		opPos := p.tok.pos
		p.advance()
		y := p.product()
		x = &BinaryExpr{Op: op, OpPos: opPos, X: x, Y: y, Span: p.span(start)}
	}
}

func (p *parser) product() Expr {
	start := p.tok
	x := p.unary()
	for {
		var op BinaryOp
		switch {
		case p.isOp("*"):
			op = OpMul
		case p.isOp("/"):
			op = OpDiv
		case p.isOp("%"):
			op = OpMod
		default:
			return x
		}
		opPos := p.tok.pos
		p.advance()
		y := p.unary()
		x = &BinaryExpr{Op: op, OpPos: opPos, X: x, Y: y, Span: p.span(start)}
	}
}

// unary parses prefix minus. A minus right before a number is part of the
// literal, so -1 is the Int8 literal -1, while -(1) negates a UInt8.
func (p *parser) unary() Expr {
	start := p.tok
	if !p.isOp("-") {
		return p.afterOperand(p.primary())
	}
	if p.peek().kind == tokNumber {
		p.advance()
		return p.afterOperand(p.number(&start))
	}
	p.advance()
	if !p.nest() {
		return nil
	}
	defer p.unnest()
	x := p.unary()
	return &UnaryExpr{Op: OpNeg, X: x, Span: p.span(start)}
}

// afterOperand rejects the ClickHouse operators that may follow the operand x,
// and returns x.
func (p *parser) afterOperand(x Expr) Expr {
	if p.failed || p.tok.kind != tokIdent && !p.isOp("[") {
		return x
	}
	switch word := strings.ToUpper(p.tok.text); word {
	case "[":
		p.unsupported("[ ] subscript", hintNotInV1)
	case "LIKE", "ILIKE", "REGEXP":
		p.unsupported(word, hintCompare)
	case "DIV", "MOD":
		p.unsupported(word, "use intDiv(a, b) or a % b")
	case "GLOBAL":
		p.unsupported("GLOBAL IN", hintNotInV1)
	case "OVER":
		p.unsupported("OVER", "window functions are not in v1")
	}
	return x
}

func (p *parser) primary() Expr {
	t := p.tok
	switch t.kind {
	case tokNumber:
		return p.number(nil)
	case tokString:
		return p.stringLit()
	case tokQuoted:
		return &ColumnRef{Ident: p.name()}
	case tokIdent:
		return p.word()
	case tokOp:
		switch t.text {
		case "(":
			return p.paren()
		case "{":
			return p.param()
		case "[":
			if !p.inDefault {
				p.unsupported("array literal outside DEFAULT", hintNotInV1)
				return nil
			}
			return p.arrayLit()
		}
	case tokEOF, tokError, tokStop:
	}
	p.expected("an expression")
	return nil
}

// word parses a primary that starts with a plain identifier: a keyword
// literal, an interval, a call or a name.
func (p *parser) word() Expr {
	t := p.tok
	switch strings.ToUpper(t.text) {
	case "TRUE", "FALSE":
		p.advance()
		return &BoolLit{Value: strings.EqualFold(t.text, "TRUE"), Span: p.span(t)}
	case kwNull:
		p.advance()
		return &NullLit{Span: p.span(t)}
	case "INTERVAL":
		return p.interval()
	case "INF", "NAN":
		p.fail(p.errorAt(t.pos, "DF1008", hintNumber, "unsupported number %s", t.text))
		return nil
	case "CASE":
		p.unsupported("CASE", "use multiIf(cond1, value1, cond2, value2, default)")
		return nil
	}
	isCall := isOp(p.peek(), "(")
	// and(a, b) and or(a, b) are ClickHouse functions, which the compiler
	// rejects with the operator as the replacement (DF5201).
	andOrCall := isCall && (strings.EqualFold(t.text, kwAnd) || strings.EqualFold(t.text, "OR"))
	if reserved(t) && !andOrCall { // AND, OR, NOT, IN, IS or BETWEEN: an operator, not an operand
		p.expected("an expression")
		return nil
	}
	if isCall {
		if strings.EqualFold(t.text, "CAST") {
			p.unsupported("CAST", hintConversion)
			return nil
		}
		return p.call()
	}
	return &ColumnRef{Ident: p.name()}
}

func (p *parser) paren() Expr {
	start := p.tok
	p.advance()
	if p.isKw("SELECT") || p.isKw("WITH") {
		p.unsupportedAt(start.pos, "subquery", hintOneSource)
		return nil
	}
	x := p.expr()
	if p.isOp(",") {
		p.unsupportedAt(start.pos, "tuple", hintNotInV1)
		return nil
	}
	p.expectOp(")")
	return &ParenExpr{X: x, Span: p.span(start)}
}

func (p *parser) call() Expr {
	start := p.tok
	c := &Call{Name: p.ident()}
	p.advance() // (
	switch {
	case p.acceptOp("*"):
		c.Star = true
	case p.isOp(")"):
	default:
		if p.isKw("DISTINCT") && startsExpr(p.peek()) {
			c.Distinct, c.DistinctPos = true, p.tok.pos
			p.advance()
		}
		c.Args = p.args()
	}
	p.expectOp(")")
	if p.acceptOp("(") {
		c.Parametric, c.Params, c.Args = true, c.Args, nil
		if !p.isOp(")") {
			c.Args = p.args()
		}
		p.expectOp(")")
	}
	c.Span = p.span(start)
	return c
}

func startsExpr(t token) bool {
	switch t.kind {
	case tokIdent, tokQuoted, tokNumber, tokString:
		return true
	case tokOp:
		return t.text == "(" || t.text == "[" || t.text == "{" || t.text == "-"
	case tokEOF, tokError, tokStop:
	}
	return false
}

func (p *parser) args() []Expr {
	var args []Expr
	for {
		args = append(args, p.expr())
		if !p.acceptOp(",") {
			return args
		}
	}
}

func (p *parser) interval() Expr {
	start := p.tok
	p.advance() // INTERVAL
	iv := &Interval{Neg: p.acceptOp("-")}
	switch p.tok.kind {
	case tokNumber:
		iv.Value = p.number(nil)
	case tokString:
		iv.Value = p.stringLit()
	case tokEOF, tokIdent, tokQuoted, tokOp, tokError, tokStop:
		p.expected("a number or a string")
	}
	// The unit may be left out only after a string, as in INTERVAL '2 days',
	// which the compiler rejects (DF5404) like every form but INTERVAL n unit.
	// There, only a ClickHouse unit counts as one, so that INTERVAL '2 days'
	// AS d keeps its AS.
	_, isString := iv.Value.(*StringLit)
	switch {
	case !isString:
		iv.Unit = p.unit()
	case p.tok.kind == tokQuoted || p.tok.kind == tokIdent && isUnit(p.tok):
		iv.Unit = p.ident()
	}
	if p.acceptKw("TO") {
		to := p.ident()
		iv.To = &to
	}
	iv.Span = p.span(start)
	return iv
}

func (p *parser) arrayLit() Expr {
	start := p.tok
	p.advance() // [
	a := &ArrayLit{}
	if !p.isOp("]") {
		for {
			a.Elems = append(a.Elems, p.signedLiteral("a literal"))
			if !p.acceptOp(",") {
				break
			}
		}
	}
	p.expectOp("]")
	a.Span = p.span(start)
	return a
}

// signedLiteral parses a literal, with an optional - before a number.
func (p *parser) signedLiteral(what string) Expr {
	t := p.tok
	switch {
	case p.isOp("-") && p.peek().kind == tokNumber:
		p.advance()
		return p.number(&t)
	case t.kind == tokNumber:
		return p.number(nil)
	case t.kind == tokString:
		return p.stringLit()
	case p.isKw("TRUE"), p.isKw("FALSE"), p.isKw(kwNull):
		return p.word()
	}
	p.expected(what)
	return nil
}

func (p *parser) param() *Param {
	start := p.tok
	p.advance() // {
	pr := &Param{Name: p.name()}
	p.expectOp(":")
	pr.Type = p.typ()
	p.expectOp("}")
	pr.Span = p.span(start)
	return pr
}

func (p *parser) stringLit() *StringLit {
	t := p.tok
	if t.kind != tokString {
		p.expected("a string")
		return &StringLit{}
	}
	p.advance()
	return &StringLit{Value: t.value, Span: p.span(t)}
}

func (p *parser) integer() *NumberLit {
	if p.tok.kind != tokNumber || p.tok.isFloat {
		p.expected("an integer")
		return &NumberLit{}
	}
	return p.number(nil)
}

// signedInteger parses an integer with an optional - before it.
func (p *parser) signedInteger() *NumberLit {
	var minus *token
	if t := p.tok; p.acceptOp("-") {
		minus = &t
	}
	if p.tok.kind != tokNumber || p.tok.isFloat {
		p.expected("an integer")
		return &NumberLit{}
	}
	return p.number(minus)
}

// number parses the current number token. minus is the - before it that
// makes it a negative literal, or nil.
func (p *parser) number(minus *token) *NumberLit {
	t := p.tok
	start := t
	n := &NumberLit{Text: t.value, IsFloat: t.isFloat, Float: t.float}
	if minus != nil {
		start, n.Neg = *minus, true
	}
	if !t.isFloat {
		v, err := strconv.ParseUint(t.value, 10, 64)
		if err != nil || n.Neg && v > -math.MinInt64 {
			text := t.text
			if n.Neg {
				text = "-" + text
			}
			p.fail(p.errorAt(start.pos, "DF1009",
				"integers are between -9223372036854775808 and 18446744073709551615",
				"integer %s is out of range", text))
			return n
		}
		n.Int = v
	}
	p.advance()
	n.Span = p.span(start)
	return n
}

// unit parses the unit after the number of an interval or a LOG RETENTION.
// A word that starts a clause isn't one: the unit is missing, as in
// TTL d + INTERVAL 1 AS SELECT.
func (p *parser) unit() Ident {
	t := p.tok
	if t.kind != tokQuoted && (t.kind != tokIdent || reserved(t) || startsClause(t)) {
		p.expected("a unit")
		return Ident{}
	}
	return p.ident()
}

func startsClause(t token) bool {
	switch strings.ToUpper(t.text) {
	case "AS", "FROM", kwWhere, "GROUP", "ORDER", kwLimit,
		"PRIMARY", "DEDUP", "LOG", "ACCEPT", "SHARDS", "CONSISTENCY", "MODE", kwTTL, "SPLIT":
		return true
	}
	return false
}

// isUnit reports whether t is a ClickHouse interval unit, in full or
// abbreviated.
func isUnit(t token) bool {
	switch strings.ToUpper(t.text) {
	case "NANOSECOND", "NANOSECONDS", "MICROSECOND", "MICROSECONDS", "MILLISECOND", "MILLISECONDS",
		"SECOND", "SECONDS", "MINUTE", "MINUTES", "HOUR", "HOURS", "DAY", "DAYS", "WEEK", "WEEKS",
		"MONTH", "MONTHS", "QUARTER", "QUARTERS", "YEAR", "YEARS",
		"NS", "MCS", "MS", "SS", "S", "MI", "N", "HH", "H", "DD", "D", "WK", "WW", "MM", "M", "QQ", "Q", "YYYY", "YY":
		return true
	}
	return false
}

// IsReserved reports whether name, in any letter case, is one of the words
// reserved in expressions, which can't be names (DF1302).
func IsReserved(name string) bool {
	switch strings.ToUpper(name) {
	case kwAnd, "BETWEEN", "CASE", "FALSE", "IN", "INF", "INTERVAL", "IS", "NAN", kwNot, kwNull, "OR", "TRUE":
		return true
	}
	return false
}

func reserved(t token) bool { return t.kind == tokIdent && IsReserved(t.text) }

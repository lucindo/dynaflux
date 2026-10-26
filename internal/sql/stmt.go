// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/diag"
)

func (p *parser) create(anns []Annotation) Stmt {
	start := p.tok
	p.advance() // CREATE
	var orReplace *diag.Pos
	if p.isKw("OR") {
		pos := p.tok.pos
		orReplace = &pos
		p.advance()
		p.expectKw("REPLACE")
	}
	switch {
	case p.isKw("STREAM"):
		return p.createStream(start, anns, orReplace)
	case p.isKw("MATERIALIZED"):
		return p.createView(start, anns, orReplace)
	case orReplace != nil:
		p.unsupportedAt(*orReplace, "OR REPLACE", hintManaged)
	case p.tok.kind == tokIdent:
		p.unsupportedStatement(start.pos, "CREATE "+strings.ToUpper(p.tok.text))
	default:
		p.expected("STREAM or MATERIALIZED VIEW")
	}
	return nil
}

// createName parses the name of a CREATE statement and the modifiers around
// it. OR REPLACE and IF NOT EXISTS are reported after the name, so that the
// broken statement still defines it.
func (p *parser) createName(orReplace *diag.Pos) Ident {
	ifNotExists := p.ifNotExists()
	name := p.name()
	p.stmtName = name.Name
	switch {
	case orReplace != nil:
		p.unsupportedAt(*orReplace, "OR REPLACE", hintManaged)
	case ifNotExists != nil:
		p.unsupportedAt(*ifNotExists, "IF NOT EXISTS", hintManaged)
	}
	p.onCluster()
	return name
}

func (p *parser) createStream(start token, anns []Annotation, orReplace *diag.Pos) *CreateStream {
	s := &CreateStream{StmtHeader: StmtHeader{Annotations: anns}}
	p.advance() // STREAM
	s.Name = p.createName(orReplace)
	p.expectOp("(")
	for {
		s.Columns = append(s.Columns, p.column())
		if !p.acceptOp(",") {
			break
		}
	}
	if !p.acceptOp(")") {
		p.expected(", or )")
	}
	p.streamClauses(s)
	s.Span = p.span(start)
	return s
}

func (p *parser) streamClauses(s *CreateStream) {
	for !p.failed && !p.atStmtEnd() {
		t := p.tok
		switch {
		case p.isKw("PRIMARY"):
			keep(p, &s.PrimaryKey, p.keyClause("PRIMARY", "KEY"), "PRIMARY KEY", t)
		case p.isKw("DEDUP"):
			keep(p, &s.DedupBy, p.keyClause("DEDUP", "BY"), "DEDUP BY", t)
		case p.isKw("LOG"):
			keep(p, &s.LogRetention, p.logRetention(), "LOG RETENTION", t)
		case p.isKw("ACCEPT"):
			keep(p, &s.Accept, p.exprClause(), "ACCEPT", t)
		case p.isKw("SHARDS"):
			keep(p, &s.Shards, p.intClause(), "SHARDS", t)
		case p.isKw("CONSISTENCY"):
			keep(p, &s.Consistency, p.consistency(), "CONSISTENCY", t)
		case p.isKw("ENGINE"), p.isKw("SETTINGS"):
			p.unsupported(strings.ToUpper(t.text), hintManaged)
		default:
			p.expected("PRIMARY KEY, DEDUP BY, LOG RETENTION, ACCEPT, SHARDS, CONSISTENCY or ;")
		}
	}
}

func (p *parser) column() *Column {
	start := p.tok
	c := &Column{Name: p.name()}
	c.Type = p.typ()
	for !p.failed {
		t := p.tok
		switch {
		case p.isKw("DEFAULT"):
			p.inDefault = true
			keep(p, &c.Default, p.exprClause(), "DEFAULT", t)
			p.inDefault = false
		case p.isKw("PII"):
			keep(p, &c.PII, p.pii(), "PII", t)
		default:
			name := p.otherColumnOption()
			if name == "" {
				c.Span = p.span(start)
				return c
			}
			if slices.ContainsFunc(c.Options, func(o ColumnOption) bool { return o.Name == name }) {
				p.repeated(name, t)
			} else {
				c.Options = append(c.Options, ColumnOption{Span: p.span(t), Name: name})
			}
		}
	}
	return c
}

func (p *parser) pii() *PII {
	start := p.tok
	p.advance()
	pii := &PII{}
	if p.acceptOp("(") {
		pii.Category = p.stringLit()
		p.expectOp(")")
	}
	pii.Span = p.span(start)
	return pii
}

// exprClause parses a keyword and an expression, such as WHERE x.
func (p *parser) exprClause() *ExprClause {
	start := p.tok
	p.advance()
	c := &ExprClause{X: p.expr()}
	c.Span = p.span(start)
	return c
}

// otherColumnOption parses a ClickHouse column option that Dynaflux rejects,
// and returns its name, or "" when no option starts here.
func (p *parser) otherColumnOption() string {
	t := p.tok
	if t.kind != tokIdent {
		return ""
	}
	name := strings.ToUpper(t.text)
	switch name {
	case kwNull:
		p.advance()
	case kwNot:
		if !isKw(p.peek(), kwNull) {
			return ""
		}
		p.advance()
		p.advance()
		name = "NOT NULL"
	case "MATERIALIZED", "ALIAS", kwTTL:
		p.advance()
		p.expr()
	case "EPHEMERAL":
		p.advance()
		if !p.isOp(",") && !p.isOp(")") && !isColumnOption(p.tok) {
			p.expr()
		}
	case "COMMENT":
		p.advance()
		p.stringLit()
	case "CODEC":
		p.advance()
		p.expectOp("(")
		p.args()
		p.expectOp(")")
	default:
		return ""
	}
	return name
}

func isColumnOption(t token) bool {
	if t.kind != tokIdent {
		return false
	}
	switch strings.ToUpper(t.text) {
	case "DEFAULT", "PII", kwNull, kwNot, "MATERIALIZED", "ALIAS", "EPHEMERAL", "CODEC", kwTTL, "COMMENT":
		return true
	}
	return false
}

// keyClause parses a two-word keyword and a name list, such as PRIMARY KEY (a).
func (p *parser) keyClause(first, second string) *NameList {
	start := p.tok
	p.expectKw(first)
	p.expectKw(second)
	l := &NameList{}
	if p.acceptOp("(") {
		for {
			l.Names = append(l.Names, p.name())
			if !p.acceptOp(",") {
				break
			}
		}
		p.expectOp(")")
	} else {
		l.Names = []Ident{p.name()}
	}
	l.Span = p.span(start)
	return l
}

func (p *parser) logRetention() *LogRetention {
	start := p.tok
	p.advance() // LOG
	p.expectKw("RETENTION")
	r := &LogRetention{Forever: p.acceptKw("FOREVER")}
	if !r.Forever {
		r.Neg = p.acceptOp("-")
		switch {
		case p.tok.kind == tokNumber:
			r.Value = p.number(nil)
			r.Unit = p.unit()
		case r.Neg:
			p.expected("a number")
		default:
			p.expected("a number or FOREVER")
		}
	}
	r.Span = p.span(start)
	return r
}

func (p *parser) intClause() *IntClause {
	start := p.tok
	p.advance()
	c := &IntClause{Value: p.integer()}
	c.Span = p.span(start)
	return c
}

func (p *parser) consistency() *Consistency {
	start := p.tok
	p.advance()
	p.expectOp("=")
	c := &Consistency{Value: p.stringLit()}
	c.Span = p.span(start)
	return c
}

func (p *parser) createView(start token, anns []Annotation, orReplace *diag.Pos) *CreateView {
	v := &CreateView{StmtHeader: StmtHeader{Annotations: anns}}
	p.advance() // MATERIALIZED
	p.expectKw("VIEW")
	v.Name = p.createName(orReplace)
	p.viewClauses(v)
	p.expectKw("AS")
	v.Select = p.selectStmt()
	v.Span = p.span(start)
	return v
}

// viewClauses parses the clauses before AS.
func (p *parser) viewClauses(v *CreateView) {
	for !p.failed && !p.isKw("AS") {
		t := p.tok
		switch {
		case p.isKw("MODE"):
			keep(p, &v.Mode, p.mode(), "MODE", t)
		case p.isKw("ORDER"):
			keep(p, &v.OrderBy, p.keyClause("ORDER", "BY"), "ORDER BY", t)
		case p.isKw("PRIMARY"):
			keep(p, &v.PrimaryKey, p.keyClause("PRIMARY", "KEY"), "PRIMARY KEY", t)
		case p.isKw(kwTTL):
			keep(p, &v.TTL, p.exprClause(), kwTTL, t)
		case p.isKw("SPLIT"):
			keep(p, &v.Split, p.intClause(), "SPLIT", t)
		case p.isKw("ENGINE"), p.isKw("TO"), p.isKw("POPULATE"), p.isKw("SETTINGS"):
			p.unsupported(strings.ToUpper(t.text), hintManaged)
		default:
			p.expected("MODE, ORDER BY, PRIMARY KEY, TTL, SPLIT or AS")
		}
	}
}

func (p *parser) mode() *Mode {
	start := p.tok
	p.advance()
	m := &Mode{}
	switch {
	case p.acceptKw("ASYNC"):
	case p.acceptKw("SYNC"):
		m.Sync = true
	default:
		p.expected("ASYNC or SYNC")
	}
	m.Span = p.span(start)
	return m
}

// ifNotExists parses IF NOT EXISTS, if it comes next, and returns where it
// starts.
func (p *parser) ifNotExists() *diag.Pos {
	if !p.isKw("IF") || !isKw(p.peek(), kwNot) {
		return nil
	}
	pos := p.tok.pos
	p.advance()
	p.advance()
	p.expectKw("EXISTS")
	return &pos
}

func (p *parser) onCluster() {
	if p.isKw("ON") && isKw(p.peek(), "CLUSTER") {
		p.unsupported("ON CLUSTER", hintManaged)
	}
}

func (p *parser) query(anns []Annotation) *Query {
	sel := p.selectStmt()
	return &Query{Span: sel.Span, StmtHeader: StmtHeader{Annotations: anns}, Select: sel}
}

// selectStmt parses a SELECT, shared by views and queries, through the end of
// the statement.
func (p *parser) selectStmt() *Select {
	start := p.tok
	sel := &Select{}
	switch {
	case p.isKw("WITH"):
		p.unsupported("WITH", hintOneSource)
	case p.isOp("(") && isKw(p.peek(), "SELECT"):
		p.unsupported("subquery", hintOneSource)
	}
	p.expectKw("SELECT")
	switch {
	case p.isKw("DISTINCT"):
		p.unsupportedAt(start.pos, "SELECT DISTINCT", "use GROUP BY")
	case p.isKw("TOP"):
		p.unsupported("TOP", hintPagination)
	}
	for {
		sel.Items = append(sel.Items, p.selectItem())
		if !p.acceptOp(",") {
			break
		}
	}
	if last := sel.Items[len(sel.Items)-1]; !p.acceptKw("FROM") {
		p.expectedHint("FROM", p.aliasHint(last.X, last.Alias))
	}
	if p.isOp("(") && (isKw(p.peek(), "SELECT") || isKw(p.peek(), "WITH")) {
		p.unsupported("subquery", hintOneSource)
	}
	sel.From = p.name()
	if p.isOp(",") {
		p.unsupported("JOIN", hintOneSource) // FROM a, b
	}
	next := p.selectClauses(sel)
	if !p.atStmtEnd() {
		p.selectEnd(next)
	}
	sel.Span = p.span(start)
	return sel
}

// selectClauses parses the clauses after FROM and returns those that could
// still follow, for the error message when something else comes.
func (p *parser) selectClauses(sel *Select) []string {
	clauses := []string{"ARRAY JOIN", kwWhere, "GROUP BY", "ORDER BY", kwLimit}
	next := 0
	for p.isKw("ARRAY") || p.isKw("LEFT") && isKw(p.peek(), "ARRAY") {
		sel.ArrayJoins = append(sel.ArrayJoins, p.arrayJoin())
	}
	if p.isKw(kwWhere) {
		sel.Where = p.exprClause()
		next = 2
	}
	if t := p.tok; p.acceptKw("GROUP") {
		p.expectKw("BY")
		g := &GroupBy{Items: p.args()}
		g.Span = p.span(t)
		sel.GroupBy = g
		next = 3
	}
	if p.isKw("ORDER") {
		sel.OrderBy = p.orderBy()
		next = 4
	}
	if p.isKw(kwLimit) {
		sel.Limit = p.limit()
		next = len(clauses)
	}
	return clauses[next:]
}

func (p *parser) selectItem() *SelectItem {
	start := p.tok
	it := &SelectItem{}
	if p.acceptOp("*") {
		it.Star = true
	} else {
		it.X = p.expr()
		if p.acceptKw("AS") {
			alias := p.name()
			it.Alias = &alias
		}
	}
	it.Span = p.span(start)
	return it
}

// aliasHint returns the hint for an expression followed by what looks like an
// alias without AS, as in SELECT count() views.
func (p *parser) aliasHint(x Expr, alias *Ident) string {
	if p.failed || x == nil || alias != nil || p.tok.kind != tokIdent && p.tok.kind != tokQuoted {
		return ""
	}
	next := p.peek()
	switch {
	case isOp(next, ","), isOp(next, ";"), next.kind == tokEOF:
	case next.kind == tokIdent:
		switch strings.ToUpper(next.text) {
		case "FROM", kwWhere, "GROUP", "ORDER", kwLimit, "ARRAY", "LEFT":
		default:
			return ""
		}
	default:
		return ""
	}
	// In SELECT a, FROM t the , made FROM an item.
	if c, ok := x.(*ColumnRef); ok && c.Quote == 0 && c.Qualifier == nil && strings.EqualFold(c.Name, "FROM") {
		return "remove the , before FROM"
	}
	s := x.Extent()
	return "aliases need AS: " + normalize(p.lex.src[s.Start:s.End]) + " AS " + p.tok.text
}

func (p *parser) arrayJoin() *ArrayJoin {
	start := p.tok
	j := &ArrayJoin{Left: p.acceptKw("LEFT")}
	p.expectKw("ARRAY")
	p.expectKw("JOIN")
	for {
		t := p.tok
		it := &ArrayItem{X: p.expr()}
		if p.acceptKw("AS") {
			alias := p.name()
			it.Alias = &alias
		} else if !p.isOp(",") && !p.atStmtEnd() {
			if hint := p.aliasHint(it.X, nil); hint != "" {
				p.expectedHint("AS", hint)
			}
		}
		it.Span = p.span(t)
		j.Items = append(j.Items, it)
		if !p.acceptOp(",") {
			break
		}
	}
	j.Span = p.span(start)
	return j
}

func (p *parser) orderBy() *OrderBy {
	start := p.tok
	p.advance() // ORDER
	p.expectKw("BY")
	if p.isKw("ALL") {
		p.unsupportedAt(start.pos, "ORDER BY ALL", "name the column")
	}
	o := &OrderBy{}
	for {
		o.Items = append(o.Items, p.orderItem())
		if !p.acceptOp(",") {
			break
		}
	}
	o.Span = p.span(start)
	return o
}

func (p *parser) orderItem() *OrderItem {
	start := p.tok
	it := &OrderItem{X: p.expr()}
	if n, ok := it.X.(*NumberLit); ok && !n.Neg && !n.IsFloat {
		p.unsupportedAt(n.Pos, "positional ORDER BY", "name the column")
	}
	switch {
	case p.acceptKw("ASC"):
	case p.acceptKw("DESC"):
		it.Desc = true
	}
	if p.acceptKw("NULLS") {
		switch {
		case p.acceptKw("FIRST"):
			it.NullsFirst = true
		case p.acceptKw("LAST"):
		default:
			p.expected("FIRST or LAST")
		}
	}
	it.Span = p.span(start)
	return it
}

func (p *parser) limit() *ExprClause {
	start := p.tok
	p.advance()
	c := &ExprClause{}
	switch {
	case p.isOp("{"):
		c.X = p.param()
	case p.tok.kind == tokNumber && !p.tok.isFloat:
		c.X = p.number(nil)
	default:
		p.expected("an integer or a parameter")
	}
	switch {
	case p.isOp(","):
		p.unsupportedAt(start.pos, "LIMIT m, n", hintPagination)
	case p.isKw("BY"):
		p.unsupportedAt(start.pos, "LIMIT n BY", hintPagination)
	}
	c.Span = p.span(start)
	return c
}

// selectEnd reports what comes after the last clause of a SELECT.
func (p *parser) selectEnd(next []string) {
	if t := p.tok; t.kind == tokIdent {
		word := strings.ToUpper(t.text)
		switch word {
		case "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "OUTER", "SEMI", "ANTI", "ANY", "ALL", "ASOF", "PASTE", "GLOBAL":
			p.unsupported("JOIN", hintOneSource)
		case "UNION", "INTERSECT", "EXCEPT":
			p.unsupported(word, hintOneSource)
		case "HAVING":
			p.unsupported(word, "not in v1; filter the rows in the caller")
		case "PREWHERE", "FINAL", "SAMPLE", "SETTINGS", "FORMAT":
			p.unsupported(word, hintMerged)
		case "OFFSET":
			p.unsupported(word, hintPagination)
		case "WITH":
			if isKw(p.peek(), "TIES") {
				p.unsupported("WITH TIES", hintPagination)
			}
		}
	}
	p.expected(orList(append(next, ";")))
}

// name parses a name: an identifier, or a qualified name such as t.col.
func (p *parser) name() Ident {
	start := p.tok
	var qualifier []Ident
	id := p.ident()
	for p.isOp(".") && (p.peek().kind == tokIdent || p.peek().kind == tokQuoted) {
		qualifier = append(qualifier, id)
		p.advance()
		id = p.ident()
	}
	id.Qualifier = qualifier
	id.Span = p.span(start)
	return id
}

// ident parses one identifier, quoted or not.
func (p *parser) ident() Ident {
	t := p.tok
	if t.kind != tokIdent && t.kind != tokQuoted {
		p.expected("a name")
		return Ident{}
	}
	id := Ident{Name: t.value}
	if t.kind == tokQuoted {
		id.Quote = t.text[0]
	}
	p.advance()
	id.Span = p.span(t)
	return id
}

func (p *parser) typ() *Type {
	start := p.tok
	ty := &Type{}
	if !p.nest() {
		return ty
	}
	defer p.unnest()
	if p.tok.kind != tokIdent && p.tok.kind != tokQuoted {
		p.expected("a type")
		return ty
	}
	ty.Name = p.ident()
	if p.acceptOp("(") {
		ty.Parens = true
		if !p.isOp(")") {
			for {
				ty.Args = append(ty.Args, p.typeArg())
				if !p.acceptOp(",") {
					break
				}
			}
		}
		p.expectOp(")")
	}
	ty.Span = p.span(start)
	return ty
}

func (p *parser) typeArg() TypeArg {
	switch p.tok.kind {
	case tokNumber:
		if !p.tok.isFloat {
			return p.number(nil)
		}
	case tokString:
		start := p.tok
		s := p.stringLit()
		if !p.acceptOp("=") {
			return s
		}
		e := &EnumValue{Name: s, Value: p.signedInteger()}
		e.Span = p.span(start)
		return e
	case tokIdent, tokQuoted:
		return p.typ()
	case tokEOF, tokOp, tokError, tokStop:
	}
	p.expected("a type, an integer or a string")
	return &Type{}
}

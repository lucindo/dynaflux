// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/diag"
)

// Parse parses one SQL file. path is the file's path as diagnostics show it.
//
// The diagnostics are the lexical and syntax errors (DF1001 to DF1010 and
// DF1101 to DF1103), duplicate clauses (DF1105) and annotation errors (DF1201
// to DF1203). After a lexical or syntax error the statement is Broken, and
// parsing resumes after the next ; outside strings and comments.
func Parse(path string, src []byte) (*File, []diag.Diagnostic) {
	f := &File{Path: path, Src: src}
	if off, ok := invalidUTF8(src); ok {
		d := newError(diag.Pos{File: path, Line: 1, Col: 1}, "DF1001", "save the file as UTF-8",
			"file is not valid UTF-8 (byte %d)", off)
		return f, []diag.Diagnostic{*d}
	}
	p := &parser{lex: newLexer(path, string(src))}
	p.tok = p.lex.next()
	p.arrive(true)
	f.Stmts = p.file()
	return f, p.diags
}

func invalidUTF8(src []byte) (int, bool) {
	for off := 0; off < len(src); {
		r, size := utf8.DecodeRune(src[off:])
		if r == utf8.RuneError && size == 1 {
			return off, true
		}
		off += size
	}
	return 0, false
}

// maxNesting bounds the recursion of expressions and types, so that no input
// can exhaust the stack.
const maxNesting = 1000

type parser struct {
	lex     *lexer
	tok     token  // the current token
	ahead   *token // the token after it, once peeked
	prevEnd int    // where the last consumed token ends
	diags   []diag.Diagnostic

	// After a lexical or syntax error, failed is set, tok is a tokStop token,
	// and resync restarts the lexer at skipFrom.
	failed   bool
	skipFrom token

	pending   []Annotation // annotations before the statement that starts at tok
	stmtName  string       // the statement's name, once known, for diagnostics
	inDefault bool         // array literals are allowed only in DEFAULT
	depth     int
}

func (p *parser) file() []Stmt {
	var stmts []Stmt
	for {
		switch {
		case p.tok.kind == tokEOF:
			p.misplaced(p.pending)
			return stmts
		case p.isOp(";"):
			p.misplaced(p.pending)
			p.advance()
			continue
		}
		anns := p.pending
		p.pending = nil
		s := p.statement(anns)
		if p.failed {
			s = brokenStmt(s)
			p.resync()
		}
		if s != nil {
			stmts = append(stmts, s)
		}
		if p.isOp(";") {
			p.advance()
		}
	}
}

func (p *parser) statement(anns []Annotation) Stmt {
	switch {
	case p.isKw("CREATE"):
		return p.create(anns)
	case p.isKw("SELECT"):
		for _, a := range anns {
			if a.Key == AnnotationName {
				p.stmtName = a.Name
				break
			}
		}
		return p.query(anns)
	case p.isKw("WITH"):
		p.unsupported("WITH", hintOneSource)
	case p.isOp("(") && (isKw(p.peek(), "SELECT") || isKw(p.peek(), "WITH")):
		p.unsupported("subquery", hintOneSource)
	case p.tok.kind == tokIdent:
		p.unsupportedStatement(p.tok.pos, strings.ToUpper(p.tok.text))
	default:
		p.expected("CREATE or SELECT")
	}
	return nil
}

// brokenStmt keeps what a statement with an error still defines.
func brokenStmt(s Stmt) Stmt {
	if s == nil {
		return nil
	}
	h := StmtHeader{Annotations: s.Header().Annotations, Broken: true}
	switch s := s.(type) {
	case *CreateStream:
		return &CreateStream{Span: s.Span, StmtHeader: h, Name: s.Name}
	case *CreateView:
		return &CreateView{Span: s.Span, StmtHeader: h, Name: s.Name}
	case *Query:
		return &Query{Span: s.Span, StmtHeader: h}
	}
	return nil
}

func (p *parser) advance() {
	if p.failed {
		return
	}
	endsStmt := p.isOp(";")
	if endsStmt {
		p.stmtName = ""
	}
	p.prevEnd = p.tok.end
	if p.ahead != nil {
		p.tok, p.ahead = *p.ahead, nil
	} else {
		p.tok = p.lex.next()
	}
	p.arrive(endsStmt)
}

// arrive handles the comments before the new current token: its annotations
// belong to the statement it starts, if it starts one.
func (p *parser) arrive(startsStmt bool) {
	for _, d := range p.tok.annDiags {
		d.Statement = p.stmtName
		p.diags = append(p.diags, d)
	}
	if startsStmt {
		p.pending = p.tok.anns
	} else {
		p.misplaced(p.tok.anns)
	}
}

func (p *parser) misplaced(anns []Annotation) {
	for _, a := range anns {
		if !a.Bad { // a bad one already has its diagnostic
			p.report(p.errorAt(a.Pos, "DF1203", "", "annotation must come right before a statement"))
		}
	}
}

func (p *parser) peek() token {
	if p.failed {
		return p.tok
	}
	if p.ahead == nil {
		t := p.lex.next()
		p.ahead = &t
	}
	return *p.ahead
}

// resync skips the rest of a statement with an error, up to its ; or the end
// of the file.
func (p *parser) resync() {
	p.lex.reset(p.skipFrom.start, p.skipFrom.pos)
	p.lex.skip()
	p.failed = false
	p.tok = p.lex.next()
}

// Keywords that the parser names in several places.
const (
	kwAnd   = "AND"
	kwNot   = "NOT"
	kwNull  = "NULL"
	kwLimit = "LIMIT"
	kwWhere = "WHERE"
	kwTTL   = "TTL"
)

func (p *parser) isKw(word string) bool { return isKw(p.tok, word) }

func isKw(t token, word string) bool {
	return t.kind == tokIdent && strings.EqualFold(t.text, word)
}

func (p *parser) isOp(op string) bool { return isOp(p.tok, op) }

func isOp(t token, op string) bool { return t.kind == tokOp && t.text == op }

func (p *parser) acceptOp(op string) bool {
	if p.isOp(op) {
		p.advance()
		return true
	}
	return false
}

func (p *parser) acceptKw(word string) bool {
	if p.isKw(word) {
		p.advance()
		return true
	}
	return false
}

func (p *parser) expectOp(op string) {
	if !p.acceptOp(op) {
		p.expected(op)
	}
}

func (p *parser) expectKw(word string) {
	if !p.acceptKw(word) {
		p.expected(word)
	}
}

func (p *parser) atStmtEnd() bool { return p.tok.kind == tokEOF || p.isOp(";") }

// span returns the span from start through the last consumed token.
func (p *parser) span(start token) Span {
	return Span{Pos: start.pos, Start: start.start, End: p.prevEnd}
}

func (p *parser) errorAt(pos diag.Pos, code, hint, format string, args ...any) diag.Diagnostic {
	d := newError(pos, code, hint, format, args...)
	d.Statement = p.stmtName
	return *d
}

// report adds a diagnostic that doesn't end the statement.
func (p *parser) report(d diag.Diagnostic) {
	p.diags = append(p.diags, d)
}

// fail reports the statement's lexical or syntax error. Later errors in the
// same statement are dropped.
func (p *parser) fail(d diag.Diagnostic) {
	if p.failed {
		return
	}
	d.Statement = p.stmtName
	p.diags = append(p.diags, d)
	p.failed = true
	p.skipFrom = p.tok
	p.tok = token{kind: tokStop}
	p.ahead = nil
}

func (p *parser) expected(what string) { p.expectedHint(what, "") }

// expectedHint reports that the current token is not what the grammar allows
// here. A token that is an error by itself is reported as such.
func (p *parser) expectedHint(what, hint string) {
	t := p.tok
	switch {
	case p.failed:
	case t.kind == tokError:
		p.fail(*t.err)
	case isOp(t, "."):
		p.fail(p.errorAt(t.pos, "DF1002", "", "unexpected character ."))
	case isOp(t, "||"):
		p.unsupported("||", "use concat(a, b)")
	case isOp(t, "::"):
		p.unsupported("::", hintConversion)
	case isOp(t, "->"):
		p.unsupported("->", hintNotInV1)
	case isOp(t, "<=>"):
		p.unsupported("<=>", hintNullSafe)
	case isOp(t, "?"):
		p.unsupported("?", "use if(cond, a, b)")
	default:
		p.fail(p.errorAt(t.pos, "DF1101", hint, "expected %s, found %s", what, t.found()))
	}
}

// Hints for unsupported SQL (DF1102).
const (
	hintOneSource  = "a statement reads one stream or view; put the data you need into the stream, and split the work into views and queries"
	hintManaged    = "Dynaflux creates and manages the tables"
	hintPagination = "no pagination in v1; narrow the key range instead"
	hintMerged     = "not needed: view rows are always merged"
	hintConversion = "no type conversions in v1"
	hintNullSafe   = "use IS NULL and ="
	hintNotInV1    = "not in v1"
	hintCompare    = "compare with = or IN"
)

func (p *parser) unsupported(feature, hint string) { p.unsupportedAt(p.tok.pos, feature, hint) }

func (p *parser) unsupportedAt(pos diag.Pos, feature, hint string) {
	p.fail(p.errorAt(pos, "DF1102", hint, "%s is not supported", feature))
}

func (p *parser) unsupportedStatement(pos diag.Pos, kind string) {
	p.fail(p.errorAt(pos, "DF1103",
		"use CREATE STREAM, CREATE MATERIALIZED VIEW or a named SELECT; for CREATE TABLE, use CREATE STREAM",
		"%s statements are not supported", kind))
}

// keep stores a clause in *field, unless the clause came before: then it
// reports the repeat (DF1105) and keeps the first one.
func keep[T any](p *parser, field **T, clause *T, name string, at token) {
	if *field != nil {
		p.repeated(name, at)
		return
	}
	*field = clause
}

func (p *parser) repeated(clause string, at token) {
	if !p.failed {
		p.report(p.errorAt(at.pos, "DF1105", "", "%s appears twice", clause))
	}
}

// nest guards the depth of expressions and types; the caller calls unnest
// when it returns true.
func (p *parser) nest() bool {
	if p.depth >= maxNesting {
		p.unsupported(fmt.Sprintf("nesting deeper than %d levels", maxNesting), "simplify the expression")
		return false
	}
	p.depth++
	return true
}

func (p *parser) unnest() { p.depth-- }

// orList joins words as "a, b or c".
func orList(words []string) string {
	if len(words) == 1 {
		return words[0]
	}
	return strings.Join(words[:len(words)-1], ", ") + " or " + words[len(words)-1]
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/diag"
)

type tokKind uint8

const (
	tokEOF    tokKind = iota
	tokIdent          // a plain identifier, which may be a keyword
	tokQuoted         // a quoted identifier
	tokNumber
	tokString
	tokOp    // an operator or punctuation; text says which
	tokError // a lexical error; err holds it
	tokStop  // what the parser sees after a syntax error, until it resyncs
)

type token struct {
	kind    tokKind
	text    string // as written
	value   string // identifier without quotes, unescaped string, or number without underscores
	isFloat bool
	float   float64
	pos     diag.Pos
	start   int
	end     int
	err     *diag.Diagnostic

	// Annotations, and their diagnostics, from the comments before this token.
	anns     []Annotation
	annDiags []diag.Diagnostic
}

// found renders t for "expected X, found Y".
func (t token) found() string {
	if t.kind == tokEOF {
		return "end of file"
	}
	return t.text
}

type lexer struct {
	path string
	src  string
	off  int
	line int
	col  int
}

const bom = "\xEF\xBB\xBF"

func newLexer(path, src string) *lexer {
	l := &lexer{path: path, src: src, line: 1, col: 1}
	if strings.HasPrefix(src, bom) {
		l.off = len(bom)
	}
	return l
}

func (l *lexer) pos() diag.Pos {
	return diag.Pos{File: l.path, Line: l.line, Col: l.col}
}

// at returns the byte i bytes ahead, or 0 past the end.
func (l *lexer) at(i int) byte {
	if l.off+i < len(l.src) {
		return l.src[l.off+i]
	}
	return 0
}

func (l *lexer) eof() bool { return l.off >= len(l.src) }

// atLineEnd reports whether a line ends i bytes ahead: at a line feed, or at the carriage
// return before one. A lone carriage return is an ordinary character (sql.md §3.1).
func (l *lexer) atLineEnd(i int) bool {
	c := l.at(i)
	return c == '\n' || c == '\r' && l.at(i+1) == '\n'
}

// step moves past one character, keeping the line and column.
func (l *lexer) step() {
	if l.src[l.off] == '\n' {
		l.off++
		l.line++
		l.col = 1
		return
	}
	_, size := utf8.DecodeRuneInString(l.src[l.off:])
	l.off += size
	l.col++
}

// stepASCII moves past n characters known to be ASCII and not line feeds.
func (l *lexer) stepASCII(n int) {
	l.off += n
	l.col += n
}

func (l *lexer) reset(off int, pos diag.Pos) {
	l.off, l.line, l.col = off, pos.Line, pos.Col
}

func newError(pos diag.Pos, code, hint, format string, args ...any) *diag.Diagnostic {
	return &diag.Diagnostic{
		Pos:      pos,
		Severity: diag.SeverityError,
		Code:     code,
		Message:  fmt.Sprintf(format, args...),
		Hint:     hint,
	}
}

// next returns the next token, with the annotations in the comments before it.
func (l *lexer) next() token {
	var anns []Annotation
	var annDiags []diag.Diagnostic
	for {
		for !l.eof() && isSpace(l.src[l.off]) {
			l.step()
		}
		switch {
		case l.at(0) == '-' && l.at(1) == '-':
			a, d := l.lineComment()
			if d != nil {
				annDiags = append(annDiags, *d)
			}
			if a != nil {
				anns = append(anns, *a)
			}
			continue
		case l.at(0) == '/' && l.at(1) == '*':
			start, pos := l.off, l.pos()
			if !l.blockComment() {
				tok := l.errorToken(start, pos, newError(pos, "DF1004",
					"add */; block comments nest, so every /* needs its own */", "block comment is not closed"))
				tok.anns, tok.annDiags = anns, annDiags
				return tok
			}
			continue
		}
		tok := l.scan()
		tok.anns, tok.annDiags = anns, annDiags
		return tok
	}
}

func (l *lexer) token(kind tokKind, start int, pos diag.Pos) token {
	text := l.src[start:l.off]
	return token{kind: kind, text: text, value: text, pos: pos, start: start, end: l.off}
}

func (l *lexer) errorToken(start int, pos diag.Pos, err *diag.Diagnostic) token {
	tok := l.token(tokError, start, pos)
	tok.err = err
	return tok
}

func (l *lexer) scan() token {
	start, pos := l.off, l.pos()
	if l.eof() {
		return l.token(tokEOF, start, pos)
	}
	c := l.src[l.off]
	switch {
	case isLetter(c) || c == '_':
		for !l.eof() && isIdentChar(l.src[l.off]) {
			l.stepASCII(1)
		}
		return l.token(tokIdent, start, pos)
	case isDigit(c) || c == '.' && isDigit(l.at(1)):
		return l.number(start, pos)
	case c == '\'':
		return l.string(start, pos)
	case c == '"' || c == '`':
		return l.quoted(start, pos)
	case c == '#' || c == '/' && l.at(1) == '/':
		kind := "#"
		switch {
		case c == '/':
			kind = "//"
		case l.at(1) == '!':
			kind = "#!"
		}
		l.skipLine()
		return l.errorToken(start, pos, newError(pos, "DF1005", "use -- or /* */", "%s comments are not supported", kind))
	}
	if op := l.op(); op != "" {
		l.stepASCII(len(op))
		return l.token(tokOp, start, pos)
	}
	r, _ := utf8.DecodeRuneInString(l.src[l.off:])
	l.step()
	return l.errorToken(start, pos, newError(pos, "DF1002", "", "unexpected character %s", showRune(r)))
}

// op returns the operator or punctuation at the current offset, or "".
// ||, ::, ->, <=> and ? are tokens only so that the parser can reject them
// with a hint.
func (l *lexer) op() string {
	rest := l.src[l.off:]
	for _, op := range [...]string{
		"<=>", "||", "::", "->", "==", "!=", "<>", "<=", ">=",
		"+", "-", "*", "/", "%", "=", "<", ">", "(", ")", "[", "]", "{", "}", ",", ";", ":", ".", "?",
	} {
		if strings.HasPrefix(rest, op) {
			return op
		}
	}
	return ""
}

// number scans a number token: every following letter, digit, _ and ., and a
// sign right after e or E, so that 123abc is one bad number, not a number and
// a name.
func (l *lexer) number(start int, pos diag.Pos) token {
	for !l.eof() {
		c := l.src[l.off]
		sign := (c == '+' || c == '-') && (l.src[l.off-1] == 'e' || l.src[l.off-1] == 'E')
		if !isIdentChar(c) && c != '.' && !sign {
			break
		}
		l.stepASCII(1)
	}
	text := l.src[start:l.off]
	badNumber := func() token {
		return l.errorToken(start, pos, newError(pos, "DF1008", hintNumber, "unsupported number %s", text))
	}
	digits, isFloat, ok := splitNumber(text)
	if !ok {
		return badNumber()
	}
	tok := l.token(tokNumber, start, pos)
	tok.value, tok.isFloat = digits, isFloat
	if isFloat {
		f, err := strconv.ParseFloat(digits, 64)
		if err != nil {
			return badNumber() // too large for Float64; an underflow is 0, without an error
		}
		tok.float = f
	}
	return tok
}

// splitNumber checks text against the number grammar and returns it without
// underscores.
func splitNumber(text string) (string, bool, bool) {
	rest, ok := cutDigits(text)
	if !ok {
		return "", false, false
	}
	isFloat := false
	if after, found := strings.CutPrefix(rest, "."); found {
		if rest, ok = cutDigits(after); !ok {
			return "", false, false
		}
		isFloat = true
	}
	if rest != "" && (rest[0] == 'e' || rest[0] == 'E') {
		exponent := rest[1:]
		if exponent != "" && (exponent[0] == '+' || exponent[0] == '-') {
			exponent = exponent[1:]
		}
		if rest, ok = cutDigits(exponent); !ok {
			return "", false, false
		}
		isFloat = true
	}
	if rest != "" {
		return "", false, false
	}
	return strings.ReplaceAll(text, "_", ""), isFloat, true
}

// cutDigits removes digits, with single underscores between them, from the
// start of s.
func cutDigits(s string) (string, bool) {
	if s == "" || !isDigit(s[0]) {
		return s, false
	}
	i := 1
	for i < len(s) && (isDigit(s[i]) || s[i] == '_' && i+1 < len(s) && isDigit(s[i+1])) {
		i++
	}
	return s[i:], true
}

const hintNumber = "write decimal digits with digits on both sides of the point, such as 0.5; inf and nan aren't allowed"

const hintUnclosedString = "close it with ', and write a line break as \\n"

// string scans a string literal.
func (l *lexer) string(start int, pos diag.Pos) token {
	l.stepASCII(1)
	var b strings.Builder
	for {
		if l.eof() || l.atLineEnd(0) {
			return l.errorToken(start, pos,
				newError(pos, "DF1003", hintUnclosedString, "string literal is not closed on this line"))
		}
		switch l.src[l.off] {
		case '\'':
			if l.at(1) == '\'' {
				b.WriteByte('\'')
				l.stepASCII(len("''"))
				continue
			}
			l.stepASCII(1)
			return l.stringToken(start, pos, b.String())
		case '\\':
			if err := l.escape(&b); err != nil {
				return l.errorToken(start, pos, err)
			}
		default:
			r, _ := utf8.DecodeRuneInString(l.src[l.off:])
			b.WriteRune(r)
			l.step()
		}
	}
}

func (l *lexer) stringToken(start int, pos diag.Pos, value string) token {
	if !utf8.ValidString(value) {
		return l.errorToken(start, pos,
			newError(pos, "DF1007", "check the \\xHH escapes", "string literal is not valid UTF-8"))
	}
	tok := l.token(tokString, start, pos)
	tok.value = value
	return tok
}

// escape scans a backslash escape into b. A backslash before a line break
// stays for the caller to report as an unclosed string.
func (l *lexer) escape(b *strings.Builder) *diag.Diagnostic {
	pos := l.pos()
	c := l.at(1)
	if l.off+1 >= len(l.src) || l.atLineEnd(1) {
		l.stepASCII(1)
		return nil
	}
	switch c {
	case '\'', '\\':
		b.WriteByte(c)
	case 'n':
		b.WriteByte('\n')
	case 'r':
		b.WriteByte('\r')
	case 't':
		b.WriteByte('\t')
	case '0':
		b.WriteByte(0)
	case 'x':
		digits := l.src[l.off+len(`\x`) : min(l.off+len(`\xHH`), len(l.src))]
		if v, err := hex.DecodeString(digits); err == nil && len(v) == 1 {
			b.WriteByte(v[0])
			l.stepASCII(len(`\xHH`))
			return nil
		}
		l.stepASCII(len(`\x`))
		return l.badEscape(pos, 'x')
	default:
		l.stepASCII(1)
		r, _ := utf8.DecodeRuneInString(l.src[l.off:])
		l.step()
		return l.badEscape(pos, r)
	}
	l.stepASCII(len(`\n`))
	return nil
}

func (l *lexer) badEscape(pos diag.Pos, r rune) *diag.Diagnostic {
	return newError(pos, "DF1006", `the escapes are '' \' \\ \n \r \t \0 \xHH`,
		"invalid escape \\%s in string literal", showRune(r))
}

// quoted scans a quoted identifier, which has no escapes. ClickHouse reads a
// backslash or a doubled quote inside it as an escape, so both are errors.
func (l *lexer) quoted(start int, pos diag.Pos) token {
	quote := l.src[l.off]
	l.stepASCII(1)
	for !l.eof() {
		c := l.src[l.off]
		if c == '\\' || l.atLineEnd(0) || c == quote && l.at(1) == quote {
			break
		}
		if c == quote {
			l.stepASCII(1)
			if l.off-start == len(`""`) {
				break
			}
			tok := l.token(tokQuoted, start, pos)
			tok.value = l.src[start+1 : l.off-1]
			return tok
		}
		l.step()
	}
	return l.errorToken(start, pos, newError(pos, "DF1010",
		`a quoted name can't be empty or contain its quote character, \ or a line break`, "invalid quoted identifier"))
}

// lineComment scans a -- comment and returns what parseAnnotation returns
// for it.
func (l *lexer) lineComment() (*Annotation, *diag.Diagnostic) {
	start, pos := l.off, l.pos()
	l.skipLine()
	end := l.off
	if end > start && l.src[end-1] == '\r' {
		end--
	}
	return parseAnnotation(l.src[start:end], Span{Pos: pos, Start: start, End: end})
}

func (l *lexer) skipLine() {
	for !l.eof() && l.src[l.off] != '\n' {
		l.step()
	}
}

// blockComment scans a nested /* */ comment and reports whether it is closed.
func (l *lexer) blockComment() bool {
	l.stepASCII(len("/*"))
	depth := 1
	for !l.eof() {
		switch {
		case l.at(0) == '/' && l.at(1) == '*':
			depth++
			l.stepASCII(len("/*"))
		case l.at(0) == '*' && l.at(1) == '/':
			depth--
			l.stepASCII(len("*/"))
			if depth == 0 {
				return true
			}
		default:
			l.step()
		}
	}
	return false
}

// skip moves to the next ; outside strings, quoted identifiers and comments,
// or to the end of the file. Unclosed strings and quoted identifiers end at
// their line end, and #, #! and // comments count as comments, as they do for
// ClickHouse.
func (l *lexer) skip() {
	for !l.eof() {
		c := l.src[l.off]
		switch {
		case c == ';':
			return
		case c == '\'' || c == '"' || c == '`':
			l.skipQuoted(c)
		case c == '-' && l.at(1) == '-', c == '#', c == '/' && l.at(1) == '/':
			l.skipLine()
		case c == '/' && l.at(1) == '*':
			l.blockComment()
		default:
			l.step()
		}
	}
}

// skipQuoted needs no case for a doubled quote: closing at the first and
// reopening at the second skips the same text.
func (l *lexer) skipQuoted(quote byte) {
	l.stepASCII(1)
	for !l.eof() && !l.atLineEnd(0) {
		c := l.src[l.off]
		l.step()
		switch c {
		case '\\':
			if !l.eof() && !l.atLineEnd(0) {
				l.step()
			}
		case quote:
			return
		}
	}
}

func isSpace(c byte) bool     { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }
func isLetter(c byte) bool    { return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' }
func isDigit(c byte) bool     { return '0' <= c && c <= '9' }
func isIdentChar(c byte) bool { return isLetter(c) || isDigit(c) || c == '_' }

// showRune renders r for a message: itself when it is visible, else U+XXXX.
func showRune(r rune) string {
	if r != ' ' && unicode.IsPrint(r) {
		return string(r)
	}
	return fmt.Sprintf("U+%04X", r)
}

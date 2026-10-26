// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import "strings"

// Text returns the source text of n as diagnostics quote it: without
// comments, and with each run of whitespace written as one space.
func (f *File) Text(n Node) string {
	s := n.Extent()
	return normalize(string(f.Src[s.Start:s.End]))
}

func normalize(text string) string {
	l := &lexer{src: text, line: 1, col: 1}
	var b strings.Builder
	space := false
	for !l.eof() {
		start := l.off
		switch c := text[l.off]; {
		case isSpace(c):
			l.step()
			space = true
			continue
		case c == '-' && l.at(1) == '-':
			l.skipLine()
			space = true
			continue
		case c == '/' && l.at(1) == '*':
			l.blockComment()
			space = true
			continue
		case c == '\'' || c == '"' || c == '`':
			l.skipQuoted(c)
		default:
			l.step()
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteString(text[start:l.off])
	}
	return b.String()
}

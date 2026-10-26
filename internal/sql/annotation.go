// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"strings"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/diag"
)

const blanks = " \t"

// parseAnnotation reads a -- comment, without its line end. It returns the
// annotation, if its key is known, and the diagnostic for a bad one; both are
// nil for an ordinary comment.
func parseAnnotation(comment string, span Span) (*Annotation, *diag.Diagnostic) {
	rest := strings.TrimLeft(comment[len("--"):], blanks)
	a := &Annotation{Span: span}
	var after string
	switch {
	case strings.HasPrefix(rest, "name:"):
		a.Key, after = AnnotationName, rest[len("name:"):]
	case strings.HasPrefix(rest, "expose:"):
		a.Key, after = AnnotationExpose, rest[len("expose:"):]
	case strings.HasPrefix(rest, "go:"):
		key := rest
		if i := strings.IndexAny(rest, blanks); i >= 0 {
			key = rest[:i]
		}
		if key != "go:type" {
			return nil, unknownAnnotation(comment, span.Pos)
		}
		a.Key, after = AnnotationGoType, rest[len(key):]
	default:
		return nil, nil
	}

	words := strings.FieldsFunc(after, func(r rune) bool { return r == ' ' || r == '\t' })
	want := 1
	if a.Key == AnnotationName {
		want = 2
	}
	if after == "" || !strings.ContainsRune(blanks, rune(after[0])) || len(words) != want {
		a.Bad = true
		return a, malformedAnnotation(comment, span.Pos)
	}

	var d *diag.Diagnostic
	switch a.Key {
	case AnnotationName:
		a.Many = words[1] == ":many"
		if words[1] != ":one" && !a.Many {
			a.Bad, d = true, unknownAnnotation(comment, span.Pos)
		}
	case AnnotationExpose:
		if words[0] != "http" {
			a.Bad, d = true, unknownAnnotation(comment, span.Pos)
		}
		return a, d
	case AnnotationGoType:
	}
	a.Name = words[0]
	nameOff := len(comment) - len(strings.TrimLeft(after, blanks))
	a.NamePos = span.Pos
	a.NamePos.Col += utf8.RuneCountInString(comment[:nameOff])
	return a, d
}

func malformedAnnotation(comment string, pos diag.Pos) *diag.Diagnostic {
	return newError(pos, "DF1201",
		"the forms are -- name: <Name> :one, -- name: <Name> :many, -- expose: http, -- go:type <Name>",
		"malformed annotation: %s", strings.TrimRight(comment, blanks))
}

func unknownAnnotation(comment string, pos diag.Pos) *diag.Diagnostic {
	return newError(pos, "DF1202", "the keys are name, expose and go:type; the values are :one, :many and http",
		"unknown annotation %s", strings.TrimRight(comment, blanks))
}

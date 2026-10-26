// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package diag holds the diagnostics the Dynaflux front end reports for SQL
// definitions, dynaflux.yaml and dynaflux.lock, in the format of
// docs/spec/sql.md appendix A.1.
package diag

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

// Severity tells whether a diagnostic fails validation.
type Severity uint8

// Severities.
const (
	SeverityError Severity = iota + 1
	SeverityWarning
)

func (s Severity) String() string {
	switch s {
	case SeverityError:
		return "error"
	case SeverityWarning:
		return "warning"
	default:
		return fmt.Sprintf("Severity(%d)", uint8(s))
	}
}

// Pos is a position in a project file.
type Pos struct {
	File string // relative to the project root, with / separators
	Line int    // from 1
	Col  int    // from 1, in Unicode code points
}

func (p Pos) String() string {
	return fmt.Sprintf("%s:%d:%d", p.File, p.Line, p.Col)
}

// Diagnostic is one problem found in the project.
type Diagnostic struct {
	Pos      Pos
	Severity Severity
	Code     string // stable code such as DF3101
	// Statement names the stream, view or query, when the statement got far
	// enough to have a name.
	Statement string
	Message   string
	Hint      string
}

// String renders d as validate prints it: one line, plus a hint line when
// d has a hint. Control characters print escaped, as \n or \x1b: the text can
// come from a cloned repository, and must not fake a line or drive the terminal.
func (d Diagnostic) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s %s: ", d.Pos, d.Severity, d.Code)
	if d.Statement != "" {
		b.WriteString(d.Statement)
		b.WriteString(": ")
	}
	b.WriteString(d.Message)
	line := escapeControl(b.String())
	if d.Hint == "" {
		return line
	}
	return line + "\n  hint: " + escapeControl(d.Hint)
}

func escapeControl(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsControl(r) {
			q := strconv.QuoteRune(r)
			b.WriteString(q[1 : len(q)-1])
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Sort orders ds by path, line, column, code and message, so output is the
// same on every run.
func Sort(ds []Diagnostic) {
	slices.SortStableFunc(ds, func(a, b Diagnostic) int {
		return cmp.Or(
			cmp.Compare(a.Pos.File, b.Pos.File),
			cmp.Compare(a.Pos.Line, b.Pos.Line),
			cmp.Compare(a.Pos.Col, b.Pos.Col),
			cmp.Compare(a.Code, b.Code),
			cmp.Compare(a.Message, b.Message),
		)
	})
}

// HasErrors reports whether any diagnostic in ds is an error.
func HasErrors(ds []Diagnostic) bool {
	return slices.ContainsFunc(ds, func(d Diagnostic) bool { return d.Severity == SeverityError })
}

// Write prints ds to w, one diagnostic per line, in the order given.
func Write(w io.Writer, ds []Diagnostic) error {
	for _, d := range ds {
		if _, err := fmt.Fprintln(w, d); err != nil {
			return fmt.Errorf("write diagnostics: %w", err)
		}
	}
	return nil
}

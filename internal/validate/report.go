// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"fmt"

	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
)

// diags collects the diagnostics of a project.
type diags struct {
	list []diag.Diagnostic
}

func (d *diags) add(sev diag.Severity, stmt string, pos diag.Pos, code, hint, msg string) {
	d.list = append(d.list, diag.Diagnostic{
		Pos: pos, Severity: sev, Code: code, Statement: stmt, Message: msg, Hint: hint,
	})
}

// stmt reports the diagnostics of one statement, prefixed with its name.
type stmt struct {
	d    *diags
	f    *sql.File
	name string
}

func (s *stmt) errorf(pos diag.Pos, code, hint, format string, args ...any) {
	s.d.add(diag.SeverityError, s.name, pos, code, hint, fmt.Sprintf(format, args...))
}

func (s *stmt) warnf(pos diag.Pos, code, hint, format string, args ...any) {
	s.d.add(diag.SeverityWarning, s.name, pos, code, hint, fmt.Sprintf(format, args...))
}

// text returns the source text of n as diagnostics quote it.
func (s *stmt) text(n sql.Node) string {
	return s.f.Text(n)
}

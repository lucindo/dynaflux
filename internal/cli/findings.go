// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/meta"
)

// finding is a diagnostic about a deployed store (docs/spec/lifecycle.md §12).
type finding struct {
	pos      *diag.Pos // its local source; nil when it has none
	regions  []string
	severity diag.Severity
	code     string
	message  string
	hint     string
}

func regionFinding(region string, sev diag.Severity, code, msg, hint string) finding {
	return finding{regions: []string{region}, severity: sev, code: code, message: msg, hint: hint}
}

// write prints f: from its source, or else its regions.
func (f finding) write(b *strings.Builder) {
	where := strings.Join(f.regions, ", ")
	if f.pos != nil {
		where = f.pos.String()
	}
	fmt.Fprintf(b, "%s: %s %s: %s\n  hint: %s\n", where, f.severity, f.code, f.message, f.hint)
}

// findingJSON is a finding in -json output.
type findingJSON struct {
	File     string   `json:"file,omitzero"`
	Line     int      `json:"line,omitzero"`
	Column   int      `json:"column,omitzero"`
	Regions  []string `json:"regions"`
	Code     string   `json:"code"`
	Severity string   `json:"severity"`
	Message  string   `json:"message"`
	Hint     string   `json:"hint"`
}

func (f finding) json() findingJSON {
	j := findingJSON{Regions: f.regions, Code: f.code, Severity: f.severity.String(), Message: f.message, Hint: f.hint}
	if f.pos != nil {
		j.File, j.Line, j.Column = f.pos.File, f.pos.Line, f.pos.Col
	}
	return j
}

// formatFinding is DF7001 for a deployed catalog in a newer format than this dynaflux reads,
// and false for one it reads.
func formatFinding(region string, reg meta.Registered) (finding, bool) {
	if reg.Format <= catalog.FormatVersion {
		return finding{}, false
	}
	return regionFinding(region, diag.SeverityError, "DF7001",
		fmt.Sprintf("the deployed catalog has format %d; this dynaflux reads formats %d to %d",
			reg.Format, catalog.MinFormatVersion, catalog.FormatVersion),
		"upgrade dynaflux to the version that generated it, "+reg.Generator), true
}

// worst returns the exit status that findings call for: exitErrors for an error.
func worst(fs []finding) int {
	for _, f := range fs {
		if f.severity == diag.SeverityError {
			return exitErrors
		}
	}
	return exitOK
}

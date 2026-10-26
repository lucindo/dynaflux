// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/validate"
)

// codeUnreadable is the diagnostic for a file that can't be read, an I/O problem that
// exits with status 2 (docs/spec/config.md).
const codeUnreadable = "DF6002"

func validateCommand() command {
	return command{
		name:    "validate",
		summary: "check dynaflux.yaml and the SQL definitions",
		doc: `Validate finds dynaflux.yaml in the working directory or the nearest parent that has
one, and checks it and every SQL file it matches. It prints each problem on
standard error as

  path:line:col: severity code: message
    hint: hint

with paths relative to the project root.

Exit status: 0 when there are no errors (warnings are allowed), 1 when there is an
error, and 2 for usage and I/O problems.
`,
		setup: func(*flag.FlagSet) func(*cli, []string) int {
			return (*cli).validate
		},
	}
}

func (c *cli) validate(args []string) int {
	if len(args) > 0 {
		return c.usageError("validate", "unexpected arguments: "+strings.Join(args, " "))
	}
	_, ds, status := c.load()
	return c.diagnostics(ds, status)
}

// project is a project that compiled.
type project struct {
	dir string // the project root
	cfg *config.Config
	cat *catalog.Catalog
	// lock is dynaflux.lock as generate writes it, and lockChanges how it differs from the
	// lock read.
	lock        *catalog.Lock
	lockChanges []catalog.LockChange
}

// load finds the project that governs the working directory and compiles it. It returns
// the diagnostics and the exit status they call for. When there is no project to compile,
// it reports that itself.
func (c *cli) load() (*project, []diag.Diagnostic, int) {
	path, err := config.Find(c.workdir)
	if errors.Is(err, config.ErrNotFound) {
		c.errorf("error DF6001: %v\n  hint: run the command inside your project, or create %s at its root",
			err, config.FileName)
		return nil, nil, exitUsage
	}
	if err != nil {
		c.errorf("dynaflux: %v", err)
		return nil, nil, exitUsage
	}
	dir := filepath.Dir(path)
	// The os.Root keeps every read inside the project, links included.
	root, err := os.OpenRoot(dir)
	if err != nil {
		c.errorf("dynaflux: %v", err)
		return nil, nil, exitUsage
	}
	defer root.Close()
	r, ds := validate.Compile(root.FS(), dir)
	switch {
	case slices.ContainsFunc(ds, func(d diag.Diagnostic) bool { return d.Code == codeUnreadable }):
		return nil, ds, exitUsage
	case diag.HasErrors(ds):
		return nil, ds, exitErrors
	default:
		return &project{dir: dir, cfg: r.Config, cat: r.Catalog, lock: r.Lock, lockChanges: r.LockChanges}, ds, exitOK
	}
}

// diagnostics writes ds and returns status, or exitUsage when the write fails.
func (c *cli) diagnostics(ds []diag.Diagnostic, status int) int {
	if err := diag.Write(&c.errOut, ds); err != nil {
		c.errorf("dynaflux: %v", err)
		return exitUsage
	}
	return status
}

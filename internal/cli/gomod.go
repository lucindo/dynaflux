// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// goMod is what dynaflux reads of a go.mod file.
type goMod struct {
	module string // the path in the module directive; "" without one
	// library is the version of the Dynaflux module that a require directive names, and
	// replace the target of a replace directive that names the module; "" without one.
	library, replace string
}

// parseGoMod reads the module directive of a go.mod file, and the require and replace
// directives that name the Dynaflux module, in their one-line and block forms.
func parseGoMod(data []byte) goMod {
	var m goMod
	block := "" // the directive of the block the line is in
	for line := range strings.Lines(string(data)) {
		line, _, _ = strings.Cut(line, "//")
		f := strings.Fields(line)
		switch {
		case len(f) == 0:
			continue
		case block != "" && f[0] == ")":
			block = ""
			continue
		case block != "":
			f = append([]string{block}, f...)
		case f[len(f)-1] == "(" || len(f) == 1 && strings.HasSuffix(f[0], "("): // go accepts "require("
			block = strings.TrimSuffix(f[0], "(")
			continue
		}
		verb, args := f[0], f[1:]
		if len(args) == 0 {
			continue
		}
		path := args[0]
		if unquoted, err := strconv.Unquote(path); err == nil {
			path = unquoted
		}
		switch {
		case verb == "module" && m.module == "":
			m.module = path
		case verb == "require" && path == modulePath && len(args) > 1:
			m.library = args[1]
		case verb == "replace" && path == modulePath:
			if i := slices.Index(args, "=>"); i > 0 {
				m.replace = strings.Join(args[i+1:], " ")
			}
		}
	}
	return m
}

// readGoMod reads the go.mod of the module that holds dir: the one in dir or the nearest
// directory above it, up to c.goModCeiling. It returns that go.mod's directory, or "" when
// there is none.
func (c *cli) readGoMod(dir string) (string, goMod, error) {
	mod, err := filepath.Abs(dir)
	if err != nil {
		return "", goMod{}, err
	}
	for {
		data, readErr := os.ReadFile(filepath.Join(mod, "go.mod"))
		if readErr == nil {
			return mod, parseGoMod(data), nil
		}
		if !errors.Is(readErr, fs.ErrNotExist) {
			return "", goMod{}, readErr
		}
		parent := filepath.Dir(mod)
		if parent == mod || mod == c.goModCeiling {
			return "", goMod{}, nil
		}
		mod = parent
	}
}

// needsLibrary reports whether a project in module m must add the Dynaflux library to its
// go.mod: the module neither requires it nor is it.
func (m goMod) needsLibrary() bool {
	return m.library == "" && m.module != modulePath
}

// libraryText is the Dynaflux library that m builds with, as version prints it.
func (m goMod) libraryText() string {
	switch {
	case m.module == modulePath:
		return develVersion // the module is Dynaflux's own checkout
	case m.library == "":
		return "(not in go.mod)"
	case m.replace != "":
		return m.library + " => " + m.replace
	}
	return m.library
}

// otherLibrary returns the warning that generated code from release, the CLI's, may not
// build with the Dynaflux library that m requires, or "" when they match or can't be
// compared: a CLI or library from a checkout, a replaced library, or none.
func (m goMod) otherLibrary(release string) string {
	if release == develVersion || m.module == modulePath || m.replace != "" || m.library == "" || m.library == release {
		return ""
	}
	return "this dynaflux is " + release + ", and go.mod requires " + modulePath + " " + m.library +
		": code that one release generates may not build with another\n" +
		"  hint: run the release that go.mod requires with go tool dynaflux, " +
		"or require this one with go get " + modulePath + "@" + release
}

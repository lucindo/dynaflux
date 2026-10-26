// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/diag"
)

const defaultSQLPattern = "dynaflux/**/*.sql"

// Pattern is an sql glob pattern.
type Pattern struct {
	Glob string // cleaned
	// Pos is the pattern in dynaflux.yaml, or the top-level mapping for the default pattern.
	Pos diag.Pos
}

func (d *decoder) sqlPatterns(top object, c *Config) {
	if !top.has("sql") {
		c.SQL = []Pattern{{Glob: defaultSQLPattern, Pos: top.pos}}
		return
	}
	seen := map[string]bool{}
	for _, e := range d.list(top, "sql", `["`+defaultSQLPattern+`"]`, true) {
		if !d.validPattern(e) {
			continue
		}
		g := path.Clean(e.Value)
		if !d.repeated(seen, "sql", g, e) {
			c.SQL = append(c.SQL, Pattern{Glob: g, Pos: at(e)})
		}
	}
}

// validPattern checks pattern n before it is cleaned, so that dynaflux/**/../*.sql is
// refused.
func (d *decoder) validPattern(n *yaml.Node) bool {
	const (
		hintRelative = "write the pattern relative to the project root"
		hintSyntax   = "patterns use *, ?, [a-z], and ** for any number of directories"
	)
	p := n.Value
	switch {
	case p == "":
		d.errorf(at(n), "DF6202", hintSyntax, "an sql pattern is empty")
		return false
	case strings.HasPrefix(p, "/"):
		d.errorf(at(n), "DF6201", hintRelative, "sql pattern %q is absolute", p)
		return false
	case strings.HasSuffix(p, "/"):
		d.errorf(at(n), "DF6202", hintSyntax, "sql pattern %q ends in /", p)
		return false
	}
	for seg := range strings.SplitSeq(p, "/") {
		switch {
		case seg == "..":
			d.errorf(at(n), "DF6201", hintRelative, "sql pattern %q has a .. segment", p)
			return false
		case seg == "**":
		case strings.Contains(seg, "**"):
			d.errorf(at(n), "DF6203", "write ** as a whole segment, as in dynaflux/**/*.sql",
				"sql pattern %q has ** inside a segment", p)
			return false
		default:
			if _, err := path.Match(seg, ""); err != nil {
				d.errorf(at(n), "DF6202", hintSyntax, "sql pattern %q has bad syntax in %q", p, seg)
				return false
			}
		}
	}
	return true
}

// SQLFiles expands the sql patterns on fsys, the project root. root names that directory as
// the user should see it, such as its absolute path; the DF6204 message ends with it, so it
// must not be empty. SQLFiles returns the matched files' paths, each once, in byte order,
// and sorted diagnostics.
func (c *Config) SQLFiles(fsys fs.FS, root string) ([]string, []diag.Diagnostic) {
	var d decoder
	found := map[string]bool{}
	for _, p := range c.SQL {
		before := len(d.diags)
		files, err := d.expand(fsys, p)
		if err != nil {
			d.errorf(p.Pos, "DF6002", "check the path and its permissions", "sql pattern %q: %v", p.Glob, err)
			continue
		}
		if len(files) == 0 && len(d.diags) == before {
			d.errorf(p.Pos, "DF6204",
				"patterns are relative to the project root, and wildcards skip names that start with . or _, and testdata",
				"sql pattern %q matches no files in %s", p.Glob, root)
		}
		for _, f := range files {
			found[f] = true
		}
	}
	diag.Sort(d.diags)
	return slices.Sorted(maps.Keys(found)), d.diags
}

// expand returns the files in fsys that p matches.
func (d *decoder) expand(fsys fs.FS, p Pattern) ([]string, error) {
	segs := strings.Split(p.Glob, "/")
	var files []string
	err := fs.WalkDir(fsys, ".", func(name string, e fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if name == "." {
			return nil
		}
		names := strings.Split(name, "/")
		if e.IsDir() {
			if match(segs, names, true) {
				return nil
			}
			return fs.SkipDir
		}
		if !match(segs, names, false) {
			return nil
		}
		switch {
		case e.Type().IsRegular():
			files = append(files, name)
		case e.Type()&fs.ModeSymlink != 0:
			ok, err := d.linkedFile(fsys, name, p)
			if err != nil {
				return err
			}
			if ok {
				files = append(files, name)
			}
		}
		return nil
	})
	return files, err
}

// linkedFile reports whether the link at name resolves to a regular file inside fsys. It
// reports DF6205 for a link that is absolute or whose target path leaves fsys, and skips a
// broken link. A link that leaves through a link to a directory outside, which only
// [os.Root] catches, is a read error.
func (d *decoder) linkedFile(fsys fs.FS, name string, p Pattern) (bool, error) {
	info, err := fs.Stat(fsys, name)
	if err == nil {
		return info.Mode().IsRegular(), nil
	}
	// Checked before ErrNotExist: fstest.MapFS reports an escaping link as missing.
	if target, out := leavesRoot(fsys, name); out {
		d.errorf(p.Pos, "DF6205", "move the file into the project",
			"%s links to %s; links must be relative and stay inside the project root", name, target)
		return false, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// leavesRoot reports whether the link at name points outside fsys: by an absolute path,
// which [os.Root] refuses even inside the root, or through "..".
func leavesRoot(fsys fs.FS, name string) (string, bool) {
	target, err := fs.ReadLink(fsys, name)
	if err != nil {
		// Unknown: the caller reports the Stat error instead.
		return "", false
	}
	if filepath.IsAbs(target) {
		return target, true
	}
	j := path.Join(path.Dir(name), filepath.ToSlash(target))
	return target, j == ".." || strings.HasPrefix(j, "../")
}

// match reports whether the file at names, a path split at "/", matches the pattern
// segments segs. For a directory (dir set), it reports whether a file below it may match,
// so the walk enters it.
func match(segs, names []string, dir bool) bool {
	// Filled from the last segment: next[j] reports whether the segments after seg match
	// names[j:], and cur[j] whether seg and those after it do. Backtracking over ** instead
	// takes time exponential in the number of ** segments.
	n := len(names)
	next := make([]bool, n+1)
	// Both used up: a file matches, and nothing below a directory can.
	next[n] = !dir
	for _, seg := range slices.Backward(segs) {
		cur := make([]bool, n+1)
		// The path used up before the pattern: a directory may still hold a match.
		cur[n] = dir
		for j := n - 1; j >= 0; j-- {
			isDir := dir || j < n-1
			if seg == "**" {
				// ** matches no directory, or names[j] and maybe more.
				cur[j] = next[j] || isDir && !skipped(seg, names[j], true) && cur[j+1]
			} else {
				cur[j] = matchName(seg, names[j], isDir) && next[j+1]
			}
		}
		next = cur
	}
	return next[0]
}

func matchName(seg, name string, isDir bool) bool {
	if !strings.ContainsAny(seg, `*?[\`) {
		return seg == name
	}
	if skipped(seg, name, isDir) {
		return false
	}
	ok, err := path.Match(seg, name)
	return err == nil && ok // validPattern checked the syntax
}

// skipped reports whether wildcard segment seg skips name, as the go command does: names
// that start with . or _, unless seg starts with the same character, and directories named
// testdata.
func skipped(seg, name string, isDir bool) bool {
	if isDir && name == "testdata" {
		return true
	}
	return (name[0] == '.' || name[0] == '_') && seg[0] != name[0]
}

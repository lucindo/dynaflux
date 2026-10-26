// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
)

// lockPos is where diagnostics about dynaflux.lock point, except a JSON syntax error.
func lockPos() diag.Pos {
	return diag.Pos{File: catalog.LockFileName, Line: 1, Col: 1}
}

// readLock reads dynaflux.lock: an empty lock when there is none (DF6002, DF6013).
func readLock(fsys fs.FS) (*catalog.Lock, []diag.Diagnostic) {
	data, err := fs.ReadFile(fsys, catalog.LockFileName)
	if errors.Is(err, fs.ErrNotExist) {
		return &catalog.Lock{}, nil
	}
	if err != nil {
		return nil, []diag.Diagnostic{{
			Pos: lockPos(), Severity: diag.SeverityError, Code: "DF6002",
			Message: fmt.Sprintf("can't read %s: %v", catalog.LockFileName, err),
			Hint:    "check the path and its permissions",
		}}
	}
	lock, err := catalog.ParseLock(data)
	if err != nil {
		return nil, []diag.Diagnostic{{
			Pos: syntaxErrorPos(data, err), Severity: diag.SeverityError, Code: "DF6013",
			Message: fmt.Sprintf("can't use %s: %v", catalog.LockFileName, err),
			Hint: "if it has merge conflicts, keep the lock of the branch that merged first and run dynaflux generate; " +
				"undo hand edits, and don't delete the file, which renumbers every view",
		}}
	}
	return lock, nil
}

// syntaxErrorPos returns where a JSON syntax error in data is, or line 1, column 1 for
// any other error.
func syntaxErrorPos(data []byte, err error) diag.Pos {
	pos := lockPos()
	var se *json.SyntaxError
	if !errors.As(err, &se) {
		return pos
	}
	// Offset counts the bytes read, the offending one included.
	for _, r := range string(data[:max(se.Offset-1, 0)]) {
		if r == '\n' {
			pos.Line, pos.Col = pos.Line+1, 1
		} else {
			pos.Col++
		}
	}
	return pos
}

// numberViews numbers the views by dynaflux.lock (sql.md §9.9). A view whose definition
// has errors, and so no fingerprint, keeps its locked versions, retained. The views keep
// the order of the SQL files, and those that only the lock holds follow by name.
func (p *project) numberViews() {
	defs := map[string]catalog.ViewVersion{}
	for _, d := range p.decls {
		if d.view != nil && d.view.version.Fingerprint != "" {
			defs[d.name] = d.view.version
		}
	}
	p.next, p.lockChanges = p.lock.Apply(defs)
	views := p.next.CatalogViews()
	for _, d := range p.decls {
		if d.view == nil {
			continue
		}
		if i := slices.IndexFunc(views, func(v catalog.View) bool { return v.Name == d.name }); i >= 0 {
			p.views = append(p.views, views[i])
			views = slices.Delete(views, i, i+1)
		}
	}
	p.views = append(p.views, views...)
}

// nextNumber returns the number view would get with a new definition.
func (p *project) nextNumber(view string) int {
	n := 1
	for _, vv := range p.lock.Views[view] {
		n = max(n, vv.Number+1)
	}
	return n
}

// checkRetained reports a retained view version that the projector couldn't keep
// maintaining (DF6015): its stream is gone, it reads a column that the stream no longer
// has with the type it reads, or a stream took its view's name.
func (p *project) checkRetained() {
	for _, v := range p.views {
		for i := range v.Versions {
			vv := &v.Versions[i]
			if vv.State != catalog.StateRetained {
				continue
			}
			id := catalog.VersionID(v.Name, vv.Number)
			if problem, fix := p.retainedProblem(v.Name, vv); problem != "" {
				p.d.add(diag.SeverityError, "", lockPos(), "DF6015", fix+", or retire the version: dynaflux retire "+id,
					fmt.Sprintf("%s, retained in %s, %s", id, catalog.LockFileName, problem))
			}
		}
	}
}

// retainedProblem returns why the retained version vv of view can't be maintained, and
// what fixes it, or "" when it can.
func (p *project) retainedProblem(view string, vv *catalog.ViewVersion) (string, string) {
	if d, ok := p.tables[view]; ok && d.kind == declStream {
		return "has the name of stream " + view, "rename the stream"
	}
	d, ok := p.tables[vv.Stream]
	if !ok || d.kind != declStream {
		return "reads stream " + vv.Stream + ", which the SQL files no longer define", "define the stream again"
	}
	if d.stream == nil {
		return "", "" // The stream's statement has errors, reported already.
	}
	// A stream column wins over an alias, so the version would read the column instead.
	if aj := vv.ArrayJoin; aj != nil {
		if _, found := d.stream.stream.Column(aj.Alias); found {
			return fmt.Sprintf("names its ARRAY JOIN element %s, which is now a column of stream %s", aj.Alias, vv.Stream),
				fmt.Sprintf("rename column %s of stream %s", aj.Alias, vv.Stream)
		}
	}
	for _, ref := range columnRefs(vv) {
		if d.stream.badColumns[ref.Name] {
			continue
		}
		if c, found := d.stream.stream.Column(ref.Name); !found || c.Type != ref.Type {
			return fmt.Sprintf("reads column %s of stream %s as %s, which the stream no longer has with that type",
					ref.Name, vv.Stream, ref.Type),
				fmt.Sprintf("give stream %s the column %s %s again", vv.Stream, ref.Name, ref.Type)
		}
	}
	return "", ""
}

// columnRefs returns the stream columns that vv reads: its OpColumn nodes, except the
// ARRAY JOIN alias.
func columnRefs(vv *catalog.ViewVersion) []*catalog.Expr {
	roots := []*catalog.Expr{vv.Where}
	if vv.ArrayJoin != nil {
		roots = append(roots, &vv.ArrayJoin.Array)
	}
	for i := range vv.Keys {
		roots = append(roots, &vv.Keys[i].Expr)
	}
	for i := range vv.Aggregates {
		a := &vv.Aggregates[i]
		roots = append(roots, a.Cond)
		for j := range a.Args {
			roots = append(roots, &a.Args[j])
		}
	}
	var refs []*catalog.Expr
	var walk func(e *catalog.Expr)
	walk = func(e *catalog.Expr) {
		if e.Op == catalog.OpColumn && (vv.ArrayJoin == nil || e.Name != vv.ArrayJoin.Alias) {
			refs = append(refs, e)
		}
		for i := range e.Args {
			walk(&e.Args[i])
		}
	}
	for _, e := range roots {
		if e != nil {
			walk(e)
		}
	}
	return refs
}

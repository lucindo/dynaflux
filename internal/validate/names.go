// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
)

type declKind uint8

const (
	declStream declKind = iota + 1
	declView
	declQuery
)

func (k declKind) String() string {
	switch k {
	case declStream:
		return "CREATE STREAM"
	case declView:
		return "CREATE MATERIALIZED VIEW"
	case declQuery:
		return "SELECT"
	}
	return "statement"
}

// decl is one statement and the name it defines.
type decl struct {
	st   *stmt
	node sql.Stmt
	kind declKind
	// name is the stream or view name, or the query's -- name:, as written; it prefixes
	// the statement's diagnostics.
	name string
	pos  diag.Pos // where name is written
	// defines is false when the statement defines no name: a broken one without a name,
	// or a query without a well-formed -- name:.
	defines bool
	// earlier is the first definition of name when this statement repeats it (DF1303).
	earlier *decl
	// stream, view and query are the compiled statement, kept only on the first
	// definition of a name.
	stream *streamInfo
	view   *viewInfo
	query  *queryInfo
}

func (d *decl) broken() bool {
	return d.node.Header().Broken
}

func (p *project) declare(f *sql.File, s sql.Stmt) {
	d := &decl{node: s}
	switch s := s.(type) {
	case *sql.CreateStream:
		d.kind, d.name, d.pos = declStream, s.Name.Name, s.Name.Pos
		d.defines = d.name != ""
	case *sql.CreateView:
		d.kind, d.name, d.pos = declView, s.Name.Name, s.Name.Pos
		d.defines = d.name != ""
	case *sql.Query:
		d.kind = declQuery
		if a, ok := nameAnnotation(s); ok {
			d.name, d.pos = a.Name, a.NamePos
			d.defines = !a.Bad
		}
	}
	d.st = &stmt{d: &p.d, f: f, name: d.name}
	p.decls = append(p.decls, d)
	if !d.defines {
		return
	}
	names := p.tables
	if d.kind == declQuery {
		names = p.queries
	}
	if first, ok := names[d.name]; ok {
		d.earlier = first
	} else {
		names[d.name] = d
	}
}

func nameAnnotation(s sql.Stmt) (sql.Annotation, bool) {
	anns := s.Header().Annotations
	i := slices.IndexFunc(anns, func(a sql.Annotation) bool { return a.Key == sql.AnnotationName })
	if i < 0 {
		return sql.Annotation{}, false
	}
	return anns[i], true
}

// checkNames checks the name a statement defines and its annotations. A broken
// statement's name counts as defined, but nothing in it is checked.
func checkNames(d *decl) {
	if d.broken() {
		return
	}
	switch s := d.node.(type) {
	case *sql.CreateStream:
		checkName(d.st, s.Name)
	case *sql.CreateView:
		checkName(d.st, s.Name)
	case *sql.Query:
	}
	checkAnnotations(d)
	if e := d.earlier; e != nil {
		d.st.errorf(d.pos, "DF1303", "streams and views share one set of names, and queries have their own; rename one",
			"%s is already defined at %s", d.name, posLine(e.pos))
	}
}

// checkName reports a name that a statement defines and can't use: qualified (DF5102),
// reserved (DF1302) or invalid (DF1301). reserved lists the names reserved in its place
// besides the expression keywords. It reports whether the name is valid.
func checkName(st *stmt, id sql.Ident, reserved ...string) bool {
	switch {
	case qualified(st, id):
	case sql.IsReserved(id.Name) || slices.Contains(reserved, id.Name):
		st.errorf(id.Pos, "DF1302", "choose another name", "%s is reserved", id.Name)
	case !catalog.IsName(id.Name):
		st.errorf(id.Pos, "DF1301", "names use lowercase letters, digits and _, and start with a letter",
			"invalid name %s", id.Name)
	default:
		return true
	}
	return false
}

// isGoName reports whether s matches [A-Z][A-Za-z0-9]*.
func isGoName(s string) bool {
	if s == "" || s[0] < 'A' || s[0] > 'Z' {
		return false
	}
	for i := range len(s) {
		if c := s[i]; !isLowerASCII(c) && !isUpperASCII(c) && !isDigitASCII(c) {
			return false
		}
	}
	return true
}

func isLowerASCII(c byte) bool { return 'a' <= c && c <= 'z' }
func isUpperASCII(c byte) bool { return 'A' <= c && c <= 'Z' }
func isDigitASCII(c byte) bool { return '0' <= c && c <= '9' }

// checkAnnotations checks what the parser leaves to the compiler: the statement kind
// (DF1204), repeats (DF1205), Go names (DF1206) and a SELECT without -- name: (DF1104).
// A bad annotation counts as present, and nothing else is checked on it.
func checkAnnotations(d *decl) {
	seen := make(map[sql.AnnotationKey]bool)
	for _, a := range d.node.Header().Annotations {
		key := annotationKey(a.Key)
		if seen[a.Key] {
			if !a.Bad {
				d.st.errorf(a.Pos, "DF1205", "", "duplicate -- %s annotation", key)
			}
			continue
		}
		seen[a.Key] = true
		switch {
		case a.Bad:
		case !annotationApplies(a.Key, d.kind):
			d.st.errorf(a.Pos, "DF1204", "name and expose apply to SELECT; go:type applies to CREATE STREAM",
				"-- %s doesn't apply to %s", key, d.kind)
		case a.Key != sql.AnnotationExpose && !isGoName(a.Name):
			d.st.errorf(a.NamePos, "DF1206", "use CamelCase that starts with an uppercase letter, such as PageStats",
				"%s is not a valid Go name", a.Name)
		}
	}
	if d.kind == declQuery && !seen[sql.AnnotationName] {
		d.st.errorf(d.node.Extent().Pos, "DF1104", "add -- name: <Name> :many above the SELECT",
			"SELECT has no -- name: annotation")
	}
}

// annotationKey returns the key as it is written, such as name: or go:type.
func annotationKey(k sql.AnnotationKey) string {
	if k == sql.AnnotationGoType {
		return k.String()
	}
	return k.String() + ":"
}

func annotationApplies(k sql.AnnotationKey, kind declKind) bool {
	if k == sql.AnnotationGoType {
		return kind == declStream
	}
	return kind == declQuery
}

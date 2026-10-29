// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"
	"strconv"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/goname"
	"github.com/lucindo/dynaflux/internal/sql"
)

// goName is a name that code generation declares (sql.md §3.2).
type goName struct {
	name  string
	thing string // what it names, such as "the event type of stream page_views"
	d     *decl  // the statement that implies it; nil for the names every package has
	pos   diag.Pos
}

// checkGoNames reports generated Go names that collide (DF1207): package-level names
// across the store, and field names within each struct. The diagnostic points at the
// later definition.
func (p *project) checkGoNames() {
	var pkg []goName
	for _, name := range goname.Reserved() {
		pkg = append(pkg, goName{name: name, thing: name + ", which every generated package declares"})
	}
	for _, d := range p.decls {
		if d.broken() || !d.defines || d.earlier != nil {
			continue
		}
		switch s := d.node.(type) {
		case *sql.CreateStream:
			pkg = append(pkg, streamGoNames(d, s)...)
			reportCollisions(eventFields(d, s))
		case *sql.Query:
			if isGoName(d.name) {
				n := goname.ForQuery(d.name)
				pkg = append(pkg, d.goName(n.Method, "query "+d.name),
					d.goName(n.Params, "the parameter type of query "+d.name),
					d.goName(n.Row, "the row type of query "+d.name))
			}
			if q := d.query; q != nil {
				reportCollisions(fields(d, "query", "parameter", q.params,
					func(p *sql.Param) (string, diag.Pos) { return p.Name.Name, p.Pos }))
				reportCollisions(fields(d, "query", "column", q.outs,
					func(o *output) (string, diag.Pos) { return o.name, o.pos }))
			}
		case *sql.CreateView:
		}
	}
	reportCollisions(pkg)
}

// goName returns a name that d's statement implies, which DF1207 reports at d's name.
func (d *decl) goName(name, thing string) goName {
	return goName{name: name, thing: thing, d: d, pos: d.pos}
}

func streamGoNames(d *decl, s *sql.CreateStream) []goName {
	n := goname.ForStream(d.name, goType(s))
	of := " of stream " + d.name
	return []goName{
		d.goName(n.Event, "the event type"+of),
		d.goName(n.Append, "the append function"+of),
		d.goName(n.AppendBatch, "the batch append function"+of),
		d.goName(n.Read, "the read function"+of),
		d.goName(n.Key, "the key type"+of),
	}
}

// eventFields lists the fields of stream d's event type (sql.md §8.3). The implicit
// columns' come first, since a collision is reported at the later name; a declared column
// named like an implicit one is DF1302 and gets no field.
func eventFields(d *decl, s *sql.CreateStream) []goName {
	var out []goName
	implicit := map[string]bool{}
	for _, c := range catalog.ImplicitColumns() {
		implicit[c.Name] = true
		out = append(out, goName{
			name: goname.Field(c.Name), thing: "the field of implicit column " + c.Name + " of stream " + d.name,
		})
	}
	declared := slices.DeleteFunc(slices.Clone(s.Columns), func(c *sql.Column) bool { return implicit[c.Name.Name] })
	return append(out, fields(d, "stream", "column", declared,
		func(c *sql.Column) (string, diag.Pos) { return c.Name.Name, c.Name.Pos })...)
}

// fields lists the fields of a struct that d's statement generates: one per valid name of
// items, from its first use. kind is the statement's kind, and what the items'.
func fields[T any](d *decl, kind, what string, items []T, name func(T) (string, diag.Pos)) []goName {
	var out []goName
	seen := map[string]bool{}
	for _, item := range items {
		n, pos := name(item)
		if catalog.IsName(n) && !seen[n] {
			seen[n] = true
			out = append(out, goName{
				name: goname.Field(n), thing: "the field of " + what + " " + n + " of " + kind + " " + d.name, d: d, pos: pos,
			})
		}
	}
	return out
}

func reportCollisions(names []goName) {
	first := map[string]goName{}
	for _, n := range names {
		earlier, ok := first[n.name]
		if !ok {
			first[n.name] = n
			continue
		}
		other := earlier.thing
		if earlier.d != nil {
			other += " at " + posLine(earlier.pos)
		}
		n.d.st.errorf(n.pos, "DF1207", "rename one of them, or set -- go:type on the stream",
			"generated Go name %s for %s collides with %s", n.name, n.thing, other)
	}
}

func posLine(p diag.Pos) string {
	return p.File + ":" + strconv.Itoa(p.Line)
}

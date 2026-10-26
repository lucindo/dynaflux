// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
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
	pkg := []goName{{name: "New", thing: "the constructor New"}, {name: "Client", thing: "the Client type"}}
	for _, d := range p.decls {
		if d.broken() || !d.defines || d.earlier != nil {
			continue
		}
		switch s := d.node.(type) {
		case *sql.CreateStream:
			pkg = append(pkg, streamGoNames(d, s)...)
			reportCollisions(fields(d, "stream", "column", s.Columns,
				func(c *sql.Column) (string, diag.Pos) { return c.Name.Name, c.Name.Pos }))
		case *sql.Query:
			if isGoName(d.name) {
				pkg = append(pkg, d.goName(d.name, "query "+d.name),
					d.goName(d.name+"Params", "the parameter type of query "+d.name),
					d.goName(d.name+"Row", "the row type of query "+d.name))
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
	event := goType(s)
	if event == "" {
		event = camelCase(singular(d.name))
	}
	name := camelCase(d.name)
	of := " of stream " + d.name
	return []goName{
		d.goName(event, "the event type"+of),
		d.goName("Append"+event, "the append function"+of),
		d.goName("Append"+event+"Batch", "the batch append function"+of),
		d.goName("Read"+name, "the read function"+of),
		d.goName("Import"+name, "the import function"+of),
		d.goName(name+"Key", "the key type"+of),
	}
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
				name: camelCase(n), thing: "the field of " + what + " " + n + " of " + kind + " " + d.name, d: d, pos: pos,
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

// camelCase turns a snake_case name into a Go name with Go's initialisms, as sqlc does:
// user_id becomes UserID.
func camelCase(name string) string {
	var b strings.Builder
	for part := range strings.SplitSeq(name, "_") {
		if part == "" {
			continue
		}
		if upper := strings.ToUpper(part); isInitialism(upper) {
			b.WriteString(upper)
			continue
		}
		b.WriteString(strings.ToUpper(part[:1]) + part[1:])
	}
	return b.String()
}

// isInitialism reports whether s is one of the initialisms Go names keep in capitals:
// golint's list, plus MS for milliseconds.
func isInitialism(s string) bool {
	switch s {
	case "ACL", "API", "ASCII", "CPU", "CSS", "DNS", "EOF", "GUID", "HTML", "HTTP", "HTTPS", "ID", "IP", "JSON",
		"LHS", "MS", "QPS", "RAM", "RHS", "RPC", "SLA", "SMTP", "SQL", "SSH", "TCP", "TLS", "TTL", "UDP", "UI",
		"UID", "UUID", "URI", "URL", "UTF8", "VM", "XML", "XMPP", "XSRF", "XSS":
		return true
	}
	return false
}

// singular returns a snake_case name with its last word in the singular: page_views
// becomes page_view, and categories becomes category.
func singular(name string) string {
	i := strings.LastIndexByte(name, '_') + 1
	head, word := name[:i], name[i:]
	switch {
	case strings.HasSuffix(word, "ies") && len(word) > len("ies"):
		word = strings.TrimSuffix(word, "ies") + "y"
	case strings.HasSuffix(word, "sses"), strings.HasSuffix(word, "shes"), strings.HasSuffix(word, "ches"),
		strings.HasSuffix(word, "xes"), strings.HasSuffix(word, "zes"):
		word = strings.TrimSuffix(word, "es")
	case strings.HasSuffix(word, "ss"), strings.HasSuffix(word, "us"), strings.HasSuffix(word, "is"):
	case strings.HasSuffix(word, "s"):
		word = strings.TrimSuffix(word, "s")
	}
	return head + word
}

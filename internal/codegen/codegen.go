// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package codegen writes the typed Go client that dynaflux generate puts in go.out. Its
// methods call the helpers of package fluxgen. The output depends only on the catalog and
// the options, and is formatted with go/format.
package codegen

import (
	"bytes"
	"cmp"
	"embed"
	"errors"
	"fmt"
	"go/format"
	"slices"
	"strconv"
	"strings"
	"text/template"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/goname"
	"github.com/lucindo/dynaflux/internal/gotype"
)

//go:embed templates/*.tmpl
var templateFS embed.FS

// Options are the inputs to code generation besides the catalog.
type Options struct {
	// Package is go.package, such as "flux".
	Package string
	// Version is the dynaflux version in each file's header, such as "v0.4.0".
	Version string
}

// Package returns the files of the client package for cat, by name: catalog.go,
// client.go, queries.go and streams.go.
func Package(cat *catalog.Catalog, o Options) (map[string][]byte, error) {
	if cat == nil || o.Package == "" || o.Version == "" || strings.ContainsAny(o.Version, "\r\n") {
		return nil, errors.New("codegen: need a catalog, a package name and a one-line version")
	}
	m, err := newModel(cat, o)
	if err != nil {
		return nil, err
	}
	tmpl, err := template.ParseFS(templateFS, "templates/*.tmpl")
	if err != nil {
		return nil, fmt.Errorf("codegen: %w", err)
	}
	files := map[string][]byte{}
	for _, name := range []string{"catalog.go", "client.go", "queries.go", "streams.go"} {
		var buf bytes.Buffer
		if err = tmpl.ExecuteTemplate(&buf, name+".tmpl", m); err != nil {
			return nil, fmt.Errorf("codegen: %s: %w", name, err)
		}
		if files[name], err = format.Source(buf.Bytes()); err != nil {
			return nil, fmt.Errorf("codegen: %s isn't valid Go: %w", name, err)
		}
	}
	return files, nil
}

type model struct {
	Package    string
	Version    string
	Store      string
	CatalogLit string // the catalog JSON as a Go string literal
	Streams    []streamModel
	Queries    []queryModel
	// QueriesTime is set when queries.go uses time.Time.
	QueriesTime bool
}

// field is a struct field.
type field struct {
	Name    string
	Type    string
	Comment string
}

type streamModel struct {
	goname.Stream

	SQLName   string
	Columns   []field
	KeyFields []field
	Dedup     bool
	// Retention is the LOG RETENTION period, or "" for FOREVER.
	Retention string
	// Fields are the event's field names in catalog order: declared, then implicit.
	Fields []string
}

type queryModel struct {
	goname.Query

	One bool
	// NotFound is set for a :one query that can find no row: one that doesn't aggregate
	// without GROUP BY (sql.md §10.7), or has a LIMIT parameter, which can be 0.
	NotFound    bool
	Source      string // such as "view page_daily"
	ParamFields []field
	RowFields   []field
}

func newModel(cat *catalog.Catalog, o Options) (*model, error) {
	data, err := catalog.Marshal(cat)
	if err != nil {
		return nil, fmt.Errorf("codegen: %w", err)
	}
	// Generate from the catalog the runtime decodes, in its canonical order.
	if cat, err = catalog.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("codegen: %w", err)
	}
	m := &model{Package: o.Package, Version: o.Version, Store: cat.Store, CatalogLit: goString(string(data))}
	for i := range cat.Streams {
		sm, serr := newStream(&cat.Streams[i])
		if serr != nil {
			return nil, serr
		}
		m.Streams = append(m.Streams, sm)
	}
	for i := range cat.Queries {
		qm := newQuery(&cat.Queries[i])
		if slices.ContainsFunc(slices.Concat(qm.ParamFields, qm.RowFields), func(f field) bool {
			return strings.Contains(f.Type, "time.Time")
		}) {
			m.QueriesTime = true
		}
		m.Queries = append(m.Queries, qm)
	}
	return m, nil
}

func newStream(s *catalog.Stream) (streamModel, error) {
	sm := streamModel{Stream: goname.ForStream(s.Name, s.GoType), SQLName: s.Name, Dedup: len(s.DedupBy) > 0}
	if p, ok := s.LogRetention.Period(); ok {
		text, err := p.MarshalText()
		if err != nil {
			return streamModel{}, fmt.Errorf("codegen: stream %s: %w", s.Name, err)
		}
		sm.Retention = string(text)
	}
	for _, c := range s.Columns {
		comment := c.Name + " " + c.Type.String()
		if c.Default != nil {
			comment += " DEFAULT " + sqlLiteral(c.Default) +
				": only for events stored before the column existed; Append stores " + cmp.Or(utcDate(c.Type), "the value given")
		} else {
			comment = withUTCDate(comment, c.Type)
		}
		sm.Columns = append(sm.Columns, field{goname.Field(c.Name), gotype.Of(c.Type).String(), comment})
	}
	for _, c := range slices.Concat(s.Columns, catalog.ImplicitColumns()) {
		sm.Fields = append(sm.Fields, goname.Field(c.Name))
	}
	for _, k := range s.PrimaryKey {
		c, _ := s.Column(k) // The compiler checked that the key columns exist.
		sm.KeyFields = append(sm.KeyFields, field{goname.Field(c.Name), gotype.Of(c.Type).String(), withUTCDate(c.Name+" "+c.Type.String(), c.Type)})
	}
	return sm, nil
}

func newQuery(q *catalog.Query) queryModel {
	one := q.Cardinality == catalog.CardinalityOne
	limitParam := q.Limit != nil && q.Limit.Op == catalog.OpParam
	qm := queryModel{
		Query:    goname.ForQuery(q.Name),
		One:      one,
		NotFound: one && (q.Aggregation == nil || len(q.Aggregation.GroupBy) > 0 || limitParam),
		Source:   string(q.Source.Kind) + " " + q.Source.Name,
	}
	for _, p := range q.Params {
		comment := withUTCDate("{"+p.Name+":"+p.Type.String()+"}", p.Type)
		if limitParam && q.Limit.Name == p.Name {
			comment += " LIMIT: 0 returns no rows"
		}
		qm.ParamFields = append(qm.ParamFields, field{goname.Field(p.Name), gotype.Of(p.Type).String(), comment})
	}
	for _, c := range q.Columns {
		qm.RowFields = append(qm.RowFields, field{goname.Field(c.Name), gotype.Of(c.Expr.Type).String(), c.Name + " " + c.Expr.Type.String()})
	}
	return qm
}

// utcDate says, for an input of type t, that a time given stands for its UTC date, or
// returns "" when t isn't a Date type.
func utcDate(t catalog.Type) string {
	if t.Kind != catalog.KindDate {
		return ""
	}
	return "the UTC date of the time given"
}

func withUTCDate(comment string, t catalog.Type) string {
	if note := utcDate(t); note != "" {
		return comment + ": " + note
	}
	return comment
}

// sqlLiteral writes a column DEFAULT as SQL: an OpLiteral, or an OpArray of them.
func sqlLiteral(e *catalog.Expr) string {
	if e.Op == catalog.OpArray {
		elems := make([]string, len(e.Args))
		for i := range e.Args {
			elems[i] = sqlLiteral(&e.Args[i])
		}
		return "[" + strings.Join(elems, ", ") + "]"
	}
	switch e.Type.Kind {
	case catalog.KindString, catalog.KindUUID, catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		return sqlString(e.Value)
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64, catalog.KindInt8,
		catalog.KindInt16, catalog.KindInt32, catalog.KindInt64, catalog.KindFloat64, catalog.KindDecimal, catalog.KindBool:
		return e.Value
	}
	return e.Value
}

// sqlString quotes s as a SQL string literal (sql.md §3.5). A character that isn't
// printable, which a Go comment can't hold safely, is written as \x escapes of its bytes.
func sqlString(s string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for _, r := range s {
		switch {
		case r == '\'' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == 0:
			b.WriteString(`\0`)
		case strconv.IsPrint(r):
			b.WriteRune(r)
		default:
			for _, c := range []byte(string(r)) {
				fmt.Fprintf(&b, `\x%02x`, c)
			}
		}
	}
	b.WriteByte('\'')
	return b.String()
}

// goString writes s as a raw string literal when it can, so the JSON stays readable. A
// backquote, or a character that isn't printable, such as a byte-order mark, which the
// compiler rejects inside a file, forces a quoted literal.
func goString(s string) string {
	if strconv.CanBackquote(s) && !strings.ContainsFunc(s, func(r rune) bool { return !strconv.IsPrint(r) }) {
		return "`" + s + "`"
	}
	return strconv.Quote(s)
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql_test

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
)

// dump renders a parsed file for golden tests: one line per statement,
// annotation, column, clause and item, ending with its position, and
// expressions as S-expressions.
func dump(f *sql.File) string {
	d := &dumper{}
	for _, s := range f.Stmts {
		d.stmt(s)
	}
	return d.b.String()
}

type dumper struct {
	b      strings.Builder
	indent int
}

func (d *dumper) linef(pos diag.Pos, format string, args ...any) {
	d.b.WriteString(strings.Repeat("  ", d.indent))
	fmt.Fprintf(&d.b, format, args...)
	fmt.Fprintf(&d.b, " @%d:%d\n", pos.Line, pos.Col)
}

func (d *dumper) nested(f func()) {
	d.indent++
	f()
	d.indent--
}

func (d *dumper) stmt(s sql.Stmt) {
	switch s := s.(type) {
	case *sql.CreateStream:
		d.linef(s.Pos, "stream %s%s", ident(s.Name), broken(s.Broken))
		d.nested(func() { d.stream(s) })
	case *sql.CreateView:
		d.linef(s.Pos, "view %s%s", ident(s.Name), broken(s.Broken))
		d.nested(func() { d.view(s) })
	case *sql.Query:
		d.linef(s.Pos, "query%s", broken(s.Broken))
		d.nested(func() {
			d.annotations(s.Annotations)
			if s.Select != nil {
				d.selectStmt(s.Select)
			}
		})
	}
}

func broken(b bool) string {
	if b {
		return " broken"
	}
	return ""
}

func (d *dumper) annotations(anns []sql.Annotation) {
	for _, a := range anns {
		switch {
		case a.Bad && a.Name != "":
			d.linef(a.Pos, "bad -- %s %s (name @%d:%d)", a.Key, a.Name, a.NamePos.Line, a.NamePos.Col)
		case a.Bad:
			d.linef(a.Pos, "bad -- %s", a.Key)
		case a.Key == sql.AnnotationName:
			shape := ":one"
			if a.Many {
				shape = ":many"
			}
			d.linef(a.Pos, "-- name: %s %s (name @%d:%d)", a.Name, shape, a.NamePos.Line, a.NamePos.Col)
		case a.Key == sql.AnnotationExpose:
			d.linef(a.Pos, "-- expose: http")
		case a.Key == sql.AnnotationGoType:
			d.linef(a.Pos, "-- go:type %s (name @%d:%d)", a.Name, a.NamePos.Line, a.NamePos.Col)
		}
	}
}

func (d *dumper) stream(s *sql.CreateStream) {
	d.annotations(s.Annotations)
	for _, c := range s.Columns {
		d.linef(c.Pos, "column %s %s", ident(c.Name), typ(c.Type))
		d.nested(func() { d.columnOptions(c) })
	}
	d.names("primary key", s.PrimaryKey)
	d.names("dedup by", s.DedupBy)
	if r := s.LogRetention; r != nil {
		if r.Forever {
			d.linef(r.Pos, "log retention forever")
		} else {
			d.linef(r.Pos, "log retention %s%s %s", when(r.Neg, "-"), expr(r.Value), ident(r.Unit))
		}
	}
	d.exprClause("accept", s.Accept)
	d.intClause("shards", s.Shards)
	if c := s.Consistency; c != nil {
		d.linef(c.Pos, "consistency %s", expr(c.Value))
	}
}

func (d *dumper) columnOptions(c *sql.Column) {
	if c.Default != nil {
		d.linef(c.Default.Pos, "default %s", expr(c.Default.X))
	}
	if c.PII != nil {
		category := ""
		if c.PII.Category != nil {
			category = " " + expr(c.PII.Category)
		}
		d.linef(c.PII.Pos, "pii%s", category)
	}
	for _, o := range c.Options {
		d.linef(o.Pos, "option %s", o.Name)
	}
}

func (d *dumper) view(v *sql.CreateView) {
	d.annotations(v.Annotations)
	if v.Mode != nil {
		mode := "async"
		if v.Mode.Sync {
			mode = "sync"
		}
		d.linef(v.Mode.Pos, "mode %s", mode)
	}
	d.names("order by", v.OrderBy)
	d.names("primary key", v.PrimaryKey)
	d.exprClause("ttl", v.TTL)
	d.intClause("split", v.Split)
	if v.Select != nil {
		d.selectStmt(v.Select)
	}
}

func (d *dumper) names(clause string, l *sql.NameList) {
	if l == nil {
		return
	}
	names := make([]string, len(l.Names))
	for i, n := range l.Names {
		names[i] = ident(n)
	}
	d.linef(l.Pos, "%s %s", clause, strings.Join(names, ", "))
}

func (d *dumper) exprClause(clause string, c *sql.ExprClause) {
	if c != nil {
		d.linef(c.Pos, "%s %s", clause, expr(c.X))
	}
}

func (d *dumper) intClause(clause string, c *sql.IntClause) {
	if c != nil {
		d.linef(c.Pos, "%s %s", clause, expr(c.Value))
	}
}

func (d *dumper) selectStmt(s *sql.Select) {
	d.linef(s.Pos, "select")
	d.indent++
	defer func() { d.indent-- }()
	for _, it := range s.Items {
		if it.Star {
			d.linef(it.Pos, "item *")
		} else {
			d.linef(it.Pos, "item %s%s", expr(it.X), alias(it.Alias))
		}
	}
	d.linef(s.From.Pos, "from %s", ident(s.From))
	for _, j := range s.ArrayJoins {
		d.linef(j.Pos, "%sarray join", when(j.Left, "left "))
		d.nested(func() {
			for _, it := range j.Items {
				d.linef(it.Pos, "item %s%s", expr(it.X), alias(it.Alias))
			}
		})
	}
	d.exprClause("where", s.Where)
	if g := s.GroupBy; g != nil {
		d.linef(g.Pos, "group by %s", exprs(g.Items))
	}
	if o := s.OrderBy; o != nil {
		d.linef(o.Pos, "order by")
		d.nested(func() {
			for _, it := range o.Items {
				d.linef(it.Pos, "item %s%s%s", expr(it.X), when(it.Desc, " desc"), when(it.NullsFirst, " nulls first"))
			}
		})
	}
	d.exprClause("limit", s.Limit)
}

func when(b bool, s string) string {
	if b {
		return s
	}
	return ""
}

func alias(a *sql.Ident) string {
	if a == nil {
		return ""
	}
	return " as " + ident(*a)
}

func ident(id sql.Ident) string {
	var b strings.Builder
	for _, q := range id.Qualifier {
		b.WriteString(ident(q))
		b.WriteByte('.')
	}
	if id.Quote != 0 {
		b.WriteByte(id.Quote)
		b.WriteString(id.Name)
		b.WriteByte(id.Quote)
	} else {
		b.WriteString(id.Name)
	}
	return b.String()
}

func typ(t *sql.Type) string {
	if !t.Parens {
		return ident(t.Name)
	}
	args := make([]string, len(t.Args))
	for i, a := range t.Args {
		switch a := a.(type) {
		case *sql.Type:
			args[i] = typ(a)
		case *sql.NumberLit:
			args[i] = expr(a)
		case *sql.StringLit:
			args[i] = expr(a)
		case *sql.EnumValue:
			args[i] = expr(a.Name) + " = " + expr(a.Value)
		}
	}
	return ident(t.Name) + "(" + strings.Join(args, ", ") + ")"
}

// expr renders x as an S-expression. Calls keep call syntax, literals and
// names their SQL form.
func expr(x sql.Expr) string {
	switch x := x.(type) {
	case nil:
		return "<nil>"
	case *sql.NumberLit:
		return when(x.Neg, "-") + x.Text
	case *sql.StringLit:
		return quoteString(x.Value)
	case *sql.BoolLit:
		return strconv.FormatBool(x.Value)
	case *sql.NullLit:
		return "NULL"
	case *sql.Param:
		return "{" + ident(x.Name) + ":" + typ(x.Type) + "}"
	case *sql.ColumnRef:
		return ident(x.Ident)
	case *sql.Call:
		return call(x)
	case *sql.Interval:
		s := "(interval " + when(x.Neg, "-") + expr(x.Value)
		if x.Unit.Name != "" {
			s += " " + ident(x.Unit)
		}
		if x.To != nil {
			s += " TO " + ident(*x.To)
		}
		return s + ")"
	case *sql.ArrayLit:
		return "[" + exprs(x.Elems) + "]"
	case *sql.ParenExpr:
		return "(paren " + expr(x.X) + ")"
	case *sql.UnaryExpr:
		op := "not"
		if x.Op == sql.OpNeg {
			op = "neg"
		}
		return "(" + op + " " + expr(x.X) + ")"
	case *sql.BinaryExpr:
		return "(" + strings.ToLower(x.Op.String()) + " " + expr(x.X) + " " + expr(x.Y) + ")"
	case *sql.InExpr:
		var rhs string
		if x.Param != nil {
			rhs = expr(x.Param)
		} else {
			rhs = "(list " + joinExprs(x.List, " ") + ")"
		}
		return "(" + when(x.Not, "not-") + "in " + expr(x.X) + " " + rhs + ")"
	case *sql.BetweenExpr:
		return "(" + when(x.Not, "not-") + "between " + expr(x.X) + " " + expr(x.Lo) + " " + expr(x.Hi) + ")"
	case *sql.IsNullExpr:
		return "(is-" + when(x.Not, "not-") + "null " + expr(x.X) + ")"
	}
	return fmt.Sprintf("<%T>", x)
}

func call(c *sql.Call) string {
	first := exprs(c.Args)
	if c.Parametric {
		first = exprs(c.Params)
	}
	switch {
	case c.Star:
		first = "*"
	case c.Distinct:
		first = "DISTINCT " + first
	}
	s := ident(c.Name) + "(" + first + ")"
	if c.Parametric {
		s += "(" + exprs(c.Args) + ")"
	}
	return s
}

func exprs(xs []sql.Expr) string { return joinExprs(xs, ", ") }

func joinExprs(xs []sql.Expr, sep string) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = expr(x)
	}
	return strings.Join(parts, sep)
}

// quoteString renders a string value as a SQL literal with backslash escapes.
func quoteString(s string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for _, r := range s {
		switch r {
		case '\'', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case 0:
			b.WriteString(`\0`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

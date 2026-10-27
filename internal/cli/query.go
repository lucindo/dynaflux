// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/valuetext"
)

// valueForms describes the text form of values, for the help of commands that take them.
const valueForms = `Values are written as in -json output, without quotes: integers and decimals in
plain notation, true or false, UUIDs as 8-4-4-4-12 hex digits, a Date as
2026-10-26, a time in RFC 3339 such as 2026-10-26T13:45:00.250Z, and an array as a
ClickHouse literal such as ['/a','/b'].
`

func queryCommand() command {
	return command{
		name:    "query",
		args:    "[flags] <Name>",
		summary: "run a named query on the deployed store",
		doc: `Query runs the named query with the deployed catalog of one region, so it reads what
the region serves, and prints the rows as a table, or as JSON Lines with -json.
-param gives each parameter as name=value. ` + valueForms + `
A query over a view needs the view's version to be live in the region;
-allow-partial reads it anyway, and the rows may then miss events.

Exit status: 0 when the query ran, also without rows; 1 when a :one query found no
row, the version isn't live, or the query failed to evaluate; and 2 for usage and
I/O problems, AWS access, and a query the deployed catalog doesn't define.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			o := queryOptions{aws: addAWSFlags(fs)}
			fs.Var(&o.params, "param", "a parameter, as `name=value`; repeat it for each one")
			fs.BoolVar(&o.allowPartial, "allow-partial", false, "run the query even when the view's version isn't live")
			fs.BoolVar(&o.json, "json", false, "print the rows as JSON Lines, one object per row")
			return func(c *cli, args []string) int { return c.query(o, args) }
		},
	}
}

type queryOptions struct {
	aws          *awsFlags
	params       pairs
	allowPartial bool
	json         bool
}

func (c *cli) query(o queryOptions, args []string) int {
	if len(args) != 1 {
		return c.usageError("query", "name one query")
	}
	d, status := c.openDeployment("query", o.aws)
	if status != exitOK {
		return status
	}
	def := d.compiled.Catalog()
	i := slices.IndexFunc(def.Queries, func(q catalog.Query) bool { return q.Name == args[0] })
	if i < 0 {
		names := make([]string, len(def.Queries))
		for j, q := range def.Queries {
			names[j] = q.Name
		}
		return c.usageError("query", notDeployed("query", args[0], d.region, names))
	}
	q := &def.Queries[i]
	params, err := bindParams(q, o.params)
	if err != nil {
		return c.usageError("query", err.Error())
	}
	rows, err := d.engine.Query(c.ctx, d.compiled, q.Name, params, engine.QueryOptions{AllowPartial: o.allowPartial})
	switch {
	case errors.Is(err, engine.ErrViewNotReady):
		c.errorf("dynaflux query: %s", strings.TrimPrefix(engineError(err), "query "+q.Name+": "))
		c.notReadyHint(d, o.aws, q)
		return exitErrors
	case errors.Is(err, engine.ErrNotFound):
		c.errorf("dynaflux query: %s found no row", q.Name)
		return exitErrors
	case errors.Is(err, engine.ErrEvaluation):
		c.errorf("dynaflux query: %s", engineError(err))
		return exitErrors
	case errors.Is(err, engine.ErrInvalidValue), errors.Is(err, engine.ErrTooManyKeys):
		return c.usageError("query", engineError(err))
	case err != nil:
		c.errorf("dynaflux query: %s: %s", d.region, engineError(err))
		return exitUsage
	}
	cols := make([]column, len(q.Columns))
	for j, col := range q.Columns {
		cols[j] = column{col.Name, col.Expr.Type}
	}
	if o.json {
		writeJSONLines(&c.out, cols, rows)
	} else {
		writeTable(&c.out, cols, rows)
	}
	return exitOK
}

// engineError is an engine error's text without its "dynaflux: ", which the command's
// name replaces.
func engineError(err error) string {
	return strings.TrimPrefix(err.Error(), "dynaflux: ")
}

// notDeployed says that the region's deployed catalog has no kind called name, and suggests
// one of names.
func notDeployed(kind, name, region string, names []string) string {
	msg := fmt.Sprintf("the deployed catalog of %s has no %s %s", region, kind, name)
	if s, ok := diag.Suggest(name, names); ok {
		msg += "; did you mean " + s + "?"
	}
	return msg
}

// notReadyHint says on stderr how to make the version that q reads live, by its state.
func (c *cli) notReadyHint(d *deployment, f *awsFlags, q *catalog.Query) {
	v, _ := d.compiled.Catalog().View(q.Source.Name)
	vv, _ := v.Current() // the engine found the version, so the view has a current one
	id := catalog.VersionID(v.Name, vv.Number)
	states, err := d.meta.ViewStates(c.ctx)
	if err != nil {
		c.errorf("note: %s: read the view states: %v", d.region, err)
		return
	}
	where := " -region " + d.region
	if f.prefix.set {
		where += " -prefix " + cmp.Or(f.prefix.value, `""`)
	}
	switch states[id].State {
	case "", meta.PendingBackfill:
		c.errorf("  hint: make it live with dynaflux backfill %s%s, or read it anyway with -allow-partial", id, where)
	case meta.Activating:
		c.errorf("  hint: a backfill is filling it; follow it with dynaflux backfill %s -watch%s, or read it anyway with -allow-partial", id, where)
	case meta.Broken:
		c.errorf("  hint: rebuild it with dynaflux generate -rebuild %s, then deploy and backfill the new version", v.Name)
	case meta.Live, meta.Retired:
	}
}

// bindParams parses the value of each of q's parameters from given.
func bindParams(q *catalog.Query, given pairs) ([]expr.Value, error) {
	types := map[string]catalog.Type{}
	for _, p := range q.Params {
		types[p.Name] = p.Type
	}
	vals, err := parsePairs("-param", "parameter", given, types)
	if err != nil {
		return nil, err
	}
	params := make([]expr.Value, len(q.Params))
	var missing []string
	for i, p := range q.Params {
		v, ok := vals[p.Name]
		if !ok {
			missing = append(missing, p.Name+" ("+p.Type.String()+")")
		}
		params[i] = v
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s needs -param for %s", q.Name, strings.Join(missing, ", "))
	}
	return params, nil
}

// parsePairs parses each value in given by the type of its name in types. An unknown name
// is an error that suggests a known one.
func parsePairs(flagName, what string, given pairs, types map[string]catalog.Type) (map[string]expr.Value, error) {
	vals := make(map[string]expr.Value, len(given))
	for _, p := range given {
		t, ok := types[p.name]
		if !ok {
			msg := fmt.Sprintf("%s %s=%s: no %s %s", flagName, p.name, p.value, what, p.name)
			if s, found := diag.Suggest(p.name, slices.Sorted(maps.Keys(types))); found {
				msg += "; did you mean " + s + "?"
			}
			return nil, errors.New(msg)
		}
		v, err := valuetext.ParseText(t, p.value)
		if err != nil {
			return nil, fmt.Errorf("%s %s: %w", flagName, p.name, err)
		}
		vals[p.name] = v
	}
	return vals, nil
}

// pairs is a repeatable name=value flag.
type pairs []pair

type pair struct{ name, value string }

func (p *pairs) String() string {
	s := make([]string, len(*p))
	for i, q := range *p {
		s[i] = q.name + "=" + q.value
	}
	return strings.Join(s, " ")
}

func (p *pairs) Set(s string) error {
	name, value, ok := strings.Cut(s, "=")
	if !ok || name == "" {
		return errors.New("want name=value")
	}
	if slices.ContainsFunc(*p, func(q pair) bool { return q.name == name }) {
		return fmt.Errorf("%s is already set", name)
	}
	*p = append(*p, pair{name, value})
	return nil
}

// column is an output column.
type column struct {
	name string
	typ  catalog.Type
}

// writeTable writes rows as a table under a header line, its columns aligned.
func writeTable(b *strings.Builder, cols []column, rows [][]expr.Value) {
	lines := make([][]string, 0, len(rows)+1)
	header := make([]string, len(cols))
	for i, col := range cols {
		header[i] = cell(col.name)
	}
	lines = append(lines, header)
	for _, row := range rows {
		line := make([]string, len(cols))
		for i, col := range cols {
			line[i] = cell(string(valuetext.AppendText(nil, col.typ, row[i])))
		}
		lines = append(lines, line)
	}
	alignColumns(b, lines)
}

// alignColumns writes lines with two spaces after the widest cell of each column.
func alignColumns(b *strings.Builder, lines [][]string) {
	const gap = 2
	var widths []int
	for _, line := range lines {
		for i, s := range line {
			if i == len(widths) {
				widths = append(widths, 0)
			}
			widths[i] = max(widths[i], utf8.RuneCountInString(s))
		}
	}
	for _, line := range lines {
		for i, s := range line {
			b.WriteString(s)
			if i < len(line)-1 {
				b.WriteString(strings.Repeat(" ", widths[i]-utf8.RuneCountInString(s)+gap))
			}
		}
		b.WriteByte('\n')
	}
}

// cell escapes control characters, such as a tab or a newline, which would break the table
// or drive the terminal.
func cell(s string) string {
	if !strings.ContainsFunc(s, unicode.IsControl) {
		return s
	}
	var b strings.Builder
	for _, r := range s {
		if unicode.IsControl(r) {
			q := fmt.Sprintf("%+q", r)
			b.WriteString(q[1 : len(q)-1])
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// writeJSONLines writes each row as a JSON object, its keys in column order.
func writeJSONLines(b *strings.Builder, cols []column, rows [][]expr.Value) {
	name := catalog.Type{Kind: catalog.KindString}
	var line []byte
	for _, row := range rows {
		line = append(line[:0], '{')
		for i, col := range cols {
			if i > 0 {
				line = append(line, ',')
			}
			line = valuetext.AppendJSON(line, name, expr.String(col.name))
			line = append(line, ':')
			line = valuetext.AppendJSON(line, col.typ, row[i])
		}
		b.Write(append(line, '}', '\n'))
	}
}

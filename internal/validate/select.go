// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"

	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
)

// output is a SELECT item and the name of the column it defines.
type output struct {
	item *sql.SelectItem
	// name is "" when the item needs an alias that it lacks (DF5106).
	name string
	pos  diag.Pos // where the name is written: the alias, or else the item
	// key is set on a view's SELECT item that GROUP BY names.
	key bool
}

// selectOutputs names the SELECT items (sql.md §5.3). It reports SELECT * (DF4002), an
// item that needs an alias (DF5106), a duplicate name (DF5104), an alias that is a column
// of source (DF5103), and an alias that a definition can't use (DF1301, DF1302).
// isColumn tells the columns of source; nil means they're unknown.
func selectOutputs(st *stmt, items []*sql.SelectItem, source string, isColumn func(string) bool, reserved ...string) []*output {
	var outs []*output
	seen := map[string]bool{}
	for _, item := range items {
		if item.Star {
			st.errorf(item.Pos, "DF4002", "list the columns", "SELECT * is not supported")
			continue
		}
		out := outputName(st, item, source, isColumn, reserved)
		if out.name != "" && seen[out.name] {
			st.errorf(out.pos, "DF5104", "", "duplicate output column %s", out.name)
		}
		seen[out.name] = true
		outs = append(outs, out)
	}
	return outs
}

func outputName(st *stmt, item *sql.SelectItem, source string, isColumn func(string) bool, reserved []string) *output {
	if a := item.Alias; a != nil {
		checkName(st, *a, reserved...)
		if isColumn != nil && isColumn(a.Name) {
			st.errorf(a.Pos, "DF5103", "choose another alias", "alias %s is also a column of %s", a.Name, source)
		}
		return &output{item: item, name: a.Name, pos: a.Pos}
	}
	if ref, ok := item.X.(*sql.ColumnRef); ok {
		return &output{item: item, name: ref.Name, pos: ref.Pos}
	}
	st.errorf(item.X.Extent().Pos, "DF5106", "add AS <name>", "%s needs a name", st.text(item.X))
	return &output{item: item, pos: item.Pos}
}

// groupByNames returns the names that GROUP BY lists. Each item must be a name (DF5107),
// listed once (DF5108). ok is false when an item isn't a name.
func groupByNames(st *stmt, g *sql.GroupBy) ([]*sql.ColumnRef, bool) {
	var refs []*sql.ColumnRef
	ok := true
	for _, item := range g.Items {
		ref, isRef := item.(*sql.ColumnRef)
		switch {
		case !isRef:
			st.errorf(item.Extent().Pos, "DF5107", "give the expression an alias in SELECT and group by the alias",
				"GROUP BY item must be a name")
			ok = false
		case qualified(st, ref.Ident):
			ok = false
		case slices.ContainsFunc(refs, func(r *sql.ColumnRef) bool { return r.Name == ref.Name }):
			st.errorf(ref.Pos, "DF5108", "", "%s appears twice in GROUP BY", ref.Name)
		default:
			refs = append(refs, ref)
		}
	}
	return refs, ok
}

// listNames returns the names of a clause such as ORDER BY (a, b). ok is false when one is
// qualified (DF5102).
func listNames(st *stmt, list *sql.NameList) ([]string, bool) {
	names := make([]string, 0, len(list.Names))
	ok := true
	for _, id := range list.Names {
		ok = !qualified(st, id) && ok
		names = append(names, id.Name)
	}
	return names, ok
}

// qualified reports a qualified name such as t.col (DF5102).
func qualified(st *stmt, id sql.Ident) bool {
	if len(id.Qualifier) == 0 {
		return false
	}
	st.errorf(id.Pos, "DF5102", "use the column name alone", "qualified name %s is not supported", st.text(id))
	return true
}

// unknownColumn reports a name that resolves to nothing (DF5101), suggesting one of the
// names visible there (sql.md §3.4).
func unknownColumn(st *stmt, ref *sql.ColumnRef, visible []string) operand {
	hint := didYouMean(ref.Name, visible)
	if ref.Quote == '"' {
		const quotes = "string literals use single quotes"
		if hint == "" {
			hint = quotes
		} else {
			hint += "; " + quotes
		}
	}
	st.errorf(ref.Pos, "DF5101", hint, "unknown column %s", ref.Name)
	return badOperand()
}

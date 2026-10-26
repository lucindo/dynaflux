// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"maps"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// scope resolves the names of a view's or a query's SELECT (sql.md §5.3): the columns of
// the rows it reads, and its output columns.
type scope struct {
	// columns are the source columns, with their types. It is nil when FROM names no
	// compiled source; every name is then bad, as FROM is reported already.
	columns map[string]catalog.Type
	// bad are the source columns with an invalid definition: what reads them is reported
	// already.
	bad    map[string]bool
	outs   []*output
	byName map[string]*output // the first output of each name
	// guards are the divisors that WHERE rules out zero for (DF5501).
	guards []catalog.Expr
	// param resolves a parameter; nil where parameters aren't allowed (DF4403).
	param func(c *checker, p *sql.Param, role paramRole) operand
	// dedup is the DEDUP stream that a view reads, for DF3504.
	dedup *catalog.Stream
}

// checker returns a checker for an expression in place, whose names resolve with column.
func (s *scope) checker(st *stmt, place string, column func(*checker, *sql.ColumnRef) operand) *checker {
	return &checker{st: st, guards: s.guards, env: env{place: place, column: column, param: s.param}}
}

func (s *scope) isColumn(name string) bool {
	_, ok := s.columns[name]
	return ok || s.bad[name]
}

// source resolves a source column. It returns false when ref names none.
func (s *scope) source(st *stmt, ref *sql.ColumnRef) (operand, bool) {
	if s.columns == nil || s.bad[ref.Name] {
		return badOperand(), true
	}
	t, ok := s.columns[ref.Name]
	if !ok {
		return operand{}, false
	}
	if s.dedup != nil && (ref.Name == catalog.ColumnIngestTime || ref.Name == catalog.ColumnOriginRegion) {
		st.warnf(ref.Pos, "DF3504", "group by event_time instead of ingest_time; avoid origin_region",
			"view reads %s from DEDUP stream %s; a duplicate accepted in two regions has a different %s in each",
			ref.Name, s.dedup.Name, ref.Name)
	}
	return operand{ir: catalog.Expr{Op: catalog.OpColumn, Type: t, Name: ref.Name}, node: ref}, true
}

// selectColumn resolves a name inside a SELECT item, where only source columns resolve
// (DF5105).
func (s *scope) selectColumn(c *checker, ref *sql.ColumnRef) operand {
	if o, ok := s.source(c.st, ref); ok {
		return o
	}
	if out := s.byName[ref.Name]; out != nil && out.item.Alias != nil {
		c.st.errorf(ref.Pos, "DF5105", "aliases work in WHERE, GROUP BY and ORDER BY; repeat the expression",
			"alias %s can't be used here", ref.Name)
		return badOperand()
	}
	return unknownColumn(c.st, ref, s.visible(false))
}

// visible returns the names that an unknown column's suggestion comes from: the source
// columns, and the SELECT aliases when they resolve.
func (s *scope) visible(aliases bool) []string {
	names := slices.Collect(maps.Keys(s.columns))
	if aliases {
		names = slices.AppendSeq(names, maps.Keys(s.byName))
	}
	return names
}

// streamColumnTypes returns the declared and implicit columns of a stream, by name.
func streamColumnTypes(s *catalog.Stream) map[string]catalog.Type {
	types := map[string]catalog.Type{}
	for _, c := range slices.Concat(s.Columns, catalog.ImplicitColumns()) {
		types[c.Name] = c.Type
	}
	return types
}

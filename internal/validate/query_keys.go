// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"maps"
	"math/big"
	"reflect"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// bindKind is how a WHERE term binds a key column (sql.md §10.4).
type bindKind uint8

const (
	bindEq    bindKind = iota + 1 // k = v
	bindSet                       // k IN (…) or k IN {p:Array(T)}
	bindRange                     // a lower bound, an upper bound, or both for BETWEEN
)

// keyTerm is a WHERE term that binds a key column.
type keyTerm struct {
	term   int // index in queryCompiler.terms
	column string
	kind   bindKind
	// values are the distinct values of an equality or a set.
	values       []catalog.Expr
	lower, upper *catalog.RangeBound
}

// columnTerms are the terms that bind one key column.
type columnTerms struct {
	sets, lowers, uppers []*keyTerm // sets holds the equalities too
}

// bindKeys binds the source's key from the WHERE terms (sql.md §10.4, §10.5) and keeps
// the other terms as the residual filter (§10.6).
func (q *queryCompiler) bindKeys() {
	if q.src == nil || !q.src.keysOK {
		return
	}
	byColumn := map[string]*columnTerms{}
	for i, t := range q.terms {
		if t.o.bad {
			continue
		}
		if kt, ok := q.classify(t); ok {
			kt.term = i
			ct := byColumn[kt.column]
			if ct == nil {
				ct = &columnTerms{}
				byColumn[kt.column] = ct
			}
			ct.add(&kt)
		}
	}
	q.boundTwice(byColumn)
	used := q.plan(byColumn)
	q.checkCombinations()
	q.q.Filter = q.residual(used)
}

func (ct *columnTerms) add(kt *keyTerm) {
	if kt.kind != bindRange {
		ct.sets = append(ct.sets, kt)
	}
	if kt.lower != nil {
		ct.lowers = append(ct.lowers, kt)
	}
	if kt.upper != nil {
		ct.uppers = append(ct.uppers, kt)
	}
}

// classify returns the key binding of a term, if it has one of the forms of sql.md §10.4
// with a key column on either side.
func (q *queryCompiler) classify(t whereTerm) (keyTerm, bool) {
	e := t.o.ir
	if b, ok := unparen(t.x).(*sql.BetweenExpr); ok && !b.Not {
		lo, hi := e.Args[0].Args, e.Args[1].Args
		if !q.isKeyColumn(lo[0]) || !isConstant(lo[1]) || !isConstant(hi[1]) {
			return keyTerm{}, false
		}
		return keyTerm{column: lo[0].Name, kind: bindRange,
			lower: &catalog.RangeBound{Value: lo[1], Inclusive: true},
			upper: &catalog.RangeBound{Value: hi[1], Inclusive: true}}, true
	}
	if e.Op == catalog.OpIn {
		// A stream's event_time is bound only by = or a range.
		if !q.isKeyColumn(e.Args[0]) || !allConstant(e.Args[1:]) || q.src.stream != nil && e.Args[0].Name == catalog.ColumnEventTime {
			return keyTerm{}, false
		}
		return keyTerm{column: e.Args[0].Name, kind: bindSet, values: distinct(e.Args[1:])}, true
	}
	if len(e.Args) != argsOfBinaryFun {
		return keyTerm{}, false
	}
	for i, side := range [][]catalog.Expr{e.Args, {e.Args[1], e.Args[0]}} {
		if q.isKeyColumn(side[0]) && isConstant(side[1]) {
			return comparisonTerm(e.Op, side[0].Name, side[1], i == 1)
		}
	}
	return keyTerm{}, false
}

// comparisonTerm returns the binding of column op v, or v op column when flipped.
func comparisonTerm(op catalog.Op, column string, v catalog.Expr, flipped bool) (keyTerm, bool) {
	if op == catalog.OpEquals {
		return keyTerm{column: column, kind: bindEq, values: []catalog.Expr{v}}, true
	}
	lower := op == catalog.OpGreater || op == catalog.OpGreaterOrEquals
	upper := op == catalog.OpLess || op == catalog.OpLessOrEquals
	if !lower && !upper {
		return keyTerm{}, false
	}
	bound := &catalog.RangeBound{Value: v, Inclusive: op == catalog.OpGreaterOrEquals || op == catalog.OpLessOrEquals}
	kt := keyTerm{column: column, kind: bindRange}
	if lower != flipped {
		kt.lower = bound
	} else {
		kt.upper = bound
	}
	return kt, true
}

// isKeyColumn reports whether e is a key column of the source, or a stream's event_time.
func (q *queryCompiler) isKeyColumn(e catalog.Expr) bool {
	if e.Op != catalog.OpColumn {
		return false
	}
	return slices.Contains(q.src.keys, e.Name) || q.src.stream != nil && e.Name == catalog.ColumnEventTime
}

// isConstant reports whether e has no column reference or aggregate.
func isConstant(e catalog.Expr) bool {
	return e.Op != catalog.OpColumn && e.Op != catalog.OpAggregate && allConstant(e.Args)
}

func allConstant(es []catalog.Expr) bool {
	for _, e := range es {
		if !isConstant(e) {
			return false
		}
	}
	return true
}

// distinct returns values without repeats: a value repeated in a list counts once.
func distinct(values []catalog.Expr) []catalog.Expr {
	var out []catalog.Expr
	for _, v := range values {
		if !slices.ContainsFunc(out, func(o catalog.Expr) bool { return reflect.DeepEqual(o, v) }) {
			out = append(out, v)
		}
	}
	return out
}

// boundTwice reports a key column bound by two equalities or sets, or by two lower or two
// upper bounds (DF4102), at each term after the first.
func (q *queryCompiler) boundTwice(byColumn map[string]*columnTerms) {
	reported := map[int]bool{}
	for _, column := range slices.Sorted(maps.Keys(byColumn)) {
		ct := byColumn[column]
		for _, kts := range [][]*keyTerm{ct.sets, ct.lowers, ct.uppers} {
			for _, kt := range kts[min(1, len(kts)):] {
				if reported[kt.term] {
					continue
				}
				reported[kt.term] = true
				q.st.errorf(q.terms[kt.term].x.Extent().Pos, "DF4102",
					"combine the conditions: one = or IN, or one lower and one upper bound", "%s is bound more than once", column)
			}
		}
	}
}

// plan builds the key read and returns the terms it uses. Every partition key column must
// be bound by = or IN (DF4101). A view's sort key columns are then taken in order while =
// or IN binds them; a range on the next one bounds the read. A stream's event_time may
// bound it, with = as a range of one instant.
func (q *queryCompiler) plan(byColumn map[string]*columnTerms) map[int]bool {
	used := map[int]bool{}
	keys := &q.q.Keys
	for _, column := range q.src.keys[:q.src.partition] {
		if b, ok := setBinding(byColumn[column], used); ok {
			keys.Partition = append(keys.Partition, b)
		} else {
			q.unboundPartition(column)
		}
	}
	q.allEq = q.src.view != nil && allEqual(byColumn, q.src.keys)
	if q.src.stream != nil {
		keys.Range = streamRange(byColumn[catalog.ColumnEventTime], used)
		return used
	}
	for _, column := range q.src.keys[q.src.partition:] {
		if b, ok := setBinding(byColumn[column], used); ok {
			keys.SortPrefix = append(keys.SortPrefix, b)
			continue
		}
		keys.Range = keyRange(column, byColumn[column], used)
		break
	}
	return used
}

func allEqual(byColumn map[string]*columnTerms, columns []string) bool {
	for _, c := range columns {
		if ct := byColumn[c]; ct == nil || len(ct.sets) == 0 || ct.sets[0].kind != bindEq {
			return false
		}
	}
	return true
}

// setBinding returns the binding of a column by its first = or IN term.
func setBinding(ct *columnTerms, used map[int]bool) (catalog.KeyBinding, bool) {
	if ct == nil || len(ct.sets) == 0 {
		return catalog.KeyBinding{}, false
	}
	kt := ct.sets[0]
	used[kt.term] = true
	return catalog.KeyBinding{Column: kt.column, Values: kt.values}, true
}

// keyRange returns the range that the first lower and upper bounds of a column set, or
// nil.
func keyRange(column string, ct *columnTerms, used map[int]bool) *catalog.KeyRange {
	if ct == nil || len(ct.lowers)+len(ct.uppers) == 0 {
		return nil
	}
	r := &catalog.KeyRange{Column: column}
	if len(ct.lowers) > 0 {
		r.Lower = ct.lowers[0].lower
		used[ct.lowers[0].term] = true
	}
	if len(ct.uppers) > 0 {
		r.Upper = ct.uppers[0].upper
		used[ct.uppers[0].term] = true
	}
	return r
}

// streamRange returns the range that event_time terms set: event_time = v reads the
// instant v.
func streamRange(ct *columnTerms, used map[int]bool) *catalog.KeyRange {
	if ct != nil && len(ct.sets) > 0 {
		kt := ct.sets[0]
		used[kt.term] = true
		bound := &catalog.RangeBound{Value: kt.values[0], Inclusive: true}
		return &catalog.KeyRange{Column: catalog.ColumnEventTime, Lower: bound, Upper: bound}
	}
	return keyRange(catalog.ColumnEventTime, ct, used)
}

// unboundPartition reports a partition key column that WHERE doesn't bind with = or IN
// (DF4101), unless a WHERE term with an error may bind it. The hint suggests a parameter
// of the column's type, DateTime64(6) for a DateTime.
func (q *queryCompiler) unboundPartition(column string) {
	if q.whereBad {
		return
	}
	pos := q.sel.From.Pos
	if q.sel.Where != nil {
		pos = q.sel.Where.Pos
	}
	t := q.src.columns[column]
	if t.Kind == catalog.KindDateTime {
		t = catalog.Type{Kind: catalog.KindDateTime64, Precision: catalog.DateTime64Micros}
	}
	q.st.errorf(pos, "DF4101",
		"add "+column+" = {"+column+":"+t.String()+"} to WHERE, or declare a view whose PRIMARY KEY matches this query",
		"%s %s is partitioned by (%s), but WHERE does not bind \"%s\" with = or IN. Every query must fix the partition key.",
		q.src.kind, q.src.name, strings.Join(q.src.keys[:q.src.partition], ", "), column)
}

// checkCombinations checks that the literal lists of the key binding read at most 100
// key combinations (DF4103). An Array parameter counts as one value here; the engine
// checks its length at call time.
func (q *queryCompiler) checkCombinations() {
	n := big.NewInt(1)
	for _, b := range slices.Concat(q.q.Keys.Partition, q.q.Keys.SortPrefix) {
		n.Mul(n, big.NewInt(int64(len(b.Values))))
	}
	if n.Cmp(big.NewInt(catalog.MaxKeyCombinations)) > 0 {
		q.st.errorf(q.sel.Where.Pos, "DF4103", "split the call, or declare a view with a coarser key",
			"the key binding reads %s key combinations; the limit is %d", n, catalog.MaxKeyCombinations)
	}
}

// residual returns the WHERE terms that the key read doesn't use, or nil.
func (q *queryCompiler) residual(used map[int]bool) *catalog.Expr {
	var terms []catalog.Expr
	nullable := false
	for i, t := range q.terms {
		if !used[i] && !t.o.bad {
			terms = append(terms, t.o.ir)
			nullable = nullable || t.o.typ().Nullable
		}
	}
	switch len(terms) {
	case 0:
		return nil
	case 1:
		return &terms[0]
	}
	return &catalog.Expr{Op: catalog.OpAnd, Type: boolType(nullable), Args: terms}
}

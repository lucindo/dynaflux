// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

// aggregate checks a read-time aggregate (sql.md §7.4, §10.7) and refers to it by its
// position among the query's aggregates. Its arguments read the source rows.
func (q *queryCompiler) aggregate(c *checker, x *sql.Call, fn aggregateFunc) operand {
	rows := q.checker(c.st, "SELECT", q.selectColumn)
	agg, ok := q.queryAggregate(rows, x, fn)
	if !ok {
		return badOperand()
	}
	q.aggs = append(q.aggs, agg)
	return operand{ir: catalog.Expr{Op: catalog.OpAggregate, Type: agg.Type, Index: len(q.aggs) - 1}, node: x}
}

func (q *queryCompiler) queryAggregate(c *checker, x *sql.Call, fn aggregateFunc) (catalog.QueryAggregate, bool) {
	switch {
	case fn.state:
		c.st.errorf(x.Pos, "DF5206", hintCombinator, "combinator -State is not supported")
	case fn.mergeIf:
		c.st.errorf(x.Pos, "DF4206", "filter the rows with WHERE", "%s is not supported", x.Name.Name)
	case fn.merge:
		return q.merge(c, x, fn)
	default:
		agg, ok := c.aggregateCall(x, fn)
		return catalog.QueryAggregate{Aggregate: agg}, ok && q.combinable(c.st, x, agg)
	}
	return catalog.QueryAggregate{}, false
}

// merge checks <fn>Merge(c), which merges the stored states of the view column c.
func (q *queryCompiler) merge(c *checker, x *sql.Call, fn aggregateFunc) (catalog.QueryAggregate, bool) {
	inner := c.insideAggregate()
	params, paramsOK := inner.aggregateParams(x, fn.fn)
	if !inner.arity(x, 1, 1, false) || !paramsOK {
		return catalog.QueryAggregate{}, false
	}
	arg := inner.value(x.Args[0])
	if arg.bad || q.src == nil || !q.mergeColumn(c.st, x, fn, arg, params) {
		return catalog.QueryAggregate{}, false
	}
	// The column holds the finalized values of the same aggregate, so it has the merge's type.
	return catalog.QueryAggregate{Merge: true, Aggregate: catalog.Aggregate{
		Fn: fn.fn, Params: params, Args: []catalog.Expr{arg.ir}, Type: arg.typ(),
	}}, true
}

// mergeColumn checks that the argument of <fn>Merge is a view column defined by <fn>, or
// <fn>If for <fn>IfMerge, with the same parameters except level (DF4205). A uniqExact
// column can't be merged at all (DF4201).
func (q *queryCompiler) mergeColumn(st *stmt, x *sql.Call, fn aggregateFunc, arg operand, params []float64) bool {
	var def *catalog.ViewAggregate
	if arg.ir.Op == catalog.OpColumn {
		def = q.src.aggregate(arg.ir.Name)
	}
	var what string
	switch {
	case q.src.view == nil:
		what = "a column of stream " + q.src.name
	case arg.ir.Op != catalog.OpColumn:
		what = "not a column"
	case def == nil:
		what = "a key of " + q.src.name
	case def.Fn == catalog.AggUniqExact && (fn.fn == catalog.AggUniq || fn.fn == catalog.AggUniqExact):
		q.notCombinable(st, x, def.Name)
		return false
	default:
		what = mergeMismatch(def, fn, params)
	}
	if what == "" {
		return true
	}
	column, need := st.text(x.Args[0]), strings.TrimSuffix(x.Name.Name, combMerge)
	hint := ""
	if def != nil && def.Fn == fn.fn && def.Cond != nil && !fn.withIf {
		hint = "for " + withArticle(need+combIf) + " column, use " + need + "IfMerge(" + column + ")"
	}
	st.errorf(x.Pos, "DF4205", hint, "%s(%s) needs %s column of a view; %s is %s",
		x.Name.Name, column, withArticle(need), column, what)
	return false
}

// mergeMismatch describes how the definition of a view column differs from what a merge
// needs, or returns "".
func mergeMismatch(def *catalog.ViewAggregate, fn aggregateFunc, params []float64) string {
	defName := string(def.Fn)
	if def.Cond != nil {
		defName += combIf
	}
	switch {
	case def.Fn != fn.fn || fn.withIf != (def.Cond != nil):
		return withArticle(defName) + " column"
	case def.Fn == catalog.AggQuantileDD && params[0] != def.Params[0]:
		return withArticle(defName) + " column with rel_acc " + formatFloat(def.Params[0])
	}
	return ""
}

// withArticle puts "a" or "an" before an aggregate function name: an avg, a uniq.
func withArticle(fn string) string {
	if strings.ContainsRune("aeio", rune(fn[0])) { // uniq starts with a consonant sound
		return "an " + fn
	}
	return "a " + fn
}

// combinable reports a plain aggregate of a view column whose finalized values can't be
// combined that way (DF4201 to DF4204), with or without -If on either side.
func (q *queryCompiler) combinable(st *stmt, x *sql.Call, agg catalog.Aggregate) bool {
	if q.src == nil || len(agg.Args) == 0 || agg.Args[0].Op != catalog.OpColumn {
		return true
	}
	def := q.src.aggregate(agg.Args[0].Name)
	if def == nil {
		return true
	}
	col := def.Name
	call := callHead(st, x) + "(" + col + ")"
	switch {
	case agg.Fn == catalog.AggUniqExact && def.Fn == catalog.AggUniqExact:
		q.notCombinable(st, x, col)
	case agg.Fn == catalog.AggAvg && def.Fn == catalog.AggAvg:
		st.errorf(x.Pos, "DF4202", "use avgMerge("+col+")", "%s would average the averages of rows with different counts", call)
	case agg.Fn == catalog.AggUniq && def.Fn == catalog.AggUniq:
		st.errorf(x.Pos, "DF4203", "use uniqMerge("+col+")", "%s would count distinct estimates, not distinct values", call)
	case isQuantile(agg.Fn) && isQuantile(def.Fn):
		level := formatFloat(agg.Params[len(agg.Params)-1])
		st.errorf(x.Pos, "DF4204", "use quantileMerge("+level+")("+col+"), or quantileDDMerge for a quantileDD column",
			"%s would take a quantile of quantiles", call)
	default:
		return true
	}
	return false
}

func isQuantile(fn catalog.AggFunc) bool {
	return fn == catalog.AggQuantile || fn == catalog.AggQuantileDD
}

// notCombinable reports an aggregate that would combine uniqExact values of different
// view rows (DF4201). The hint suggests a view with one row per group: per GROUP BY key,
// or without GROUP BY, per key column that WHERE binds with =.
func (q *queryCompiler) notCombinable(st *stmt, x *sql.Call, col string) {
	call := callHead(st, x) + "(" + col + ")"
	keys := strings.Join(q.groupNames, ", ")
	if q.sel.GroupBy == nil {
		keys = strings.Join(q.equalityKeys(), ", ")
	}
	hint := "make " + col + " a uniq() column and use uniqMerge(" + col + ") for an approximate count"
	if keys != "" {
		hint = "declare a view grouped by (" + keys + "), or " + hint
	}
	if q.sel.GroupBy == nil {
		st.errorf(x.Pos, "DF4201", hint, "%s cannot be combined across rows (the query combines all rows of %s).",
			call, q.src.name)
		return
	}
	var rows []string
	for _, k := range q.src.view.version.Keys {
		rows = append(rows, k.Name)
	}
	st.errorf(x.Pos, "DF4201", hint, "%s cannot be combined across rows (the query groups by (%s) but %s rows are (%s)).",
		call, keys, q.src.name, strings.Join(rows, ", "))
}

// callHead returns a call's name with its parameter list, if it has one: quantile(0.9).
func callHead(st *stmt, x *sql.Call) string {
	if !x.Parametric {
		return x.Name.Name
	}
	params := make([]string, len(x.Params))
	for i, p := range x.Params {
		params[i] = st.text(p)
	}
	return x.Name.Name + "(" + strings.Join(params, ", ") + ")"
}

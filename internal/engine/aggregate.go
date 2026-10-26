// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
)

// aggregation is a query's read-time aggregation (sql.md §7.4, §10.7).
type aggregation struct {
	groupBy    []*expr.Program
	groupTypes []catalog.Type
	aggs       []queryAggregate
	// rowKey holds the source columns of the row key, in key order: the rows are aggregated
	// in that order, which breaks the ties of argMax, argMin and any.
	rowKey     []int
	rowKeyType []catalog.Type
}

// queryAggregate is a compiled read-time aggregate.
type queryAggregate struct {
	fn *agg.Func
	// acc makes a group's state: fn, or for a -Merge the view column's Func, whose states
	// merge only with its own.
	acc  *agg.Func
	args []*expr.Program
	cond *expr.Program // the -If condition; nil without -If
	// merge is the view aggregate whose stored states a -Merge merges, or -1.
	merge int
}

// rowKey returns the indexes in q.cols of the row key: the view key, or (event_time,
// event_id) for a stream, whose keyCols also start with the PRIMARY KEY.
func (q *query) rowKey(keyCols []string) []int {
	if q.stream != nil {
		keyCols = keyCols[len(keyCols)-2:]
	}
	idx := make([]int, len(keyCols))
	for i, name := range keyCols {
		idx[i] = slices.IndexFunc(q.cols, func(c catalog.Column) bool { return c.Name == name })
	}
	return idx
}

// compileAggregation compiles the GROUP BY keys and the aggregates over the source rows.
func (q *query) compileAggregation(scope expr.Scope) error {
	a, def := q.agg, q.def.Aggregation
	for _, i := range a.rowKey {
		a.rowKeyType = append(a.rowKeyType, q.cols[i].Type)
	}
	for _, g := range def.GroupBy {
		p, err := expr.Compile(&g.Expr, scope)
		if err != nil {
			return fmt.Errorf("GROUP BY %s: %w", g.Name, err)
		}
		a.groupBy = append(a.groupBy, p)
		a.groupTypes = append(a.groupTypes, g.Expr.Type)
	}
	for i := range def.Aggregates {
		qa, err := q.compileAggregate(&def.Aggregates[i], scope)
		if err != nil {
			return fmt.Errorf("aggregate %d (%s): %w", i, def.Aggregates[i].Fn, err)
		}
		a.aggs = append(a.aggs, qa)
	}
	return nil
}

func (q *query) compileAggregate(def *catalog.QueryAggregate, scope expr.Scope) (queryAggregate, error) {
	qa := queryAggregate{merge: -1}
	spec := agg.Spec{Agg: def.Aggregate, ReadTime: true}
	if def.Merge {
		if q.view != nil && def.Args[0].Op == catalog.OpColumn {
			qa.merge = slices.IndexFunc(q.view.Aggregates, func(va catalog.ViewAggregate) bool {
				return va.Name == def.Args[0].Name && va.Fn == def.Fn
			})
		}
		if qa.merge < 0 {
			return queryAggregate{}, fmt.Errorf("%sMerge doesn't name a %s column of a view", def.Fn, def.Fn)
		}
		q.merges, qa.acc = true, q.funcs[qa.merge]
		// fn decodes the merged states, so it takes the view column's argument types.
		spec.ReadTime = false
		for _, arg := range q.view.Aggregates[qa.merge].Args {
			spec.ArgTypes = append(spec.ArgTypes, arg.Type)
		}
	} else {
		for i := range def.Args {
			p, err := expr.Compile(&def.Args[i], scope)
			if err != nil {
				return queryAggregate{}, err
			}
			qa.args = append(qa.args, p)
			spec.ArgTypes = append(spec.ArgTypes, def.Args[i].Type)
		}
	}
	if def.Cond != nil {
		p, err := expr.Compile(def.Cond, scope)
		if err != nil {
			return queryAggregate{}, err
		}
		qa.cond = p
	}
	f, err := agg.Compile(spec)
	if err != nil {
		return queryAggregate{}, err
	}
	qa.fn = f
	if qa.acc == nil {
		qa.acc = f
	}
	return qa, nil
}

// group is one group of an aggregation.
type group struct {
	keys   []expr.Value
	states []agg.State
}

// groups aggregates the source rows and returns a frame per group, with its GROUP BY keys as
// columns and its finalized aggregates. Without GROUP BY there is exactly one group, also
// over no rows.
func (a *aggregation) groups(src []source, params []expr.Value) ([]expr.Frame, error) {
	slices.SortStableFunc(src, func(x, y source) int {
		for i, col := range a.rowKey {
			if c := expr.Compare(x.vals[col], a.rowKeyType[i], y.vals[col], a.rowKeyType[i]); c != 0 {
				return c
			}
		}
		return 0
	})
	order, err := a.accumulate(src, params)
	if err != nil {
		return nil, err
	}
	if len(a.groupBy) == 0 && len(order) == 0 {
		order = append(order, a.newGroup(nil))
	}
	frames := make([]expr.Frame, len(order))
	for i, g := range order {
		frames[i] = expr.Frame{Cols: g.keys, Params: params, Aggs: make([]expr.Value, len(g.states))}
		for j, s := range g.states {
			if frames[i].Aggs[j], err = a.aggs[j].final(s); err != nil {
				return nil, err
			}
		}
	}
	return frames, nil
}

// accumulate adds each source row, in order, to its group, and returns the groups in the
// order of their first rows.
func (a *aggregation) accumulate(src []source, params []expr.Value) ([]*group, error) {
	byKey := map[string]*group{}
	var order []*group
	for pos, row := range src {
		f := &expr.Frame{Cols: row.vals, Params: params}
		ks := make([]expr.Value, len(a.groupBy))
		for i, p := range a.groupBy {
			var err error
			if ks[i], err = p.Eval(f); err != nil {
				return nil, err
			}
		}
		gk := groupKey(a.groupTypes, ks)
		g := byKey[gk]
		if g == nil {
			g = a.newGroup(ks)
			byKey[gk] = g
			order = append(order, g)
		}
		for i := range a.aggs {
			if err := a.aggs[i].add(g.states[i], f, row.states, pos); err != nil {
				return nil, err
			}
		}
	}
	return order, nil
}

func (a *aggregation) newGroup(ks []expr.Value) *group {
	g := &group{keys: ks, states: make([]agg.State, len(a.aggs))}
	for i := range a.aggs {
		g.states[i] = a.aggs[i].acc.New()
	}
	return g
}

// add adds a source row to s: for a -Merge, the row's stored state, which an empty state
// leaves alone; otherwise the row's arguments, unless the -If condition doesn't hold or an
// argument is NULL. pos is the row's position in key order, which breaks ties.
func (qa *queryAggregate) add(s agg.State, f *expr.Frame, states []agg.State, pos int) error {
	if qa.merge >= 0 {
		return s.Merge(states[qa.merge])
	}
	args, ok, err := agg.EvalArgs(qa.cond, qa.args, f)
	if err != nil || !ok {
		return err
	}
	return s.Add(agg.Input{Args: args, ArrayIndex: pos})
}

// final returns the value of a group's state s. A merged state goes through its stored
// form to fn once, which may finalize it differently: quantileMerge may take another level.
func (qa *queryAggregate) final(s agg.State) (expr.Value, error) {
	if qa.acc == qa.fn {
		return s.Final()
	}
	if s.Empty() {
		return qa.fn.New().Final()
	}
	a, err := s.Encode()
	if err != nil {
		return expr.Value{}, err
	}
	if s, err = qa.fn.Decode(a); err != nil {
		return expr.Value{}, err
	}
	return s.Final()
}

// groupKey returns a text that equal GROUP BY key values, and only they, share.
func groupKey(types []catalog.Type, vals []expr.Value) string {
	var b []byte
	for i, v := range vals {
		if v.IsNull() {
			b = append(b, 0)
			continue
		}
		c := keys.Canonical(nil, types[i], v)
		b = binary.AppendUvarint(append(b, 1), uint64(len(c)))
		b = append(b, c...)
	}
	return string(b)
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package projector keeps views current: it applies a delivery unit's change-stream records
// to every view version of their stream, with conditional writes that make a redelivered
// record count once (docs/spec/encoding.md §7). It also merges stored rows for the read
// path, so rows are read with the code that writes them.
package projector

import (
	"fmt"
	"slices"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// Compiled is a catalog compiled for projection and reads. It never changes, so it is safe
// for concurrent use.
type Compiled struct {
	def      *catalog.Catalog
	streams  map[string]*Stream
	views    map[string]*view   // by version ID, such as page_daily@v2
	byStream map[string][]*view // in catalog order
}

// view is a compiled view version.
type view struct {
	id     string
	name   string
	def    *catalog.ViewVersion
	stream *Stream
	// reads are the indexes of the decoded event values that the version reads.
	reads    []int
	array    *expr.Program // the ARRAY JOIN operand; nil without ARRAY JOIN
	where    *expr.Program // nil without WHERE
	keys     []*expr.Program
	keyTypes []catalog.Type
	ttlKey   int // the index in keys of the TTL key; -1 without TTL
	aggs     []aggregate
}

// aggregate is a compiled aggregate column.
type aggregate struct {
	name     string
	fn       *agg.Func
	args     []*expr.Program
	argTypes []catalog.Type
	cond     *expr.Program // the -If condition; nil without -If
	// markers is set for uniqExact, whose values count through marker items.
	markers bool
}

// Compile compiles every stream and view version of cat.
func Compile(cat *catalog.Catalog) (*Compiled, error) {
	c := &Compiled{
		def:      cat,
		streams:  make(map[string]*Stream, len(cat.Streams)),
		views:    make(map[string]*view),
		byStream: make(map[string][]*view),
	}
	for i := range cat.Streams {
		s, err := compileStream(&cat.Streams[i])
		if err != nil {
			return nil, fmt.Errorf("stream %s: %w", cat.Streams[i].Name, err)
		}
		c.streams[s.Def.Name] = s
	}
	for i := range cat.Views {
		for j := range cat.Views[i].Versions {
			vv := &cat.Views[i].Versions[j]
			id := catalog.VersionID(cat.Views[i].Name, vv.Number)
			s, ok := c.streams[vv.Stream]
			if !ok {
				return nil, fmt.Errorf("view %s: no stream %s", id, vv.Stream)
			}
			v, err := compileView(cat.Views[i].Name, id, vv, s)
			if err != nil {
				return nil, fmt.Errorf("view %s: %w", id, err)
			}
			c.views[id] = v
			c.byStream[vv.Stream] = append(c.byStream[vv.Stream], v)
		}
	}
	return c, nil
}

// Stream returns the compiled stream named name.
func (c *Compiled) Stream(name string) (*Stream, bool) {
	s, ok := c.streams[name]
	return s, ok
}

// Version returns the view version with ID id, such as page_daily@v2, and its view's name.
func (c *Compiled) Version(id string) (string, *catalog.ViewVersion, bool) {
	v, ok := c.views[id]
	if !ok {
		return "", nil, false
	}
	return v.name, v.def, true
}

func compileView(name, id string, def *catalog.ViewVersion, s *Stream) (*view, error) {
	v := &view{id: id, name: name, def: def, stream: s, ttlKey: -1}
	cols := s.names()
	if def.ArrayJoin != nil {
		cols = append(cols, def.ArrayJoin.Alias)
	}
	scope := expr.Scope{Columns: cols}
	var err error
	if def.ArrayJoin != nil {
		if v.array, err = expr.Compile(&def.ArrayJoin.Array, scope); err != nil {
			return nil, fmt.Errorf("ARRAY JOIN: %w", err)
		}
	}
	if def.Where != nil {
		if v.where, err = expr.Compile(def.Where, scope); err != nil {
			return nil, fmt.Errorf("WHERE: %w", err)
		}
	}
	for _, k := range def.Keys {
		p, kerr := expr.Compile(&k.Expr, scope)
		if kerr != nil {
			return nil, fmt.Errorf("key %s: %w", k.Name, kerr)
		}
		v.keys = append(v.keys, p)
		v.keyTypes = append(v.keyTypes, k.Expr.Type)
	}
	if def.TTL != nil {
		v.ttlKey = slices.IndexFunc(def.Keys, func(k catalog.NamedExpr) bool { return k.Name == def.TTL.Key })
		if v.ttlKey < 0 {
			return nil, fmt.Errorf("TTL: no key %s", def.TTL.Key)
		}
	}
	for i := range def.Aggregates {
		a, aerr := compileAggregate(&def.Aggregates[i], scope)
		if aerr != nil {
			return nil, fmt.Errorf("aggregate %s: %w", def.Aggregates[i].Name, aerr)
		}
		v.aggs = append(v.aggs, a)
	}
	v.reads = reads(def, cols)
	return v, nil
}

func compileAggregate(def *catalog.ViewAggregate, scope expr.Scope) (aggregate, error) {
	a := aggregate{name: def.Name, markers: def.Fn == catalog.AggUniqExact}
	for i := range def.Args {
		p, err := expr.Compile(&def.Args[i], scope)
		if err != nil {
			return aggregate{}, err
		}
		a.args = append(a.args, p)
		a.argTypes = append(a.argTypes, def.Args[i].Type)
	}
	if def.Cond != nil {
		p, err := expr.Compile(def.Cond, scope)
		if err != nil {
			return aggregate{}, err
		}
		a.cond = p
	}
	f, err := agg.Compile(agg.Spec{Agg: def.Aggregate, ArgTypes: a.argTypes})
	if err != nil {
		return aggregate{}, err
	}
	a.fn = f
	return a, nil
}

// reads returns the indexes in cols of the event values a version reads: the columns its
// expressions name, and event_time and event_id for the aggregates whose ties they break.
func reads(def *catalog.ViewVersion, cols []string) []int {
	var exprs []*catalog.Expr
	if def.ArrayJoin != nil {
		exprs = append(exprs, &def.ArrayJoin.Array)
	}
	if def.Where != nil {
		exprs = append(exprs, def.Where)
	}
	for i := range def.Keys {
		exprs = append(exprs, &def.Keys[i].Expr)
	}
	names := map[string]bool{}
	for i := range def.Aggregates {
		a := &def.Aggregates[i]
		for j := range a.Args {
			exprs = append(exprs, &a.Args[j])
		}
		if a.Cond != nil {
			exprs = append(exprs, a.Cond)
		}
		switch a.Fn { //nolint:exhaustive // only these break ties by event
		case catalog.AggArgMax, catalog.AggArgMin, catalog.AggAny:
			names[catalog.ColumnEventTime], names[catalog.ColumnEventID] = true, true
		}
	}
	for _, e := range exprs {
		e.ColumnNames(names)
	}
	var idx []int
	for i, name := range cols {
		if names[name] && (def.ArrayJoin == nil || name != def.ArrayJoin.Alias) {
			idx = append(idx, i)
		}
	}
	return idx
}

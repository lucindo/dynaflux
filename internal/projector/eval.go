// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"fmt"
	"slices"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// rowGroup is one row item of a view version and the batch's or backfill chunk's
// contributions to it, in the order they are applied.
type rowGroup struct {
	v     *view
	owner byte // keys.OwnerLive or keys.OwnerBackfill: the namespace the contributions count in
	key   store.Key
	lsk   string // the sort fields without a SPLIT suffix, for marker keys
	keys  []expr.Value
	// exp is when the row expires, in microseconds, when hasExp: the version has TTL.
	exp      int64
	hasExp   bool
	contribs []*contribution
	// owners caches marker owners across attempts: a marker's owner never changes.
	owners map[store.Key]string
}

// contribution is what one record adds to one row item.
type contribution struct {
	seq       string // in PadSeq's form
	delivered string
	unitKey   string
	states    []agg.State   // by aggregate; nil where no input reached it
	values    [][]uniqValue // by aggregate with markers: the record's distinct values
	seen      map[uniqKey]bool
}

type uniqValue struct {
	canon string
	value expr.Value
}

type uniqKey struct {
	agg   int
	canon string
}

type groupKey struct {
	v *view
	k store.Key
}

// rows collects a batch's or backfill chunk's contributions by row item, in the order the
// items were reached.
type rows struct {
	owner  byte // keys.OwnerLive, or keys.OwnerBackfill for the backfill's namespace B
	groups map[groupKey]*rowGroup
	order  []*rowGroup
}

// sourceRow is one input row of a view: an event, or one element of its ARRAY JOIN array
// at the 1-based position index.
type sourceRow struct {
	frame expr.Frame
	index int
}

// add evaluates one event for v and adds its contributions: per source row, WHERE, then the
// keys, then each aggregate's condition and arguments. An error is deterministic: it opens
// v's breaker.
func (rs *rows) add(v *view, rec *record, vals []expr.Value, bad []error) error {
	for _, i := range v.reads {
		if bad != nil && bad[i] != nil {
			return bad[i]
		}
	}
	srcs, err := v.sourceRows(vals)
	if err != nil {
		return err
	}
	n := len(v.stream.Columns)
	in := agg.Input{
		EventTime: vals[n+implicitEventTime].AsMicros(),
		EventID:   vals[n+implicitEventID].AsString(),
	}
	byRow := map[store.Key]*contribution{}
	for _, src := range srcs {
		ok, kvs, kerr := v.rowKeys(&src.frame)
		if kerr != nil {
			return kerr
		}
		if !ok {
			continue
		}
		k, lsk, kerr := v.itemKey(kvs, rec.unit)
		if kerr != nil {
			return kerr
		}
		c := byRow[k]
		if c == nil {
			if c, err = rs.contribution(v, k, lsk, kvs, rec); err != nil {
				return err
			}
			byRow[k] = c
		}
		in.ArrayIndex = src.index
		if err = v.addInputs(c, &src.frame, in); err != nil {
			return err
		}
	}
	return nil
}

func (v *view) sourceRows(vals []expr.Value) ([]sourceRow, error) {
	if v.array == nil {
		return []sourceRow{{frame: expr.Frame{Cols: vals}}}, nil
	}
	cols := append(slices.Clone(vals), expr.Null())
	a, err := v.array.Eval(&expr.Frame{Cols: cols})
	if err != nil {
		return nil, err
	}
	elems := a.AsArray()
	srcs := make([]sourceRow, len(elems))
	for i, e := range elems {
		c := slices.Clone(cols)
		c[len(c)-1] = e
		srcs[i] = sourceRow{frame: expr.Frame{Cols: c}, index: i + 1}
	}
	return srcs, nil
}

// rowKeys returns the key values of the row a source row reaches, and false when WHERE
// drops it.
func (v *view) rowKeys(f *expr.Frame) (bool, []expr.Value, error) {
	if v.where != nil {
		w, err := v.where.Eval(f)
		if err != nil || !w.IsTrue() {
			return false, nil, err
		}
	}
	kvs := make([]expr.Value, len(v.keys))
	for i, p := range v.keys {
		var err error
		if kvs[i], err = p.Eval(f); err != nil {
			return false, nil, err
		}
	}
	return true, kvs, nil
}

// itemKey returns the key of the item that a record of unit writes for the row with key
// values kvs, and the row's sort fields without a SPLIT suffix.
func (v *view) itemKey(kvs []expr.Value, unit string) (store.Key, string, error) {
	n := v.def.PartitionKeyLen
	pk, err := keys.Key(v.keyTypes[:n], kvs[:n])
	if err != nil {
		return store.Key{}, "", fmt.Errorf("%w: partition key: %w", expr.ErrEval, err)
	}
	lsk := ""
	if len(kvs) > n {
		if lsk, err = keys.Key(v.keyTypes[n:], kvs[n:]); err != nil {
			return store.Key{}, "", fmt.Errorf("%w: sort key: %w", expr.ErrEval, err)
		}
	}
	sk := lsk
	switch {
	case v.def.Split > 1:
		sk = keys.SplitKey(lsk, keys.Bucket(unit, v.def.Split))
	case sk == "":
		sk = keys.NoSortKey
	}
	if len(pk) > store.MaxPKLen || len(sk) > store.MaxSKLen {
		return store.Key{}, "", fmt.Errorf("%w: the row key is %d and %d bytes, over DynamoDB's %d and %d",
			expr.ErrEval, len(pk), len(sk), store.MaxPKLen, store.MaxSKLen)
	}
	return store.Key{PK: pk, SK: sk}, lsk, nil
}

func (rs *rows) contribution(v *view, k store.Key, lsk string, kvs []expr.Value, rec *record) (*contribution, error) {
	gk := groupKey{v: v, k: k}
	g := rs.groups[gk]
	if g == nil {
		exp, hasExp, err := v.expiry(kvs)
		if err != nil {
			return nil, err
		}
		g = &rowGroup{v: v, owner: rs.owner, key: k, lsk: lsk, keys: kvs, exp: exp, hasExp: hasExp, owners: map[store.Key]string{}}
		rs.groups[gk] = g
		rs.order = append(rs.order, g)
	}
	c := &contribution{
		seq:       rec.seq,
		delivered: rec.Seq,
		unitKey:   rec.unitKey,
		states:    make([]agg.State, len(v.aggs)),
		values:    make([][]uniqValue, len(v.aggs)),
	}
	g.contribs = append(g.contribs, c)
	return c, nil
}

// addInputs adds a source row's input to each aggregate whose -If condition holds and whose
// arguments aren't NULL. A uniqExact value is kept once per record and row.
func (v *view) addInputs(c *contribution, f *expr.Frame, in agg.Input) error {
	for i, a := range v.aggs {
		args, ok, err := agg.EvalArgs(a.cond, a.args, f)
		switch {
		case err != nil:
			return err
		case !ok:
		case a.markers:
			c.addValue(i, string(keys.Canonical(nil, a.argTypes[0], args[0])), args[0])
		default:
			if c.states[i] == nil {
				c.states[i] = a.fn.New()
			}
			in.Args = args
			if err = c.states[i].Add(in); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *contribution) addValue(i int, canon string, v expr.Value) {
	k := uniqKey{agg: i, canon: canon}
	if c.seen == nil {
		c.seen = map[uniqKey]bool{}
	}
	if !c.seen[k] {
		c.seen[k] = true
		c.values[i] = append(c.values[i], uniqValue{canon: canon, value: v})
	}
}

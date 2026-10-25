// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
)

// check enforces the invariants that [Unmarshal] promises.
func check(c *Catalog) error {
	if err := unique("stream", c.Streams, func(s Stream) string { return s.Name }); err != nil {
		return err
	}
	if err := unique("view", c.Views, func(v View) string { return v.Name }); err != nil {
		return err
	}
	if err := unique("query", c.Queries, func(q Query) string { return q.Name }); err != nil {
		return err
	}
	// Streams and views share one namespace: both become tables and tables: keys.
	for _, v := range c.Views {
		if _, found := c.Stream(v.Name); found {
			return fmt.Errorf("view %s has the name of a stream", v.Name)
		}
	}
	for i := range c.Streams {
		if err := checkStream(&c.Streams[i]); err != nil {
			return fmt.Errorf("stream %s: %w", c.Streams[i].Name, err)
		}
	}
	for i := range c.Views {
		if err := checkView(&c.Views[i]); err != nil {
			return err
		}
	}
	for i := range c.Queries {
		if err := checkQuery(&c.Queries[i]); err != nil {
			return fmt.Errorf("query %s: %w", c.Queries[i].Name, err)
		}
	}
	return nil
}

func checkStream(s *Stream) error {
	if err := unique("column", slices.Concat(s.Columns, ImplicitColumns()), func(c Column) string { return c.Name }); err != nil {
		return err
	}
	if s.Shards < 1 || s.Shards > MaxShards {
		return fmt.Errorf("shards %d is not between 1 and %d", s.Shards, MaxShards)
	}
	if err := checkStorageFormat(s.StorageFormat); err != nil {
		return err
	}
	for _, col := range s.Columns {
		if col.Default == nil {
			continue
		}
		if col.Type.Nullable {
			return fmt.Errorf("column %s: a Nullable column has no default", col.Name)
		}
		if err := col.Default.check(0); err != nil {
			return fmt.Errorf("column %s: %w", col.Name, err)
		}
	}
	return nil
}

func checkView(v *View) error {
	if err := unique("version", v.Versions, func(vv ViewVersion) string { return strconv.Itoa(vv.Number) }); err != nil {
		return fmt.Errorf("view %s: %w", v.Name, err)
	}
	current := 0
	for i := range v.Versions {
		vv := &v.Versions[i]
		if vv.State == StateCurrent {
			current++
		}
		if err := checkVersion(vv); err != nil {
			return fmt.Errorf("view %s: %w", VersionID(v.Name, vv.Number), err)
		}
	}
	if current > 1 {
		return fmt.Errorf("view %s: %d versions are current", v.Name, current)
	}
	return nil
}

func checkVersion(vv *ViewVersion) error {
	switch {
	case vv.Number < 1:
		return errors.New("version numbers start at 1")
	case vv.Split < 1 || vv.Split > MaxSplit:
		return fmt.Errorf("split %d is not between 1 and %d", vv.Split, MaxSplit)
	case vv.PartitionKeyLen < 1 || vv.PartitionKeyLen > len(vv.Keys):
		return fmt.Errorf("partition key length %d is not between 1 and the %d keys", vv.PartitionKeyLen, len(vv.Keys))
	}
	if err := checkStorageFormat(vv.StorageFormat); err != nil {
		return err
	}
	columns := make([]string, 0, len(vv.Keys)+len(vv.Aggregates))
	for _, k := range vv.Keys {
		columns = append(columns, k.Name)
	}
	for _, a := range vv.Aggregates {
		columns = append(columns, a.Name)
	}
	if err := unique("column", columns, func(name string) string { return name }); err != nil {
		return err
	}
	exprs := []*Expr{vv.Where}
	if vv.ArrayJoin != nil {
		exprs = append(exprs, &vv.ArrayJoin.Array)
	}
	if err := checkExprs(0, append(exprs, namedExprs(vv.Keys)...)...); err != nil {
		return err
	}
	for i := range vv.Aggregates {
		if err := vv.Aggregates[i].check(false); err != nil {
			return fmt.Errorf("aggregate %s: %w", vv.Aggregates[i].Name, err)
		}
	}
	// Last, because every other mistake changes the fingerprint too.
	fp, err := Fingerprint(vv)
	if err != nil {
		return err
	}
	if fp != vv.Fingerprint {
		return fmt.Errorf("fingerprint %q doesn't match the definition, whose fingerprint is %s", vv.Fingerprint, fp)
	}
	return nil
}

func checkQuery(q *Query) error {
	if err := unique("parameter", q.Params, func(p Param) string { return p.Name }); err != nil {
		return err
	}
	rowExprs := []*Expr{q.Filter, q.Limit}
	for _, b := range slices.Concat(q.Keys.Partition, q.Keys.SortPrefix) {
		rowExprs = append(rowExprs, pointers(b.Values)...)
	}
	if r := q.Keys.Range; r != nil {
		for _, bound := range []*RangeBound{r.Lower, r.Upper} {
			if bound != nil {
				rowExprs = append(rowExprs, &bound.Value)
			}
		}
	}
	nAggs := 0
	if a := q.Aggregation; a != nil {
		nAggs = len(a.Aggregates)
		rowExprs = append(rowExprs, namedExprs(a.GroupBy)...)
		for i := range a.Aggregates {
			if err := a.Aggregates[i].check(a.Aggregates[i].Merge); err != nil {
				return fmt.Errorf("aggregate %d: %w", i, err)
			}
		}
	}
	if err := checkExprs(0, rowExprs...); err != nil {
		return err
	}
	// Only the output sees the aggregates.
	output := namedExprs(q.Columns)
	for i := range q.OrderBy {
		output = append(output, &q.OrderBy[i].Expr)
	}
	return checkExprs(nAggs, output...)
}

func checkStorageFormat(format int) error {
	if format < 1 || format > StorageFormatVersion {
		return fmt.Errorf("storage format %d is not supported", format)
	}
	return nil
}

// checkExprs checks each expression that isn't nil.
func checkExprs(nAggs int, exprs ...*Expr) error {
	for _, e := range exprs {
		if e == nil {
			continue
		}
		if err := e.check(nAggs); err != nil {
			return err
		}
	}
	return nil
}

// unique reports the first name that two items share.
func unique[T any](what string, items []T, name func(T) string) error {
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		n := name(item)
		if seen[n] {
			return fmt.Errorf("%s %s appears twice", what, n)
		}
		seen[n] = true
	}
	return nil
}

func pointers(exprs []Expr) []*Expr {
	out := make([]*Expr, len(exprs))
	for i := range exprs {
		out[i] = &exprs[i]
	}
	return out
}

func namedExprs(ns []NamedExpr) []*Expr {
	out := make([]*Expr, len(ns))
	for i := range ns {
		out[i] = &ns[i].Expr
	}
	return out
}

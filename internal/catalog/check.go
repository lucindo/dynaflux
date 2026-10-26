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
	if c.PruneWindowSeconds < 1 || c.PruneWindowSeconds > MaxPruneWindowSeconds {
		return fmt.Errorf("prune window %d seconds is not between 1 and %d", c.PruneWindowSeconds, MaxPruneWindowSeconds)
	}
	if err := names("stream", c.Streams, func(s Stream) string { return s.Name }); err != nil {
		return err
	}
	if err := names("view", c.Views, func(v View) string { return v.Name }); err != nil {
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
	if err := names("column", slices.Concat(s.Columns, ImplicitColumns()), func(c Column) string { return c.Name }, AttrPK, AttrSK); err != nil {
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
	if len(v.Versions) == 0 {
		return fmt.Errorf("view %s has no versions", v.Name)
	}
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
	if err := checkVersionNames(vv); err != nil {
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

// checkVersionNames checks the names a view version defines: its columns, which become
// DynamoDB attributes, and its ARRAY JOIN alias.
func checkVersionNames(vv *ViewVersion) error {
	columns := make([]string, 0, len(vv.Keys)+len(vv.Aggregates))
	for _, k := range vv.Keys {
		columns = append(columns, k.Name)
	}
	for _, a := range vv.Aggregates {
		columns = append(columns, a.Name)
	}
	if err := names("column", columns, func(name string) string { return name }, AttrPK, AttrSK); err != nil {
		return err
	}
	if vv.ArrayJoin != nil && !IsName(vv.ArrayJoin.Alias) {
		return fmt.Errorf("invalid ARRAY JOIN alias %q", vv.ArrayJoin.Alias)
	}
	return nil
}

func checkQuery(q *Query) error {
	if err := checkQueryShape(q); err != nil {
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

// checkQueryShape checks a query's names, key range and sort order.
func checkQueryShape(q *Query) error {
	if err := names("parameter", q.Params, func(p Param) string { return p.Name }); err != nil {
		return err
	}
	if err := names("output column", q.Columns, exprName); err != nil {
		return err
	}
	if a := q.Aggregation; a != nil {
		if err := names("GROUP BY key", a.GroupBy, exprName); err != nil {
			return err
		}
	}
	if r := q.Keys.Range; r != nil && r.Lower == nil && r.Upper == nil {
		return fmt.Errorf("key range on %s has no bound", r.Column)
	}
	// Only an aggregation without GROUP BY returns one row and needs no sort order.
	if len(q.OrderBy) == 0 && (q.Aggregation == nil || len(q.Aggregation.GroupBy) > 0) {
		return errors.New("no sort order")
	}
	return nil
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

// names checks the names that SQL defines (sql.md §3.4): each is [a-z][a-z0-9_]* and not
// one of reserved, and no two are equal.
func names[T any](what string, items []T, name func(T) string, reserved ...string) error {
	for _, item := range items {
		if n := name(item); !IsName(n) || slices.Contains(reserved, n) {
			return fmt.Errorf("invalid %s name %q", what, n)
		}
	}
	return unique(what, items, name)
}

// IsName reports whether s is a valid stream, view, column or parameter name:
// [a-z][a-z0-9_]* (sql.md §3.4).
func IsName(s string) bool {
	if s == "" || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for i := range len(s) {
		if c := s[i]; (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
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

func exprName(n NamedExpr) string { return n.Name }

func namedExprs(ns []NamedExpr) []*Expr {
	out := make([]*Expr, len(ns))
	for i := range ns {
		out[i] = &ns[i].Expr
	}
	return out
}

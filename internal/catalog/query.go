// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

// Query is a named query. It runs in this order:
//  1. read the items that Keys selects;
//  2. keep the rows where Filter is true;
//  3. with an Aggregation, group the rows and aggregate each group;
//  4. compute Columns, sort by OrderBy and keep the first Limit rows.
type Query struct {
	// Name is the -- name: annotation, the generated Go function name.
	Name        string      `json:"name"`
	Cardinality Cardinality `json:"cardinality"`
	// ExposeHTTP is set by -- expose: http.
	ExposeHTTP bool `json:"expose_http,omitzero"`
	// Params are in order of first appearance; generated code passes values in this order.
	Params []Param `json:"params,omitempty"`
	// Source is the stream or view in FROM. A view query reads the view's current version.
	Source Source  `json:"source"`
	Keys   KeyPlan `json:"keys"`
	// Filter holds the residual WHERE terms that don't bind keys; nil keeps every row.
	Filter      *Expr        `json:"filter,omitzero"`
	Aggregation *Aggregation `json:"aggregation,omitzero"`
	// Columns are the output columns in SELECT order; generated code uses this order.
	Columns []NamedExpr `json:"columns,omitempty"`
	// OrderBy is the full sort order: the ORDER BY items, then the columns of the row key
	// that they don't sort by, in the direction of the last ORDER BY item (ascending
	// without ORDER BY). The row key is the view key, (event_time, event_id) for a stream,
	// or the GROUP BY keys. OrderBy is empty only for an aggregation without GROUP BY,
	// which returns one row.
	OrderBy []OrderItem `json:"order_by,omitempty"`
	// Limit is an unsigned integer literal or parameter, or nil for no limit.
	Limit *Expr `json:"limit,omitzero"`
}

// Param is a query parameter. An Array type appears only on the right of IN.
type Param struct {
	Name string `json:"name"`
	Type Type   `json:"type"`
}

// Source is what a query reads.
type Source struct {
	Kind SourceKind `json:"kind"`
	Name string     `json:"name"`
}

// SourceKind says whether a query reads a stream or a view.
type SourceKind string

// Source kinds.
const (
	SourceStream SourceKind = "stream"
	SourceView   SourceKind = "view"
)

func (k SourceKind) known() bool {
	switch k {
	case SourceStream, SourceView:
		return true
	}
	return false
}

// UnmarshalText implements [encoding.TextUnmarshaler]; it rejects unknown kinds.
func (k *SourceKind) UnmarshalText(text []byte) error {
	return decodeEnum(k, text, "query source kind")
}

// MaxKeyCombinations is the most key combinations one query call may read.
const MaxKeyCombinations = 100

// KeyPlan is the bounded read of a query. The read covers every combination of the bound
// values, at most [MaxKeyCombinations]; with SPLIT or SHARDS, each combination reads all
// its items.
//
// Bound values keep their own types, which may differ from the key column's, such as a
// Date parameter that bounds a DateTime key. The engine converts them exactly, so the read
// returns exactly the rows the WHERE terms select: a value the key type can't hold matches
// nothing, and a range bound that falls between two key values moves inward.
type KeyPlan struct {
	// Partition binds every partition key column in key order: the view's PRIMARY KEY or
	// the stream's.
	Partition []KeyBinding `json:"partition,omitempty"`
	// SortPrefix binds the view sort key columns that follow, in key order. A stream query
	// has none.
	SortPrefix []KeyBinding `json:"sort_prefix,omitempty"`
	// Range bounds the next sort key column, or a stream's event_time; nil reads the whole
	// remaining key range. A stream query's event_time = v is the range from v to v, both
	// inclusive.
	Range *KeyRange `json:"range,omitzero"`
}

// KeyBinding fixes a key column to a set of values.
type KeyBinding struct {
	Column string `json:"column"`
	// Values are expressions over literals and parameters. An Array-typed value contributes
	// its elements. One scalar value is an equality.
	Values []Expr `json:"values,omitempty"`
}

// KeyRange bounds a key column. At least one bound is set.
type KeyRange struct {
	Column string      `json:"column"`
	Lower  *RangeBound `json:"lower,omitzero"`
	Upper  *RangeBound `json:"upper,omitzero"`
}

// RangeBound is one end of a [KeyRange]: an expression over literals and parameters.
type RangeBound struct {
	Value     Expr `json:"value"`
	Inclusive bool `json:"inclusive,omitzero"`
}

// Aggregation is a query's read-time aggregation. Without GroupBy, it yields exactly one
// row, even over no rows.
type Aggregation struct {
	// GroupBy are the GROUP BY keys. Their names are the columns that Columns and OrderBy
	// see besides the aggregates.
	GroupBy []NamedExpr `json:"group_by,omitempty"`
	// Aggregates are in the order they appear in the query, one per call; an OpAggregate
	// refers to one by its position.
	Aggregates []QueryAggregate `json:"aggregates,omitempty"`
}

// QueryAggregate is a read-time aggregate.
type QueryAggregate struct {
	Aggregate

	// Merge is set for <fn>Merge: Args[0] is a view column whose stored states are merged.
	// Otherwise the aggregate reads finalized values.
	Merge bool `json:"merge,omitzero"`
}

// OrderItem is one sort key of a query. NULLs sort last unless NullsFirst is set.
type OrderItem struct {
	Expr       Expr `json:"expr"`
	Desc       bool `json:"desc,omitzero"`
	NullsFirst bool `json:"nulls_first,omitzero"`
}

// Cardinality is a query's result shape.
type Cardinality string

// Cardinalities.
const (
	CardinalityOne  Cardinality = "one"  // One row, or a not-found error.
	CardinalityMany Cardinality = "many" // A list of rows, possibly empty.
)

func (c Cardinality) known() bool {
	switch c {
	case CardinalityOne, CardinalityMany:
		return true
	}
	return false
}

// UnmarshalText implements [encoding.TextUnmarshaler]; it rejects unknown values.
func (c *Cardinality) UnmarshalText(text []byte) error {
	return decodeEnum(c, text, "query cardinality")
}

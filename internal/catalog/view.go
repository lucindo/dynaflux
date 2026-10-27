// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"fmt"
	"slices"
	"strconv"
)

// MaxSplit is the largest SPLIT count.
const MaxSplit = 16

// VersionID returns the name of a view version, such as page_daily@v2, in the form that
// Dynaflux shows and stores.
func VersionID(view string, number int) string {
	return view + "@v" + strconv.Itoa(number)
}

// View is a materialized view: every version that isn't retired.
type View struct {
	Name     string        `json:"name"`
	Versions []ViewVersion `json:"versions,omitempty"`
}

// Current returns the version the SQL files define, which queries read. A view whose SQL
// was removed has only retained versions.
func (v *View) Current() (*ViewVersion, bool) {
	i := slices.IndexFunc(v.Versions, func(vv ViewVersion) bool { return vv.State == StateCurrent })
	if i < 0 {
		return nil, false
	}
	return &v.Versions[i], true
}

// ViewVersion is one immutable version of a view, stored in its own table.
//
// [Fingerprint] hashes the JSON of every field except Number, State, Fingerprint and Mode.
// So a field added here, or to a type a version holds (Expr, NamedExpr, ArrayJoin, ViewTTL,
// ViewAggregate), must be omitzero or omitempty, even when only queries use it: otherwise
// every fingerprint changes and every deployed view gets a new version.
type ViewVersion struct {
	// Number is the version number from dynaflux.lock, starting at 1.
	Number      int          `json:"number"`
	State       VersionState `json:"state"`
	Fingerprint string       `json:"fingerprint"`
	// Mode can change in place, so it isn't part of the fingerprint.
	Mode Mode `json:"mode"`
	// Rebuild is the view's rebuild count in dynaflux.lock when the version was defined. It
	// is part of the fingerprint, so dynaflux generate -rebuild gives an unchanged definition
	// a new version.
	Rebuild int `json:"rebuild,omitzero"`
	// Stream is the source stream.
	Stream    string     `json:"stream"`
	ArrayJoin *ArrayJoin `json:"array_join,omitzero"`
	// Where is the WHERE condition, applied after ARRAY JOIN; nil keeps every row.
	Where *Expr `json:"where,omitzero"`
	// Keys are the view keys in ORDER BY order.
	Keys []NamedExpr `json:"keys,omitempty"`
	// PartitionKeyLen is how many leading Keys form the PRIMARY KEY (the partition key);
	// the rest form the sort key.
	PartitionKeyLen int      `json:"partition_key_len"`
	TTL             *ViewTTL `json:"ttl,omitzero"`
	// Split is the SPLIT count: the items each row is stored as, 1 without the clause and at
	// most [MaxSplit].
	Split int `json:"split"`
	// Aggregates are the aggregate columns. Their order carries no meaning.
	Aggregates    []ViewAggregate `json:"aggregates,omitempty"`
	StorageFormat int             `json:"storage_format"`
}

// FencedHandoff reports whether a backfill of v needs the fenced handoff: it does when an
// aggregate is additive, because an additive state counts an event applied twice.
func (v *ViewVersion) FencedHandoff() bool {
	return slices.ContainsFunc(v.Aggregates, func(a ViewAggregate) bool { return !a.Fn.Idempotent() })
}

// ArrayJoin is a view's ARRAY JOIN: one source row per element of Array.
type ArrayJoin struct {
	// Array is the Array-typed operand.
	Array Expr `json:"array"`
	// Alias names the element column.
	Alias string `json:"alias"`
}

// ViewTTL is a view's TTL: a row expires at its Key value plus Plus.
type ViewTTL struct {
	// Key is a view key of type Date, DateTime or DateTime64.
	Key string `json:"key"`
	// Plus is the INTERVAL added to Key, or nil.
	Plus *Interval `json:"plus,omitzero"`
}

// Aggregate is an aggregate function call with its resolved result type.
type Aggregate struct {
	Fn AggFunc `json:"fn"`
	// Params are the literal parameters of a parametric aggregate, in SQL order:
	// [level] for quantile, [rel_acc, level] for quantileDD. A level left out in SQL is 0.5.
	Params []float64 `json:"params,omitempty"`
	// Args are the arguments, without the -If condition.
	Args []Expr `json:"args,omitempty"`
	// Cond is the -If condition, or nil.
	Cond *Expr `json:"cond,omitzero"`
	// Type is the finalized result type.
	Type Type `json:"type"`
}

// ViewAggregate is an aggregate column of a view. Name is also its DynamoDB attribute.
type ViewAggregate struct {
	Aggregate

	Name string `json:"name"`
}

// AggFunc is an aggregate function, without combinators. Aliases are resolved: uniqHLL12
// is uniq.
type AggFunc string

// Aggregate functions.
const (
	AggCount      AggFunc = "count"
	AggSum        AggFunc = "sum"
	AggMin        AggFunc = "min"
	AggMax        AggFunc = "max"
	AggAvg        AggFunc = "avg"
	AggArgMax     AggFunc = "argMax"
	AggArgMin     AggFunc = "argMin"
	AggAny        AggFunc = "any"
	AggUniq       AggFunc = "uniq"
	AggUniqExact  AggFunc = "uniqExact"
	AggQuantile   AggFunc = "quantile"
	AggQuantileDD AggFunc = "quantileDD"
)

// Idempotent reports whether f has the idempotent state class, where applying an event
// twice changes nothing, rather than the additive one. -If forms share their function's
// class.
func (f AggFunc) Idempotent() bool {
	switch f {
	case AggMin, AggMax, AggArgMax, AggArgMin, AggAny, AggUniq, AggUniqExact:
		return true
	case AggCount, AggSum, AggAvg, AggQuantile, AggQuantileDD:
		return false
	}
	return false
}

// check reports an aggregate whose argument or parameter count doesn't fit its function.
// A merge reads one view column, whatever the function.
func (a *Aggregate) check(merge bool) error {
	var args, params int
	switch a.Fn {
	case AggCount:
	case AggSum, AggMin, AggMax, AggAvg, AggAny, AggUniq, AggUniqExact:
		args = 1
	case AggArgMax, AggArgMin:
		args = 2
	case AggQuantile:
		args, params = 1, 1
	case AggQuantileDD:
		args, params = 1, 2
	default:
		return fmt.Errorf("unknown aggregate function %q", a.Fn)
	}
	if merge {
		args = 1
	}
	if len(a.Args) != args || len(a.Params) != params {
		return fmt.Errorf("%s takes %d arguments and %d parameters, not %d and %d",
			a.Fn, args, params, len(a.Args), len(a.Params))
	}
	return checkExprs(0, slices.Concat(pointers(a.Args), []*Expr{a.Cond})...)
}

// VersionState is the lifecycle state of a view version in dynaflux.lock. The catalog's
// views hold current and retained versions, and [Catalog.Retired] names the retired ones.
type VersionState string

// Version states.
const (
	StateCurrent  VersionState = "current"  // Defined by the SQL files; queries read it.
	StateRetained VersionState = "retained" // An older version that is still maintained.
	StateRetired  VersionState = "retired"  // Removed by dynaflux retire; its number is never reused.
)

func (s VersionState) known() bool {
	switch s {
	case StateCurrent, StateRetained, StateRetired:
		return true
	}
	return false
}

// UnmarshalText implements [encoding.TextUnmarshaler]; it rejects unknown states.
func (s *VersionState) UnmarshalText(text []byte) error {
	return decodeEnum(s, text, "version state")
}

// Mode is how a view version is updated.
type Mode string

// View modes.
const (
	ModeAsync Mode = "async" // By the projector in each region.
	ModeSync  Mode = "sync"  // In the append's transaction, in the writer's region.
)

func (m Mode) known() bool {
	switch m {
	case ModeAsync, ModeSync:
		return true
	}
	return false
}

// UnmarshalText implements [encoding.TextUnmarshaler]; it rejects unknown modes.
func (m *Mode) UnmarshalText(text []byte) error {
	return decodeEnum(m, text, "view mode")
}

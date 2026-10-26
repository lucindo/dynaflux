// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package store is the storage boundary: DynamoDB in production, an in-memory store in
// tests. Its operations mirror DynamoDB requests with a closed set of conditions, so every
// protocol rule lives above it, once.
//
// Items passed to a backend and items it returns belong to the caller: a backend keeps no
// reference to them.
//
// Every backend sorts its errors into the same classes:
//   - a failed condition is a *[ConditionError], which matches [ErrConditionFailed];
//   - [ErrTooLarge] comes from size checks made before a write is sent;
//   - [ErrInvalidRequest] is a request DynamoDB rejects on every attempt: a Dynaflux bug;
//   - every other error is transient: throttling, server errors, a canceled context.
package store

import (
	"context"
	"errors"
	"iter"
)

// Type is the DynamoDB type of a [Value]. Dynaflux never writes NULL or sets.
type Type uint8

// DynamoDB types.
const (
	S Type = iota + 1
	N
	B
	BOOL
	L
	M
)

// Value is a DynamoDB attribute value. Only the field of its Type is read.
type Value struct {
	Type Type
	Str  string           // S, and N as decimal text
	Bin  []byte           // B
	Bool bool             // BOOL
	List []Value          // L
	Map  map[string]Value // M
}

// Item is a DynamoDB item.
type Item map[string]Value

// Key is the primary key of an item: every table has the string keys pk and sk.
type Key struct{ PK, SK string }

// DynamoDB limits, in bytes except for MaxBatchGetKeys.
const (
	MaxItemSize     = 400 << 10 // as [ItemSize] counts it
	MaxPKLen        = 2048
	MaxSKLen        = 1024
	MaxBatchGetKeys = 100
)

// Cond is the condition of a Put. The zero Cond is unconditional.
type Cond struct {
	op      condOp
	version int64
}

type condOp uint8

const (
	condNone condOp = iota
	condAbsent
	condVersion
)

// IfAbsent holds when no item has the key: attribute_not_exists(pk).
func IfAbsent() Cond { return Cond{op: condAbsent} }

// IfVersion holds when the stored item's _v is the number v: _v = v. Versions start at 1.
func IfVersion(v int64) Cond { return Cond{op: condVersion, version: v} }

// Absent reports whether c is [IfAbsent].
func (c Cond) Absent() bool { return c.op == condAbsent }

// Version returns the version of an [IfVersion] condition.
func (c Cond) Version() (int64, bool) { return c.version, c.op == condVersion }

// Query reads one partition in sort-key order.
type Query struct {
	PK string
	// From and To bound the sort key: sk ≥ From and sk ≤ To. An empty bound is open.
	From, To string
	// ToExclusive makes the upper bound sk < To. DynamoDB has no exclusive BETWEEN, so
	// From must be open.
	ToExclusive bool
	Desc        bool
	Limit       int // items in total, across pages; 0 for no limit
	// PageSize is the item limit of the first request; each later request doubles it.
	// 0 leaves pages at DynamoDB's 1 MB.
	PageSize   int
	Consistent bool
}

// Backend is a store. Every call takes the invocation's context.
type Backend interface {
	// Get reads one item, nil when absent.
	Get(ctx context.Context, table string, k Key, consistent bool) (Item, error)
	// BatchGet reads up to [MaxBatchGetKeys] items. Keys may repeat; the result is aligned
	// with ks, nil where an item is absent.
	BatchGet(ctx context.Context, table string, ks []Key, consistent bool) ([]Item, error)
	// Query yields the items of one partition a page at a time. It stops after an error,
	// and sends no request after the caller stops.
	Query(ctx context.Context, table string, q Query) iter.Seq2[Item, error]
	// Put writes a whole item, replacing a stored one, when c holds.
	Put(ctx context.Context, table string, it Item, c Cond) error
}

// Error classes; see the package documentation.
var (
	ErrConditionFailed = errors.New("store: condition failed")
	ErrTooLarge        = errors.New("store: item or key over DynamoDB's size limit")
	ErrInvalidRequest  = errors.New("store: request DynamoDB rejects")
)

// ConditionError is a Put whose condition failed. Current is the stored item, nil when
// absent, so a lost race needs no extra read.
type ConditionError struct {
	Current Item
}

func (*ConditionError) Error() string { return ErrConditionFailed.Error() }

func (*ConditionError) Unwrap() error { return ErrConditionFailed }

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux

import (
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/engine"
)

// Errors to check with [errors.Is]. Returned errors wrap them with details.
var (
	// ErrDuplicate is an event that another append stored before: the same PRIMARY KEY,
	// event_time and DEDUP BY values. The append returns that event's Token.
	ErrDuplicate = engine.ErrDuplicate // "dynaflux: duplicate event"
	// ErrOutsideWindow is an event_time outside the stream's ACCEPT window.
	ErrOutsideWindow = engine.ErrOutsideWindow // "dynaflux: event_time outside the ACCEPT window"
	// ErrInvalidValue is a value that Dynaflux can't store, which a [ValueError] names, an
	// event over DynamoDB's 400 KB item limit, or an event of an archived stream whose
	// archive record could be over Firehose's record limit, which an [ArchiveSizeError]
	// names.
	ErrInvalidValue = engine.ErrInvalidValue // "invalid value"
	// ErrNotFound is a :one query that found no row.
	ErrNotFound = engine.ErrNotFound // "dynaflux: no row found"
	// ErrTooManyKeys is a query call whose IN parameters make more than 100 key
	// combinations to read.
	ErrTooManyKeys = engine.ErrTooManyKeys // "too many key combinations"
	// ErrEvaluation is an expression of a query that failed on the rows it read, such as an
	// integer division by zero, or a sum outside its result type.
	ErrEvaluation = engine.ErrEvaluation // "evaluation error"
)

// ValueError is a value that Dynaflux can't store: invalid UTF-8, a Float64 that isn't
// finite or is outside DynamoDB's range, a Decimal outside its type, a time outside 1970 to
// 2106, an array of more than 1,000 elements, a PRIMARY KEY longer than DynamoDB allows, or
// a missing event_time on a stream with DEDUP BY. Err matches [ErrInvalidValue].
type ValueError struct {
	// Name is the column, as in SQL; for a PRIMARY KEY too long for DynamoDB, the key's
	// columns joined by ", ", such as "org_id, user_id".
	Name string
	Err  error
}

func (e *ValueError) Error() string { return "dynaflux: " + e.Name + ": " + e.Err.Error() }

func (e *ValueError) Unwrap() error { return e.Err }

// ArchiveSizeError is an event of an archived stream whose archive record, with the longest
// shard ID, sequence number and arrival time, could be over Firehose's record limit of
// 1,024,000 bytes, so no projector could archive it. It matches [ErrInvalidValue].
type ArchiveSizeError struct {
	Stream string
	Size   int // the record's size in bytes
}

func (e *ArchiveSizeError) Error() string {
	return (&engine.ArchiveSizeError{Stream: e.Stream, Size: e.Size}).Error()
}

func (*ArchiveSizeError) Unwrap() error { return ErrInvalidValue }

// BatchError is the error of a batch append. Errs lines up with the events, nil for each
// one the call stored.
type BatchError struct {
	Errs []error
}

func (e *BatchError) Error() string {
	var b strings.Builder
	failed := e.Unwrap()
	b.WriteString("dynaflux: " + strconv.Itoa(len(failed)) + " of " + strconv.Itoa(len(e.Errs)) + " events failed")
	for i, err := range e.Errs {
		if err != nil {
			// The event's error starts with the prefix this text already has.
			b.WriteString("; event " + strconv.Itoa(i) + ": " + strings.TrimPrefix(err.Error(), "dynaflux: "))
		}
	}
	return b.String()
}

// Unwrap returns the errors of the events that failed.
func (e *BatchError) Unwrap() []error {
	var failed []error
	for _, err := range e.Errs {
		if err != nil {
			failed = append(failed, err)
		}
	}
	return failed
}

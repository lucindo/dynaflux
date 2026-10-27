// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package meta reads and writes the items of a store's regional meta table
// (docs/spec/encoding.md §8): one unit item per delivery unit, with its progress and
// fences; one view-state item per view version; one job item per backfill job; and the
// deployed catalog.
//
// Every write but a catalog body's is a whole-item read-modify-write conditional on _v, so
// the protocol's rules are checked here, in Go, the same for every backend. A failed
// condition returns the stored item, which the write checks again without another read.
// That also turns a write the SDK retried after a lost response into success: the second
// check finds the write done.
package meta

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// MaxViewStateAge is the longest a projector or a query uses a view state it read.
const MaxViewStateAge = 30 * time.Second

// Bounds are a backfill's time bounds (docs/spec/lifecycle.md §5.1).
type Bounds struct {
	Skew    time.Duration // ε: how far a writer's clock may be from the true time
	Landing time.Duration // L: how long after its ingest_time an event may take to land in the region
	Listing time.Duration // how long after activation [Meta.FenceUnits] waits to list the units
	Clock   time.Duration // how far the clocks of two hosts may differ
}

// DefaultBounds are production's. Listing is the longest a projector uses a view state, plus
// a minute for clock differences and request latency: by then every projector knows the
// version is activating.
//
//nolint:gochecknoglobals,mnd // read-only, as a constant
var DefaultBounds = Bounds{Skew: 5 * time.Minute, Landing: 15 * time.Minute, Listing: MaxViewStateAge + time.Minute, Clock: time.Minute}

func (b Bounds) check() error {
	if b.Skew < 0 || b.Landing < 0 || b.Listing < 0 || b.Clock < 0 {
		return fmt.Errorf("meta: bounds %+v: a bound is negative", b)
	}
	return nil
}

var (
	ErrFenced        = errors.New("meta: a fence exists for a version this batch didn't apply")
	ErrTransition    = errors.New("meta: view state can't move there")
	ErrNotActivating = errors.New("meta: the version isn't activating")
)

// Meta reads and writes one meta table. It is safe for concurrent use.
type Meta struct {
	b     store.Backend
	table string
}

// New returns the meta table named table in b.
func New(b store.Backend, table string) *Meta {
	return &Meta{b: b, table: table}
}

// decodeError names the item that holds something Dynaflux doesn't write.
func (m *Meta) decodeError(it store.Item, err error) error {
	return fmt.Errorf("meta: item %q/%q of table %s: %w", it[catalog.AttrPK].Str, it[catalog.AttrSK].Str, m.table, err)
}

// maxAttempts bounds a read-modify-write. Few writers share an item, so a condition that
// keeps failing means the backend disagrees with the _v it returns.
const maxAttempts = 100

func (m *Meta) errAttempts(k store.Key) error {
	return fmt.Errorf("meta: item %q/%q of table %s: condition failed %d times in a row", k.PK, k.SK, m.table, maxAttempts)
}

// cond is the condition that the stored item still has version v, 0 for no item.
func cond(v int64) store.Cond {
	if v == 0 {
		return store.IfAbsent()
	}
	return store.IfVersion(v)
}

func str(s string) store.Value { return store.Value{Type: store.S, Str: s} }

func num(n int64) store.Value { return store.Value{Type: store.N, Str: strconv.FormatInt(n, 10)} }

// micros returns t in Unix microseconds, rounded up, so that a stored deadline or activation
// time is never earlier than the real one and waits measured from it are never short.
func micros(t time.Time) int64 {
	us := t.UnixMicro()
	if t.Nanosecond()%int(time.Microsecond) != 0 {
		us++
	}
	return us
}

// stamp returns t as an item stores and returns it: rounded up to the microsecond, in UTC.
func stamp(t time.Time) time.Time { return time.UnixMicro(micros(t)).UTC() }

// putTime sets the attribute name to t, leaving a zero t out.
func putTime(it store.Item, name string, t time.Time) {
	if !t.IsZero() {
		it[name] = num(micros(t))
	}
}

// maxErrorBytes bounds a stored error message, which may quote a value of any size.
const maxErrorBytes = 1024

// cutError returns the first 1 KB of s. Dropping invalid UTF-8 also drops a rune that the
// cut splits.
func cutError(s string) string {
	return strings.ToValidUTF8(s[:min(len(s), maxErrorBytes)], "")
}

// isSeq reports whether s is a sequence number in its stored form: 40 digits, zero-padded,
// at least 1.
func isSeq(s string) bool {
	stored, err := keys.PadSeq(s)
	return err == nil && stored == s
}

// ZeroFence is the fence of a unit without progress: every record is above it.
func ZeroFence() string { return strings.Repeat("0", catalog.SequenceWidth) }

func isFence(s string) bool { return isSeq(s) || s == ZeroFence() }

// reader decodes the attributes of an item or map strictly: it keeps the first error, and
// [reader.close] rejects attributes nobody asked for.
type reader struct {
	attrs map[string]store.Value
	asked []string
	err   error
}

func (r *reader) failf(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf(format, args...)
	}
}

// value returns the attribute name and whether it is present with the type t.
func (r *reader) value(name string, t store.Type, required bool) (store.Value, bool) {
	r.asked = append(r.asked, name)
	v, ok := r.attrs[name]
	switch {
	case !ok && required:
		r.failf("%s is missing", name)
	case ok && v.Type != t:
		r.failf("%s has the wrong type", name)
		ok = false
	}
	return v, ok
}

func (r *reader) str(name string) string {
	v, _ := r.value(name, store.S, true)
	return v.Str
}

// text reads a required string that isn't empty.
func (r *reader) text(name string) string {
	v, ok := r.value(name, store.S, true)
	if ok && v.Str == "" {
		r.failf("%s is empty", name)
	}
	return v.Str
}

// seq reads a string that valid accepts, [isSeq] or [isFence]; "" when it is absent.
func (r *reader) seq(name string, required bool, valid func(string) bool) string {
	v, ok := r.value(name, store.S, required)
	if ok && !valid(v.Str) {
		r.failf("%s is %q, not a sequence number in its stored form", name, v.Str)
	}
	return v.Str
}

func (r *reader) integer(name string, required bool) (int64, bool) {
	v, ok := r.value(name, store.N, required)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v.Str, 10, 64)
	if err != nil {
		r.failf("%s is %q, not an integer", name, v.Str)
		return 0, false
	}
	return n, true
}

// count reads a required integer that isn't negative.
func (r *reader) count(name string) int64 {
	n, ok := r.integer(name, true)
	if ok && n < 0 {
		r.failf("%s is %d", name, n)
	}
	return n
}

func (r *reader) boolean(name string) bool {
	v, _ := r.value(name, store.BOOL, true)
	return v.Bool
}

// list reads a list; nil when it is absent.
func (r *reader) list(name string, required bool) []store.Value {
	v, _ := r.value(name, store.L, required)
	return v.List
}

// nested reads the map attribute name with read and a reader of its own, if it is present.
func (r *reader) nested(name string, required bool, read func(*reader)) {
	if v, ok := r.value(name, store.M, required); ok {
		r.inner(name, v, read)
	}
}

// each reads each map of the list attribute name with read and a reader of its own.
func (r *reader) each(name string, required bool, read func(*reader)) {
	for i, v := range r.list(name, required) {
		if v.Type != store.M {
			r.failf("%s[%d] isn't a map", name, i)
			continue
		}
		r.inner(fmt.Sprintf("%s[%d]", name, i), v, read)
	}
}

func (r *reader) inner(name string, v store.Value, read func(*reader)) {
	nr := reader{attrs: v.Map}
	read(&nr)
	if err := nr.close(); err != nil {
		r.failf("%s: %w", name, err)
	}
}

// instant reads a time in Unix microseconds; the zero time when it is absent.
func (r *reader) instant(name string, required bool) time.Time {
	us, ok := r.integer(name, required)
	if !ok {
		return time.Time{}
	}
	return time.UnixMicro(us).UTC()
}

func (r *reader) version() int64 {
	v, ok := r.integer(catalog.AttrRowVersion, true)
	if ok && v < 1 {
		r.failf("%s is %d", catalog.AttrRowVersion, v)
	}
	return v
}

func (r *reader) close() error {
	for _, name := range slices.Sorted(maps.Keys(r.attrs)) {
		if !slices.Contains(r.asked, name) {
			r.failf("unexpected attribute %s", name)
			break
		}
	}
	return r.err
}

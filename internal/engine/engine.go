// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package engine appends to and reads streams, runs named queries and reads view rows over a
// [store.Backend], with the rules of sql.md §4.3, §8 and §10, for every way into a store:
// generated code, the CLI and the query handler.
package engine

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// Errors that callers branch on; package dynaflux exports them. An error's text starts with
// "dynaflux: " once, so only the sentinels that start one carry it.
var (
	ErrDuplicate     = errors.New("dynaflux: duplicate event")
	ErrOutsideWindow = errors.New("dynaflux: event_time outside the ACCEPT window")
	ErrInvalidValue  = errors.New("invalid value")
	ErrNotFound      = errors.New("dynaflux: no row found")
	ErrTooManyKeys   = errors.New("too many key combinations")
	ErrViewNotReady  = errors.New("view version not ready")
	// ErrEvaluation is expr.ErrEval: a query's evaluation errors are those of sql.md §5.8.
	ErrEvaluation = expr.ErrEval
)

// ValueError is a value that sql.md §4.3 rejects. Err matches [ErrInvalidValue].
type ValueError struct {
	// Name is the column, as in SQL; for a PRIMARY KEY too long for DynamoDB, the key's
	// columns joined by ", ".
	Name string
	Err  error
}

func (e *ValueError) Error() string { return "dynaflux: " + e.Name + ": " + e.Err.Error() }

func (e *ValueError) Unwrap() error { return e.Err }

// ArchiveSizeError is an event of an archived stream whose archive record could be over
// Firehose's record limit, so no projector could archive it. It matches [ErrInvalidValue].
type ArchiveSizeError struct {
	Stream string
	// Size is the record's size in bytes with the longest bookkeeping values.
	Size int
}

func (e *ArchiveSizeError) Error() string {
	return fmt.Sprintf("dynaflux: append to %s: %v: the event's archive record can reach %d bytes, over Firehose's record limit of %d",
		e.Stream, ErrInvalidValue, e.Size, archive.MaxRecordSize)
}

func (e *ArchiveSizeError) Unwrap() error { return ErrInvalidValue }

func invalid(name string, err error) error {
	return &ValueError{Name: name, Err: fmt.Errorf("%w: %w", ErrInvalidValue, err)}
}

// Catalog is a store's catalog, compiled once per process.
type Catalog struct {
	def     *catalog.Catalog
	p       *projector.Compiled
	streams map[string]*stream
	queries map[string]*query
}

// Compile decodes a catalog that [catalog.Marshal] wrote and compiles it.
func Compile(data []byte) (*Catalog, error) {
	def, err := catalog.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	p, err := projector.Compile(def)
	if err != nil {
		return nil, err
	}
	c := &Catalog{def: def, p: p, streams: make(map[string]*stream, len(def.Streams)), queries: map[string]*query{}}
	for i := range def.Streams {
		ps, _ := p.Stream(def.Streams[i].Name)
		c.streams[ps.Def.Name] = compileStream(ps)
	}
	for id, vv := range def.Versions() {
		if vv.Mode == catalog.ModeSync {
			c.streams[vv.Stream].sync = append(c.streams[vv.Stream].sync, id)
		}
	}
	for _, s := range c.streams {
		slices.Sort(s.sync)
	}
	for i := range def.Queries {
		q, qerr := c.compileQuery(&def.Queries[i])
		if qerr != nil {
			return nil, fmt.Errorf("query %s: %w", def.Queries[i].Name, qerr)
		}
		c.queries[q.def.Name] = q
	}
	return c, nil
}

// Catalog returns the definitions c was compiled from. The caller must not modify them.
func (c *Catalog) Catalog() *catalog.Catalog { return c.def }

// Projector returns the catalog compiled for the projector.
func (c *Catalog) Projector() *projector.Compiled { return c.p }

func (c *Catalog) stream(name string) (*stream, error) {
	s, ok := c.streams[name]
	if !ok {
		return nil, fmt.Errorf("dynaflux: no stream %s in the catalog", name)
	}
	return s, nil
}

// stream is a compiled stream.
type stream struct {
	*projector.Stream

	key []int // indexes in Columns of the PRIMARY KEY columns
	// idTypes are the types of the values a DEDUP BY event_id hashes: the PRIMARY KEY,
	// event_time, then the DEDUP BY columns. Empty without DEDUP BY.
	idTypes []catalog.Type
	dedup   []int // indexes in Columns of the DEDUP BY columns
	// sync are the IDs of the stream's SYNC versions, sorted.
	sync []string
	// archive encodes the stream's archive records; nil when the stream isn't archived.
	archive *archive.Encoder
}

func compileStream(ps *projector.Stream) *stream {
	s := &stream{Stream: ps}
	if ps.Def.Archive != nil {
		s.archive = archive.NewEncoder(ps.Def)
	}
	index := func(name string) int {
		return slices.IndexFunc(ps.Columns, func(c projector.Column) bool { return c.Name == name })
	}
	for _, name := range ps.Def.PrimaryKey {
		s.key = append(s.key, index(name))
	}
	for _, name := range ps.Def.DedupBy {
		s.dedup = append(s.dedup, index(name))
	}
	if len(s.dedup) > 0 {
		for _, i := range s.key {
			s.idTypes = append(s.idTypes, ps.Columns[i].Type)
		}
		s.idTypes = append(s.idTypes, catalog.EventTimeType())
		for _, i := range s.dedup {
			s.idTypes = append(s.idTypes, ps.Columns[i].Type)
		}
	}
	return s
}

// Options configure an [Engine].
type Options struct {
	// Region is the AWS region the engine runs in, which appends store as origin_region.
	Region string
	// Prefix is the deployment's prefix of AWS resource names, as in [catalog.Naming].
	Prefix string
	// Entropy supplies the random bits of event_ids; nil means crypto/rand.Reader. Engines
	// that write to one store need different sources: two that draw the same bits in one
	// millisecond make the same event_id.
	Entropy io.Reader
}

// Engine appends to and reads the streams of a store. It is safe for concurrent use.
type Engine struct {
	b      store.Backend
	region string
	prefix string
	ulids  *keys.ULIDs

	mu     sync.Mutex
	last   int64                       // the clock reading of the last Append call, in microseconds
	states map[string]*meta.StateCache // by meta table, made on first use
}

// New returns an engine over b. Its event_ids are monotonic: within one millisecond, each
// is one more than the one before, so one engine's appends keep their order.
func New(b store.Backend, o Options) *Engine {
	return &Engine{b: b, region: o.Region, prefix: o.Prefix, ulids: keys.NewULIDs(o.Entropy), states: map[string]*meta.StateCache{}}
}

// now reads the clock for an Append call. Each call gets a later microsecond than the one
// before, so a stored event's ingest_time tells this engine's calls apart.
func (e *Engine) now() int64 {
	now := time.Now().UnixMicro()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.last = max(now, e.last+1)
	return e.last
}

func (e *Engine) naming(cat *Catalog) catalog.Naming {
	return catalog.Naming{Prefix: e.prefix, Store: cat.def.Store}
}

func (e *Engine) table(cat *Catalog, s *stream) string {
	return e.naming(cat).StreamTable(s.Def.Name)
}

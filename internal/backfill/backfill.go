// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package backfill fills one view version in one region (docs/spec/lifecycle.md §5,
// ADR 0011). [Step] does a job's phases until its deadline less a reserve; all state is in
// the job item, the version's state item and the staged runs, so any number of callers may
// step one job, in any process, and a caller that dies loses nothing but its current work.
//
// A job reads its source, the region's archive or its replica of the stream table, into
// sorted runs, merges them in mark order and applies the records its split keeps, chunk by
// chunk, each after the one before succeeded. The cursor moves only
// after a chunk's rows all succeeded, so a worker that starts at the cursor starts at a
// completed prefix, and any number of workers keep S1's invariant (ADR 0011 §3).
package backfill

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// Env is what a step works with in one region.
type Env struct {
	Naming  catalog.Naming
	Catalog *catalog.Catalog // the catalog of the code that steps
	Hash    string           // Catalog's hash, as catalog.Hash gives it
	Meta    *meta.Meta
	Store   store.Backend // reads the region's stream tables
	Core    Applier
	Units   Lister
	Tables  Tables
	Archive Archive // nil where the archive can't be read yet (AWS in M5)
	Runs    Runs
	Lag     Lag
	Bounds  meta.Bounds
	// Memory is the bytes a step may hold in sort buffers and merge blocks.
	Memory int64
	// Chunk is the most records applied at once; 0 means 2,000.
	Chunk int
	// Segments is how many segments a table scan reads in; 0 means 64. DynamoDB Local ignores
	// segments, so a scan of it uses 1.
	Segments int
	// Entropy names staged runs; nil means crypto/rand.Reader. Steps that share a staging
	// prefix need sources that don't repeat each other.
	Entropy io.Reader
	// Now is the host's clock, which a table source reads to wait for its read_after; nil
	// means time.Now.
	Now func() time.Time
	Log *slog.Logger
}

// Applier applies a backfill chunk: [projector.Core.ApplyBackfill].
type Applier interface {
	ApplyBackfill(ctx context.Context, version string, chunk []projector.BackfillRecord) error
}

// Lister lists a stream's delivery units: DescribeStream, every page.
type Lister interface {
	Units(ctx context.Context, streamTable string) ([]string, error)
}

// Tables describes a stream table's replicas.
type Tables interface {
	// Born returns the earliest CreationDateTime over the stream table's replicas, and whether
	// any replica was restored from a backup. Every replica must answer.
	Born(ctx context.Context, streamTable string) (time.Time, bool, error)
}

// Archive reads a region's archive of one stream. M6 reads Athena behind it, and may reshape
// it (m5-plan §17).
type Archive interface {
	// Complete reports whether the archive holds every record put before cut.
	Complete(ctx context.Context, stream string, cut time.Time) (bool, error)
	// Export returns the files that hold the stream's archived records, delivered and in the
	// error prefix, or done false while the export runs.
	Export(ctx context.Context, job *meta.Job) ([]string, bool, error)
	// Open reads a file: one archive record per line, in the form archive.Encoder writes.
	Open(ctx context.Context, file string) (io.ReadCloser, error)
}

// Runs stores a job's staged objects.
type Runs interface {
	// Put stores under name what write writes, replacing any object there.
	Put(ctx context.Context, name string, write func(io.Writer) error) error
	// ReadAt reads n bytes of name from off, or with a negative off its last n bytes. A
	// missing object fails with [ErrStagingMissing].
	ReadAt(ctx context.Context, name string, off, n int64) ([]byte, error)
	DeletePrefix(ctx context.Context, prefix string) error
}

// Lag reports how far behind the region's projector is: the highest IteratorAge of its
// functions over the last minutes.
type Lag interface {
	Lag(ctx context.Context) (time.Duration, error)
}

// ErrStagingMissing is a staged object that is gone, expired or deleted: the job stages again.
var ErrStagingMissing = errors.New("backfill: a staged object is missing")

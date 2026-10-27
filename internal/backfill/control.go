// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"context"
	"errors"
	"fmt"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
)

// Source is where a job reads its version's events.
type Source string

// Sources.
const (
	SourceArchive Source = "archive"
	SourceTable   Source = "table"
)

// SourceOf returns the source that split reads.
func SourceOf(split meta.Split) Source {
	if split == meta.SplitFence {
		return SourceArchive
	}
	return SourceTable
}

// ErrArchiveSource is a request for the archive source where this release can't serve it
// (lifecycle.md §5.2, DF7503).
var ErrArchiveSource = errors.New("backfill: the archive source isn't available")

// ErrJobEnded is a cancel or a rate change of a job that is done or broken.
var ErrJobEnded = errors.New("backfill: the job has ended")

// Choose returns the split of a new job of version in region, whose deployed catalog is cat
// (lifecycle.md §5.2). An additive version splits by fences, reading the archive, when the
// stream is fresh, as CheckStream reports it, and the region archives it with events: all:
// the fresh rule then most likely makes the version live without a read. Otherwise it reads
// the table, split by time for an additive version and not split for an idempotent one,
// which goes live as soon from an empty table and needs no archive when an event comes
// first. want "" chooses so; SourceTable takes the table; SourceArchive takes the archive
// where it can serve, and otherwise fails with ErrArchiveSource, naming the condition that
// failed.
func Choose(cat *catalog.Catalog, region, version string, fresh bool, want Source) (meta.Split, error) {
	vv, ok := cat.Version(version)
	if !ok {
		return "", fmt.Errorf("backfill: %s isn't in the catalog", version)
	}
	archived := cat.ExactHandoff(region, vv.Stream)
	switch {
	case fresh && archived && (want == SourceArchive || want == "" && vv.FencedHandoff()):
		return meta.SplitFence, nil
	case want == SourceArchive && !archived:
		return "", fmt.Errorf("%w: %s doesn't archive stream %s with events: all", ErrArchiveSource, region, vv.Stream)
	case want == SourceArchive:
		return "", fmt.Errorf("%w: stream %s isn't fresh in %s, and this release reads the archive only of a stream without events",
			ErrArchiveSource, vv.Stream, region)
	case vv.FencedHandoff():
		return meta.SplitTime, nil
	}
	return meta.SplitNone, nil
}

// Cancel stops a version's running job: a step that finds it cancelled ends, and the version
// stays activating. A cancelled job stays so; one that is done or broken fails with
// ErrJobEnded.
func Cancel(ctx context.Context, m *meta.Meta, version string) (meta.Job, error) {
	return m.UpdateJob(ctx, version, func(j *meta.Job) (bool, error) {
		switch j.State {
		case meta.JobCancelled:
			return false, nil
		case meta.JobDone, meta.JobBroken:
			return false, fmt.Errorf("backfill: cancel %s: its job is %s: %w", version, j.State, ErrJobEnded)
		case meta.JobRunning:
		}
		j.State = meta.JobCancelled
		return true, nil
	})
}

// SetRate sets the records a second that a version's job applies, 0 for no limit, from its
// next chunk. A job that is done or broken fails with ErrJobEnded.
func SetRate(ctx context.Context, m *meta.Meta, version string, rate int) error {
	if rate < 0 {
		return fmt.Errorf("backfill: rate %d for %s", rate, version)
	}
	_, err := m.UpdateJob(ctx, version, func(j *meta.Job) (bool, error) {
		if j.State == meta.JobDone || j.State == meta.JobBroken {
			return false, fmt.Errorf("backfill: set the rate of %s: its job is %s: %w", version, j.State, ErrJobEnded)
		}
		write := j.Rate != rate
		j.Rate = rate
		return write, nil
	})
	return err
}

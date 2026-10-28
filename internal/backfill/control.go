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

// ErrJobEnded is a cancel or a rate change of a job that ended.
var ErrJobEnded = errors.New("backfill: the job has ended")

// Choose returns the split of a new job of version in region, whose deployed catalog is cat,
// given v, the completeness of the region's archive of the version's stream (lifecycle.md
// §5.2): the fence split, reading the archive, whenever it is complete. Otherwise it reads the
// table, split by time for an additive version and not split for an idempotent one. want ""
// chooses so; SourceTable takes the table; SourceArchive takes the archive, and fails with
// ErrIncomplete, naming the rule that failed, when it isn't complete.
func Choose(cat *catalog.Catalog, region, version string, v Verdict, want Source) (meta.Split, error) {
	vv, ok := cat.Version(version)
	switch {
	case !ok:
		return "", fmt.Errorf("backfill: %s isn't in the catalog", version)
	case v.Complete && want != SourceTable:
		return meta.SplitFence, nil
	case want == SourceArchive:
		return "", fmt.Errorf("%w: %s in %s: rule %d: %s", ErrIncomplete, vv.Stream, region, v.Rule, v.Reason)
	case vv.FencedHandoff():
		return meta.SplitTime, nil
	}
	return meta.SplitNone, nil
}

// Cancel stops a version's running job: a step that finds it cancelled ends, and the version
// stays activating. A cancelled job stays so; one that ended fails with ErrJobEnded.
func Cancel(ctx context.Context, m *meta.Meta, version string) (meta.Job, error) {
	return m.UpdateJob(ctx, version, func(j *meta.Job) (bool, error) {
		switch j.State {
		case meta.JobCancelled:
			return false, nil
		case meta.JobDone, meta.JobBroken, meta.JobFailed:
			return false, fmt.Errorf("backfill: cancel %s: its job is %s: %w", version, j.State, ErrJobEnded)
		case meta.JobRunning:
		}
		j.State = meta.JobCancelled
		return true, nil
	})
}

// SetRate sets the records a second that a version's job applies, 0 for no limit, from its
// next chunk. A job that ended fails with ErrJobEnded.
func SetRate(ctx context.Context, m *meta.Meta, version string, rate int) error {
	if rate < 0 {
		return fmt.Errorf("backfill: rate %d for %s", rate, version)
	}
	_, err := m.UpdateJob(ctx, version, func(j *meta.Job) (bool, error) {
		if j.State.Ended() {
			return false, fmt.Errorf("backfill: set the rate of %s: its job is %s: %w", version, j.State, ErrJobEnded)
		}
		write := j.Rate != rate
		j.Rate = rate
		return write, nil
	})
	return err
}

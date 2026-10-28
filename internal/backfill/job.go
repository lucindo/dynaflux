// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/meta"
)

// Request asks for a version's job.
type Request struct {
	Version, Stream string
	Name            string // the job's execution name, without -a<attempt>
	// Execution names the execution that will step an attempt: its ARN, or "local".
	Execution func(attempt int) string
	Split     meta.Split // for a version that isn't activating yet
	Rate      int        // records a second, 0 for no limit; a new job's
}

// LocalExecution names the execution of every attempt "local": a process that steps the job
// itself.
func LocalExecution(int) string { return "local" }

// Start creates a version's job, or claims a running or cancelled one for its next attempt,
// and returns it. A version already activating keeps the split of its state item, so another
// r.Split fails with meta.ErrTransition, as does a version that is live, broken or retired.
// A job takes the split asked for until its version is activated; after that its split is
// fixed. A job that is done or broken fails with meta.ErrJobExists. A caller that loses the
// race to create or claim the job gets the winner's attempt.
func Start(ctx context.Context, m *meta.Meta, r Request) (meta.Job, error) {
	if r.Version == "" || r.Stream == "" || r.Name == "" || r.Execution == nil {
		return meta.Job{}, fmt.Errorf("backfill: start %q of stream %q needs a name and an execution", r.Version, r.Stream)
	}
	states, err := m.ViewStates(ctx)
	if err != nil {
		return meta.Job{}, err
	}
	split := r.Split
	switch vs, ok := states[r.Version]; {
	case !ok:
	case vs.State == meta.Activating && (split == "" || split == vs.Split):
		split = vs.Split
	default:
		return meta.Job{}, fmt.Errorf("backfill: start %s with the %s split: it is %s with the %s split: %w", r.Version, r.Split, vs.State, vs.Split, meta.ErrTransition)
	}
	j, ok, err := m.Job(ctx, r.Version)
	if err != nil {
		return meta.Job{}, err
	}
	if !ok {
		now := time.Now()
		j, err = m.CreateJob(ctx, meta.Job{
			Version:   r.Version,
			Stream:    r.Stream,
			Split:     split,
			State:     meta.JobRunning,
			Phase:     meta.PhaseActivate,
			Attempt:   1,
			Name:      r.Name,
			Execution: r.Execution(1),
			CreatedAt: now,
			Staging:   "backfill/" + r.Version + "/" + strconv.FormatInt(now.UnixMicro(), 10) + "/",
			Rate:      r.Rate,
		})
		if !errors.Is(err, meta.ErrJobExists) {
			return j, err
		}
		// Steps of one attempt may run at once, so the loser steps the winner's.
		if j, _, err = m.Job(ctx, r.Version); err != nil {
			return meta.Job{}, err
		}
		return j, checkSplit(j, split)
	}
	next := j.Attempt + 1
	return m.UpdateJob(ctx, r.Version, func(j *meta.Job) (bool, error) {
		switch {
		case j.State != meta.JobRunning && j.State != meta.JobCancelled:
			return false, fmt.Errorf("backfill: start %s: its job is %s: %w", r.Version, j.State, meta.ErrJobExists)
		case j.Phase != meta.PhaseActivate && checkSplit(*j, split) != nil:
			return false, checkSplit(*j, split)
		case j.Attempt >= next:
			// Claimed: by this call, retried after a lost response, or by another.
			return false, nil
		}
		if split != "" {
			// Not activated yet: nothing the job did depends on its split.
			j.Split = split
		}
		j.State, j.Attempt, j.Execution = meta.JobRunning, next, r.Execution(next)
		return true, nil
	})
}

// checkSplit fails with meta.ErrTransition when split is set and isn't j's.
func checkSplit(j meta.Job, split meta.Split) error {
	if split != "" && split != j.Split {
		return fmt.Errorf("backfill: start %s with the %s split: its job has the %s split: %w", j.Version, split, j.Split, meta.ErrTransition)
	}
	return nil
}

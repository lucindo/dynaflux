// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/lucindo/dynaflux/internal/meta"
)

// Apply pauses while the region's projector is more than lagPause behind, and a paused job
// resumes below lagResume: a new view table takes about 4,000 writes a second, and the
// projector writes to it too (lifecycle.md §5.4).
const (
	lagPause  = 5 * time.Minute
	lagResume = lagPause / 2
	lagEvery  = time.Minute // between a step's lag checks
	lagPoll   = time.Minute
)

// lagged reports whether apply pauses for the projector's lag: above lagPause, or above
// lagResume while j is paused. The pause is stored in the job, so it holds across steps.
func (s *step) lagged(ctx context.Context, j meta.Job) (bool, error) {
	if !s.lagAt.IsZero() && time.Since(s.lagAt) < lagEvery {
		return false, nil
	}
	lag, err := s.env.Lag.Lag(ctx)
	if err != nil {
		return false, fmt.Errorf("backfill: read the projector's lag: %w", err)
	}
	s.lagAt = time.Now()
	limit := lagPause
	if j.Paused {
		limit = lagResume
	}
	paused := lag > limit
	if paused != j.Paused {
		_, err = s.update(ctx, meta.PhaseApply, func(j *meta.Job) (bool, error) {
			write := j.Paused != paused
			j.Paused = paused
			return write, nil
		})
	}
	return paused, err
}

// pacer spaces a step's chunks so that it applies at most a rate of records a second.
type pacer struct{ next time.Time }

// take books n records at rate, 0 for no limit, and returns how long to wait before they go.
func (p *pacer) take(rate, n int) time.Duration {
	if rate <= 0 {
		return 0
	}
	now := time.Now()
	if p.next.Before(now) {
		p.next = now
	}
	wait := p.next.Sub(now)
	p.next = p.next.Add(time.Duration(n) * time.Second / time.Duration(rate))
	return wait
}

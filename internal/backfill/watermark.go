// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"time"

	"github.com/lucindo/dynaflux/internal/meta"
)

// Freshness is what CloudWatch shows of a stream's Firehose delivery stream, for the archive's
// watermark (ADR 0016 §2).
type Freshness struct {
	// Points are the AWS/Firehose DeliveryToIceberg.DataFreshness datapoints, Maximum, from
	// before the cut to now.
	Points []Datapoint
	// Active and Listed are the evidence that silence means an empty Firehose: the delivery
	// stream is ACTIVE, and ListMetrics lists the metric for it. A wrong name, dimension or
	// region returns no datapoints rather than an error.
	Active, Listed bool
}

// Datapoint is a period's DataFreshness: the age of the oldest record in Firehose.
type Datapoint struct {
	At        time.Time // the period's start, which CloudWatch stamps it with
	Freshness time.Duration
}

// IdleAfter is how long past the cut the idle rule waits: Firehose's 7,200 s of retries, its
// 900 s buffer, and 15 minutes for the last retry and its error object.
const IdleAfter = 2*time.Hour + 30*time.Minute

// The idle rule then needs no datapoint for periods starting in [now − idleSince,
// now − idleUntil].
const (
	idleSince = 20 * time.Minute
	idleUntil = 5 * time.Minute
)

// Delivered reports whether f shows at now that Firehose delivered every record it accepted
// before cut: some datapoint at period start t with freshness d has t − d past cut by more
// than the clock margin, so everything older than t − d had left Firehose by t. Or, past
// IdleAfter, Firehose is empty now and the evidence says the metric is the right one.
func Delivered(f Freshness, cut, now time.Time) bool {
	margin := meta.DefaultBounds.Clock
	recent := false
	for _, p := range f.Points {
		if p.At.Add(-p.Freshness).After(cut.Add(margin)) {
			return true
		}
		recent = recent || !p.At.Before(now.Add(-idleSince)) && !p.At.After(now.Add(-idleUntil))
	}
	return f.Active && f.Listed && now.After(cut.Add(IdleAfter)) && !recent
}

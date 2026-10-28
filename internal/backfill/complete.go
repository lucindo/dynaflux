// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
)

// ErrIncomplete is a region's archive of a stream that may miss events (ADR 0017): a job
// split by fences fails on it, and its version needs a rebuild from the table.
var ErrIncomplete = errors.New("backfill: the archive isn't complete")

// Witness reads what AWS shows of a stream's history in the region (ADR 0017 §4).
type Witness interface {
	// StreamARN returns the ARN of streamTable's change stream in the region.
	StreamARN(ctx context.Context, streamTable string) (string, error)
	// Facts returns what AWS shows of streamTable since the origin of its stream item, which
	// has one: projector activity from the item's CheckedThrough on.
	Facts(ctx context.Context, streamTable string, item meta.StreamItem) (Facts, error)
}

// Facts are a stream's gap evidence that AWS holds.
type Facts struct {
	ARN     string // the ARN of the stream's change stream in the region now
	Mapping bool   // an event source mapping reads it, or soon will from its checkpoint
	Gaps    []Gap  // batches of it that Lambda discarded
	// Silent is the first hour since the origin with writes to the stream table and no
	// invocation of its projector that succeeded in it or the next 22; zero when there is none.
	Silent time.Time
	// Checked is the end of the hours whose writes the activity check covered, which it
	// needn't check again; zero when it covered none.
	Checked time.Time
	// Unchecked is set when hours since the origin before it went unchecked: CloudWatch keeps
	// no hourly data so old, and no earlier check covered them, now or when the stream item
	// recorded so.
	Unchecked time.Time
}

// Gap is a batch of a stream's records that Lambda discarded (lifecycle.md §7.2).
type Gap struct {
	Object string    // its on-failure record
	At     time.Time // when its first record arrived
}

// Evidence is what the completeness rules read.
type Evidence struct {
	Facts

	Stream meta.StreamItem
	Exact  bool // the region's deployed catalog archives every event of the stream there
}

// The rules of [Completeness].
const (
	RuleOrigin = 1 // a valid origin, under a catalog that archives every event
	RuleHole   = 2 // no change of the archive mode since the origin
	RuleGap    = 3 // no gap evidence since the origin
)

// Verdict is whether a region's archive of a stream is complete, or the first rule it fails.
type Verdict struct {
	Complete bool
	Rule     int
	At       time.Time // the hole's or the evidence's time, when it has one
	Reason   string
	// Suspect is evidence that the views applied live may miss records too: a lag, an hour
	// of writes that no projector followed, or no mapping. A batch that Lambda discarded is a
	// gap found, not suspected.
	Suspect bool
}

// Completeness evaluates ADR 0017's rules on e. Holes count by their place after the
// origin, never by their times.
func Completeness(e Evidence) Verdict {
	o := e.Stream.Origin
	switch {
	case o == nil:
		return Verdict{Rule: RuleOrigin, Reason: "no origin was recorded"}
	case !e.Exact:
		return Verdict{Rule: RuleOrigin, Reason: "the deployed catalog doesn't archive every event of the stream here"}
	case o.Void(e.ARN):
		return Verdict{Rule: RuleOrigin, Reason: "the stream was re-created after the origin: it is " + cmp.Or(e.ARN, "off") + " now"}
	}
	for _, h := range e.Stream.After() {
		switch h.Cause {
		case meta.HoleUpdate, meta.HoleCreate:
			return Verdict{Rule: RuleHole, At: h.At, Reason: fmt.Sprintf("a deploy changed its archive mode from %s to %s at %s", h.From, h.To, rfc3339(h.At))}
		case meta.HoleLag:
			return lagged(h)
		case meta.HoleErrors:
			return Verdict{Rule: RuleGap, At: h.At, Reason: "the error prefix holds " + h.Object + ", which isn't Dynaflux records"}
		}
	}
	switch {
	case e.Stream.Mode != meta.ArchiveAll:
		// Only an origin written while a deploy changed the mode, which RecordOrigin refuses.
		return Verdict{Rule: RuleOrigin, Reason: "the last deploy set its archive mode to " + string(e.Stream.Mode)}
	case len(e.Gaps) > 0:
		g := e.Gaps[0]
		return Verdict{Rule: RuleGap, At: g.At, Reason: "Lambda discarded a batch of records arrived from " + rfc3339(g.At) + " (" + g.Object + ")"}
	case !e.Silent.IsZero():
		return silent(e.Silent)
	case !e.Mapping:
		return unmapped()
	}
	return Verdict{Complete: true}
}

// Suspected returns the evidence in e that the views applied live may miss records of the
// stream (lifecycle.md §7.2), whatever the archive: every lag hole, before the origin too,
// since its views may still miss what was trimmed then; and with an origin, which AWS's facts
// need, an hour of writes that no projector followed, and no mapping that reads the stream.
func Suspected(e Evidence) []Verdict {
	var out []Verdict
	for _, h := range e.Stream.Holes {
		if h.Cause == meta.HoleLag {
			out = append(out, lagged(h))
		}
	}
	if e.Stream.Origin == nil {
		return out
	}
	if !e.Silent.IsZero() {
		out = append(out, silent(e.Silent))
	}
	if !e.Mapping {
		out = append(out, unmapped())
	}
	return out
}

func lagged(h meta.Hole) Verdict {
	return Verdict{Rule: RuleGap, At: h.At, Reason: "a projector handled a record almost 24 hours old at " + rfc3339(h.At), Suspect: true}
}

func silent(hour time.Time) Verdict {
	return Verdict{Rule: RuleGap, At: hour, Reason: "no projector invocation succeeded within 23 hours of the writes of the hour from " + rfc3339(hour), Suspect: true}
}

func unmapped() Verdict {
	return Verdict{Rule: RuleGap, Reason: "no event source mapping reads the stream", Suspect: true}
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// Gather reads, without writing, the evidence of the region's archive of stream: the stream
// item and the deployed catalog from the meta table, and, when there is an origin, the facts
// AWS holds.
func Gather(ctx context.Context, env *Env, stream string) (Evidence, error) {
	item, _, err := env.Meta.Stream(ctx, stream)
	if err != nil {
		return Evidence{}, err
	}
	e := Evidence{Stream: item}
	if e.Exact, err = exact(ctx, env, stream); err != nil {
		return Evidence{}, err
	}
	if item.Origin == nil {
		return e, nil
	}
	table := env.Naming.StreamTable(stream)
	if e.Facts, err = env.Witness.Facts(ctx, table, item); err != nil {
		return Evidence{}, fmt.Errorf("backfill: the history of %s: %w", table, err)
	}
	return e, nil
}

// exact reports whether the region's deployed catalog archives every event of stream there.
// A region without one archives nothing yet.
func exact(ctx context.Context, env *Env, stream string) (bool, error) {
	_, cat, ok, err := deployedCatalog(ctx, env)
	return ok && cat.ExactHandoff(env.Region, stream), err
}

// deployedCatalog returns the region's current catalog and its registration, and whether one
// is registered.
func deployedCatalog(ctx context.Context, env *Env) (meta.Registered, *catalog.Catalog, bool, error) {
	reg, ok, err := env.Meta.Current(ctx)
	if err != nil || !ok || reg.Hash == env.Hash {
		return reg, env.Catalog, ok, err
	}
	data, err := env.Meta.ReadCatalog(ctx, reg.Hash)
	if err != nil {
		return reg, nil, false, err
	}
	cat, err := catalog.Unmarshal(data)
	if err != nil {
		return reg, nil, false, fmt.Errorf("backfill: the region's deployed catalog: %w", err)
	}
	return reg, cat, true, nil
}

// checkArchive fails with ErrIncomplete when the region's archive of the job's stream may
// miss events.
func (s *step) checkArchive(ctx context.Context, j meta.Job) error {
	e, err := Gather(ctx, s.env, j.Stream)
	if err != nil {
		return err
	}
	// Later checks start where this one passed, so no check needs CloudWatch's hourly data
	// past its 455 days while backfills keep running.
	if e.Silent.IsZero() && e.Checked.After(e.Stream.CheckedThrough) {
		if err = s.env.Meta.SetChecked(ctx, j.Stream, e.Checked, e.Unchecked); err != nil {
			return err
		}
	}
	if v := Completeness(e); !v.Complete {
		return fmt.Errorf("%w: %s in %s: rule %d: %s", ErrIncomplete, j.Stream, s.env.Region, v.Rule, v.Reason)
	}
	return nil
}

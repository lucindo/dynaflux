// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
)

// Every rule, and a hole in each place: before the origin, or after it, whatever the clocks
// of their writers say.
func TestCompleteness(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	born := t0.Add(-time.Hour)
	off := meta.Hole{At: t0.Add(time.Minute), Cause: meta.HoleUpdate, From: meta.ArchiveAll, To: meta.ArchiveOff}
	on := meta.Hole{At: t0.Add(2 * time.Minute), Cause: meta.HoleUpdate, From: meta.ArchiveOff, To: meta.ArchiveAll}
	lag := meta.Hole{At: t0.Add(time.Hour), Cause: meta.HoleLag}
	errs := meta.Hole{At: t0.Add(time.Hour), Cause: meta.HoleErrors, Object: "errors/firehose/events/x.gz"}
	origin := func(after int) *meta.Origin {
		return &meta.Origin{At: t0, Hash: "h", RegisteredAt: t0, Born: born, ARN: streamARN, Holes: after}
	}
	facts := backfill.Facts{ARN: streamARN, Mapping: true}
	evidence := func(o *meta.Origin, holes ...meta.Hole) backfill.Evidence {
		return backfill.Evidence{
			Stream: meta.StreamItem{Stream: stream, Mode: meta.ArchiveAll, Origin: o, Holes: holes},
			Exact:  true, Facts: facts,
		}
	}
	change := func(e backfill.Evidence, f func(*backfill.Evidence)) backfill.Evidence { f(&e); return e }
	tests := []struct {
		name   string
		e      backfill.Evidence
		rule   int
		reason string
	}{
		{"complete", evidence(origin(0)), 0, ""},
		{"holes before the origin", evidence(origin(2), off, on), 0, ""},
		{"a hole before the origin by a later clock", evidence(origin(1), meta.Hole{At: t0.Add(time.Hour), Cause: meta.HoleCreate, From: meta.ArchiveOff, To: meta.ArchiveAll}), 0, ""},
		{"no origin", evidence(nil), backfill.RuleOrigin, "no origin was recorded"},
		{"a catalog that doesn't archive every event", change(evidence(origin(0)), func(e *backfill.Evidence) { e.Exact = false }),
			backfill.RuleOrigin, "the deployed catalog doesn't archive every event of the stream here"},
		{"an origin written while a deploy stopped archiving", change(evidence(origin(1), off), func(e *backfill.Evidence) { e.Stream.Mode = meta.ArchiveOff }),
			backfill.RuleOrigin, "the last deploy set its archive mode to off"},
		{"a re-created stream", change(evidence(origin(0)), func(e *backfill.Evidence) { e.ARN = "arn:new" }),
			backfill.RuleOrigin, "the stream was re-created after the origin: it is arn:new now"},
		{"a stream turned off", change(evidence(origin(0)), func(e *backfill.Evidence) { e.ARN = "" }),
			backfill.RuleOrigin, "the stream was re-created after the origin: it is off now"},
		{"a mode change after the origin", evidence(origin(0), off, on), backfill.RuleHole, "a deploy changed its archive mode from all to off at 2026-10-01T12:01:00Z"},
		{"a mode change after the origin by an earlier clock", evidence(origin(0), meta.Hole{At: t0.Add(-time.Minute), Cause: meta.HoleUpdate, From: meta.ArchiveAll, To: meta.ArchiveLocal}),
			backfill.RuleHole, "a deploy changed its archive mode from all to local at 2026-10-01T11:59:00Z"},
		{"a lag", evidence(origin(1), off, lag), backfill.RuleGap, "a projector handled a record almost 24 hours old at 2026-10-01T13:00:00Z"},
		{"an unreadable error object", evidence(origin(0), errs), backfill.RuleGap, "the error prefix holds errors/firehose/events/x.gz, which isn't Dynaflux records"},
		{"a discarded batch", change(evidence(origin(0)), func(e *backfill.Evidence) {
			e.Gaps = []backfill.Gap{{Object: "aws/lambda/x", At: t0.Add(3 * time.Hour)}}
		}), backfill.RuleGap, "Lambda discarded a batch of records arrived from 2026-10-01T15:00:00Z (aws/lambda/x)"},
		{"an hour of writes no projector followed", change(evidence(origin(0)), func(e *backfill.Evidence) { e.Silent = t0.Add(5 * time.Hour) }),
			backfill.RuleGap, "no projector invocation succeeded within 23 hours of the writes of the hour from 2026-10-01T17:00:00Z"},
		{"no mapping", change(evidence(origin(0)), func(e *backfill.Evidence) { e.Mapping = false }),
			backfill.RuleGap, "no event source mapping reads the stream"},
	}
	// The views applied live may miss records too.
	suspects := []string{"a lag", "an hour of writes no projector followed", "no mapping"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := backfill.Completeness(tt.e)
			suspect := slices.Contains(suspects, tt.name)
			if v.Complete != (tt.rule == 0) || v.Rule != tt.rule || v.Reason != tt.reason || v.Suspect != suspect {
				t.Errorf("Completeness = %+v, want rule %d: %q, suspect %t", v, tt.rule, tt.reason, suspect)
			}
		})
	}
}

// Every lag is a suspected gap in the views applied live, wherever it sits and with or
// without an origin; AWS's facts, which need an origin, add an hour no projector followed and a
// missing mapping. A mode change suspects nothing of the views.
func TestSuspected(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	off := meta.Hole{At: t0.Add(time.Minute), Cause: meta.HoleUpdate, From: meta.ArchiveAll, To: meta.ArchiveOff}
	lag := meta.Hole{At: t0.Add(time.Hour), Cause: meta.HoleLag}
	o := func(after int) *meta.Origin { return &meta.Origin{At: t0, ARN: streamARN, Holes: after} }
	lagged := "a projector handled a record almost 24 hours old at 2026-10-01T13:00:00Z"
	tests := []struct {
		name  string
		o     *meta.Origin
		holes []meta.Hole
		facts backfill.Facts
		want  []string
	}{
		{"none", o(0), nil, backfill.Facts{ARN: streamARN, Mapping: true}, nil},
		{"a lag without an origin", nil, []meta.Hole{lag}, backfill.Facts{}, []string{lagged}},
		{"a lag before the origin", o(1), []meta.Hole{lag}, backfill.Facts{ARN: streamARN, Mapping: true}, []string{lagged}},
		{"a lag after a mode change", o(0), []meta.Hole{off, lag}, backfill.Facts{ARN: streamARN, Mapping: true}, []string{lagged}},
		{"facts", o(0), nil, backfill.Facts{ARN: streamARN, Silent: t0.Add(2 * time.Hour)}, []string{
			"no projector invocation succeeded within 23 hours of the writes of the hour from 2026-10-01T14:00:00Z",
			"no event source mapping reads the stream",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := backfill.Evidence{Stream: meta.StreamItem{Stream: stream, Mode: meta.ArchiveAll, Origin: tt.o, Holes: tt.holes}, Exact: true, Facts: tt.facts}
			var got []string
			for _, v := range backfill.Suspected(e) {
				if !v.Suspect || v.Complete {
					t.Errorf("verdict %+v, want a suspected gap", v)
				}
				got = append(got, v.Reason)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("Suspected = %q, want %q", got, tt.want)
			}
		})
	}
}

// stopArchive records, as a deploy does before its functions change, that the region stopped
// archiving the stream.
func (f *fixture) stopArchive(ctx context.Context) {
	f.t.Helper()
	err := f.m.ApplyArchiveModes(ctx, map[string]meta.ArchiveMode{stream: meta.ArchiveAll}, map[string]meta.ArchiveMode{stream: meta.ArchiveOff}, false, time.Now())
	if err != nil {
		f.t.Fatal(err)
	}
}

// After its last read of the archive, before it applies a record, a job checks that the
// archive is still complete; it fails naming the rule, and its version stays activating.
func TestStepChecksTheArchiveAfterItsLastRead(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		during func(f *fixture)
		want   string // the error's end; "" for none
	}{
		{"complete", func(*fixture) {}, ""},
		{"a deploy stopped archiving", func(f *fixture) { f.stopArchive(f.t.Context()) }, "rule 2: a deploy changed its archive mode from all to off"},
		{"a lag", func(f *fixture) {
			if err := f.m.MarkLag(f.t.Context(), stream, time.Now()); err != nil {
				f.t.Fatal(err)
			}
		}, "rule 3: a projector handled a record almost 24 hours old"},
		{"no mapping", func(f *fixture) { f.witness.set(backfill.Facts{ARN: streamARN}) }, "rule 3: no event source mapping reads the stream"},
		{"a re-created stream", func(f *fixture) { f.witness.set(backfill.Facts{ARN: "arn:new", Mapping: true}) }, "rule 1: the stream was re-created"},
		// The replicas' earliest creation changes when the oldest region leaves: the region's
		// stream, and so its archive, goes on.
		{"the oldest replica removed", func(f *fixture) { f.tables.born = time.Now() }, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.units.set("u1")
				f.progress("u1", 1)
				f.record("delivered", "u1", 1, "a", 10)
				f.archive.onOpen = func(string) { tt.during(f) }
				f.start()
				f.run()
				j, state, rows := f.job(), f.state().State, f.rows("a")
				if tt.want == "" {
					if j.State != meta.JobDone || state != meta.Live || rows[0] != "a 1 10" {
						t.Errorf("job %s, version %s, rows %q; want done, live and a 1 10", j.State, state, rows)
					}
					return
				}
				if j.State != meta.JobBroken || j.Error == nil || j.Error.Phase != meta.PhaseStage || state != meta.Activating || rows[0] != "a -" ||
					!strings.HasPrefix(j.Error.Message, backfill.ErrIncomplete.Error()+": events in us-east-1: "+tt.want) {
					t.Errorf("job %s with %+v, version %s, rows %q; want broken in stage by %q, activating, no row", j.State, j.Error, state, rows, tt.want)
				}
			})
		})
	}
}

// deploying records a hole before it applies the first chunk: a deploy stopped archiving
// while the job applied.
type deploying struct {
	backfill.Applier

	f *fixture
}

func (d deploying) ApplyBackfill(ctx context.Context, version string, chunk []projector.BackfillRecord) error {
	d.f.stopArchive(ctx)
	return d.Applier.ApplyBackfill(ctx, version, chunk)
}

// A job checks the archive again before it makes its version live.
func TestStepChecksTheArchiveBeforeLive(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.units.set("u1")
		f.progress("u1", 1)
		f.record("delivered", "u1", 1, "a", 10)
		f.env.Core = deploying{Applier: f.env.Core, f: f}
		f.start()
		f.run()
		if j, state := f.job(), f.state().State; j.State != meta.JobBroken || j.Error == nil || j.Error.Phase != meta.PhaseLive || state != meta.Activating {
			t.Errorf("job %s with %+v, version %s; want broken in live, activating", j.State, j.Error, state)
		}
	})
}

// A job records how far an activity check that passed reached, so later checks start there,
// and the hours it left unchecked; a check that found a silent hour records nothing.
func TestStepRecordsTheActivityCheck(t *testing.T) {
	t.Parallel()
	checked := time.Date(1999, 12, 30, 5, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name            string
		facts           backfill.Facts
		want, unchecked time.Time
	}{
		{"passed", backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked}, checked, time.Time{}},
		{"passed past CloudWatch's hourly data", backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked, Unchecked: checked.Add(-time.Hour)},
			checked, checked.Add(-time.Hour)},
		{"silent", backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked, Silent: checked.Add(-time.Hour)}, time.Time{}, time.Time{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.units.set("u1")
				f.progress("u1", 1)
				f.record("delivered", "u1", 1, "a", 10)
				f.witness.set(tt.facts)
				f.start()
				f.run()
				item, _, err := f.m.Stream(t.Context(), stream)
				if err != nil || !item.CheckedThrough.Equal(tt.want) || !item.Unchecked.Equal(tt.unchecked) {
					t.Errorf("checked through %v, unchecked before %v, %v; want %v and %v", item.CheckedThrough, item.Unchecked, err, tt.want, tt.unchecked)
				}
			})
		})
	}
}

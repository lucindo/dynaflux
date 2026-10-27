// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

func activate(t *testing.T, m *meta.Meta) {
	t.Helper()
	if _, err := m.Activate(t.Context(), version, stream, meta.SplitFence, meta.DefaultBounds); err != nil {
		t.Fatalf("Activate: %v", err)
	}
}

func makeLive(t *testing.T, m *meta.Meta) {
	t.Helper()
	activate(t, m)
	if err := m.MakeLive(t.Context(), version, time.Time{}); err != nil {
		t.Fatalf("MakeLive: %v", err)
	}
}

// failure is the failure markBroken records.
func failure() meta.Failure {
	return meta.Failure{Cause: meta.CauseRecord, Unit: "u1", Seq: seq(1), EventID: "e", Error: "boom", At: bubbleStart}
}

// markBroken breaks the version at sequence number 1 of unit u1.
func markBroken(t *testing.T, m *meta.Meta) {
	t.Helper()
	if _, _, err := m.MarkBroken(t.Context(), version, stream, failure()); err != nil {
		t.Fatalf("MarkBroken: %v", err)
	}
}

func retire(t *testing.T, m *meta.Meta) {
	t.Helper()
	if _, err := m.Retire(t.Context(), version, stream); err != nil {
		t.Fatalf("Retire: %v", err)
	}
}

// stateOf returns the stored state of the version, "" without an item.
func stateOf(t *testing.T, m *meta.Meta) meta.State {
	t.Helper()
	return viewStates(t, m)[version].State
}

func TestMoves(t *testing.T) {
	t.Parallel()
	from := map[string]func(t *testing.T, m *meta.Meta){
		"absent":     func(*testing.T, *meta.Meta) {},
		"activating": activate,
		"live":       makeLive,
		"broken":     markBroken,
		"retired":    retire,
	}
	moves := map[string]func(ctx context.Context, m *meta.Meta) error{
		"activate": func(ctx context.Context, m *meta.Meta) error {
			_, err := m.Activate(ctx, version, stream, meta.SplitFence, meta.DefaultBounds)
			return err
		},
		"make live": func(ctx context.Context, m *meta.Meta) error { return m.MakeLive(ctx, version, time.Time{}) },
		"break": func(ctx context.Context, m *meta.Meta) error {
			_, _, err := m.MarkBroken(ctx, version, stream, failure())
			return err
		},
		"retire": func(ctx context.Context, m *meta.Meta) error {
			_, err := m.Retire(ctx, version, stream)
			return err
		},
	}
	tests := []struct {
		from, move string
		ok         bool
		want       meta.State // stored afterwards
	}{
		{"absent", "activate", true, meta.Activating},
		{"absent", "make live", false, ""},
		{"absent", "break", true, meta.Broken},
		{"absent", "retire", true, meta.Retired},
		{"activating", "activate", true, meta.Activating},
		{"activating", "make live", true, meta.Live},
		{"activating", "break", true, meta.Broken},
		{"activating", "retire", true, meta.Retired},
		{"live", "activate", false, meta.Live},
		{"live", "make live", true, meta.Live},
		{"live", "break", true, meta.Broken},
		{"live", "retire", true, meta.Retired},
		{"broken", "activate", false, meta.Broken},
		{"broken", "make live", false, meta.Broken},
		{"broken", "break", true, meta.Broken},
		{"broken", "retire", true, meta.Retired},
		{"retired", "activate", false, meta.Retired},
		{"retired", "make live", false, meta.Retired},
		{"retired", "break", true, meta.Retired},
		{"retired", "retire", true, meta.Retired},
	}
	for _, tt := range tests {
		t.Run(tt.from+" "+tt.move, func(t *testing.T) {
			t.Parallel()
			for _, bk := range backends() {
				t.Run(bk.name, func(t *testing.T) {
					t.Parallel()
					m := meta.New(bk.new(t), table)
					from[tt.from](t, m)
					err := moves[tt.move](t.Context(), m)
					if tt.ok && err != nil || !tt.ok && !errors.Is(err, meta.ErrTransition) {
						t.Errorf("%s = %v, want ok=%t or ErrTransition", tt.move, err, tt.ok)
					}
					if got := stateOf(t, m); got != tt.want {
						t.Errorf("state %q, want %q", got, tt.want)
					}
				})
			}
		})
	}
}

// The split is fixed by the first activation: a job can't change the source of a version.
func TestActivateWithAnotherSplit(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(t), table)
			activate(t, m)
			_, err := m.Activate(t.Context(), version, stream, meta.SplitTime, meta.DefaultBounds)
			if !errors.Is(err, meta.ErrTransition) {
				t.Errorf("Activate = %v, want ErrTransition", err)
			}
			if got := viewStates(t, m)[version]; got.State != meta.Activating || got.Split != meta.SplitFence {
				t.Errorf("state %+v, want activating with the fence split", got)
			}
		})
	}
}

func TestActivateTimeSplit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		bounds          meta.Bounds
		splitAt, splitL time.Duration // after the activation
	}{
		// B = 90 s + 5 min; B + 5 min + 15 min.
		{"production", meta.DefaultBounds, 390 * time.Second, 1590 * time.Second},
		{"dev", meta.Bounds{Skew: time.Second, Landing: 5 * time.Second}, time.Second, 7 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				m := meta.New(mem.New(), table)
				got, err := m.Activate(t.Context(), version, stream, meta.SplitTime, tt.bounds)
				if err != nil {
					t.Fatal(err)
				}
				want := meta.ViewState{
					Version: version, Stream: stream, State: meta.Activating, Split: meta.SplitTime,
					ActivatedAt: bubbleStart, SplitAt: bubbleStart.Add(tt.splitAt), SplitLate: bubbleStart.Add(tt.splitL),
				}
				if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(viewStates(t, m)[version], want) {
					t.Errorf("Activate returned %+v and stored %+v, want %+v", got, viewStates(t, m)[version], want)
				}
			})
		})
	}
}

func TestActivateRejectsBadArguments(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		split  meta.Split
		bounds meta.Bounds
	}{
		"unknown split":  {"archive", meta.DefaultBounds},
		"negative bound": {meta.SplitTime, meta.Bounds{Skew: -time.Second}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(mem.New(), table)
			if _, err := m.Activate(t.Context(), version, stream, tt.split, tt.bounds); err == nil {
				t.Error("Activate succeeded")
			}
			if got := stateOf(t, m); got != "" {
				t.Errorf("state %q after a rejected activation", got)
			}
		})
	}
}

// G3 reads a version's stream from its item, so a write that names another stream is a bug.
func TestWritesRejectAnotherStream(t *testing.T) {
	t.Parallel()
	writes := map[string]func(ctx context.Context, m *meta.Meta) error{
		"Activate": func(ctx context.Context, m *meta.Meta) error {
			_, err := m.Activate(ctx, version, "clicks", meta.SplitFence, meta.DefaultBounds)
			return err
		},
		"MarkBroken": func(ctx context.Context, m *meta.Meta) error {
			_, _, err := m.MarkBroken(ctx, version, "clicks", failure())
			return err
		},
		"Retire": func(ctx context.Context, m *meta.Meta) error {
			_, err := m.Retire(ctx, version, "clicks")
			return err
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(mem.New(), table)
			activate(t, m)
			if err := write(t.Context(), m); err == nil || !strings.Contains(err.Error(), "clicks") {
				t.Errorf("got %v, want an error naming stream clicks", err)
			}
			if got := viewStates(t, m)[version]; got.State != meta.Activating || got.Stream != stream {
				t.Errorf("state %+v, want activating on %s", got, stream)
			}
		})
	}
}

// A first write that names no stream fails before it stores an item that every read of the
// view states would reject.
func TestFirstWriteWithoutStreamFails(t *testing.T) {
	t.Parallel()
	writes := map[string]func(ctx context.Context, m *meta.Meta) error{
		"Activate": func(ctx context.Context, m *meta.Meta) error {
			_, err := m.Activate(ctx, version, "", meta.SplitFence, meta.DefaultBounds)
			return err
		},
		"MarkBroken": func(ctx context.Context, m *meta.Meta) error {
			_, _, err := m.MarkBroken(ctx, version, "", failure())
			return err
		},
		"Retire": func(ctx context.Context, m *meta.Meta) error {
			_, err := m.Retire(ctx, version, "")
			return err
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(mem.New(), table)
			if err := write(t.Context(), m); err == nil || !strings.HasSuffix(err.Error(), ": stream is empty") {
				t.Errorf("got %v, want an error ending in \": stream is empty\"", err)
			}
			if states, err := m.ViewStates(t.Context()); err != nil || len(states) != 0 {
				t.Errorf("ViewStates = %v, %v; want none", states, err)
			}
		})
	}
}

// A retired version keeps what it had, its failure included, for status and inspect.
func TestRetireKeepsWhatTheVersionHad(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		m := meta.New(mem.New(), table)
		if _, err := m.Activate(t.Context(), version, stream, meta.SplitTime, meta.DefaultBounds); err != nil {
			t.Fatal(err)
		}
		// 1.5 µs past the day before is stored rounded up.
		if err := m.MakeLive(t.Context(), version, bubbleStart.Add(-24*time.Hour+1500*time.Nanosecond)); err != nil {
			t.Fatal(err)
		}
		markBroken(t, m)
		time.Sleep(time.Hour)
		retire(t, m)
		f := failure()
		want := meta.ViewState{
			Version: version, Stream: stream, State: meta.Retired, Split: meta.SplitTime,
			ActivatedAt: bubbleStart, SplitAt: bubbleStart.Add(390 * time.Second), SplitLate: bubbleStart.Add(1590 * time.Second),
			PartialSince: bubbleStart.Add(-24*time.Hour + 2*time.Microsecond), RetiredAt: bubbleStart.Add(time.Hour),
			Failure: &f,
		}
		if got := viewStates(t, m)[version]; !reflect.DeepEqual(got, want) {
			t.Errorf("state\n got  %+v\n want %+v", got, want)
		}
	})
}

func TestMarkBroken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(t *testing.T, m *meta.Meta)
		want   string // the failure's sequence number afterwards; "" for none
		opened bool
	}{
		{"from absent", func(*testing.T, *meta.Meta) {}, seq(2), true},
		{"from activating", activate, seq(2), true},
		{"from live", makeLive, seq(2), true},
		{"keeps the first failure", markBroken, seq(1), false},
		{"broken by the same record before", func(t *testing.T, m *meta.Meta) {
			t.Helper()
			if _, _, err := m.MarkBroken(t.Context(), version, stream, meta.Failure{Cause: meta.CauseRecord, Unit: "u1", Seq: seq(2)}); err != nil {
				t.Fatal(err)
			}
		}, seq(2), false},
		{"ignored once retired", retire, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for _, bk := range backends() {
				t.Run(bk.name, func(t *testing.T) {
					t.Parallel()
					m := meta.New(bk.new(t), table)
					tt.setup(t, m)
					f := meta.Failure{Cause: meta.CauseRecord, Unit: "u1", Seq: seq(2)}
					_, opened, err := m.MarkBroken(t.Context(), version, stream, f)
					if err != nil || opened != tt.opened {
						t.Errorf("MarkBroken = %t, %v; want %t", opened, err, tt.opened)
					}
					if got := failureSeq(t, m); got != tt.want {
						t.Errorf("failure at %q, want %q", got, tt.want)
					}
				})
			}
		})
	}
}

// failureSeq returns the sequence number of the version's failure, "" without one.
func failureSeq(t *testing.T, m *meta.Meta) string {
	t.Helper()
	if f := viewStates(t, m)[version].Failure; f != nil {
		return f.Seq
	}
	return ""
}

// A record read from the stream table has no unit or sequence number: its event_id names it.
func TestMarkBrokenByATableRecord(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(t), table)
			f := meta.Failure{Cause: meta.CauseSplit, EventID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Error: "late", At: bubbleStart}
			if _, opened, err := m.MarkBroken(t.Context(), version, stream, f); !opened || err != nil {
				t.Fatalf("MarkBroken = %t, %v; want it opened", opened, err)
			}
			if got := viewStates(t, m)[version].Failure; got == nil || *got != f {
				t.Errorf("failure %+v, want %+v", got, f)
			}
		})
	}
}

func TestMarkBrokenRejectsBadFailures(t *testing.T) {
	t.Parallel()
	tests := map[string]meta.Failure{
		"sequence number 7":            {Cause: meta.CauseRecord, Unit: "u1", Seq: "7"},
		"sequence number 0":            {Cause: meta.CauseRecord, Unit: "u1", Seq: seq(0)},
		"a sequence number, no unit":   {Cause: meta.CauseRecord, Seq: seq(1)},
		"a unit, no sequence number":   {Cause: meta.CauseRecord, Unit: "u1"},
		"no unit and no event_id":      {Cause: meta.CauseSplit},
		"no cause":                     {Unit: "u1", Seq: seq(1)},
		"a cause Dynaflux doesn't use": {Cause: "gap", Unit: "u1", Seq: seq(1)},
	}
	for name, f := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(mem.New(), table)
			if _, _, err := m.MarkBroken(t.Context(), version, stream, f); err == nil {
				t.Error("MarkBroken succeeded")
			}
			if got := stateOf(t, m); got != "" {
				t.Errorf("state %q after a rejected break", got)
			}
		})
	}
}

// Activation stamps the time once; later moves and repeated calls keep it, because the
// listing for fences is measured from the first.
func TestActivatedAt(t *testing.T) {
	t.Parallel()
	for _, bk := range memBackends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				m := meta.New(bk.new(t), table)
				activate(t, m)
				time.Sleep(time.Minute)
				makeLive(t, m) // activates again first
				time.Sleep(time.Minute)
				markBroken(t, m)
				if got := viewStates(t, m)[version].ActivatedAt; !got.Equal(bubbleStart) {
					t.Errorf("ActivatedAt = %s, want %s", got, bubbleStart)
				}
			})
		})
	}
}

func TestSetLive(t *testing.T) {
	t.Parallel()
	m := meta.New(mem.New(), table)
	activate(t, m) // a version part way there
	cat := &catalog.Catalog{Views: []catalog.View{
		{Name: "page_daily", Versions: []catalog.ViewVersion{{Number: 1, Stream: stream}, {Number: 2, Stream: stream}}},
		{Name: "other", Versions: []catalog.ViewVersion{{Number: 1, Stream: "clicks"}}},
	}}
	if err := m.SetLive(t.Context(), cat); err != nil {
		t.Fatal(err)
	}
	got := viewStates(t, m)
	want := map[string]string{"page_daily@v1": stream, version: stream, "other@v1": "clicks"}
	for v, s := range want {
		if vs := got[v]; vs.State != meta.Live || vs.Stream != s || vs.Split != meta.SplitFence {
			t.Errorf("state of %s = %+v, want live on %s with the fence split", v, vs, s)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%d states, want %d", len(got), len(want))
	}
}

func TestViewStatesListsEveryVersion(t *testing.T) {
	t.Parallel()
	m := meta.New(mem.New(), table)
	activate(t, m)
	if _, err := m.Activate(t.Context(), "other@v1", stream, meta.SplitNone, meta.DefaultBounds); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.MarkBroken(t.Context(), "third@v3", stream, failure()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Retire(t.Context(), "old@v1", stream); err != nil {
		t.Fatal(err)
	}
	got := viewStates(t, m)
	want := map[string]meta.State{version: meta.Activating, "other@v1": meta.Activating, "third@v3": meta.Broken, "old@v1": meta.Retired}
	if len(got) != len(want) {
		t.Errorf("%d states, want %d", len(got), len(want))
	}
	for v, s := range want {
		if got[v].Version != v || got[v].State != s {
			t.Errorf("state of %s = %+v, want %s", v, got[v], s)
		}
	}
}

func TestMarkBrokenCutsLongError(t *testing.T) {
	t.Parallel()
	m := meta.New(mem.New(), table)
	// 1,023 bytes, a 2-byte rune that a cut at 1 KB would split, then more than an item holds.
	f := failure()
	f.Error = strings.Repeat("a", 1023) + "é" + strings.Repeat("b", 500_000)
	if _, _, err := m.MarkBroken(t.Context(), version, stream, f); err != nil {
		t.Fatal(err)
	}
	if got := viewStates(t, m)[version].Failure; got == nil || got.Error != strings.Repeat("a", 1023) {
		t.Errorf("failure %+v, want its error cut to the first 1,023 bytes", got)
	}
}

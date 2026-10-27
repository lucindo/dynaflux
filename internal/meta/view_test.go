// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta_test

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

func setStates(t *testing.T, m *meta.Meta, states ...meta.State) {
	t.Helper()
	for _, s := range states {
		if err := m.SetViewState(t.Context(), version, s); err != nil {
			t.Fatalf("SetViewState(%s): %v", s, err)
		}
	}
}

// markBroken breaks the version at sequence number 1.
func markBroken(t *testing.T, m *meta.Meta) {
	t.Helper()
	f := meta.Failure{Unit: "u1", Seq: seq(1), EventID: "e", Error: "boom", At: bubbleStart}
	if _, err := m.MarkBroken(t.Context(), version, f); err != nil {
		t.Fatalf("MarkBroken: %v", err)
	}
}

// stateOf returns the stored state of the version, "" without an item.
func stateOf(t *testing.T, m *meta.Meta) meta.State {
	t.Helper()
	return viewStates(t, m)[version].State
}

func TestSetViewState(t *testing.T) {
	t.Parallel()
	from := map[string]func(t *testing.T, m *meta.Meta){
		"absent":     func(*testing.T, *meta.Meta) {},
		"activating": func(t *testing.T, m *meta.Meta) { t.Helper(); setStates(t, m, meta.Activating) },
		"live":       func(t *testing.T, m *meta.Meta) { t.Helper(); setStates(t, m, meta.Activating, meta.Live) },
		"broken":     func(t *testing.T, m *meta.Meta) { t.Helper(); markBroken(t, m) },
	}
	tests := []struct {
		from string
		to   meta.State
		ok   bool
		want meta.State // stored afterwards
	}{
		{"absent", meta.Activating, true, meta.Activating},
		{"absent", meta.Live, false, ""},
		{"absent", meta.PendingBackfill, false, ""},
		{"absent", meta.Broken, false, ""},
		{"activating", meta.Activating, true, meta.Activating},
		{"activating", meta.Live, true, meta.Live},
		{"activating", meta.PendingBackfill, false, meta.Activating},
		{"activating", meta.Broken, false, meta.Activating},
		{"live", meta.Live, true, meta.Live},
		{"live", meta.Activating, false, meta.Live},
		{"live", meta.PendingBackfill, false, meta.Live},
		{"broken", meta.Activating, false, meta.Broken},
		{"broken", meta.Live, false, meta.Broken},
		{"broken", meta.PendingBackfill, false, meta.Broken},
	}
	for _, tt := range tests {
		t.Run(tt.from+"_to_"+string(tt.to), func(t *testing.T) {
			t.Parallel()
			for _, bk := range backends() {
				t.Run(bk.name, func(t *testing.T) {
					t.Parallel()
					m := meta.New(bk.new(t), table)
					from[tt.from](t, m)
					err := m.SetViewState(t.Context(), version, tt.to)
					if tt.ok && err != nil || !tt.ok && !errors.Is(err, meta.ErrTransition) {
						t.Errorf("SetViewState = %v, want ok=%t or ErrTransition", err, tt.ok)
					}
					if got := stateOf(t, m); got != tt.want {
						t.Errorf("state %q, want %q", got, tt.want)
					}
				})
			}
		})
	}
}

func TestMarkBroken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(t *testing.T, m *meta.Meta)
		want   string // the failure's sequence number afterwards
		opened bool
	}{
		{"from absent", func(*testing.T, *meta.Meta) {}, seq(2), true},
		{"from activating", func(t *testing.T, m *meta.Meta) { t.Helper(); setStates(t, m, meta.Activating) }, seq(2), true},
		{"from live", func(t *testing.T, m *meta.Meta) { t.Helper(); setStates(t, m, meta.Activating, meta.Live) }, seq(2), true},
		{"keeps the first failure", func(t *testing.T, m *meta.Meta) { t.Helper(); markBroken(t, m) }, seq(1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for _, bk := range backends() {
				t.Run(bk.name, func(t *testing.T) {
					t.Parallel()
					m := meta.New(bk.new(t), table)
					tt.setup(t, m)
					opened, err := m.MarkBroken(t.Context(), version, meta.Failure{Unit: "u1", Seq: seq(2)})
					if err != nil || opened != tt.opened {
						t.Errorf("MarkBroken = %t, %v; want %t", opened, err, tt.opened)
					}
					got := viewStates(t, m)[version]
					if got.State != meta.Broken || got.Failure == nil || got.Failure.Seq != tt.want {
						t.Errorf("state %+v, want broken at %s", got, tt.want)
					}
				})
			}
		})
	}
}

func TestMarkBrokenRejectsBadSequenceNumbers(t *testing.T) {
	t.Parallel()
	m := meta.New(mem.New(), table)
	for _, sn := range []string{"7", seq(0)} {
		if _, err := m.MarkBroken(t.Context(), version, meta.Failure{Seq: sn}); err == nil {
			t.Errorf("MarkBroken at sequence number %q succeeded", sn)
		}
	}
	if got := stateOf(t, m); got != "" {
		t.Errorf("state %q after a rejected break", got)
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
				for _, s := range []meta.State{meta.Activating, meta.Activating, meta.Live} {
					setStates(t, m, s)
					time.Sleep(time.Minute)
				}
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
	setStates(t, m, meta.Activating) // a version part way there
	cat := &catalog.Catalog{Views: []catalog.View{
		{Name: "page_daily", Versions: []catalog.ViewVersion{{Number: 1}, {Number: 2}}},
		{Name: "other", Versions: []catalog.ViewVersion{{Number: 1}}},
	}}
	if err := m.SetLive(t.Context(), cat); err != nil {
		t.Fatal(err)
	}
	got := viewStates(t, m)
	for _, v := range []string{"page_daily@v1", version, "other@v1"} {
		if got[v].State != meta.Live {
			t.Errorf("state of %s = %q, want live", v, got[v].State)
		}
	}
	if len(got) != 3 {
		t.Errorf("%d states, want 3", len(got))
	}
}

func TestViewStatesListsEveryVersion(t *testing.T) {
	t.Parallel()
	m := meta.New(mem.New(), table)
	setStates(t, m, meta.Activating)
	if err := m.SetViewState(t.Context(), "other@v1", meta.Activating); err != nil {
		t.Fatal(err)
	}
	if _, err := m.MarkBroken(t.Context(), "third@v3", meta.Failure{Seq: seq(1)}); err != nil {
		t.Fatal(err)
	}
	got := viewStates(t, m)
	want := map[string]meta.State{version: meta.Activating, "other@v1": meta.Activating, "third@v3": meta.Broken}
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
	long := strings.Repeat("a", 1023) + "é" + strings.Repeat("b", 500_000)
	if _, err := m.MarkBroken(t.Context(), version, meta.Failure{Seq: seq(1), Error: long}); err != nil {
		t.Fatal(err)
	}
	if got := viewStates(t, m)[version].Failure; got == nil || got.Error != strings.Repeat("a", 1023) {
		t.Errorf("failure %+v, want its error cut to the first 1,023 bytes", got)
	}
}

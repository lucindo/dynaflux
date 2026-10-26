// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

func TestAdvanceStoresProgressAndDeadline(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(), table)
			deadline := bubbleStart.Add(42 * time.Second)
			for _, upTo := range []int{10, 20} {
				if err := m.Advance(t.Context(), readUnit(t, m, "u1"), seq(upTo), deadline, never); err != nil {
					t.Fatalf("Advance to %d: %v", upTo, err)
				}
			}
			if u := readUnit(t, m, "u1"); u.Progress != seq(20) || !u.ProgressAt.Equal(deadline) {
				t.Errorf("progress %s at %s, want %s at %s", u.Progress, u.ProgressAt, seq(20), deadline)
			}
		})
	}
}

// A stored deadline never moves earlier, or pruning could start before the invocation ends.
func TestProgressAtRoundsUpToTheMicrosecond(t *testing.T) {
	t.Parallel()
	m := meta.New(mem.New(), table)
	if err := m.Advance(t.Context(), readUnit(t, m, "u1"), seq(1), bubbleStart.Add(time.Nanosecond), never); err != nil {
		t.Fatal(err)
	}
	if got, want := readUnit(t, m, "u1").ProgressAt, bubbleStart.Add(time.Microsecond); !got.Equal(want) {
		t.Errorf("ProgressAt = %s, want %s", got, want)
	}
}

func TestAdvanceAlreadyHigherIsSuccess(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(), table)
			stale := readUnit(t, m, "u1")
			first := bubbleStart.Add(time.Minute)
			if err := m.Advance(t.Context(), stale, seq(20), first, never); err != nil {
				t.Fatal(err)
			}
			for _, upTo := range []int{15, 20} {
				if err := m.Advance(t.Context(), stale, seq(upTo), bubbleStart.Add(time.Hour), never); err != nil {
					t.Errorf("Advance to %d below stored progress 20: %v", upTo, err)
				}
			}
			if u := readUnit(t, m, "u1"); u.Progress != seq(20) || !u.ProgressAt.Equal(first) {
				t.Errorf("progress %s at %s, want %s at %s", u.Progress, u.ProgressAt, seq(20), first)
			}
		})
	}
}

// A batch that read the unit before a fence existed, and ran without the fenced version,
// loses the conditional write, finds the fence in the stored item and can't advance.
func TestStaleAdvanceSeesNewFence(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(), table)
			advance(t, m, "u1", seq(10))
			stale := readUnit(t, m, "u1")
			if _, err := m.EnsureFence(t.Context(), readUnit(t, m, "u1"), version); err != nil {
				t.Fatal(err)
			}

			notV := func(v string) bool { return v != version }
			if err := m.Advance(t.Context(), stale, seq(20), bubbleStart, notV); !errors.Is(err, meta.ErrFenced) {
				t.Fatalf("Advance without %s = %v, want ErrFenced", version, err)
			}
			if u := readUnit(t, m, "u1"); u.Progress != seq(10) {
				t.Fatalf("progress %s after a fenced advance, want %s", u.Progress, seq(10))
			}
			if err := m.Advance(t.Context(), stale, seq(20), bubbleStart, always); err != nil {
				t.Fatalf("Advance covering %s: %v", version, err)
			}
			if u := readUnit(t, m, "u1"); u.Progress != seq(20) || u.Fences[version] != seq(10) {
				t.Errorf("unit %+v, want progress %s and fence %s", u, seq(20), seq(10))
			}
		})
	}
}

// A zero deadline would make every _hwm entry of the unit prunable at once.
func TestAdvanceRejectsBadArguments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		upTo string
		at   time.Time
	}{
		{"empty", "", bubbleStart},
		{"2 digits", "10", bubbleStart},
		{"41 digits", seq(10) + "0", bubbleStart},
		{"not digits", strings.Repeat("x", 40), bubbleStart},
		{"sequence number 0", seq(0), bubbleStart},
		{"no deadline", seq(10), time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(mem.New(), table)
			if err := m.Advance(t.Context(), readUnit(t, m, "u1"), tt.upTo, tt.at, never); err == nil {
				t.Error("Advance succeeded")
			}
			if u := readUnit(t, m, "u1"); u.Progress != "" {
				t.Errorf("progress %s after a rejected advance", u.Progress)
			}
		})
	}
}

func TestEnsureFence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(t *testing.T, m *meta.Meta) meta.Unit // returns the unit EnsureFence gets
		want  string
	}{
		{
			name:  "unit without an item",
			setup: func(t *testing.T, m *meta.Meta) meta.Unit { t.Helper(); return readUnit(t, m, "u1") },
			want:  seq(0),
		},
		{
			name: "unit with progress",
			setup: func(t *testing.T, m *meta.Meta) meta.Unit {
				t.Helper()
				advance(t, m, "u1", seq(10))
				return readUnit(t, m, "u1")
			},
			want: seq(10),
		},
		{
			name: "progress raised after the read",
			setup: func(t *testing.T, m *meta.Meta) meta.Unit {
				t.Helper()
				advance(t, m, "u1", seq(10))
				stale := readUnit(t, m, "u1")
				advance(t, m, "u1", seq(20))
				return stale
			},
			want: seq(20),
		},
		{
			name: "existing fence",
			setup: func(t *testing.T, m *meta.Meta) meta.Unit {
				t.Helper()
				advance(t, m, "u1", seq(10))
				stale := readUnit(t, m, "u1")
				if _, err := m.EnsureFence(t.Context(), readUnit(t, m, "u1"), version); err != nil {
					t.Fatal(err)
				}
				advance(t, m, "u1", seq(30))
				return stale
			},
			want: seq(10),
		},
	}
	for _, tt := range tests {
		for _, bk := range backends() {
			t.Run(tt.name+"_"+bk.name, func(t *testing.T) {
				t.Parallel()
				m := meta.New(bk.new(), table)
				u, err := m.EnsureFence(t.Context(), tt.setup(t, m), version)
				if err != nil {
					t.Fatal(err)
				}
				if u.Fences[version] != tt.want {
					t.Errorf("returned fence %s, want %s", u.Fences[version], tt.want)
				}
				if got := readUnit(t, m, "u1").Fences[version]; got != tt.want {
					t.Errorf("stored fence %s, want %s", got, tt.want)
				}
			})
		}
	}
}

// The unit EnsureFence returns is the stored one, so the same batch advances from it.
func TestAdvanceFromEnsuredFence(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(), table)
			u, err := m.EnsureFence(t.Context(), readUnit(t, m, "u1"), version)
			if err == nil {
				u, err = m.EnsureFence(t.Context(), u, "other@v1")
			}
			if err == nil {
				err = m.Advance(t.Context(), u, seq(5), bubbleStart, always)
			}
			if err != nil {
				t.Fatal(err)
			}
			got := readUnit(t, m, "u1")
			if got.Progress != seq(5) || got.Fences[version] != seq(0) || got.Fences["other@v1"] != seq(0) {
				t.Errorf("unit %+v, want progress %s and two zero fences", got, seq(5))
			}
		})
	}
}

// Advances that don't cover a version race its fence. The fence splits them exactly: each
// one at or below it succeeded, each one above it failed with ErrFenced, and progress is
// the highest that succeeded.
func TestConcurrentAdvancesAndFence(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(), table)
			for round := range 10 {
				id := fmt.Sprintf("u%d", round)
				errs := raceFence(t, m, id, 24)
				got := readUnit(t, m, id)
				fence, highest := got.Fences[version], ""
				for i, err := range errs {
					switch upTo := seq(i + 1); {
					case err == nil && upTo <= fence:
						highest = max(highest, upTo)
					case errors.Is(err, meta.ErrFenced) && upTo > fence:
					default:
						t.Errorf("unit %s: Advance to %d = %v with fence %s", id, i+1, err, fence)
					}
				}
				if got.Progress != highest {
					t.Errorf("unit %s: progress %q, want %q", id, got.Progress, highest)
				}
			}
		})
	}
}

// raceFence runs Advance to 1 through n, none covering the version, at the same time as
// EnsureFence, and returns each Advance's error.
func raceFence(t *testing.T, m *meta.Meta, id string, n int) []error {
	t.Helper()
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			u, err := m.Unit(t.Context(), stream, id)
			if err == nil {
				err = m.Advance(t.Context(), u, seq(i+1), bubbleStart, never)
			}
			errs[i] = err
		})
	}
	wg.Go(func() {
		u, err := m.Unit(t.Context(), stream, id)
		if err == nil {
			_, err = m.EnsureFence(t.Context(), u, version)
		}
		if err != nil {
			t.Errorf("EnsureFence: %v", err)
		}
	})
	wg.Wait()
	return errs
}

func TestUnitsByKey(t *testing.T) {
	t.Parallel()
	m := meta.New(newStaleReads(), table)
	const units = 150 // more than one BatchGet
	ids := make([]string, units)
	unitKeys := make([]string, 0, units+3)
	for i := range units {
		ids[i] = fmt.Sprintf("shardId-%03d", i)
		advance(t, m, ids[i], seq(i+1))
		unitKeys = append(unitKeys, keys.UnitKey(ids[i]))
	}
	unitKeys = append(unitKeys, keys.UnitKey("no-item"), unitKeys[0], keys.UnitKey("no-item-either"))

	got, err := m.Units(t.Context(), stream, unitKeys, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != units {
		t.Errorf("%d units, want %d", len(got), units)
	}
	for i, id := range ids {
		u := got[keys.UnitKey(id)]
		if u.ID != id || u.Progress != seq(i+1) || u.Stream != stream {
			t.Errorf("unit %s = %+v, want progress %s", id, u, seq(i+1))
		}
	}

	// The replica behind eventually consistent reads has seen no write.
	got, err = m.Units(t.Context(), stream, unitKeys, false)
	if err != nil || len(got) != 0 {
		t.Errorf("eventually consistent read: %d units, %v; want none", len(got), err)
	}
}

func TestFenceUnits(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				m := meta.New(bk.new(), table)
				advance(t, m, "old", seq(10))
				if err := m.SetViewState(t.Context(), version, meta.Activating); err != nil {
					t.Fatal(err)
				}
				time.Sleep(meta.ListingDelay)
				listedAt := time.Now()
				units := []string{"old", "new"}
				if err := m.FenceUnits(t.Context(), stream, version, units, listedAt); err != nil {
					t.Fatal(err)
				}
				advance(t, m, "old", seq(20))
				// Repeating the call keeps the fences.
				if err := m.FenceUnits(t.Context(), stream, version, units, listedAt); err != nil {
					t.Fatal(err)
				}
				wantFences(t, m, map[string]string{"old": seq(10), "new": seq(0)})
			})
		})
	}
}

func wantFences(t *testing.T, m *meta.Meta, want map[string]string) {
	t.Helper()
	for id, fence := range want {
		if got := readUnit(t, m, id).Fences[version]; got != fence {
			t.Errorf("fence of %s = %s, want %s", id, got, fence)
		}
	}
}

// A listing taken before every projector saw the version activating could miss a unit that
// such a projector creates without a fence.
func TestFenceUnitsRefusesEarlyListing(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		m := meta.New(mem.New(), table)
		if err := m.SetViewState(t.Context(), version, meta.Activating); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Hour) // the check is on listedAt, not on the clock
		listedAt := bubbleStart.Add(90*time.Second - time.Microsecond)
		if err := m.FenceUnits(t.Context(), stream, version, []string{"u1"}, listedAt); !errors.Is(err, meta.ErrListedTooEarly) {
			t.Fatalf("FenceUnits = %v, want ErrListedTooEarly", err)
		}
		if u := readUnit(t, m, "u1"); u.Fences != nil {
			t.Errorf("fences %v after a refused listing", u.Fences)
		}
	})
}

func TestFenceUnitsNeedsActivating(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(t *testing.T, m *meta.Meta)
	}{
		{"pending_backfill", func(*testing.T, *meta.Meta) {}},
		{"live", func(t *testing.T, m *meta.Meta) { t.Helper(); setStates(t, m, meta.Activating, meta.Live) }},
		{"broken", func(t *testing.T, m *meta.Meta) { t.Helper(); markBroken(t, m, 1) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				m := meta.New(mem.New(), table)
				tt.setup(t, m)
				time.Sleep(meta.ListingDelay) // the listing alone would be accepted
				err := m.FenceUnits(t.Context(), stream, version, []string{"u1"}, time.Now())
				if !errors.Is(err, meta.ErrNotActivating) {
					t.Errorf("FenceUnits = %v, want ErrNotActivating", err)
				}
				if u := readUnit(t, m, "u1"); u.Fences != nil {
					t.Errorf("fences %v", u.Fences)
				}
			})
		})
	}
}

// A listing time ahead of the clock would skip the wait for projectors to see activating.
func TestFenceUnitsRefusesFutureListing(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		m := meta.New(mem.New(), table)
		setStates(t, m, meta.Activating)
		listedAt := time.Now().Add(meta.ListingDelay + time.Minute)
		if err := m.FenceUnits(t.Context(), stream, version, []string{"u1"}, listedAt); err == nil {
			t.Error("FenceUnits succeeded")
		}
		if u := readUnit(t, m, "u1"); u.Fences != nil {
			t.Errorf("fences %v after a refused listing", u.Fences)
		}
	})
}

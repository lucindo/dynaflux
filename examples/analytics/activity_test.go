// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package analytics_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/fluxtest"
)

func appendMemberEvents(t *testing.T, c *flux.Client, evs []flux.MemberEvent) {
	t.Helper()
	if _, err := c.AppendMemberEventBatch(t.Context(), evs); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceSummary(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		appendMemberEvents(t, c, []flux.MemberEvent{
			{WorkspaceID: "w1", MemberID: "m1", Action: "login", EventTime: start.Add(-3 * time.Hour)},
			{WorkspaceID: "w1", MemberID: "m1", Action: "logout", EventTime: start.Add(-time.Hour)},
			{WorkspaceID: "w1", MemberID: "m2", Action: "login", EventTime: start.Add(-30 * time.Hour)},
			{WorkspaceID: "w1", MemberID: "m3", Action: "logout", EventTime: start.Add(-2 * time.Hour)},
			{WorkspaceID: "w1", MemberID: "m3", Action: "login", EventTime: start.Add(-10 * time.Minute)},
			{WorkspaceID: "w2", MemberID: "m9", Action: "logout", EventTime: start.Add(-time.Hour)},
		})
		// One row per member of w1, counted by its latest state.
		got, err := c.WorkspaceSummary(t.Context(), flux.WorkspaceSummaryParams{WorkspaceID: "w1", Since: start.Add(-2 * time.Hour)})
		if want := (flux.WorkspaceSummaryRow{Members: 3, RecentlyActive: 2, LoggedOut: 1}); err != nil || got != want {
			t.Errorf("WorkspaceSummary(w1) = %+v, %v; want %+v", got, err, want)
		}
		// Without GROUP BY, a workspace without members still has its row of zeros.
		got, err = c.WorkspaceSummary(t.Context(), flux.WorkspaceSummaryParams{WorkspaceID: "w3", Since: start})
		if want := (flux.WorkspaceSummaryRow{}); err != nil || got != want {
			t.Errorf("WorkspaceSummary(w3) = %+v, %v; want %+v", got, err, want)
		}
	})
}

func TestWorkspaceActiveMembers(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		day1, day2, day3 := start.Add(-72*time.Hour), start.Add(-48*time.Hour), start.Add(-24*time.Hour)
		appendMemberEvents(t, c, []flux.MemberEvent{
			{WorkspaceID: "w1", MemberID: "m1", Action: "login", EventTime: day1.Add(9 * time.Hour)},
			{WorkspaceID: "w1", MemberID: "m2", Action: "login", EventTime: day1.Add(10 * time.Hour)},
			{WorkspaceID: "w1", MemberID: "m2", Action: "view", EventTime: day2.Add(9 * time.Hour)},
			{WorkspaceID: "w1", MemberID: "m3", Action: "login", EventTime: day2.Add(11 * time.Hour)},
			{WorkspaceID: "w1", MemberID: "m4", Action: "login", EventTime: day3.Add(9 * time.Hour)},
		})
		// The daily sketches merge, so m2, active on both days, counts once.
		got, err := c.WorkspaceActiveMembers(t.Context(), flux.WorkspaceActiveMembersParams{WorkspaceID: "w1", From: day1, To: day2})
		if want := (flux.WorkspaceActiveMembersRow{Members: 3}); err != nil || got != want {
			t.Errorf("WorkspaceActiveMembers = %+v, %v; want %+v", got, err, want)
		}
	})
}

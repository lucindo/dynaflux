// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/fluxtest"
)

// nonce keeps the keys of this process apart from an earlier run's on the same AWS store.
//
//nolint:gochecknoglobals // one per process
var nonce = strings.ToLower(rand.Text()[:6])

// run is one scenario's appends. Every key it writes starts with tag, and each region has an
// oracle: an in-memory store fed the events that region must apply.
type run struct {
	t      *testing.T
	h      *harness
	tag    string
	at     time.Time // the events' time, unless a scenario sets another
	oracle map[*region]*dynaflux.Store
	// pending holds, by region, the appends of other regions that its oracle hasn't had
	// yet, while replication is paused.
	pending map[*region][]appendFn
	paused  bool

	pages, tags, accounts, workspaces map[string]bool
}

type appendFn func(context.Context, *flux.Client) error

func newRun(t *testing.T, h *harness, name string) *run {
	t.Helper()
	r := &run{
		t: t, h: h, tag: name + "-" + nonce, at: time.Now().UTC(),
		oracle: map[*region]*dynaflux.Store{}, pending: map[*region][]appendFn{},
		pages: map[string]bool{}, tags: map[string]bool{}, accounts: map[string]bool{}, workspaces: map[string]bool{},
	}
	for _, reg := range h.regions {
		r.oracle[reg] = fluxtest.NewStore(t, flux.Catalog)
	}
	return r
}

// key returns a key of the scenario.
func (r *run) key(name string) string { return r.tag + "-" + name }

// add appends in region in, and to the oracle of each region the append reaches.
func (r *run) add(in *region, fn appendFn) {
	r.t.Helper()
	if err := fn(r.t.Context(), in.client); err != nil {
		r.t.Fatalf("append in %s: %v", in.name, err)
	}
	r.feed(in, fn)
}

// feed appends to the oracle of each region that an append in region in reaches.
func (r *run) feed(in *region, fn appendFn) {
	r.t.Helper()
	for _, reg := range r.h.regions {
		switch {
		case reg == in:
		case !r.h.replicated:
			continue
		case r.paused:
			r.pending[reg] = append(r.pending[reg], fn)
			continue
		}
		if err := fn(r.t.Context(), flux.New(r.oracle[reg])); err != nil {
			r.t.Fatalf("append to the oracle of %s: %v", reg.name, err)
		}
	}
}

// resume gives each oracle the appends that replication held back.
func (r *run) resume() {
	r.t.Helper()
	r.paused = false
	for reg, fns := range r.pending {
		for _, fn := range fns {
			if err := fn(r.t.Context(), flux.New(r.oracle[reg])); err != nil {
				r.t.Fatalf("append to the oracle of %s: %v", reg.name, err)
			}
		}
	}
	clear(r.pending)
}

func (r *run) pageViews(in *region, evs ...flux.PageView) {
	r.t.Helper()
	for _, ev := range evs {
		r.pages[ev.Page] = true
		for _, tag := range ev.Tags {
			r.tags[tag] = true
		}
	}
	r.add(in, func(ctx context.Context, c *flux.Client) error {
		_, err := c.AppendPageViewBatch(ctx, evs)
		return err
	})
}

func (r *run) memberEvents(in *region, evs ...flux.MemberEvent) {
	r.t.Helper()
	for _, ev := range evs {
		r.workspaces[ev.WorkspaceID] = true
	}
	r.add(in, func(ctx context.Context, c *flux.Client) error {
		_, err := c.AppendMemberEventBatch(ctx, evs)
		return err
	})
}

func (r *run) apiCalls(in *region, evs ...flux.APICall) {
	r.t.Helper()
	for _, ev := range evs {
		r.accounts[ev.AccountID] = true
	}
	r.add(in, func(ctx context.Context, c *flux.Client) error {
		_, err := c.AppendAPICallBatch(ctx, evs)
		return err
	})
}

// converged waits until the views of each region give the query results of its oracle.
func (r *run) converged(regions ...*region) {
	r.t.Helper()
	for _, reg := range regions {
		r.h.await(r.t, r.h.patience, reg.name+": views against the oracle", func(ctx context.Context) error {
			return r.compare(ctx, reg)
		})
	}
}

func (r *run) compare(ctx context.Context, reg *region) error {
	want, err := r.views(ctx, r.oracle[reg])
	if err != nil {
		return fmt.Errorf("oracle: %w", err)
	}
	got, err := r.views(ctx, reg.store)
	if err != nil {
		return err
	}
	if reflect.DeepEqual(got, want) {
		return nil
	}
	// JSON shows what a *float64 points to; Go's names are fine as its keys.
	g, err := json.Marshal(got) //nolint:musttag // read by people only
	if err != nil {
		return err
	}
	w, err := json.Marshal(want) //nolint:musttag // read by people only
	if err != nil {
		return err
	}
	return fmt.Errorf("views\n%s\nwant\n%s", g, w)
}

// views is what the queries return for a scenario's keys: the example's, which the
// application runs, and testdata/queries.sql's, which read the rest of every view.
type views struct {
	Pages      map[string]page
	Tags       []flux.TagTotalsRow
	Quota      map[string]flux.QuotaUsedRow // only the accounts with a row
	Workspaces map[string]workspace
}

type page struct {
	Stats  []flux.PageStatsRow
	Totals []pageTotalsRow
}

type workspace struct {
	Summary flux.WorkspaceSummaryRow
	Active  flux.WorkspaceActiveMembersRow
	Days    []workspaceDayRow
	Members []memberRow
}

// The rows of testdata/queries.sql.
type (
	pageTotalsRow struct {
		Day      time.Time
		TotalMS  uint64
		LastView time.Time
	}
	workspaceDayRow struct {
		Day           time.Time
		Logins        uint64
		AvgDurationMS *float64
	}
	memberRow struct {
		MemberID, LastAction string
		LastSeen             time.Time
		Actions              uint64
	}
)

func (r *pageTotalsRow) fields() []any   { return []any{&r.Day, &r.TotalMS, &r.LastView} }
func (r *workspaceDayRow) fields() []any { return []any{&r.Day, &r.Logins, &r.AvgDurationMS} }
func (r *memberRow) fields() []any       { return []any{&r.MemberID, &r.LastAction, &r.LastSeen, &r.Actions} }

func (r *run) views(ctx context.Context, s *dynaflux.Store) (views, error) {
	c, queries := flux.New(s), r.h.p.queries
	from, to := r.at.Add(-48*time.Hour), r.at.Add(time.Hour)
	v := views{Pages: map[string]page{}, Quota: map[string]flux.QuotaUsedRow{}, Workspaces: map[string]workspace{}}
	var err error
	for name := range r.pages {
		var p page
		if p.Stats, err = c.PageStats(ctx, flux.PageStatsParams{Page: name, From: from, To: to}); err != nil {
			return views{}, fmt.Errorf("PageStats %s: %w", name, err)
		}
		p.Totals, err = fluxgen.QueryRows(ctx, s, queries, "E2EPageTotals", []any{&name, &from, &to}, (*pageTotalsRow).fields)
		if err != nil {
			return views{}, fmt.Errorf("E2EPageTotals %s: %w", name, err)
		}
		v.Pages[name] = p
	}
	if len(r.tags) > 0 {
		tags := slices.Sorted(maps.Keys(r.tags))
		if v.Tags, err = c.TagTotals(ctx, flux.TagTotalsParams{Tags: tags, Since: from}); err != nil {
			return views{}, fmt.Errorf("TagTotals: %w", err)
		}
	}
	for account := range r.accounts {
		row, qerr := c.QuotaUsed(ctx, flux.QuotaUsedParams{AccountID: account, At: r.at})
		switch {
		case qerr == nil:
			v.Quota[account] = row
		case !errors.Is(qerr, dynaflux.ErrNotFound):
			return views{}, fmt.Errorf("QuotaUsed %s: %w", account, qerr)
		}
	}
	for id := range r.workspaces {
		var w workspace
		if w.Summary, err = c.WorkspaceSummary(ctx, flux.WorkspaceSummaryParams{WorkspaceID: id, Since: r.at}); err != nil {
			return views{}, fmt.Errorf("WorkspaceSummary %s: %w", id, err)
		}
		w.Active, err = c.WorkspaceActiveMembers(ctx, flux.WorkspaceActiveMembersParams{WorkspaceID: id, From: from, To: to})
		if err != nil {
			return views{}, fmt.Errorf("WorkspaceActiveMembers %s: %w", id, err)
		}
		w.Days, err = fluxgen.QueryRows(ctx, s, queries, "E2EWorkspaceDays", []any{&id, &from, &to}, (*workspaceDayRow).fields)
		if err != nil {
			return views{}, fmt.Errorf("E2EWorkspaceDays %s: %w", id, err)
		}
		if w.Members, err = fluxgen.QueryRows(ctx, s, queries, "E2EMembers", []any{&id}, (*memberRow).fields); err != nil {
			return views{}, fmt.Errorf("E2EMembers %s: %w", id, err)
		}
		v.Workspaces[id] = w
	}
	return v, nil
}

// readPageViews reads every event of user in a region.
func readPageViews(ctx context.Context, c *flux.Client, user string) ([]flux.PageView, error) {
	var evs []flux.PageView
	for ev, err := range c.ReadPageViews(ctx, flux.PageViewsKey{UserID: user}) {
		if err != nil {
			return nil, fmt.Errorf("read page views of %s: %w", user, err)
		}
		evs = append(evs, ev)
	}
	return evs, nil
}

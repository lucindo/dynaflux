// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

const usersVersion = "site_users@v1"

func TestQueryNeedsALiveVersion(t *testing.T) {
	t.Parallel()
	activate := func(ctx context.Context, m *meta.Meta) error {
		_, err := m.Activate(ctx, usersVersion, "visits", meta.SplitFence, meta.DefaultBounds)
		return err
	}
	live := func(partialSince time.Time) func(context.Context, *meta.Meta) error {
		return func(ctx context.Context, m *meta.Meta) error {
			if err := activate(ctx, m); err != nil {
				return err
			}
			return m.MakeLive(ctx, usersVersion, partialSince)
		}
	}
	tests := []struct {
		name string
		set  func(context.Context, *meta.Meta) error
		want string // the error; "" for none
	}{
		{"pending without a state item", func(context.Context, *meta.Meta) error { return nil },
			"dynaflux: query Users: view version not ready: site_users@v1 is pending_backfill in us-east-1"},
		{"activating", activate,
			"dynaflux: query Users: view version not ready: site_users@v1 is activating in us-east-1"},
		{"live", live(time.Time{}), ""},
		{"live with a partial history", live(start.Add(-time.Hour)), ""},
		{"broken", func(ctx context.Context, m *meta.Meta) error {
			_, _, err := m.MarkBroken(ctx, usersVersion, "visits", meta.Failure{
				Cause: meta.CauseRecord, EventID: "01J9ZQ3ZQ8V7W6X5Y4Z3A2B1C0", Error: "intDiv: division by zero",
			})
			return err
		}, "dynaflux: query Users: view version not ready: site_users@v1 is broken in us-east-1: " +
			"event 01J9ZQ3ZQ8V7W6X5Y4Z3A2B1C0: intDiv: division by zero"},
		{"retired", func(ctx context.Context, m *meta.Meta) error {
			_, err := m.Retire(ctx, usersVersion, "visits")
			return err
		},
			"dynaflux: query Users: view version not ready: site_users@v1 is retired in us-east-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				b := mem.New()
				if err := tt.set(t.Context(), meta.New(b, catalog.Naming{Store: "app"}.MetaTable())); err != nil {
					t.Fatal(err)
				}
				rows, err := newEngine(b).Query(t.Context(), compile(t, queries), "Users", []expr.Value{strs("a")}, engine.QueryOptions{})
				switch {
				case tt.want == "" && (err != nil || len(rows) != 0):
					t.Errorf("Users = %v, %v; want no rows and no error", rows, err)
				case tt.want != "" && (!errors.Is(err, engine.ErrViewNotReady) || err.Error() != tt.want):
					t.Errorf("Users = %v, %v; want %q matching ErrViewNotReady", rows, err, tt.want)
				}
			})
		})
	}
}

func TestQueryAllowPartialReadsWithoutState(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newQueryStore(t, queries)
		s.visit(t, "a", "u1", 10, expr.Null(), start.Add(-time.Hour))
		f := meta.Failure{Cause: meta.CauseRecord, EventID: "01J9ZQ3ZQ8V7W6X5Y4Z3A2B1C0", Error: "boom"}
		if _, _, err := meta.New(s.mem, catalog.Naming{Store: "app"}.MetaTable()).MarkBroken(t.Context(), usersVersion, "visits", f); err != nil {
			t.Fatal(err)
		}
		s.reads.reset()
		got, err := s.queryWith(t, engine.QueryOptions{AllowPartial: true}, "Users", strs("a"))
		if n := s.reads.metaReads.Load(); err != nil || !slices.Equal(got, []string{"a u1 1"}) || n != 0 {
			t.Errorf("Users with AllowPartial = %q, %v after %d meta reads; want the broken version's row and no meta read", got, err, n)
		}
		if _, err = s.query(t, "Users", strs("a")); !errors.Is(err, engine.ErrViewNotReady) {
			t.Errorf("Users without AllowPartial: %v, want ErrViewNotReady", err)
		}
	})
}

func TestQueryReadsStatesOncePerMaxAge(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newQueryStore(t, queries)
		s.reads.reset()
		reads := func(query string, params ...expr.Value) int64 {
			t.Helper()
			if _, err := s.query(t, query, params...); err != nil {
				t.Fatal(err)
			}
			return s.reads.metaReads.Load()
		}
		if n := reads("Referrers", expr.String("a")); n != 0 {
			t.Errorf("a stream query read the states %d times, want 0", n)
		}
		reads("Users", strs("a"))
		if n := reads("Hours", expr.String("a"), expr.Micros(0), expr.Micros(1)); n != 1 {
			t.Errorf("two view queries read the states %d times, want once", n)
		}
		time.Sleep(meta.MaxViewStateAge)
		if n := reads("Users", strs("a")); n != 2 {
			t.Errorf("after MaxViewStateAge the states were read %d times in all, want 2", n)
		}
	})
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// build returns a core over f's store that runs another build of f's catalog: without the
// versions in drop, and listing retired as retired.
func (f *fixture) build(t *testing.T, drop, retired []string) *projector.Core {
	t.Helper()
	return f.buildOn(t, f.mem, drop, retired)
}

// buildOn is build over b.
func (f *fixture) buildOn(t *testing.T, b store.Backend, drop, retired []string) *projector.Core {
	t.Helper()
	def := *f.cat.Catalog()
	def.Views, def.Retired = nil, retired
	for _, v := range f.cat.Catalog().Views {
		v.Versions = slices.DeleteFunc(slices.Clone(v.Versions), func(vv catalog.ViewVersion) bool {
			return slices.Contains(drop, catalog.VersionID(v.Name, vv.Number))
		})
		if len(v.Versions) > 0 {
			def.Views = append(def.Views, v)
		}
	}
	p, err := projector.Compile(&def)
	if err != nil {
		t.Fatal(err)
	}
	return projector.New(p, b, f.archive, projector.Config{Naming: catalog.Naming{Store: "app"}, Region: region})
}

func handleWith(t *testing.T, core *projector.Core, recs ...projector.Record) (projector.Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	return core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: recs})
}

// setState moves page_totals@v1 to state through the moves a store makes.
func (f *fixture) setState(t *testing.T, state meta.State) {
	t.Helper()
	ctx := t.Context()
	var err error
	switch state {
	case meta.Activating:
		_, err = f.meta.Activate(ctx, pageTotals, "clicks", meta.SplitFence, meta.DefaultBounds)
	case meta.Live:
		if _, err = f.meta.Activate(ctx, pageTotals, "clicks", meta.SplitFence, meta.DefaultBounds); err == nil {
			err = f.meta.MakeLive(ctx, pageTotals, time.Time{})
		}
	case meta.Broken:
		failure := meta.Failure{Cause: meta.CauseRecord, Unit: "shardId-2", Seq: padSeq(t, seq(9)), EventID: "e", Error: "boom"}
		_, _, err = f.meta.MarkBroken(ctx, pageTotals, "clicks", failure)
	case meta.Retired:
		_, err = f.meta.Retire(ctx, pageTotals, "clicks")
	case meta.PendingBackfill:
	}
	if err != nil {
		t.Fatal(err)
	}
}

// TestHandleFailsOnAnActiveVersionItDoesNotDefine: a build whose catalog lacks a version that
// is activating or live fails every batch with an INSERT or a MODIFY (G3), even when its
// catalog has no view of the stream; a pending, broken or retired version, or one its catalog
// lists as retired, doesn't stop it.
func TestHandleFailsOnAnActiveVersionItDoesNotDefine(t *testing.T) {
	t.Parallel()
	clicks := []string{pageTotals, "page_users@v1", "page_visitors@v1", "page_rates@v1", "page_keys@v1", "slow_pages@v1", "user_tags@v1", "page_days@v1"}
	tests := []struct {
		name    string
		state   meta.State
		drop    []string
		retired []string
		modify  bool // a batch of one MODIFY instead of two INSERTs
		fails   bool
	}{
		{"activating", meta.Activating, []string{pageTotals}, nil, false, true},
		{"live", meta.Live, []string{pageTotals}, nil, false, true},
		{"live, in a MODIFY-only batch", meta.Live, []string{pageTotals}, nil, true, true},
		{"activating, with no view of the stream", meta.Activating, clicks, nil, false, true},
		{"pending", meta.PendingBackfill, []string{pageTotals}, nil, false, false},
		{"broken", meta.Broken, []string{pageTotals}, nil, false, false},
		{"retired", meta.Retired, []string{pageTotals}, nil, false, false},
		{"live, listed as retired", meta.Live, []string{pageTotals}, []string{pageTotals}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newPendingFixture(t, nil)
				f.setState(t, tt.state)
				a, b := f.click(t, "u1", "/a", 100), f.click(t, "u2", "/a", 200)
				recs := []projector.Record{insert(1, a), insert(2, b)}
				if tt.modify {
					recs = []projector.Record{{Op: projector.Modify, Seq: seq(1), Old: a, New: b}}
				}
				_, err := handleWith(t, f.build(t, tt.drop, tt.retired), recs...)
				checkUnknown(t, err, tt.fails, tt.state)
				want := []string{"", "0", ""}
				if !tt.fails {
					want = []string{padSeq(t, seq(2)), "2", "/a 2"}
				}
				got := []string{f.unit(t).Progress, strconv.Itoa(len(f.archive.seqs())), f.row(t, "page_visitors@v1", page("/a"))}
				if !slices.Equal(got, want) {
					t.Errorf("progress, records archived, page_visitors /a = %q, want %q", got, want)
				}
			})
		})
	}
}

// checkUnknown checks that err is ErrUnknownVersion, naming page_totals@v1 in state and the
// fix, when fails is set, and nil otherwise.
func checkUnknown(t *testing.T, err error, fails bool, state meta.State) {
	t.Helper()
	if !fails {
		if err != nil {
			t.Fatalf("Handle = %v, want success", err)
		}
		return
	}
	fix := pageTotals + " of stream clicks is " + string(state) + "; deploy a build whose catalog defines it"
	if !errors.Is(err, projector.ErrUnknownVersion) || !strings.HasSuffix(err.Error(), fix) {
		t.Fatalf("Handle = %v, want ErrUnknownVersion ending with %q", err, fix)
	}
}

// TestHandleSettlesRetiredFences: a fence of a retired version doesn't stop progress, a build
// that lists a live version as retired retires it first, and a build that still defines a
// version the state calls retired leaves it alone.
func TestHandleSettlesRetiredFences(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		tombstone bool
		drop      []string
		retired   []string
	}{
		{"retired in meta, defined by the build", true, nil, nil},
		{"live in meta, listed as retired by the build", false, []string{pageTotals}, []string{pageTotals}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, nil)
				f.handle(t, insert(1, f.click(t, "u1", "/a", 100))) // fences page_totals at zero
				if tt.tombstone {
					f.setState(t, meta.Retired)
				}
				res, err := handleWith(t, f.build(t, tt.drop, tt.retired), insert(2, f.click(t, "u2", "/a", 200)))
				if err != nil || res.FailedSeq != "" {
					t.Fatalf("Handle = %+v, %v; want success", res, err)
				}
				u := f.unit(t)
				got := []string{u.Progress, u.Fences[pageTotals], f.row(t, pageTotals, page("/a")), f.row(t, "page_users@v1", page("/a")), string(f.state(t))}
				want := []string{padSeq(t, seq(2)), strings.Repeat("0", catalog.SequenceWidth), "/a 100 1.00 100 100 1 1", "/a 2", "retired"}
				if !slices.Equal(got, want) {
					t.Errorf("progress, page_totals fence and row, page_users row, page_totals state = %q, want %q", got, want)
				}
			})
		})
	}
}

// A build whose cached states predate an activation, and which meets the version's fence on
// its unit, reads the states again and fails under G3 at once.
func TestHandleRereadsStatesOnAFenceOfAnUnknownVersion(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newPendingFixture(t, nil)
		old := f.build(t, []string{pageTotals}, nil)
		if _, err := handleWith(t, old, insert(1, f.click(t, "u1", "/a", 100))); err != nil {
			t.Fatal(err)
		}
		f.activate(t, pageTotals)
		if _, err := f.meta.EnsureFence(t.Context(), f.unit(t), pageTotals); err != nil {
			t.Fatal(err)
		}
		_, err := handleWith(t, old, insert(2, f.click(t, "u2", "/a", 200)))
		checkUnknown(t, err, true, meta.Activating)
	})
}

// state returns page_totals@v1's stored state.
func (f *fixture) state(t *testing.T) meta.State {
	t.Helper()
	states, err := f.meta.ViewStates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return states[pageTotals].State
}

// failRetire fails every write of a retired view state.
type failRetire struct{ store.Backend }

func (b failRetire) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if it["pk"].Str == "view" && it["state"].Str == string(meta.Retired) {
		return errors.New("throttled")
	}
	return b.Backend.Put(ctx, table, it, c)
}

// A build that lists a live version as retired skips it only once its tombstone is stored:
// otherwise an older build deployed again would apply the version without the records this
// one skipped.
func TestHandleRetiresBeforeItSkips(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		f.handle(t, insert(1, f.click(t, "u1", "/a", 100)))
		drop := []string{pageTotals}
		_, err := handleWith(t, f.buildOn(t, failRetire{f.mem}, drop, drop), insert(2, f.click(t, "u2", "/a", 200)))
		if err == nil || !strings.HasSuffix(err.Error(), "throttled") {
			t.Fatalf("Handle = %v, want the failed tombstone's error", err)
		}
		if got := []string{f.unit(t).Progress, string(f.state(t))}; !slices.Equal(got, []string{padSeq(t, seq(1)), "live"}) {
			t.Errorf("progress and state = %q, want the first record's and live", got)
		}
	})
}

// A build fails on an additive version split neither at fences nor by time, which it would
// count twice, and on one with uniqExact; an idempotent version without it takes any split.
func TestHandleFailsOnASplitItDoesNotApply(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		version string
		split   meta.Split
		live    bool
		err     string // what Handle's error adds to ErrSplit; "" when it succeeds
	}{
		{"additive, time split", pageTotals, meta.SplitTime, false, ""},
		{"additive, no split, live", pageTotals, meta.SplitNone, true, pageTotals + " of stream clicks is live with the none split; deploy a build that applies it"},
		{"uniqExact, no split, live", "page_users@v1", meta.SplitNone, true, "page_users@v1 of stream clicks is live with the none split; deploy a build that applies it"},
		{"idempotent, no split", "page_visitors@v1", meta.SplitNone, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newPendingFixture(t, nil)
				_, err := f.meta.Activate(t.Context(), tt.version, "clicks", tt.split, meta.DefaultBounds)
				if err == nil && tt.live {
					err = f.meta.MakeLive(t.Context(), tt.version, time.Time{})
				}
				if err != nil {
					t.Fatal(err)
				}
				_, err = handleWith(t, f.core, insert(1, f.click(t, "u1", "/a", 100)))
				got := []string{fmt.Sprint(err), f.unit(t).Progress}
				want := []string{"<nil>", padSeq(t, seq(1))}
				if tt.err != "" {
					want = []string{projector.ErrSplit.Error() + ": " + tt.err, ""}
				}
				if !slices.Equal(got, want) {
					t.Errorf("Handle error and progress = %q, want %q", got, want)
				}
			})
		})
	}
}

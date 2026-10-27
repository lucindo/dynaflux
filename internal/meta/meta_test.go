// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/store/storetest"
)

const (
	table   = "app-meta"
	stream  = "page_views"
	version = "page_daily@v2"
)

// bubbleStart is where the fake clock of a synctest bubble starts.
var bubbleStart = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) //nolint:gochecknoglobals // a constant time.Time

// seq returns sequence number n in its stored form.
func seq(n int) string { return fmt.Sprintf("%040d", n) }

func never(string) bool { return false }

func always(string) bool { return true }

type backend struct {
	name string
	new  func(t *testing.T) store.Backend
}

// memBackends are a plain in-memory store, one where every conditional write is applied and
// then reported as a failed condition, as after an SDK retry, and one whose eventually
// consistent reads see no write. Unlike [backends], they run in a synctest bubble.
func memBackends() []backend {
	return []backend{
		{"mem", func(*testing.T) store.Backend { return mem.New() }},
		{"retried", func(*testing.T) store.Backend { return storetest.RetriedWrite(mem.New()) }},
		{"stale reads", func(*testing.T) store.Backend { return newStaleReads() }},
	}
}

func newStaleReads() store.Backend { return storetest.StaleReads(mem.New(), mem.New()) }

// deletedAfterRead reads from snapshot and writes to a store without its items, as when an
// item expires between a read and the conditional write.
type deletedAfterRead struct {
	store.Backend

	snapshot store.Backend
}

func (b deletedAfterRead) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	return b.snapshot.Get(ctx, table, k, consistent)
}

var errSpun = errors.New("stuck: still writing")

// stuck fails every write's condition and returns the stored item unchanged, as a backend
// whose condition disagrees with the _v it returns would. After 1000 writes it returns
// errSpun, so a write loop without a bound ends.
type stuck struct {
	store.Backend

	puts int
}

func (b *stuck) Put(ctx context.Context, table string, it store.Item, _ store.Cond) error {
	if b.puts++; b.puts > 1000 {
		return errSpun
	}
	cur, err := b.Get(ctx, table, store.Key{PK: it["pk"].Str, SK: it["sk"].Str}, true)
	if err != nil {
		return err
	}
	return &store.ConditionError{Current: cur}
}

func readUnit(t *testing.T, m *meta.Meta, id string) meta.Unit {
	t.Helper()
	u, err := m.Unit(t.Context(), stream, id)
	if err != nil {
		t.Fatalf("Unit(%s): %v", id, err)
	}
	return u
}

func advance(t *testing.T, m *meta.Meta, id string, upTo string) {
	t.Helper()
	if err := m.Advance(t.Context(), readUnit(t, m, id), upTo, bubbleStart, always); err != nil {
		t.Fatalf("Advance(%s, %s): %v", id, upTo, err)
	}
}

func viewStates(t *testing.T, m *meta.Meta) map[string]meta.ViewState {
	t.Helper()
	states, err := m.ViewStates(t.Context())
	if err != nil {
		t.Fatalf("ViewStates: %v", err)
	}
	return states
}

func s(v string) store.Value { return store.Value{Type: store.S, Str: v} }

func n(v string) store.Value { return store.Value{Type: store.N, Str: v} }

func wantItem(t *testing.T, b store.Backend, want store.Item) {
	t.Helper()
	k := store.Key{PK: want["pk"].Str, SK: want["sk"].Str}
	got, err := b.Get(t.Context(), table, k, true)
	if err != nil {
		t.Fatalf("Get(%v): %v", k, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("item %v:\n got  %v\n want %v", k, got, want)
	}
}

// The stored layout is part of the storage format.
func TestItemLayout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := mem.New()
		m := meta.New(b, table)
		ctx := t.Context()
		deadline := bubbleStart.Add(30 * time.Second)
		if err := m.Advance(ctx, readUnit(t, m, "shardId-1"), seq(10), deadline, never); err != nil {
			t.Fatal(err)
		}
		if _, err := m.EnsureFence(ctx, readUnit(t, m, "shardId-1"), version); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Activate(ctx, version, stream, meta.SplitTime, meta.DefaultBounds); err != nil {
			t.Fatal(err)
		}
		if err := m.MakeLive(ctx, version, bubbleStart.Add(-24*time.Hour)); err != nil {
			t.Fatal(err)
		}
		f := meta.Failure{Cause: meta.CauseRecord, Unit: "shardId-1", Seq: seq(7), EventID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Error: "division by zero", At: bubbleStart.Add(time.Second)}
		if _, _, err := m.MarkBroken(ctx, version, stream, f); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		if _, err := m.Retire(ctx, version, stream); err != nil {
			t.Fatal(err)
		}

		wantItem(t, b, store.Item{
			"pk":          s("unit#page_views#" + keys.UnitKey("shardId-1")),
			"sk":          s("!"),
			"id":          s("shardId-1"),
			"progress":    s(seq(10)),
			"progress_at": n("946684830000000"),
			"fences":      {Type: store.M, Map: map[string]store.Value{version: s(seq(10))}},
			"_v":          n("2"),
			"_ttl":        n("981248400"), // 2000-01-01 + 400 days + 3600 s
		})
		wantItem(t, b, store.Item{
			"pk":            s("view"),
			"sk":            s(version),
			"state":         s("retired"),
			"stream":        s(stream),
			"activated_at":  n("946684800000000"),
			"split":         s("time"),
			"split_at":      n("946685190000000"), // + 90 s + 5 min
			"split_late":    n("946686390000000"), // + 5 min + 15 min more
			"partial_since": n("946598400000000"),
			"retired_at":    n("946684860000000"),
			"broken": {Type: store.M, Map: map[string]store.Value{
				"cause":    s("record"),
				"unit":     s("shardId-1"),
				"seq":      s(seq(7)),
				"event_id": s("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
				"error":    s("division by zero"),
				"at":       n("946684801000000"),
			}},
			"_v": n("4"),
		})
	})
}

func TestDecodeRejectsForeignItems(t *testing.T) {
	t.Parallel()
	unitPK := "unit#page_views#" + keys.UnitKey("shardId-1")
	unit := func(attrs store.Item) store.Item {
		it := store.Item{"pk": s(unitPK), "sk": s("!"), "id": s("shardId-1"), "_v": n("1"), "_ttl": n("1")}
		maps.Copy(it, attrs)
		return it
	}
	view := func(attrs store.Item) store.Item {
		it := store.Item{"pk": s("view"), "sk": s(version), "stream": s(stream), "_v": n("1")}
		maps.Copy(it, attrs)
		return it
	}
	activating := func(attrs store.Item) store.Item {
		it := store.Item{"state": s("activating"), "activated_at": n("1"), "split": s("fence")}
		maps.Copy(it, attrs)
		return view(it)
	}
	broken := func(attrs store.Item) store.Item {
		f := store.Item{"cause": s("record"), "unit": s("u1"), "seq": s(seq(1)), "event_id": s("e"), "error": s("boom"), "at": n("1")}
		maps.Copy(f, attrs)
		return view(store.Item{"state": s("broken"), "broken": {Type: store.M, Map: f}})
	}
	tests := []struct {
		name string
		item store.Item
	}{
		{"unit with an unknown attribute", unit(store.Item{"note": s("x")})},
		{"unit progress of 2 digits", unit(store.Item{"progress": s("10"), "progress_at": n("1")})},
		{"unit progress of zero", unit(store.Item{"progress": s(seq(0)), "progress_at": n("1")})},
		{"unit progress without progress_at", unit(store.Item{"progress": s(seq(10))})},
		{"unit progress_at without progress", unit(store.Item{"progress_at": n("1")})},
		{"unit fence as a number", unit(store.Item{"fences": {Type: store.M, Map: map[string]store.Value{version: n("10")}}})},
		{"unit version 0", unit(store.Item{"_v": n("0")})},
		{"unit without _ttl", func() store.Item { it := unit(nil); delete(it, "_ttl"); return it }()},
		{"view in an unknown state", view(store.Item{"state": s("paused")})},
		{"view without a stream", func() store.Item { it := activating(nil); delete(it, "stream"); return it }()},
		{"view with an empty stream", activating(store.Item{"stream": s("")})},
		{"view activating without activated_at", view(store.Item{"state": s("activating"), "split": s("fence")})},
		{"view activating without a split", view(store.Item{"state": s("activating"), "activated_at": n("1")})},
		{"view split by archive", activating(store.Item{"split": s("archive")})},
		{"view split by time without split_at", activating(store.Item{"split": s("time"), "split_late": n("2")})},
		{"view split by fences with split_at", activating(store.Item{"split_at": n("1"), "split_late": n("2")})},
		{"view activating with partial_since", activating(store.Item{"partial_since": n("1")})},
		{"view live with retired_at", activating(store.Item{"state": s("live"), "retired_at": n("1")})},
		{"view retired without retired_at", view(store.Item{"state": s("retired")})},
		{"view pending with a split", view(store.Item{"state": s("pending_backfill"), "split": s("fence")})},
		{"view failure without a cause", broken(store.Item{"cause": s("")})},
		{"view failure with a sequence number and no unit", broken(store.Item{"unit": s("")})},
		{"view failure without unit, sequence number or event_id", broken(store.Item{"unit": s(""), "seq": s(""), "event_id": s("")})},
		{"view broken without its failure", view(store.Item{"state": s("broken")})},
		{"view failure with an unknown attribute", broken(store.Item{"note": s("x")})},
		{"view failure at sequence number 7", broken(store.Item{"seq": s("7")})},
		{"view failure at sequence number 0", broken(store.Item{"seq": s(seq(0))})},
		{"view live with a failure", activating(store.Item{"state": s("live"), "broken": {Type: store.M, Map: map[string]store.Value{}}})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := mem.New()
			if err := b.Put(t.Context(), table, tt.item, store.Cond{}); err != nil {
				t.Fatal(err)
			}
			m := meta.New(b, table)
			var err error
			if tt.item["pk"].Str == "view" {
				_, err = m.ViewStates(t.Context())
			} else {
				_, err = m.Unit(t.Context(), stream, "shardId-1")
			}
			if err == nil || !strings.Contains(err.Error(), table) || !strings.Contains(err.Error(), tt.item["pk"].Str) {
				t.Errorf("got %v, want an error naming table %s and key %s", err, table, tt.item["pk"].Str)
			}
		})
	}
}

// An item deleted between the read and the conditional write is written again from no item:
// the stale unit's fence isn't carried over, and the version's state starts from
// pending_backfill.
func TestWriteAfterItemDeleted(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	snapshot := mem.New()
	before := meta.New(snapshot, table)
	advance(t, before, "u1", seq(10))
	if _, err := before.EnsureFence(ctx, readUnit(t, before, "u1"), version); err != nil {
		t.Fatal(err)
	}
	activate(t, before)

	stored := mem.New()
	m := meta.New(deletedAfterRead{Backend: stored, snapshot: snapshot}, table)
	stale := readUnit(t, m, "u1")
	if err := m.Advance(ctx, stale, seq(20), bubbleStart, always); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if _, err := m.EnsureFence(ctx, stale, "other@v1"); err != nil {
		t.Fatalf("EnsureFence: %v", err)
	}
	markBroken(t, m)

	after := meta.New(stored, table)
	if u := readUnit(t, after, "u1"); u.Progress != seq(20) || !maps.Equal(u.Fences, map[string]string{"other@v1": seq(20)}) {
		t.Errorf("unit %+v, want progress %s and only a fence of other@v1 at it", u, seq(20))
	}
	if vs := viewStates(t, after)[version]; vs.State != meta.Broken || !vs.ActivatedAt.IsZero() {
		t.Errorf("view state %+v, want broken and never activated", vs)
	}
}

// A backend whose condition never holds ends the write with an error naming the item, rather
// than writing until the context ends.
func TestWritesGiveUpWhenTheConditionNeverHolds(t *testing.T) {
	t.Parallel()
	writes := map[string]func(ctx context.Context, m *meta.Meta) error{
		"Advance": func(ctx context.Context, m *meta.Meta) error {
			u, err := m.Unit(ctx, stream, "u1")
			if err == nil {
				err = m.Advance(ctx, u, seq(20), bubbleStart, always)
			}
			return err
		},
		"EnsureFence": func(ctx context.Context, m *meta.Meta) error {
			u, err := m.Unit(ctx, stream, "u1")
			if err == nil {
				_, err = m.EnsureFence(ctx, u, "other@v1")
			}
			return err
		},
		"Activate": func(ctx context.Context, m *meta.Meta) error {
			_, err := m.Activate(ctx, "other@v1", stream, meta.SplitFence, meta.DefaultBounds)
			return err
		},
		"MakeLive": func(ctx context.Context, m *meta.Meta) error { return m.MakeLive(ctx, version, time.Time{}) },
		"MarkBroken": func(ctx context.Context, m *meta.Meta) error {
			_, _, err := m.MarkBroken(ctx, version, stream, failure())
			return err
		},
		"Retire": func(ctx context.Context, m *meta.Meta) error {
			_, err := m.Retire(ctx, version, stream)
			return err
		},
		"UpdateJob": func(ctx context.Context, m *meta.Meta) error {
			_, err := m.UpdateJob(ctx, version, func(j *meta.Job) (bool, error) { j.Rate = 10; return true, nil })
			return err
		},
		"RegisterCatalog": func(ctx context.Context, m *meta.Meta) error {
			_, err := m.RegisterCatalog(ctx, catalogA, "v2")
			return err
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b := mem.New()
			m := meta.New(b, table)
			advance(t, m, "u1", seq(10))
			activate(t, m)
			createJob(t, m, tableJob())
			if _, err := m.RegisterCatalog(t.Context(), catalogA, "v1"); err != nil {
				t.Fatal(err)
			}
			err := write(t.Context(), meta.New(&stuck{Backend: b}, table))
			if err == nil || errors.Is(err, errSpun) || !strings.Contains(err.Error(), table) {
				t.Errorf("got %v, want an error naming table %s", err, table)
			}
		})
	}
}

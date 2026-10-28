// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta_test

import (
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

const streamARN = "arn:aws:dynamodb:us-east-1:123456789012:table/app-stream-page_views/stream/2000-01-01T00:00:00.000"

func readStream(t *testing.T, m *meta.Meta) meta.StreamItem {
	t.Helper()
	s, _, err := m.Stream(t.Context(), stream)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	return s
}

func applyModes(t *testing.T, m *meta.Meta, old, next map[string]meta.ArchiveMode, create bool) {
	t.Helper()
	if err := m.ApplyArchiveModes(t.Context(), old, next, create, bubbleStart); err != nil {
		t.Fatalf("ApplyArchiveModes(%v, %v): %v", old, next, err)
	}
}

func origin(born time.Time) meta.Origin {
	return meta.Origin{At: bubbleStart.Add(time.Second), Hash: hashA, RegisteredAt: bubbleStart, Born: born, ARN: streamARN}
}

// reborn is the origin of the stream of a table re-created at born.
func reborn(born time.Time) meta.Origin {
	o := origin(born)
	o.ARN = "arn:aws:dynamodb:us-east-1:123456789012:table/app-stream-page_views/stream/" + born.UTC().Format("2006-01-02T15:04:05.000")
	return o
}

func recordOrigin(t *testing.T, m *meta.Meta, o meta.Origin) meta.StreamItem {
	t.Helper()
	s, err := m.RecordOrigin(t.Context(), readStream(t, m), o)
	if err != nil {
		t.Fatalf("RecordOrigin: %v", err)
	}
	return s
}

func causes(hs []meta.Hole) string {
	var out []string
	for _, h := range hs {
		c := string(h.Cause)
		if h.From != "" {
			c += " " + string(h.From) + ">" + string(h.To)
		}
		if h.Object != "" {
			c += " " + h.Object
		}
		out = append(out, c)
	}
	return strings.Join(out, ", ")
}

var all = map[string]meta.ArchiveMode{stream: meta.ArchiveAll} //nolint:gochecknoglobals // a constant

// The stored layout is part of the storage format.
func TestStreamItemLayout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := mem.New()
		m := meta.New(b, table)
		ctx := t.Context()
		applyModes(t, m, nil, all, true)
		recordOrigin(t, m, origin(bubbleStart.Add(-time.Hour)))
		time.Sleep(2 * time.Second)
		if err := m.ApplyArchiveModes(ctx, all, map[string]meta.ArchiveMode{stream: meta.ArchiveOff}, false, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := m.MarkLag(ctx, stream, bubbleStart.Add(3*time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := m.MarkErrors(ctx, stream, "errors/firehose/page_views/x.gz", bubbleStart.Add(4*time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := m.SetChecked(ctx, stream, bubbleStart.Add(5*time.Second), bubbleStart.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		hole := func(attrs map[string]store.Value) store.Value { return store.Value{Type: store.M, Map: attrs} }
		wantItem(t, b, store.Item{
			"pk":      s("stream#page_views"),
			"sk":      s("!"),
			"mode":    s("off"),
			"mode_at": n("946684802000000"),
			"origin": {Type: store.M, Map: map[string]store.Value{
				"at": n("946684801000000"), "hash": s(hashA), "registered_at": n("946684800000000"),
				"born": n("946681200000000"), "arn": s(streamARN), "holes": n("0"),
			}},
			"holes": {Type: store.L, List: []store.Value{
				hole(map[string]store.Value{"at": n("946684802000000"), "cause": s("update"), "from": s("all"), "to": s("off")}),
				hole(map[string]store.Value{"at": n("946684803000000"), "cause": s("lag")}),
				hole(map[string]store.Value{"at": n("946684804000000"), "cause": s("errors"), "object": s("errors/firehose/page_views/x.gz")}),
			}},
			"checked_through":  n("946684805000000"),
			"unchecked_before": n("946681200000000"),
			"_v":               n("6"),
		})
	})
}

func TestDecodeRejectsForeignStreamItems(t *testing.T) {
	t.Parallel()
	item := func(attrs store.Item) store.Item {
		it := store.Item{"pk": s("stream#page_views"), "sk": s("!"), "mode": s("all"), "mode_at": n("1"), "_v": n("1")}
		maps.Copy(it, attrs)
		return it
	}
	holes := func(hs ...map[string]store.Value) store.Value {
		l := store.Value{Type: store.L}
		for _, h := range hs {
			l.List = append(l.List, store.Value{Type: store.M, Map: h})
		}
		return l
	}
	o := func(attrs map[string]store.Value) store.Value {
		m := map[string]store.Value{"at": n("1"), "hash": s(hashA), "registered_at": n("1"), "born": n("1"), "arn": s(streamARN), "holes": n("0")}
		maps.Copy(m, attrs)
		return store.Value{Type: store.M, Map: m}
	}
	tests := []struct {
		name string
		item store.Item
	}{
		{"an unknown attribute", item(store.Item{"note": s("x")})},
		{"an unknown mode", item(store.Item{"mode": s("some")})},
		{"no mode", func() store.Item { it := item(nil); delete(it, "mode"); return it }()},
		{"no mode_at", func() store.Item { it := item(nil); delete(it, "mode_at"); return it }()},
		{"an origin after more holes than there are", item(store.Item{"origin": o(map[string]store.Value{"holes": n("1")})})},
		{"an origin without an ARN", item(store.Item{"origin": o(map[string]store.Value{"arn": s("")})})},
		{"an origin with an unknown attribute", item(store.Item{"origin": o(map[string]store.Value{"note": s("x")})})},
		{"a hole of an unknown cause", item(store.Item{"holes": holes(map[string]store.Value{"at": n("1"), "cause": s("flood")})})},
		{"a mode change without its modes", item(store.Item{"holes": holes(map[string]store.Value{"at": n("1"), "cause": s("update")})})},
		{"a mode change to an unknown mode", item(store.Item{"holes": holes(map[string]store.Value{
			"at": n("1"), "cause": s("create"), "from": s("off"), "to": s("some"),
		})})},
		{"an errors hole without its object", item(store.Item{"holes": holes(map[string]store.Value{"at": n("1"), "cause": s("errors")})})},
		{"a hole that isn't a map", item(store.Item{"holes": {Type: store.L, List: []store.Value{s("lag")}}})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := mem.New()
			if err := b.Put(t.Context(), table, tt.item, store.Cond{}); err != nil {
				t.Fatal(err)
			}
			_, _, err := meta.New(b, table).Stream(t.Context(), stream)
			if err == nil || !strings.Contains(err.Error(), table) || !strings.Contains(err.Error(), "stream#page_views") {
				t.Errorf("got %v, want an error naming table %s and key stream#page_views", err, table)
			}
		})
	}
}

// An origin counts the holes before it, is written only under mode all and only over the
// item it was read from, and replaces only a void one.
func TestRecordOrigin(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(t), table)
			checkFirstOrigin(t, m)
			checkVoidOrigin(t, m)
		})
	}
}

// checkFirstOrigin records an origin after a deploy stopped archiving and another started it
// again.
func checkFirstOrigin(t *testing.T, m *meta.Meta) {
	t.Helper()
	off := map[string]meta.ArchiveMode{stream: meta.ArchiveOff}
	applyModes(t, m, nil, all, true)
	applyModes(t, m, all, off, false)
	if _, err := m.RecordOrigin(t.Context(), readStream(t, m), origin(bubbleStart)); !errors.Is(err, meta.ErrTransition) {
		t.Errorf("RecordOrigin under mode off = %v, want ErrTransition", err)
	}
	applyModes(t, m, off, all, false)
	got := recordOrigin(t, m, origin(bubbleStart))
	if want := 2; got.Origin == nil || got.Origin.Holes != want || len(got.After()) != 0 {
		t.Fatalf("RecordOrigin = %+v, want an origin after both holes", got)
	}
	if !reflect.DeepEqual(readStream(t, m), got) {
		t.Errorf("Stream = %+v, want what RecordOrigin returned, %+v", readStream(t, m), got)
	}
	if _, err := m.RecordOrigin(t.Context(), got, origin(bubbleStart)); !errors.Is(err, meta.ErrTransition) {
		t.Errorf("RecordOrigin over a valid origin = %v, want ErrTransition", err)
	}
	// The oldest replica's region left: the earliest replica is younger, the region's stream the same.
	if _, err := m.RecordOrigin(t.Context(), got, origin(bubbleStart.Add(time.Hour))); !errors.Is(err, meta.ErrTransition) {
		t.Errorf("RecordOrigin over an origin whose replicas changed = %v, want ErrTransition", err)
	}
}

// checkVoidOrigin replaces the origin of a re-created stream table, whose lag stays behind,
// once it reads the item as it is.
func checkVoidOrigin(t *testing.T, m *meta.Meta) {
	t.Helper()
	if err := m.MarkLag(t.Context(), stream, bubbleStart); err != nil {
		t.Fatal(err)
	}
	read := readStream(t, m)
	if err := m.MarkErrors(t.Context(), stream, "errors/x", bubbleStart); err != nil {
		t.Fatal(err)
	}
	o := reborn(bubbleStart.Add(time.Hour))
	stored, err := m.RecordOrigin(t.Context(), read, o)
	if !errors.Is(err, meta.ErrStreamChanged) || len(stored.Holes) != 4 {
		t.Fatalf("RecordOrigin over a changed item = %+v, %v; want ErrStreamChanged and the stored item", stored, err)
	}
	got, err := m.RecordOrigin(t.Context(), stored, o)
	if err != nil || got.Origin.Holes != 4 || got.Origin.ARN != o.ARN || len(got.After()) != 0 {
		t.Errorf("RecordOrigin over a void origin = %+v, %v; want the new origin after the 4 holes", got, err)
	}
}

// A deploy writes a hole wherever a stream's mode changes to or from all, in either direction
// and on a rollback too, and never removes one. A new stream gets its mode and no hole.
func TestApplyArchiveModes(t *testing.T) {
	t.Parallel()
	const a, b, c = "a", "b", "c"
	first := map[string]meta.ArchiveMode{a: meta.ArchiveAll, b: meta.ArchiveLocal}
	second := map[string]meta.ArchiveMode{a: meta.ArchiveOff, b: meta.ArchiveOff, c: meta.ArchiveAll}
	steps := []struct {
		name      string
		old, next map[string]meta.ArchiveMode
		create    bool
		want      map[string]string // stream → mode, then its holes
	}{
		{"create", nil, first, true, map[string]string{a: "all:", b: "local:", c: ""}},
		{"update", first, second, false, map[string]string{a: "off:update all>off", b: "off:", c: "all:"}},
		{"rollback", second, first, false, map[string]string{a: "all:update all>off, update off>all", b: "local:", c: "off:update all>off"}},
		{"create again", nil, first, true, map[string]string{
			a: "all:update all>off, update off>all, create off>all", b: "local:", c: "off:update all>off",
		}},
	}
	for _, bk := range memBackends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				m := meta.New(bk.new(t), table)
				for _, st := range steps {
					applyModes(t, m, st.old, st.next, st.create)
					for _, name := range []string{a, b, c} {
						if got := modeAndHoles(t, m, name); got != st.want[name] {
							t.Errorf("%s: stream %s = %q, want %q", st.name, name, got, st.want[name])
						}
					}
				}
			})
		})
	}
}

// modeAndHoles returns the mode of the stream item of name, then its holes; "" without one.
func modeAndHoles(t *testing.T, m *meta.Meta, name string) string {
	t.Helper()
	it, ok, err := m.Stream(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return ""
	}
	return string(it.Mode) + ":" + causes(it.Holes)
}

func TestApplyArchiveModesRefusesAnUnknownMode(t *testing.T) {
	t.Parallel()
	b := mem.New()
	err := meta.New(b, table).ApplyArchiveModes(t.Context(), nil, map[string]meta.ArchiveMode{stream: "every"}, true, bubbleStart)
	if err == nil {
		t.Fatal("ApplyArchiveModes took mode every")
	}
	if it, _ := b.Get(t.Context(), table, store.Key{PK: "stream#page_views", SK: "!"}, true); it != nil {
		t.Errorf("ApplyArchiveModes wrote %v", it)
	}
}

// mark records a lag, and an error object named for the clock, then waits a second.
func mark(t *testing.T, m *meta.Meta) {
	t.Helper()
	if err := m.MarkLag(t.Context(), stream, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := m.MarkErrors(t.Context(), stream, "errors/"+time.Now().UTC().Format(time.TimeOnly), time.Now()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
}

// Each cause gets one hole per origin; a stream without an item gets none.
func TestMarkHolesOncePerOrigin(t *testing.T) {
	t.Parallel()
	for _, bk := range memBackends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				m := meta.New(bk.new(t), table)
				mark(t, m)
				if _, ok, err := m.Stream(t.Context(), stream); ok || err != nil {
					t.Fatalf("a stream without an item got one: %v", err)
				}
				applyModes(t, m, nil, all, true)
				mark(t, m)
				mark(t, m)
				recordOrigin(t, m, origin(bubbleStart))
				mark(t, m)
				mark(t, m)
				recordOrigin(t, m, reborn(bubbleStart.Add(time.Hour)))
				mark(t, m)
				want := "lag, errors errors/00:00:01, lag, errors errors/00:00:03, lag, errors errors/00:00:05"
				if got := readStream(t, m); causes(got.Holes) != want || len(got.After()) != 2 {
					t.Errorf("holes %s, %d after the origin; want %s, 2 after it", causes(got.Holes), len(got.After()), want)
				}
			})
		})
	}
}

// A check of projector activity only moves forward, and the hours it left unchecked stay
// recorded after later checks that start past them.
func TestSetChecked(t *testing.T) {
	t.Parallel()
	b := mem.New()
	m := meta.New(b, table)
	applyModes(t, m, nil, all, true)
	unchecked := bubbleStart.Add(-time.Hour)
	for _, c := range []struct{ through, unchecked time.Time }{
		{bubbleStart, unchecked},
		{bubbleStart.Add(2 * time.Hour), time.Time{}},
		{bubbleStart.Add(time.Hour), bubbleStart},
	} {
		if err := m.SetChecked(t.Context(), stream, c.through, c.unchecked); err != nil {
			t.Fatal(err)
		}
	}
	if got := readStream(t, m); !got.CheckedThrough.Equal(bubbleStart.Add(2*time.Hour)) || !got.Unchecked.Equal(unchecked) {
		t.Errorf("checked through %v, unchecked before %v; want %v and %v", got.CheckedThrough, got.Unchecked, bubbleStart.Add(2*time.Hour), unchecked)
	}
}

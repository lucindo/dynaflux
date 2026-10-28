// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package setup_test

import (
	"strings"
	"testing"
	"testing/synctest"

	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/setup"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// modeRequest is ArchiveMode's request as CloudFormation sends it: the modes by stream, and
// on an update the old ones.
func modeRequest(action string, modes, old map[string]any) setup.Request {
	p := func(streams map[string]any) map[string]any {
		return map[string]any{
			"ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:prod-analytics-setup", "ServiceTimeout": "300",
			"MetaTable": metaTable, "Streams": streams,
		}
	}
	r := setup.Request{Type: setup.TypeArchiveMode, Action: action, Props: p(modes)}
	if action != setup.ActionCreate {
		r.PhysicalID, r.OldProps = metaTable+"/archive-mode", p(old)
	}
	return r
}

// history returns each stream item of names as its mode, then its holes.
func history(t *testing.T, b store.Backend, names ...string) []string {
	t.Helper()
	var out []string
	for _, name := range names {
		s, ok, err := meta.New(b, metaTable).Stream(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			out = append(out, name+" -")
			continue
		}
		line := []string{name, string(s.Mode)}
		for _, h := range s.Holes {
			line = append(line, string(h.Cause)+":"+string(h.From)+">"+string(h.To))
		}
		out = append(out, strings.Join(line, " "))
	}
	return out
}

// Each request records the modes it sends, and a hole wherever a stream's mode changes to or
// from all between the old properties and the new, a rollback's swapped ones included.
func TestArchiveMode(t *testing.T) {
	t.Parallel()
	first := map[string]any{"clicks": "all", "views": "local"}
	second := map[string]any{"clicks": "off", "views": "off", "orders": "all"}
	steps := []struct {
		name string
		req  setup.Request
		want string
	}{
		{"create", modeRequest(setup.ActionCreate, first, nil), "clicks all|views local|orders -"},
		{"update: stop archiving, a new stream", modeRequest(setup.ActionUpdate, second, first),
			"clicks off update:all>off|views off|orders all"},
		{"rollback: start archiving, a removed stream", modeRequest(setup.ActionUpdate, first, second),
			"clicks all update:all>off update:off>all|views local|orders off update:all>off"},
		{"create over existing items", modeRequest(setup.ActionCreate, first, nil),
			"clicks all update:all>off update:off>all create:off>all|views local|orders off update:all>off"},
		{"delete", setup.Request{Type: setup.TypeArchiveMode, Action: setup.ActionDelete, PhysicalID: metaTable + "/archive-mode"},
			"clicks all update:all>off update:off>all create:off>all|views local|orders off update:all>off"},
	}
	synctest.Test(t, func(t *testing.T) {
		b := mem.New()
		h := setup.Handler{Meta: b}
		for _, st := range steps {
			resp, err := h.Handle(t.Context(), st.req)
			if err != nil || resp.PhysicalID != metaTable+"/archive-mode" || resp.Data != nil {
				t.Errorf("%s: got %+v, %v; want physical ID %s/archive-mode and no data", st.name, resp, err, metaTable)
			}
			if got := strings.Join(history(t, b, "clicks", "views", "orders"), "|"); got != st.want {
				t.Errorf("%s: streams %s, want %s", st.name, got, st.want)
			}
		}
	})
}

// A role that a deploy just granted can be refused for a while: the handler retries.
func TestArchiveModeRetriesAccessDenied(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := &failing{Mem: mem.New(), code: "AccessDeniedException", calls: 3}
		if _, err := (&setup.Handler{Meta: b}).Handle(t.Context(), modeRequest(setup.ActionCreate, map[string]any{"clicks": "all"}, nil)); err != nil {
			t.Fatal(err)
		}
		if got := history(t, b, "clicks"); got[0] != "clicks all" {
			t.Errorf("streams %v, want clicks all", got)
		}
	})
}

func TestArchiveModeRefusesBadProperties(t *testing.T) {
	t.Parallel()
	noTable := modeRequest(setup.ActionCreate, map[string]any{"clicks": "all"}, nil)
	delete(noTable.Props, "MetaTable")
	for _, tt := range []struct {
		req  setup.Request
		want string
	}{
		{noTable, "property MetaTable is missing"},
		{modeRequest(setup.ActionCreate, map[string]any{"clicks": "every"}, nil), `meta: archive mode "every" of clicks isn't all, local or off`},
		{modeRequest(setup.ActionUpdate, map[string]any{"clicks": "all"}, map[string]any{"clicks": "some"}), `meta: archive mode "some" of clicks isn't all, local or off`},
	} {
		b := mem.New()
		_, err := (&setup.Handler{Meta: b}).Handle(t.Context(), tt.req)
		if errorText(err) != tt.want || history(t, b, "clicks")[0] != "clicks -" {
			t.Errorf("got %v, want %s and no write", err, tt.want)
		}
	}
}

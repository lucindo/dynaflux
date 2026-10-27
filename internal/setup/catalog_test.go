// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package setup_test

import (
	"context"
	"errors"
	"iter"
	"maps"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/setup"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

const metaTable = "prod-analytics-meta"

const catalogSQL = `
CREATE STREAM clicks (user_id String, page String, ms UInt32) PRIMARY KEY (user_id);
CREATE MATERIALIZED VIEW kept ORDER BY (page) AS SELECT page, max(ms) AS longest FROM clicks GROUP BY page;
CREATE MATERIALIZED VIEW old ORDER BY (page) AS SELECT page, max(ms) AS longest FROM clicks GROUP BY page;
CREATE MATERIALIZED VIEW idle ORDER BY (page) AS SELECT page, max(ms) AS longest FROM clicks GROUP BY page;
`

// catalogs returns two catalogs of store analytics: before defines kept, old and idle, and
// after keeps kept and lists old@v1, idle@v1 and gone@v3 retired.
func catalogs(t *testing.T) ([]byte, []byte) {
	t.Helper()
	before := catalogtest.Marshal(t, "analytics", catalogSQL)
	cat, err := catalog.Unmarshal(before)
	if err != nil {
		t.Fatal(err)
	}
	kept, ok := cat.View("kept")
	if !ok {
		t.Fatal("no view kept")
	}
	cat.Views = []catalog.View{*kept}
	cat.Retired = []string{"old@v1", "idle@v1", "gone@v3"}
	after, err := catalog.Marshal(cat)
	if err != nil {
		t.Fatal(err)
	}
	return before, after
}

func catalogRequest(action, physicalID, hash string) setup.Request {
	return setup.Request{Type: setup.TypeCatalog, Action: action, PhysicalID: physicalID, Props: map[string]any{
		"ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:prod-analytics-setup", "ServiceTimeout": "300",
		"MetaTable": metaTable, "CatalogHash": hash, "Generator": "v0.5.0",
	}}
}

func physical(data []byte) string { return metaTable + "/" + catalog.Hash(data) }

// register makes each catalog current in turn, as earlier deploys did.
func register(t *testing.T, m *meta.Meta, catalogs ...[]byte) {
	t.Helper()
	for _, data := range catalogs {
		if _, err := m.RegisterCatalog(t.Context(), data, "v0.4.0"); err != nil {
			t.Fatal(err)
		}
	}
}

// makeLive gives each version of stream clicks a live state item.
func makeLive(t *testing.T, m *meta.Meta, versions ...string) {
	t.Helper()
	for _, v := range versions {
		if _, err := m.Activate(t.Context(), v, "clicks", meta.SplitFence, meta.DefaultBounds); err != nil {
			t.Fatal(err)
		}
		if err := m.MakeLive(t.Context(), v, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
}

// states returns each version's state and stream.
func states(t *testing.T, m *meta.Meta) map[string]string {
	t.Helper()
	vs, err := m.ViewStates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for id, s := range vs {
		out[id] = string(s.State) + " " + s.Stream
	}
	return out
}

func current(t *testing.T, m *meta.Meta) string {
	t.Helper()
	reg, ok, err := m.Current(t.Context())
	if err != nil || !ok {
		t.Fatalf("no current catalog: %v", err)
	}
	return reg.Hash + " " + reg.Generator
}

// Create and Update make the catalog the template names current: the binary's own, or one
// that an earlier deploy registered, which a rollback names.
func TestCatalogRegisters(t *testing.T) {
	t.Parallel()
	before, after := catalogs(t)
	tests := []struct {
		name       string
		registered [][]byte
		req        setup.Request
		wantID     string
		wantErr    string
		want       string // the current catalog's hash and generator
	}{
		{"create registers the binary's catalog", nil, catalogRequest(setup.ActionCreate, "", catalog.Hash(after)),
			physical(after), "", catalog.Hash(after) + " v0.5.0"},
		{"update to the binary's catalog", [][]byte{before}, catalogRequest(setup.ActionUpdate, physical(before), catalog.Hash(after)),
			physical(after), "", catalog.Hash(after) + " v0.5.0"},
		{"update back to a catalog registered before", [][]byte{before, after},
			catalogRequest(setup.ActionUpdate, physical(after), catalog.Hash(before)),
			physical(before), "", catalog.Hash(before) + " v0.5.0"},
		{"update to a catalog never registered", [][]byte{after}, catalogRequest(setup.ActionUpdate, physical(after), catalog.Hash(before)),
			physical(after), "catalog " + catalog.Hash(before) + " isn't the runtime binary's (" + catalog.Hash(after) +
				") and was never registered: deploy the templates and the binary from the same dynaflux generate run",
			catalog.Hash(after) + " v0.4.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := mem.New()
			m := meta.New(b, metaTable)
			register(t, m, tt.registered...)
			h := setup.Handler{Meta: b, Catalog: after}
			resp, err := h.Handle(t.Context(), tt.req)
			if got := errorText(err); got != tt.wantErr || resp.PhysicalID != tt.wantID || resp.Data != nil {
				t.Errorf("got %+v, %q; want physical ID %s, no data and error %q", resp, got, tt.wantID, tt.wantErr)
			}
			if got := current(t, m); got != tt.want {
				t.Errorf("current catalog %s, want %s", got, tt.want)
			}
		})
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// The versions that the current catalog lists retired get their tombstones on Create, and on
// the Delete that follows a successful update, which replaced the registration. A version
// without a state item takes its stream from the replaced catalog.
func TestCatalogRetires(t *testing.T) {
	t.Parallel()
	before, after := catalogs(t)
	live := map[string]string{"kept@v1": "live clicks", "old@v1": "live clicks"}
	tests := []struct {
		name       string
		registered [][]byte
		req        setup.Request
		want       map[string]string
	}{
		{"create", nil, catalogRequest(setup.ActionCreate, "", catalog.Hash(after)),
			map[string]string{"kept@v1": "live clicks", "old@v1": "retired clicks"}},
		{"update", [][]byte{before}, catalogRequest(setup.ActionUpdate, physical(before), catalog.Hash(after)), live},
		{"delete of the replaced registration", [][]byte{before, after}, catalogRequest(setup.ActionDelete, physical(before), catalog.Hash(before)),
			map[string]string{"kept@v1": "live clicks", "old@v1": "retired clicks", "idle@v1": "retired clicks"}},
		{"delete of the current registration, as a stack's", [][]byte{before, after},
			catalogRequest(setup.ActionDelete, physical(after), catalog.Hash(after)), live},
		{"delete after a rollback, whose catalog retires nothing", [][]byte{after, before},
			catalogRequest(setup.ActionDelete, physical(after), catalog.Hash(after)), live},
		{"delete of a registration never stored", [][]byte{after}, catalogRequest(setup.ActionDelete, "request-1", catalog.Hash(before)),
			map[string]string{"kept@v1": "live clicks", "old@v1": "retired clicks"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := mem.New()
			m := meta.New(b, metaTable)
			register(t, m, tt.registered...)
			makeLive(t, m, "kept@v1", "old@v1")
			h := setup.Handler{Meta: b, Catalog: after}
			if _, err := h.Handle(t.Context(), tt.req); err != nil {
				t.Fatal(err)
			}
			if got := states(t, m); !maps.Equal(got, tt.want) {
				t.Errorf("states %v, want %v", got, tt.want)
			}
		})
	}
}

// A stack's deletion may delete its meta table before the registration.
func TestCatalogDeleteWithoutMetaTable(t *testing.T) {
	t.Parallel()
	_, after := catalogs(t)
	h := setup.Handler{Meta: &failing{Mem: mem.New(), code: "ResourceNotFoundException", calls: -1}, Catalog: after}
	resp, err := h.Handle(t.Context(), catalogRequest(setup.ActionDelete, physical(after), catalog.Hash(after)))
	if err != nil || resp.PhysicalID != physical(after) {
		t.Errorf("got %+v, %v; want physical ID %s", resp, err, physical(after))
	}
}

// The setup role's grant on the meta table may be new in the same deploy, so the handler
// retries AccessDenied for about a minute.
func TestCatalogRetriesAccessDenied(t *testing.T) {
	t.Parallel()
	before, after := catalogs(t)
	tests := []struct {
		name   string
		req    setup.Request
		denied int
		ok     bool
	}{
		{"create denied briefly", catalogRequest(setup.ActionCreate, "", catalog.Hash(after)), 3, true},
		{"delete denied briefly", catalogRequest(setup.ActionDelete, physical(before), catalog.Hash(before)), 3, true},
		{"create denied for good", catalogRequest(setup.ActionCreate, "", catalog.Hash(after)), -1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				b := &failing{Mem: mem.New(), code: "AccessDeniedException"}
				m := meta.New(b, metaTable)
				register(t, m, before, after)
				makeLive(t, m, "old@v1")
				b.calls = tt.denied
				h := setup.Handler{Meta: b, Catalog: after}
				start := time.Now()
				_, err := h.Handle(t.Context(), tt.req)
				waited := time.Since(start)
				switch {
				case tt.ok && err != nil:
					t.Errorf("got %v, want success", err)
				case tt.ok:
					if got := states(t, m)["old@v1"]; got != "retired clicks" {
						t.Errorf("old@v1 is %s, want retired clicks", got)
					}
				case !errors.Is(err, setup.ErrAccessDenied):
					t.Errorf("got %v, want access denied", err)
				case waited > time.Minute || waited < 54*time.Second:
					t.Errorf("gave up after %v, want about a minute", waited)
				}
			})
		})
	}
}

func TestCatalogRefusesMissingProperties(t *testing.T) {
	t.Parallel()
	for _, prop := range []string{"MetaTable", "CatalogHash", "Generator"} {
		r := catalogRequest(setup.ActionCreate, "", "0123")
		delete(r.Props, prop)
		_, err := (&setup.Handler{Meta: mem.New()}).Handle(t.Context(), r)
		if want := "property " + prop + " is missing"; errorText(err) != want {
			t.Errorf("without %s: got %v, want %s", prop, err, want)
		}
	}
}

// apiError is an AWS API error as the SDK returns it, which the handler knows by its code.
type apiError string

func (e apiError) Error() string     { return "api error " + string(e) }
func (e apiError) ErrorCode() string { return string(e) }

// failing is a backend whose next calls fail with code: all of them when calls is negative.
type failing struct {
	*mem.Mem

	code  string
	calls int
}

func (f *failing) fail() error {
	if f.calls == 0 {
		return nil
	}
	f.calls--
	return apiError(f.code)
}

func (f *failing) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	if err := f.fail(); err != nil {
		return nil, err
	}
	return f.Mem.Get(ctx, table, k, consistent)
}

func (f *failing) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if err := f.fail(); err != nil {
		return err
	}
	return f.Mem.Put(ctx, table, it, c)
}

func (f *failing) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	if err := f.fail(); err != nil {
		return func(yield func(store.Item, error) bool) { yield(nil, err) }
	}
	return f.Mem.Query(ctx, table, q)
}

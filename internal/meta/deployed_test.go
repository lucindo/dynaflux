// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta_test

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// Catalog bytes and their SHA-256. Registering reads only their format.
var (
	catalogA = []byte(`{"format":1,"catalog":{"store":"app"}}`)            //nolint:gochecknoglobals // a constant
	catalogB = []byte(`{"format":1,"catalog":{"store":"app","views":[]}}`) //nolint:gochecknoglobals // a constant
)

const (
	hashA = "7fb96418def14e204e657c791f3e778f8433d7646c9645ccc1f502cc09f6de94"
	hashB = "c04b14d1207431a9a3f34625bb2b17984546e20bbd5f0de9de8906aab5eff756"
)

func register(t *testing.T, m *meta.Meta, data []byte, generator string) meta.Registered {
	t.Helper()
	reg, err := m.RegisterCatalog(t.Context(), data, generator)
	if err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}
	return reg
}

func current(t *testing.T, m *meta.Meta) meta.Registered {
	t.Helper()
	reg, ok, err := m.Current(t.Context())
	if err != nil || !ok {
		t.Fatalf("Current = %t, %v; want a catalog", ok, err)
	}
	return reg
}

func readCatalog(t *testing.T, m *meta.Meta, hash string) []byte {
	t.Helper()
	data, err := m.ReadCatalog(t.Context(), hash)
	if err != nil {
		t.Fatalf("ReadCatalog(%s): %v", hash, err)
	}
	return data
}

func gzipped(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestRegisterCatalog(t *testing.T) {
	t.Parallel()
	for _, bk := range memBackends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) { checkRegistration(t, bk.new(t)) })
		})
	}
}

func checkRegistration(t *testing.T, b store.Backend) {
	t.Helper()
	m := meta.New(b, table)
	if _, ok, err := m.Current(t.Context()); ok || err != nil {
		t.Fatalf("Current before a registration = %t, %v; want none", ok, err)
	}
	want := meta.Registered{Hash: hashA, Generator: "v0.5.0", Format: 1, At: bubbleStart}
	if got := register(t, m, catalogA, "v0.5.0"); got != want {
		t.Errorf("RegisterCatalog = %+v, want %+v", got, want)
	}
	wantItem(t, b, store.Item{
		"pk": s("catalog"), "sk": s("current"), "hash": s(hashA), "format": n("1"),
		"generator": s("v0.5.0"), "registered_at": n("946684800000000"), "_v": n("1"),
	})
	if got := current(t, m); got != want {
		t.Errorf("Current = %+v, want %+v", got, want)
	}
	if got := readCatalog(t, m, hashA); !bytes.Equal(got, catalogA) {
		t.Errorf("ReadCatalog = %s, want %s", got, catalogA)
	}
	// A repeated registration, as a retried deploy sends, keeps the item.
	time.Sleep(time.Minute)
	if got := register(t, m, catalogA, "v0.5.0"); got != want {
		t.Errorf("RegisterCatalog again = %+v, want %+v", got, want)
	}
}

// A rollback points the current item back at a catalog stored before, which stays readable.
func TestPointCatalog(t *testing.T) {
	t.Parallel()
	for _, bk := range backends() {
		t.Run(bk.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(bk.new(t), table)
			register(t, m, catalogA, "v0.5.0")
			register(t, m, catalogB, "v0.6.0")
			if got := current(t, m); got.Hash != hashB || got.Generator != "v0.6.0" {
				t.Errorf("Current = %+v, want %s by v0.6.0", got, hashB)
			}
			if err := m.PointCatalog(t.Context(), hashA, "v0.5.0"); err != nil {
				t.Fatal(err)
			}
			if got := current(t, m); got.Hash != hashA || got.Generator != "v0.5.0" || got.Format != 1 {
				t.Errorf("Current = %+v, want %s by v0.5.0", got, hashA)
			}
			if got := readCatalog(t, m, hashB); !bytes.Equal(got, catalogB) {
				t.Errorf("ReadCatalog(%s) = %s, want %s", hashB, got, catalogB)
			}
		})
	}
}

func TestPointCatalogNeedsAStoredBody(t *testing.T) {
	t.Parallel()
	m := meta.New(mem.New(), table)
	register(t, m, catalogA, "v0.5.0")
	if err := m.PointCatalog(t.Context(), hashB, "v0.6.0"); !errors.Is(err, meta.ErrNoCatalog) {
		t.Errorf("PointCatalog = %v, want ErrNoCatalog", err)
	}
	if got := current(t, m); got.Hash != hashA {
		t.Errorf("Current = %+v, want %s", got, hashA)
	}
}

func TestRegisterCatalogRejects(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		data      []byte
		generator string
	}{
		"no generator":   {catalogA, ""},
		"no format":      {[]byte(`{"catalog":{}}`), "v0.5.0"},
		"not a catalog":  {[]byte("catalog"), "v0.5.0"},
		"format 0":       {[]byte(`{"format":0}`), "v0.5.0"},
		"empty catalog":  {nil, "v0.5.0"},
		"format as text": {[]byte(`{"format":"1"}`), "v0.5.0"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(mem.New(), table)
			if _, err := m.RegisterCatalog(t.Context(), tt.data, tt.generator); err == nil {
				t.Error("RegisterCatalog succeeded")
			}
			if _, ok, err := m.Current(t.Context()); ok || err != nil {
				t.Errorf("Current = %t, %v; want none", ok, err)
			}
		})
	}
}

// A body is checked against its hash and size when read, so a damaged one is never decoded
// as a catalog.
func TestReadCatalogChecksTheBody(t *testing.T) {
	t.Parallel()
	tests := map[string]store.Value{
		"another catalog's bytes": b(gzipped(t, catalogB)...),
		"bytes that aren't gzip":  b(catalogA...),
		"a string":                s(string(catalogA)),
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bk := mem.New()
			it := store.Item{"pk": s("catalog#" + hashA), "sk": s("body"), "body": body}
			if err := bk.Put(t.Context(), table, it, store.Cond{}); err != nil {
				t.Fatal(err)
			}
			m := meta.New(bk, table)
			if _, err := m.ReadCatalog(t.Context(), hashA); err == nil || !strings.Contains(err.Error(), hashA) {
				t.Errorf("ReadCatalog = %v, want an error naming the item", err)
			}
			if _, err := m.RegisterCatalog(t.Context(), catalogA, "v0.5.0"); err == nil {
				t.Error("RegisterCatalog over a damaged body succeeded")
			}
		})
	}
}

func TestReadCatalogRejectsBadHashes(t *testing.T) {
	t.Parallel()
	m := meta.New(mem.New(), table)
	register(t, m, catalogA, "v0.5.0")
	for _, hash := range []string{"", strings.ToUpper(hashA), hashA[:62], hashA + "00", "#" + hashA[1:]} {
		if _, err := m.ReadCatalog(t.Context(), hash); err == nil || errors.Is(err, meta.ErrNoCatalog) {
			t.Errorf("ReadCatalog(%q) = %v, want a bad-hash error", hash, err)
		}
	}
}

// A body decompresses to at most 16 MiB, so a damaged or hostile one can't exhaust memory.
func TestReadCatalogCapsTheBody(t *testing.T) {
	t.Parallel()
	for size, ok := range map[int]bool{16 << 20: true, 16<<20 + 1: false} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			data := bytes.Repeat([]byte(" "), size)
			sum := sha256.Sum256(data)
			hash := hex.EncodeToString(sum[:])
			bk := mem.New()
			it := store.Item{"pk": s("catalog#" + hash), "sk": s("body"), "body": b(gzipped(t, data)...)}
			if err := bk.Put(t.Context(), table, it, store.Cond{}); err != nil {
				t.Fatal(err)
			}
			got, err := meta.New(bk, table).ReadCatalog(t.Context(), hash)
			if ok != (err == nil) || ok && len(got) != size {
				t.Errorf("ReadCatalog = %d bytes, %v; want ok=%t", len(got), err, ok)
			}
		})
	}
}

// The stored body is catalog.Compress's, the bytes that generate measures against DF6019.
func TestRegisterCatalogStoresWhatGenerateMeasures(t *testing.T) {
	t.Parallel()
	b := mem.New()
	register(t, meta.New(b, table), catalogA, "v0.5.0")
	it, err := b.Get(t.Context(), table, store.Key{PK: "catalog#" + hashA, SK: "body"}, true)
	if err != nil {
		t.Fatal(err)
	}
	want, err := catalog.Compress(catalogA)
	if err != nil {
		t.Fatal(err)
	}
	if got := it["body"].Bin; !bytes.Equal(got, want) {
		t.Errorf("body %x, want catalog.Compress's %x", got, want)
	}
}

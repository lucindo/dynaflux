// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package catalogtest builds catalogs from SQL for tests.
package catalogtest

import (
	"bytes"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/validate"
)

// Marshal returns the catalog, as dynaflux generate embeds it, of a store named store in
// us-east-1 whose one SQL file holds sql. It fails the test when the SQL doesn't compile.
func Marshal(tb testing.TB, store, sql string) []byte {
	tb.Helper()
	fsys := fstest.MapFS{
		"dynaflux.yaml":      {Data: []byte("store: " + store + "\nregions: [us-east-1]\n")},
		"dynaflux/store.sql": {Data: []byte(sql)},
	}
	_, def, ds := validate.Load(fsys, "/project")
	if def == nil {
		var b bytes.Buffer
		if err := diag.Write(&b, ds); err != nil {
			tb.Fatal(err)
		}
		tb.Fatalf("the store doesn't compile:\n%s", b.Bytes())
	}
	data, err := catalog.Marshal(def)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

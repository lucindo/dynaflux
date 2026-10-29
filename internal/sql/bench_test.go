// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package sql_test

import (
	"io/fs"
	"os"
	"testing"

	"github.com/lucindo/dynaflux/internal/sql"
)

// BenchmarkParse parses every SQL file of the example project, read once beforehand.
func BenchmarkParse(b *testing.B) {
	src := os.DirFS("../../examples/analytics/dynaflux")
	names, err := fs.Glob(src, "*.sql")
	if err != nil {
		b.Fatal(err)
	}
	nested, err := fs.Glob(src, "*/*.sql")
	if err != nil {
		b.Fatal(err)
	}
	names = append(names, nested...)
	if len(names) == 0 {
		b.Fatal("the example has no SQL files")
	}
	files := make([][]byte, len(names))
	size := 0
	for i, name := range names {
		if files[i], err = fs.ReadFile(src, name); err != nil {
			b.Fatal(err)
		}
		size += len(files[i])
	}
	b.SetBytes(int64(size))
	for b.Loop() {
		for i, name := range names {
			if _, ds := sql.Parse(name, files[i]); len(ds) > 0 {
				b.Fatalf("%s: %v", name, ds)
			}
		}
	}
}

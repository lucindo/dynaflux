// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate_test

import (
	"io/fs"
	"os"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/validate"
)

// exampleFS returns the example project's dynaflux.yaml, lock and SQL files, read once so
// that the benchmarks measure no file system.
func exampleFS(b *testing.B) fstest.MapFS {
	b.Helper()
	src := os.DirFS("../../examples/analytics")
	m := fstest.MapFS{}
	for _, pattern := range []string{"dynaflux.yaml", "dynaflux.lock", "dynaflux/*.sql", "dynaflux/*/*.sql"} {
		names, err := fs.Glob(src, pattern)
		if err != nil {
			b.Fatal(err)
		}
		for _, name := range names {
			data, rerr := fs.ReadFile(src, name)
			if rerr != nil {
				b.Fatal(rerr)
			}
			m[name] = &fstest.MapFile{Data: data}
		}
	}
	return m
}

// BenchmarkCompile validates and compiles the example project, as dynaflux validate and
// generate do: parse, resolve, check every guardrail, lay out, and number the views.
func BenchmarkCompile(b *testing.B) {
	files := exampleFS(b)
	for b.Loop() {
		if res, ds := validate.Compile(files, projectRoot); res == nil {
			b.Fatalf("the example doesn't compile: %v", ds)
		}
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra_test

import (
	"bytes"
	"flag"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/infra/cloudformation"
	"github.com/lucindo/dynaflux/internal/txtar"
	"github.com/lucindo/dynaflux/internal/validate"
)

var update = flag.Bool("update", false, "rewrite the want/ sections from the rendered files") //nolint:gochecknoglobals // go test flags are package variables

const (
	wantPrefix = "want/"
	// renders is how many times each case is built and rendered to check that the bytes
	// never change.
	renders = 10
)

// TestGolden builds and renders each archive under testdata/cases:
//   - Its files, other than the want/ sections, are a project, which validate loads.
//   - "-- want/<file> --" holds each rendered file. go test -update rewrites them; review
//     the diff.
//
// Every render of a case must give the same bytes. The cases are also the corpus that
// TestCfnLint lints.
func TestGolden(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/cases/*.txtar")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no cases in testdata/cases: %v", err)
	}
	for _, path := range paths {
		t.Run(strings.TrimSuffix(filepath.Base(path), ".txtar"), func(t *testing.T) {
			t.Parallel()
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			ar := txtar.Parse(data)
			files := render(t, ar)
			for range renders - 1 {
				if again := render(t, ar); !maps.EqualFunc(files, again, bytes.Equal) {
					t.Fatal("two renders of the same project differ")
				}
			}
			if *update {
				writeWant(t, path, ar, files)
				return
			}
			checkWant(t, ar, files)
		})
	}
}

// render builds the manifest of the project in ar and renders it.
func render(t *testing.T, ar *txtar.Archive) map[string][]byte {
	t.Helper()
	fsys := fstest.MapFS{}
	for _, f := range ar.Files {
		if !strings.HasPrefix(f.Name, wantPrefix) {
			fsys[f.Name] = &fstest.MapFile{Data: f.Data}
		}
	}
	cfg, cat, ds := validate.Load(fsys, "/project")
	if cat == nil {
		var b bytes.Buffer
		if err := diag.Write(&b, ds); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("the project doesn't compile:\n%s", b.Bytes())
	}
	m, err := infra.Build(cfg, cat, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	files, err := cloudformation.Render(m, "v0.0.0-test")
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func checkWant(t *testing.T, ar *txtar.Archive, files map[string][]byte) {
	t.Helper()
	want := map[string][]byte{}
	for _, f := range ar.Files {
		if name, ok := strings.CutPrefix(f.Name, wantPrefix); ok {
			want[name] = f.Data
		}
	}
	if got, w := slices.Sorted(maps.Keys(files)), slices.Sorted(maps.Keys(want)); !slices.Equal(got, w) {
		t.Fatalf("rendered files %q, want sections for %q", got, w)
	}
	for name, data := range files {
		if !bytes.Equal(data, want[name]) {
			t.Errorf("%s:\n%s\nwant:\n%s", name, data, want[name])
		}
	}
}

func writeWant(t *testing.T, path string, ar *txtar.Archive, files map[string][]byte) {
	t.Helper()
	ar.Files = slices.DeleteFunc(ar.Files, func(f txtar.File) bool { return strings.HasPrefix(f.Name, wantPrefix) })
	for _, name := range slices.Sorted(maps.Keys(files)) {
		ar.Files = append(ar.Files, txtar.File{Name: wantPrefix + name, Data: files[name]})
	}
	if err := os.WriteFile(path, txtar.Format(ar), 0o644); err != nil {
		t.Fatal(err)
	}
}

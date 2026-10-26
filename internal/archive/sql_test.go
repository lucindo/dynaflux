// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package archive_test

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

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/txtar"
)

var update = flag.Bool("update", false, "rewrite the want/ sections from the generated SQL") //nolint:gochecknoglobals // go test flags are package variables

const (
	minimalConfig = "store: analytics\nregions: [us-east-1]\n"
	wantPrefix    = "want/"
	db            = "prod_analytics"
	warehouse     = "s3://prod-analytics-123456789012-us-east-1-an/warehouse"
)

// TestGolden writes the archive schema of us-east-1 for each archive under testdata:
//   - Its files, other than the want/ sections, are a project. An archive without
//     dynaflux.yaml gets a minimal one.
//   - "-- want/<table>.create.sql --" and "-- want/<table>.dedup.sql --" hold each table's
//     CREATE TABLE and dedup view, and "-- want/fingerprint --" the schema's fingerprint.
//     go test -update rewrites them; review the diff.
func TestGolden(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/*.txtar")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no cases in testdata: %v", err)
	}
	for _, path := range paths {
		t.Run(strings.TrimSuffix(filepath.Base(path), ".txtar"), func(t *testing.T) {
			t.Parallel()
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			ar := txtar.Parse(data)
			files := generate(t, ar)
			if *update {
				writeWant(t, path, ar, files)
				return
			}
			checkWant(t, ar, files)
		})
	}
}

func TestAddColumns(t *testing.T) {
	t.Parallel()
	got := archive.AddColumns(db, archive.Table{Name: "page_views"}, []archive.Column{
		{Name: "clicks", Type: "bigint", Default: new("BIGINT '0'")},
		{Name: "date", Type: "date"},
	})
	want := "ALTER TABLE `prod_analytics`.`page_views` ADD COLUMNS (`clicks` bigint, `date` date)"
	if got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
}

func generate(t *testing.T, ar *txtar.Archive) map[string][]byte {
	t.Helper()
	fsys := fstest.MapFS{"dynaflux.yaml": {Data: []byte(minimalConfig)}}
	for _, f := range ar.Files {
		if !strings.HasPrefix(f.Name, wantPrefix) {
			fsys[f.Name] = &fstest.MapFile{Data: f.Data}
		}
	}
	s := archive.SchemaFor(loadProject(t, fsys), "us-east-1")
	files := map[string][]byte{"fingerprint": []byte(s.Fingerprint() + "\n")}
	for _, tb := range s.Tables {
		if err := tb.Check(); err != nil {
			t.Errorf("the generated table fails its check: %v", err)
		}
		files[tb.Name+".create.sql"] = []byte(archive.CreateTable(db, archive.Location(warehouse, db, tb.Name), tb) + "\n")
		files[tb.Name+".dedup.sql"] = []byte(archive.DedupView(db, tb) + "\n")
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
		t.Fatalf("generated files %q, want sections for %q", got, w)
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

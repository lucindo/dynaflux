// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
)

// project is a project root with SQL files where the skip rules matter.
func project() fstest.MapFS {
	file := &fstest.MapFile{}
	link := func(target string) *fstest.MapFile {
		return &fstest.MapFile{Data: []byte(target), Mode: fs.ModeSymlink}
	}
	return fstest.MapFS{
		"dynaflux/a.sql":              file,
		"dynaflux/views/daily/b.sql":  file,
		"dynaflux/.hidden.sql":        file,
		"dynaflux/.git/c.sql":         file,
		"dynaflux/_draft.sql":         file,
		"dynaflux/_drafts/d.sql":      file,
		"dynaflux/testdata/e.sql":     file,
		"dynaflux/Q.SQL":              file,
		"dynaflux/notes.txt":          file,
		"dynaflux/dir.sql/f.txt":      file,
		"dynaflux/link.sql":           link("a.sql"),
		"dynaflux/broken.sql":         link("missing.sql"),
		"dynaflux/views/to_views.sql": link("../views"),
		"other/o.sql":                 file,
	}
}

// expand loads a config with the given sql line and expands it on fsys.
func expand(t *testing.T, fsys fstest.MapFS, sql string) ([]string, []diag.Diagnostic) {
	t.Helper()
	fsys[config.FileName] = &fstest.MapFile{Data: []byte(base + sql)}
	cfg, ds := config.Load(fsys)
	if cfg == nil {
		t.Fatalf("Load: %v", ds)
	}
	return cfg.SQLFiles(fsys, "/project")
}

func TestSQLFiles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			"default pattern skips dot, underscore, testdata, directories and broken links",
			"",
			[]string{"dynaflux/a.sql", "dynaflux/link.sql", "dynaflux/views/daily/b.sql"},
		},
		{"** matches zero directories", "sql: [dynaflux/**/a.sql]\n", []string{"dynaflux/a.sql"}},
		{"leading **", "sql: ['**/*.sql']\n", []string{
			"dynaflux/a.sql", "dynaflux/link.sql", "dynaflux/views/daily/b.sql", "other/o.sql",
		}},
		{"* matches one directory", "sql: [dynaflux/*/*/*.sql]\n", []string{"dynaflux/views/daily/b.sql"}},
		{"literal underscore directory", "sql: [dynaflux/_drafts/*.sql]\n", []string{"dynaflux/_drafts/d.sql"}},
		{"literal testdata directory", "sql: [dynaflux/testdata/*.sql]\n", []string{"dynaflux/testdata/e.sql"}},
		{"literal dot file", "sql: [dynaflux/.hidden.sql]\n", []string{"dynaflux/.hidden.sql"}},
		{"wildcard after a literal underscore", "sql: [dynaflux/_*.sql]\n", []string{"dynaflux/_draft.sql"}},
		{"case-sensitive", "sql: [dynaflux/*.SQL]\n", []string{"dynaflux/Q.SQL"}},
		{"character class and ?", "sql: ['dynaflux/[a-b].sq?']\n", []string{"dynaflux/a.sql"}},
		{
			"overlapping patterns read each file once, in byte order",
			"sql: [other/*.sql, dynaflux/*.sql, dynaflux/a.sql]\n",
			[]string{"dynaflux/a.sql", "dynaflux/link.sql", "other/o.sql"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ds := expand(t, project(), tt.sql)
			if len(ds) != 0 || !slices.Equal(got, tt.want) {
				t.Errorf("SQLFiles = %q, %v; want %q", got, ds, tt.want)
			}
		})
	}
}

func TestSQLFilesDiagnostics(t *testing.T) {
	t.Parallel()
	escaping := project()
	escaping["dynaflux/up.sql"] = &fstest.MapFile{Data: []byte("../../secret.sql"), Mode: fs.ModeSymlink}
	escaping["dynaflux/abs.sql"] = &fstest.MapFile{Data: []byte("/etc/secret.sql"), Mode: fs.ModeSymlink}
	tests := []struct {
		name       string
		fsys       fstest.MapFS
		unreadable string // a directory that can't be listed
		sql        string
		want       []string
		files      []string
	}{
		{"DF6204 default pattern", fstest.MapFS{"other/o.sql": {}}, "", "", []string{"1:1 error DF6204"}, nil},
		{
			"DF6204 one pattern of two",
			project(), "", "sql: [other/*.sql, nothing/*.sql]\n",
			[]string{"3:20 error DF6204"}, []string{"other/o.sql"},
		},
		{"DF6204 ** matches directories only", project(), "", "sql: ['other/**']\n", []string{"3:7 error DF6204"}, nil},
		{
			"DF6205 links out of the root",
			escaping, "", "sql: [dynaflux/*.sql]\n",
			[]string{"3:7 error DF6205", "3:7 error DF6205"}, []string{"dynaflux/a.sql", "dynaflux/link.sql"},
		},
		{
			"DF6002 unreadable directory",
			project(), "dynaflux/views", "sql: [dynaflux/views/*/*.sql, other/*.sql]\n",
			[]string{"3:7 error DF6002"}, []string{"other/o.sql"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fsys[config.FileName] = &fstest.MapFile{Data: []byte(base + tt.sql)}
			cfg, ds := config.Load(tt.fsys)
			if cfg == nil {
				t.Fatalf("Load: %v", ds)
			}
			got, ds := cfg.SQLFiles(unreadable{tt.fsys, tt.unreadable}, "/project")
			if !slices.Equal(render(ds), tt.want) || !slices.Equal(got, tt.files) {
				t.Errorf("SQLFiles = %q, %q; want %q, %q\n%v", got, render(ds), tt.files, tt.want, ds)
			}
		})
	}
}

// The CLI expands the patterns on an [os.Root], whose links behave unlike those of
// [fstest.MapFS].
func TestSQLFilesOnRoot(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "project")
	if err := os.MkdirAll(filepath.Join(dir, "dynaflux/views"), 0o750); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"secret.sql":                   "",
		"project/dynaflux/a.sql":       "",
		"project/dynaflux/views/b.sql": "",
		"project/" + config.FileName:   base + "sql: [dynaflux/*.sql]\n",
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(tmp, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	absTarget := filepath.Join(dir, "dynaflux/a.sql")
	links := map[string]string{
		"dynaflux/link.sql":   "a.sql",
		"dynaflux/up.sql":     "../../secret.sql",
		"dynaflux/abs.sql":    absTarget, // os.Root refuses absolute links, even inside the root
		"dynaflux/views.sql":  "views",
		"dynaflux/broken.sql": "missing.sql",
	}
	for name, target := range links {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Skipf("can't create links: %v", err)
		}
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	cfg, ds := config.Load(root.FS())
	if cfg == nil {
		t.Fatalf("Load: %v", ds)
	}

	got, ds := cfg.SQLFiles(root.FS(), dir)
	const rule = "; links must be relative and stay inside the project root"
	want := []string{
		"dynaflux.yaml:3:7: error DF6205: dynaflux/abs.sql links to " + absTarget + rule,
		"dynaflux.yaml:3:7: error DF6205: dynaflux/up.sql links to ../../secret.sql" + rule,
	}
	var lines []string
	for _, d := range ds {
		line, _, _ := strings.Cut(d.String(), "\n")
		lines = append(lines, line)
	}
	if wantFiles := []string{"dynaflux/a.sql", "dynaflux/link.sql"}; !slices.Equal(got, wantFiles) || !slices.Equal(lines, want) {
		t.Errorf("SQLFiles = %q,\n%s\nwant %q,\n%s", got, strings.Join(lines, "\n"), wantFiles, strings.Join(want, "\n"))
	}
}

// A pasted pattern must not hang validate. Backtracking over ** would try every way to split
// the path of notes.txt, which doesn't match, among the 200 ** segments: about 10^11 ways,
// so a regression shows as a test timeout.
func TestSQLFilesManyDoubleStars(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"dynaflux/a/b/c/d/e/f/g.sql":     {},
		"dynaflux/a/b/c/d/e/f/notes.txt": {},
		"dynaflux/x.sql":                 {},
	}
	got, ds := expand(t, fsys, "sql: ['dynaflux/"+strings.Repeat("**/", 200)+"*.sql']\n")
	if want := []string{"dynaflux/a/b/c/d/e/f/g.sql", "dynaflux/x.sql"}; len(ds) != 0 || !slices.Equal(got, want) {
		t.Errorf("SQLFiles = %q, %v; want %q", got, ds, want)
	}
}

// unreadable fails to list the directory dir.
type unreadable struct {
	fstest.MapFS

	dir string
}

func (u unreadable) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == u.dir {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrPermission}
	}
	return u.MapFS.ReadDir(name)
}

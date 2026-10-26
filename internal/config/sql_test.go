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
		{"escaped literal underscore", "sql: ['dynaflux/\\_draft.sql']\n", []string{"dynaflux/_draft.sql"}},
		{"wildcard after an escaped underscore", "sql: ['dynaflux/\\_*.sql']\n", []string{"dynaflux/_draft.sql"}},
		{"escaped literal testdata directory", "sql: ['dynaflux/test\\data/*.sql']\n", []string{"dynaflux/testdata/e.sql"}},
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
	// Each case gets its own map: the subtests write dynaflux.yaml into it in parallel.
	escaping := func() fstest.MapFS {
		fsys := project()
		fsys["dynaflux/up.sql"] = &fstest.MapFile{Data: []byte("../../secret.sql"), Mode: fs.ModeSymlink}
		fsys["dynaflux/abs.sql"] = &fstest.MapFile{Data: []byte("/etc/secret.sql"), Mode: fs.ModeSymlink}
		return fsys
	}
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
			escaping(), "", "sql: [dynaflux/*.sql]\n",
			[]string{"3:7 error DF6205", "3:7 error DF6205"}, []string{"dynaflux/a.sql", "dynaflux/link.sql"},
		},
		{"DF6205 hides DF6204", escaping(), "", "sql: [dynaflux/up.sql]\n", []string{"3:7 error DF6205"}, nil},
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
// [fstest.MapFS]. The link rules don't depend on the file system: [os.DirFS] follows links
// out of the root, and the same links are DF6205 there.
func TestSQLFilesOnDisk(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "project")
	absTarget := filepath.Join(dir, "dynaflux/a.sql")
	tree(t, tmp, map[string]string{
		"secret.sql":                   "",
		"project/dynaflux/a.sql":       "",
		"project/dynaflux/views/b.sql": "",
		"project/" + config.FileName:   base + "sql: [dynaflux/*.sql]\n",
	}, map[string]string{
		"project/dynaflux/link.sql":   "a.sql",
		"project/dynaflux/up.sql":     "../../secret.sql",
		"project/dynaflux/abs.sql":    absTarget, // os.Root refuses absolute links, even inside the root
		"project/dynaflux/views.sql":  "views",
		"project/dynaflux/broken.sql": "missing.sql",
	})
	root, cfg := loadRoot(t, dir)
	const rule = "; links must be relative and stay inside the project root"
	want := []string{
		`dynaflux.yaml:3:7: error DF6205: "dynaflux/abs.sql" links to "` + absTarget + `"` + rule,
		`dynaflux.yaml:3:7: error DF6205: "dynaflux/up.sql" links to "../../secret.sql"` + rule,
	}
	wantFiles := []string{"dynaflux/a.sql", "dynaflux/link.sql"}
	for name, fsys := range map[string]fs.FS{"os.Root": root.FS(), "os.DirFS": os.DirFS(dir)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, ds := cfg.SQLFiles(fsys, dir)
			var lines []string
			for _, d := range ds {
				line, _, _ := strings.Cut(d.String(), "\n")
				lines = append(lines, line)
			}
			if !slices.Equal(got, wantFiles) || !slices.Equal(lines, want) {
				t.Errorf("SQLFiles = %q,\n%s\nwant %q,\n%s", got, strings.Join(lines, "\n"), wantFiles, strings.Join(want, "\n"))
			}
		})
	}
}

// A link whose own target stays inside the root, but resolves through a link to a directory
// outside, can't be read through an [os.Root]. The read error stops the pattern.
func TestSQLFilesLinkThroughLink(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	tree(t, tmp, map[string]string{
		"outside/s.sql":              "",
		"project/dynaflux/a.sql":     "",
		"project/" + config.FileName: base + "sql: [dynaflux/*.sql]\n",
	}, map[string]string{
		"project/dynaflux/ext":     "../../outside",
		"project/dynaflux/via.sql": "ext/s.sql",
	})
	root, cfg := loadRoot(t, filepath.Join(tmp, "project"))
	got, ds := cfg.SQLFiles(root.FS(), "/project")
	if !slices.Equal(render(ds), []string{"3:7 error DF6002"}) || !strings.Contains(ds[0].Message, "dynaflux/via.sql") ||
		len(got) != 0 {
		t.Errorf("SQLFiles = %q, %v; want no files and DF6002 for dynaflux/via.sql", got, ds)
	}
}

// tree creates files and links below dir, and skips the test where links can't be made.
func tree(t *testing.T, dir string, files, links map[string]string) {
	t.Helper()
	for name, data := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range links {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Skipf("can't create links: %v", err)
		}
	}
}

// loadRoot opens an [os.Root] at dir and loads its config.
func loadRoot(t *testing.T, dir string) (*os.Root, *config.Config) {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	cfg, ds := config.Load(root.FS())
	if cfg == nil {
		t.Fatalf("Load: %v", ds)
	}
	return root, cfg
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

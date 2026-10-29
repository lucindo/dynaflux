// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"maps"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/txtar"
)

// initNext is what init from a checkout prints last in a module whose go.mod doesn't
// require the library.
const initNext = `
Next:
  go get github.com/lucindo/dynaflux  add the Dynaflux library to go.mod
  dynaflux generate                   write the Go client package, the runtime's main.go and the templates
  go mod tidy                         add the modules that the generated code imports to go.mod
  dynaflux dev                        run the store on DynamoDB Local
`

func runInit(t *testing.T, dir string, vars map[string]string, args ...string) result {
	t.Helper()
	var stdout, stderr strings.Builder
	status := cli.RunEnv(t.Context(), append([]string{"init"}, args...), nil, &stdout, &stderr, dir, cli.Env{Vars: vars})
	return result{status, stdout.String(), stderr.String()}
}

func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestInit checks the files init writes against testdata/init.txtar, and that the project
// they make validates.
func TestInit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "go.mod"), "module example.com/shop\n")
	got := runInit(t, dir, nil, "-store", "shop", "-region", "eu-west-1", "-region", "us-east-1")
	want := result{0, "wrote dynaflux.yaml\nwrote dynaflux/queries/pages.sql\nwrote dynaflux/streams/page_views.sql\n" +
		"wrote dynaflux/views/page_daily.sql\n" + initNext, ""}
	if got != want {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
	data, err := os.ReadFile(filepath.Join("testdata", "init.txtar"))
	if err != nil {
		t.Fatal(err)
	}
	wantFiles := map[string][]byte{"go.mod": []byte("module example.com/shop\n")}
	for _, f := range txtar.Parse(data).Files {
		wantFiles[f.Name] = f.Data
	}
	if files := snapshot(t, dir); !maps.EqualFunc(files, wantFiles, slices.Equal) {
		t.Errorf("files:\n%q\nwant\n%q", files, wantFiles)
	}
	if v := run(dir, "validate"); v != (result{0, "", ""}) {
		t.Errorf("validate: %+v", v)
	}
}

func TestInitDefaults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, dir string
		vars      map[string]string
		want      string // the identity lines of dynaflux.yaml
	}{
		{"store from the directory", "Web_Shop", map[string]string{"AWS_REGION": "eu-central-1"}, "store: web-shop\nregions: [eu-central-1]\n"},
		{"no AWS_REGION", "shop", nil, "store: shop\nregions: [us-east-1]\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), tt.dir)
			writeFile(t, filepath.Join(dir, "go.mod"), "module example.com/shop\n")
			if got := runInit(t, dir, tt.vars); got.status != 0 || got.stderr != "" {
				t.Fatalf("got %+v", got)
			}
			yaml, err := os.ReadFile(filepath.Join(dir, "dynaflux.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(yaml), tt.want) {
				t.Errorf("dynaflux.yaml:\n%s\nwant it to hold\n%s", yaml, tt.want)
			}
		})
	}
}

// A released dynaflux asks for its own release of the library, which generate then finds in
// go.mod.
func TestInitPinsTheRelease(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "go.mod"), "module example.com/shop\n")
	var stdout, stderr strings.Builder
	build := &debug.BuildInfo{Main: debug.Module{Path: "github.com/lucindo/dynaflux", Version: "v1.2.0"}}
	status := cli.RunEnv(t.Context(), []string{"init", "-store", "shop"}, nil, &stdout, &stderr, dir, cli.Env{Build: build})
	want := "\nNext:\n" +
		"  go get github.com/lucindo/dynaflux@v1.2.0  add the Dynaflux library to go.mod\n" +
		"  dynaflux generate                          write the Go client package, the runtime's main.go and the templates\n" +
		"  go mod tidy                                add the modules that the generated code imports to go.mod\n" +
		"  dynaflux dev                               run the store on DynamoDB Local\n"
	if status != 0 || stderr.String() != "" || !strings.HasSuffix(stdout.String(), want) {
		t.Errorf("got %d, %q, %q\nwant status 0 and stdout ending in\n%s", status, stdout.String(), stderr.String(), want)
	}
}

// A go.mod that requires the library, as after go get -tool, needs no go get.
func TestInitInModuleWithLibrary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "go.mod"), "module example.com/shop\n\nrequire (\n\tgithub.com/lucindo/dynaflux v1.0.0 // indirect\n)\n")
	got := runInit(t, dir, nil, "-store", "shop")
	want := "\nNext:\n" +
		"  dynaflux generate  write the Go client package, the runtime's main.go and the templates\n" +
		"  go mod tidy        add the modules that the generated code imports to go.mod\n" +
		"  dynaflux dev       run the store on DynamoDB Local\n"
	if got.status != 0 || got.stderr != "" || !strings.HasSuffix(got.stdout, want) {
		t.Errorf("got %+v\nwant status 0 and stdout ending in\n%s", got, want)
	}
}

// Without a go.mod, the first step makes one.
func TestInitWithoutModule(t *testing.T) {
	t.Parallel()
	got := runInit(t, t.TempDir(), nil, "-store", "shop")
	want := "dynaflux init: warning: no go.mod here; dynaflux.yaml belongs at the root of a Go module (go mod init)\n"
	next := "\nNext:\n" +
		"  go mod init <module path>           make this directory a Go module\n" +
		"  go get github.com/lucindo/dynaflux  add the Dynaflux library to go.mod\n" +
		"  dynaflux generate                   write the Go client package, the runtime's main.go and the templates\n" +
		"  go mod tidy                         add the modules that the generated code imports to go.mod\n" +
		"  dynaflux dev                        run the store on DynamoDB Local\n"
	if got.status != 0 || got.stderr != want || !strings.HasSuffix(got.stdout, next) {
		t.Errorf("got %+v\nwant status 0, stderr %q and stdout ending in\n%s", got, want, next)
	}
}

// TestInitRefuses checks each refusal, and that a refused init writes nothing.
func TestInitRefuses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		dir    string            // the working directory, under the temporary one
		files  map[string]string // what the temporary directory holds before
		args   []string
		status int
		stderr string // with <dir> for the temporary directory
	}{
		{
			"project in a parent", "app/sub", map[string]string{"app/dynaflux.yaml": "store: app\n"}, []string{"-store", "shop"},
			1, "dynaflux init: <dir>/app/dynaflux.yaml already exists, so this directory is in that project\n",
		},
		{
			"file it would write", "app", map[string]string{"app/dynaflux/views/page_daily.sql": "-- mine\n"}, []string{"-store", "shop"},
			1, "dynaflux init: dynaflux/views/page_daily.sql exists; init writes nothing over your files\n",
		},
		{
			"invalid store name", "app", nil, []string{"-store", "Shop"},
			2, "dynaflux init: the project wouldn't be valid; nothing written\n" +
				"dynaflux.yaml:3:8: error DF6101: store is \"Shop\", which isn't a valid store name\n" +
				"  hint: use lowercase letters, digits and -; start with a letter; don't end with -. For example analytics\n",
		},
		{
			"directory name that isn't a store name", "1app", nil, nil,
			2, "dynaflux init: the project wouldn't be valid; nothing written\n" +
				"dynaflux.yaml:3:8: error DF6101: store is \"1app\", which isn't a valid store name\n" +
				"  hint: use lowercase letters, digits and -; start with a letter; don't end with -. For example analytics\n" +
				"  hint: name the store with -store\n",
		},
		{
			"invalid region", "app", nil, []string{"-store", "shop", "-region", "mars-1"},
			2, "dynaflux init: the project wouldn't be valid; nothing written\n" +
				"dynaflux.yaml:4:11: error DF6103: \"mars-1\" isn't an AWS region code\n  hint: use a code such as us-east-1\n",
		},
		{
			"FIPS pseudo-region", "app", nil, []string{"-store", "shop", "-region", "fips-us-east-1"},
			2, "dynaflux init: the project wouldn't be valid; nothing written\n" +
				"dynaflux.yaml:4:11: error DF6103: \"fips-us-east-1\" is a FIPS pseudo-region\n" +
				"  hint: name the region, such as us-east-1, and set AWS_USE_FIPS_ENDPOINT=true to use FIPS endpoints\n",
		},
		{
			"an argument", "app", nil, []string{"shop"},
			2, "dynaflux init: unexpected arguments: shop\nRun 'dynaflux help init' for usage.\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for name, data := range tt.files {
				writeFile(t, filepath.Join(root, name), data)
			}
			dir := filepath.Join(root, tt.dir)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			before := snapshot(t, root)
			got := runInit(t, dir, nil, tt.args...)
			want := result{tt.status, "", strings.ReplaceAll(tt.stderr, "<dir>", root)}
			if got != want {
				t.Errorf("got  %+v\nwant %+v", got, want)
			}
			if after := snapshot(t, root); !maps.EqualFunc(before, after, slices.Equal) {
				t.Errorf("a refused init changed files: %q became %q", slices.Sorted(maps.Keys(before)), slices.Sorted(maps.Keys(after)))
			}
		})
	}
}

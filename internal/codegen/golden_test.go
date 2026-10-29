// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package codegen_test

import (
	"bytes"
	"errors"
	"flag"
	"go/ast"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/codegen"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/goname"
	"github.com/lucindo/dynaflux/internal/txtar"
	"github.com/lucindo/dynaflux/internal/validate"
)

var update = flag.Bool("update", false, "rewrite the want/ sections from the generated code") //nolint:gochecknoglobals // go test flags are package variables

const (
	minimalConfig = "store: analytics\nregions: [us-east-1, us-west-2]\n"
	wantPrefix    = "want/"
)

// TestGolden generates the client package of each archive under testdata:
//   - Its files, other than the want/ sections, are a project, which validate loads. An
//     archive without dynaflux.yaml gets a minimal one.
//   - "-- want/<file> --" holds each generated file. go test -update rewrites them; review
//     the diff.
//
// The generated package must also type-check against the packages it imports, so the
// templates can't drift from the signatures of fluxgen unnoticed; its catalogJSON must be
// the marshaled catalog; and every exported name it declares must be one that DF1207
// checks for collisions.
func TestGolden(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/*.txtar")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no cases in testdata: %v", err)
	}
	imp := &sourceImporter{imp: importer.ForCompiler(token.NewFileSet(), "source", nil)}
	for _, path := range paths {
		t.Run(strings.TrimSuffix(filepath.Base(path), ".txtar"), func(t *testing.T) {
			t.Parallel()
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			ar := txtar.Parse(data)
			cat, files := generate(t, ar)
			fset, parsed := parse(t, files)
			typeCheck(t, imp, cat, fset, parsed)
			checkDeclaredNames(t, cat, fset, parsed)
			if *update {
				writeWant(t, path, ar, files)
				return
			}
			checkWant(t, ar, files)
		})
	}
}

func generate(t *testing.T, ar *txtar.Archive) (*catalog.Catalog, map[string][]byte) {
	t.Helper()
	fsys := fstest.MapFS{"dynaflux.yaml": {Data: []byte(minimalConfig)}}
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
	files, err := codegen.Package(cat, codegen.Options{Package: cfg.Go.Package, Version: "v0.0.0-test"})
	if err != nil {
		t.Fatal(err)
	}
	return cat, files
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

func parse(t *testing.T, files map[string][]byte) (*token.FileSet, []*ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	var parsed []*ast.File
	for _, name := range slices.Sorted(maps.Keys(files)) {
		f, err := parser.ParseFile(fset, name, files[name], parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, f)
	}
	return fset, parsed
}

// typeCheck checks the generated package with go/types, and checks that its constant
// catalogJSON holds the bytes of catalog.Marshal.
func typeCheck(t *testing.T, imp types.Importer, cat *catalog.Catalog, fset *token.FileSet, parsed []*ast.File) {
	t.Helper()
	var errs []error
	conf := types.Config{Importer: imp, Error: func(err error) {
		errs = append(errs, err)
	}}
	pkg, err := conf.Check("example.com/app/internal/flux", fset, parsed, nil)
	if err != nil {
		t.Fatalf("the generated code doesn't type-check:\n%v", errors.Join(errs...))
	}
	want, err := catalog.Marshal(cat)
	if err != nil {
		t.Fatal(err)
	}
	c, ok := pkg.Scope().Lookup("catalogJSON").(*types.Const)
	if !ok || c.Val().Kind() != constant.String {
		t.Fatal("the generated code has no string constant catalogJSON")
	}
	if got := constant.StringVal(c.Val()); got != string(want) {
		t.Errorf("catalogJSON is\n%s\nwant the marshaled catalog:\n%s", got, want)
	}
}

// checkDeclaredNames checks that each exported name the package declares, methods
// included, is in goname.Reserved or derived from a stream or a query. DF1207 checks only
// those for collisions, so a template that declares another name must add it to Reserved.
func checkDeclaredNames(t *testing.T, cat *catalog.Catalog, fset *token.FileSet, parsed []*ast.File) {
	t.Helper()
	known := map[string]bool{}
	for _, name := range goname.Reserved() {
		known[name] = true
	}
	for _, s := range cat.Streams {
		n := goname.ForStream(s.Name, s.GoType)
		for _, name := range []string{n.Event, n.Key, n.Append, n.AppendBatch, n.Read} {
			known[name] = true
		}
	}
	for _, q := range cat.Queries {
		n := goname.ForQuery(q.Name)
		known[n.Method], known[n.Params], known[n.Row] = true, true, true
	}
	for _, f := range parsed {
		for _, id := range declaredNames(f) {
			if id.IsExported() && !known[id.Name] {
				t.Errorf("%s: %s isn't in goname.Reserved", fset.Position(id.Pos()), id.Name)
			}
		}
	}
}

// declaredNames returns the names that f declares at package level, and its methods.
func declaredNames(f *ast.File) []*ast.Ident {
	var ids []*ast.Ident
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			ids = append(ids, d.Name)
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					ids = append(ids, s.Name)
				case *ast.ValueSpec:
					ids = append(ids, s.Names...)
				}
			}
		}
	}
	return ids
}

// sourceImporter type-checks imported packages from source, one import at a time.
type sourceImporter struct {
	mu  sync.Mutex
	imp types.Importer
}

func (s *sourceImporter) Import(path string) (*types.Package, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.imp.Import(path)
}

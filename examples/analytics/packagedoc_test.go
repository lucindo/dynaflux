// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package analytics_test

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/doc/comment"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/fluxtest"
)

// packageDoc is the code of package dynaflux's doc, its blocks in order, then return nil.
//
//nolint:forbidigo // the doc's code prints what it reads
func packageDoc(ctx context.Context, store *dynaflux.Store) error {
	client := flux.New(store)

	view := flux.PageView{UserID: "u1", Page: "/home", EventTime: time.Now()}
	tok, err := client.AppendPageView(ctx, view, dynaflux.Waitable())
	if err != nil {
		return err
	}
	day := flux.PageStatsParams{Page: view.Page, From: view.EventTime, To: view.EventTime}
	rows, err := client.PageStats(ctx, day, dynaflux.WaitFor(tok, 5*time.Second))
	if err != nil {
		return err
	}
	for _, row := range rows {
		fmt.Println(row.Day, row.Views, row.Visitors)
	}

	key := flux.PageViewsKey{UserID: "u1"}
	for ev, err := range client.ReadPageViews(ctx, key, dynaflux.Since(time.Now().Add(-time.Hour))) {
		if err != nil {
			return err
		}
		fmt.Println(ev.Page, ev.EventTime)
	}

	switch ve, ok := errors.AsType[*dynaflux.ValueError](err); {
	case ok:
		fmt.Println("Dynaflux can't store the value of", ve.Name) // a column or query parameter
	case errors.Is(err, dynaflux.ErrDuplicate):
		// A stream with DEDUP BY stored the event before; tok names that event.
	case errors.Is(err, dynaflux.ErrWaitTimeout):
		// The projector hasn't handled the event yet: query again later.
	}
	return nil
}

// The code in package dynaflux's doc is packageDoc's, which compiles against the example's
// client and runs on a fluxtest store.
func TestPackageDoc(t *testing.T) {
	t.Parallel()
	blocks := docCode(t, "../../dynaflux.go")
	body := funcBody(t, "packagedoc_test.go", "packageDoc")
	if want := strings.Join(blocks, "\n") + "return nil\n"; body != want {
		t.Errorf("packageDoc's body:\n%s\nwant package dynaflux's code blocks, then return nil:\n%s", body, want)
	}
	synctest.Test(t, func(t *testing.T) {
		if err := packageDoc(t.Context(), fluxtest.NewStore(t, flux.Catalog)); err != nil {
			t.Error(err)
		}
	})
}

// docCode returns the code blocks of the package doc in file.
func docCode(t *testing.T, file string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ParseComments|parser.PackageClauseOnly)
	if err != nil {
		t.Fatal(err)
	}
	var blocks []string
	for _, b := range new(comment.Parser).Parse(f.Doc.Text()).Content {
		if code, ok := b.(*comment.Code); ok {
			blocks = append(blocks, code.Text)
		}
	}
	return blocks
}

// funcBody returns the body of function name in file, unindented once.
func funcBody(t *testing.T, file, name string) string {
	t.Helper()
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == name {
			body := src[fset.Position(fn.Body.Lbrace).Offset+2 : fset.Position(fn.Body.Rbrace).Offset]
			return strings.ReplaceAll("\n"+string(body), "\n\t", "\n")[1:]
		}
	}
	t.Fatalf("%s has no function %s", file, name)
	return ""
}

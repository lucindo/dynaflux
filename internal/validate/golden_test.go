// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/txtar"
	"github.com/lucindo/dynaflux/internal/validate"
)

var update = flag.Bool("update", false, "write the want-text sections from the actual output") //nolint:gochecknoglobals // go test flags are package variables

const (
	casesDir      = "testdata/cases"
	minimalConfig = "store: analytics\nregions: [us-east-1, us-west-2]\n"
	projectRoot   = "/project"
	// retired is the severity column of a code that no check reports any more.
	retired = "retired"
)

var (
	codeRow = regexp.MustCompile(`(?m)^\| (DF\d{4}) \| (error|warning|retired) \|`)
	wantRow = regexp.MustCompile(`^\S+:\d+:\d+: (error|warning) (DF\d{4})$`)
)

// deferredCodes are the codes that no case covers, with the reason.
func deferredCodes() map[string]string {
	return map[string]string{
		"DF6001": "Load gets the project root; finding dynaflux.yaml in a parent directory is the CLI's job",
		"DF6205": "internal/config's tests cover it; a txtar archive can't hold a symbolic link",
		"DF6016": "generate reports it against its previous templates, not validate",
		"DF6017": "generate reports it against its previous templates, not validate",
		"DF6018": "generate reports it against its previous templates, not validate",
		"DF6019": "generate reports it after compiling, not validate; internal/cli tests it",
	}
}

type goldenCase struct {
	name string // path under casesDir, without .txtar
	file string
	ar   *txtar.Archive
}

// TestGolden runs every archive under testdata/cases as a project, written from the specs
// alone:
//   - Its files, other than the want sections, are the project's files. An archive without
//     dynaflux.yaml gets the minimal one from docs/spec/config.md.
//   - "-- want --" has one line per diagnostic, in output order: path:line:col: severity CODE.
//     It lists every diagnostic, so an empty section means none.
//   - An optional "-- want-text --" holds the exact output, messages and hints included.
//     go test -update writes it for every case with output whose want lines match, except
//     under examples/, whose text is quoted from docs/spec/sql.md. Review what it writes
//     against the code tables before committing it.
//   - An optional "-- want-catalog --" is JSON that the marshaled catalog must match: an
//     object matches when each of its keys does, so a key it leaves out can hold anything,
//     and an array matches element by element. The catalog omits zero values, so false, 0,
//     "", null, [] and {} also match a missing key. It is written by hand from the specs,
//     and -update never touches it.
//
// Load must return sorted diagnostics, and a config and a catalog exactly when none is an
// error.
func TestGolden(t *testing.T) {
	t.Parallel()
	for _, c := range loadCases(t) {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			cfg, cat, ds := validate.Load(projectFS(c.ar), projectRoot)
			sorted := slices.Clone(ds)
			diag.Sort(sorted)
			if !slices.Equal(ds, sorted) {
				t.Error("diagnostics aren't sorted")
			}
			if checkWant(t, c, ds) {
				checkText(t, c, ds)
			}
			checkResults(t, c, cfg, cat, ds)
		})
	}
}

// TestEveryCodeHasCase checks the want sections against the code tables of the specs: each
// line names a code that isn't retired, with its severity, and every code that isn't retired
// has a case or a reason in deferredCodes.
func TestEveryCodeHasCase(t *testing.T) {
	t.Parallel()
	spec := specCodes(t)
	covered := map[string]bool{}
	for _, c := range loadCases(t) {
		want, ok := section(c.ar, "want")
		if !ok {
			continue // TestGolden reports it
		}
		for _, line := range lines(want.Data) {
			m := wantRow.FindStringSubmatch(line)
			switch {
			case m == nil:
				t.Errorf("%s: want line %q isn't path:line:col: severity CODE", c.name, line)
			case spec[m[2]] == "":
				t.Errorf("%s: %s isn't in the specs", c.name, m[2])
			case spec[m[2]] == retired:
				t.Errorf("%s: %s is retired", c.name, m[2])
			case spec[m[2]] != m[1]:
				t.Errorf("%s: %s has severity %s in the specs, not %s", c.name, m[2], spec[m[2]], m[1])
			default:
				covered[m[2]] = true
			}
		}
	}
	deferred := deferredCodes()
	for _, code := range slices.Sorted(maps.Keys(spec)) {
		if !covered[code] && deferred[code] == "" && spec[code] != retired {
			t.Errorf("%s has no case", code)
		}
	}
	for _, code := range slices.Sorted(maps.Keys(deferred)) {
		switch {
		case spec[code] == "", spec[code] == retired:
			t.Errorf("deferred code %s isn't a current code in the specs", code)
		case covered[code]:
			t.Errorf("deferred code %s has a case; remove it from deferredCodes", code)
		}
	}
}

func loadCases(t *testing.T) []goldenCase {
	t.Helper()
	var cases []goldenCase
	err := filepath.WalkDir(casesDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".txtar" {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(casesDir, p)
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(filepath.ToSlash(rel), ".txtar")
		cases = append(cases, goldenCase{name: name, file: p, ar: txtar.Parse(data)})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatalf("no cases in %s", casesDir)
	}
	return cases
}

func projectFS(ar *txtar.Archive) fstest.MapFS {
	fsys := fstest.MapFS{}
	for _, f := range ar.Files {
		switch f.Name {
		case "want", "want-text", "want-catalog":
		default:
			fsys[f.Name] = &fstest.MapFile{Data: f.Data}
		}
	}
	if _, ok := fsys["dynaflux.yaml"]; !ok {
		fsys["dynaflux.yaml"] = &fstest.MapFile{Data: []byte(minimalConfig)}
	}
	return fsys
}

func section(ar *txtar.Archive, name string) (*txtar.File, bool) {
	for i := range ar.Files {
		if ar.Files[i].Name == name {
			return &ar.Files[i], true
		}
	}
	return nil, false
}

func lines(data []byte) []string {
	var ls []string
	for l := range strings.Lines(string(data)) {
		if l = strings.TrimSpace(l); l != "" {
			ls = append(ls, l)
		}
	}
	return ls
}

// matches reports whether each diagnostic line starts with its want line, followed by ':'.
func matches(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !strings.HasPrefix(got[i], want[i]+":") {
			return false
		}
	}
	return true
}

// checkWant reports whether the diagnostics match the want section.
func checkWant(t *testing.T, c goldenCase, ds []diag.Diagnostic) bool {
	t.Helper()
	want, ok := section(c.ar, "want")
	if !ok {
		t.Error("no want section")
		return false
	}
	got := make([]string, len(ds))
	for i, d := range ds {
		got[i], _, _ = strings.Cut(d.String(), "\n")
	}
	if wantLines := lines(want.Data); !matches(got, wantLines) {
		t.Errorf("diagnostics:\n%s\nwant:\n%s", listing(got), listing(wantLines))
		return false
	}
	return true
}

func listing(ls []string) string {
	if len(ls) == 0 {
		return "(none)"
	}
	return strings.Join(ls, "\n")
}

func checkText(t *testing.T, c goldenCase, ds []diag.Diagnostic) {
	t.Helper()
	var b bytes.Buffer
	if err := diag.Write(&b, ds); err != nil {
		t.Fatal(err)
	}
	text, ok := section(c.ar, "want-text")
	if ok && bytes.Equal(b.Bytes(), text.Data) {
		return
	}
	if !*update || strings.HasPrefix(c.name, "examples/") {
		if ok {
			t.Errorf("output:\n%s\nwant-text:\n%s", b.Bytes(), text.Data)
		}
		return
	}
	switch {
	case ok:
		text.Data = b.Bytes()
	case b.Len() > 0:
		c.ar.Files = append(c.ar.Files, txtar.File{Name: "want-text", Data: b.Bytes()})
	default:
		return
	}
	if err := os.WriteFile(c.file, txtar.Format(c.ar), 0o644); err != nil {
		t.Fatal(err)
	}
}

func checkResults(t *testing.T, c goldenCase, cfg *config.Config, cat *catalog.Catalog, ds []diag.Diagnostic) {
	t.Helper()
	if diag.HasErrors(ds) {
		if cfg != nil || cat != nil {
			t.Error("errors, but the config or the catalog isn't nil")
		}
		return
	}
	if cfg == nil || cat == nil {
		t.Fatal("no errors, but the config or the catalog is nil")
	}
	got, err := catalog.Marshal(cat)
	if err != nil {
		t.Fatalf("no errors, but the catalog doesn't marshal: %v", err)
	}
	if want, ok := section(c.ar, "want-catalog"); ok {
		checkCatalog(t, got, want.Data)
	}
}

func checkCatalog(t *testing.T, got, want []byte) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("want-catalog: %v", err)
	}
	for _, m := range mismatches("", w, g) {
		t.Error(m)
	}
}

// mismatches returns where got doesn't match the pattern want, by JSON path.
func mismatches(path string, want, got any) []string {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return []string{fmt.Sprintf("%s: got %v, want an object", path, got)}
		}
		var out []string
		for _, k := range slices.Sorted(maps.Keys(w)) {
			gv, found := g[k]
			switch {
			case found:
				out = append(out, mismatches(path+"."+k, w[k], gv)...)
			case !isZero(w[k]):
				out = append(out, path+"."+k+": missing")
			}
		}
		return out
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return []string{fmt.Sprintf("%s: got %v, want %d elements", path, got, len(w))}
		}
		var out []string
		for i := range w {
			out = append(out, mismatches(fmt.Sprintf("%s[%d]", path, i), w[i], g[i])...)
		}
		return out
	}
	if want != got {
		return []string{fmt.Sprintf("%s: got %v, want %v", path, got, want)}
	}
	return nil
}

// isZero reports whether v is a JSON zero value, which the catalog JSON leaves out.
func isZero(v any) bool {
	switch v := v.(type) {
	case nil:
		return true
	case bool:
		return !v
	case float64:
		return v == 0
	case string:
		return v == ""
	case []any:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	}
	return false
}

// specCodes returns the codes in the code tables of the specs with their severity column:
// error, warning or retired.
func specCodes(t *testing.T) map[string]string {
	t.Helper()
	codes := map[string]string{}
	for _, p := range []string{"../../docs/spec/sql.md", "../../docs/spec/config.md"} {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range codeRow.FindAllSubmatch(data, -1) {
			codes[string(m[1])] = string(m[2])
		}
	}
	if len(codes) == 0 {
		t.Fatal("found no code tables in the specs")
	}
	return codes
}

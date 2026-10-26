// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/txtar"
	"github.com/lucindo/dynaflux/internal/validate"
)

// FuzzLoad checks that Load never panics on a SQL file, and that the catalog it returns
// without errors always marshals.
func FuzzLoad(f *testing.F) {
	walkErr := filepath.WalkDir(casesDir, func(p string, _ fs.DirEntry, err error) error {
		if err != nil || filepath.Ext(p) != ".txtar" {
			return err
		}
		data, err := os.ReadFile(p)
		var all []string
		for _, file := range txtar.Parse(data).Files {
			if strings.HasSuffix(file.Name, ".sql") {
				f.Add(string(file.Data))
				all = append(all, string(file.Data))
			}
		}
		// Views and queries check little without their sources, which are often in
		// other files of the case.
		if len(all) > 1 {
			f.Add(strings.Join(all, "\n;\n"))
		}
		return err
	})
	if walkErr != nil {
		f.Fatal(walkErr)
	}
	f.Fuzz(func(t *testing.T, src string) {
		fsys := fstest.MapFS{
			"dynaflux.yaml":  {Data: []byte(minimalConfig)},
			"dynaflux/f.sql": {Data: []byte(src)},
		}
		cfg, cat, ds := validate.Load(fsys, projectRoot)
		if diag.HasErrors(ds) {
			if cfg != nil || cat != nil {
				t.Error("errors, but the config or the catalog isn't nil")
			}
			return
		}
		if _, err := catalog.Marshal(cat); err != nil {
			t.Errorf("no errors, but the catalog doesn't marshal: %v", err)
		}
	})
}

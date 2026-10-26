// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/lucindo/dynaflux/internal/config"
)

func TestFind(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mkdir := func(rel string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(root, rel), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	write := func(rel string) string {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.WriteFile(p, []byte("store: analytics\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	mkdir("outer/project/dynaflux/views")
	outerFile := write("outer/" + config.FileName)
	projectFile := write("outer/project/" + config.FileName)
	mkdir("outer/project/nested/" + config.FileName) // a directory doesn't count
	mkdir("elsewhere")

	tests := []struct {
		name string
		dir  string
		want string
	}{
		{"in the directory", filepath.Join(root, "outer/project"), projectFile},
		{"nearest parent wins", filepath.Join(root, "outer/project/dynaflux/views"), projectFile},
		{"past a directory of the same name", filepath.Join(root, "outer/project/nested"), projectFile},
		{"outer project", filepath.Join(root, "outer"), outerFile},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := config.Find(tt.dir)
			if err != nil || got != tt.want {
				t.Errorf("Find(%s) = %q, %v; want %q", tt.dir, got, err, tt.want)
			}
		})
	}
	// Assumes that no directory above the temporary directory holds dynaflux.yaml.
	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		got, err := config.Find(filepath.Join(root, "elsewhere"))
		if !errors.Is(err, config.ErrNotFound) {
			t.Errorf("Find = %q, %v; want ErrNotFound", got, err)
		}
	})
	// A mistyped directory must not select the project of a parent.
	t.Run("missing directory", func(t *testing.T) {
		t.Parallel()
		got, err := config.Find(filepath.Join(root, "outer/project/missing"))
		if !errors.Is(err, fs.ErrNotExist) || errors.Is(err, config.ErrNotFound) {
			t.Errorf("Find = %q, %v; want fs.ErrNotExist", got, err)
		}
	})
	t.Run("file instead of a directory", func(t *testing.T) {
		t.Parallel()
		got, err := config.Find(projectFile)
		if err == nil || errors.Is(err, config.ErrNotFound) {
			t.Errorf("Find = %q, %v; want an error other than ErrNotFound", got, err)
		}
	})
}

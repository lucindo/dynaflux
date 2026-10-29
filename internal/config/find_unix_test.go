// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package config_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/lucindo/dynaflux/internal/config"
)

// Reading a FIFO blocks until a writer opens it, so Find must refuse it.
func TestFindRefusesFIFO(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(dir, config.FileName), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := config.Find(dir)
	if err == nil || errors.Is(err, config.ErrNotFound) {
		t.Errorf("Find = %q, %v; want an error other than ErrNotFound", got, err)
	}
}

// A dynaflux.yaml in a directory that anyone can write to, as /tmp, could be another user's,
// so Find refuses it rather than let it govern every directory below. A project directory of
// its own below such a directory is found as usual.
func TestFindRefusesSharedDirectory(t *testing.T) {
	t.Parallel()
	shared := t.TempDir()
	if err := os.Chmod(shared, 0o777|os.ModeSticky); err != nil {
		t.Fatal(err)
	}
	planted, project := filepath.Join(shared, config.FileName), filepath.Join(shared, "project")
	for _, d := range []string{filepath.Join(shared, "work"), project} {
		if err := os.Mkdir(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{planted, filepath.Join(project, config.FileName)} {
		if err := os.WriteFile(p, []byte("store: analytics\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := config.Find(filepath.Join(shared, "work")); err == nil || !strings.Contains(err.Error(), "anyone can write") {
		t.Errorf("Find below %s = %q, %v; want it refused", shared, got, err)
	}
	if got, err := config.Find(project); err != nil || got != filepath.Join(project, config.FileName) {
		t.Errorf("Find(%s) = %q, %v; want its own file", project, got, err)
	}
}

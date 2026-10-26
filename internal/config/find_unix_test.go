// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package config_test

import (
	"errors"
	"path/filepath"
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

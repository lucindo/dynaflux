// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// ErrNotFound means that neither the start directory nor any parent holds dynaflux.yaml
// (DF6001).
var ErrNotFound = errors.New("no " + FileName + " in the directory or any parent")

// Find returns the path of the dynaflux.yaml that governs dir: the one in dir or in its
// nearest parent, the way the go command finds go.mod. dir must be an existing directory.
func Find(dir string) (string, error) {
	start, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("find %s: %w", FileName, err)
	}
	// Walking up from a mistyped directory would select the project of a parent.
	switch info, statErr := os.Stat(start); {
	case statErr != nil:
		return "", fmt.Errorf("find %s: %w", FileName, statErr)
	case !info.IsDir():
		return "", fmt.Errorf("find %s: %s isn't a directory", FileName, start)
	}
	for d := start; ; {
		p := filepath.Join(d, FileName)
		info, statErr := os.Stat(p)
		if statErr == nil && !info.IsDir() {
			return p, nil
		}
		if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
			return "", fmt.Errorf("find %s: %w", FileName, statErr)
		}
		parent := filepath.Dir(d)
		if parent == d {
			return "", fmt.Errorf("%w: %s", ErrNotFound, start)
		}
		d = parent
	}
}

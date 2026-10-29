// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// ErrNotFound means that neither the start directory nor any parent holds dynaflux.yaml
// (DF6001).
var ErrNotFound = errors.New("no " + FileName + " in the directory or any parent")

// Find returns the path of the dynaflux.yaml that governs dir: the one in dir or in its
// nearest parent, the way the go command finds go.mod. dir must be an existing directory.
// The nearest entry of that name must be a regular file or a link to one, in a directory
// that only its owners can write to.
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
		switch {
		// Reading a FIFO would block, and skipping the entry would select a parent's project.
		case statErr == nil && !info.Mode().IsRegular():
			return "", fmt.Errorf("find %s: %s isn't a regular file", FileName, p)
		case statErr == nil:
			if err = shared(d, p); err != nil {
				return "", err
			}
			return p, nil
		case !errors.Is(statErr, fs.ErrNotExist):
			return "", fmt.Errorf("find %s: %w", FileName, statErr)
		}
		parent := filepath.Dir(d)
		if parent == d {
			return "", fmt.Errorf("%w: %s", ErrNotFound, start)
		}
		d = parent
	}
}

// shared fails when anyone can write to dir, which holds the dynaflux.yaml p: in /tmp, it
// could be another user's, and would govern every command run below it. Windows has no such
// permission bit.
func shared(dir, p string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("find %s: %w", FileName, err)
	}
	if info.Mode().Perm()&0o002 != 0 {
		return fmt.Errorf("find %s: %s is in %s, which anyone can write to, so another user could have put it there; "+
			"keep the project in a directory of its own", FileName, p, dir)
	}
	return nil
}

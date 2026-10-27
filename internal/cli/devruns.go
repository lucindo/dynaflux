// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"

	"github.com/lucindo/dynaflux/internal/backfill"
)

// dirRuns stages dev's jobs in a directory, as backfill.Runs: an object's name is its path
// there. A run of dev stages in a directory of its own, so a job that it resumes stages
// again.
type dirRuns struct{ root *os.Root }

func openDirRuns(dir string) (*dirRuns, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("staging: %w", err)
	}
	return &dirRuns{root}, nil
}

func (r *dirRuns) close() error { return r.root.Close() }

// Put implements backfill.Runs.
func (r *dirRuns) Put(_ context.Context, name string, write func(io.Writer) error) error {
	const dirPerm = 0o700
	if err := r.root.MkdirAll(path.Dir(name), dirPerm); err != nil {
		return fmt.Errorf("staging: %w", err)
	}
	f, err := r.root.Create(name)
	if err != nil {
		return fmt.Errorf("staging: %w", err)
	}
	w := bufio.NewWriter(f)
	err = write(w)
	if err == nil {
		err = w.Flush()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// ReadAt implements backfill.Runs. It returns fewer than n bytes past the object's end.
func (r *dirRuns) ReadAt(_ context.Context, name string, off, n int64) ([]byte, error) {
	f, err := r.root.Open(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%s: %w", name, backfill.ErrStagingMissing)
	}
	if err != nil {
		return nil, fmt.Errorf("staging: %w", err)
	}
	defer f.Close()
	if off < 0 {
		st, serr := f.Stat()
		if serr != nil {
			return nil, fmt.Errorf("staging: %w", serr)
		}
		off = max(0, st.Size()-n)
	}
	buf := make([]byte, max(0, n))
	k, err := f.ReadAt(buf, off)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return buf[:k], err
}

// DeletePrefix implements backfill.Runs. A job's prefixes end in a slash: they name a
// directory.
func (r *dirRuns) DeletePrefix(_ context.Context, prefix string) error {
	return r.root.RemoveAll(path.Clean(prefix))
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
)

// MemRuns keeps staged objects in memory: [Runs] for tests and the simulation harness. It is
// safe for concurrent use.
type MemRuns struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func NewMemRuns() *MemRuns { return &MemRuns{objects: map[string][]byte{}} }

// Put implements [Runs].
func (r *MemRuns) Put(_ context.Context, name string, write func(io.Writer) error) error {
	var b bytes.Buffer
	if err := write(&b); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.objects[name] = b.Bytes()
	return nil
}

// ReadAt implements [Runs]. It returns fewer than n bytes past the object's end.
func (r *MemRuns) ReadAt(_ context.Context, name string, off, n int64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, ok := r.objects[name]
	if !ok {
		return nil, fmt.Errorf("%s: %w", name, ErrStagingMissing)
	}
	size := int64(len(data))
	if off < 0 {
		off = max(0, size-n)
	}
	off = min(off, size)
	return slices.Clone(data[off:min(size, off+max(0, n))]), nil
}

// DeletePrefix implements [Runs].
func (r *MemRuns) DeletePrefix(_ context.Context, prefix string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	maps.DeleteFunc(r.objects, func(name string, _ []byte) bool { return strings.HasPrefix(name, prefix) })
	return nil
}

// Names returns the names of the objects under prefix, sorted.
func (r *MemRuns) Names(prefix string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for name := range r.objects {
		if strings.HasPrefix(name, prefix) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package mem

// Snapshots returns how many scan snapshots m holds.
func (m *Mem) Snapshots() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.scans)
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta

import "time"

// At returns when the read of the cached states was sent.
func (c *StateCache) At() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

// SetAt stamps the cached states with at, as if their read was sent then.
func (c *StateCache) SetAt(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = at
}

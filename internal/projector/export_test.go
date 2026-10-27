// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import "time"

// StatesAt returns when the read of the cached view states was sent.
func (c *Core) StatesAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statesAt
}

// SetStatesAt stamps the cached view states with at, as if their read was sent then.
func (c *Core) SetStatesAt(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statesAt = at
}

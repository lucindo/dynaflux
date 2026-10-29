// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !integration

package fluxruntime_test

import "testing"

// runTests runs the tests. With the integration tag, it also checks the projector's grants.
func runTests(m *testing.M) int { return m.Run() }

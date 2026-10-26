// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import "runtime/debug"

// ModuleVersion exposes moduleVersion: a test binary's build info can't show how the
// version reads in an installed binary or under go tool.
func ModuleVersion(bi *debug.BuildInfo) string {
	return moduleVersion(bi)
}

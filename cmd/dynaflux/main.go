// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Dynaflux checks a Dynaflux project's SQL definitions and dynaflux.yaml, and explains
// the layout and cost of its streams, views and queries. Run "dynaflux help" for the
// commands.
package main

import (
	"os"

	"github.com/lucindo/dynaflux/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr, "."))
}

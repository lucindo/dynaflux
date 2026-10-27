// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Dynaflux checks a Dynaflux project's SQL definitions and dynaflux.yaml, explains the
// layout and cost of its streams, views and queries, generates its typed Go client
// package and dynaflux.lock, and reads and operates the deployed store. Run "dynaflux help"
// for the commands.
package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/lucindo/dynaflux/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	// After the first interrupt, a second one kills the process.
	context.AfterFunc(ctx, stop)
	status := cli.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, ".")
	stop()
	os.Exit(status)
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"io"
	"log/slog"

	"github.com/aws/aws-lambda-go/cfn"
	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/lucindo/dynaflux/internal/archive/firehose"
	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/setup"
	"github.com/lucindo/dynaflux/internal/store"
)

// Env is what a function reads from its environment besides its store.
type Env struct {
	Mode, Region, Prefix, ArchiveSchema string
	LogLevel                            slog.Level
	Memory                              int64
}

// NewProjector returns the Invoke of a projector function that writes to b, archives
// through s and logs to w, as Run builds it.
func NewProjector(c *engine.Catalog, e Env, b store.Backend, s firehose.Sender, w io.Writer) func(context.Context, []byte) ([]byte, error) {
	p := newProjector(c, env{prefix: e.Prefix, region: e.Region, archiveSchema: e.ArchiveSchema}, b, s, newLogger(w, e.LogLevel))
	return p.Invoke
}

// ReadEnv runs readEnv over vars.
func ReadEnv(cat *catalog.Catalog, vars map[string]string) (Env, error) {
	e, err := readEnv(cat, func(k string) string { return vars[k] })
	return Env{Mode: e.mode, Region: e.region, Prefix: e.prefix, ArchiveSchema: e.archiveSchema, LogLevel: e.logLevel, Memory: e.memory}, err
}

// AWSConfig exposes awsConfig.
func AWSConfig(ctx context.Context, region string) (aws.Config, error) { return awsConfig(ctx, region) }

// NewSetup returns the setup function over h, logging to w, as Run builds it.
func NewSetup(h *setup.Handler, w io.Writer) func(context.Context, cfn.Event) (string, error) {
	return newSetup(h, newLogger(w, slog.LevelInfo))
}

// BackfillParts are what a backfill function reaches AWS with.
type BackfillParts struct {
	Store  store.Backend
	Units  backfill.Lister
	Tables backfill.Tables
	Lag    backfill.Lag
	Runs   func(bucket string) backfill.Runs
}

// NewBackfill returns the Invoke of a backfill function over p, logging to w, as Run builds
// it.
func NewBackfill(c *engine.Catalog, e Env, p BackfillParts, w io.Writer) (func(context.Context, []byte) ([]byte, error), error) {
	f, err := newBackfill(c, env{prefix: e.Prefix, region: e.Region, memory: e.Memory},
		backfillParts{store: p.Store, units: p.Units, tables: p.Tables, lag: p.Lag, runs: p.Runs}, newLogger(w, e.LogLevel))
	if err != nil {
		return nil, err
	}
	return f.Invoke, nil
}

// Cut exposes cut.
func Cut(s string, n int) string { return cut(s, n) }

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package fluxruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// backfillEvent is what the backfill state machine sends each step (infra.md §6.4).
type backfillEvent struct {
	Job       string `json:"job"`       // the version
	Attempt   int    `json:"attempt"`   // the attempt the execution runs
	Execution string `json:"execution"` // the execution's ARN
}

// backfillReply tells the state machine whether the job needs more steps, and how long to
// wait before the next.
type backfillReply struct {
	Done        bool `json:"done"`
	WaitSeconds int  `json:"wait_seconds"`
}

// backfillParts are what a backfill function reaches AWS with: the store, the adapters, and
// the runs and the archive of the bucket a name gives.
type backfillParts struct {
	store   store.Backend
	units   backfill.Lister
	tables  backfill.Tables
	lag     backfill.Lag
	runs    func(bucket string) backfill.Runs
	archive func(bucket string) archiveReader
}

// archiveReader reads a region's archive and what AWS shows of its streams' history.
type archiveReader interface {
	backfill.Archive
	backfill.Witness
}

// backfillFn serves DYNAFLUX_MODE=backfill: each invocation steps a job until its deadline
// less backfill.Reserve (ADR 0011).
type backfillFn struct {
	env     backfill.Env // without Runs and Archive, whose bucket names the account
	runs    func(bucket string) backfill.Runs
	archive func(bucket string) archiveReader
	region  string
	log     *slog.Logger
}

// backfillRows is how many rows a step writes at once (ADR 0011).
const backfillRows = 64

// newBackfill returns the backfill function of region e.region, whose steps hold e.memory
// bytes of staged records. Its core has no archiver: a backfill writes only view rows.
func newBackfill(c *engine.Catalog, e env, p backfillParts, log *slog.Logger) (*backfillFn, error) {
	cat := c.Catalog()
	data, err := catalog.Marshal(cat)
	if err != nil {
		return nil, err
	}
	naming := catalog.Naming{Prefix: e.prefix, Store: cat.Store}
	return &backfillFn{
		env: backfill.Env{
			Naming: naming, Catalog: cat, Hash: catalog.Hash(data), Meta: meta.New(p.store, naming.MetaTable()), Store: p.store,
			Core:  projector.New(c.Projector(), p.store, nil, projector.Config{Naming: naming, BackfillRows: backfillRows, Region: e.region}),
			Units: p.units, Tables: p.tables, Region: e.region,
			Lag: p.lag, Bounds: meta.DefaultBounds, Memory: e.memory, Log: log,
		},
		runs: p.runs, archive: p.archive, region: e.region, log: log,
	}, nil
}

// Invoke implements aws-lambda-go's Handler. An error fails the invocation, which the state
// machine retries.
func (f *backfillFn) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	var ev backfillEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		return nil, f.fail(ctx, ev, fmt.Errorf("fluxruntime: decode the event: %w", err))
	}
	if ev.Job == "" || ev.Attempt < 1 || ev.Execution == "" {
		return nil, f.fail(ctx, ev, fmt.Errorf("fluxruntime: the event %s lacks a job, an attempt or an execution", payload))
	}
	account, err := invokedAccount(ctx)
	if err != nil {
		return nil, f.fail(ctx, ev, err)
	}
	env, bucket := f.env, f.env.Naming.Bucket(account, f.region)
	env.Runs = f.runs(bucket)
	if f.archive != nil {
		a := f.archive(bucket)
		env.Archive, env.Witness = a, a
	}
	wait, done, err := backfill.Step(ctx, &env, ev.Job, ev.Attempt, ev.Execution)
	if err != nil {
		return nil, f.fail(ctx, ev, err)
	}
	// The state machine's Wait takes whole seconds.
	return json.Marshal(backfillReply{Done: done, WaitSeconds: int((wait + time.Second - 1) / time.Second)})
}

// fail logs why a step failed, at level ERROR, and returns err.
func (f *backfillFn) fail(ctx context.Context, ev backfillEvent, err error) error {
	f.log.LogAttrs(ctx, slog.LevelError, "step failed", slog.String("requestId", lambdaRequestID(ctx)),
		slog.String("job", ev.Job), slog.Int("attempt", ev.Attempt), slog.String("execution", ev.Execution),
		slog.String("error", err.Error()))
	return err
}

// invokedAccount returns the account ID in the ARN of the function the invocation called,
// arn:<partition>:lambda:<region>:<account>:function:<name>.
func invokedAccount(ctx context.Context) (string, error) {
	lc, ok := lambdacontext.FromContext(ctx)
	if !ok {
		return "", errors.New("fluxruntime: the invocation has no Lambda context")
	}
	parts := strings.Split(lc.InvokedFunctionArn, ":")
	if len(parts) < 6 || parts[4] == "" {
		return "", fmt.Errorf("fluxruntime: the invoked function's ARN %q names no account", lc.InvokedFunctionArn)
	}
	return parts[4], nil
}

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

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/archive/firehose"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/streamevent"
)

// Reasons that a failed invocation's log line gives.
const (
	reasonLifetime       = "lifetime"
	reasonInvalidRequest = "invalid_request"
	reasonFenced         = "fenced"
	reasonArchiveSchema  = "archive_schema"
	reasonArchive        = "archive"
	reasonEvent          = "event"
	reasonOther          = "other"
)

// projectorFn serves DYNAFLUX_MODE=projector. Its fields are set once, by newProjector:
// state kept from one invocation for the next would break pruning (S9 rule 1).
type projectorFn struct {
	core    *projector.Core
	streams map[string]string // stream table → stream
	guard   error             // the archive schema check (ADR 0008), nil when it passed
	log     *slog.Logger
	store   string // the metrics' Store dimension
}

// newProjector returns the projector of region e.region, which writes views to b and
// archives through s.
func newProjector(c *engine.Catalog, e env, b store.Backend, s firehose.Sender, log *slog.Logger) *projectorFn {
	cat := c.Catalog()
	naming := catalog.Naming{Prefix: e.prefix, Store: cat.Store}
	w := firehose.NewWriter(c.Projector(), naming, e.region, s)
	p := &projectorFn{
		core:    projector.New(c.Projector(), b, w, projector.Config{Naming: naming, Region: e.region}),
		streams: map[string]string{},
		guard:   archive.CheckSchema(cat, e.region, e.archiveSchema),
		log:     log,
		store:   naming.Base(),
	}
	for _, st := range cat.Streams {
		p.streams[naming.StreamTable(st.Name)] = st.Name
	}
	return p
}

// Invoke implements aws-lambda-go's Handler. It applies the batch synchronously with
// Lambda's context, whose deadline the core stops sending before, so no request is in
// flight when it returns. An error fails the invocation, and Lambda retries the batch.
func (p *projectorFn) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	resp := streamevent.Reply{BatchItemFailures: []streamevent.ItemFailure{}}
	var ev streamevent.Event
	if err := json.Unmarshal(payload, &ev); err != nil {
		return nil, p.fail(ctx, projector.Batch{}, reasonEvent, fmt.Errorf("fluxruntime: decode the event: %w", err))
	}
	if len(ev.Records) == 0 {
		// The window's final invocation; until it succeeds, Lambda stops the shard.
		return json.Marshal(resp)
	}
	b := projector.Batch{Unit: ev.ShardID}
	if b.Unit == "" {
		return nil, p.fail(ctx, b, reasonEvent, errors.New("fluxruntime: the event has records but no shardId: "+
			"the event source mapping needs TumblingWindowInSeconds (ADR 0003)"))
	}
	var ok bool
	if b.Stream, ok = p.streams[tableName(ev.EventSourceARN)]; !ok {
		return nil, p.fail(ctx, b, reasonEvent, fmt.Errorf("fluxruntime: %s isn't the stream of a stream table of store %s",
			ev.EventSourceARN, p.store))
	}
	if p.guard != nil {
		return nil, p.fail(ctx, b, reasonArchiveSchema, p.guard)
	}
	var err error
	if b.Records, err = streamevent.Records(ev.Records); err != nil {
		return nil, p.fail(ctx, b, reasonEvent, fmt.Errorf("fluxruntime: %w", err))
	}
	res, err := p.core.Handle(ctx, b)
	logCounts(ctx, p.log, p.store, b, res.Stats)
	if err != nil {
		return nil, p.fail(ctx, b, reason(err), err)
	}
	if res.FailedSeq != "" {
		attrs := append(batchAttrs(ctx, b), slog.String("failedSeq", res.FailedSeq), slog.String("error", cut(res.RowErr.Error(), errorBytes)))
		p.log.LogAttrs(ctx, slog.LevelWarn, "rows failed", attrs...)
		resp.BatchItemFailures = append(resp.BatchItemFailures, streamevent.ItemFailure{ItemIdentifier: res.FailedSeq})
	}
	return json.Marshal(resp)
}

// fail logs why the invocation fails, at level ERROR, and returns err. aws-lambda-go logs
// the returned error as plain text, which Lambda files as INFO.
func (p *projectorFn) fail(ctx context.Context, b projector.Batch, reason string, err error) error {
	attrs := append(batchAttrs(ctx, b), slog.String("reason", reason), slog.String("error", err.Error()))
	p.log.LogAttrs(ctx, slog.LevelError, "batch failed", attrs...)
	return err
}

// reason classifies an error of the core.
func reason(err error) string {
	switch {
	case errors.Is(err, projector.ErrLifetime):
		return reasonLifetime
	case errors.Is(err, meta.ErrFenced):
		return reasonFenced
	case errors.Is(err, projector.ErrArchive):
		return reasonArchive
	case errors.Is(err, store.ErrInvalidRequest):
		return reasonInvalidRequest
	}
	return reasonOther
}

// tableName returns the table of a stream ARN, arn:…:table/<table>/stream/<label>, or "".
// The ARN's region isn't read.
func tableName(arn string) string {
	_, rest, ok := strings.Cut(arn, ":table/")
	if !ok {
		return ""
	}
	table, _, ok := strings.Cut(rest, "/stream/")
	if !ok {
		return ""
	}
	return table
}

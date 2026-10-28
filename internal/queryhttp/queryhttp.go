// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package queryhttp serves a store's exposed named queries, those annotated
// -- expose: http, to Lambda function URLs, API Gateway HTTP and REST APIs, and direct
// invoke (ADR 0018). docs/spec/http.md is the wire format.
package queryhttp

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/store"
)

// maxPayload is Lambda's limit on a synchronous response. It counts the whole payload: a
// door's envelope escapes the body's quotes, which can double its size.
const maxPayload = 6 << 20

// reserve is how long before Lambda's deadline the handler gives up and answers 504, so a
// slow query is a response rather than a function timeout.
const reserve = time.Second

// Config is what a Handler needs besides the store and the catalog.
type Config struct {
	Region string // the function's, which every response names
	Prefix string // the deployment's prefix, as in [catalog.Naming]
	Log    *slog.Logger
}

// Handler serves the exposed named queries to every front door and to direct invoke. It is
// safe for concurrent use.
type Handler struct {
	e       *engine.Engine
	cat     *engine.Catalog
	queries map[string]*catalog.Query // the exposed queries, by name
	views   []string                  // the current versions they read, which /health checks
	region  string
	store   string // the Store dimension of QueryFailures
	log     *slog.Logger
}

// New returns the handler of cat's exposed queries, which reads them from b.
func New(b store.Backend, cat *engine.Catalog, c Config) (*Handler, error) {
	if c.Region == "" || c.Log == nil {
		return nil, errors.New("queryhttp: the config needs a region and a logger")
	}
	def := cat.Catalog()
	h := &Handler{
		e: engine.New(b, engine.Options{Region: c.Region, Prefix: c.Prefix}), cat: cat, queries: map[string]*catalog.Query{},
		region: c.Region, store: catalog.Naming{Prefix: c.Prefix, Store: def.Store}.Base(), log: c.Log,
	}
	for i := range def.Queries {
		if q := &def.Queries[i]; q.ExposeHTTP {
			h.queries[q.Name] = q
		}
	}
	for v, vv := range def.ExposedViews() {
		h.views = append(h.views, catalog.VersionID(v.Name, vv.Number))
	}
	return h, nil
}

// Invoke implements aws-lambda-go's Handler, so the handler owns the response bytes. A
// request it can't serve is a response with an error status; only an event of no door it
// knows is an error, which fails the invocation.
func (h *Handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	began := time.Now()
	req, err := parse(payload)
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelError, "unknown event", slog.String("requestId", requestID(ctx)), slog.String("error", err.Error()))
		return nil, err
	}
	work := ctx
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		work, cancel = context.WithDeadline(ctx, deadline.Add(-reserve))
		defer cancel()
	}
	r := h.answer(work, req)
	out, err := h.encode(ctx, req.door, r)
	if err == nil && len(out) > maxPayload {
		big := tooLarge()
		big.params, big.rows = r.params, r.rows
		r = big
		out, err = h.encode(ctx, req.door, r)
	}
	if err != nil {
		return nil, err
	}
	h.logRequest(ctx, req, r, len(out), time.Since(began))
	return out, nil
}

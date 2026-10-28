// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/emf"
)

// logRequest writes the request's one log line. It names the parameters but never logs
// their values, which can hold personal data. A 500 or 504 also counts in QueryFailures, in
// CloudWatch embedded metric format, at level ERROR, which a raised log level keeps.
func (h *Handler) logRequest(ctx context.Context, req request, r reply, size int, took time.Duration) {
	level := slog.LevelInfo
	attrs := []slog.Attr{
		slog.String("requestId", requestID(ctx)), slog.String("door", req.door.String()),
		slog.Int("status", r.status), slog.Int("bytes", size), slog.Int64("durationMs", took.Milliseconds()),
	}
	if req.door == doorDirect {
		attrs = append(attrs, slog.String("query", req.query))
	} else {
		attrs = append(attrs, slog.String("method", req.method), slog.String("path", req.path))
	}
	if r.params != nil {
		attrs = append(attrs, slog.Any("params", r.params))
	}
	if r.code != "" {
		attrs = append(attrs, slog.String("code", r.code))
	}
	if r.code == "" || r.code == codeResultTooLarge {
		attrs = append(attrs, slog.Int("rows", r.rows))
	}
	if r.cause != nil {
		level = slog.LevelWarn
		attrs = append(attrs, slog.String("error", r.cause.Error()))
	}
	if r.status == http.StatusInternalServerError || r.status == http.StatusGatewayTimeout {
		level = slog.LevelError
		attrs = append(attrs, emf.Attrs(h.store, emf.Count{Name: catalog.MetricQueryFailures, N: 1})...)
	}
	h.log.LogAttrs(ctx, level, "request", attrs...)
}

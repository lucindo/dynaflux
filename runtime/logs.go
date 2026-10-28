// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-lambda-go/lambdacontext"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/emf"
	"github.com/lucindo/dynaflux/internal/projector"
)

// newLogger returns the logger of the function's JSON log lines. Each line has a top-level
// timestamp in RFC 3339: Lambda files a line without a valid one at level INFO, which an
// application log level of WARN or ERROR then drops.
func newLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				return slog.String("timestamp", a.Value.Time().UTC().Format(time.RFC3339Nano))
			}
			return a
		},
	}))
}

// errorBytes is how much of the rows' errors a partial failure logs.
const errorBytes = 1024

// logCounts writes the batch's metrics as one line in CloudWatch embedded metric format,
// when it counted something. The line's level is the most severe of the counts, so the
// alarms' data survives a raised application log level.
func logCounts(ctx context.Context, log *slog.Logger, dimension string, b projector.Batch, st projector.Stats) {
	level := slog.LevelWarn
	if len(st.Broken) > 0 || st.TooLarge > 0 {
		level = slog.LevelError
	}
	counts := slices.DeleteFunc([]emf.Count{
		{Name: catalog.MetricBreakerOpened, N: len(st.Broken)},
		{Name: catalog.MetricRowTooLarge, N: st.TooLarge},
		{Name: catalog.MetricDivergentDuplicates, N: st.Divergent},
	}, func(c emf.Count) bool { return c.N == 0 })
	if len(counts) == 0 {
		return
	}
	attrs := append(emf.Attrs(dimension, counts...), batchAttrs(ctx, b)...)
	if len(st.Broken) > 0 {
		attrs = append(attrs, slog.Any("broken", st.Broken))
	}
	log.LogAttrs(ctx, level, "batch metrics", attrs...)
}

// batchAttrs names the invocation and its batch in a log line.
func batchAttrs(ctx context.Context, b projector.Batch) []slog.Attr {
	return []slog.Attr{slog.String("requestId", lambdaRequestID(ctx)), slog.String("stream", b.Stream), slog.String("unit", b.Unit)}
}

// lambdaRequestID returns the ID of the invocation, which every log line names as requestId.
func lambdaRequestID(ctx context.Context) string {
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		return lc.AwsRequestID
	}
	return ""
}

// cut returns s, or its first n bytes and an ellipsis, cut at a rune boundary.
func cut(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + " …"
}

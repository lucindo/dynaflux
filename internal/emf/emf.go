// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package emf publishes counts in a function's log line as CloudWatch metrics, in the
// embedded metric format, under the namespace and dimension that the generated alarms read.
package emf

import (
	"log/slog"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Count is a metric and its count in one log line.
type Count struct {
	Name string
	N    int
}

// directive is the embedded metric format's metric directive.
type directive struct {
	Namespace  string     `json:"Namespace"`
	Dimensions [][]string `json:"Dimensions"`
	Metrics    []metric   `json:"Metrics"`
}

type metric struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

// Attrs returns the attributes of a log line that publish counts as metrics of the store
// whose Store dimension is store.
func Attrs(store string, counts ...Count) []slog.Attr {
	var attrs []slog.Attr
	metrics := make([]metric, len(counts))
	for i, c := range counts {
		attrs = append(attrs, slog.Int(c.Name, c.N))
		metrics[i] = metric{Name: c.Name, Unit: "Count"}
	}
	return append(attrs,
		slog.String(catalog.MetricDimensionStore, store),
		slog.Group("_aws",
			slog.Int64("Timestamp", time.Now().UnixMilli()),
			slog.Any("CloudWatchMetrics", []directive{{
				Namespace:  catalog.MetricNamespace,
				Dimensions: [][]string{{catalog.MetricDimensionStore}},
				Metrics:    metrics,
			}}),
		),
	)
}

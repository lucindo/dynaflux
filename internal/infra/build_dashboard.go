// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Dashboard limits and layout. CloudWatch's grid is 24 columns wide; graphs sit two to a
// row, under the alarms.
const (
	maxDashboardName   = 255
	maxAlarmsPerWidget = 100
	gridWidth          = 24
	graphWidth         = gridWidth / 2
	widgetHeight       = 6
)

// widget is a dashboard widget, with alarmProperties or graph as its properties.
type widget struct {
	Type       string `json:"type"`
	X          int    `json:"x"`
	Y          int    `json:"y"`
	Width      int    `json:"width"`
	Height     int    `json:"height"`
	Properties any    `json:"properties"`
}

type alarmProperties struct {
	Title  string   `json:"title"`
	Alarms []string `json:"alarms"`
	// SortBy puts the alarms that fire first.
	SortBy string `json:"sortBy"`
}

// graph holds the properties of a metric widget. Each line is a metric, as its namespace,
// name and dimension name and value pairs, or a search expression.
type graph struct {
	Title   string  `json:"title"`
	Region  string  `json:"region"`
	Stat    string  `json:"stat,omitempty"`
	Period  int     `json:"period"`
	Metrics [][]any `json:"metrics"`
}

type expression struct {
	Expression string `json:"expression"`
}

// dashboard returns the store's dashboard in a region: the states of the alarms of s, then
// graphs of the projectors, the archive, the tables, the backfill executions and the
// runtime's metrics. In the regions that don't archive, the archive graphs stay empty.
func (b *builder) dashboard(s *Scope) (Dashboard, error) {
	var widgets []widget
	y := 0
	alarms := make([]string, 0, len(s.Alarms))
	for _, a := range s.Alarms {
		alarms = append(alarms, string(regionalARN("cloudwatch", "alarm:"+a.Name)))
	}
	for chunk := range slices.Chunk(alarms, maxAlarmsPerWidget) {
		widgets = append(widgets, widget{
			Type: "alarm", Y: y, Width: gridWidth, Height: widgetHeight,
			Properties: alarmProperties{Title: "Alarms", Alarms: chunk, SortBy: "stateUpdatedTimestamp"},
		})
		y += widgetHeight
	}
	for i, g := range b.graphs(s) {
		g.Region, g.Period = placeholder(PlaceholderRegion), alarmPeriodSeconds
		widgets = append(widgets, widget{
			Type: "metric", X: i % 2 * graphWidth, Y: y + i/2*widgetHeight, Width: graphWidth, Height: widgetHeight, Properties: g,
		})
	}
	// One widget per line keeps the template's diffs readable.
	lines := make([]string, 0, len(widgets))
	for _, w := range widgets {
		data, err := json.Marshal(w)
		if err != nil {
			return Dashboard{}, fmt.Errorf("infra: the dashboard: %w", err)
		}
		lines = append(lines, string(data))
	}
	return Dashboard{
		Common: Common{ID: idDashboard},
		Name:   b.fit(maxDashboardName, b.naming.Base()+"-"+placeholder(PlaceholderRegion)),
		Body:   Text("{\"widgets\":[\n" + strings.Join(lines, ",\n") + "\n]}"),
	}, nil
}

// graphs returns the metric widgets of the dashboard, without their region and period. The
// graphs of the tables search for the tables whose names hold {prefix}{store} as whole
// words, so a new view version needs no change. CloudWatch's search matches words, not
// prefixes, so they can also show the tables of a store whose name starts with this one's,
// or, with an empty prefix, of the store's prefixed environments.
func (b *builder) graphs(s *Scope) []graph {
	var projectors, firehoses []string
	for _, f := range s.Functions {
		if f.Mode == catalog.EnvModeProjector {
			projectors = append(projectors, string(f.Name))
		}
	}
	for _, d := range s.DeliveryStreams {
		firehoses = append(firehoses, string(d.Name))
	}
	tables := dimTableName + "=" + b.naming.Base()
	gs := []graph{
		{Title: "Projector iterator age (ms)", Stat: statMaximum, Metrics: lines(namespaceLambda, dimFunctionName, projectors, metricIteratorAge)},
		{
			Title: "Projector invocations, errors and throttles", Stat: statSum,
			Metrics: lines(namespaceLambda, dimFunctionName, projectors, "Invocations", metricErrors, metricThrottles),
		},
	}
	if len(firehoses) > 0 {
		gs = append(gs,
			graph{Title: "Archive records", Stat: statSum, Metrics: lines(namespaceFirehose, dimDeliveryStream, firehoses,
				"IncomingRecords", "DeliveryToIceberg.SuccessfulRowCount", metricFailedRows, "ThrottledRecords")},
			graph{Title: "Archive data freshness (s)", Stat: statMaximum, Metrics: lines(namespaceFirehose, dimDeliveryStream, firehoses, metricDataFreshness)},
		)
	}
	gs = append(gs,
		graph{Title: "Consumed write capacity units", Metrics: [][]any{search(dimTableName, "ConsumedWriteCapacityUnits", statSum, tables)}},
		graph{Title: "Throttled reads and writes", Metrics: [][]any{
			search(dimTableName, "ReadThrottleEvents", statSum, tables),
			search(dimTableName, "WriteThrottleEvents", statSum, tables),
		}},
	)
	if len(b.regions) > 1 {
		gs = append(gs, graph{Title: "Replication latency to the other regions (ms)", Metrics: [][]any{
			search("ReceivingRegion,"+dimTableName, "ReplicationLatency", statMaximum, tables),
		}})
	}
	return append(gs,
		b.backfillGraph(),
		graph{Title: "DynamoDB system errors", Metrics: [][]any{search("Operation,"+dimTableName, "SystemErrors", statSum, tables)}},
		graph{Title: "Opened breakers, too-large rows, divergent duplicates and values left out", Stat: statSum, Metrics: lines(
			catalog.MetricNamespace, catalog.MetricDimensionStore, []string{b.naming.Base()},
			catalog.MetricBreakerOpened, catalog.MetricRowTooLarge, catalog.MetricDivergentDuplicates, catalog.MetricLeftOut)},
	)
}

// lines returns a graph line for each metric name and dimension value.
func lines(namespace, dimension string, values []string, names ...string) [][]any {
	var ls [][]any
	for _, n := range names {
		for _, v := range values {
			ls = append(ls, []any{namespace, n, dimension, v})
		}
	}
	return ls
}

// search returns a graph line that searches DynamoDB's metric name with the dimensions for
// the series that match term.
func search(dimensions, name, statistic, term string) []any {
	return []any{expression{fmt.Sprintf(`SEARCH('{%s,%s} MetricName="%s" %s', '%s', %d)`,
		namespaceDynamoDB, dimensions, name, term, statistic, alarmPeriodSeconds)}}
}

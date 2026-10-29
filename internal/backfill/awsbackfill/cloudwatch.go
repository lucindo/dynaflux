// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfill

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/deployed"
)

// lagWindow is how far back Lag looks: Lambda publishes a minute's IteratorAge a minute or
// two after it ends.
const lagWindow = 5 * time.Minute

// ProjectorFunctions returns the projector functions of cat's store, named by naming: the
// store's and each stream's, since the catalog doesn't say which runtime.split deployed. A
// function that doesn't exist has no data.
func ProjectorFunctions(naming catalog.Naming, cat *catalog.Catalog) []string {
	functions := []string{naming.ProjectorFunction()}
	for _, s := range cat.Streams {
		functions = append(functions, naming.StreamProjectorFunction(s.Name))
	}
	return functions
}

// IteratorAges reads, by function, the latest 1-minute maximum IteratorAge from since to now.
// A function without data then is absent.
func IteratorAges(ctx context.Context, c CloudWatch, functions []string, since, now time.Time) (map[string]time.Duration, error) {
	ages := map[string]time.Duration{}
	if len(functions) == 0 {
		return ages, nil
	}
	queries := make([]types.MetricDataQuery, len(functions))
	for i, f := range functions {
		dims := []types.Dimension{{Name: aws.String("FunctionName"), Value: aws.String(f)}}
		queries[i] = metricQuery("f"+strconv.Itoa(i), "AWS/Lambda", "IteratorAge", dims, time.Minute, statMaximum)
	}
	in := &cloudwatch.GetMetricDataInput{
		MetricDataQueries: queries, StartTime: aws.Time(since), EndTime: aws.Time(now), ScanBy: types.ScanByTimestampDescending,
	}
	for {
		out, err := c.GetMetricData(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("awsbackfill: read the projectors' IteratorAge: %w", err)
		}
		for _, r := range out.MetricDataResults {
			if err = deployed.CheckMetricResult(r, out.NextToken == nil); err != nil {
				return nil, fmt.Errorf("awsbackfill: read the projectors' IteratorAge: %w", err)
			}
			i, ierr := strconv.Atoi(aws.ToString(r.Id)[1:])
			if ierr != nil || i >= len(functions) || len(r.Values) == 0 {
				continue
			}
			// A later page continues a series with older values.
			if _, ok := ages[functions[i]]; !ok {
				ages[functions[i]] = time.Duration(r.Values[0] * float64(time.Millisecond))
			}
		}
		if out.NextToken == nil {
			return ages, nil
		}
		in.NextToken = out.NextToken
	}
}

// Lag reads how far behind the region's projector functions are: [backfill.Lag].
type Lag struct {
	c         CloudWatch
	functions []string
}

// NewLag returns the lag of the projector functions of cat's store, named by naming.
func NewLag(c CloudWatch, naming catalog.Naming, cat *catalog.Catalog) *Lag {
	return &Lag{c: c, functions: ProjectorFunctions(naming, cat)}
}

// Lag implements [backfill.Lag]: the highest of each function's latest 1-minute maximum
// IteratorAge, or 0 when none has data, as when no batch arrived.
func (l *Lag) Lag(ctx context.Context) (time.Duration, error) {
	end := time.Now()
	ages, err := IteratorAges(ctx, l.c, l.functions, end.Add(-lagWindow), end)
	if err != nil {
		return 0, err
	}
	return slices.Max(append(slices.Collect(maps.Values(ages)), 0)), nil
}

// CloudWatch's statistics that the adapters read.
const (
	statMaximum = "Maximum"
	statSum     = "Sum"
)

// point is a datapoint: its period's start and its value.
type point struct {
	at    time.Time
	value float64
}

// metricQuery returns the query, by id, of statistic stat of a metric, in periods of period.
func metricQuery(id, namespace, metric string, dims []types.Dimension, period time.Duration, stat string) types.MetricDataQuery {
	return types.MetricDataQuery{
		Id: aws.String(id),
		MetricStat: &types.MetricStat{
			Metric: &types.Metric{Namespace: aws.String(namespace), MetricName: aws.String(metric), Dimensions: dims},
			Period: aws.Int32(int32(period / time.Second)), //nolint:gosec // a period is at most a day
			Stat:   aws.String(stat),
		},
	}
}

// metricData reads queries over [start, end), every page, and returns each query's
// datapoints by its ID. A series that CloudWatch couldn't read whole fails it.
func metricData(ctx context.Context, c CloudWatch, queries []types.MetricDataQuery, start, end time.Time) (map[string][]point, error) {
	data := map[string][]point{}
	pages := cloudwatch.NewGetMetricDataPaginator(c, &cloudwatch.GetMetricDataInput{MetricDataQueries: queries, StartTime: &start, EndTime: &end})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("awsbackfill: read metrics: %w", err)
		}
		for _, r := range page.MetricDataResults {
			id := aws.ToString(r.Id)
			if err = deployed.CheckMetricResult(r, !pages.HasMorePages()); err != nil {
				return nil, fmt.Errorf("awsbackfill: read metrics: %w", err)
			}
			if len(r.Timestamps) != len(r.Values) {
				return nil, fmt.Errorf("awsbackfill: CloudWatch returned %d timestamps for %d values of %s", len(r.Timestamps), len(r.Values), id)
			}
			for i, v := range r.Values {
				data[id] = append(data[id], point{at: r.Timestamps[i], value: v})
			}
		}
	}
	return data, nil
}

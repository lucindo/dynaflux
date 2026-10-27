// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package deployed

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

const (
	// Recent is how far back the latest IteratorAge and DataFreshness may be: Lambda and
	// Firehose publish a minute's data a minute or two after it ends.
	Recent = 10 * time.Minute
	// FailedRowsWindow is how far back the archive's failed rows are counted.
	FailedRowsWindow = 24 * time.Hour
)

// Metrics are a region's archive metrics. A delivery stream without data, such as one that
// doesn't exist or got no records, is absent.
type Metrics struct {
	// Freshness is, by delivery stream, the latest 1-minute maximum of
	// DeliveryToIceberg.DataFreshness of the last [Recent].
	Freshness map[string]time.Duration
	// FailedRows is, by delivery stream, how many rows Firehose couldn't write to the
	// archive in the last [FailedRowsWindow].
	FailedRows map[string]int64
}

// series is a metric to read, and what to do with its values, newest first.
type series struct {
	namespace, metric, dimension, value, stat string
	add                                       func(values []float64)
}

// ReadMetrics reads the freshness and failed rows of the delivery streams, up to now.
func ReadMetrics(ctx context.Context, c CloudWatch, deliveryStreams []string, now time.Time) (Metrics, error) {
	m := Metrics{Freshness: map[string]time.Duration{}, FailedRows: map[string]int64{}}
	var recent, failed []series
	for _, d := range deliveryStreams {
		recent = append(recent, series{"AWS/Firehose", "DeliveryToIceberg.DataFreshness", "DeliveryStreamName", d, "Maximum",
			func(vs []float64) {
				// The first values a series gets are its latest: a later page continues it with older ones.
				if _, ok := m.Freshness[d]; !ok {
					m.Freshness[d] = time.Duration(vs[0] * float64(time.Second))
				}
			}})
		failed = append(failed, series{"AWS/Firehose", "DeliveryToIceberg.FailedRowCount", "DeliveryStreamName", d, "Sum",
			func(vs []float64) {
				for _, v := range vs {
					m.FailedRows[d] += int64(v)
				}
			}})
	}
	if err := read(ctx, c, recent, int32(time.Minute/time.Second), now.Add(-Recent), now); err != nil {
		return Metrics{}, err
	}
	if err := read(ctx, c, failed, int32(FailedRowsWindow/time.Second), now.Add(-FailedRowsWindow), now); err != nil {
		return Metrics{}, err
	}
	return m, nil
}

// read reads each series in periods of period seconds over [start, end], newest data first.
func read(ctx context.Context, c CloudWatch, ss []series, period int32, start, end time.Time) error {
	if len(ss) == 0 {
		return nil
	}
	queries := make([]types.MetricDataQuery, len(ss))
	byID := make(map[string]func([]float64), len(ss))
	for i, s := range ss {
		id := "m" + strconv.Itoa(i)
		byID[id] = s.add
		queries[i] = types.MetricDataQuery{
			Id: aws.String(id),
			MetricStat: &types.MetricStat{
				Metric: &types.Metric{
					Namespace: aws.String(s.namespace), MetricName: aws.String(s.metric),
					Dimensions: []types.Dimension{{Name: aws.String(s.dimension), Value: aws.String(s.value)}},
				},
				Period: aws.Int32(period),
				Stat:   aws.String(s.stat),
			},
		}
	}
	pages := cloudwatch.NewGetMetricDataPaginator(c, &cloudwatch.GetMetricDataInput{
		MetricDataQueries: queries, StartTime: aws.Time(start), EndTime: aws.Time(end), ScanBy: types.ScanByTimestampDescending,
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("deployed: read metrics: %w", err)
		}
		for _, r := range page.MetricDataResults {
			if add, ok := byID[aws.ToString(r.Id)]; ok && len(r.Values) > 0 {
				add(r.Values)
			}
		}
	}
	return nil
}

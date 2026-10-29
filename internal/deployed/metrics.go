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

	"github.com/lucindo/dynaflux/internal/catalog"
)

const (
	// Recent is how far back the latest IteratorAge and DataFreshness may be: Lambda and
	// Firehose publish a minute's data a minute or two after it ends.
	Recent = 10 * time.Minute
	// CountWindow is how far back the archive's failed rows and the values left out of views
	// are counted.
	CountWindow = 24 * time.Hour
)

// Metrics are a region's archive metrics. A delivery stream without data, such as one that
// doesn't exist or got no records, is absent.
type Metrics struct {
	// Freshness is, by delivery stream, the latest 1-minute maximum of
	// DeliveryToIceberg.DataFreshness of the last [Recent].
	Freshness map[string]time.Duration
	// Delivered is, by delivery stream, that datapoint's time less its freshness: Firehose
	// had delivered every record it took before then.
	Delivered map[string]time.Time
	// FailedRows is, by delivery stream, how many rows Firehose couldn't write to the
	// archive in the last [CountWindow].
	FailedRows map[string]int64
	// LeftOut is how many source rows and aggregate inputs the store's views left out in the
	// last [CountWindow], for a value too long to store (sql.md §5.8).
	LeftOut int64
}

// series is a metric to read, and what to do with its values and their times, newest first.
type series struct {
	namespace, metric, dimension, value, stat string
	add                                       func(values []float64, times []time.Time)
}

// ReadMetrics reads the freshness and failed rows of the delivery streams, and the values
// left out of the views of store, its dimension value, up to now.
func ReadMetrics(ctx context.Context, c CloudWatch, store string, deliveryStreams []string, now time.Time) (Metrics, error) {
	m := Metrics{Freshness: map[string]time.Duration{}, Delivered: map[string]time.Time{}, FailedRows: map[string]int64{}}
	var recent, counts []series
	for _, d := range deliveryStreams {
		recent = append(recent, series{"AWS/Firehose", "DeliveryToIceberg.DataFreshness", "DeliveryStreamName", d, "Maximum",
			func(vs []float64, ts []time.Time) {
				// The first values a series gets are its latest: a later page continues it with older ones.
				if _, ok := m.Freshness[d]; !ok {
					m.Freshness[d] = time.Duration(vs[0] * float64(time.Second))
					m.Delivered[d] = ts[0].Add(-m.Freshness[d])
				}
			}})
		counts = append(counts, series{"AWS/Firehose", "DeliveryToIceberg.FailedRowCount", "DeliveryStreamName", d, "Sum",
			func(vs []float64, _ []time.Time) {
				for _, v := range vs {
					m.FailedRows[d] += int64(v)
				}
			}})
	}
	counts = append(counts, series{catalog.MetricNamespace, catalog.MetricLeftOut, catalog.MetricDimensionStore, store, "Sum",
		func(vs []float64, _ []time.Time) {
			for _, v := range vs {
				m.LeftOut += int64(v)
			}
		}})
	if err := read(ctx, c, recent, int32(time.Minute/time.Second), now.Add(-Recent), now); err != nil {
		return Metrics{}, err
	}
	if err := read(ctx, c, counts, int32(CountWindow/time.Second), now.Add(-CountWindow), now); err != nil {
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
	byID := make(map[string]func([]float64, []time.Time), len(ss))
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
			if err = CheckMetricResult(r, page.NextToken == nil); err != nil {
				return fmt.Errorf("deployed: read metrics: %w", err)
			}
			if add, ok := byID[aws.ToString(r.Id)]; ok && len(r.Values) > 0 {
				if len(r.Timestamps) != len(r.Values) {
					return fmt.Errorf("deployed: read metrics: %s has %d values and %d times", aws.ToString(r.Id), len(r.Values), len(r.Timestamps))
				}
				add(r.Values, r.Timestamps)
			}
		}
	}
	return nil
}

// CheckMetricResult fails on a result of a page of GetMetricData that doesn't hold its
// series: CloudWatch's internal error, a read it denied, or partial data on the last page,
// which last says this is. Missing datapoints would read as no activity.
func CheckMetricResult(r types.MetricDataResult, last bool) error {
	switch r.StatusCode {
	case types.StatusCodeComplete:
		return nil
	case types.StatusCodePartialData:
		if !last {
			return nil
		}
	case types.StatusCodeInternalError, types.StatusCodeForbidden:
	}
	var why []string
	for _, m := range r.Messages {
		why = append(why, aws.ToString(m.Code)+": "+aws.ToString(m.Value))
	}
	return fmt.Errorf("CloudWatch returned series %s with status %q %v", aws.ToString(r.Id), r.StatusCode, why)
}

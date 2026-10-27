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
		queries[i] = types.MetricDataQuery{
			Id: aws.String("f" + strconv.Itoa(i)),
			MetricStat: &types.MetricStat{
				Metric: &types.Metric{
					Namespace: aws.String("AWS/Lambda"), MetricName: aws.String("IteratorAge"),
					Dimensions: []types.Dimension{{Name: aws.String("FunctionName"), Value: aws.String(f)}},
				},
				Period: aws.Int32(int32(time.Minute / time.Second)),
				Stat:   aws.String("Maximum"),
			},
		}
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

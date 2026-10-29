// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package deployed_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

func TestReadCatalog(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := mem.New()
	m := meta.New(b, "shop-meta")
	if _, err := deployed.ReadCatalog(ctx, m); !errors.Is(err, deployed.ErrNotDeployed) {
		t.Errorf("an empty meta table: %v, want ErrNotDeployed", err)
	}
	if _, err := deployed.ReadCatalog(ctx, meta.New(missingTable{b}, "prod-shop-meta")); !errors.Is(err, deployed.ErrNotDeployed) {
		t.Errorf("no meta table: %v, want ErrNotDeployed", err)
	}
	data := []byte(`{"format":2,"catalog":{}}`)
	if _, err := m.RegisterCatalog(ctx, data, "v9.0.0"); err != nil {
		t.Fatal(err)
	}
	c, err := deployed.ReadCatalog(ctx, m)
	if err != nil || string(c.Data) != string(data) || c.Format != 2 || c.Generator != "v9.0.0" {
		t.Errorf("got %+v, %v; want the catalog registered, in format 2", c, err)
	}
}

// missingTable answers as DynamoDB does for a table that doesn't exist.
type missingTable struct{ store.Backend }

func (missingTable) Get(context.Context, string, store.Key, bool) (store.Item, error) {
	return nil, &ddbtypes.ResourceNotFoundException{Message: aws.String("Requested resource not found")}
}

const machine = "arn:aws:states:us-east-1:123456789012:stateMachine:shop-backfill"

func execution(name string) string {
	return "arn:aws:states:us-east-1:123456789012:execution:shop-backfill:" + name
}

func TestRunning(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		recorded string
		status   map[string]sfntypes.ExecutionStatus
		want     deployed.Execution
	}{
		{"running", execution("v-a1"), map[string]sfntypes.ExecutionStatus{execution("v-a1"): sfntypes.ExecutionStatusRunning},
			deployed.Execution{ARN: execution("v-a1")}},
		{"continuing", execution("v-a1"), map[string]sfntypes.ExecutionStatus{
			execution("v-a1"): sfntypes.ExecutionStatusSucceeded, execution("v-a1-c1"): sfntypes.ExecutionStatusRunning,
		}, deployed.Execution{ARN: execution("v-a1-c1"), Continuation: true}},
		{"continuing a continuation", execution("v-a1-c9"), map[string]sfntypes.ExecutionStatus{
			execution("v-a1-c9"): sfntypes.ExecutionStatusSucceeded, execution("v-a1-c10"): sfntypes.ExecutionStatusRunning,
		}, deployed.Execution{ARN: execution("v-a1-c10"), Continuation: true}},
		{"continuation missing", execution("v-a1"), map[string]sfntypes.ExecutionStatus{execution("v-a1"): sfntypes.ExecutionStatusSucceeded},
			deployed.Execution{}},
		{"continuation ended", execution("v-a1"), map[string]sfntypes.ExecutionStatus{
			execution("v-a1"): sfntypes.ExecutionStatusSucceeded, execution("v-a1-c1"): sfntypes.ExecutionStatusTimedOut,
		}, deployed.Execution{}},
		{"failed", execution("v-a1"), map[string]sfntypes.ExecutionStatus{execution("v-a1"): sfntypes.ExecutionStatusFailed}, deployed.Execution{}},
		{"missing", execution("v-a1"), nil, deployed.Execution{}},
		{"local", "local", nil, deployed.Execution{}},
		{"another machine", "arn:aws:states:us-east-1:123456789012:execution:old-backfill:v-a1",
			map[string]sfntypes.ExecutionStatus{"arn:aws:states:us-east-1:123456789012:execution:old-backfill:v-a1": sfntypes.ExecutionStatusRunning},
			deployed.Execution{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := deployed.Running(t.Context(), fakeSFN(tt.status), machine, meta.Job{Execution: tt.recorded})
			if err != nil || got != tt.want {
				t.Errorf("got %+v, %v; want %+v", got, err, tt.want)
			}
		})
	}
}

func TestRunningFailsOnAnError(t *testing.T) {
	t.Parallel()
	_, err := deployed.Running(t.Context(), failingSFN{}, machine, meta.Job{Execution: execution("v-a1")})
	if err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("got %v, want the AccessDenied", err)
	}
}

type fakeSFN map[string]sfntypes.ExecutionStatus

func (f fakeSFN) DescribeExecution(_ context.Context, in *sfn.DescribeExecutionInput, _ ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error) {
	status, ok := f[aws.ToString(in.ExecutionArn)]
	if !ok {
		return nil, &sfntypes.ExecutionDoesNotExist{Message: aws.String("Execution Does Not Exist")}
	}
	return &sfn.DescribeExecutionOutput{ExecutionArn: in.ExecutionArn, Status: status}, nil
}

type failingSFN struct{}

func (failingSFN) DescribeExecution(context.Context, *sfn.DescribeExecutionInput, ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error) {
	return nil, errors.New("AccessDenied: states:DescribeExecution")
}

// Mappings reads every page, and names each mapping's function without its qualifier.
func TestMappings(t *testing.T) {
	t.Parallel()
	pages := fakeLambda{
		"": {EventSourceMappings: []lambdatypes.EventSourceMappingConfiguration{{
			UUID: aws.String("u1"), FunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:shop-projector"),
			State: aws.String("Enabled"), LastProcessingResult: aws.String("OK"),
		}}, NextMarker: aws.String("m2")},
		"m2": {EventSourceMappings: []lambdatypes.EventSourceMappingConfiguration{{
			UUID: aws.String("u2"), FunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:audit:live"),
			State: aws.String("Disabled"),
		}}},
	}
	got, err := deployed.Mappings(t.Context(), pages, "arn:stream")
	want := []deployed.Mapping{
		{UUID: "u1", Function: "shop-projector", State: "Enabled", LastResult: "OK"},
		{UUID: "u2", Function: "audit", State: "Disabled"},
	}
	if err != nil || !slices.Equal(got, want) {
		t.Errorf("got %+v, %v; want %+v", got, err, want)
	}
}

// fakeLambda answers ListEventSourceMappings with a page by marker.
type fakeLambda map[string]lambda.ListEventSourceMappingsOutput

func (f fakeLambda) ListEventSourceMappings(_ context.Context, in *lambda.ListEventSourceMappingsInput, _ ...func(*lambda.Options)) (*lambda.ListEventSourceMappingsOutput, error) {
	if aws.ToString(in.EventSourceArn) != "arn:stream" {
		return &lambda.ListEventSourceMappingsOutput{}, nil
	}
	out := f[aws.ToString(in.Marker)]
	return &out, nil
}

// ReadMetrics keeps the newest value of a series that continues on a later page, converts
// seconds, takes the freshness from its datapoint's time for the delivered time, sums failed
// rows and the values left out over every page, and leaves out a series without data.
func TestReadMetrics(t *testing.T) {
	t.Parallel()
	now := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	minutes := func(ms ...int) []time.Time {
		var ts []time.Time
		for _, m := range ms {
			ts = append(ts, now.Add(time.Duration(-m)*time.Minute))
		}
		return ts
	}
	partial, complete := cwtypes.StatusCodePartialData, cwtypes.StatusCodeComplete
	cw := &fakeCloudWatch{pages: map[string][]cloudwatch.GetMetricDataOutput{
		"DeliveryToIceberg.DataFreshness": {
			{MetricDataResults: []cwtypes.MetricDataResult{{Id: aws.String("m0"), Values: []float64{90, 99}, Timestamps: minutes(2, 3), StatusCode: partial},
				{Id: aws.String("m1"), StatusCode: complete}}, NextToken: aws.String("2")},
			{MetricDataResults: []cwtypes.MetricDataResult{{Id: aws.String("m0"), Values: []float64{400}, Timestamps: minutes(4), StatusCode: complete}}},
		},
		// m2 is the views' LeftOut, after the failed rows of both delivery streams.
		"DeliveryToIceberg.FailedRowCount": {
			{MetricDataResults: []cwtypes.MetricDataResult{{Id: aws.String("m0"), Values: []float64{2}, Timestamps: minutes(60), StatusCode: partial},
				{Id: aws.String("m2"), Values: []float64{6}, Timestamps: minutes(30), StatusCode: partial}}, NextToken: aws.String("2")},
			{MetricDataResults: []cwtypes.MetricDataResult{{Id: aws.String("m0"), Values: []float64{3}, Timestamps: minutes(120), StatusCode: complete},
				{Id: aws.String("m2"), Values: []float64{1}, Timestamps: minutes(90), StatusCode: complete}}},
		},
	}}
	m, err := deployed.ReadMetrics(t.Context(), cw, "shop", []string{"shop-archive-page_views", "shop-archive-clicks"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Freshness; len(got) != 1 || got["shop-archive-page_views"] != 90*time.Second {
		t.Errorf("Freshness %v, want 1m30s", got)
	}
	if got, want := m.Delivered, time.Date(1999, 12, 31, 23, 56, 30, 0, time.UTC); len(got) != 1 || !got["shop-archive-page_views"].Equal(want) {
		t.Errorf("Delivered %v, want %v", got, want)
	}
	if got := m.FailedRows; len(got) != 1 || got["shop-archive-page_views"] != 5 {
		t.Errorf("FailedRows %v, want 5", got)
	}
	if m.LeftOut != 7 {
		t.Errorf("LeftOut %d, want 7", m.LeftOut)
	}
	if want := []string{"10m0s 60", "24h0m0s 86400"}; !slices.Equal(cw.windows, want) {
		t.Errorf("windows and periods %q, want %q", cw.windows, want)
	}
}

// fakeCloudWatch answers GetMetricData with pages by the first query's metric and the
// token, and keeps each request's window and period.
type fakeCloudWatch struct {
	pages   map[string][]cloudwatch.GetMetricDataOutput
	windows []string
}

func (f *fakeCloudWatch) GetMetricData(_ context.Context, in *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	first := in.MetricDataQueries[0].MetricStat
	page := 0
	if in.NextToken != nil {
		page = 1
	} else {
		f.windows = append(f.windows, fmt.Sprintf("%s %d", in.EndTime.Sub(*in.StartTime), aws.ToInt32(first.Period)))
	}
	pages := f.pages[aws.ToString(first.Metric.MetricName)]
	if page >= len(pages) {
		return &cloudwatch.GetMetricDataOutput{}, nil
	}
	return &pages[page], nil
}

func TestGaps(t *testing.T) {
	t.Parallel()
	record := `{"requestContext":{"condition":"RetryAttemptsExhausted"},"version":"1.0","DDBStreamBatchInfo":{` +
		`"shardId":"shardId-0001","startSequenceNumber":"100","endSequenceNumber":"250",` +
		`"approximateArrivalOfFirstRecord":"2000-01-01T00:29:00Z","approximateArrivalOfLastRecord":"2000-01-01T00:30:00.5Z","batchSize":3,` +
		`"streamArn":"arn:aws:dynamodb:us-east-1:123456789012:table/shop-stream-page_views/stream/2000-01-01T00:00:00.000"},"payload":"{}"}`
	bucket := fakeBucket{"aws/lambda/u/shardId-0001/a": record, "aws/lambda/u/shardId-0001/b": record, "backfill/x": "not a record"}
	scan, err := deployed.Gaps(t.Context(), bucket, "shop-bucket", time.Time{})
	want := deployed.Gap{
		Key: "aws/lambda/u/shardId-0001/a", Table: "shop-stream-page_views", Shard: "shardId-0001", FirstSeq: "100", LastSeq: "250",
		StreamARN:    "arn:aws:dynamodb:us-east-1:123456789012:table/shop-stream-page_views/stream/2000-01-01T00:00:00.000",
		FirstArrival: time.Date(2000, 1, 1, 0, 29, 0, 0, time.UTC), LastArrival: time.Date(2000, 1, 1, 0, 30, 0, 5e8, time.UTC), Records: 3,
	}
	if err != nil || len(scan.Gaps) != 2 || scan.Gaps[0] != want || scan.Gaps[1].Key != "aws/lambda/u/shardId-0001/b" || len(scan.Bad) > 0 {
		t.Errorf("got %+v, %v; want two gaps, the first %+v", scan, err, want)
	}
	// A record that isn't one is reported, and the others still read.
	bucket["aws/lambda/u/shardId-0001/c"] = `{"requestContext":{}}`
	scan, err = deployed.Gaps(t.Context(), bucket, "shop-bucket", time.Time{})
	if err != nil || len(scan.Gaps) != 2 || len(scan.Bad) != 1 || !strings.Contains(scan.Bad[0].Error(), "s3://shop-bucket/aws/lambda/u/shardId-0001/c") {
		t.Errorf("a record without DDBStreamBatchInfo: %d gaps, bad %v, %v; want 2 gaps and an error naming it", len(scan.Gaps), scan.Bad, err)
	}
}

// Gaps skips the records written before the time it is given, and reads the newest MaxGaps
// of the rest.
func TestGapsSinceAndCap(t *testing.T) {
	t.Parallel()
	since := time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)
	record := `{"DDBStreamBatchInfo":{"shardId":"s","streamArn":"arn:aws:dynamodb:us-east-1:1:table/t/stream/x"}}`
	bucket := fakeBucket{}
	for i := range deployed.MaxGaps + 5 {
		at := since.Add(time.Duration(i-3) * time.Minute)
		bucket["aws/lambda/u/s/"+at.Format("2006/01/02/2006-01-02T15.04.05")+"-r"] = record
	}
	scan, err := deployed.Gaps(t.Context(), bucket, "shop-bucket", since)
	first := "aws/lambda/u/s/2000/01/02/2000-01-02T00.02.00-r" // the newest 100 of the 102 since
	if err != nil || scan.Older != 3 || scan.Capped != 2 || len(scan.Gaps) != deployed.MaxGaps || scan.Gaps[0].Key != first {
		t.Errorf("older %d, capped %d, %d gaps from %s, %v; want 3, 2, %d from %s", scan.Older, scan.Capped, len(scan.Gaps),
			scan.Gaps[0].Key, err, deployed.MaxGaps, first)
	}
}

// fakeBucket is a bucket's objects, listed one key a page.
type fakeBucket map[string]string

func (f fakeBucket) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	var keys []string
	for k := range f {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) && k > aws.ToString(in.ContinuationToken) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	if len(keys) == 0 {
		return &s3.ListObjectsV2Output{}, nil
	}
	out := &s3.ListObjectsV2Output{Contents: []s3types.Object{{Key: aws.String(keys[0])}}}
	if len(keys) > 1 {
		out.IsTruncated, out.NextContinuationToken = aws.Bool(true), aws.String(keys[0])
	}
	return out, nil
}

func (f fakeBucket) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(f[aws.ToString(in.Key)]))}, nil
}

func TestContinuation(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]string{
		"page_daily-v2-0a1b2c-a1": "page_daily-v2-0a1b2c-a1-c1", "page_daily-v2-0a1b2c-a3-c9": "page_daily-v2-0a1b2c-a3-c10",
		"plan-counts-v2-0a1b2c-a1": "plan-counts-v2-0a1b2c-a1-c1",
	} {
		if got := deployed.Continuation(name); got != want {
			t.Errorf("Continuation(%s) = %s, want %s", name, got, want)
		}
	}
}

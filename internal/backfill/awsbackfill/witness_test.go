// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfill_test

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/firehose"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill/awsbackfilltest"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
)

const deliveryStream = "prod-analytics-archive-page_views"

// always is a series with a datapoint of v in every period from from on.
func always(from time.Time, v float64) func(time.Time) (float64, bool) {
	return func(at time.Time) (float64, bool) { return v, !at.Before(from) }
}

// never is a series that CloudWatch lists, without datapoints.
func never(time.Time) (float64, bool) { return 0, false }

// The archive holds every record put before the cut once a DataFreshness datapoint shows it
// left Firehose more than a minute after the cut, or, past 2 h 30 min, when Firehose is empty
// and the evidence says the metric is the right one (ADR 0016 §2).
func TestArchiveComplete(t *testing.T) {
	t.Parallel()
	cut := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	dims := map[string]string{"DeliveryStreamName": deliveryStream}
	tests := []struct {
		name     string
		after    time.Duration // from the cut to now
		series   func(time.Time) (float64, bool)
		firehose []string
		want     bool
	}{
		{"delivered", 3 * time.Minute, always(cut.Add(-time.Minute), 0), []string{deliveryStream}, true},
		{"within the clock margin", 2 * time.Minute, always(cut.Add(-time.Minute), 0), []string{deliveryStream}, false},
		{"a record waits", 6 * time.Minute, always(cut.Add(-time.Minute), 300), []string{deliveryStream}, false},
		{"idle", 2*time.Hour + 31*time.Minute, never, []string{deliveryStream}, true},
		{"idle too soon", 2*time.Hour + 29*time.Minute, never, []string{deliveryStream}, false},
		{"idle, the metric unlisted", 2*time.Hour + 31*time.Minute, nil, []string{deliveryStream}, false},
		{"idle, no delivery stream", 2*time.Hour + 31*time.Minute, never, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := cut.Add(tt.after)
			metrics := awsbackfilltest.NewMetrics(func() time.Time { return now })
			if tt.series != nil {
				metrics.Set("AWS/Firehose", "DeliveryToIceberg.DataFreshness", dims, tt.series)
			}
			a := archiveOf(t, regionClients{metrics: metrics, firehose: awsbackfilltest.NewFirehose(tt.firehose...)}, now)
			if got, err := a.Complete(t.Context(), "page_views", cut); err != nil || got != tt.want {
				t.Errorf("Complete = %v, %v; want %v", got, err, tt.want)
			}
		})
	}
}

// periods keeps the GetMetricData request, and answers it without data.
type periods struct {
	in *cloudwatch.GetMetricDataInput
}

func (p *periods) GetMetricData(_ context.Context, in *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	p.in = in
	return &cloudwatch.GetMetricDataOutput{}, nil
}

func (p *periods) ListMetrics(context.Context, *cloudwatch.ListMetricsInput, ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error) {
	return &cloudwatch.ListMetricsOutput{}, nil
}

// DataFreshness is read from a minute before the cut in 1-minute periods, in 5-minute ones
// once CloudWatch keeps no 1-minute data that old, and from CloudWatch's 63 days at most.
func TestArchiveCompletePeriods(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 28, 12, 30, 0, 0, time.UTC)
	tests := []struct {
		name   string
		cut    time.Time
		start  time.Time
		period int32
	}{
		{"an hour old", now.Add(-time.Hour + 30*time.Second), time.Date(2026, 10, 28, 11, 29, 0, 0, time.UTC), 60},
		{"20 days old", time.Date(2026, 10, 8, 6, 0, 30, 0, time.UTC), time.Date(2026, 10, 8, 5, 59, 0, 0, time.UTC), 300},
		{"70 days old", time.Date(2026, 8, 19, 6, 0, 0, 0, time.UTC), time.Date(2026, 8, 26, 12, 35, 0, 0, time.UTC), 300},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &periods{}
			a := awsbackfill.NewArchive(awsbackfill.Clients{
				CloudWatch: p, Metrics: p, Firehose: firehose.NewFromConfig(config(awsbackfilltest.NewFirehose())),
			}, catalog.Naming{Prefix: "prod-", Store: "analytics"}, archiveCatalog(t), bucket)
			a.Now = func() time.Time { return now }
			if _, err := a.Complete(t.Context(), "page_views", tt.cut); err != nil {
				t.Fatal(err)
			}
			q := p.in.MetricDataQueries[0].MetricStat
			if got := aws.ToTime(p.in.StartTime); !got.Equal(tt.start) || aws.ToInt32(q.Period) != tt.period || !aws.ToTime(p.in.EndTime).Equal(now) ||
				aws.ToString(q.Metric.MetricName) != "DeliveryToIceberg.DataFreshness" || aws.ToString(q.Stat) != "Maximum" ||
				aws.ToString(q.Metric.Dimensions[0].Value) != deliveryStream {
				t.Errorf("read %+v from %v to %v in %d s; want DataFreshness of %s from %v in %d s", q.Metric, got, aws.ToTime(p.in.EndTime),
					aws.ToInt32(q.Period), deliveryStream, tt.start, tt.period)
			}
		})
	}
}

// hourly is a series with a datapoint of 1 in each of hours and none in the others.
func hourly(hours ...time.Time) func(time.Time) (float64, bool) {
	return func(at time.Time) (float64, bool) { return 1, slices.ContainsFunc(hours, at.Equal) }
}

// gapRecord is Lambda's on-failure record of a batch of stream's records that arrived at at.
func gapRecord(stream string, at time.Time) []byte {
	return fmt.Appendf(nil, `{"requestContext":{"requestId":"r1","condition":"RetryAttemptsExhausted"},"DDBStreamBatchInfo":{`+
		`"shardId":"shardId-1","startSequenceNumber":"100","endSequenceNumber":"200","approximateArrivalOfFirstRecord":"%s",`+
		`"approximateArrivalOfLastRecord":"%s","batchSize":3,"streamArn":"%s"}}`, at.Format(time.RFC3339), at.Format(time.RFC3339), stream)
}

func gapKey(at time.Time) string {
	return "aws/lambda/0a1b/shardId-1/" + at.Format("2006/01/02/2006-01-02T15.04.05") + "-x"
}

// factsFakes are what AWS shows of the stream in factsArchive.
type factsFakes struct {
	mappings []awsbackfilltest.Mapping
	objects  map[string][]byte
	writes   map[string]string // the write series' dimensions besides TableName; nil for no series
	// status, when set, is the write series' StatusCode, without datapoints.
	status string
	runs   []time.Time
	// failed are the hours in which the stream projector's invocations all failed.
	failed []time.Time
}

// factsArchive returns an archive at the clock now whose stream table is streamTable, with
// f's mappings, the bucket's objects, a write series of streamTable and the stream
// projector's runs.
func factsArchive(t *testing.T, now time.Time, f factsFakes) *awsbackfill.Archive {
	t.Helper()
	s3 := awsbackfilltest.NewS3()
	for k, v := range f.objects {
		s3.Put(bucket, k, v)
	}
	metrics := awsbackfilltest.NewMetrics(func() time.Time { return now })
	written := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	if f.writes != nil {
		dims := map[string]string{"TableName": streamTable}
		maps.Copy(dims, f.writes)
		if f.status != "" {
			metrics.Fail("AWS/DynamoDB", "ConsumedWriteCapacityUnits", dims, f.status)
		} else {
			metrics.Set("AWS/DynamoDB", "ConsumedWriteCapacityUnits", dims, hourly(written, now.Add(-460*24*time.Hour).Truncate(time.Hour)))
		}
	}
	fn := map[string]string{"FunctionName": "prod-analytics-projector-page_views"}
	metrics.Set("AWS/Lambda", "Invocations", fn, hourly(f.runs...))
	metrics.Set("AWS/Lambda", "Errors", fn, hourly(f.failed...))
	return archiveOf(t, regionClients{
		s3: s3, metrics: metrics, lambda: awsbackfilltest.NewLambda(f.mappings...),
		ddb: describer{streamTable: {StreamSpecification: &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true)}, LatestStreamArn: aws.String(streamARN)}},
	}, now)
}

// The facts are the stream's ARN now, whether a mapping that reads it invokes its projector,
// the gaps Lambda wrote for the origin's stream since the origin, and the first hour with
// writes to the stream table and no projector run that succeeded in it or the next 22, checked
// from the origin's hour or the last check's end, as far as CloudWatch keeps hourly data.
func TestArchiveFacts(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 28, 12, 30, 0, 0, time.UTC)
	origin := time.Date(2026, 10, 20, 9, 15, 0, 0, time.UTC)
	writes := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)  // when factsArchive's series writes
	checked := time.Date(2026, 10, 27, 14, 0, 0, 0, time.UTC) // now less 23 hours and 10 minutes, by the hour
	projector := "arn:aws:lambda:us-east-1:123456789012:function:prod-analytics-projector"
	enabled := awsbackfilltest.Mapping{UUID: "0a1b", FunctionArn: projector, EventSourceArn: streamARN, State: "Enabled"}
	gapAt := time.Date(2026, 10, 25, 9, 59, 0, 0, time.UTC)
	type fakes = factsFakes
	in := func(state string) fakes {
		m := enabled
		m.State = state
		return fakes{mappings: []awsbackfilltest.Mapping{m}}
	}
	tests := []struct {
		name    string
		item    func(*meta.StreamItem)
		fakes   fakes
		want    backfill.Facts
		wantErr string
	}{
		{"complete", nil, fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "Customer"},
			runs: []time.Time{writes.Add(22 * time.Hour)}}, backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked}, ""},
		{"a silent hour", nil, fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "GlobalTable"},
			runs: []time.Time{writes.Add(23 * time.Hour)}}, backfill.Facts{ARN: streamARN, Mapping: true, Silent: writes}, ""},
		// Lambda counts an invocation that failed, from its start or on the view states.
		{"a silent hour with failed runs", nil, fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "Customer"},
			runs: []time.Time{writes, writes.Add(22 * time.Hour)}, failed: []time.Time{writes, writes.Add(22 * time.Hour)}},
			backfill.Facts{ARN: streamARN, Mapping: true, Silent: writes}, ""},
		{"a run that succeeded after failed ones", nil, fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "Customer"},
			runs: []time.Time{writes, writes.Add(22 * time.Hour)}, failed: []time.Time{writes}},
			backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked}, ""},
		{"a silent hour by the table's series", nil, fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{}},
			backfill.Facts{ARN: streamARN, Mapping: true, Silent: writes}, ""},
		{"a silent hour checked before", func(it *meta.StreamItem) { it.CheckedThrough = writes.Add(time.Hour) },
			fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "Customer"}},
			backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked}, ""},
		{"past CloudWatch's hourly data", func(it *meta.StreamItem) { it.Origin.At = now.Add(-500 * 24 * time.Hour) },
			fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "Customer"}, runs: []time.Time{writes}},
			backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked, Unchecked: time.Date(2025, 7, 30, 13, 0, 0, 0, time.UTC)}, ""},
		{"past CloudWatch's hourly data, checked before", func(it *meta.StreamItem) {
			it.Origin.At, it.CheckedThrough = now.Add(-500*24*time.Hour), writes
		}, fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "Customer"}, runs: []time.Time{writes}},
			backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked}, ""},
		{"past CloudWatch's hourly data, unchecked by a check before", func(it *meta.StreamItem) {
			it.Origin.At, it.CheckedThrough, it.Unchecked = now.Add(-500*24*time.Hour), writes, now.Add(-470*24*time.Hour)
		}, fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "Customer"}, runs: []time.Time{writes}},
			backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked, Unchecked: now.Add(-470 * 24 * time.Hour)}, ""},
		// A deploy that changes the mapping passes through these; it reads from its checkpoint.
		{"a mapping being created", nil, in("Creating"), backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked}, ""},
		{"a mapping being enabled", nil, in("Enabling"), backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked}, ""},
		{"a mapping being updated", nil, in("Updating"), backfill.Facts{ARN: streamARN, Mapping: true, Checked: checked}, ""},
		{"a disabled mapping", nil, in("Disabled"), backfill.Facts{ARN: streamARN, Checked: checked}, ""},
		{"a mapping being disabled", nil, in("Disabling"), backfill.Facts{ARN: streamARN, Checked: checked}, ""},
		{"a mapping being deleted", nil, in("Deleting"), backfill.Facts{ARN: streamARN, Checked: checked}, ""},
		{"another function's mapping", nil, fakes{mappings: []awsbackfilltest.Mapping{{UUID: "0a1b", FunctionArn: projector + "-other",
			EventSourceArn: streamARN, State: "Enabled"}}}, backfill.Facts{ARN: streamARN, Checked: checked}, ""},
		{"a gap", nil, fakes{mappings: []awsbackfilltest.Mapping{enabled}, objects: map[string][]byte{
			gapKey(gapAt):                       gapRecord(streamARN, gapAt),
			gapKey(gapAt.Add(time.Minute)):      gapRecord(streamARN+"-other", gapAt),
			gapKey(origin.Add(-48 * time.Hour)): gapRecord(streamARN, origin.Add(-48*time.Hour)),
		}}, backfill.Facts{ARN: streamARN, Mapping: true, Gaps: []backfill.Gap{{Object: gapKey(gapAt), At: gapAt}}, Checked: checked}, ""},
		{"a record that isn't Lambda's", nil, fakes{mappings: []awsbackfilltest.Mapping{enabled}, objects: map[string][]byte{
			gapKey(gapAt): []byte(`{"not":"a record"}`),
		}}, backfill.Facts{}, "the archive's gaps are unknown until the objects under s3://" + bucket + "/aws/lambda/ that aren't " +
			"Lambda's on-failure records are removed (1 of them)"},
		// A series that CloudWatch couldn't read would show no writes, and no silent hour.
		{"a write series CloudWatch failed", nil, fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "Customer"},
			status: "InternalError"}, backfill.Facts{}, `with status "InternalError"`},
		{"a write series CloudWatch denied", nil, fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "Customer"},
			status: "Forbidden"}, backfill.Facts{}, `with status "Forbidden"`},
		{"a write series with partial data on the last page", nil, fakes{mappings: []awsbackfilltest.Mapping{enabled}, writes: map[string]string{"Source": "Customer"},
			status: "PartialData"}, backfill.Facts{}, `with status "PartialData"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := factsArchive(t, now, tt.fakes)
			item := meta.StreamItem{Stream: "page_views", Mode: meta.ArchiveAll, Origin: &meta.Origin{At: origin, ARN: streamARN}}
			if tt.item != nil {
				tt.item(&item)
			}
			got, err := a.Facts(t.Context(), streamTable, item)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("Facts: %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Facts = %+v, %v\nwant %+v", got, err, tt.want)
			}
		})
	}
}

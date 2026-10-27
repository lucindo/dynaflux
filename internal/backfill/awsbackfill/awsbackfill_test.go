// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfill_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill/awsbackfilltest"
	"github.com/lucindo/dynaflux/internal/catalog"
)

const (
	bucket      = "prod-analytics-123456789012-us-east-1-an"
	streamTable = "prod-analytics-stream-page_views"
	streamARN   = "arn:aws:dynamodb:us-east-1:123456789012:table/" + streamTable + "/stream/2026-10-26T12:00:00.000"
)

// inProcess is an HTTP client that serves each request with h, opening no socket.
type inProcess struct{ h http.Handler }

func (c inProcess) Do(req *http.Request) (*http.Response, error) {
	if req.Body == nil {
		req.Body = http.NoBody // as a server sees a request without one
	}
	rec := httptest.NewRecorder()
	c.h.ServeHTTP(rec, req)
	return rec.Result(), nil
}

// config returns an SDK configuration in us-east-1, with fake credentials and a loopback
// endpoint that is never dialed: h answers in process.
func config(h http.Handler) aws.Config {
	return aws.Config{
		Region:       "us-east-1",
		BaseEndpoint: aws.String("http://127.0.0.1:1"),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDFAKE", SecretAccessKey: "fake", Source: "test"}, nil
		}),
		HTTPClient:       inProcess{h},
		RetryMaxAttempts: 1,
	}
}

func newRuns(fake *awsbackfilltest.S3) *awsbackfill.Runs {
	return awsbackfill.NewRuns(s3.NewFromConfig(config(fake), func(o *s3.Options) { o.UsePathStyle = true }), bucket)
}

func put(t *testing.T, r *awsbackfill.Runs, name, data string) {
	t.Helper()
	if err := r.Put(t.Context(), name, func(w io.Writer) error {
		_, err := io.WriteString(w, data)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// A run is read by ranges: from an offset, and its last bytes, which may be all of it.
func TestRunsReadAt(t *testing.T) {
	t.Parallel()
	r := newRuns(awsbackfilltest.NewS3())
	put(t, r, "backfill/v/1/runs/a", "0123456789")
	tests := []struct {
		off, n int64
		want   string
	}{
		{0, 4, "0123"},
		{6, 4, "6789"},
		{8, 10, "89"},
		{-1, 3, "789"},
		{-1, 20, "0123456789"},
		{3, 0, ""},
	}
	for _, tt := range tests {
		got, err := r.ReadAt(t.Context(), "backfill/v/1/runs/a", tt.off, tt.n)
		if err != nil || string(got) != tt.want {
			t.Errorf("ReadAt(%d, %d) = %q, %v; want %q", tt.off, tt.n, got, err, tt.want)
		}
	}
}

// A staged object that is gone is ErrStagingMissing, so the job stages again, also when S3
// answers AccessDenied because the caller may list the bucket only under a prefix. A refusal
// of an object that exists is no such thing.
func TestRunsMissing(t *testing.T) {
	t.Parallel()
	fake := awsbackfilltest.NewS3()
	r := newRuns(fake)
	put(t, r, "backfill/v/1/runs/kept", "data")
	if _, err := r.ReadAt(t.Context(), "backfill/v/1/runs/gone", 0, 4); !errors.Is(err, backfill.ErrStagingMissing) {
		t.Errorf("a missing run: %v, want ErrStagingMissing", err)
	}
	fake.DenyGet(func(string) bool { return true })
	if _, err := r.ReadAt(t.Context(), "backfill/v/1/runs/gone", 0, 4); !errors.Is(err, backfill.ErrStagingMissing) {
		t.Errorf("a missing run, refused: %v, want ErrStagingMissing", err)
	}
	_, err := r.ReadAt(t.Context(), "backfill/v/1/runs/kept", 0, 4)
	if err == nil || errors.Is(err, backfill.ErrStagingMissing) || !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("a refused run that exists: %v, want AccessDenied", err)
	}
}

// DeletePrefix deletes every object under the prefix, past a listing's page of 1,000, and
// nothing else.
func TestRunsDeletePrefix(t *testing.T) {
	t.Parallel()
	fake := awsbackfilltest.NewS3()
	r := newRuns(fake)
	for i := range 1500 {
		put(t, r, fmt.Sprintf("backfill/v/1/runs/%04d", i), "x")
	}
	put(t, r, "backfill/v/10/runs/0", "x")
	put(t, r, "backfill/w/1/runs/0", "x")
	if err := r.DeletePrefix(t.Context(), "backfill/v/1/"); err != nil {
		t.Fatal(err)
	}
	if got, want := fake.Keys(bucket, ""), []string{"backfill/v/10/runs/0", "backfill/w/1/runs/0"}; !slices.Equal(got, want) {
		t.Errorf("left %q, want %q", got, want)
	}
	if err := r.DeletePrefix(t.Context(), "backfill/none/"); err != nil {
		t.Errorf("an empty prefix: %v", err)
	}
}

// fakeDynamoDB answers DescribeTable in each region with the table described there, and
// DescribeStream with pages of shards. It reads the region from the request's signature.
type fakeDynamoDB struct {
	tables map[string]string   // region → DescribeTable body
	pages  map[string][]string // ExclusiveStartShardId ("" first) → shard IDs; the next page starts after the last
	last   string              // the last page's start

	mu    sync.Mutex
	calls []string // "<region> <operation>"
}

var signedRegion = regexp.MustCompile(`Credential=[^/]+/\d+/([^/]+)/`)

func (f *fakeDynamoDB) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	region := signedRegion.FindStringSubmatch(r.Header.Get("Authorization"))[1]
	_, op, _ := strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	f.mu.Lock()
	f.calls = append(f.calls, region+" "+op)
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	switch op {
	case "DescribeTable":
		body, ok := f.tables[region]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			body = `{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}`
		}
		io.WriteString(w, body)
	case "DescribeStream":
		start := ""
		if m := regexp.MustCompile(`"ExclusiveStartShardId":"([^"]+)"`).FindSubmatch(body); m != nil {
			start = string(m[1])
		}
		shards := f.pages[start]
		var list []string
		for _, s := range shards {
			list = append(list, `{"ShardId":"`+s+`"}`)
		}
		next := ""
		if start != f.last {
			next = `,"LastEvaluatedShardId":"` + shards[len(shards)-1] + `"`
		}
		io.WriteString(w, `{"StreamDescription":{"StreamArn":"`+streamARN+`","Shards":[`+strings.Join(list, ",")+`]`+next+`}}`)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func describeBody(created int64, restored bool, replicas ...string) string {
	var rs []string
	for _, r := range replicas {
		rs = append(rs, `{"RegionName":"`+r+`","ReplicaStatus":"ACTIVE"}`)
	}
	restore := ""
	if restored {
		restore = `,"RestoreSummary":{"RestoreDateTime":1.7614E9,"RestoreInProgress":false}`
	}
	return fmt.Sprintf(`{"Table":{"TableName":"%s","TableStatus":"ACTIVE","CreationDateTime":%d,"LatestStreamArn":"%s",`+
		`"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"},"Replicas":[%s]%s}}`,
		streamTable, created, streamARN, strings.Join(rs, ","), restore)
}

// Units lists every shard of the table's latest stream, page after page.
func TestUnits(t *testing.T) {
	t.Parallel()
	f := &fakeDynamoDB{
		tables: map[string]string{"us-east-1": describeBody(1761480000, false)},
		pages:  map[string][]string{"": {"shardId-1", "shardId-2"}, "shardId-2": {"shardId-3"}},
		last:   "shardId-2",
	}
	cfg := config(f)
	u := awsbackfill.NewUnits(dynamodb.NewFromConfig(cfg), dynamodbstreams.NewFromConfig(cfg))
	got, err := u.Units(t.Context(), streamTable)
	if want := []string{"shardId-1", "shardId-2", "shardId-3"}; err != nil || !slices.Equal(got, want) {
		t.Errorf("Units = %q, %v; want %q", got, err, want)
	}
	if want := []string{"us-east-1 DescribeTable", "us-east-1 DescribeStream", "us-east-1 DescribeStream"}; !slices.Equal(f.calls, want) {
		t.Errorf("calls %q, want %q", f.calls, want)
	}
}

// A stream table is as old as its oldest replica, which each replica's region reports, and
// restored when any replica was restored.
func TestReplicasBorn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		tables   map[string]string
		born     int64
		restored bool
	}{
		{"oldest elsewhere", map[string]string{
			"us-east-1": describeBody(1761480000, false, "us-east-1", "us-west-2", "eu-west-1"),
			"us-west-2": describeBody(1761400000, false), "eu-west-1": describeBody(1761500000, false),
		}, 1761400000, false},
		{"restored elsewhere", map[string]string{
			"us-east-1": describeBody(1761480000, false, "us-west-2"), "us-west-2": describeBody(1761490000, true),
		}, 1761480000, true},
		{"one region", map[string]string{"us-east-1": describeBody(1761480000, false)}, 1761480000, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			born, restored, err := awsbackfill.NewReplicas(config(&fakeDynamoDB{tables: tt.tables})).Born(t.Context(), streamTable)
			if err != nil || !born.Equal(time.Unix(tt.born, 0)) || restored != tt.restored {
				t.Errorf("Born = %v, %v, %v; want %v, %v", born, restored, err, time.Unix(tt.born, 0), tt.restored)
			}
		})
	}
	failing := &fakeDynamoDB{tables: map[string]string{"us-east-1": describeBody(1761480000, false, "us-west-2")}}
	_, _, err := awsbackfill.NewReplicas(config(failing)).Born(t.Context(), streamTable)
	if err == nil || !strings.Contains(err.Error(), "describe "+streamTable+" in us-west-2") {
		t.Errorf("a replica that doesn't answer: %v, want its error", err)
	}
}

// fakeMetrics answers GetMetricData with results for the function names in values, and
// keeps the request.
type fakeMetrics struct {
	values map[string][]float64 // function → values, latest first
	in     *cloudwatch.GetMetricDataInput
}

func (f *fakeMetrics) GetMetricData(_ context.Context, in *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	f.in = in
	out := &cloudwatch.GetMetricDataOutput{}
	for _, q := range in.MetricDataQueries {
		out.MetricDataResults = append(out.MetricDataResults, types.MetricDataResult{
			Id: q.Id, Values: f.values[aws.ToString(q.MetricStat.Metric.Dimensions[0].Value)],
		})
	}
	return out, nil
}

// The lag is the highest latest IteratorAge over the store's projector, or each stream's,
// whichever runtime.split deployed; 0 without data.
func TestLag(t *testing.T) {
	t.Parallel()
	cat := &catalog.Catalog{Store: "analytics", Streams: []catalog.Stream{{Name: "page_views"}, {Name: "clicks"}}}
	naming := catalog.Naming{Prefix: "prod-", Store: "analytics"}
	f := &fakeMetrics{values: map[string][]float64{
		"prod-analytics-projector-page_views": {90_000, 400_000},
		"prod-analytics-projector-clicks":     {120_000},
	}}
	lag, err := awsbackfill.NewLag(f, naming, cat).Lag(t.Context())
	if err != nil || lag != 2*time.Minute {
		t.Errorf("Lag = %v, %v; want 2m", lag, err)
	}
	var names []string
	for _, q := range f.in.MetricDataQueries {
		m := q.MetricStat
		if aws.ToString(m.Metric.Namespace) != "AWS/Lambda" || aws.ToString(m.Metric.MetricName) != "IteratorAge" ||
			aws.ToString(m.Stat) != "Maximum" || aws.ToInt32(m.Period) != 60 {
			t.Errorf("query %s reads %+v", aws.ToString(q.Id), m)
		}
		names = append(names, aws.ToString(m.Metric.Dimensions[0].Value))
	}
	want := []string{"prod-analytics-projector", "prod-analytics-projector-page_views", "prod-analytics-projector-clicks"}
	if !slices.Equal(names, want) || f.in.ScanBy != types.ScanByTimestampDescending || f.in.EndTime.Sub(*f.in.StartTime) != 5*time.Minute {
		t.Errorf("queries of %q from %v to %v by %s; want %q over 5 minutes, latest first", names, f.in.StartTime, f.in.EndTime, f.in.ScanBy, want)
	}
	idle, err := awsbackfill.NewLag(&fakeMetrics{}, naming, cat).Lag(t.Context())
	if err != nil || idle != 0 {
		t.Errorf("Lag without data = %v, %v; want 0", idle, err)
	}
}

// The adapters' CloudWatch requests go out in RPC v2 CBOR, as the fake answers them.
func TestLagOverHTTP(t *testing.T) {
	t.Parallel()
	cat := &catalog.Catalog{Store: "analytics", Streams: []catalog.Stream{{Name: "page_views"}}}
	c := cloudwatch.NewFromConfig(config(http.HandlerFunc(awsbackfilltest.CloudWatch)))
	lag, err := awsbackfill.NewLag(c, catalog.Naming{Store: "analytics"}, cat).Lag(t.Context())
	if err != nil || lag != 0 {
		t.Errorf("Lag = %v, %v; want 0", lag, err)
	}
}

// pagedMetrics answers GetMetricData with a page by token, of values by query ID.
type pagedMetrics []map[string][]float64

func (p pagedMetrics) GetMetricData(_ context.Context, in *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	page := 0
	if in.NextToken != nil {
		page = 1
	}
	out := &cloudwatch.GetMetricDataOutput{}
	for _, q := range in.MetricDataQueries {
		out.MetricDataResults = append(out.MetricDataResults, types.MetricDataResult{Id: q.Id, Values: p[page][aws.ToString(q.Id)]})
	}
	if page+1 < len(p) {
		out.NextToken = aws.String("2")
	}
	return out, nil
}

// IteratorAges keeps each function's latest value, which a later page continues with older
// ones, converts milliseconds, and leaves out a function without data.
func TestIteratorAges(t *testing.T) {
	t.Parallel()
	pages := pagedMetrics{{"f0": {1500, 99000}}, {"f0": {400000}, "f2": {90}}}
	now := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err := awsbackfill.IteratorAges(t.Context(), pages, []string{"a", "b", "c"}, now.Add(-10*time.Minute), now)
	if want := map[string]time.Duration{"a": 1500 * time.Millisecond, "c": 90 * time.Millisecond}; err != nil || !maps.Equal(got, want) {
		t.Errorf("IteratorAges = %v, %v; want %v", got, err, want)
	}
}

// describer answers DescribeTable with a description by table.
type describer map[string]ddbtypes.TableDescription

func (d describer) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	t := d[aws.ToString(in.TableName)]
	return &dynamodb.DescribeTableOutput{Table: &t}, nil
}

// A table's stream ARN is its latest stream's, or "" with its stream off.
func TestStreamARN(t *testing.T) {
	t.Parallel()
	tables := describer{
		streamTable: {StreamSpecification: &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true)}, LatestStreamArn: aws.String(streamARN)},
		"off":       {StreamSpecification: &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(false)}, LatestStreamArn: aws.String(streamARN)},
	}
	for table, want := range map[string]string{streamTable: streamARN, "off": ""} {
		if got, err := awsbackfill.StreamARN(t.Context(), tables, table); err != nil || got != want {
			t.Errorf("%s: got %q, %v; want %q", table, got, err, want)
		}
	}
}

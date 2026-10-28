// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfill

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	fhtypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/archive/athenafail"
	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
)

// Archive reads a region's archive with Athena in the backfill workgroup, S3, CloudWatch and
// Firehose (ADR 0016), and what AWS shows of its streams' history (ADR 0017): the
// [backfill.Archive] and [backfill.Witness] of a region.
type Archive struct {
	*Witness

	c   Clients
	cat *catalog.Catalog
}

// NewArchive returns the archive of cat's store, named by naming, in bucket, the store's
// bucket in the region of c.
func NewArchive(c Clients, naming catalog.Naming, cat *catalog.Catalog, bucket string) *Archive {
	w := NewWitness(WitnessClients{DynamoDB: c.DynamoDB, S3: c.S3, CloudWatch: c.CloudWatch, Lambda: c.Lambda}, naming, bucket)
	return &Archive{Witness: w, c: c, cat: cat}
}

// Start implements [backfill.Archive]: one of package archive's statements, with the token
// always set, so that a call sent again starts no other query.
func (a *Archive) Start(ctx context.Context, q backfill.Query, token string) (string, error) {
	s, ok := a.cat.Stream(q.Stream)
	if !ok {
		return "", fmt.Errorf("awsbackfill: no stream %s in this build's catalog", q.Stream)
	}
	db := a.naming.ArchiveDatabase()
	statement := archive.PartitionsQuery(db, s)
	if q.Kind == backfill.QueryExport {
		statement = archive.Unload(db, s, q.From, q.To, "s3://"+a.bucket+"/"+q.Dest)
	}
	out, err := a.c.Athena.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: &statement, ClientRequestToken: &token, WorkGroup: aws.String(a.naming.BackfillWorkGroup()),
	})
	switch {
	case err != nil && athenafail.CallFailure(err) == athenafail.Throttled:
		return "", fmt.Errorf("%w: %w", backfill.ErrThrottled, err)
	case err != nil:
		return "", fmt.Errorf("awsbackfill: start a query of %s: %w", q.Stream, err)
	}
	return aws.ToString(out.QueryExecutionId), nil
}

// Status implements [backfill.Archive].
func (a *Archive) Status(ctx context.Context, id string) (backfill.QueryStatus, error) {
	q, err := a.execution(ctx, id)
	if err != nil {
		return backfill.QueryStatus{}, err
	}
	var st backfill.QueryStatus
	if q.Statistics != nil {
		st.Scanned = aws.ToInt64(q.Statistics.DataScannedInBytes)
	}
	switch q.Status.State {
	case athenatypes.QueryExecutionStateSucceeded:
		st.State = backfill.QuerySucceeded
	case athenatypes.QueryExecutionStateFailed:
		st.State, st.Failure, st.Reason = backfill.QueryFailed, failureKind(q.Status.AthenaError), reason(q.Status)
	case athenatypes.QueryExecutionStateCancelled:
		st.State, st.Reason = backfill.QueryCancelled, reason(q.Status)
	case athenatypes.QueryExecutionStateQueued, athenatypes.QueryExecutionStateRunning:
		st.State = backfill.QueryRunning
	default:
		return backfill.QueryStatus{}, fmt.Errorf("awsbackfill: query %s is in state %q", id, q.Status.State)
	}
	return st, nil
}

// execution returns the query with id, which has a status.
func (a *Archive) execution(ctx context.Context, id string) (*athenatypes.QueryExecution, error) {
	out, err := a.c.Athena.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: &id})
	if err != nil {
		return nil, fmt.Errorf("awsbackfill: query %s: %w", id, err)
	}
	if out.QueryExecution == nil || out.QueryExecution.Status == nil {
		return nil, fmt.Errorf("awsbackfill: Athena returned query %s without its status", id)
	}
	return out.QueryExecution, nil
}

// failureKind returns what a job does about a query that failed with e (ADR 0016 §6).
func failureKind(e *athenatypes.AthenaError) backfill.FailureKind {
	switch athenafail.QueryFailure(e) {
	case athenafail.TimedOut, athenafail.Exhausted:
		return backfill.FailureTimeout
	case athenafail.Retryable, athenafail.Throttled:
		return backfill.FailureRetryable
	case athenafail.Permission:
		return backfill.FailurePermission
	case athenafail.Other, athenafail.User:
	}
	return backfill.FailureOther
}

// reason returns why a query ended as it did, in Athena's words.
func reason(s *athenatypes.QueryExecutionStatus) string {
	if r := aws.ToString(s.StateChangeReason); r != "" {
		return r
	}
	if s.AthenaError != nil {
		return aws.ToString(s.AthenaError.ErrorMessage)
	}
	return ""
}

// Stop implements [backfill.Archive].
func (a *Archive) Stop(ctx context.Context, id string) error {
	if _, err := a.c.Athena.StopQueryExecution(ctx, &athena.StopQueryExecutionInput{QueryExecutionId: &id}); err != nil {
		return fmt.Errorf("awsbackfill: stop query %s: %w", id, err)
	}
	return nil
}

// Partitions implements [backfill.Archive]: the rows of archive.PartitionsQuery, each a
// day, written as a date or as days since 1970, its records and bytes.
func (a *Archive) Partitions(ctx context.Context, id string) ([]backfill.Partition, error) {
	var parts []backfill.Partition
	pages := athena.NewGetQueryResultsPaginator(a.c.Athena, &athena.GetQueryResultsInput{QueryExecutionId: &id})
	for first := true; pages.HasMorePages(); first = false {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("awsbackfill: the results of query %s: %w", id, err)
		}
		if page.ResultSet == nil {
			return nil, fmt.Errorf("awsbackfill: Athena returned the results of query %s without rows", id)
		}
		rows := page.ResultSet.Rows
		// A query's first row holds its columns' names.
		if first && len(rows) > 0 && header(page.ResultSet, rows[0]) {
			rows = rows[1:]
		}
		for _, row := range rows {
			p, perr := partition(row)
			if perr != nil {
				return nil, fmt.Errorf("awsbackfill: the results of query %s: %w", id, perr)
			}
			parts = append(parts, p)
		}
	}
	return parts, nil
}

// header reports whether row holds the names of the columns of rs.
func header(rs *athenatypes.ResultSet, row athenatypes.Row) bool {
	if rs.ResultSetMetadata == nil || len(rs.ResultSetMetadata.ColumnInfo) != len(row.Data) {
		return false
	}
	for i, c := range rs.ResultSetMetadata.ColumnInfo {
		if aws.ToString(row.Data[i].VarCharValue) != aws.ToString(c.Name) {
			return false
		}
	}
	return true
}

// partition reads a row of archive.PartitionsQuery: day, records, bytes.
func partition(row athenatypes.Row) (backfill.Partition, error) {
	const oneDay = 24 * time.Hour
	cells := make([]string, len(row.Data))
	for i, d := range row.Data {
		cells[i] = aws.ToString(d.VarCharValue)
	}
	if len(cells) != 3 { //nolint:mnd // the query's three columns
		return backfill.Partition{}, fmt.Errorf("a row of %d columns, want 3", len(cells))
	}
	day, count, size := cells[0], cells[1], cells[2]
	from, err := time.Parse(time.DateOnly, day)
	if err != nil {
		days, ierr := strconv.ParseInt(day, 10, 64)
		if ierr != nil {
			return backfill.Partition{}, fmt.Errorf("the day %q is neither a date nor days since 1970", day)
		}
		from = time.Unix(days*int64(oneDay/time.Second), 0).UTC()
	}
	records, err := strconv.ParseInt(count, 10, 64)
	if err != nil {
		return backfill.Partition{}, fmt.Errorf("the record count %q of %s: %w", count, day, err)
	}
	bytes, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return backfill.Partition{}, fmt.Errorf("the size %q of %s: %w", size, day, err)
	}
	return backfill.Partition{From: from, To: from.Add(oneDay), Records: records, Bytes: bytes}, nil
}

// maxManifest bounds an export's data manifest: one S3 URI per file.
const maxManifest = 16 << 20

// Files implements [backfill.Archive]: the files that the export's data manifest lists, all
// in the bucket. A manifest or file gone with the lifecycle rules is
// [backfill.ErrStagingMissing].
func (a *Archive) Files(ctx context.Context, id string) ([]string, error) {
	q, err := a.execution(ctx, id)
	if err != nil {
		return nil, err
	}
	var location string
	if q.Statistics != nil {
		location = aws.ToString(q.Statistics.DataManifestLocation)
	}
	manifest, err := a.key(location)
	if err != nil {
		return nil, fmt.Errorf("awsbackfill: the data manifest of query %s: %w", id, err)
	}
	body, err := get(ctx, a.c.S3, a.bucket, manifest, nil)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	data, err := readAll(manifest, body, maxManifest+1)
	if err != nil {
		return nil, err
	}
	if len(data) > maxManifest {
		return nil, fmt.Errorf("awsbackfill: the data manifest %s is over %d bytes", manifest, maxManifest)
	}
	var files []string
	for line := range strings.Lines(string(data)) {
		if line = strings.TrimSpace(line); line == "" {
			continue
		}
		file, kerr := a.key(line)
		if kerr != nil {
			return nil, fmt.Errorf("awsbackfill: the data manifest %s: %w", manifest, kerr)
		}
		files = append(files, file)
	}
	return files, nil
}

// key returns the key of uri, s3://<bucket>/<key>, which must be in the bucket.
func (a *Archive) key(uri string) (string, error) {
	key, ok := strings.CutPrefix(uri, "s3://"+a.bucket+"/")
	if !ok || key == "" {
		return "", fmt.Errorf("%q isn't an object of s3://%s", uri, a.bucket)
	}
	return key, nil
}

// ErrorObjects implements [backfill.Archive]: the keys under the stream's error prefix after
// after, in S3's order.
func (a *Archive) ErrorObjects(ctx context.Context, stream, after string, since time.Time) ([]string, error) {
	in := &s3.ListObjectsV2Input{Bucket: &a.bucket, Prefix: aws.String(archive.ErrorPrefix(stream))}
	if after != "" {
		in.StartAfter = &after
	}
	var keys []string
	pages := s3.NewListObjectsV2Paginator(a.c.S3, in)
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("awsbackfill: list %s: %w", archive.ErrorPrefix(stream), err)
		}
		for _, o := range page.Contents {
			if !aws.ToTime(o.LastModified).Before(since) {
				keys = append(keys, aws.ToString(o.Key))
			}
		}
	}
	return keys, nil
}

// Open implements [backfill.Archive].
func (a *Archive) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	return get(ctx, a.c.S3, a.bucket, name, nil)
}

// CloudWatch's metrics (infra.md §10), and how long it keeps datapoints of each period.
const (
	namespaceFirehose   = "AWS/Firehose"
	metricDataFreshness = "DeliveryToIceberg.DataFreshness"
	dimDeliveryStream   = "DeliveryStreamName"
	minuteData          = 15 * 24 * time.Hour
	coarsePeriod        = 5 * time.Minute
	fiveMinuteData      = 63 * 24 * time.Hour
)

// Complete implements [backfill.Archive] with [backfill.Delivered] over the stream's
// DataFreshness, Maximum, from a minute before the cut to now: in 1-minute periods, or 5
// once CloudWatch keeps no 1-minute data of the first (ADR 0016 §2). The idle rule's
// evidence is that DescribeDeliveryStream reports the stream ACTIVE and ListMetrics lists
// the metric for it.
func (a *Archive) Complete(ctx context.Context, stream string, cut time.Time) (bool, error) {
	now, name := a.now(), a.naming.ArchiveStream(stream)
	start, period := cut.Add(-time.Minute).Truncate(time.Minute), time.Minute
	if oldest := now.Add(-fiveMinuteData); start.Before(oldest) {
		// CloudWatch keeps no datapoint this old; asking would need hourly periods.
		start = oldest.Truncate(coarsePeriod).Add(coarsePeriod)
	}
	if start.Before(now.Add(-minuteData)) {
		period = coarsePeriod
	}
	dims := []cwtypes.Dimension{{Name: aws.String(dimDeliveryStream), Value: &name}}
	data, err := metricData(ctx, a.c.CloudWatch, []cwtypes.MetricDataQuery{
		metricQuery("f", namespaceFirehose, metricDataFreshness, dims, period, statMaximum),
	}, start, now)
	if err != nil {
		return false, err
	}
	f := backfill.Freshness{}
	for _, p := range data["f"] {
		f.Points = append(f.Points, backfill.Datapoint{At: p.at, Freshness: time.Duration(p.value * float64(time.Second))})
	}
	if f.Active, err = a.active(ctx, name); err != nil {
		return false, err
	}
	if f.Listed, err = a.listed(ctx, name, dims); err != nil {
		return false, err
	}
	return backfill.Delivered(f, cut, now), nil
}

// active reports whether the delivery stream exists and is ACTIVE.
func (a *Archive) active(ctx context.Context, name string) (bool, error) {
	out, err := a.c.Firehose.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{DeliveryStreamName: &name})
	if _, ok := errors.AsType[*fhtypes.ResourceNotFoundException](err); ok {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("awsbackfill: describe %s: %w", name, err)
	}
	d := out.DeliveryStreamDescription
	return d != nil && d.DeliveryStreamStatus == fhtypes.DeliveryStreamStatusActive, nil
}

// listed reports whether ListMetrics lists DataFreshness with dims.
func (a *Archive) listed(ctx context.Context, name string, dims []cwtypes.Dimension) (bool, error) {
	filters := make([]cwtypes.DimensionFilter, len(dims))
	for i, d := range dims {
		filters[i] = cwtypes.DimensionFilter{Name: d.Name, Value: d.Value}
	}
	out, err := a.c.Metrics.ListMetrics(ctx, &cloudwatch.ListMetricsInput{
		Namespace: aws.String(namespaceFirehose), MetricName: aws.String(metricDataFreshness), Dimensions: filters,
	})
	if err != nil {
		return false, fmt.Errorf("awsbackfill: list the metrics of %s: %w", name, err)
	}
	return slices.ContainsFunc(out.Metrics, func(m cwtypes.Metric) bool { return aws.ToString(m.MetricName) == metricDataFreshness }), nil
}

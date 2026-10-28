// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfill

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/meta"
)

// The activity check (ADR 0017 §4 item 3).
const (
	// activityWindow is how long from the start of an hour with writes some projector
	// invocation must succeed: in that hour or the next 22.
	activityWindow = 23 * time.Hour
	// hourlyData is how long CloudWatch keeps hourly datapoints.
	hourlyData = 455 * 24 * time.Hour
	// gapSkew is how much earlier than the origin, by the clock of the host that wrote it,
	// Lambda's clock may name a gap's record.
	gapSkew = time.Hour
)

// Witness reads, with reads only, what AWS shows of the history of a region's streams
// (ADR 0017 §4): the [backfill.Witness] of the backfill function, and of dynaflux backfill
// and status.
type Witness struct {
	c      WitnessClients
	naming catalog.Naming
	bucket string
	// Now is the host's clock; nil means time.Now.
	Now func() time.Time
}

// WitnessClients are the clients of a region that [Witness] calls.
type WitnessClients struct {
	DynamoDB   DynamoDB
	S3         deployed.S3
	CloudWatch CloudWatch
	Lambda     Lambda
}

// NewWitness returns the witness of the store that naming names, whose bucket in the region
// of c is bucket.
func NewWitness(c WitnessClients, naming catalog.Naming, bucket string) *Witness {
	return &Witness{c: c, naming: naming, bucket: bucket}
}

func (w *Witness) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

// StreamARN implements [backfill.Witness].
func (w *Witness) StreamARN(ctx context.Context, streamTable string) (string, error) {
	return StreamARN(ctx, w.c.DynamoDB, streamTable)
}

// Facts implements [backfill.Witness] (ADR 0017 §4): the stream's ARN now, whether a mapping
// that reads it invokes its projector, the gaps that Lambda wrote for the origin's ARN, and
// the first hour with writes and no projector run that succeeded after it.
func (w *Witness) Facts(ctx context.Context, streamTable string, item meta.StreamItem) (backfill.Facts, error) {
	o := item.Origin
	if o == nil {
		return backfill.Facts{}, fmt.Errorf("awsbackfill: the archive of %s has no origin", item.Stream)
	}
	arn, err := StreamARN(ctx, w.c.DynamoDB, streamTable)
	if err != nil {
		return backfill.Facts{}, err
	}
	f := backfill.Facts{ARN: arn}
	if f.Mapping, err = w.mapped(ctx, item.Stream, arn); err != nil {
		return backfill.Facts{}, err
	}
	if f.Gaps, err = w.gaps(ctx, *o); err != nil {
		return backfill.Facts{}, err
	}
	from := o.At.Truncate(time.Hour)
	if item.CheckedThrough.After(from) {
		from = item.CheckedThrough
	}
	f.Silent, f.Checked, f.Unchecked, err = w.activity(ctx, streamTable, item.Stream, from)
	if item.Unchecked.After(f.Unchecked) {
		f.Unchecked = item.Unchecked
	}
	return f, err
}

// projectors returns the functions that may project stream: the store's or the stream's,
// since the catalog doesn't say which runtime.split deployed.
func (w *Witness) projectors(stream string) []string {
	return []string{w.naming.ProjectorFunction(), w.naming.StreamProjectorFunction(stream)}
}

// mapped reports whether an event source mapping that delivers, or soon will from its
// checkpoint, invokes stream's projector with the records of arn: a deploy that changes the
// mapping passes through Creating, Enabling or Updating.
func (w *Witness) mapped(ctx context.Context, stream, arn string) (bool, error) {
	if arn == "" {
		return false, nil
	}
	mappings, err := deployed.Mappings(ctx, w.c.Lambda, arn)
	if err != nil {
		return false, err
	}
	projectors := w.projectors(stream)
	return slices.ContainsFunc(mappings, func(m deployed.Mapping) bool {
		return m.Delivering() && slices.Contains(projectors, m.Function)
	}), nil
}

// gaps returns the batches of the origin's stream that Lambda discarded since the origin. A
// record it can't read, or one past those it reads, fails it: either may be the stream's.
func (w *Witness) gaps(ctx context.Context, o meta.Origin) ([]backfill.Gap, error) {
	scan, err := deployed.Gaps(ctx, w.c.S3, w.bucket, o.At.Add(-gapSkew))
	switch {
	case err != nil:
		return nil, err
	case len(scan.Bad) > 0:
		return nil, fmt.Errorf("awsbackfill: the archive's gaps are unknown until the objects under s3://%s/%s that aren't "+
			"Lambda's on-failure records are removed (%d of them): %w", w.bucket, deployed.GapPrefix, len(scan.Bad), scan.Bad[0])
	case scan.Capped > 0:
		return nil, fmt.Errorf("awsbackfill: the archive's gaps are unknown: s3://%s/%s holds more than %d records since %s; "+
			"remove those of gaps already rebuilt", w.bucket, deployed.GapPrefix, deployed.MaxGaps, o.At.UTC().Format(time.RFC3339))
	}
	var gaps []backfill.Gap
	for _, g := range scan.Gaps {
		if g.StreamARN == o.ARN {
			gaps = append(gaps, backfill.Gap{Object: g.Key, At: g.FirstArrival})
		}
	}
	return gaps, nil
}

// activity checks, for each hour from from, an hour start, with writes to streamTable, that
// an invocation of a projector of stream succeeded in it or the next 22, as far as
// CloudWatch's hourly data reaches and those 23 hours have ended. Lambda counts failed
// invocations in Invocations too: a projector that fails on every batch, from its start or on
// the view states, may let DynamoDB trim records it never handled. It returns the first hour
// that fails, or else the end of the hours it checked, both zero when it could check none;
// and the oldest hour CloudWatch keeps when the hours from from start before it.
func (w *Witness) activity(ctx context.Context, streamTable, stream string, from time.Time) (time.Time, time.Time, time.Time, error) {
	now := w.now()
	var unchecked time.Time
	if oldest := now.Add(-hourlyData).Truncate(time.Hour).Add(time.Hour); from.Before(oldest) {
		from, unchecked = oldest, oldest
	}
	// CloudWatch may still add to an hour that ended within deployed.Recent.
	end := now.Add(-deployed.Recent - activityWindow).Truncate(time.Hour).Add(time.Hour)
	if !from.Before(end) {
		return time.Time{}, time.Time{}, unchecked, nil
	}
	table := types.Dimension{Name: aws.String("TableName"), Value: &streamTable}
	source := func(s string) types.Dimension {
		return types.Dimension{Name: aws.String("Source"), Value: aws.String(s)}
	}
	// Writes to a replica come from its region's clients or from replication, and a series by
	// table alone may hold both.
	queries := []types.MetricDataQuery{
		metricQuery("w0", "AWS/DynamoDB", "ConsumedWriteCapacityUnits", []types.Dimension{table}, time.Hour, statSum),
		metricQuery("w1", "AWS/DynamoDB", "ConsumedWriteCapacityUnits", []types.Dimension{table, source("Customer")}, time.Hour, statSum),
		metricQuery("w2", "AWS/DynamoDB", "ConsumedWriteCapacityUnits", []types.Dimension{table, source("GlobalTable")}, time.Hour, statSum),
	}
	for i, f := range w.projectors(stream) {
		dims := []types.Dimension{{Name: aws.String("FunctionName"), Value: aws.String(f)}}
		queries = append(queries, metricQuery("i"+strconv.Itoa(i), "AWS/Lambda", "Invocations", dims, time.Hour, statSum),
			metricQuery("e"+strconv.Itoa(i), "AWS/Lambda", "Errors", dims, time.Hour, statSum))
	}
	data, err := metricData(ctx, w.c.CloudWatch, queries, from, end.Add(activityWindow-time.Hour))
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	// By the hour's Unix time: times from different sources differ in location.
	writes, succeeded := map[int64]bool{}, map[int64]float64{}
	for id, points := range data {
		for _, p := range points {
			h := p.at.Truncate(time.Hour).Unix()
			switch id[0] {
			case 'w':
				writes[h] = writes[h] || p.value > 0
			case 'i':
				succeeded[h] += p.value
			case 'e':
				succeeded[h] -= p.value
			}
		}
	}
	runs := map[int64]bool{}
	for h, n := range succeeded {
		runs[h] = n > 0
	}
	for h := from; h.Before(end); h = h.Add(time.Hour) {
		if writes[h.Unix()] && !ranWithin(runs, h) {
			return h, time.Time{}, unchecked, nil
		}
	}
	return time.Time{}, end, unchecked, nil
}

// ranWithin reports whether runs holds h or one of the 22 hours after it.
func ranWithin(runs map[int64]bool, h time.Time) bool {
	for t := h; t.Before(h.Add(activityWindow)); t = t.Add(time.Hour) {
		if runs[t.Unix()] {
			return true
		}
	}
	return false
}

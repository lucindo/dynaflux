// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package deployed

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// GapPrefix is where the event source mappings' on-failure destination writes in the
// store's bucket.
const GapPrefix = "aws/lambda/"

// maxGapRecord bounds the bytes read of an on-failure record, which holds the batch's
// invocation payload: at most Lambda's 6 MB.
const maxGapRecord = 16 << 20

// Gap is a batch that an event source mapping discarded, as Lambda recorded it in the
// bucket ([on-failure records]). Its records reached neither the views nor the archive.
//
// [on-failure records]: https://docs.aws.amazon.com/lambda/latest/dg/services-dynamodb-errors.html
type Gap struct {
	Key       string // the record's object key
	StreamARN string
	Table     string // the stream table, from StreamARN
	Shard     string // the delivery unit
	// FirstSeq and LastSeq are the batch's first and last sequence numbers, as DynamoDB
	// Streams writes them.
	FirstSeq, LastSeq         string
	FirstArrival, LastArrival time.Time
	Records                   int
}

// onFailure is the part of an on-failure record that a gap needs.
type onFailure struct {
	Info *struct {
		Shard        string    `json:"shardId"`
		First        string    `json:"startSequenceNumber"`
		Last         string    `json:"endSequenceNumber"`
		FirstArrival time.Time `json:"approximateArrivalOfFirstRecord"`
		LastArrival  time.Time `json:"approximateArrivalOfLastRecord"`
		Size         int       `json:"batchSize"`
		StreamARN    string    `json:"streamArn"`
	} `json:"DDBStreamBatchInfo"`
}

const (
	// MaxGaps bounds the on-failure records that Gaps reads: the newest of them.
	MaxGaps = 100
	// gapReads is how many records Gaps reads at once.
	gapReads = 8
	// gapTime is the form of the time at the start of a record's file name.
	gapTime = "2006-01-02T15.04.05"
)

// GapScan is what Gaps found under [GapPrefix].
type GapScan struct {
	Gaps []Gap // the records read, in key order
	// Older counts the records written before the time Gaps was given, which it didn't read,
	// and Capped those past the newest [MaxGaps] of the rest.
	Older, Capped int
	// Bad holds an error for each record that couldn't be read or isn't a DynamoDB stream's
	// on-failure record.
	Bad []error
}

// Gaps reads the on-failure records under [GapPrefix] in bucket that Lambda wrote since
// since, the newest [MaxGaps] of them, several at once. Lambda writes a record once the
// batch's records arrived, and its key holds the time,
// aws/lambda/<mapping>/<shard>/YYYY/MM/DD/YYYY-MM-DDTHH.MM.SS-<id>; a key without one is read.
func Gaps(ctx context.Context, c S3, bucket string, since time.Time) (GapScan, error) {
	var scan GapScan
	keys, err := listGaps(ctx, c, bucket)
	if err != nil {
		return GapScan{}, err
	}
	listed := len(keys)
	keys = slices.DeleteFunc(keys, func(k gapKey) bool { return !k.at.IsZero() && k.at.Before(since) })
	scan.Older = listed - len(keys)
	if len(keys) > MaxGaps {
		// Newest first, and a key without its time before any.
		slices.SortStableFunc(keys, func(a, b gapKey) int {
			if a.at.IsZero() || b.at.IsZero() {
				return cmp.Compare(btoi(b.at.IsZero()), btoi(a.at.IsZero()))
			}
			return b.at.Compare(a.at)
		})
		scan.Capped, keys = len(keys)-MaxGaps, keys[:MaxGaps]
		slices.SortFunc(keys, func(a, b gapKey) int { return strings.Compare(a.key, b.key) })
	}
	gaps, errs := make([]Gap, len(keys)), make([]error, len(keys))
	sem := make(chan struct{}, gapReads)
	var wg sync.WaitGroup
	for i, k := range keys {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			gaps[i], errs[i] = readGap(ctx, c, bucket, k.key)
		})
	}
	wg.Wait()
	for i := range keys {
		if errs[i] != nil {
			scan.Bad = append(scan.Bad, errs[i])
			continue
		}
		scan.Gaps = append(scan.Gaps, gaps[i])
	}
	return scan, nil
}

// gapKey is the key of an on-failure record, and the time it holds; zero when it holds none.
type gapKey struct {
	key string
	at  time.Time
}

// listGaps lists the keys under [GapPrefix] in bucket.
func listGaps(ctx context.Context, c S3, bucket string) ([]gapKey, error) {
	var keys []gapKey
	pages := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: aws.String(GapPrefix)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("deployed: list s3://%s/%s: %w", bucket, GapPrefix, err)
		}
		for _, o := range page.Contents {
			k := gapKey{key: aws.ToString(o.Key)}
			if at, perr := time.Parse(gapTime, cutGapTime(path.Base(k.key))); perr == nil {
				k.at = at
			}
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// cutGapTime returns the part of a record's file name that holds its time.
func cutGapTime(name string) string {
	if len(name) < len(gapTime) {
		return name
	}
	return name[:len(gapTime)]
}

func readGap(ctx context.Context, c S3, bucket, key string) (Gap, error) {
	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return Gap{}, fmt.Errorf("deployed: read s3://%s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()
	var rec onFailure
	if err = json.NewDecoder(io.LimitReader(out.Body, maxGapRecord)).Decode(&rec); err != nil {
		return Gap{}, fmt.Errorf("deployed: read s3://%s/%s: %w", bucket, key, err)
	}
	if rec.Info == nil {
		return Gap{}, fmt.Errorf("deployed: s3://%s/%s isn't a DynamoDB stream's on-failure record", bucket, key)
	}
	i := rec.Info
	return Gap{
		Key: key, StreamARN: i.StreamARN, Table: streamTable(i.StreamARN), Shard: i.Shard, FirstSeq: i.First, LastSeq: i.Last,
		FirstArrival: i.FirstArrival.UTC(), LastArrival: i.LastArrival.UTC(), Records: i.Size,
	}, nil
}

// streamTable returns the table in a change stream's ARN,
// arn:<partition>:dynamodb:<region>:<account>:table/<table>/stream/<label>.
func streamTable(arn string) string {
	_, rest, _ := strings.Cut(arn, ":table/")
	table, _, _ := strings.Cut(rest, "/")
	return table
}

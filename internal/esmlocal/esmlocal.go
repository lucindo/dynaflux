// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package esmlocal reads a DynamoDB stream on an emulator and renders its records as the
// tumbling-window events Lambda's event source mapping sends (docs/decisions/0003). Tests
// call the handler with these events themselves; neither emulator runs mappings. It never
// runs in production.
package esmlocal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

const (
	batchSize   = 100 // the mapping's BatchSize (infra.md §8), and GetRecords' Limit
	maxFailures = 3
)

// Reader reads every shard of one stream from TRIM_HORIZON.
type Reader struct {
	c         *dynamodbstreams.Client
	streamARN string
	iters     map[string]*string // by ShardId: where the next read starts; nil once the shard ended
}

func NewReader(c *dynamodbstreams.Client, streamARN string) *Reader {
	return &Reader{c: c, streamARN: streamARN, iters: map[string]*string{}}
}

// Next returns, per ShardId, the Lambda record JSON of the records read since the last
// call (GetRecords, Limit 100, until an empty read), in sequence order: the stream's JSON
// as the emulator wrote it, plus the eventSourceARN Lambda adds to each record. On an
// error it returns nothing and the next call reads the same records again.
func (r *Reader) Next(ctx context.Context) (map[string][]json.RawMessage, error) {
	ids, err := r.shards(ctx)
	if err != nil {
		return nil, err
	}
	recs := map[string][]json.RawMessage{}
	iters := map[string]*string{}
	for _, id := range ids {
		var shardRecs []json.RawMessage
		if shardRecs, iters[id], err = r.readShard(ctx, id); err != nil {
			return nil, fmt.Errorf("esmlocal: shard %s: %w", id, err)
		}
		if len(shardRecs) > 0 {
			recs[id] = shardRecs
		}
	}
	maps.Copy(r.iters, iters)
	return recs, nil
}

func (r *Reader) shards(ctx context.Context) ([]string, error) {
	var ids []string
	in := &dynamodbstreams.DescribeStreamInput{StreamArn: &r.streamARN}
	for {
		out, err := r.c.DescribeStream(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("esmlocal: describe stream %s: %w", r.streamARN, err)
		}
		for _, s := range out.StreamDescription.Shards {
			ids = append(ids, *s.ShardId)
		}
		if out.StreamDescription.LastEvaluatedShardId == nil {
			return ids, nil
		}
		in.ExclusiveStartShardId = out.StreamDescription.LastEvaluatedShardId
	}
}

// readShard reads shard id from where the last call stopped until an empty read, and
// returns the records and where the next call starts.
func (r *Reader) readShard(ctx context.Context, id string) ([]json.RawMessage, *string, error) {
	it, ok := r.iters[id]
	if !ok {
		out, err := r.c.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn: &r.streamARN, ShardId: &id, ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
		})
		if err != nil {
			return nil, nil, err
		}
		it = out.ShardIterator
	}
	var recs []json.RawMessage
	for it != nil {
		page, next, err := r.read(ctx, it)
		if err != nil {
			return nil, nil, err
		}
		it = next
		if len(page) == 0 {
			break
		}
		recs = append(recs, page...)
	}
	return recs, it, nil
}

// read makes one GetRecords call and returns its records as Lambda's, and the next
// iterator.
func (r *Reader) read(ctx context.Context, it *string) ([]json.RawMessage, *string, error) {
	keep := &bodyKeeper{}
	out, err := r.c.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: it, Limit: new(int32(batchSize))},
		func(o *dynamodbstreams.Options) {
			keep.next = o.HTTPClient
			o.HTTPClient = keep
		})
	if err != nil {
		return nil, nil, err
	}
	var page struct {
		Records []map[string]json.RawMessage `json:"Records"`
	}
	if err = json.Unmarshal(keep.body, &page); err != nil {
		return nil, nil, fmt.Errorf("GetRecords response: %w", err)
	}
	src, err := json.Marshal(r.streamARN)
	if err != nil {
		return nil, nil, err
	}
	recs := make([]json.RawMessage, len(page.Records))
	for i, rec := range page.Records {
		rec["eventSourceARN"] = src
		if recs[i], err = json.Marshal(rec); err != nil {
			return nil, nil, err
		}
	}
	return recs, out.NextShardIterator, nil
}

// bodyKeeper keeps the body of the response it passes on: the SDK decodes records into
// types that drop the wire JSON a Lambda event carries.
type bodyKeeper struct {
	next dynamodbstreams.HTTPClient
	body []byte
}

func (k *bodyKeeper) Do(req *http.Request) (*http.Response, error) {
	resp, err := k.next.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if k.body, err = io.ReadAll(resp.Body); err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(k.body))
	return resp, nil
}

// Every event has this window: the projector ignores windows (docs/decisions/0003), and a
// fixed one renders a redelivered batch as the same bytes.
const (
	windowStart = "1970-01-01T00:00:00Z"
	windowEnd   = "1970-01-01T00:15:00Z"
)

type window struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type event struct {
	Records        []json.RawMessage `json:"Records"`
	ShardID        string            `json:"shardId"`
	EventSourceARN string            `json:"eventSourceARN"`
	Window         window            `json:"window"`
	State          struct{}          `json:"state"`
	Final          bool              `json:"isFinalInvokeForWindow"`
	EarlyEnd       bool              `json:"isWindowTerminatedEarly"`
}

// Event renders one invocation's event: Records, shardId, eventSourceARN, window, state
// {} and isFinalInvokeForWindow. No records is a window's final invocation. It panics
// when a record isn't JSON, a mistake in the test that crafted it.
func Event(shardID, streamARN string, recs ...json.RawMessage) []byte {
	if recs == nil {
		recs = []json.RawMessage{} // the final invocation has "Records": []
	}
	b, err := json.Marshal(event{
		Records: recs, ShardID: shardID, EventSourceARN: streamARN,
		Window: window{Start: windowStart, End: windowEnd}, Final: len(recs) == 0,
	})
	if err != nil {
		panic(fmt.Sprintf("esmlocal: event: %v", err))
	}
	return b
}

// Function is one invocation: the event's JSON in, the response's JSON out. An error is a
// function error. ctx carries the invocation's deadline.
type Function func(ctx context.Context, event []byte) ([]byte, error)

// Drain calls Next until it returns no records, and delivers the records in batches of at
// most 100, shard by shard, in sequence order, each call with a deadline timeout away.
// After a partial failure it resumes at the returned itemIdentifier; after a function
// error, or a response without state, it repeats the call. It fails after 3 failed calls
// of one batch.
func Drain(ctx context.Context, r *Reader, timeout time.Duration, fn Function) error {
	for {
		recs, err := r.Next(ctx)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			return nil
		}
		for _, shard := range slices.Sorted(maps.Keys(recs)) {
			for batch := range slices.Chunk(recs[shard], batchSize) {
				if err = deliver(ctx, r.streamARN, shard, batch, timeout, fn); err != nil {
					return err
				}
			}
		}
	}
}

func deliver(ctx context.Context, streamARN, shard string, batch []json.RawMessage, timeout time.Duration, fn Function) error {
	var errs []error
	for range maxFailures {
		done, err := invoke(ctx, streamARN, shard, batch, timeout, fn)
		if err == nil {
			return nil
		}
		errs = append(errs, err)
		batch = batch[done:]
	}
	return fmt.Errorf("esmlocal: shard %s: %d failed calls of one batch: %w", shard, maxFailures, errors.Join(errs...))
}

// invoke makes one call and returns how many records of the batch succeeded, with an
// error unless all of them did. A partial failure succeeds the records before the lowest
// itemIdentifier.
func invoke(ctx context.Context, streamARN, shard string, batch []json.RawMessage, timeout time.Duration, fn Function) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := fn(ctx, Event(shard, streamARN, batch...))
	if err != nil {
		return 0, fmt.Errorf("function error: %w", err)
	}
	var resp struct {
		State    map[string]string `json:"state"`
		Failures []struct {
			ItemIdentifier string `json:"itemIdentifier"`
		} `json:"batchItemFailures"`
	}
	if err = json.Unmarshal(out, &resp); err != nil {
		return 0, fmt.Errorf("response: %w", err)
	}
	if resp.State == nil {
		return 0, fmt.Errorf("response %s has no state, which Lambda takes as a failed invocation", out)
	}
	done := len(batch)
	for _, f := range resp.Failures {
		i := slices.IndexFunc(batch, func(rec json.RawMessage) bool {
			var r struct {
				Change struct {
					SequenceNumber string `json:"SequenceNumber"`
				} `json:"dynamodb"`
			}
			return json.Unmarshal(rec, &r) == nil && r.Change.SequenceNumber == f.ItemIdentifier
		})
		if i < 0 {
			return 0, fmt.Errorf("itemIdentifier %q isn't in the batch, which Lambda takes as a failed invocation", f.ItemIdentifier)
		}
		done = min(done, i)
	}
	if done < len(batch) {
		return done, fmt.Errorf("partial failure from record %d of %d", done, len(batch))
	}
	return done, nil
}

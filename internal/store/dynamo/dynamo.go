// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package dynamo is the DynamoDB [store.Backend]. Each operation is one SDK call, except that
// BatchGet resends unprocessed keys and Query follows pages. store's request checks run
// before anything is sent, so both backends reject the same requests.
package dynamo

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

// api is every DynamoDB operation the backend sends: the upper bound of what its IAM role
// must grant.
type api interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	BatchGetItem(ctx context.Context, in *dynamodb.BatchGetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

// Backend is a [store.Backend] over a DynamoDB client. It is safe for concurrent use.
type Backend struct {
	c api
}

// New returns a backend that sends every request through c. It sets no client option:
// retries, timeouts and endpoints are the caller's.
func New(c *dynamodb.Client) *Backend { return &Backend{c: c} }

const (
	// DynamoDB asks for exponential backoff before unprocessed keys are sent again.
	unprocessedBase = 25 * time.Millisecond
	unprocessedMax  = time.Second
	// maxPageItems stops PageSize doubling, as in mem: past it, DynamoDB's 1 MB bounds a
	// page anyway.
	maxPageItems = 1 << 20
)

// Get implements [store.Backend].
func (b *Backend) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	if err := store.CheckKey(k); err != nil {
		return nil, fmt.Errorf("dynamo: get from %s: %w", table, err)
	}
	out, err := b.c.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &table,
		Key:            keyAV(k),
		ConsistentRead: &consistent,
	})
	if err != nil {
		return nil, fmt.Errorf("dynamo: get from %s: %w", table, classify(err))
	}
	return fromItem(out.Item), nil
}

// BatchGet implements [store.Backend]. DynamoDB rejects a repeated key, so each distinct key
// is sent once and its item converted for every position that asked for it.
func (b *Backend) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	if err := store.CheckKeys(ks); err != nil {
		return nil, fmt.Errorf("dynamo: batch get from %s: %w", table, err)
	}
	out := make([]store.Item, len(ks))
	at := make(map[store.Key][]int, len(ks))
	var keys []map[string]types.AttributeValue
	for i, k := range ks {
		if _, seen := at[k]; !seen {
			keys = append(keys, keyAV(k))
		}
		at[k] = append(at[k], i)
	}
	var delay time.Duration
	for len(keys) > 0 {
		if err := sleep(ctx, delay); err != nil {
			return nil, fmt.Errorf("dynamo: batch get from %s: %w", table, err)
		}
		res, err := b.c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{table: {Keys: keys, ConsistentRead: &consistent}},
		})
		if err != nil {
			return nil, fmt.Errorf("dynamo: batch get from %s: %w", table, classify(err))
		}
		for _, av := range res.Responses[table] {
			idx := at[keyOf(av)]
			if len(idx) == 0 {
				return nil, fmt.Errorf("dynamo: batch get from %s: the response holds an item for a key not asked for", table)
			}
			for _, i := range idx {
				out[i] = fromItem(av)
			}
		}
		left := res.UnprocessedKeys[table].Keys
		// A successful BatchGetItem processes at least one key; anything else would loop.
		if len(left) >= len(keys) {
			return nil, fmt.Errorf("dynamo: batch get from %s: a response processed no key", table)
		}
		keys = left
		delay = min(max(delay+delay, unprocessedBase), unprocessedMax)
	}
	return out, nil
}

// sleep waits a random time below d (full jitter), or returns the context's error.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(rand.N(d)) //nolint:gosec // jitter needs no cryptographic randomness
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Query implements [store.Backend].
func (b *Backend) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	return func(yield func(store.Item, error) bool) {
		if err := store.CheckQuery(q); err != nil {
			yield(nil, fmt.Errorf("dynamo: query %s: %w", table, err))
			return
		}
		p := pager{in: queryInput(table, q), size: q.PageSize, left: q.Limit, limited: q.Limit > 0}
		for !p.done {
			items, err := p.next(ctx, b.c)
			if err != nil {
				yield(nil, fmt.Errorf("dynamo: query %s: %w", table, err))
				return
			}
			for _, it := range items {
				if !yield(it, nil) {
					return
				}
			}
		}
	}
}

// pager is a Query between pages.
type pager struct {
	in      *dynamodb.QueryInput
	size    int // item limit of the next page; 0 for none
	left    int // items left under the Query's Limit
	limited bool
	done    bool
}

// next reads one page and advances p.
func (p *pager) next(ctx context.Context, c api) ([]store.Item, error) {
	p.in.Limit = pageLimit(p.size, p.left)
	out, err := c.Query(ctx, p.in)
	if err != nil {
		return nil, classify(err)
	}
	items := make([]store.Item, len(out.Items))
	for i, av := range out.Items {
		items[i] = fromItem(av)
	}
	p.left -= len(items)
	p.done = len(out.LastEvaluatedKey) == 0 || p.limited && p.left == 0
	p.in.ExclusiveStartKey = out.LastEvaluatedKey
	if p.size > 0 && p.size <= maxPageItems {
		p.size += p.size
	}
	return items, nil
}

// pageLimit is the Limit of the next request: the page size, cut to the items left under
// the Query's Limit; nil when neither applies, since DynamoDB rejects a Limit of 0.
func pageLimit(size, left int) *int32 {
	n := size
	if left > 0 && (n == 0 || left < n) {
		n = left
	}
	if n == 0 {
		return nil
	}
	return new(int32(min(n, math.MaxInt32))) //nolint:gosec // clamped to int32
}

// queryInput builds the request of q. Every name and value is a placeholder: DynamoDB
// rejects a bare name that starts with "_".
func queryInput(table string, q store.Query) *dynamodb.QueryInput {
	names := map[string]string{"#pk": catalog.AttrPK}
	values := map[string]types.AttributeValue{":pk": s(q.PK)}
	cond := "#pk = :pk"
	switch {
	case q.From != "" && q.To != "":
		cond += " AND #sk BETWEEN :from AND :to"
	case q.From != "":
		cond += " AND #sk >= :from"
	case q.To != "" && q.ToExclusive:
		cond += " AND #sk < :to"
	case q.To != "":
		cond += " AND #sk <= :to"
	}
	if q.From != "" || q.To != "" {
		names["#sk"] = catalog.AttrSK
	}
	if q.From != "" {
		values[":from"] = s(q.From)
	}
	if q.To != "" {
		values[":to"] = s(q.To)
	}
	return &dynamodb.QueryInput{
		TableName:                 &table,
		KeyConditionExpression:    &cond,
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		ScanIndexForward:          new(!q.Desc),
		ConsistentRead:            new(q.Consistent),
	}
}

// Put implements [store.Backend]. On a failed condition DynamoDB returns the stored item
// (ReturnValuesOnConditionCheckFailure), which makes an SDK retry of a write that already
// landed safe: the caller finds its own write in Current.
func (b *Backend) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if err := store.CheckPut(it, c); err != nil {
		return fmt.Errorf("dynamo: put to %s: %w", table, err)
	}
	item, err := toItem(it)
	if err != nil {
		return fmt.Errorf("dynamo: put to %s: %w", table, err)
	}
	in := &dynamodb.PutItemInput{TableName: &table, Item: item}
	if v, ok := c.Version(); ok {
		in.ConditionExpression = new("#v = :v")
		in.ExpressionAttributeNames = map[string]string{"#v": catalog.AttrRowVersion}
		in.ExpressionAttributeValues = map[string]types.AttributeValue{
			":v": &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)},
		}
	}
	if c.Absent() {
		in.ConditionExpression = new("attribute_not_exists(#pk)")
		in.ExpressionAttributeNames = map[string]string{"#pk": catalog.AttrPK}
	}
	if in.ConditionExpression != nil {
		in.ReturnValuesOnConditionCheckFailure = types.ReturnValuesOnConditionCheckFailureAllOld
	}
	_, err = b.c.PutItem(ctx, in)
	if ccf, ok := errors.AsType[*types.ConditionalCheckFailedException](err); ok {
		if c.Absent() && len(ccf.Item) == 0 {
			// attribute_not_exists(pk) fails only over an item, so the endpoint ignored
			// ReturnValuesOnConditionCheckFailure; a nil Current would read as "absent".
			return fmt.Errorf("dynamo: put to %s: the condition failed without the stored item", table)
		}
		return &store.ConditionError{Current: fromItem(ccf.Item)}
	}
	if err != nil {
		return fmt.Errorf("dynamo: put to %s: %w", table, classify(err))
	}
	return nil
}

func keyAV(k store.Key) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{catalog.AttrPK: s(k.PK), catalog.AttrSK: s(k.SK)}
}

// keyOf reads the key of an item DynamoDB returned; both key attributes are strings.
func keyOf(av map[string]types.AttributeValue) store.Key {
	pk, okPK := av[catalog.AttrPK].(*types.AttributeValueMemberS)
	sk, okSK := av[catalog.AttrSK].(*types.AttributeValueMemberS)
	if !okPK || !okSK {
		return store.Key{}
	}
	return store.Key{PK: pk.Value, SK: sk.Value}
}

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynamo

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/lucindo/dynaflux/internal/store"
)

const (
	// DynamoDB applies a token's transaction at most once for 10 minutes, after which a
	// resend is a new request; the last minute is left to the SDK's retries of a send.
	tokenWindow = 9 * time.Minute
	inDoubtBase = 50 * time.Millisecond
	inDoubtMax  = time.Second
)

// Transact implements [store.Backend]. It sets the transaction's ClientRequestToken itself, so
// that a resend is applied at most once. While the outcome is in doubt after the SDK's
// retries (a transport error or timeout, a 5xx, or TransactionInProgressException, which AWS
// says to resend until at least 5 s after the first send), it resends the same input with
// backoff until the outcome is known, ctx ends, or the token's 10 minutes are nearly over. The
// last two return an error that says the outcome is unknown.
func (b *Backend) Transact(ctx context.Context, puts []store.TxPut) error {
	if err := store.CheckTransact(puts); err != nil {
		return fmt.Errorf("dynamo: transact: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("dynamo: transact: %w", err)
	}
	in, err := transactInput(puts, rand.Text())
	if err != nil {
		return fmt.Errorf("dynamo: transact: %w", err)
	}
	first := time.Now()
	var delay time.Duration
	for {
		_, err = b.c.TransactWriteItems(ctx, in)
		_, answered := errors.AsType[smithy.APIError](err)
		switch {
		case err == nil:
			return nil
		case !inDoubt(err) && (answered || ctx.Err() == nil):
			// DynamoDB's answer settles the outcome, even when ctx ended since.
			return outcome(err, puts)
		case ctx.Err() != nil:
			return fmt.Errorf("dynamo: transact: the outcome is unknown: %w", err)
		}
		delay = min(max(delay+delay, inDoubtBase), inDoubtMax)
		if time.Since(first)+delay >= tokenWindow {
			return fmt.Errorf("dynamo: transact: the outcome is unknown after resending for %v: %w", time.Since(first), err)
		}
		if serr := sleep(ctx, delay); serr != nil {
			return fmt.Errorf("dynamo: transact: the outcome is unknown: %w after %w", serr, err)
		}
	}
}

// transactInput builds the request of puts. Every conditioned put asks for the stored item
// on failure.
func transactInput(puts []store.TxPut, token string) (*dynamodb.TransactWriteItemsInput, error) {
	items := make([]types.TransactWriteItem, len(puts))
	for i, p := range puts {
		item, err := toItem(p.Item)
		if err != nil {
			return nil, fmt.Errorf("put %d: %w", i, err)
		}
		put := &types.Put{TableName: &p.Table, Item: item}
		put.ConditionExpression, put.ExpressionAttributeNames, put.ExpressionAttributeValues = condition(p.Cond)
		if put.ConditionExpression != nil {
			put.ReturnValuesOnConditionCheckFailure = types.ReturnValuesOnConditionCheckFailureAllOld
		}
		items[i] = types.TransactWriteItem{Put: put}
	}
	return &dynamodb.TransactWriteItemsInput{TransactItems: items, ClientRequestToken: &token}, nil
}

// inDoubt reports whether err may follow a transaction that DynamoDB applied: a request
// sent without a response read whole, a 5xx, or a transaction still in progress.
func inDoubt(err error) bool {
	if _, ok := errors.AsType[*types.TransactionInProgressException](err); ok {
		return true
	}
	if _, ok := errors.AsType[*smithyhttp.RequestSendError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*smithy.DeserializationError](err); ok {
		return true
	}
	re, ok := errors.AsType[*smithyhttp.ResponseError](err)
	return ok && re.HTTPStatusCode() >= http.StatusInternalServerError
}

// outcome sorts a known outcome other than success into the store's classes.
func outcome(err error, puts []store.TxPut) error {
	tce, ok := errors.AsType[*types.TransactionCanceledException](err)
	if !ok {
		return fmt.Errorf("dynamo: transact: %w", classify(err))
	}
	err = canceled(tce.CancellationReasons, puts)
	if _, failed := errors.AsType[*store.TxError](err); failed {
		return err
	}
	return fmt.Errorf("dynamo: transact: %w: %w", err, tce)
}

// canceled sorts a cancellation by its reasons, which line up with the puts. A refusal comes
// first, then failed conditions, then conflicts, then throttling: a conflict is never
// reported as throttling.
func canceled(reasons []types.CancellationReason, puts []store.TxPut) error {
	if len(reasons) != len(puts) {
		return fmt.Errorf("%d cancellation reasons for %d puts", len(reasons), len(puts))
	}
	var tx store.TxError
	var conflict, throttled, unknown bool
	for i, r := range reasons {
		switch code := aws.ToString(r.Code); code {
		case "None":
		case "ConditionalCheckFailed":
			// attribute_not_exists(pk) fails only over an item: see Put.
			if puts[i].Cond.Absent() && len(r.Item) == 0 {
				return fmt.Errorf("put %d: the condition failed without the stored item", i)
			}
			if tx.Conds == nil {
				tx.Conds = make([]*store.ConditionError, len(puts))
			}
			tx.Conds[i] = &store.ConditionError{Current: fromItem(r.Item)}
		case "TransactionConflict":
			conflict = true
		case "ThrottlingError", "ProvisionedThroughputExceeded":
			throttled = true
		case "ValidationError", "ItemCollectionSizeLimitExceeded":
			return fmt.Errorf("%w: put %d: %s", store.ErrInvalidRequest, i, code)
		default:
			unknown = true
		}
	}
	switch {
	case unknown:
		return errors.New("a cancellation reason the store doesn't know")
	case tx.Conds != nil:
		return &tx
	case conflict:
		return store.ErrConflict
	case throttled:
		return store.ErrThrottled
	}
	return errors.New("a cancellation without a reason")
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awssetup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"

	"github.com/lucindo/dynaflux/internal/setup"
)

const (
	// A query's state is read after pollFirst, then at doubling intervals up to pollMax.
	pollFirst = 500 * time.Millisecond
	pollMax   = 5 * time.Second
	// A throttled query is submitted again after resubmitFirst, then at doubling intervals
	// up to resubmitMax.
	resubmitFirst = time.Second
	resubmitMax   = 30 * time.Second
	// stopTimeout bounds the call that stops a query, which may run after the caller's
	// context ended.
	stopTimeout = 10 * time.Second
)

// Athena's error types (https://docs.aws.amazon.com/athena/latest/ug/error-reference.html).
const (
	errorThrottling      = 1400 // through 1406: Athena, Glue or S3 throttling
	errorTableVersions   = 1402 // too many Glue table versions, which waiting doesn't fix
	errorThrottlingLast  = 1406
	errorPermission      = 1500
	errorPermissionS3    = 1501
	commitConflictReason = "ICEBERG_COMMIT_ERROR"
	// Two CREATE TABLEs of one table race, such as a request CloudFormation delivered twice.
	alreadyExistsReason = "AlreadyExistsException"
)

// errThrottled marks a query that may succeed if submitted again.
var errThrottled = errors.New("throttled")

// athenaRunner implements [setup.Athena].
type athenaRunner struct {
	c athenaAPI
}

// Exec implements [setup.Athena]. It submits sql again while Athena fails it as throttled
// or retryable, until ctx ends.
func (a *athenaRunner) Exec(ctx context.Context, workgroup, sql string) error {
	for wait := resubmitFirst; ; wait = min(wait+wait, resubmitMax) {
		err := a.run(ctx, workgroup, sql)
		if !errors.Is(err, errThrottled) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(wait):
		}
	}
}

// run runs sql once and waits for it to finish. When the wait ends first, it stops the
// query, so that a statement CloudFormation was told failed, or that a retry submits
// again, doesn't commit later.
func (a *athenaRunner) run(ctx context.Context, workgroup, sql string) error {
	start, err := a.c.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{QueryString: &sql, WorkGroup: &workgroup})
	if _, ok := errors.AsType[*types.TooManyRequestsException](err); ok {
		return fmt.Errorf("%w: %w", errThrottled, err)
	}
	if err != nil {
		return classify(err)
	}
	id := aws.ToString(start.QueryExecutionId)
	finished, err := a.wait(ctx, id)
	if !finished {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
		defer cancel()
		if _, serr := a.c.StopQueryExecution(sctx, &athena.StopQueryExecutionInput{QueryExecutionId: &id}); serr != nil {
			err = errors.Join(err, fmt.Errorf("stop query %s: %w", id, serr))
		}
	}
	return err
}

// wait polls query id until it finishes, and returns its failure. It reports whether the
// query finished: not when ctx ended or a poll failed.
func (a *athenaRunner) wait(ctx context.Context, id string) (bool, error) {
	for wait := pollFirst; ; wait = min(wait+wait, pollMax) {
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("query %s: %w", id, ctx.Err())
		case <-time.After(wait):
		}
		out, err := a.c.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: &id})
		if err != nil {
			return false, fmt.Errorf("query %s: %w", id, classify(err))
		}
		switch st := out.QueryExecution.Status; st.State {
		case types.QueryExecutionStateSucceeded:
			return true, nil
		case types.QueryExecutionStateFailed, types.QueryExecutionStateCancelled:
			return true, failure(id, st)
		case types.QueryExecutionStateQueued, types.QueryExecutionStateRunning:
		}
	}
}

// failure classifies the end of a query that didn't succeed.
func failure(id string, st *types.QueryExecutionStatus) error {
	reason := aws.ToString(st.StateChangeReason)
	err := fmt.Errorf("query %s %s: %s", id, st.State, reason)
	var typ int32
	var retryable bool
	if e := st.AthenaError; e != nil {
		typ, retryable = aws.ToInt32(e.ErrorType), e.Retryable
	}
	switch {
	case typ == errorPermission || typ == errorPermissionS3:
		return fmt.Errorf("%w: %w", setup.ErrAccessDenied, err)
	case strings.Contains(reason, commitConflictReason) || strings.Contains(reason, alreadyExistsReason):
		return fmt.Errorf("%w: %w", setup.ErrConflict, err)
	case typ != errorTableVersions && (retryable || typ >= errorThrottling && typ <= errorThrottlingLast):
		return fmt.Errorf("%w: %w", errThrottled, err)
	}
	return err
}

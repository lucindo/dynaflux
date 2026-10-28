// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynamo

import (
	"errors"
	"fmt"

	"github.com/aws/smithy-go"

	"github.com/lucindo/dynaflux/internal/store"
)

// classify sorts an error left after the SDK's own retries into the store's classes,
// keeping the SDK's typed error reachable. A failed condition never reaches it, and
// ErrTooLarge never comes from a response: the checks made before sending decide it.
// TransactionInProgressException is neither a conflict nor throttling: the transaction may
// still commit.
func classify(err error) error {
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch apiErr.ErrorCode() {
		case "ValidationException", "ResourceNotFoundException", "IdempotentParameterMismatchException":
			return fmt.Errorf("%w: %w", store.ErrInvalidRequest, err)
		case "TransactionConflictException":
			return fmt.Errorf("%w: %w", store.ErrConflict, err)
		case "ProvisionedThroughputExceededException", "ThrottlingException", "RequestLimitExceeded":
			return fmt.Errorf("%w: %w", store.ErrThrottled, err)
		}
	}
	// 5xx, transport errors, a canceled context, and AccessDenied, which an IAM fix clears
	// with nothing lost: transient.
	return err
}

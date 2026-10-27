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
func classify(err error) error {
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch apiErr.ErrorCode() {
		case "ValidationException", "ResourceNotFoundException":
			return fmt.Errorf("%w: %w", store.ErrInvalidRequest, err)
		}
	}
	// Throttling, 5xx, transport errors, a canceled context, and AccessDenied, which an IAM
	// fix clears with nothing lost: transient.
	return err
}

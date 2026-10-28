// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package athenafail classifies Athena's failures by what a caller does about them, for the
// setup function's statements and the backfill's exports (ADR 0016).
package athenafail

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/smithy-go"
)

// Failure is the class of an Athena failure.
type Failure int

const (
	// Other is every failure the classes below leave out.
	Other Failure = iota
	// Throttled is a call refused for the request rate or the account's query
	// concurrency. A refused StartQueryExecution created no query.
	Throttled
	// TimedOut is a query that ran past the workgroup's time limit.
	TimedOut
	// Exhausted is a query that exhausted Athena's resources at its scale.
	Exhausted
	// Retryable is a query that may succeed if submitted again: Athena, Glue or S3
	// throttled it, or Athena says so.
	Retryable
	// Permission is a refusal for lack of a grant.
	Permission
	// User is a request that Athena refused as invalid, such as one with a parameter out of
	// range.
	User
)

// Athena's error types (https://docs.aws.amazon.com/athena/latest/ug/error-reference.html).
const (
	exhaustedLast  = 8 // 0 through 8
	timedOut       = 206
	timedOutToo    = 210
	throttling     = 1400 // through 1406: Athena, Glue or S3 throttling
	tableVersions  = 1402 // too many Glue table versions, which waiting doesn't fix
	throttlingLast = 1406
	permission     = 1500
	permissionS3   = 1501
	noType         = -1
)

func (f Failure) String() string {
	switch f {
	case Throttled:
		return "throttled"
	case TimedOut:
		return "timed out"
	case Exhausted:
		return "exhausted resources"
	case Retryable:
		return "retryable"
	case Permission:
		return "permission"
	case User:
		return "invalid request"
	case Other:
	}
	return "other"
}

// CallFailure classifies the error of an Athena API call, such as StartQueryExecution or
// GetQueryExecution.
func CallFailure(err error) Failure {
	if _, ok := errors.AsType[*types.TooManyRequestsException](err); ok {
		return Throttled
	}
	if _, ok := errors.AsType[*types.InvalidRequestException](err); ok {
		return User
	}
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch apiErr.ErrorCode() {
		case "ThrottlingException":
			return Throttled
		case "AccessDeniedException":
			return Permission
		}
	}
	return Other
}

// QueryFailure classifies the end of a query that didn't succeed by the AthenaError of its
// status, which may be nil.
func QueryFailure(e *types.AthenaError) Failure {
	if e == nil {
		return Other
	}
	typ := int32(noType)
	if e.ErrorType != nil {
		typ = *e.ErrorType
	}
	switch {
	case typ == permission || typ == permissionS3:
		return Permission
	case typ == timedOut || typ == timedOutToo:
		return TimedOut
	case typ >= 0 && typ <= exhaustedLast:
		return Exhausted
	case typ == tableVersions:
		return Other
	case e.Retryable || typ >= throttling && typ <= throttlingLast:
		return Retryable
	}
	return Other
}

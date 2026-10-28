// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package athenafail_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"

	"github.com/lucindo/dynaflux/internal/archive/athenafail"
)

// answer is an HTTP client that answers every request in process with one response.
type answer struct {
	status int
	body   string
	err    error
}

func (a answer) Do(*http.Request) (*http.Response, error) {
	if a.err != nil {
		return nil, a.err
	}
	rec := httptest.NewRecorder()
	rec.Header().Set("Content-Type", "application/x-amz-json-1.1")
	rec.WriteHeader(a.status)
	_, _ = io.WriteString(rec, a.body)
	return rec.Result(), nil
}

// The errors the SDK returns for StartQueryExecution, from AWS's JSON error responses, are
// classified by what the caller does about them.
func TestCallFailure(t *testing.T) {
	t.Parallel()
	refusal := func(fields map[string]string) answer {
		data, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		return answer{status: http.StatusBadRequest, body: string(data)}
	}
	tests := []struct {
		name   string
		answer answer
		want   athenafail.Failure
	}{
		{"query concurrency", refusal(map[string]string{
			"__type": "TooManyRequestsException", "Reason": "CONCURRENT_QUERY_LIMIT_EXCEEDED",
			"Message": "You have exceeded the limit for the number of queries you can run concurrently.",
		}), athenafail.Throttled},
		{"request rate", refusal(map[string]string{"__type": "ThrottlingException", "Message": "Rate exceeded"}), athenafail.Throttled},
		{"token reused for another statement", refusal(map[string]string{
			"__type": "InvalidRequestException", "AthenaErrorCode": "INVALID_INPUT", "Message": "Idempotent parameters do not match",
		}), athenafail.User},
		{"no grant", refusal(map[string]string{
			"__type": "AccessDeniedException",
			"Message": "User: arn:aws:sts::123456789012:assumed-role/prod-analytics-backfill/fn is not authorized to perform: " +
				"athena:StartQueryExecution on resource: arn:aws:athena:us-east-1:123456789012:workgroup/prod-analytics-backfill",
		}), athenafail.Permission},
		{"server error", answer{status: http.StatusInternalServerError, body: `{"__type":"InternalServerException","Message":"Internal error"}`}, athenafail.Other},
		{"transport", answer{err: io.ErrUnexpectedEOF}, athenafail.Other},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := athena.NewFromConfig(aws.Config{
				Region:       "us-east-1",
				BaseEndpoint: new("http://127.0.0.1:1"),
				Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
					return aws.Credentials{AccessKeyID: "AKIDFAKE", SecretAccessKey: "fake", Source: "test"}, nil
				}),
				HTTPClient: tt.answer,
				Retryer:    func() aws.Retryer { return aws.NopRetryer{} },
			})
			_, err := c.StartQueryExecution(t.Context(), &athena.StartQueryExecutionInput{QueryString: new("SELECT 1"), WorkGroup: new("wg")})
			if err == nil {
				t.Fatal("the call succeeded")
			}
			if got := athenafail.CallFailure(err); got != tt.want {
				t.Errorf("%v is %v, want %v", err, got, tt.want)
			}
		})
	}
	if got := athenafail.CallFailure(errors.New("not from AWS")); got != athenafail.Other {
		t.Errorf("a plain error is %v, want %v", got, athenafail.Other)
	}
}

// A failed query's AthenaError gives its class: its error type first, then the Retryable flag.
func TestQueryFailure(t *testing.T) {
	t.Parallel()
	failed := func(typ int32, retryable bool) *types.AthenaError {
		return &types.AthenaError{ErrorCategory: new(int32(2)), ErrorType: new(typ), Retryable: retryable}
	}
	tests := []struct {
		name string
		err  *types.AthenaError
		want athenafail.Failure
	}{
		{"no AthenaError, as for a cancelled query", nil, athenafail.Other},
		{"no type", &types.AthenaError{}, athenafail.Other},
		{"no type, retryable", &types.AthenaError{Retryable: true}, athenafail.Retryable},
		{"exhausted resources, first type", failed(0, false), athenafail.Exhausted},
		{"exhausted resources, last type", failed(8, false), athenafail.Exhausted},
		{"internal service error", failed(100, false), athenafail.Other},
		{"internal service error, retryable", failed(100, true), athenafail.Retryable},
		{"timed out", failed(206, false), athenafail.TimedOut},
		{"timed out, retryable", failed(206, true), athenafail.TimedOut},
		{"failed to cancel", failed(209, false), athenafail.Other},
		{"timed out, other type", failed(210, false), athenafail.TimedOut},
		{"Iceberg error", failed(233, false), athenafail.Other},
		{"syntax error", failed(1006, false), athenafail.Other},
		{"throttling", failed(1400, false), athenafail.Retryable},
		{"Glue throttling", failed(1401, false), athenafail.Retryable},
		{"too many table versions, retryable", failed(1402, true), athenafail.Other},
		{"S3 throttling", failed(1403, false), athenafail.Retryable},
		{"Athena throttling", failed(1406, false), athenafail.Retryable},
		{"past the throttling types", failed(1407, false), athenafail.Other},
		{"permission", failed(1500, false), athenafail.Permission},
		{"S3 permission", failed(1501, true), athenafail.Permission},
	}
	for _, tt := range tests {
		if got := athenafail.QueryFailure(tt.err); got != tt.want {
			t.Errorf("%s: got %v, want %v", tt.name, got, tt.want)
		}
	}
}

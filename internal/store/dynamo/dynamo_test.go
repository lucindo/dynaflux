// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynamo_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go"

	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// newBackend returns a backend whose client sends every request through h, with fake
// credentials and the SDK's standard retries without backoff.
func newBackend(h aws.HTTPClient) *dynamo.Backend {
	return dynamo.New(dynamodb.NewFromConfig(aws.Config{
		Region:       "us-east-1",
		BaseEndpoint: new("http://127.0.0.1:1"), // never dialed
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "fake", SecretAccessKey: "fake"}, nil
		}),
		HTTPClient: h,
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.RateLimiter = ratelimit.None
				o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
			})
		},
	}))
}

// fake answers each request with what it returns for the request's operation and body,
// without a network.
type fake func(op string, body []byte) (status int, response string)

func (f fake) Do(r *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	_, op, _ := strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	status, response := f(op, body)
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader(response)),
		Request:    r,
	}, nil
}

func errorBody(code string) string {
	return `{"__type":"com.amazonaws.dynamodb.v20120810#` + code + `","message":"fake"}`
}

func str(v string) store.Value { return store.Value{Type: store.S, Str: v} }

var key = store.Key{PK: "p", SK: "a"} //nolint:gochecknoglobals // a constant struct

// ops calls each operation of b once and returns their errors by name.
func ops(ctx context.Context, b store.Backend) map[string]error {
	_, getErr := b.Get(ctx, "t", key, true)
	_, batchErr := b.BatchGet(ctx, "t", []store.Key{key}, true)
	var queryErr error
	for _, err := range b.Query(ctx, "t", store.Query{PK: key.PK}) {
		if err != nil {
			queryErr = err
		}
	}
	putErr := b.Put(ctx, "t", store.Item{"pk": str(key.PK), "sk": str(key.SK)}, store.Cond{})
	return map[string]error{"Get": getErr, "BatchGet": batchErr, "Query": queryErr, "Put": putErr}
}

// The SDK retries what may pass on a later attempt; the backend sorts what is left. Only a
// request DynamoDB rejects as malformed is ErrInvalidRequest, and the SDK's error stays
// reachable.
func TestErrorClasses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status   int
		code     string
		invalid  bool
		attempts int32
	}{
		{400, "ProvisionedThroughputExceededException", false, 3},
		{400, "ThrottlingException", false, 3},
		{400, "RequestLimitExceeded", false, 3},
		{500, "InternalServerError", false, 3},
		{503, "ServiceUnavailable", false, 3},
		{400, "TransactionConflictException", false, 1},
		{400, "ReplicatedWriteConflictException", false, 1},
		{400, "AccessDeniedException", false, 1},
		{400, "UnrecognizedClientException", false, 1},
		{400, "ValidationException", true, 1},
		{400, "ResourceNotFoundException", true, 1},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			b := newBackend(fake(func(string, []byte) (int, string) {
				attempts.Add(1)
				return tt.status, errorBody(tt.code)
			}))
			for op, err := range ops(t.Context(), b) {
				apiErr, ok := errors.AsType[smithy.APIError](err)
				if errors.Is(err, store.ErrInvalidRequest) != tt.invalid || !ok || apiErr.ErrorCode() != tt.code {
					t.Errorf("%s: error %v; want the SDK's %s, ErrInvalidRequest %v", op, err, tt.code, tt.invalid)
				}
			}
			if got, want := attempts.Load(), 4*tt.attempts; got != want {
				t.Errorf("%d attempts for four operations, want %d", got, want)
			}
		})
	}
}

// The SDK sends an invalid UTF-8 byte as U+FFFD without an error, so it would name another
// key or value; store's checks stop such a request before anything is sent.
func TestInvalidRequestsSendNothing(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	b := newBackend(fake(func(string, []byte) (int, string) {
		attempts.Add(1)
		return 200, `{}`
	}))
	bad := store.Key{PK: "p", SK: "a\xff"}
	ctx := t.Context()
	_, getErr := b.Get(ctx, "t", bad, true)
	_, batchErr := b.BatchGet(ctx, "t", []store.Key{key, bad}, true)
	var queryErr error
	for _, err := range b.Query(ctx, "t", store.Query{PK: bad.SK}) {
		queryErr = err
	}
	putErr := b.Put(ctx, "t", store.Item{"pk": str("p"), "sk": str("a"), "x": str("\xff")}, store.Cond{})
	for op, err := range map[string]error{"Get": getErr, "BatchGet": batchErr, "Query": queryErr, "Put": putErr} {
		if !errors.Is(err, store.ErrInvalidRequest) {
			t.Errorf("%s: error %v, want ErrInvalidRequest", op, err)
		}
	}
	if n := attempts.Load(); n != 0 {
		t.Errorf("%d requests sent, want none", n)
	}
}

// A transport error leaves the outcome unknown, so the SDK retries it and it stays transient.
func TestTransportErrorIsTransient(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	reset := errors.New("connection reset by peer")
	b := newBackend(httpFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, reset
	}))
	_, err := b.Get(t.Context(), "t", key, true)
	if !errors.Is(err, reset) || errors.Is(err, store.ErrInvalidRequest) || attempts.Load() != 3 {
		t.Errorf("Get = %v after %d attempts; want the transport error after 3", err, attempts.Load())
	}
}

type httpFunc func(*http.Request) (*http.Response, error)

func (f httpFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

// Only a foreign writer leaves NULL or a set; the backend returns the zero Value for them.
func TestForeignTypesReadAsZeroValue(t *testing.T) {
	t.Parallel()
	b := newBackend(fake(func(string, []byte) (int, string) {
		return 200, `{"Item":{"pk":{"S":"p"},"sk":{"S":"a"},"n":{"NULL":true},"ss":{"SS":["x"]},` +
			`"l":{"L":[{"NS":["1"]},{"S":"y"}]}}}`
	}))
	got, err := b.Get(t.Context(), "t", key, true)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := store.Item{
		"pk": str("p"), "sk": str("a"), "n": {}, "ss": {},
		"l": {Type: store.L, List: []store.Value{{}, str("y")}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Get = %v, want %v", got, want)
	}
}

// A failed attribute_not_exists(pk) always has an item to return, so a failure without one
// means the endpoint ignored ALL_OLD, and a nil Current would read as "absent". A failed
// _v = :v without an item is an absent item.
func TestConditionFailedWithoutItem(t *testing.T) {
	t.Parallel()
	b := newBackend(fake(func(string, []byte) (int, string) {
		return 400, errorBody("ConditionalCheckFailedException")
	}))
	it := store.Item{"pk": str("p"), "sk": str("a"), "_v": {Type: store.N, Str: "2"}}

	err := b.Put(t.Context(), "t", it, store.IfAbsent())
	if want := "dynamo: put to t: the condition failed without the stored item"; err == nil || err.Error() != want {
		t.Errorf("IfAbsent: Put = %v, want %q", err, want)
	}
	err = b.Put(t.Context(), "t", it, store.IfVersion(1))
	if ce, ok := errors.AsType[*store.ConditionError](err); !ok || ce.Current != nil {
		t.Errorf("IfVersion: Put = %v, want a *store.ConditionError with no Current", err)
	}
}

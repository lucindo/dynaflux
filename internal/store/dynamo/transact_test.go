// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynamo_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/smithy-go"

	"github.com/lucindo/dynaflux/internal/store"
)

// The transaction most tests send: an event put if absent, a row put at version 1.
func txPuts() []store.TxPut {
	return []store.TxPut{
		{Table: "t", Item: store.Item{"pk": str("p"), "sk": str("event")}, Cond: store.IfAbsent()},
		{Table: "t", Item: store.Item{"pk": str("p"), "sk": str("row"), "_v": {Type: store.N, Str: "2"}}, Cond: store.IfVersion(1)},
	}
}

// txServer answers each TransactWriteItems request with what answer returns for the
// request's index and its time since the first, and notes each request's token and time.
type txServer struct {
	answer func(n int, since time.Duration) (status int, body string, err error)

	mu     sync.Mutex
	tokens []string
	at     []time.Time
}

func (s *txServer) Do(r *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	var in struct {
		ClientRequestToken string `json:"ClientRequestToken"`
	}
	if err = json.Unmarshal(body, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.tokens = append(s.tokens, in.ClientRequestToken)
	s.at = append(s.at, time.Now())
	n, since := len(s.at)-1, time.Since(s.at[0])
	s.mu.Unlock()
	status, response, err := s.answer(n, since)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader(response)),
		Request:    r,
	}, nil
}

// sent returns the requests' tokens, and how long after the first the last one arrived.
func (s *txServer) sent() ([]string, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.at) == 0 {
		return nil, 0
	}
	return s.tokens, s.at[len(s.at)-1].Sub(s.at[0])
}

// canceled is a TransactionCanceledException body with the given reasons, as JSON objects.
func canceled(reasons ...string) string {
	return `{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","message":"fake",` +
		`"CancellationReasons":[` + strings.Join(reasons, ",") + `]}`
}

const (
	none      = `{"Code":"None"}`
	conflict  = `{"Code":"TransactionConflict","Message":"fake"}`
	throttled = `{"Code":"ThrottlingError","Message":"fake"}`
	failed    = `{"Code":"ConditionalCheckFailed","Message":"fake"}`
)

func failedWith(item string) string {
	return `{"Code":"ConditionalCheckFailed","Message":"fake","Item":` + item + `}`
}

// Names of the store's error classes.
const (
	condition = "ErrConditionFailed"
	invalid   = "ErrInvalidRequest"
	conflicts = "ErrConflict"
	throttles = "ErrThrottled"
)

// classes returns the names of the store's error classes that err matches.
func classes(err error) []string {
	var out []string
	for _, c := range []struct {
		name  string
		class error
	}{{condition, store.ErrConditionFailed}, {invalid, store.ErrInvalidRequest}, {conflicts, store.ErrConflict}, {throttles, store.ErrThrottled}} {
		if errors.Is(err, c.class) {
			out = append(out, c.name)
		}
	}
	return out
}

// Every conditioned put asks for the stored item on failure, and each call has a token of
// its own.
func TestTransactRequest(t *testing.T) {
	t.Parallel()
	var bodies []string
	b := newBackend(fake(func(_ string, body []byte) (int, string) {
		bodies = append(bodies, string(body))
		return 200, `{}`
	}))
	puts := append(txPuts(), store.TxPut{Table: "u", Item: store.Item{"pk": str("p"), "sk": str("plain")}})
	for range 2 {
		if err := b.Transact(t.Context(), puts); err != nil {
			t.Fatalf("Transact: %v", err)
		}
	}
	type put struct {
		TableName                           string            `json:"TableName"`
		ConditionExpression                 string            `json:"ConditionExpression"`
		ExpressionAttributeNames            map[string]string `json:"ExpressionAttributeNames"`
		ReturnValuesOnConditionCheckFailure string            `json:"ReturnValuesOnConditionCheckFailure"`
	}
	var tokens []string
	for _, body := range bodies {
		var in struct {
			ClientRequestToken string `json:"ClientRequestToken"`
			TransactItems      []struct {
				Put put `json:"Put"`
			} `json:"TransactItems"`
		}
		if err := json.Unmarshal([]byte(body), &in); err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, in.ClientRequestToken)
		var got []put
		for _, it := range in.TransactItems {
			got = append(got, it.Put)
		}
		want := []put{
			{"t", "attribute_not_exists(#pk)", map[string]string{"#pk": "pk"}, "ALL_OLD"},
			{"t", "#v = :v", map[string]string{"#v": "_v"}, "ALL_OLD"},
			{"u", "", nil, ""},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("puts = %+v, want %+v", got, want)
		}
	}
	if len(tokens) != 2 || tokens[0] == "" || len(tokens[0]) > 36 || tokens[0] == tokens[1] {
		t.Errorf("tokens = %q, want two different tokens of 1 to 36 characters", tokens)
	}
}

// A cancellation's reasons line up with the puts. A failed condition wins over a conflict,
// and a conflict over throttling.
func TestTransactCancellations(t *testing.T) {
	t.Parallel()
	event := `{"pk":{"S":"p"},"sk":{"S":"event"},"_r":{"S":"r1"}}`
	tests := []struct {
		name    string
		reasons []string
		want    []string
		conds   []store.Item // for a *store.TxError: each put's Current; nil where none failed
	}{
		{"conflict", []string{none, conflict}, []string{conflicts}, nil},
		{"throttled", []string{throttled, none}, []string{throttles}, nil},
		{"throughput", []string{none, `{"Code":"ProvisionedThroughputExceeded"}`}, []string{throttles}, nil},
		{"conflict and throttled", []string{throttled, conflict}, []string{conflicts}, nil},
		{"validation", []string{none, `{"Code":"ValidationError"}`}, []string{invalid}, nil},
		{"item collection", []string{`{"Code":"ItemCollectionSizeLimitExceeded"}`, none}, []string{invalid}, nil},
		{"event taken", []string{failedWith(event), none}, []string{condition},
			[]store.Item{{"pk": str("p"), "sk": str("event"), "_r": str("r1")}, nil}},
		{"absent row", []string{none, failed}, []string{condition}, []store.Item{nil, nil}},
		{"failed and conflict", []string{failedWith(event), conflict}, []string{condition},
			[]store.Item{{"pk": str("p"), "sk": str("event"), "_r": str("r1")}, nil}},
		// attribute_not_exists(pk) fails only over an item.
		{"event taken without its item", []string{failed, none}, nil, nil},
		{"too few reasons", []string{conflict}, nil, nil},
		{"unknown reason", []string{none, `{"Code":"Mystery"}`}, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			requests := 0
			b := newBackend(fake(func(string, []byte) (int, string) {
				requests++
				return 400, canceled(tt.reasons...)
			}))
			err := b.Transact(t.Context(), txPuts())
			if err == nil || !reflect.DeepEqual(classes(err), tt.want) {
				t.Fatalf("Transact = %v, classes %v; want %v", err, classes(err), tt.want)
			}
			if requests != 1 {
				t.Errorf("%d requests, want 1: a cancellation is a known outcome", requests)
			}
			if tt.conds != nil {
				wantConds(t, err, tt.reasons, tt.conds)
			}
		})
	}
}

// DynamoDB's answer settles the outcome even when ctx ends while it is on its way.
func TestTransactCancellationAsCtxEnds(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	event := `{"pk":{"S":"p"},"sk":{"S":"event"},"_r":{"S":"r1"}}`
	reasons := []string{failedWith(event), none}
	b := newBackend(fake(func(string, []byte) (int, string) {
		cancel()
		return 400, canceled(reasons...)
	}))
	err := b.Transact(ctx, txPuts())
	if !reflect.DeepEqual(classes(err), []string{condition}) {
		t.Fatalf("Transact = %v, classes %v; want %v", err, classes(err), []string{condition})
	}
	wantConds(t, err, reasons, []store.Item{{"pk": str("p"), "sk": str("event"), "_r": str("r1")}, nil})
}

// wantConds fails unless err is a *store.TxError with a failure over conds[i] for each
// reason i that is a failed condition, and none elsewhere.
func wantConds(t *testing.T, err error, reasons []string, conds []store.Item) {
	t.Helper()
	txErr, ok := errors.AsType[*store.TxError](err)
	if !ok || len(txErr.Conds) != len(conds) {
		t.Fatalf("Transact = %v, want a *store.TxError with %d Conds", err, len(conds))
	}
	for i, want := range conds {
		c := txErr.Conds[i]
		failedHere := strings.Contains(reasons[i], "ConditionalCheckFailed")
		if (c != nil) != failedHere || c != nil && !reflect.DeepEqual(c.Current, want) {
			t.Errorf("Conds[%d] = %v, want a failure %v over %v", i, c, failedHere, want)
		}
	}
}

// Errors other than cancellations, and what isn't in doubt, end the call at once: only the
// three throttling exceptions are ErrThrottled.
func TestTransactErrorClasses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code string
		want []string
	}{
		{"ThrottlingException", []string{throttles}},
		{"ProvisionedThroughputExceededException", []string{throttles}},
		{"RequestLimitExceeded", []string{throttles}},
		{"TransactionConflictException", []string{conflicts}},
		{"ValidationException", []string{invalid}},
		{"ResourceNotFoundException", []string{invalid}},
		{"IdempotentParameterMismatchException", []string{invalid}},
		{"AccessDeniedException", nil},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			t.Parallel()
			b := newBackend(fake(func(string, []byte) (int, string) { return 400, errorBody(tt.code) }))
			err := b.Transact(t.Context(), txPuts())
			apiErr, ok := errors.AsType[smithy.APIError](err)
			if !ok || apiErr.ErrorCode() != tt.code || !reflect.DeepEqual(classes(err), tt.want) {
				t.Errorf("Transact = %v, classes %v; want the SDK's %s, classes %v", err, classes(err), tt.code, tt.want)
			}
		})
	}
}

// While the outcome is in doubt, Transact resends the same request with the same token,
// past the SDK's retries, until the outcome is known.
func TestTransactResendsInDoubt(t *testing.T) {
	t.Parallel()
	inProgress := func() (int, string, error) { return 400, errorBody("TransactionInProgressException"), nil }
	lost := func() (int, string, error) { return 0, "", errors.New("read: connection reset by peer") }
	failing := func() (int, string, error) { return 500, errorBody("InternalServerError"), nil }
	tests := []struct {
		name string
		fail func() (int, string, error)
		// The requests fail is the answer to: the first failures, and those within the
		// given time of the first request; the rest succeed.
		failures int
		within   time.Duration
	}{
		{"in progress, then committed", inProgress, 1, 0},
		{"responses lost past the SDK's retries", lost, 3, 0},
		{"5xx past the SDK's retries", failing, 3, 0},
		{"in progress for 5 s", inProgress, 0, 5 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				srv := &txServer{answer: func(n int, since time.Duration) (int, string, error) {
					if n < tt.failures || since < tt.within {
						return tt.fail()
					}
					return 200, `{}`, nil
				}}
				if err := newBackend(srv).Transact(t.Context(), txPuts()); err != nil {
					t.Fatalf("Transact = %v, want success", err)
				}
				tokens, span := srv.sent()
				if tt.within == 0 && len(tokens) != tt.failures+1 || span < tt.within {
					t.Errorf("%d requests, the last %v after the first; want %d, or one %v or more after it",
						len(tokens), span, tt.failures+1, tt.within)
				}
				wantOneToken(t, tokens)
			})
		})
	}
}

func wantOneToken(t *testing.T, tokens []string) {
	t.Helper()
	if len(slices.Compact(slices.Clone(tokens))) != 1 || tokens[0] == "" {
		t.Errorf("tokens = %q, want one token on every request", tokens)
	}
}

// When ctx ends or the token's window nears its end first, the error says the outcome is
// unknown, and is neither a conflict nor throttling.
func TestTransactOutcomeUnknown(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		status  int
		code    string
		timeout time.Duration
		cause   error // nil: the window ends the call first
	}{
		{"5xx until ctx ends", 500, "InternalServerError", 3 * time.Second, context.DeadlineExceeded},
		{"in progress until ctx ends", 400, "TransactionInProgressException", 3 * time.Second, context.DeadlineExceeded},
		{"in progress past the token's window", 400, "TransactionInProgressException", 11 * time.Minute, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				srv := &txServer{answer: func(int, time.Duration) (int, string, error) {
					return tt.status, errorBody(tt.code), nil
				}}
				ctx, cancel := context.WithTimeout(t.Context(), tt.timeout)
				defer cancel()
				err := newBackend(srv).Transact(ctx, txPuts())
				if err == nil || !strings.Contains(err.Error(), "the outcome is unknown") || classes(err) != nil {
					t.Errorf("Transact = %v, classes %v; want the outcome unknown and no class", err, classes(err))
				}
				if tt.cause != nil && !errors.Is(err, tt.cause) || tt.cause == nil && ctx.Err() != nil {
					t.Errorf("Transact = %v, ctx %v; want it to match %v", err, ctx.Err(), tt.cause)
				}
				if _, span := srv.sent(); span >= 10*time.Minute {
					t.Errorf("the last request arrived %v after the first, past the token's 10 minutes", span)
				}
			})
		})
	}
}

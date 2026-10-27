// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynamo_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// server notes when each request has arrived whole, and how many handlers are still open.
type server struct {
	mu       sync.Mutex
	arrivals []time.Time
	open     int
}

func (s *server) handle(then func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			return // the client went away mid-request
		}
		s.mu.Lock()
		s.arrivals = append(s.arrivals, time.Now())
		s.open++
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			s.open--
			s.mu.Unlock()
		}()
		then(w, r)
	}
}

func (s *server) state() ([]time.Time, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.arrivals), s.open
}

func throttle(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusBadRequest)
	io.WriteString(w, errorBody("ThrottlingException"))
}

func hang(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }

// slack bounds how long after the deadline an attempt may start, or a request arrive: the
// context's timer can fire late, and net/http may finish writing an attempt the SDK started
// before it. Both take about a millisecond, well within the projector's reserve of 0.5 s.
const slack = 50 * time.Millisecond

// putUntil sends one Put to a server that answers with then, under a context that ends
// after timeout, and checks what the projector's cutoff relies on: once the context ends,
// the SDK soon sends nothing more, the call returns without waiting for the server, and no
// attempt is left in flight. It returns the attempts.
func putUntil(t *testing.T, then http.HandlerFunc, timeout time.Duration) []emulatortest.Call {
	t.Helper()
	var srv server
	ts := httptest.NewServer(srv.handle(then))
	t.Cleanup(ts.Close)
	// No attempt timeout: only the context may end a hung call.
	rec := emulatortest.NewRecorder(awshttp.NewBuildableClient())
	b := dynamo.New(dynamodb.NewFromConfig(aws.Config{
		Region:       "us-east-1",
		BaseEndpoint: &ts.URL,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "fake", SecretAccessKey: "fake"}, nil
		}),
		HTTPClient: rec,
		// Retries every 10 ms, without a limit the deadline could hide behind.
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.RateLimiter = ratelimit.None
				o.MaxAttempts = 1000
				o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 10 * time.Millisecond, nil })
			})
		},
	}))
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	err := b.Put(ctx, "t", store.Item{"pk": str("p"), "sk": str("a")}, store.Cond{})
	returned := time.Now()

	if n := rec.InFlight(); n != 0 {
		t.Errorf("%d attempts in flight when Put returned", n)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Put = %v, want context.DeadlineExceeded", err)
	}
	if late := returned.Sub(deadline); late > time.Second {
		t.Errorf("Put returned %v after the deadline", late)
	}
	calls := rec.Calls()
	for i, c := range calls {
		if c.Start.After(deadline.Add(slack)) {
			t.Errorf("attempt %d sent %v after the deadline", i, c.Start.Sub(deadline))
		}
	}
	// The server sees the connection close and gets nothing more.
	time.Sleep(100 * time.Millisecond)
	arrivals, open := srv.state()
	for i, at := range arrivals {
		if at.After(returned.Add(slack)) {
			t.Errorf("request %d arrived %v after Put returned", i, at.Sub(returned))
		}
	}
	if open != 0 {
		t.Errorf("%d requests still open on the server", open)
	}
	return calls
}

func TestNoRequestAfterDeadline(t *testing.T) {
	t.Parallel()
	t.Run("throttled", func(t *testing.T) {
		t.Parallel()
		if calls := putUntil(t, throttle, 300*time.Millisecond); len(calls) < 2 {
			t.Errorf("%d attempts, want retries until the deadline", len(calls))
		}
	})
	t.Run("hung", func(t *testing.T) {
		t.Parallel()
		if calls := putUntil(t, hang, 200*time.Millisecond); len(calls) != 1 {
			t.Errorf("%d attempts, want 1", len(calls))
		}
	})
	t.Run("expired", func(t *testing.T) {
		t.Parallel()
		if calls := putUntil(t, throttle, -time.Millisecond); len(calls) != 0 {
			t.Errorf("%d attempts with an expired context, want none", len(calls))
		}
	})
}

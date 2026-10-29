// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package emulatortest_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lucindo/dynaflux/internal/emulatortest"
)

// Each attempt is a call of its own, so a test sees retries.
func TestRecorderSeesEachAttempt(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		if requests.Add(1) == 1 {
			w.Header().Set("X-Amzn-Errortype", "InternalServerError:http://internal.amazon.com/coral/com.amazon.coral.service/")
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `{"message":"try again"}`)
			return
		}
		fmt.Fprint(w, `{}`)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("DYNAFLUX_DYNAMODB_ENDPOINT", srv.URL)
	rec := emulatortest.NewRecorder(nil)
	c := dynamodb.NewFromConfig(emulatortest.Config(t, rec), func(o *dynamodb.Options) {
		o.Retryer = retry.AddWithMaxBackoffDelay(o.Retryer, time.Millisecond)
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	deadline, _ := ctx.Deadline()
	_, err := c.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      new("t"),
		Key:            map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "p"}, "sk": &types.AttributeValueMemberS{Value: "a"}},
		ConsistentRead: new(true),
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}

	calls := rec.Calls()
	for i, c := range calls {
		if c.Start.IsZero() || c.End.Before(c.Start) || i > 0 && c.Start.Before(calls[i-1].End) {
			t.Errorf("call %d: sent %v, read %v; want each sent after the previous one was read", i, c.Start, c.End)
		}
	}
	for i := range calls {
		calls[i].Start, calls[i].End = time.Time{}, time.Time{}
	}
	get := emulatortest.Call{
		Target: "DynamoDB_20120810.GetItem", Resources: []string{"t"}, Consistent: true, Deadline: deadline,
	}
	failed, ok := get, get
	failed.Status, failed.ErrorCode = http.StatusInternalServerError, "InternalServerError"
	ok.Status = http.StatusOK
	if want := []emulatortest.Call{failed, ok}; !reflect.DeepEqual(calls, want) {
		t.Errorf("calls:\n got  %+v\n want %+v", calls, want)
	}
	if n := rec.InFlight(); n != 0 {
		t.Errorf("InFlight = %d after the call returned", n)
	}
}

// An attempt handed over after its context ended is flagged, and net/http never writes it.
func TestRecorderFlagsAttemptsAfterTheEnd(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{}`) }))
	t.Cleanup(srv.Close)
	rec := emulatortest.NewRecorder(nil)
	ctx, cancel := context.WithCancel(t.Context())
	send := func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, strings.NewReader(`{"TableName":"t"}`))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		res, err := rec.Do(req)
		if err == nil {
			err = res.Body.Close()
		}
		return err
	}
	if err := send(); err != nil {
		t.Fatalf("Do before the end: %v", err)
	}
	cancel()
	if err := send(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Do after the end = %v, want context.Canceled", err)
	}

	type seen struct{ afterEnd, written bool }
	var got []seen
	for _, c := range rec.Calls() {
		got = append(got, seen{c.AfterEnd, !c.Start.IsZero()})
	}
	if want := []seen{{false, true}, {true, false}}; !slices.Equal(got, want) {
		t.Errorf("calls as {after the end, written} = %v, want %v", got, want)
	}
}

// fatalTB records the message of Fatalf and ends the goroutine, as [testing.T] does.
type fatalTB struct {
	testing.TB

	msg string
}

func (*fatalTB) Helper() {}

func (f *fatalTB) Fatalf(format string, args ...any) {
	f.msg = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// fatal runs fn with a [testing.TB] whose Fatalf ends it, and returns the message.
func fatal(t *testing.T, fn func(testing.TB)) string {
	t.Helper()
	tb := &fatalTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(tb)
	}()
	<-done
	return tb.msg
}

// Tests never reach AWS: a client config only for a loopback endpoint.
func TestConfigRefusesOtherHosts(t *testing.T) {
	refused := func(ep string) string {
		return fmt.Sprintf("DYNAFLUX_DYNAMODB_ENDPOINT is %q, which isn't on loopback: tests never reach AWS", ep)
	}
	tests := []struct{ endpoint, want string }{
		{"", "DYNAFLUX_DYNAMODB_ENDPOINT is unset: run make integration, or make emulators-up with DYNAFLUX_DYNAMODB_ENDPOINT=http://127.0.0.1:8000"},
		{"https://dynamodb.us-east-1.amazonaws.com", refused("https://dynamodb.us-east-1.amazonaws.com")},
		{"http://10.0.0.1:8000", refused("http://10.0.0.1:8000")},
		{"http://127.0.0.1:8000", ""},
		{"http://localhost:8000", ""},
		{"http://[::1]:8000", ""},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			t.Setenv("DYNAFLUX_DYNAMODB_ENDPOINT", tt.endpoint)
			got := fatal(t, func(tb testing.TB) {
				tb.Helper()
				emulatortest.Config(tb, nil)
			})
			if got != tt.want {
				t.Errorf("Config fails with %q, want %q", got, tt.want)
			}
		})
	}
}

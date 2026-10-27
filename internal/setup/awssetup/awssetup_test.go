// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awssetup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/lucindo/dynaflux/internal/setup"
	"github.com/lucindo/dynaflux/internal/setup/awssetup"
)

// answer is a response as AWS sends it: an HTTP status and a JSON body.
type answer struct {
	status int
	body   string
}

func ok(body string) answer { return answer{http.StatusOK, body} }

// hang is an answer that never comes: the call waits until its context ends.
var hang = answer{} //nolint:gochecknoglobals // test fixture

// awsError is a JSON protocol error response of type code.
func awsError(status int, code, message string) answer {
	data, err := json.Marshal(map[string]string{"__type": code, "Message": message})
	if err != nil {
		panic(err)
	}
	return answer{status, string(data)}
}

// call is a request the SDK sent: its operation, when, and its JSON body.
type call struct {
	op   string
	at   time.Time
	body string
}

// fakeAWS answers the SDK's requests in process, with httptest's recorder, from scripted
// answers by operation: each in turn, the last one again and again. It records every
// request. It opens no socket, so tests can run in a synctest bubble.
type fakeAWS struct {
	t *testing.T

	mu      sync.Mutex
	answers map[string][]answer
	calls   []call
}

func newFakeAWS(t *testing.T, answers map[string][]answer) *fakeAWS {
	t.Helper()
	return &fakeAWS{t: t, answers: answers}
}

func (f *fakeAWS) Do(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	// X-Amz-Target is "<service>.<operation>" in every JSON protocol.
	_, op, _ := strings.Cut(req.Header.Get("X-Amz-Target"), ".")
	f.mu.Lock()
	f.calls = append(f.calls, call{op: op, at: time.Now(), body: string(body)})
	queue := f.answers[op]
	if len(queue) == 0 {
		f.mu.Unlock()
		f.t.Errorf("unexpected call %s %s", op, body)
		return nil, io.ErrUnexpectedEOF
	}
	a := queue[0]
	if len(queue) > 1 {
		f.answers[op] = queue[1:]
	}
	f.mu.Unlock()
	if a == hang {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	rec := httptest.NewRecorder()
	rec.Header().Set("Content-Type", "application/x-amz-json-1.1")
	rec.Header().Set("X-Amzn-Requestid", "4c2d9a3e-0b1f-4e57-9d1a-5b6c7d8e9f00")
	rec.WriteHeader(a.status)
	io.WriteString(rec, a.body)
	return rec.Result(), nil
}

// ops returns the operations of the calls so far.
func (f *fakeAWS) ops() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	for i, c := range f.calls {
		out[i] = c.op
	}
	return out
}

func (f *fakeAWS) recorded() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

// handler returns the adapters over f, in us-east-1, with fake credentials and a loopback
// endpoint that is never dialed: f answers in process.
func handler(f *fakeAWS) *setup.Handler {
	return awssetup.New(aws.Config{
		Region:       "us-east-1",
		BaseEndpoint: new("http://127.0.0.1:1"),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDFAKE", SecretAccessKey: "fake", Source: "test"}, nil
		}),
		HTTPClient: f,
	})
}

// compact returns the JSON text s without insignificant spaces.
func compact(t *testing.T, s string) string {
	t.Helper()
	var b bytes.Buffer
	if err := json.Compact(&b, []byte(s)); err != nil {
		t.Fatalf("%v in %s", err, s)
	}
	return b.String()
}

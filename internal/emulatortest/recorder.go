// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package emulatortest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptrace"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Recorder is an aws.HTTPClient wrapper. Each attempt the SDK sends, retries included, is one
// Call, so a retry sent after a cutoff is seen. Recording below the SDK's retry loop is the
// point: a middleware in its Initialize step runs once per operation.
type Recorder struct {
	mu       sync.Mutex
	next     aws.HTTPClient
	calls    []Call
	inFlight int
}

// Call is one HTTP attempt.
type Call struct {
	Target     string    // X-Amz-Target: "DynamoDB_20120810.GetItem", "Firehose_20150804.PutRecordBatch"
	Resources  []string  // TableName, BatchGetItem's RequestItems tables, DeliveryStreamName
	Consistent bool      // ConsistentRead, top level or in KeysAndAttributes
	Condition  string    // ConditionExpression
	Start      time.Time // request written whole by net/http; zero when it never was
	End        time.Time // response read, or the attempt failed
	Deadline   time.Time // the attempt's context deadline; zero without one
	AfterEnd   bool      // the attempt's context had ended when the SDK handed it over
	Status     int       // HTTP status; 0 for a transport error
	ErrorCode  string    // x-amzn-ErrorType or __type, without the namespace
}

// NewRecorder returns a recorder that sends every request through next, or through a client
// with [Config]'s attempt timeout when next is nil.
func NewRecorder(next aws.HTTPClient) *Recorder {
	if next == nil {
		next = timedClient()
	}
	return &Recorder{next: next}
}

// Calls returns the attempts that have ended, in the order they ended.
func (r *Recorder) Calls() []Call {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// InFlight returns the number of attempts sent whose response isn't read yet.
func (r *Recorder) InFlight() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inFlight
}

// Do implements aws.HTTPClient. It reads the request and response bodies whole and passes on
// copies; DynamoDB, Streams and Firehose all send JSON.
func (r *Recorder) Do(req *http.Request) (*http.Response, error) {
	c, req, err := newCall(req)
	if err != nil {
		return nil, err
	}
	// The SDK can hand over an attempt after its context ended, which net/http then
	// refuses, so the attempt counts as sent only once it is written.
	var sent atomic.Pointer[time.Time]
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				sent.Store(new(time.Now()))
			}
		},
	}))
	r.mu.Lock()
	r.inFlight++
	r.mu.Unlock()
	res, err := r.next.Do(req)
	if err == nil {
		res, err = c.read(res)
	}
	c.End = time.Now()
	if at := sent.Load(); at != nil {
		c.Start = *at
	}
	r.mu.Lock()
	r.inFlight--
	r.calls = append(r.calls, c)
	r.mu.Unlock()
	return res, err
}

// CheckStopsAtEnd fails tb unless calls stop as an SDK call must once its context ends. The
// SDK checks the context before each attempt, so it hands over at most one after the end,
// and net/http refuses to write that one. Attempts are told apart by when the SDK handed them
// over, not by the clock: on a busy machine, net/http can write an attempt handed over just
// before the end long after it.
func CheckStopsAtEnd(tb testing.TB, calls []Call) {
	tb.Helper()
	after := 0
	for i, c := range calls {
		if !c.AfterEnd {
			continue
		}
		after++
		if !c.Start.IsZero() {
			tb.Errorf("attempt %d written though its context had ended", i)
		}
	}
	if after > 1 {
		tb.Errorf("%d attempts handed over after the context ended, want at most 1", after)
	}
}

// newCall describes req and returns a copy of it whose body can still be sent.
func newCall(req *http.Request) (Call, *http.Request, error) {
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if cerr := req.Body.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return Call{}, nil, fmt.Errorf("emulatortest: read the request: %w", err)
		}
	}
	var in struct {
		TableName    string `json:"TableName"`
		RequestItems map[string]struct {
			ConsistentRead bool `json:"ConsistentRead"`
		} `json:"RequestItems"`
		ConsistentRead      bool   `json:"ConsistentRead"`
		ConditionExpression string `json:"ConditionExpression"`
		DeliveryStreamName  string `json:"DeliveryStreamName"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return Call{}, nil, fmt.Errorf("emulatortest: the request body isn't JSON: %w", err)
	}
	c := Call{
		Target:     req.Header.Get("X-Amz-Target"),
		Consistent: in.ConsistentRead,
		Condition:  in.ConditionExpression,
	}
	c.Deadline, _ = req.Context().Deadline()
	c.AfterEnd = req.Context().Err() != nil
	for _, name := range []string{in.TableName, in.DeliveryStreamName} {
		if name != "" {
			c.Resources = append(c.Resources, name)
		}
	}
	for _, table := range slices.Sorted(maps.Keys(in.RequestItems)) {
		c.Resources = append(c.Resources, table)
		c.Consistent = c.Consistent || in.RequestItems[table].ConsistentRead
	}
	req = req.Clone(req.Context())
	req.Body = io.NopCloser(bytes.NewReader(body))
	return c, req, nil
}

// read records res and returns a copy of it with its body read whole. A body that fails
// to arrive is a transport error.
func (c *Call) read(res *http.Response) (*http.Response, error) {
	body, err := io.ReadAll(res.Body)
	if cerr := res.Body.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, fmt.Errorf("emulatortest: read the response: %w", err)
	}
	c.Status = res.StatusCode
	if res.StatusCode >= http.StatusMultipleChoices {
		c.ErrorCode = errorCode(res.Header, body)
	}
	res.Body = io.NopCloser(bytes.NewReader(body))
	return res, nil
}

// errorCode returns an error response's code without its namespace:
// "ThrottlingException" from "com.amazonaws.dynamodb.v20120810#ThrottlingException".
func errorCode(h http.Header, body []byte) string {
	if code, _, _ := strings.Cut(h.Get("X-Amzn-Errortype"), ":"); code != "" {
		return code
	}
	var e struct {
		Type string `json:"__type"`
	}
	if json.Unmarshal(body, &e) != nil {
		return ""
	}
	_, code, found := strings.Cut(e.Type, "#")
	if !found {
		return e.Type
	}
	return code
}

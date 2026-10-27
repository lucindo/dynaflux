// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package firehose_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	fh "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/smithy-go"

	"github.com/lucindo/dynaflux/internal/archive/firehose"
	"github.com/lucindo/dynaflux/internal/projector"
)

type putRequest struct {
	DeliveryStreamName string `json:"DeliveryStreamName"`
	Records            []struct {
		Data []byte `json:"Data"`
	} `json:"Records"`
}

// fakeEndpoint is an in-process PutRecordBatch endpoint that answers each call with the
// status and body that answer gives, and keeps the size of every request body.
type fakeEndpoint struct {
	mu    sync.Mutex
	sizes []int
}

// requests returns the size of each request body so far.
func (e *fakeEndpoint) requests() []int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.sizes)
}

// serve starts a fakeEndpoint and returns it with a client of it: loopback only, fake
// credentials.
func serve(t *testing.T, answer func(putRequest) (int, string)) (*fakeEndpoint, *fh.Client) {
	t.Helper()
	e := &fakeEndpoint{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if target := r.Header.Get("X-Amz-Target"); target != "Firehose_20150804.PutRecordBatch" {
			t.Errorf("X-Amz-Target %q", target)
		}
		var req putRequest
		if err = json.Unmarshal(body, &req); err != nil {
			t.Errorf("request %.200s: %v", body, err)
		}
		e.mu.Lock()
		e.sizes = append(e.sizes, len(body))
		e.mu.Unlock()
		status, resp := answer(req)
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.WriteHeader(status)
		if _, err = io.WriteString(w, resp); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(srv.Close)
	return e, fh.New(fh.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "dynafluxtest", SecretAccessKey: "fake", Source: "test"}, nil
		}),
	})
}

// A record is accepted only with a RecordId and no ErrorCode.
func TestSDKCodes(t *testing.T) {
	t.Parallel()
	recs := [][]byte{[]byte(`{"a":1}`), []byte(`{"a":2}`), []byte(`{"a":3}`)}
	_, client := serve(t, func(req putRequest) (int, string) {
		var data []string
		for _, r := range req.Records {
			data = append(data, string(r.Data))
		}
		if req.DeliveryStreamName != deliveryStream || !slices.Equal(data, []string{`{"a":1}`, `{"a":2}`, `{"a":3}`}) {
			t.Errorf("request to %s of %q", req.DeliveryStreamName, data)
		}
		return http.StatusOK, `{"FailedPutCount":2,"Encrypted":false,"RequestResponses":[` +
			`{"RecordId":"r0"},{"ErrorCode":"ServiceUnavailableException","ErrorMessage":"Slow down."},{}]}`
	})
	codes, err := firehose.SDK{Client: client}.PutBatch(t.Context(), deliveryStream, recs)
	if want := []string{"", "ServiceUnavailableException", "NoRecordId"}; err != nil || !slices.Equal(codes, want) {
		t.Errorf("got %q, %v; want %q", codes, err, want)
	}
}

// The writer's count of a request's bytes holds for the SDK's JSON: the records of
// TestArchiveSplitsCalls's 4,000,000-byte case go in one request of 3,999,973 bytes,
// 4,000,000 less the 27 bytes of slack in the count.
func TestSDKRequestSize(t *testing.T) {
	t.Parallel()
	e, client := serve(t, func(req putRequest) (int, string) {
		return http.StatusOK, `{"FailedPutCount":0,"RequestResponses":[` + strings.Repeat(`{"RecordId":"r"},`, len(req.Records)-1) + `{"RecordId":"r"}]}`
	})
	big := strings.Repeat("x", 1_024_000-recordOverhead)
	recs := []projector.Record{
		insert(t, 0, id(0), "us-east-1", big),
		insert(t, 1, id(1), "us-east-1", big),
		insert(t, 2, id(2), "us-east-1", strings.Repeat("x", 951_906-recordOverhead)),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := writer(t, "", firehose.SDK{Client: client}).Archive(ctx, "pv", unit, recs); err != nil {
		t.Fatal(err)
	}
	if got := e.requests(); !slices.Equal(got, []int{3_999_973}) {
		t.Errorf("requests of %v bytes, want one of 3999973", got)
	}
}

// A failed call, such as to a stream that doesn't exist yet, fails the batch at once.
func TestSDKCallError(t *testing.T) {
	t.Parallel()
	e, client := serve(t, func(putRequest) (int, string) {
		return http.StatusBadRequest, `{"__type":"ResourceNotFoundException","message":"Firehose analytics-archive-pv under account 000000000000 not found."}`
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	err := writer(t, "", firehose.SDK{Client: client}).Archive(ctx, "pv", unit, inserts(t, 2))
	var apiErr smithy.APIError
	if n := len(e.requests()); !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ResourceNotFoundException" || n != 1 {
		t.Errorf("got %v after %d requests, want ResourceNotFoundException after 1", err, n)
	}
}

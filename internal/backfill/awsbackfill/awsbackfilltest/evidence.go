// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfilltest

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
)

// Firehose answers DescribeDeliveryStream: ACTIVE for each delivery stream it holds, not
// found for any other. It is safe for concurrent use.
type Firehose struct {
	mu      sync.Mutex
	streams map[string]bool
}

// NewFirehose returns a Firehose that holds the delivery streams names.
func NewFirehose(names ...string) *Firehose {
	f := &Firehose{streams: map[string]bool{}}
	for _, n := range names {
		f.streams[n] = true
	}
	return f
}

// ServeHTTP implements [http.Handler].
func (f *Firehose) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var in struct {
		DeliveryStreamName string `json:"DeliveryStreamName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || !strings.HasSuffix(r.Header.Get("X-Amz-Target"), ".DescribeDeliveryStream") {
		http.Error(w, "the fake serves DescribeDeliveryStream only", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	found := f.streams[in.DeliveryStreamName]
	f.mu.Unlock()
	if !found {
		athenaError(w, "ResourceNotFoundException", "Firehose "+in.DeliveryStreamName+" under account 123456789012 not found.")
		return
	}
	writeJSON(w, map[string]any{"DeliveryStreamDescription": map[string]any{
		"DeliveryStreamName": in.DeliveryStreamName, "DeliveryStreamStatus": "ACTIVE", "DeliveryStreamType": "DirectPut",
		"DeliveryStreamARN": "arn:aws:firehose:us-east-1:123456789012:deliverystream/" + in.DeliveryStreamName,
		"VersionId":         "1", "Destinations": []any{}, "HasMoreDestinations": false,
	}})
}

// Mapping is an event source mapping that Lambda lists.
type Mapping struct {
	UUID, FunctionArn, EventSourceArn, State string
}

// Lambda answers ListEventSourceMappings with the mappings of the request's EventSourceArn,
// one page. It is safe for concurrent use.
type Lambda struct {
	mu       sync.Mutex
	mappings []Mapping
}

// NewLambda returns a Lambda with mappings.
func NewLambda(mappings ...Mapping) *Lambda {
	return &Lambda{mappings: mappings}
}

// ServeHTTP implements [http.Handler].
func (l *Lambda) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || !strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/event-source-mappings") {
		http.Error(w, "the fake serves ListEventSourceMappings only", http.StatusBadRequest)
		return
	}
	source := r.URL.Query().Get("EventSourceArn")
	l.mu.Lock()
	defer l.mu.Unlock()
	listed := []Mapping{}
	for _, m := range l.mappings {
		if m.EventSourceArn == source {
			listed = append(listed, m)
		}
	}
	data, err := json.Marshal(map[string]any{"EventSourceMappings": listed})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data) //nolint:errcheck,gosec // the client sees a short body
}

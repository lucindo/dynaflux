// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynamo_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/lucindo/dynaflux/internal/store"
)

// scanEndpoint answers each Scan with the next of its responses and keeps the requests.
type scanEndpoint struct {
	responses []string
	sent      []map[string]any
}

func (e *scanEndpoint) answer(op string, body []byte) (int, string) {
	var in map[string]any
	if op != "Scan" || json.Unmarshal(body, &in) != nil {
		return 400, errorBody("ValidationException")
	}
	e.sent = append(e.sent, in)
	res := e.responses[0]
	e.responses = e.responses[1:]
	return 200, res
}

func avKey(pk, sk string) map[string]any {
	return map[string]any{"pk": map[string]any{"S": pk}, "sk": map[string]any{"S": sk}}
}

func TestScanPageRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		s        store.Scan
		start    *store.Key
		response string
		wantSent map[string]any
		want     []store.Item
		wantNext *store.Key
	}{
		{
			name:     "first page of the whole table",
			s:        store.Scan{Segments: 1, Consistent: true, PageSize: 2},
			response: `{"Items":[{"pk":{"S":"p"},"sk":{"S":"a"}}],"LastEvaluatedKey":{"pk":{"S":"p"},"sk":{"S":"a"}}}`,
			wantSent: map[string]any{"TableName": "t", "ConsistentRead": true, "Limit": 2.0},
			want:     []store.Item{{"pk": str("p"), "sk": str("a")}},
			wantNext: &store.Key{PK: "p", SK: "a"},
		},
		{
			name:     "last page of a segment",
			s:        store.Scan{Segment: 3, Segments: 8},
			start:    &store.Key{PK: "p", SK: "a"},
			response: `{"Items":[]}`,
			wantSent: map[string]any{
				"TableName": "t", "ConsistentRead": false, "Segment": 3.0, "TotalSegments": 8.0,
				"ExclusiveStartKey": avKey("p", "a"),
			},
			want: []store.Item{},
		},
	}
	for _, tt := range tests {
		e := &scanEndpoint{responses: []string{tt.response}}
		got, next, err := newBackend(fake(e.answer)).ScanPage(t.Context(), "t", tt.s, tt.start)
		if err != nil {
			t.Fatalf("%s: ScanPage: %v", tt.name, err)
		}
		if !reflect.DeepEqual(e.sent, []map[string]any{tt.wantSent}) {
			t.Errorf("%s: sent %v, want %v", tt.name, e.sent, tt.wantSent)
		}
		if !reflect.DeepEqual(got, tt.want) || !reflect.DeepEqual(next, tt.wantNext) {
			t.Errorf("%s: ScanPage = %v, next %v; want %v, next %v", tt.name, got, next, tt.want, tt.wantNext)
		}
	}
}

// A LastEvaluatedKey without a string pk and sk would resume the scan somewhere else.
func TestScanPageForeignKey(t *testing.T) {
	t.Parallel()
	e := &scanEndpoint{responses: []string{`{"Items":[],"LastEvaluatedKey":{"id":{"N":"1"}}}`}}
	_, _, err := newBackend(fake(e.answer)).ScanPage(t.Context(), "t", store.Scan{Segments: 1}, nil)
	if want := "dynamo: scan t: the response's LastEvaluatedKey is not a pk and sk"; err == nil || err.Error() != want {
		t.Errorf("ScanPage = %v, want %q", err, want)
	}
}

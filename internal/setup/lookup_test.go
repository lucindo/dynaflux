// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package setup_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/setup"
)

const (
	table     = "prod-analytics-stream-page"
	streamARN = "arn:aws:dynamodb:us-east-1:123456789012:table/prod-analytics-stream-page/stream/2026-10-26T12:00:00.000"
)

// fakeDynamoDB holds table, or no table when it is nil, and refuses its first denied calls.
type fakeDynamoDB struct {
	table  *setup.StreamTable
	denied int
	calls  int
}

func (f *fakeDynamoDB) DescribeTable(_ context.Context, name string) (setup.StreamTable, bool, error) {
	f.calls++
	if f.calls <= f.denied {
		return setup.StreamTable{}, false, fmt.Errorf("%w: not authorized to perform dynamodb:DescribeTable", setup.ErrAccessDenied)
	}
	if f.table == nil || name != table {
		return setup.StreamTable{}, false, nil
	}
	return *f.table, true, nil
}

func lookup(action string) setup.Request {
	return setup.Request{
		Type: setup.TypeStreamARN, Action: action,
		Props: map[string]any{"ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:prod-analytics-setup", "TableName": table},
	}
}

func readyTable(change func(*setup.StreamTable)) *setup.StreamTable {
	t := &setup.StreamTable{Status: "ACTIVE", StreamARN: streamARN, StreamView: "NEW_AND_OLD_IMAGES"}
	change(t)
	return t
}

func TestLookup(t *testing.T) {
	t.Parallel()
	const redeploy = ": wait until the deploy of global.yaml has finished, then deploy again"
	tests := []struct {
		name  string
		table *setup.StreamTable
		want  string // the error, or "" for success
	}{
		{"active", readyTable(func(*setup.StreamTable) {}), ""},
		{"updating", readyTable(func(t *setup.StreamTable) { t.Status, t.ReplicaStatus = "UPDATING", "ACTIVE" }), ""},
		{
			"missing", nil,
			"stream table prod-analytics-stream-page doesn't exist in this region: deploy global.yaml first, with the same Prefix",
		},
		{
			"creating", readyTable(func(t *setup.StreamTable) { t.Status = "CREATING" }),
			"stream table prod-analytics-stream-page is CREATING" + redeploy,
		},
		{
			"replica creating", readyTable(func(t *setup.StreamTable) { t.ReplicaStatus = "CREATING" }),
			"the replica of stream table prod-analytics-stream-page in this region is CREATING" + redeploy,
		},
		{
			"no stream", readyTable(func(t *setup.StreamTable) { t.StreamARN, t.StreamView = "", "" }),
			"stream table prod-analytics-stream-page has no stream: deploy the global.yaml that Dynaflux generates",
		},
		{
			"keys only", readyTable(func(t *setup.StreamTable) { t.StreamView = "KEYS_ONLY" }),
			"the stream of stream table prod-analytics-stream-page is KEYS_ONLY, not NEW_AND_OLD_IMAGES: " +
				"deploy the global.yaml that Dynaflux generates",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := setup.Handler{DynamoDB: &fakeDynamoDB{table: tt.table}}
			resp, err := h.Handle(t.Context(), lookup(setup.ActionCreate))
			if tt.want != "" {
				if err == nil || err.Error() != tt.want {
					t.Errorf("got %v\nwant %s", err, tt.want)
				}
				return
			}
			want := setup.Response{PhysicalID: streamARN, Data: map[string]string{"StreamArn": streamARN}}
			if err != nil || !reflect.DeepEqual(resp, want) {
				t.Errorf("got %+v, %v; want %+v", resp, err, want)
			}
		})
	}
}

// A new setup role's grants take a while to apply, so a lookup retries for about a minute.
func TestLookupRetriesAccessDenied(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		action string
		denied int
		ok     bool
		// wait is how long a failing lookup tries.
		wait time.Duration
	}{
		{"create denied briefly", setup.ActionCreate, 3, true, 0},
		{"update denied briefly", setup.ActionUpdate, 3, true, 0},
		{"create denied for good", setup.ActionCreate, 1 << 20, false, time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				h := setup.Handler{DynamoDB: &fakeDynamoDB{table: readyTable(func(*setup.StreamTable) {}), denied: tt.denied}}
				start := time.Now()
				resp, err := h.Handle(t.Context(), lookup(tt.action))
				waited := time.Since(start)
				switch {
				case tt.ok:
					if err != nil || resp.PhysicalID != streamARN {
						t.Errorf("got %+v, %v; want stream ARN", resp, err)
					}
				case !errors.Is(err, setup.ErrAccessDenied):
					t.Errorf("got %v, want access denied", err)
				case waited > tt.wait || waited < tt.wait*9/10:
					t.Errorf("gave up after %v, want about %v", waited, tt.wait)
				}
			})
		})
	}
}

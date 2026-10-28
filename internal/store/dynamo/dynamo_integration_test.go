// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package dynamo_test

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	cfg := emulatortest.Config(t, nil)
	storetest.Run(t, func(t *testing.T) store.Backend {
		t.Helper()
		return emulatortest.Backend(t, cfg)
	})
}

// lossy loses the response to the next request for operation (".PutItem") once armed: the
// write lands and the SDK sees a connection error, as when a response is lost on the network.
type lossy struct {
	next      aws.HTTPClient
	operation string
	armed     atomic.Bool
}

func (l *lossy) Do(r *http.Request) (*http.Response, error) {
	res, err := l.next.Do(r)
	if err == nil && strings.HasSuffix(r.Header.Get("X-Amz-Target"), l.operation) && l.armed.CompareAndSwap(true, false) {
		res.Body.Close()
		return nil, errors.New("read: connection reset by peer")
	}
	return res, err
}

// The SDK retries a conditional write whose response was lost; the retry fails its
// condition, and ALL_OLD hands the caller its own write, which every caller takes as
// success (storetest.RetriedWrite is this case on any backend).
func TestRetriedWriteThroughSDK(t *testing.T) {
	t.Parallel()
	item := func(v string) store.Item {
		return store.Item{"pk": str("p"), "sk": str("a"), "_v": {Type: store.N, Str: v}, "n": {Type: store.N, Str: "1.50"}}
	}
	tests := []struct {
		name      string
		before    store.Item
		c         store.Cond
		put       store.Item
		condition string
	}{
		{"IfAbsent", nil, store.IfAbsent(), item("1"), "attribute_not_exists(#pk)"},
		{"IfVersion", item("1"), store.IfVersion(1), item("2"), "#v = :v"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l := &lossy{next: awshttp.NewBuildableClient().WithTimeout(2 * time.Second), operation: ".PutItem"}
			rec := emulatortest.NewRecorder(l)
			b := emulatortest.Backend(t, emulatortest.Config(t, rec))
			if tt.before != nil {
				if err := b.Put(t.Context(), "t", tt.before, store.Cond{}); err != nil {
					t.Fatal(err)
				}
			}
			before := len(rec.Calls())
			l.armed.Store(true)
			err := b.Put(t.Context(), "t", tt.put, tt.c)

			ce, ok := errors.AsType[*store.ConditionError](err)
			if !ok {
				t.Fatalf("Put = %v, want a *store.ConditionError", err)
			}
			want := tt.put
			want["n"] = store.Value{Type: store.N, Str: "1.5"} // as stored
			if !reflect.DeepEqual(ce.Current, want) {
				t.Errorf("Current = %v, want the caller's own write %v", ce.Current, want)
			}
			var attempts []string
			for _, c := range rec.Calls()[before:] {
				attempts = append(attempts, fmt.Sprintf("%s if %s: %d %s", c.Target, c.Condition, c.Status, c.ErrorCode))
			}
			put := "DynamoDB_20120810.PutItem if " + tt.condition + ": "
			if want := []string{
				put + "0 ", // the lost response: a transport error
				put + "400 ConditionalCheckFailedException",
			}; !reflect.DeepEqual(attempts, want) {
				t.Errorf("attempts = %q, want %q", attempts, want)
			}
		})
	}
}

// A transaction whose response was lost is sent again with its token, which DynamoDB answers
// with the first send's success without applying it twice; a resend with another token would
// fail every condition over the transaction's own items (DynamoDB Local 3.3.2 experiment E9
// of the M6 research).
func TestTransactLostResponseThroughSDK(t *testing.T) {
	t.Parallel()
	l := &lossy{next: awshttp.NewBuildableClient().WithTimeout(2 * time.Second), operation: ".TransactWriteItems"}
	rec := emulatortest.NewRecorder(l)
	b := emulatortest.Backend(t, emulatortest.Config(t, rec))
	row := func(v string) store.Item {
		return store.Item{"pk": str("p"), "sk": str("row"), "_v": {Type: store.N, Str: v}}
	}
	event := store.Item{"pk": str("p"), "sk": str("event")}
	if err := b.Put(t.Context(), "t", row("1"), store.IfAbsent()); err != nil {
		t.Fatal(err)
	}
	before := len(rec.Calls())
	l.armed.Store(true)
	err := b.Transact(t.Context(), []store.TxPut{
		{Table: "t", Item: event, Cond: store.IfAbsent()},
		{Table: "t", Item: row("2"), Cond: store.IfVersion(1)},
	})
	if err != nil {
		t.Fatalf("Transact = %v, want success", err)
	}
	var attempts []string
	for _, c := range rec.Calls()[before:] {
		attempts = append(attempts, fmt.Sprintf("%s: %d %s", c.Target, c.Status, c.ErrorCode))
	}
	tx := "DynamoDB_20120810.TransactWriteItems: "
	if want := []string{tx + "0 ", tx + "200 "}; !reflect.DeepEqual(attempts, want) {
		t.Errorf("attempts = %q, want %q", attempts, want)
	}
	for _, want := range []store.Item{event, row("2")} {
		got, gerr := b.Get(t.Context(), "t", store.Key{PK: "p", SK: want["sk"].Str}, true)
		if gerr != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("Get = %v, %v; want %v", got, gerr, want)
		}
	}
}

// What DynamoDB Local does with requests that store's checks stop before sending, straight
// through the SDK: the limits are DynamoDB's, and they count bytes.
func TestEndpointLimits(t *testing.T) {
	t.Parallel()
	cfg := emulatortest.Config(t, nil)
	table := emulatortest.Table(t, cfg)
	c := dynamodb.NewFromConfig(cfg)
	s := func(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
	n := func(v string) types.AttributeValue { return &types.AttributeValueMemberN{Value: v} }
	item := func(pk, sk string, attrs ...types.AttributeValue) map[string]types.AttributeValue {
		it := map[string]types.AttributeValue{"pk": s(pk), "sk": s(sk)}
		for _, a := range attrs {
			it["x"] = a
		}
		return it
	}
	// An item of exactly size bytes as store.ItemSize counts it: pk "p", sk "a" and x.
	sized := func(size int) map[string]types.AttributeValue {
		return item("p", "a", s(strings.Repeat("x", size-len("pkpska"+"x"))))
	}
	const valid = "ValidationException"
	tests := []struct {
		name string
		item map[string]types.AttributeValue
		want string // the error code; empty for success
	}{
		{"pk of 2048 bytes in 1024 runes", item(strings.Repeat("é", 1024), "a"), ""},
		{"pk of 2049 bytes", item(strings.Repeat("é", 1024)+"k", "a"), valid},
		{"sk of 1024 bytes in 512 runes", item("p", strings.Repeat("é", 512)), ""},
		{"sk of 1025 bytes", item("p", strings.Repeat("é", 512)+"k"), valid},
		{"item at the limit", sized(store.MaxItemSize), ""},
		{"item over the limit", sized(store.MaxItemSize + 1), valid},
		{"38 digits", item("p", "a", n(strings.Repeat("9", 38))), ""},
		{"39 digits", item("p", "a", n(strings.Repeat("9", 39))), valid},
		{"smallest magnitude", item("p", "a", n("1E-130")), ""},
		{"below the smallest magnitude", item("p", "a", n("1E-131")), valid},
		{"largest magnitude", item("p", "a", n("9."+strings.Repeat("9", 37)+"E+125")), ""},
		{"over the largest magnitude", item("p", "a", n("1E+126")), valid},
	}
	for _, tt := range tests {
		_, err := c.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: &table, Item: tt.item})
		got := ""
		if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
			got = apiErr.ErrorCode()
		} else if err != nil {
			got = err.Error()
		}
		if got != tt.want {
			t.Errorf("%s: PutItem error %q, want %q", tt.name, got, tt.want)
		}
	}
}

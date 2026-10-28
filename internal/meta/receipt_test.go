// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta_test

import (
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

const eventID = "01J9ZQ3ZQ8V7W6X5Y4Z3A2B1C0"

// A receipt's layout is part of the storage format: its key names the stored event's
// ingest_time too, and it expires 25 hours after it was written, rounded up to the second.
func TestReceiptLayout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := mem.New()
		now := bubbleStart.Add(1500 * time.Millisecond)
		if err := meta.New(b, table).PutReceipt(t.Context(), stream, meta.ReceiptKey{EventID: eventID, It: 946684800000001}, now); err != nil {
			t.Fatal(err)
		}
		wantItem(t, b, store.Item{
			"pk":   s("rcpt#page_views#" + eventID + "#946684800000001"),
			"sk":   s("!"),
			"_ttl": n("946774802"), // 2000-01-02T01:00:02Z
		})
	})
}

// Receipts reads consistently, in chunks of 100, and in the order asked: an event appended
// again after its item expired has a receipt of its own.
func TestReceipts(t *testing.T) {
	t.Parallel()
	for _, be := range backends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			m := meta.New(be.new(t), table)
			var ks []meta.ReceiptKey
			for i := range 150 {
				ks = append(ks, meta.ReceiptKey{EventID: "e" + strconv.Itoa(i), It: 7})
			}
			want := make([]bool, len(ks)+1)
			for _, i := range []int{0, 99, 100, 149} {
				want[i] = true
				if err := m.PutReceipt(t.Context(), stream, ks[i], bubbleStart); err != nil {
					t.Fatal(err)
				}
			}
			again := meta.ReceiptKey{EventID: "e0", It: 8}
			got, err := m.Receipts(t.Context(), stream, append(ks, again))
			if err != nil || !slices.Equal(got, want) {
				t.Errorf("Receipts = %v, %v; want only 0, 99, 100 and 149 present", got, err)
			}
		})
	}
}

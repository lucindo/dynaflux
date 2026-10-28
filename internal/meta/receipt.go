// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package meta

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

const (
	receiptPrefix = "rcpt#"
	// receiptLifetime outlives every change record, which DynamoDB keeps for 24 hours, so a
	// missing receipt always means "not yet".
	receiptLifetime = 25 * time.Hour
)

// ReceiptKey names the receipt of one stored event (encoding.md §8.6).
type ReceiptKey struct {
	EventID string
	// It is the stored event's ingest_time (_it) in microseconds. It tells apart the records
	// of a DEDUP BY event appended again after its item expired.
	It int64
}

func receiptItemKey(stream string, k ReceiptKey) store.Key {
	return store.Key{PK: receiptPrefix + stream + "#" + k.EventID + "#" + strconv.FormatInt(k.It, 10), SK: keys.NoSortKey}
}

// PutReceipt writes the receipt of k, an event of stream, without a condition: writing it
// again changes nothing but its expiry, 25 hours after now.
func (m *Meta) PutReceipt(ctx context.Context, stream string, k ReceiptKey, now time.Time) error {
	key := receiptItemKey(stream, k)
	expiry := now.Add(receiptLifetime)
	secs := expiry.Unix()
	if expiry.Nanosecond() > 0 {
		secs++
	}
	it := store.Item{catalog.AttrPK: str(key.PK), catalog.AttrSK: str(key.SK), catalog.AttrTTL: num(secs)}
	if err := m.b.Put(ctx, m.table, it, store.Cond{}); err != nil {
		return fmt.Errorf("meta: write the receipt of event %s of %s: %w", k.EventID, stream, err)
	}
	return nil
}

// Receipts reports, in the order of ks, which receipts of stream's events are stored. It
// reads consistently, so it sees every receipt written before it.
func (m *Meta) Receipts(ctx context.Context, stream string, ks []ReceiptKey) ([]bool, error) {
	out := make([]bool, 0, len(ks))
	for chunk := range slices.Chunk(ks, store.MaxBatchGetKeys) {
		items := make([]store.Key, len(chunk))
		for i, k := range chunk {
			items[i] = receiptItemKey(stream, k)
		}
		its, err := m.b.BatchGet(ctx, m.table, items, true)
		if err != nil {
			return nil, fmt.Errorf("meta: read receipts of %s: %w", stream, err)
		}
		for _, it := range its {
			out = append(out, it != nil)
		}
	}
	return out, nil
}

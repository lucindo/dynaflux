// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// waitable returns it as an append in region r that asked for a receipt stores it.
func waitable(it store.Item, r string) store.Item {
	out := maps.Clone(it)
	out[catalog.AttrOriginRegion] = store.Value{Type: store.S, Str: r}
	out[catalog.AttrReceipt] = store.Value{Type: store.BOOL, Bool: true}
	return out
}

func receiptKey(t *testing.T, it store.Item) meta.ReceiptKey {
	t.Helper()
	_, id, err := keys.ParseStreamSK(it[catalog.AttrSK].Str)
	if err != nil {
		t.Fatal(err)
	}
	at, err := strconv.ParseInt(it[catalog.AttrIngestTime].Str, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return meta.ReceiptKey{EventID: id, It: at}
}

// receipts returns the indexes of the items in its whose receipts are stored.
func (f *fixture) receipts(t *testing.T, its ...store.Item) []int {
	t.Helper()
	ks := make([]meta.ReceiptKey, len(its))
	for i, it := range its {
		ks[i] = receiptKey(t, it)
	}
	got, err := f.meta.Receipts(t.Context(), "clicks", ks)
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for i, ok := range got {
		if ok {
			out = append(out, i)
		}
	}
	return out
}

// Only an INSERT that its append in the core's region asked a receipt for gets one, keyed by
// its ingest_time too, and only below the lowest failed record.
func TestHandleWritesReceiptsBelowTheFailedRecord(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		fail := &failOnce{table: pageTotalsTable, pk: "/b"}
		f := newFixture(t, func(b store.Backend) store.Backend { fail.Backend = b; return fail })
		mine := waitable(f.click(t, "u1", "/a", 100), region)
		foreign := waitable(f.click(t, "u2", "/a", 100), "eu-west-1")
		plain := f.click(t, "u3", "/a", 100)
		failed := waitable(f.click(t, "u1", "/b", 100), region)
		after := waitable(f.click(t, "u2", "/c", 100), region)
		res := f.handle(t, insert(1, mine), insert(2, foreign), insert(3, plain), insert(4, failed), insert(5, after))
		got := f.receipts(t, mine, foreign, plain, failed, after)
		if res.FailedSeq != seq(4) || !slices.Equal(got, []int{0}) {
			t.Fatalf("FailedSeq %s, receipts of records %v; want %s and only the first record's", res.FailedSeq, got, seq(4))
		}
		res = f.handle(t, insert(4, failed), insert(5, after))
		later := maps.Clone(mine)
		later[catalog.AttrIngestTime] = store.Value{Type: store.N, Str: "1"}
		if got = f.receipts(t, mine, foreign, plain, failed, after, later); res.FailedSeq != "" || !slices.Equal(got, []int{0, 3, 4}) {
			t.Errorf("after the redelivery: FailedSeq %q, receipts of records %v; want none failed, receipts of 0, 3 and 4", res.FailedSeq, got)
		}
	})
}

// failReceipts fails the first write of the receipt of each event_id it holds.
type failReceipts struct {
	store.Backend

	mu  sync.Mutex
	ids map[string]bool
}

var errReceipt = errors.New("receipt write throttled")

func (f *failReceipts) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	f.mu.Lock()
	var hit bool
	for id := range f.ids {
		if strings.HasPrefix(it[catalog.AttrPK].Str, "rcpt#clicks#"+id+"#") {
			hit = true
			delete(f.ids, id)
		}
	}
	f.mu.Unlock()
	if hit {
		return errReceipt
	}
	return f.Backend.Put(ctx, table, it, c)
}

// A receipt that fails to write fails its record, whose rows and progress are written by
// then; of two, the lower. The redelivery finds them at or below progress, applies nothing
// again and writes the receipts.
func TestHandleFailsTheRecordOfAFailedReceipt(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		fail := &failReceipts{ids: map[string]bool{}}
		f := newFixture(t, func(b store.Backend) store.Backend { fail.Backend = b; return fail })
		a, b, c := waitable(f.click(t, "u1", "/a", 100), region), waitable(f.click(t, "u2", "/a", 200), region), waitable(f.click(t, "u3", "/a", 300), region)
		fail.ids[receiptKey(t, b).EventID] = true
		fail.ids[receiptKey(t, c).EventID] = true
		res := f.handle(t, insert(1, a), insert(2, b), insert(3, c))
		if res.FailedSeq != seq(2) || !errors.Is(res.RowErr, errReceipt) || f.unit(t).Progress != padSeq(t, seq(3)) {
			t.Fatalf("FailedSeq %s (%v), progress %s; want %s with the receipt's error, progress at the last record",
				res.FailedSeq, res.RowErr, f.unit(t).Progress, seq(2))
		}
		res = f.handle(t, insert(2, b), insert(3, c))
		got, receipts := f.row(t, pageTotals, page("/a")), f.receipts(t, a, b, c)
		if want := "/a 300 6.00 100 600 3 3"; res.FailedSeq != "" || got != want || !slices.Equal(receipts, []int{0, 1, 2}) {
			t.Errorf("FailedSeq %q, %s /a %q, receipts of %v; want none failed, %q, every receipt", res.FailedSeq, pageTotals, got, receipts, want)
		}
	})
}

// A fence written after the batch read its unit, for a version the batch read as pending,
// fails the batch at its progress write: the version needs the record from the projector.
// The batch writes no receipt, since one would end a wait before the version holds the
// record; the redelivery applies it and writes the receipt.
func TestHandleWritesNoReceiptWhenAFenceStopsProgress(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		hook := &afterUnitRead{}
		f := newPendingFixture(t, func(b store.Backend) store.Backend { hook.Backend = b; return hook })
		hook.do = func(context.Context) {
			f.activate(t, pageTotals)
			if _, err := f.meta.EnsureFence(t.Context(), f.unit(t), pageTotals); err != nil {
				t.Error(err)
			}
		}
		click := waitable(f.click(t, "u1", "/a", 100), region)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		_, err := f.core.Handle(ctx, projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{insert(1, click)}})
		if !errors.Is(err, meta.ErrFenced) || len(f.receipts(t, click)) != 0 {
			t.Fatalf("Handle: %v, receipts %v; want ErrFenced and no receipt", err, f.receipts(t, click))
		}
		f.handle(t, insert(1, click))
		if got, want := f.row(t, pageTotals, page("/a")), "/a 100 1.00 100 100 1 1"; got != want || len(f.receipts(t, click)) != 1 {
			t.Errorf("%s /a is %q with receipts %v; want %q and the receipt", pageTotals, got, f.receipts(t, click), want)
		}
	})
}

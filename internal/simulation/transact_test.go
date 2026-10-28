// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
)

func named(name string) store.Item {
	return store.Item{catalog.AttrPK: str(name), catalog.AttrSK: str("!")}
}

func txPut(name string, c store.Cond) store.TxPut {
	return store.TxPut{Table: viewTable, Item: named(name), Cond: c}
}

// conds returns the PK of each Current of a transaction's error, "-" where no condition
// failed and "absent" for an absent item; nil when err isn't a *store.TxError.
func conds(err error) []string {
	txErr, ok := errors.AsType[*store.TxError](err)
	if !ok {
		return nil
	}
	out := make([]string, len(txErr.Conds))
	for i, c := range txErr.Conds {
		switch {
		case c == nil:
			out[i] = "-"
		case c.Current == nil:
			out[i] = "absent"
		default:
			out[i] = c.Current[catalog.AttrPK].Str
		}
	}
	return out
}

// A failed condition writes none of the puts, and the error names the stored item.
func TestTransactGateIsAtomic(t *testing.T) {
	t.Parallel()
	var got []string
	sys := newScripted(t, func(ctx context.Context, b store.Backend, n int64) error {
		if n > 1 {
			return nil
		}
		if err := put(ctx, b, "before"); err != nil {
			return err
		}
		got = conds(b.Transact(ctx, []store.TxPut{txPut("at", store.IfAbsent()), txPut("before", store.IfAbsent())}))
		return nil
	})
	out := simulation.Run(t, oneEvent(simulation.Faults{}), sys, 1)
	if len(out.Violations) > 0 {
		t.Fatalf("violations: %v", out.Violations)
	}
	if want := []string{"-", "before"}; !reflect.DeepEqual(got, want) || !reflect.DeepEqual(sys.stored, []string{"before"}) {
		t.Errorf("Conds %v, stored %v; want %v, [before]", got, sys.stored, want)
	}
	if out.Stats.Transactions != 1 {
		t.Errorf("%d transactions, want 1", out.Stats.Transactions)
	}
}

// Retried: the transaction applies, and the resend without its token fails every condition
// over the items the first send stored.
func TestTransactGateRetried(t *testing.T) {
	t.Parallel()
	var got []string
	sys := newScripted(t, func(ctx context.Context, b store.Backend, n int64) error {
		if n == 1 {
			got = conds(b.Transact(ctx, []store.TxPut{
				txPut("before", store.IfAbsent()),
				txPut("at", store.Cond{}),
				txPut("late", store.IfAbsent()),
			}))
		}
		return nil
	})
	out := simulation.Run(t, oneEvent(simulation.Faults{Retried: 1}), sys, 1)
	if len(out.Violations) > 0 {
		t.Fatalf("violations: %v", out.Violations)
	}
	if want := []string{"before", "-", "late"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Conds %v, want %v", got, want)
	}
	if want := []string{"before", "at", "late"}; !reflect.DeepEqual(sys.stored, want) {
		t.Errorf("stored %v, want %v", sys.stored, want)
	}
}

// InProgress: until the transaction commits, its item reads as it was and a write to it
// conflicts; the caller then gets the commit's outcome.
func TestTransactGateInProgress(t *testing.T) {
	t.Parallel()
	var (
		txErr, putErr, otherTxErr error
		readMeanwhile             store.Item
	)
	sys := newScripted(t, func(ctx context.Context, b store.Backend, n int64) error {
		if n > 1 {
			return nil
		}
		var wg sync.WaitGroup
		wg.Go(func() { txErr = b.Transact(ctx, []store.TxPut{txPut("at", store.IfAbsent())}) })
		wg.Go(func() {
			time.Sleep(time.Microsecond)
			var err error
			if readMeanwhile, err = b.Get(ctx, viewTable, store.Key{PK: "at", SK: "!"}, true); err != nil {
				putErr = err
				return
			}
			putErr = put(ctx, b, "at")
			otherTxErr = b.Transact(ctx, []store.TxPut{txPut("before", store.IfAbsent()), txPut("at", store.Cond{})})
		})
		wg.Wait()
		return nil
	})
	out := simulation.Run(t, oneEvent(simulation.Faults{InProgress: 1, HoldMax: time.Second}), sys, 1)
	if len(out.Violations) > 0 {
		t.Fatalf("violations: %v", out.Violations)
	}
	if txErr != nil || readMeanwhile != nil || !errors.Is(putErr, store.ErrConflict) || !errors.Is(otherTxErr, store.ErrConflict) {
		t.Errorf("Transact = %v, read meanwhile %v, Put = %v, another Transact = %v; want success, nothing, ErrConflict, ErrConflict",
			txErr, readMeanwhile, putErr, otherTxErr)
	}
	if !reflect.DeepEqual(sys.stored, []string{"at"}) || out.Stats.InProgress != 1 || out.Stats.Conflicts != 2 {
		t.Errorf("stored %v, %d in progress, %d conflicts; want [at], 1, 2", sys.stored, out.Stats.InProgress, out.Stats.Conflicts)
	}
}

// A core whose deadline comes before the commit dies there, and the transaction still lands.
func TestTransactGateCommitsAfterItsCallerDied(t *testing.T) {
	t.Parallel()
	sys := newScripted(t, func(ctx context.Context, b store.Backend, n int64) error {
		if n > 1 {
			return nil
		}
		return b.Transact(ctx, []store.TxPut{txPut("late", store.IfAbsent())})
	})
	out := simulation.Run(t, oneEvent(simulation.Faults{InProgress: 1, HoldMax: time.Hour}), sys, 1)
	if len(out.Violations) > 0 {
		t.Fatalf("violations: %v", out.Violations)
	}
	if !reflect.DeepEqual(sys.stored, []string{"late"}) || out.Stats.Timeouts != 1 {
		t.Errorf("stored %v after %d timeouts; want [late] after 1", sys.stored, out.Stats.Timeouts)
	}
}

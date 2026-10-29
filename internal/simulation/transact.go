// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// Transact enters once for the whole transaction. A request the store refuses before
// sending never enters, as with DynamoDB.
func (g gated) Transact(ctx context.Context, puts []store.TxPut) error {
	if err := store.CheckTransact(puts); err != nil {
		return fmt.Errorf("simulation: transact: %w", err)
	}
	rg := g.rg
	op := []string{"transact"}
	for _, p := range puts {
		op = append(op, p.Table, keyText(p.Item))
	}
	text := strings.Join(op, " ")
	lat, f := rg.enter(ctx, kindTransact, text)
	rg.run.count(&rg.run.stats.Transactions)
	switch f {
	case faultTransient:
		time.Sleep(lat)
		return errTransient
	case faultConflict:
		time.Sleep(lat)
		return errConflict
	case faultInProgress:
		return rg.inProgress(ctx, puts, text, lat)
	case faultNone, faultAmbiguous, faultRetried, faultLate, faultAppliedThrottled, faultAppliedConflict:
	}
	rg.skewPuts(ctx, puts)
	err := rg.transact(ctx, puts)
	if err == nil && f == faultRetried {
		err = rg.resent(ctx, puts)
	}
	time.Sleep(lat)
	if err == nil {
		err = appliedError(f)
	}
	return err
}

func (rg *region) skewPuts(ctx context.Context, puts []store.TxPut) {
	for _, p := range puts {
		if rg.streams[p.Table] != nil {
			rg.skew(ctx, p.Item)
		}
	}
}

// prepare checks puts as DynamoDB does before a transaction writes: errHeld when a
// transaction in progress holds one of the items, else a *store.TxError holding the items
// whose conditions fail. A writer's put 0 sets taken when it finds another call's event:
// one with another origin_region or ingest_time than the event it puts, which the call
// builds once. Only gated code calls it.
func (rg *region) prepare(ctx context.Context, puts []store.TxPut) error {
	var failed *store.TxError
	for i, p := range puts {
		k := tableKey{p.Table, store.KeyOf(p.Item)}
		if rg.held[k] {
			rg.run.count(&rg.run.stats.Conflicts)
			return errHeld
		}
		cur, err := rg.mem.Get(ctx, p.Table, k.key, true)
		if err != nil {
			return err
		}
		if p.Cond.Holds(cur) {
			continue
		}
		if failed == nil {
			failed = &store.TxError{Conds: make([]*store.ConditionError, len(puts))}
		}
		failed.Conds[i] = &store.ConditionError{Current: cur}
	}
	if rg.streams[puts[0].Table] != nil {
		found := failed != nil && failed.Conds[0] != nil
		invocationOf(ctx).taken = found && !sameCall(failed.Conds[0].Current, puts[0].Item)
	}
	if failed != nil {
		return failed
	}
	return nil
}

// sameCall reports whether stored is sent, an event that an append puts: the same
// origin_region and ingest_time, which the engine sets once per call.
func sameCall(stored, sent store.Item) bool {
	return stored[catalog.AttrOriginRegion].Str == sent[catalog.AttrOriginRegion].Str &&
		stored[catalog.AttrIngestTime].Str == sent[catalog.AttrIngestTime].Str
}

// transact applies puts as one transaction: no other gated code runs between its checks
// and its writes, and each put is applied as a gated Put is, with its records and checks,
// and P11's. Only gated code calls it.
func (rg *region) transact(ctx context.Context, puts []store.TxPut) error {
	if err := rg.prepare(ctx, puts); err != nil {
		return err
	}
	for _, p := range puts {
		if err := rg.write(ctx, p.Table, p.Item, p.Cond, true); err != nil {
			rg.run.violatef("harness: %s: transaction put of %s after its condition held: %v", rg.name, keyText(p.Item), err)
			return err
		}
	}
	if _, ok := puts[0].Item[catalog.AttrSync]; ok && rg.streams[puts[0].Table] != nil {
		rg.checkSync(ctx, puts)
	}
	return nil
}

// checkSync checks P11a and P11b on an event with _sync that just landed, as put 0 of puts, a
// transaction, or as their one put: the region accepted the event; of the SYNC versions,
// puts write rows only of those _sync names, and of each of them every row the oracle says
// the event reaches, so an event put alone reaches none; and each of them was live in the
// region at some moment since 30 s before the append's call. Only gated code calls it.
func (rg *region) checkSync(ctx context.Context, puts []store.TxPut) {
	r := rg.run
	event := puts[0].Item
	where := rg.name + " " + keyText(event)
	if len(puts) == 1 {
		r.count(&r.stats.SyncedAlone)
	} else {
		r.count(&r.stats.Synced)
	}
	if origin := event[catalog.AttrOriginRegion].Str; origin != rg.name {
		r.violatef("P11a: %s: an event of %s landed in %s", where, origin, rg.name)
	}
	named := map[string]bool{}
	for _, v := range event[catalog.AttrSync].List {
		named[v.Str] = true
	}
	written, err := rg.syncRows(puts[1:])
	if err != nil {
		r.violatef("P11a: %s: %v", where, err)
		return
	}
	want := r.reaches(rg, puts[0].Table, event)
	since := invocationOf(ctx).called.Add(-meta.MaxViewStateAge)
	for _, v := range slices.Sorted(maps.Keys(named)) {
		if !slices.Equal(written[v], want[v]) {
			r.violatef("P11a: %s: the event lands with rows %q of %s, and reaches %q", where, written[v], v, want[v])
		}
		if !rg.wasLive(v, since) {
			r.violatef("P11b: %s: %s wasn't live in %s since %v before the append's call", where, v, rg.name, meta.MaxViewStateAge)
		}
	}
	for _, v := range slices.Sorted(maps.Keys(written)) {
		if !named[v] {
			r.violatef("P11a: %s: the event lands with rows of %s, which its _sync doesn't name", where, v)
		}
	}
}

// syncRows returns, by view version, the keys of the rows that puts write, sorted, as the
// oracle names them; markers are left out.
func (rg *region) syncRows(puts []store.TxPut) (map[string][]string, error) {
	r := rg.run
	p := r.cfg.Catalog.Projector()
	rows := map[string][]string{}
	for _, put := range puts {
		version := r.views[put.Table]
		if _, marker := put.Item[catalog.AttrMarkerOwner]; marker || version == "" {
			continue
		}
		_, vv, _ := p.Version(version)
		kvs, _, ok, err := p.Row(version, []store.Item{put.Item}, time.Now())
		switch {
		case err != nil:
			return nil, fmt.Errorf("row %s of %s: %w", keyText(put.Item), version, err)
		case !ok:
			return nil, fmt.Errorf("row %s of %s has expired", keyText(put.Item), version)
		}
		rows[version] = append(rows[version], rowKey(kvs, vv))
	}
	for _, keys := range rows {
		slices.Sort(keys)
	}
	return rows, nil
}

// reaches returns, by view version, the keys of the rows that the oracle says event, an
// item of a stream table, reaches, sorted.
func (r *run) reaches(rg *region, table string, event store.Item) map[string][]string {
	o := newOracle()
	rec := projector.Record{Op: projector.Insert, New: cloneItem(event)}
	if err := r.sys.Expect(o, rg.streams[table].name, "", rec); err != nil {
		r.violatef("P11a: %s: oracle: %v", rg.name, err)
	}
	rows := map[string][]string{}
	for id := range o.rows {
		rows[id.View] = append(rows[id.View], id.Key)
	}
	for _, keys := range rows {
		slices.Sort(keys)
	}
	return rows
}

// resent returns what a resend of applied puts without their token gets: every condition
// fails over the item the first send stored.
func (rg *region) resent(ctx context.Context, puts []store.TxPut) error {
	tx := &store.TxError{Conds: make([]*store.ConditionError, len(puts))}
	failed := false
	for i, p := range puts {
		if p.Cond == (store.Cond{}) {
			continue
		}
		cur, err := rg.mem.Get(ctx, p.Table, store.KeyOf(p.Item), true)
		if err != nil {
			return err
		}
		tx.Conds[i] = &store.ConditionError{Current: cur}
		failed = true
	}
	if !failed {
		return nil
	}
	return tx
}

// inProgress sends a transaction that DynamoDB holds before it commits: its items stay as
// they were, and other writes to them conflict, until it commits up to HoldMax later. The
// caller's resend with the token then gets the commit's outcome. A core whose deadline comes
// first dies there, and the transaction still commits.
func (rg *region) inProgress(ctx context.Context, puts []store.TxPut, op string, lat time.Duration) error {
	r := rg.run
	inv := invocationOf(ctx)
	rg.skewPuts(ctx, puts)
	if err := rg.prepare(ctx, puts); err != nil {
		time.Sleep(lat)
		return err
	}
	r.count(&r.stats.InProgress)
	keys := make([]tableKey, len(puts))
	for i, p := range puts {
		keys[i] = tableKey{p.Table, store.KeyOf(p.Item)}
		rg.held[keys[i]] = true
	}
	// A replicated write may land meanwhile, so the commit checks the conditions again.
	commit := func(ctx context.Context, puts []store.TxPut) error {
		for _, k := range keys {
			delete(rg.held, k)
		}
		return rg.transact(ctx, puts)
	}
	at := time.Now().Add(between(r.faults, 1, r.sc.Faults.HoldMax))
	if !inv.deadline.IsZero() && inv.deadline.Before(at) {
		puts = clonePuts(puts)
		lctx := context.WithoutCancel(ctx)
		r.sched.goActor(func() {
			time.Sleep(time.Until(at))
			if r.sched.gate(inv.id+" commit", op) {
				_ = commit(lctx, puts) //nolint:errcheck // the invocation died at its deadline: nobody reads the response
			}
		})
		time.Sleep(time.Until(inv.deadline) + time.Nanosecond)
		runtime.Goexit()
	}
	time.Sleep(time.Until(at))
	if !r.sched.gate(inv.id, "commit "+op) {
		runtime.Goexit()
	}
	err := commit(ctx, puts)
	time.Sleep(lat)
	return err
}

func clonePuts(puts []store.TxPut) []store.TxPut {
	out := make([]store.TxPut, len(puts))
	for i, p := range puts {
		out[i] = store.TxPut{Table: p.Table, Item: cloneItem(p.Item), Cond: p.Cond}
	}
	return out
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// A SYNC append makes at most maxSyncAttempts attempts for one event, each a read and a
// transaction, or a transaction alone. Before the attempt that follows a conflict or
// throttling it waits a time drawn below min(syncBackoffCap, syncBackoffBase·2^n).
const (
	maxSyncAttempts = 10
	syncBackoffBase = 5 * time.Millisecond
	syncBackoffCap  = 250 * time.Millisecond
)

// liveSync returns the SYNC versions of s that an append applies (ADR 0014): those whose
// state, read as queries read it, is live, sorted. An import, or a stream without SYNC
// versions, reads no state.
func (e *Engine) liveSync(ctx context.Context, cat *Catalog, s *stream, m AppendMode) ([]string, error) {
	if m.Import || len(s.sync) == 0 {
		return nil, nil
	}
	states, err := e.stateCache(e.naming(cat).MetaTable()).States(ctx, false)
	if err != nil {
		return nil, err
	}
	var live []string
	for _, id := range s.sync {
		if states[id].State == meta.Live {
			live = append(live, id)
		}
	}
	return live, nil
}

// syncList is the _sync attribute that names versions.
func syncList(versions []string) store.Value {
	list := make([]store.Value, len(versions))
	for i, v := range versions {
		list[i] = store.Value{Type: store.S, Str: v}
	}
	return store.Value{Type: store.L, List: list}
}

// putSync writes it, an event of s that names versions in its _sync, with its contributions
// to their rows in one transaction, and returns the event stored at its key. The rows and
// markers are read first; a row or marker that changed since is folded in and the
// transaction sent again, and after a conflict or throttling, of the transaction or the
// read, everything is read again, which also finds an event that an earlier attempt stored.
// now is the writer's clock.
func (e *Engine) putSync(ctx context.Context, cat *Catalog, table string, s *stream, it store.Item, versions []string, now int64) (store.Item, error) {
	plan, err := cat.p.PlanSync(e.naming(cat), s.Def.Name, it, versions, time.UnixMicro(now))
	if err != nil {
		return nil, fmt.Errorf("dynaflux: append to %s: %w", s.Def.Name, err)
	}
	if len(plan.Reads()) == 0 {
		return e.put(ctx, table, s, it)
	}
	var read map[projector.TableKey]store.Item // nil until read, and after a conflict or throttling
	var cause error
	for n := range maxSyncAttempts {
		if read == nil {
			if n > 0 {
				if err = backoff(ctx, it, n-1); err != nil {
					return nil, fmt.Errorf("dynaflux: append to %s: %w", s.Def.Name, err)
				}
			}
			read, err = e.readSync(ctx, plan)
			switch {
			case errors.Is(err, store.ErrThrottled):
				cause = err
				continue
			case err != nil:
				return nil, fmt.Errorf("dynaflux: append to %s: %w", s.Def.Name, err)
			}
		}
		rows, perr := plan.Puts(read)
		if perr != nil {
			return nil, fmt.Errorf("dynaflux: append to %s: %w", s.Def.Name, perr)
		}
		puts := append([]store.TxPut{{Table: table, Item: it, Cond: store.IfAbsent()}}, rows...)
		err = e.b.Transact(ctx, puts)
		tx, failed := errors.AsType[*store.TxError](err)
		switch {
		case err == nil:
			return it, nil
		case failed && tx.Conds[0] != nil:
			return s.claimed(tx.Conds[0].Current, it)
		case failed:
			cause = changed(read, puts, tx)
		case errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrThrottled):
			read, cause = nil, err
		case errors.Is(err, store.ErrTooLarge):
			return nil, fmt.Errorf("dynaflux: append to %s: %w: the SYNC transaction: %w", s.Def.Name, ErrInvalidValue, err)
		default:
			return nil, fmt.Errorf("dynaflux: append to %s: %w", s.Def.Name, err)
		}
	}
	return nil, fmt.Errorf("dynaflux: append to %s: SYNC versions %s: %d attempts failed: %w",
		s.Def.Name, strings.Join(versions, ", "), maxSyncAttempts, cause)
}

// readSync reads, consistently, every item that plan's puts need, in a BatchGet per version,
// the versions in parallel, and returns those stored.
func (e *Engine) readSync(ctx context.Context, plan *projector.SyncPlan) (map[projector.TableKey]store.Item, error) {
	reads := plan.Reads()
	items := make([][]store.Item, len(reads))
	errs := make([]error, len(reads))
	var wg sync.WaitGroup
	for i, r := range reads {
		wg.Go(func() { items[i], errs[i] = e.b.BatchGet(ctx, r.Table, r.Keys, true) })
	}
	wg.Wait()
	read := map[projector.TableKey]store.Item{}
	for i, r := range reads {
		if errs[i] != nil {
			return nil, fmt.Errorf("read %s: %w", r.Table, errs[i])
		}
		for j, k := range r.Keys {
			if items[i][j] != nil {
				read[projector.TableKey{Table: r.Table, Key: k}] = items[i][j]
			}
		}
	}
	return read, nil
}

// changed folds the stored items that tx returned for puts into read, and returns why the
// transaction failed: a row or marker that another write changed since it was read.
func changed(read map[projector.TableKey]store.Item, puts []store.TxPut, tx *store.TxError) error {
	var cause error
	for i, c := range tx.Conds {
		if c == nil {
			continue
		}
		k := projector.TableKey{Table: puts[i].Table, Key: store.KeyOf(puts[i].Item)}
		if c.Current == nil {
			delete(read, k)
		} else {
			read[k] = c.Current
		}
		if cause == nil {
			cause = fmt.Errorf("%s item %q/%q changed: %w", k.Table, k.Key.PK, k.Key.SK, store.ErrConflict)
		}
	}
	return cause
}

// backoff waits before attempt n+1 of the event it, a time drawn below min(syncBackoffCap,
// syncBackoffBase·2^n). The draw hashes the event's key and n, which spreads the appends
// that met one row and keeps a simulation run replayable.
func backoff(ctx context.Context, it store.Item, n int) error {
	ceiling := min(syncBackoffCap, syncBackoffBase<<n)
	x := it[catalog.AttrPK].Str + "\x00" + it[catalog.AttrSK].Str + "\x00" + strconv.Itoa(n)
	return sleep(ctx, time.Duration(keys.Bucket(x, int(ceiling/time.Microsecond)))*time.Microsecond)
}

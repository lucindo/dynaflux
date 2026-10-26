// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
)

// counter is a stand-in for the projector: per-unit high-water marks and conditional
// writes, over two views of one stream. View page counts events, sums their amounts and
// counts distinct users per page, with uniqExact markers; a poison event fails its
// evaluation and opens its breaker. View total counts and sums every event in one row
// that every unit writes.
type counter struct {
	poison float64 // the share of poison events
	bug    bug
	once   atomic.Bool  // for bugs that strike once per run
	broken atomic.Int64 // reads that found page broken
}

// bug is a defect the harness must catch.
type bug uint8

const (
	noBug        bug = iota
	skipHWM          // applies records at or below the high-water mark again
	ignorePoison     // counts poison events instead of opening the breaker
	versionJump      // raises _v by 2
	alwaysFail       // fails every batch
	straggle         // writes from a goroutine after Handle returns
	panics           // panics once
	emptyCall        // reads the store for an empty batch
	unitColumn       // stores the delivery unit in page rows
)

const (
	streamName  = "events"
	streamTable = "stream-events"
	viewTable   = "views"
	metaTable   = "meta"
	viewPage    = "page"
	viewTotal   = "total"
	pages       = 5
	users       = 12
	maxAmount   = 100
	seqWidth    = catalog.SequenceWidth
	neverPruned = 48 * time.Hour // a W past every budget, for cores that never prune
)

func (*counter) Config() simulation.Config {
	return simulation.Config{
		Streams: []simulation.Stream{{Name: streamName, Table: streamTable}},
		Meta:    metaTable,
		Window:  neverPruned,
	}
}

func (s *counter) Event(r *rand.Rand, n int) (string, store.Item) {
	return streamName, store.Item{
		catalog.AttrPK: str(fmt.Sprintf("e%06d", n)),
		catalog.AttrSK: str("!"),
		"page":         str("p" + strconv.Itoa(r.IntN(pages))),
		"user":         str("u" + strconv.Itoa(r.IntN(users))),
		"amount":       num(1 + r.Int64N(maxAmount)),
		"poison":       {Type: store.BOOL, Bool: r.Float64() < s.poison},
	}
}

func (s *counter) NewCore(b store.Backend, a simulation.Archiver) simulation.Core {
	return &core{sys: s, b: b, a: a}
}

func (*counter) Expect(o *simulation.Oracle, unit string, rec simulation.Record) {
	ev := rec.New
	amount := new(big.Rat).SetInt64(integer(ev["amount"]))
	total := simulation.RowID{View: viewTotal, Key: viewTotal}
	o.Count(total, "count")
	o.Sum(total, "sum", amount)
	if ev["poison"].Bool {
		o.Poison(viewPage, simulation.RecordID{Unit: unit, Seq: rec.Seq})
		return
	}
	page := simulation.RowID{View: viewPage, Key: ev["page"].Str}
	o.Count(page, "count")
	o.Sum(page, "sum", amount)
	o.UniqExact(page, "users", ev["user"].Str)
}

func (s *counter) Read(ctx context.Context, b store.Backend) (simulation.State, error) {
	st := simulation.State{Rows: simulation.Rows{}, Broken: map[string]simulation.RecordID{}}
	keys := []simulation.RowID{{View: viewTotal, Key: viewTotal}}
	for p := range pages {
		keys = append(keys, simulation.RowID{View: viewPage, Key: "p" + strconv.Itoa(p)})
	}
	for _, id := range keys {
		it, err := b.Get(ctx, viewTable, rowKey(id), true)
		if err != nil {
			return st, err
		}
		if it == nil {
			continue
		}
		row := simulation.Row{"count": it["count"].Str, "sum": it["sum"].Str}
		if id.View == viewPage {
			row["users"] = it["users"].Str
		}
		if last, ok := it["last"]; ok {
			row["last"] = last.Str
		}
		st.Rows[id] = row
	}
	it, err := b.Get(ctx, metaTable, brokenKey(), true)
	if it != nil {
		s.broken.Add(1)
		st.Broken[viewPage] = simulation.RecordID{Unit: it["unit"].Str, Seq: it["seq"].Str}
	}
	return st, err
}

func rowKey(id simulation.RowID) store.Key {
	return store.Key{PK: id.View + "#" + id.Key, SK: "!"}
}

func brokenKey() store.Key { return store.Key{PK: "view", SK: viewPage} }

type core struct {
	sys *counter
	b   store.Backend
	a   simulation.Archiver
}

func (c *core) Handle(ctx context.Context, b simulation.Batch) (simulation.Result, error) {
	switch {
	case len(b.Records) == 0 && c.sys.bug == emptyCall:
		_, err := c.b.Get(ctx, metaTable, brokenKey(), true)
		return simulation.Result{}, err
	case len(b.Records) == 0:
		return simulation.Result{}, nil
	case c.sys.bug == alwaysFail:
		return simulation.Result{}, errors.New("stuck")
	case c.sys.bug == panics && c.sys.once.CompareAndSwap(false, true):
		panic("boom")
	case c.sys.bug == straggle && c.sys.once.CompareAndSwap(false, true):
		returned := make(chan struct{})
		defer close(returned)
		go func() {
			<-returned
			c.b.Put(ctx, metaTable, store.Item{catalog.AttrPK: str("late"), catalog.AttrSK: str("!")}, store.Cond{})
		}()
	}
	recs := slices.SortedFunc(slices.Values(b.Records), func(x, y simulation.Record) int {
		return cmp.Compare(pad(x.Seq), pad(y.Seq))
	})
	if err := c.a.Archive(ctx, b.Stream, b.Unit, recs); err != nil {
		return simulation.Result{}, err
	}
	rows, byRow, err := c.group(ctx, b.Unit, recs)
	if err != nil {
		return simulation.Result{}, err
	}
	// Rows apply in parallel, as in the projector.
	var mu sync.Mutex
	var wg sync.WaitGroup
	failed := ""
	for _, row := range rows {
		wg.Go(func() {
			if c.apply(ctx, b.Unit, row, byRow[row]) == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if s := byRow[row][0].Seq; failed == "" || pad(s) < pad(failed) {
				failed = s
			}
		})
	}
	wg.Wait()
	return simulation.Result{FailedSeq: failed}, nil
}

// group sends each record to its rows, in first-appearance order. A poison record opens
// page's breaker, after which no record reaches page.
func (c *core) group(ctx context.Context, unit string, recs []simulation.Record) ([]simulation.RowID, map[simulation.RowID][]simulation.Record, error) {
	broken, err := c.b.Get(ctx, metaTable, brokenKey(), true)
	if err != nil {
		return nil, nil, err
	}
	var rows []simulation.RowID
	byRow := map[simulation.RowID][]simulation.Record{}
	add := func(id simulation.RowID, rec simulation.Record) {
		if _, ok := byRow[id]; !ok {
			rows = append(rows, id)
		}
		byRow[id] = append(byRow[id], rec)
	}
	for _, rec := range recs {
		add(simulation.RowID{View: viewTotal, Key: viewTotal}, rec)
		if rec.New["poison"].Bool && broken == nil && c.sys.bug != ignorePoison {
			if broken, err = c.markBroken(ctx, unit, rec.Seq); err != nil {
				return nil, nil, err
			}
		}
		if broken == nil {
			add(simulation.RowID{View: viewPage, Key: rec.New["page"].Str}, rec)
		}
	}
	return rows, byRow, nil
}

func (c *core) markBroken(ctx context.Context, unit, seq string) (store.Item, error) {
	it := store.Item{
		catalog.AttrPK: str("view"),
		catalog.AttrSK: str(viewPage),
		"unit":         str(unit),
		"seq":          str(seq),
	}
	err := c.b.Put(ctx, metaTable, it, store.IfAbsent())
	if ce, ok := errors.AsType[*store.ConditionError](err); ok {
		return ce.Current, nil
	}
	return it, err
}

// apply read-modify-writes one row, retrying from the stored item when another writer
// won.
func (c *core) apply(ctx context.Context, unit string, id simulation.RowID, recs []simulation.Record) error {
	cur, err := c.b.Get(ctx, viewTable, rowKey(id), true)
	for err == nil {
		next, applied, mergeErr := c.merge(ctx, unit, id, cur, recs)
		if mergeErr != nil || !applied {
			return mergeErr
		}
		cond := store.IfAbsent()
		if cur != nil {
			cond = store.IfVersion(integer(cur[catalog.AttrRowVersion]))
		}
		err = c.b.Put(ctx, viewTable, next, cond)
		ce, lost := errors.AsType[*store.ConditionError](err)
		if !lost {
			return err
		}
		cur, err = ce.Current, nil
	}
	return err
}

// merge returns cur with the records above its high-water mark for unit, and whether
// there were any.
func (c *core) merge(ctx context.Context, unit string, id simulation.RowID, cur store.Item, recs []simulation.Record) (store.Item, bool, error) {
	hwm := maps.Clone(cur[catalog.AttrHighWaterMarks].Map)
	if hwm == nil {
		hwm = map[string]store.Value{}
	}
	mark := hwm[unit].Str
	count, sum, distinct := integer(cur["count"]), integer(cur["sum"]), integer(cur["users"])
	applied := false
	for _, rec := range recs {
		if c.sys.bug != skipHWM && mark != "" && pad(rec.Seq) <= pad(mark) {
			continue
		}
		applied = true
		count++
		sum += integer(rec.New["amount"])
		if id.View == viewPage {
			owned, err := c.owns(ctx, id, rec.New["user"].Str, unit+"/"+pad(rec.Seq))
			if err != nil {
				return nil, false, err
			}
			if owned {
				distinct++
			}
		}
		mark = rec.Seq
	}
	if !applied {
		return nil, false, nil
	}
	hwm[unit] = str(mark)
	step := int64(1)
	if c.sys.bug == versionJump {
		step = 2
	}
	k := rowKey(id)
	next := store.Item{
		catalog.AttrPK:             str(k.PK),
		catalog.AttrSK:             str(k.SK),
		catalog.AttrRowVersion:     num(integer(cur[catalog.AttrRowVersion]) + step),
		catalog.AttrHighWaterMarks: {Type: store.M, Map: hwm},
		"count":                    num(count),
		"sum":                      num(sum),
	}
	if id.View == viewPage {
		next["users"] = num(distinct)
		if c.sys.bug == unitColumn {
			next["last"] = str(unit)
		}
	}
	return next, true, nil
}

// owns reports whether the record owner introduced value to the row: it holds the value's
// marker, written now or by an earlier attempt.
func (c *core) owns(ctx context.Context, id simulation.RowID, value, owner string) (bool, error) {
	k := rowKey(id)
	k.PK = "!" + k.PK + "#" + value
	it, err := c.b.Get(ctx, viewTable, k, true)
	if err != nil {
		return false, err
	}
	if it == nil {
		marker := store.Item{catalog.AttrPK: str(k.PK), catalog.AttrSK: str(k.SK), catalog.AttrMarkerOwner: str(owner)}
		err = c.b.Put(ctx, viewTable, marker, store.IfAbsent())
		ce, taken := errors.AsType[*store.ConditionError](err)
		switch {
		case err == nil:
			return true, nil
		case taken:
			it = ce.Current
		default:
			return false, err
		}
	}
	return it[catalog.AttrMarkerOwner].Str == owner, nil
}

func pad(seq string) string { return strings.Repeat("0", seqWidth-len(seq)) + seq }

func str(s string) store.Value { return store.Value{Type: store.S, Str: s} }

func num(n int64) store.Value { return store.Value{Type: store.N, Str: strconv.FormatInt(n, 10)} }

// integer reads an N; an absent value reads as 0.
func integer(v store.Value) int64 {
	n, err := strconv.ParseInt(v.Str, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

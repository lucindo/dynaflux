// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// receiptPrefix starts the partition key of a receipt (encoding.md §8.6).
const receiptPrefix = "rcpt#"

// maxWait is the longest a reader waits.
const maxWait = 3 * time.Minute

// waited is an INSERT that an append asked a receipt for: its stream, its unit and its
// sequence number in PadSeq's form.
type waited struct {
	stream string
	unit   *unit
	seq    string
}

// receiptPK returns the partition key of the receipt of it, an event of stream.
func receiptPK(stream string, it store.Item) (string, error) {
	_, id, err := keys.ParseStreamSK(it[catalog.AttrSK].Str)
	return receiptPrefix + stream + "#" + id + "#" + it[catalog.AttrIngestTime].Str, err
}

// noteWaitable indexes rec, a record of u, by its receipt when an append in this region asked
// one for it. Only gated code calls it.
func (s *stream) noteWaitable(u *unit, rec projector.Record) {
	rcpt, origin := rec.New[catalog.AttrReceipt], rec.New[catalog.AttrOriginRegion]
	if rec.Op != projector.Insert || !rcpt.Bool || origin.Str != s.rg.name {
		return
	}
	pk, err := receiptPK(s.name, rec.New)
	seq, serr := keys.PadSeq(rec.Seq)
	if err = errors.Join(err, serr); err != nil {
		s.rg.run.violatef("harness: %s: record %s: %v", s.rg.name, rec.Seq, err)
		return
	}
	s.rg.waitable[pk] = waited{stream: s.name, unit: u, seq: seq}
}

// progress returns the progress of w's unit in the region's meta, "" for none.
func (rg *region) progress(ctx context.Context, w waited) string {
	k := store.Key{PK: unitPrefix + w.stream + "#" + keys.UnitKey(w.unit.id), SK: keys.NoSortKey}
	it, err := rg.mem.Get(ctx, rg.run.meta, k, true)
	if err != nil {
		rg.run.violatef("harness: %s: read unit %s: %v", rg.name, w.unit.id, err)
	}
	return it[attrProgress].Str
}

// noteReceipt checks P12 on a receipt that just landed, and notes when it first landed, for
// P15. Only gated code calls it.
func (rg *region) noteReceipt(ctx context.Context, it store.Item) {
	r := rg.run
	pk := it[catalog.AttrPK].Str
	r.count(&r.stats.Receipts)
	w, ok := rg.waitable[pk]
	if !ok {
		r.violatef("P12: %s: receipt %s names no INSERT that an append here asked one for", rg.name, pk)
		return
	}
	if _, seen := rg.landed[pk]; !seen {
		rg.landed[pk] = time.Now()
	}
	if p := rg.progress(ctx, w); p < w.seq {
		r.violatef("P12: %s: receipt %s landed before progress covered its record: unit %s at %q, record %s", rg.name, pk, w.unit.id, p, w.seq)
	}
}

// checkReceipts checks P13: each region holds the receipt of every INSERT that an append
// there asked one for, and no other region holds it, unless an append there stored the same
// event in the same microsecond.
func (r *run) checkReceipts(ctx context.Context) {
	for _, rg := range r.regions {
		for _, pk := range slices.Sorted(maps.Keys(rg.waitable)) {
			for _, other := range r.regions {
				it, err := other.mem.Get(ctx, r.meta, store.Key{PK: pk, SK: keys.NoSortKey}, true)
				_, own := other.waitable[pk]
				switch {
				case err != nil:
					r.violatef("harness: %s: read receipt %s: %v", other.name, pk, err)
				case other == rg && it == nil:
					r.violatef("P13: %s: no receipt %s, which an append there asked for", rg.name, pk)
				case !own && it != nil:
					r.violatef("P13: %s holds receipt %s, which an append in %s asked for", other.name, pk, rg.name)
				}
			}
		}
	}
}

// startReader starts a reader that waits for the event that a, an append of ev with e in rg,
// stored. Only gated code calls it.
func (r *run) startReader(id string, rg *region, e *engine.Engine, ev Event, a engine.Appended) {
	r.pending++
	r.sched.goActor(func() { r.read(id, rg, e, ev, a) })
}

// read queries a version that a's stored event fails, or else one it reaches: once without
// waiting, which caches the view states as an app that read before it appended would, then
// after waiting for the event, whose rows it checks (P15).
func (r *run) read(id string, rg *region, e *engine.Engine, ev Event, a engine.Appended) {
	stored := a.Stored
	rng := r.actorRNG(id)
	ctx := withInvocation(context.Background(), &invocation{id: id, stallAt: -1})
	if version, q := r.waitQuery(rg, ev, stored, rng.IntN); q != nil {
		params := make([]expr.Value, len(q.Params))
		for i, p := range q.Params {
			params[i] = r.cfg.Params[p.Name]
		}
		if err := r.query(ctx, e, q.Name, params, engine.QueryOptions{}); err != nil && !errors.Is(err, engine.ErrViewNotReady) && !injected(err) {
			r.violatef("P15: %s: query %s: %v", id, q.Name, err)
		}
		w := engine.Wait{Token: e.Token(ev.Stream, []engine.Appended{a}), Timeout: between(rng, time.Second, maxWait)}
		err := r.query(ctx, e, q.Name, params, engine.QueryOptions{Wait: []engine.Wait{w}})
		r.sched.gate(id, "check")
		r.waited(rg, id, q.Name, version, ev.Stream, stored, err)
	}
	r.sched.gate(id, "done")
	r.settle(1)
}

// readAttempts bounds the times a reader sends a query that failed with an injected error.
const readAttempts = 3

// query runs a reader's query, again after an injected error, and returns its error.
func (r *run) query(ctx context.Context, e *engine.Engine, name string, params []expr.Value, o engine.QueryOptions) error {
	var err error
	for range readAttempts {
		if _, err = e.Query(ctx, r.cfg.Catalog, name, params, o); !injected(err) {
			return err
		}
		time.Sleep(writeRetry)
	}
	return err
}

// waitQuery picks a query over a version that stored, an event of ev's stream, fails, or
// else over one it reaches, with pick; nil when no query reads such a version.
func (r *run) waitQuery(rg *region, ev Event, stored store.Item, pick func(int) int) (string, *catalog.Query) {
	o := r.expectOne(rg, ev.Stream, stored)
	qs := r.queriesOf(slices.Collect(maps.Keys(o.poison)))
	if len(qs) == 0 {
		var reached []string
		for id := range o.rows {
			reached = append(reached, id.View)
		}
		qs = r.queriesOf(reached)
	}
	if len(qs) == 0 {
		return "", nil
	}
	q := qs[pick(len(qs))]
	v, _ := r.cfg.Catalog.Catalog().View(q.Source.Name)
	cur, _ := v.Current()
	return catalog.VersionID(v.Name, cur.Number), q
}

// expectOne returns an oracle fed only stored, an event of stream in rg.
func (r *run) expectOne(rg *region, stream string, stored store.Item) *Oracle {
	o := newOracle()
	if err := r.sys.Expect(o, stream, "", projector.Record{Op: projector.Insert, New: cloneItem(stored)}); err != nil {
		r.violatef("P15: %s: oracle: %v", rg.name, err)
	}
	return o
}

// queriesOf returns the named queries that read one of versions.
func (r *run) queriesOf(versions []string) []*catalog.Query {
	cat := r.cfg.Catalog.Catalog()
	var out []*catalog.Query
	for i := range cat.Queries {
		q := &cat.Queries[i]
		v, ok := cat.View(q.Source.Name)
		if q.Source.Kind != catalog.SourceView || !ok {
			continue
		}
		if cur, live := v.Current(); live && slices.Contains(versions, catalog.VersionID(v.Name, cur.Number)) {
			out = append(out, q)
		}
	}
	return out
}

// waited checks a reader's query of version, which waited for stored, an event of stream
// that an append in rg stored, and ended with err: P15 when it returned rows. Only gated
// code calls it.
func (r *run) waited(rg *region, id, query, version, stream string, stored store.Item, err error) {
	switch {
	case errors.Is(err, engine.ErrWaitTimeout):
		r.count(&r.stats.WaitTimeouts)
		return
	case errors.Is(err, engine.ErrViewNotReady):
		r.count(&r.stats.WaitsNotReady)
		return
	case err != nil && !injected(err):
		r.violatef("P15: %s: query %s after a wait: %v", id, query, err)
	case err != nil:
		return
	}
	r.count(&r.stats.Waits)
	pk, perr := receiptPK(stream, stored)
	w, ok := rg.waitable[pk]
	if perr != nil || !ok {
		r.violatef("harness: %s: the reader's event has no receipt to wait for: %v", id, perr)
		return
	}
	if at, landed := rg.landed[pk]; landed && !rg.wasLive(version, at) {
		r.violatef("P15: %s: query %s read %s, which wasn't live in %s since receipt %s landed", id, query, version, rg.name, pk)
	}
	ctx := context.Background()
	if rg.progress(ctx, w) >= w.seq {
		return
	}
	for row := range r.expectOne(rg, stream, stored).rows {
		if row.View == version && !rg.holds(ctx, row, w) {
			r.violatef("P15: %s: query %s read row %s of %s, which doesn't hold the record of receipt %s", id, query, row.Key, version, pk)
		}
	}
}

// holds reports whether an item of row has a high-water mark at or above w's record. Only
// gated code calls it.
func (rg *region) holds(ctx context.Context, row RowID, w waited) bool {
	r := rg.run
	p := r.cfg.Catalog.Projector()
	_, vv, _ := p.Version(row.View)
	for table, v := range r.views {
		if v != row.View {
			continue
		}
		for k := range rg.written[table] {
			it, err := rg.mem.Get(ctx, table, k, true)
			if _, marker := it[catalog.AttrMarkerOwner]; err != nil || it == nil || marker {
				continue
			}
			kvs, _, ok, rerr := p.Row(row.View, []store.Item{it}, time.Now())
			if rerr != nil || !ok || rowKey(kvs, vv) != row.Key {
				continue
			}
			marks, present := it[catalog.AttrHighWaterMarks]
			h, herr := keys.DecodeHWM(marks, present)
			if herr == nil && h.Units[keys.UnitKey(w.unit.id)] >= w.seq {
				return true
			}
		}
	}
	return false
}

// rowKey returns a row's key values as the oracle names them.
func rowKey(kvs []expr.Value, vv *catalog.ViewVersion) string {
	key := make([]string, len(kvs))
	for i, kv := range kvs {
		key[i] = FormatValue(kv, vv.Keys[i].Expr.Type)
	}
	return strings.Join(key, " ")
}

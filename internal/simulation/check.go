// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// check runs the properties checked at quiescence: P0 on every change stream; P1, P3 and
// the backfills' end states (P6) per region; P2 across regions. The rest of P4–P9 was
// checked as the run went.
func (r *run) check(ctx context.Context) {
	for _, rg := range r.regions {
		r.checkStream(rg)
		r.checkBackfills(ctx, rg)
	}
	states := make([]*State, len(r.regions))
	for i, rg := range r.regions {
		st, err := r.sys.Read(ctx, rg.mem)
		if err != nil {
			r.violatef("P1: %s: read: %v", rg.name, err)
			continue
		}
		r.checkRegion(rg.name, r.expect(rg), st)
		states[i] = &st
	}
	if len(r.regions) > 1 {
		r.converge(ctx, states)
	}
}

// converge checks P2 between the first region and each other one, on the logical rows
// their stores hold. states are what the read path read, nil where it failed.
func (r *run) converge(ctx context.Context, states []*State) {
	stored := make([]*State, len(r.regions))
	for i, rg := range r.regions {
		if states[i] == nil {
			continue
		}
		rows, err := r.logicalRows(ctx, rg)
		if err != nil {
			r.violatef("P2: %s: read: %v", rg.name, err)
			continue
		}
		stored[i] = &State{Rows: rows, Broken: states[i].Broken}
	}
	divergent := r.divergentRows()
	for i := 1; i < len(stored); i++ {
		if stored[0] != nil && stored[i] != nil {
			r.checkConvergence(r.regions[0].name+" vs "+r.regions[i].name, *stored[0], *stored[i], divergent)
		}
	}
}

// logicalRows returns the view rows that a region's store holds, read past the read path:
// each aggregate's state, merged across the row's SPLIT items, in its canonical encoding.
// A view that a region keys rows by, through a DEDUP BY stream's ingest_time or
// origin_region, has its rows merged across those keys. Expired rows are left out.
func (r *run) logicalRows(ctx context.Context, rg *region) (Rows, error) {
	rows := Rows{}
	for _, table := range slices.Sorted(maps.Keys(rg.written)) {
		if err := r.viewRows(ctx, rg, table, rows); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// viewRows adds the logical rows of a view table to rows.
func (r *run) viewRows(ctx context.Context, rg *region, table string, rows Rows) error {
	p, version, now := r.cfg.Catalog.Projector(), r.views[table], time.Now()
	_, vv, _ := p.Version(version)
	regional := r.regional(vv)
	merged := map[string][]agg.State{}
	ks := slices.SortedFunc(maps.Keys(rg.written[table]), func(a, b store.Key) int {
		return cmp.Or(strings.Compare(a.PK, b.PK), strings.Compare(a.SK, b.SK))
	})
	for _, k := range ks {
		it, err := rg.mem.Get(ctx, table, k, true)
		if err != nil {
			return err
		}
		if _, marker := it[catalog.AttrMarkerOwner]; it == nil || marker {
			continue
		}
		kvs, states, ok, err := p.Row(version, []store.Item{it}, now)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		var key []string
		for i, kv := range kvs {
			if !regional[i] {
				key = append(key, FormatValue(kv, vv.Keys[i].Expr.Type))
			}
		}
		if err = merge(merged, strings.Join(key, " "), states); err != nil {
			return err
		}
	}
	for id, states := range merged {
		row, err := stateRow(vv, states)
		if err != nil {
			return err
		}
		rows[RowID{View: version, Key: id}] = row
	}
	return nil
}

// merge merges states into the states of row id.
func merge(merged map[string][]agg.State, id string, states []agg.State) error {
	acc, seen := merged[id]
	if !seen {
		merged[id] = states
		return nil
	}
	for i := range acc {
		if err := acc[i].Merge(states[i]); err != nil {
			return err
		}
	}
	return nil
}

// stateRow returns the encoded states of a row of vv, by column; an empty state is absent.
func stateRow(vv *catalog.ViewVersion, states []agg.State) (Row, error) {
	row := Row{}
	for i, st := range states {
		if st.Empty() {
			continue
		}
		v, err := st.Encode()
		if err != nil {
			return nil, err
		}
		row[vv.Aggregates[i].Name] = valueText(v)
	}
	return row, nil
}

// regional reports, per key of vv, whether the region that accepted an event decides it:
// an event of a DEDUP BY stream appended in two regions has another ingest_time and
// origin_region in each (DF3504).
func (r *run) regional(vv *catalog.ViewVersion) []bool {
	out := make([]bool, len(vv.Keys))
	if !r.dedup[vv.Stream] {
		return out
	}
	for i := range vv.Keys {
		cols := map[string]bool{}
		vv.Keys[i].Expr.ColumnNames(cols)
		out[i] = cols[catalog.ColumnIngestTime] || cols[catalog.ColumnOriginRegion]
	}
	return out
}

// valueText formats a stored value: bytes in hex, a list element by element.
func valueText(v store.Value) string {
	switch v.Type {
	case store.S, store.N:
		return v.Str
	case store.B:
		return hex.EncodeToString(v.Bin)
	case store.L:
		parts := make([]string, len(v.List))
		for i, e := range v.List {
			parts[i] = valueText(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	case store.BOOL, store.M:
	}
	return fmt.Sprint(v)
}

// divergentRows returns the rows that some region's INSERT of an event appended with two
// payloads reached: regions may keep either payload (sql.md §8.5).
func (r *run) divergentRows() map[RowID]bool {
	o := newOracle()
	for _, rg := range r.regions {
		r.feed(o, rg, "P2", func(s *stream, rec projector.Record) bool { return r.divergent[tableKey{s.table, keyOf(rec.New)}] })
	}
	return o.rows
}

// expect feeds the oracle every INSERT of the region's change streams.
func (r *run) expect(rg *region) *Oracle {
	o := newOracle()
	r.feed(o, rg, "P1", func(*stream, projector.Record) bool { return true })
	return o
}

// feed feeds o the INSERT records of rg's change streams that keep selects. An error
// violates property.
func (r *run) feed(o *Oracle, rg *region, property string, keep func(*stream, projector.Record) bool) {
	for _, s := range rg.order {
		for _, u := range s.units {
			for _, rec := range u.recs {
				if rec.Op != projector.Insert || !keep(s, rec) {
					continue
				}
				if err := r.sys.Expect(o, s.name, u.id, cloneRecord(rec)); err != nil {
					r.violatef("%s: %s: oracle: %v", property, rg.name, err)
				}
			}
		}
	}
}

// checkBackfills checks that each version the run backfills ended live, or broken.
func (r *run) checkBackfills(ctx context.Context, rg *region) {
	if r.sc.Backfill.Within == 0 {
		return
	}
	states, err := meta.New(rg.mem, r.meta).ViewStates(ctx)
	if err != nil {
		r.violatef("P6: %s: read the view states: %v", rg.name, err)
		return
	}
	for _, v := range r.cfg.Backfill {
		if s := states[v].State; s != meta.Live && s != meta.Broken {
			r.violatef("P6: %s: the backfill of %s ended %s", rg.name, v, cmp.Or(s, meta.PendingBackfill))
		}
	}
}

// checkBreakers checks P3 after a Handle of b returned res without error: a live view
// that a record fails is broken by then; at quiescence, a redelivery that opened the
// breaker later would hide one the Handle lost. The Handle evaluated every record for a
// view live since the run started, and for one that doesn't need the fenced handoff. A
// backfilled fenced view was evaluated for every record below FailedSeq, by the Handle or,
// at or below the unit's fence, by its finished backfill; from FailedSeq on, a Handle whose
// cached states predate the activation leaves the records to their redelivery.
func (r *run) checkBreakers(rg *region, b projector.Batch, res projector.Result) {
	done := b.Records // in delivery order, which is sequence order
	if i := slices.IndexFunc(done, func(rec projector.Record) bool { return rec.Seq == res.FailedSeq }); i >= 0 {
		done = done[:i]
	}
	all, below := r.poisoned(rg, b, b.Records), r.poisoned(rg, b, done)
	if len(all) == 0 {
		return
	}
	states, err := meta.New(rg.mem, r.meta).ViewStates(context.Background())
	if err != nil {
		r.violatef("P3: %s: read the view states: %v", rg.name, err)
		return
	}
	for _, view := range slices.Sorted(maps.Keys(all)) {
		if states[view].State == meta.Live && (below[view] || !r.backfills(view) || !r.fenced[view]) {
			r.violatef("P3: %s: a Handle of %s returned, but view %s, which a record fails, isn't broken", rg.name, b.Unit, view)
		}
	}
}

// poisoned returns the views that an INSERT of recs, records of b, fails.
func (r *run) poisoned(rg *region, b projector.Batch, recs []projector.Record) map[string]bool {
	o := newOracle()
	for _, rec := range recs {
		if rec.Op != projector.Insert {
			continue
		}
		if err := r.sys.Expect(o, b.Stream, b.Unit, cloneRecord(rec)); err != nil {
			r.violatef("P3: %s: oracle: %v", rg.name, err)
		}
	}
	views := map[string]bool{}
	for view := range o.poison {
		views[view] = true
	}
	return views
}

// checkStream checks P0 on a region's change stream: every applied append is in it once
// as an INSERT, and once some region deleted the item, once as a REMOVE after it; an item
// appended in n regions has fewer than n MODIFY records. Nothing else is.
func (r *run) checkStream(rg *region) {
	for k, h := range r.histories(rg) {
		appends, expired := r.applied[k], 0
		if r.expired[k] {
			expired = 1
		}
		switch {
		case appends == 0:
			r.violatef("P0: %s: %s %s is in the change stream, but no append wrote it", rg.name, k.table, k.key)
		case len(h.ins) != 1:
			r.violatef("P0: %s: %s %s is in the change stream as an INSERT %d times", rg.name, k.table, k.key, len(h.ins))
		case h.mods >= appends:
			r.violatef("P0: %s: %s %s has %d MODIFY records, but %d appends wrote it", rg.name, k.table, k.key, h.mods, appends)
		case len(h.rem) != expired:
			r.violatef("P0: %s: %s %s has %d REMOVE records, want %d", rg.name, k.table, k.key, len(h.rem), expired)
		case len(h.rem) == 1 && compareSeq(h.rem[0], h.ins[0]) < 0:
			r.violatef("P0: %s: %s %s is removed at %s, before its INSERT at %s", rg.name, k.table, k.key, h.rem[0], h.ins[0])
		}
	}
}

// history is what a region's change stream holds of one item.
type history struct {
	ins, rem []string // sequence numbers
	mods     int
}

// histories returns the history of each item an append applied or the region's change
// stream holds.
func (r *run) histories(rg *region) map[tableKey]*history {
	hist := map[tableKey]*history{}
	for k := range r.applied {
		hist[k] = &history{}
	}
	for _, s := range rg.order {
		for _, u := range s.units {
			for _, rec := range u.recs {
				k := tableKey{s.table, recordKey(rec)}
				h := hist[k]
				if h == nil {
					h = &history{}
					hist[k] = h
				}
				switch rec.Op {
				case projector.Insert:
					h.ins = append(h.ins, rec.Seq)
				case projector.Modify:
					h.mods++
				case projector.Remove:
					h.rem = append(h.rem, rec.Seq)
				}
			}
		}
	}
	return hist
}

// compareSeq compares sequence numbers as delivered, unpadded.
func compareSeq(a, b string) int {
	return cmp.Or(cmp.Compare(len(a), len(b)), strings.Compare(a, b))
}

// checkRegion checks P1, each view against the oracle, and P3, the breakers. A broken
// view is left out of P1.
func (r *run) checkRegion(region string, o *Oracle, st State) {
	broken := func(id RowID) bool {
		_, ok := st.Broken[id.View]
		return ok
	}
	for _, d := range diffRows(st.Rows, o.result(), broken) {
		r.violatef("P1: %s %s", region, d)
	}
	views := slices.Sorted(maps.Keys(st.Broken))
	for view := range o.poison {
		if _, ok := st.Broken[view]; !ok {
			views = append(views, view)
		}
	}
	for _, view := range views {
		rec, isBroken := st.Broken[view]
		failing := o.poison[view]
		switch {
		case isBroken && len(failing) == 0:
			r.violatef("P3: %s: view %s is broken by %v, but no record fails it", region, view, rec)
		case !isBroken:
			r.violatef("P3: %s: view %s isn't broken, but %d records fail it", region, view, len(failing))
		case !failing[rec]:
			r.violatef("P3: %s: view %s is broken by %v, which doesn't fail it", region, view, rec)
		}
	}
}

// checkConvergence checks P2: views broken in neither region hold the same rows, but for
// those in skip.
func (r *run) checkConvergence(regions string, a, b State, skip map[RowID]bool) {
	broken := func(id RowID) bool {
		_, inA := a.Broken[id.View]
		_, inB := b.Broken[id.View]
		return inA || inB || skip[id]
	}
	for _, d := range diffRows(a.Rows, b.Rows, broken) {
		r.violatef("P2: %s %s", regions, d)
	}
}

// diffRows describes each row not skipped whose got and want differ.
func diffRows(got, want Rows, skip func(RowID) bool) []string {
	ids := slices.Collect(maps.Keys(got))
	for id := range want {
		if _, ok := got[id]; !ok {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, b RowID) int {
		return cmp.Or(strings.Compare(a.View, b.View), strings.Compare(a.Key, b.Key))
	})
	var out []string
	for _, id := range ids {
		g, inGot := got[id]
		w, inWant := want[id]
		if skip(id) || inGot == inWant && maps.Equal(g, w) {
			continue
		}
		out = append(out, id.View+"/"+id.Key+": got "+showRow(g, inGot)+", want "+showRow(w, inWant))
	}
	return out
}

func showRow(row Row, ok bool) string {
	if !ok {
		return "<absent>"
	}
	parts := make([]string, 0, len(row))
	for _, col := range slices.Sorted(maps.Keys(row)) {
		parts = append(parts, col+"="+row[col])
	}
	return "{" + strings.Join(parts, " ") + "}"
}

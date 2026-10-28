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

// check runs the properties checked at quiescence: P0 on the acknowledged appends and on
// every change stream; P1, P3, P10, the jobs that the archive broke (P14) and the backfills'
// end states (P6) per region; P2 across regions; P13. The rest of P4–P9 was checked as the
// run went. A version whose job an incomplete archive broke stays activating, so P1, P2 and
// P6 leave it out.
func (r *run) check(ctx context.Context) {
	r.checkAcked()
	r.checkReceipts(ctx)
	divergent := r.divergentRows()
	states := make([]*State, len(r.regions))
	for i, rg := range r.regions {
		r.checkStream(rg)
		vs, err := meta.New(rg.mem, r.meta).ViewStates(ctx)
		if err != nil {
			r.violatef("P6: %s: read the view states: %v", rg.name, err)
			continue
		}
		r.incomplete[rg.name] = r.checkIncomplete(ctx, rg, vs)
		r.checkBackfills(rg.name, vs)
		st, err := r.sys.Read(ctx, rg.mem)
		if err != nil {
			r.violatef("P1: %s: read: %v", rg.name, err)
			continue
		}
		timeSplit := map[string]bool{}
		for version, s := range vs {
			timeSplit[version] = s.Split == meta.SplitTime
		}
		r.checkRegion(rg.name, r.expect(rg), st, timeSplit, divergent)
		r.checkSplitBreaks(rg, vs, st)
		states[i] = &st
	}
	if len(r.regions) > 1 {
		r.converge(ctx, states, divergent)
	}
}

// converge checks P2 between the first region and each other one, on the logical rows
// their stores hold, but for those in divergent. states are what the read path read, nil
// where it failed.
func (r *run) converge(ctx context.Context, states []*State, divergent map[RowID]bool) {
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
	for i := 1; i < len(stored); i++ {
		if stored[0] != nil && stored[i] != nil {
			r.checkConvergence(r.regions[0].name, r.regions[i].name, *stored[0], *stored[i], divergent)
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
		r.feed(o, rg, "P2", func(s *stream, rec projector.Record) bool {
			return r.divergent[tableKey{s.table, store.KeyOf(rec.New)}]
		})
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

// checkBackfills checks that each version the run backfills ended live, or broken, in a
// region whose view states are states. It counts the versions split by time.
func (r *run) checkBackfills(region string, states map[string]meta.ViewState) {
	if r.sc.Backfill.Within == 0 {
		return
	}
	for _, v := range r.cfg.Backfill {
		vs := states[v]
		switch {
		case r.incomplete[region][v]:
		case vs.State != meta.Live && vs.State != meta.Broken:
			r.violatef("P6: %s: the backfill of %s ended %s", region, v, cmp.Or(vs.State, meta.PendingBackfill))
		case vs.Split != meta.SplitTime:
		case vs.State == meta.Live:
			r.count(&r.stats.TimeSplits)
		case vs.Failure.Cause == meta.CauseSplit:
			r.count(&r.stats.SplitBreaks)
		}
	}
}

// checkSplitBreaks checks the breaks of P10: a version that its time split broke names an
// INSERT at or before its split time that a batch started delivering at or after its late
// bound, or a MODIFY whose images straddle the split time; where the scenario keeps every
// bound, only such a MODIFY; and a version split by time whose stream holds such a MODIFY in
// the region is broken.
func (r *run) checkSplitBreaks(rg *region, states map[string]meta.ViewState, st State) {
	for _, view := range slices.Sorted(maps.Keys(states)) {
		vs := states[view]
		b, broken := st.Broken[view]
		switch {
		case vs.Split != meta.SplitTime:
		case !broken:
			if rec, ok := rg.straddle(vs); ok {
				r.violatef("P10: %s: %s isn't broken, but MODIFY %s straddles its split time", rg.name, view, rec)
			}
		case b.Split && rg.places(vs, b.Record):
			r.violatef("P10: %s: %s broke by its time split at %+v, which the split places", rg.name, view, b.Record)
		case b.Split && r.sc.splitBoundsHold() && !rg.straddling(b.Record, vs):
			r.violatef("P10: %s: %s broke by its time split at %+v, though every bound held", rg.name, view, b.Record)
		}
	}
}

// straddling reports whether the record that id names is a MODIFY whose images straddle the
// split time of vs.
func (rg *region) straddling(id RecordID, vs meta.ViewState) bool {
	rec, found := rg.record(id)
	return found && rec.Op == projector.Modify && straddles(vs, rec)
}

// places reports whether the time split of vs places the record that id names, which then
// can't break it: anything but an INSERT at or before the split time that a batch started
// delivering at or after the late bound, or a MODIFY whose images straddle the split time.
func (rg *region) places(vs meta.ViewState, id RecordID) bool {
	rec, found := rg.record(id)
	switch {
	case !found:
		return true
	case rec.Op == projector.Insert:
		return ingestTime(rec.New) > vs.SplitAt.UnixMicro() || rg.delivered[RecordID{Unit: id.Unit, Seq: id.Seq}].Before(vs.SplitLate)
	case rec.Op == projector.Modify:
		return !straddles(vs, rec)
	}
	return true
}

// straddle returns a MODIFY of the region's change stream of vs's stream whose images straddle
// its split time.
func (rg *region) straddle(vs meta.ViewState) (string, bool) {
	s := rg.streams[rg.run.cfg.Naming.StreamTable(vs.Stream)]
	for _, u := range s.units {
		for _, rec := range u.recs {
			if rec.Op == projector.Modify && straddles(vs, rec) {
				return u.id + "/" + rec.Seq, true
			}
		}
	}
	return "", false
}

func straddles(vs meta.ViewState, rec projector.Record) bool {
	at := vs.SplitAt.UnixMicro()
	return (ingestTime(rec.Old) <= at) != (ingestTime(rec.New) <= at)
}

func ingestTime(it store.Item) int64 { return integer(it[catalog.AttrIngestTime]) }

// record returns the record of the region's change streams that id names, by unit and
// sequence number.
func (rg *region) record(id RecordID) (projector.Record, bool) {
	for _, s := range rg.order {
		for _, u := range s.units {
			if u.id != id.Unit {
				continue
			}
			for _, rec := range u.recs {
				if rec.Seq == id.Seq {
					return rec, true
				}
			}
		}
	}
	return projector.Record{}, false
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

// checkStream checks P0 on a region's change stream, for each life of each item: every
// applied append is in it once as an INSERT, and once some region deleted the item, once as
// a REMOVE after it, which ends the life, so a REMOVE before the INSERT leaves a life
// without one; an item appended in n regions has fewer than n MODIFY records. Nothing else
// is.
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
		case h.rem != expired:
			r.violatef("P0: %s: %s %s has %d REMOVE records, want %d", rg.name, k.table, k.key, h.rem, expired)
		}
	}
}

// checkAcked checks P0 on the appends acknowledged as stored: each wrote its event.
func (r *run) checkAcked() {
	for _, a := range r.acked {
		if !r.written[a] {
			r.violatef("P0: %s %s %s: an append in %s was acknowledged as stored, but no append wrote its event of ingest_time %d",
				a.table, a.key.PK, a.key.SK, a.origin, a.at)
		}
	}
}

// history is what a region's change stream holds of one life of an item.
type history struct {
	ins  []string // sequence numbers
	rem  int
	mods int
}

// histories returns the history of each life of an item that an append applied or the
// region's change stream holds. A REMOVE ends a life, so the records of an item, in
// sequence order, fall into its lives.
func (r *run) histories(rg *region) map[lifeKey]*history {
	hist := map[lifeKey]*history{}
	for k := range r.applied {
		hist[k] = &history{}
	}
	for _, s := range rg.order {
		byItem := map[store.Key][]projector.Record{}
		for _, u := range s.units {
			for _, rec := range u.recs {
				byItem[recordKey(rec)] = append(byItem[recordKey(rec)], rec)
			}
		}
		for key, recs := range byItem {
			slices.SortFunc(recs, func(a, b projector.Record) int { return compareSeq(a.Seq, b.Seq) })
			k := lifeKey{tableKey{s.table, key}, 0}
			for _, rec := range recs {
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
					h.rem++
					k.life++
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

// checkRegion checks P1, each view against the oracle, P3, the breakers, and P10's exactness
// of the views split by time, which timeSplit names. A broken view, or one retired in the
// region, is left out of P1. So are the rows that divergent names of a view split by time:
// its backfill reads the table's copy of an event, which may hold the other payload.
func (r *run) checkRegion(region string, o *Oracle, st State, timeSplit map[string]bool, divergent map[RowID]bool) {
	broken := func(id RowID) bool {
		_, ok := st.Broken[id.View]
		return ok || r.retiredIn(region, id.View) || r.incomplete[region][id.View] || timeSplit[id.View] && divergent[id]
	}
	inexact := map[string]bool{}
	for _, d := range diffRows(st.Rows, o.result(), broken) {
		r.violatef("P1: %s %s", region, d.text)
		inexact[d.id.View] = true
	}
	for _, view := range slices.Sorted(maps.Keys(inexact)) {
		if timeSplit[view] {
			r.violatef("P10: %s: %s, split by time, is neither exact nor broken", region, view)
		}
	}
	views := slices.Sorted(maps.Keys(st.Broken))
	for view := range o.poison {
		if _, ok := st.Broken[view]; !ok {
			views = append(views, view)
		}
	}
	for _, view := range views {
		b, isBroken := st.Broken[view]
		failing := o.poison[view]
		switch {
		case b.Split:
			// P10 checks why.
		case !isBroken && r.incomplete[region][view]:
			// Its backfill evaluated none of the records at or below its fences.
		case isBroken && len(failing) == 0:
			r.violatef("P3: %s: view %s is broken by %v, but no record fails it", region, view, b.Record)
		case !isBroken:
			r.violatef("P3: %s: view %s isn't broken, but %d records fail it", region, view, len(failing))
		case !slices.ContainsFunc(slices.Collect(maps.Keys(failing)), b.Record.names):
			r.violatef("P3: %s: view %s is broken by %v, which doesn't fail it", region, view, b.Record)
		}
	}
}

// checkConvergence checks P2: views neither retired nor broken in either region hold the
// same rows, but for those in skip.
func (r *run) checkConvergence(ra, rb string, a, b State, skip map[RowID]bool) {
	broken := func(id RowID) bool {
		_, inA := a.Broken[id.View]
		_, inB := b.Broken[id.View]
		return inA || inB || skip[id] || r.retiredIn(ra, id.View) || r.retiredIn(rb, id.View) ||
			r.incomplete[ra][id.View] || r.incomplete[rb][id.View]
	}
	for _, d := range diffRows(a.Rows, b.Rows, broken) {
		r.violatef("P2: %s vs %s %s", ra, rb, d.text)
	}
}

// rowDiff is a row whose got and want differ, and its description.
type rowDiff struct {
	id   RowID
	text string
}

// diffRows describes each row not skipped whose got and want differ.
func diffRows(got, want Rows, skip func(RowID) bool) []rowDiff {
	ids := slices.Collect(maps.Keys(got))
	for id := range want {
		if _, ok := got[id]; !ok {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, b RowID) int {
		return cmp.Or(strings.Compare(a.View, b.View), strings.Compare(a.Key, b.Key))
	})
	var out []rowDiff
	for _, id := range ids {
		g, inGot := got[id]
		w, inWant := want[id]
		if skip(id) || inGot == inWant && maps.Equal(g, w) {
			continue
		}
		out = append(out, rowDiff{id, id.View + "/" + id.Key + ": got " + showRow(g, inGot) + ", want " + showRow(w, inWant)})
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

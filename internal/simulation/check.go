// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"
)

// check runs the properties checked at quiescence: P0 on every change stream, P1 and P3
// per region, P2 across regions. P4–P9 were checked as the run went.
func (r *run) check(ctx context.Context) {
	for _, rg := range r.regions {
		r.checkStream(rg)
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
	for i := 1; i < len(states); i++ {
		if states[0] != nil && states[i] != nil {
			r.checkConvergence(r.regions[0].name+" vs "+r.regions[i].name, *states[0], *states[i])
		}
	}
}

// expect feeds the oracle every INSERT of the region's change streams.
func (r *run) expect(rg *region) *Oracle {
	o := newOracle()
	for _, s := range rg.order {
		for _, u := range s.units {
			for _, rec := range u.recs {
				if rec.Op == Insert {
					r.sys.Expect(o, u.id, cloneRecord(rec))
				}
			}
		}
	}
	return o
}

// checkStream checks P0: every applied append appears exactly once in the region's
// change stream, and nothing else does.
func (r *run) checkStream(rg *region) {
	seen := map[tableKey]int{}
	for _, s := range rg.order {
		for _, u := range s.units {
			for _, rec := range u.recs {
				seen[tableKey{s.table, keyOf(rec.New)}]++
			}
		}
	}
	for k, n := range seen {
		switch {
		case !r.applied[k]:
			r.violatef("P0: %s: %s %s is in the change stream, but no append wrote it", rg.name, k.table, k.key)
		case n > 1:
			r.violatef("P0: %s: %s %s is in the change stream %d times", rg.name, k.table, k.key, n)
		}
	}
	for k := range r.applied {
		if seen[k] == 0 {
			r.violatef("P0: %s: %s %s was appended, but isn't in the change stream", rg.name, k.table, k.key)
		}
	}
}

// checkRegion checks P1, each view against the oracle, and P3, the breakers. A broken
// view is left out of P1.
func (r *run) checkRegion(region string, o *Oracle, st State) {
	broken := func(view string) bool {
		_, ok := st.Broken[view]
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

// checkConvergence checks P2: views broken in neither region hold the same rows.
func (r *run) checkConvergence(regions string, a, b State) {
	broken := func(view string) bool {
		_, inA := a.Broken[view]
		_, inB := b.Broken[view]
		return inA || inB
	}
	for _, d := range diffRows(a.Rows, b.Rows, broken) {
		r.violatef("P2: %s %s", regions, d)
	}
}

// diffRows describes each row of a view not skipped whose got and want differ.
func diffRows(got, want Rows, skip func(view string) bool) []string {
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
		if skip(id.View) || inGot == inWant && maps.Equal(g, w) {
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

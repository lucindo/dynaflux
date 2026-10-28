// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector

import (
	"errors"
	"fmt"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// TableKey is an item's key in a table.
type TableKey struct {
	Table string
	Key   store.Key
}

// TableKeys are keys of items in one table.
type TableKeys struct {
	Table string
	Keys  []store.Key
}

// SyncPlan is one event's contribution to the SYNC versions that its append applies in its
// transaction (ADR 0014). It counts uniqExact values by its own rule: a value counts only
// when its marker is absent, and the transaction writes the marker.
type SyncPlan struct {
	owner string // the markers' owner: E and the event's event_id
	rows  []syncRow
}

// syncRow is a version's row that the event reaches, and the markers of its values.
type syncRow struct {
	table   string
	g       *rowGroup
	markers []marked
}

// PlanSync evaluates event, an item of stream that an append built, for versions, SYNC
// versions of the catalog whose tables n names. An evaluation error names the version; one
// of sql.md §5.8 matches [expr.ErrEval]. A version whose WHERE drops the event, or whose row
// has expired at now, the writer's clock, gets no row: live projection would skip it too.
func (c *Compiled) PlanSync(n catalog.Naming, stream string, event store.Item, versions []string, now time.Time) (*SyncPlan, error) {
	s, ok := c.streams[stream]
	if !ok {
		return nil, fmt.Errorf("projector: no stream %s in the catalog", stream)
	}
	_, id, err := keys.ParseStreamSK(event[catalog.AttrSK].Str)
	if err != nil {
		return nil, fmt.Errorf("projector: a SYNC append's event: %w", err)
	}
	owner, err := keys.OwnerEvent(id)
	if err != nil {
		return nil, fmt.Errorf("projector: %w", err)
	}
	r := &record{Record: Record{Op: Insert, New: event}}
	vals, bad := s.Decode(event)
	rs := &rows{groups: map[groupKey]*rowGroup{}}
	for _, version := range versions {
		v := c.views[version]
		if v == nil || v.stream != s || v.def.Mode != catalog.ModeSync {
			return nil, fmt.Errorf("projector: %s is not a SYNC view version of stream %s", version, stream)
		}
		if err = rs.add(v, r, vals, bad); err != nil {
			return nil, fmt.Errorf("projector: view %s: %w", version, err)
		}
	}
	p := &SyncPlan{owner: owner}
	for _, g := range rs.order {
		if !g.hasExp || now.UnixMicro() < g.exp {
			p.rows = append(p.rows, syncRow{table: n.ViewTable(g.v.name, g.v.def.Number), g: g, markers: g.marked(g.contribs)})
		}
	}
	return p, nil
}

// Reads lists the items that Puts needs, by table: a row's item, then its values' markers.
func (p *SyncPlan) Reads() []TableKeys {
	out := make([]TableKeys, 0, len(p.rows))
	for _, r := range p.rows {
		ks := []store.Key{r.g.key}
		for _, m := range r.markers {
			ks = append(ks, m.key)
		}
		out = append(out, TableKeys{Table: r.table, Keys: ks})
	}
	return out
}

// Puts returns the row and marker puts that follow the event's put, from read, every item
// that Reads lists and is stored. Each row gets the event's contribution and _v + 1, and
// keeps _hwm, the backfill's mark included, as read: the event's presence records that the
// row holds it. Each value whose marker is absent counts, and its marker is put. A present
// marker never counts, whatever its owner.
func (p *SyncPlan) Puts(read map[TableKey]store.Item) ([]store.TxPut, error) {
	var puts []store.TxPut
	for _, r := range p.rows {
		g := r.g
		cur, err := g.current(read[TableKey{r.table, g.key}])
		if err != nil {
			return nil, fmt.Errorf("projector: %w", err)
		}
		counted := make([][]expr.Value, len(g.v.aggs))
		var markers []store.TxPut
		for _, m := range r.markers {
			if read[TableKey{r.table, m.key}] == nil {
				counted[m.agg] = append(counted[m.agg], m.val)
				markers = append(markers, store.TxPut{Table: r.table, Item: g.marker(m.key, p.owner), Cond: store.IfAbsent()})
			}
		}
		if err = cur.add(g.contribs, counted); err != nil {
			return nil, fmt.Errorf("projector: view %s: %w", g.v.id, err)
		}
		cond := store.IfAbsent()
		if cur.version > 0 {
			cond = store.IfVersion(cur.version)
		}
		cur.version++
		it, err := g.encodeItem(cur)
		if err != nil {
			return nil, fmt.Errorf("projector: view %s: %w", g.v.id, err)
		}
		puts = append(append(puts, store.TxPut{Table: r.table, Item: it, Cond: cond}), markers...)
	}
	return puts, nil
}

// errSync opens the breakers of the versions an INSERT with a malformed _sync reaches.
var errSync = errors.New(catalog.AttrSync + " is not a list of strings")

// synced returns the versions that the _sync of it, an INSERT's new image, names when its
// origin_region is the core's region: only that region's append ran the transaction. An ID
// the catalog lacks matches no version. A _sync that isn't a list of strings is an error,
// since only the writer knows which versions it applied.
func (c *Core) synced(it store.Item) (map[string]bool, error) {
	region, fromHere := it[catalog.AttrOriginRegion]
	list, has := it[catalog.AttrSync]
	if !fromHere || !has || region.Type != store.S || region.Str != c.region {
		return map[string]bool{}, nil
	}
	if list.Type != store.L {
		return nil, errSync
	}
	ids := make(map[string]bool, len(list.List))
	for _, id := range list.List {
		if id.Type != store.S {
			return nil, errSync
		}
		ids[id.Str] = true
	}
	return ids, nil
}

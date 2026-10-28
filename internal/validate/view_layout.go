// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/sql"
)

const (
	minSplit = 2
	// maxTransactItems is the most items one DynamoDB transaction writes.
	maxTransactItems = 100
)

// layout checks ORDER BY and PRIMARY KEY (sql.md §9.4) and sets the version's keys, in
// ORDER BY order, and its partition key.
func (v *viewCompiler) layout() {
	var written []string
	writtenOK := false
	if ob := v.s.OrderBy; ob != nil {
		written, writtenOK = listNames(v.st, ob)
	}
	orderOK := v.checkOrderBy(written) && writtenOK
	order := v.groupNames
	if orderOK {
		order = written
	}
	vv := &v.info.version
	for _, name := range order {
		if o, ok := v.keys[name]; ok && !o.bad {
			vv.Keys = append(vv.Keys, catalog.NamedExpr{Name: name, Expr: o.ir})
		}
	}
	pkLen, pkOK := v.primaryKey(written, len(vv.Keys))
	vv.PartitionKeyLen = pkLen
	v.info.keysOK = orderOK && pkOK && len(vv.Keys) == len(order)
	if v.info.keysOK {
		v.uuidKeys(vv.Keys[pkLen:])
	}
}

// checkOrderBy reports a missing ORDER BY (DF3007), or one that doesn't list exactly the
// GROUP BY keys (DF3101).
func (v *viewCompiler) checkOrderBy(written []string) bool {
	ob := v.s.OrderBy
	if ob == nil {
		keys := "<GROUP BY keys>"
		if len(v.groupNames) > 0 {
			keys = strings.Join(v.groupNames, ", ")
		}
		v.st.errorf(v.s.Pos, "DF3007", "add ORDER BY ("+keys+") before AS", "view has no ORDER BY")
		return false
	}
	if !v.groupOK {
		return false
	}
	if diff := keyDiff(v.groupNames, written); diff != "" {
		v.st.errorf(ob.Pos, "DF3101", "", "ORDER BY must list exactly the GROUP BY keys: %s", diff)
		return false
	}
	return true
}

// keyDiff describes how order differs from the keys: the keys it leaves out, the names
// that aren't keys, and the names it repeats.
func keyDiff(keys, order []string) string {
	var missing, extra, repeated []string
	for _, k := range keys {
		if !slices.Contains(order, k) {
			missing = append(missing, k)
		}
	}
	for i, name := range order {
		switch {
		case slices.Contains(order[:i], name):
			repeated = append(repeated, name)
		case !slices.Contains(keys, name):
			extra = append(extra, name)
		}
	}
	var parts []string
	for _, p := range []struct {
		what  string
		names []string
	}{{"missing", missing}, {"extra", extra}, {"repeated", repeated}} {
		if len(p.names) > 0 {
			parts = append(parts, p.what+" "+strings.Join(p.names, ", "))
		}
	}
	return strings.Join(parts, "; ")
}

// primaryKey checks that PRIMARY KEY is a prefix of ORDER BY (DF3102) and returns its
// length; without the clause, the primary key is all n keys.
func (v *viewCompiler) primaryKey(order []string, n int) (int, bool) {
	pk := v.s.PrimaryKey
	if pk == nil {
		return n, true
	}
	names, ok := listNames(v.st, pk)
	if !ok || v.s.OrderBy == nil {
		return n, false
	}
	const prefix = "Primary key must be a prefix of the sorting key, but "
	for i, name := range names {
		switch {
		case i >= len(order):
			v.st.errorf(pk.Pos, "DF3102", "", prefix+"its length: %d is greater than the sorting key length: %d",
				len(names), len(order))
		case order[i] != name:
			v.st.errorf(pk.Pos, "DF3102", "", prefix+"the column in the position %d is %s, not %s", i, order[i], name)
		default:
			continue
		}
		return n, false
	}
	return len(names), true
}

// uuidKeys reports a UUID key in the sort key (DF3106): ClickHouse orders UUIDs
// differently from other systems.
func (v *viewCompiler) uuidKeys(sortKey []catalog.NamedExpr) {
	for _, k := range sortKey {
		if k.Expr.Type.Kind == catalog.KindUUID {
			v.st.errorf(v.byName[k.Name].item.X.Extent().Pos, "DF3106",
				"ClickHouse orders UUIDs differently from other systems, so they can't be in the sort key",
				"UUID key %s must be in PRIMARY KEY", k.Name)
		}
	}
}

// ttl checks TTL <key> [+ INTERVAL n unit] (sql.md §9.6) and compiles it.
func (v *viewCompiler) ttl() *catalog.ViewTTL {
	clause := v.s.TTL
	if clause == nil {
		return nil
	}
	ref, iv, ok := ttlShape(clause.X)
	if !ok {
		v.st.errorf(clause.Pos, "DF3301", "", "TTL must be <key> or <key> + INTERVAL <n> <unit>")
		return nil
	}
	if qualified(v.st, ref.Ident) {
		return nil
	}
	ttl := &catalog.ViewTTL{Key: ref.Name}
	valid := true
	if iv != nil {
		var plus catalog.Interval
		plus, valid = checkInterval(v.st, iv)
		ttl.Plus = &plus
	}
	key, isKey := v.keys[ref.Name]
	switch {
	case isKey && key.bad:
		return nil
	case !isKey || !isTemporal(key.typ()):
		v.st.errorf(ref.Pos, "DF3302", "", "TTL column %s is not a Date, DateTime or DateTime64 key of the view", ref.Name)
		return nil
	case !valid:
		return nil
	}
	return ttl
}

// ttlShape splits TTL into its key and its interval, which is nil for a bare key.
func ttlShape(x sql.Expr) (*sql.ColumnRef, *sql.Interval, bool) {
	if ref, ok := unparen(x).(*sql.ColumnRef); ok {
		return ref, nil, true
	}
	b, ok := unparen(x).(*sql.BinaryExpr)
	if !ok || b.Op != sql.OpAdd {
		return nil, nil, false
	}
	ref, isRef := unparen(b.X).(*sql.ColumnRef)
	iv, isInterval := unparen(b.Y).(*sql.Interval)
	return ref, iv, isRef && isInterval
}

func split(st *stmt, c *sql.IntClause) int {
	if c == nil {
		return 1
	}
	if c.Value.Int < minSplit || c.Value.Int > catalog.MaxSplit {
		st.errorf(c.Pos, "DF3306", "", "SPLIT must be between %d and %d", minSplit, catalog.MaxSplit)
		return 1
	}
	return int(c.Value.Int)
}

// mode resolves MODE, or defaults.view_mode without the clause, and checks the rules of
// MODE SYNC (sql.md §9.7). They point at MODE, or at the view's name when the mode comes
// from dynaflux.yaml.
func (v *viewCompiler) mode() catalog.Mode {
	m, pos := v.p.cfg.Defaults.ViewMode, v.info.decl.pos
	if v.s.Mode != nil {
		m, pos = catalog.ModeAsync, v.s.Mode.Pos
		if v.s.Mode.Sync {
			m = catalog.ModeSync
		}
	}
	if m != catalog.ModeSync {
		return m
	}
	if len(v.s.Select.ArrayJoins) > 0 {
		v.st.errorf(pos, "DF3303", "use MODE ASYNC", "MODE SYNC can't be used with ARRAY JOIN")
	}
	if v.s.Split != nil {
		v.st.errorf(pos, "DF3304", "", "MODE SYNC can't be used with SPLIT in v1")
	}
	if s := v.info.stream; s != nil && !v.groupsByPartition(s.stream.PrimaryKey) {
		v.st.warnf(pos, "DF3501", "add the stream's PRIMARY KEY columns to GROUP BY, or use MODE ASYNC",
			"MODE SYNC on a view whose rows many partition keys of %s share; concurrent appends will conflict, and canceled transactions are billed", s.stream.Name)
	}
	return m
}

// groupsByPartition reports whether every column of the stream's PRIMARY KEY is a key of
// the view, by itself or under an alias.
func (v *viewCompiler) groupsByPartition(primaryKey []string) bool {
	for _, col := range primaryKey {
		if !slices.ContainsFunc(v.info.version.Keys, func(k catalog.NamedExpr) bool {
			return k.Expr.Op == catalog.OpColumn && k.Expr.Name == col
		}) {
			return false
		}
	}
	return true
}

// checkSyncTransactions checks that an append fits one DynamoDB transaction (DF3305): the
// event, a row of each SYNC view version of its stream, current or retained, and a marker
// per uniqExact aggregate of those versions.
func (p *project) checkSyncTransactions() {
	items := map[string]int{} // by stream
	for _, d := range p.decls {
		if v := d.view; v != nil && v.stream != nil {
			items[v.version.Stream] += transactItems(&v.version)
		}
	}
	for _, v := range p.views {
		for i := range v.Versions {
			if vv := &v.Versions[i]; vv.State == catalog.StateRetained {
				items[vv.Stream] += transactItems(vv)
			}
		}
	}
	for _, d := range p.decls {
		if n := 1 + items[d.name]; d.stream != nil && n > maxTransactItems {
			d.st.errorf(d.pos, "DF3305",
				"make some views ASYNC, use fewer uniqExact aggregates in SYNC views, or retire old versions",
				"an append to %s writes %d items in one transaction; DynamoDB allows %d", d.name, n, maxTransactItems)
		}
	}
}

// transactItems returns the items an append writes for a view version in its transaction:
// none for an ASYNC version, and otherwise its row and a marker per uniqExact aggregate.
func transactItems(vv *catalog.ViewVersion) int {
	if vv.Mode != catalog.ModeSync {
		return 0
	}
	n := 1
	for _, a := range vv.Aggregates {
		if a.Fn == catalog.AggUniqExact {
			n++
		}
	}
	return n
}

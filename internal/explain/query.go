// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// eventualRead is what an eventually consistent read costs, as a share of a strongly
// consistent one.
const eventualRead = 0.5

// readPlan is what a query call reads.
type readPlan struct {
	q *catalog.Query
	// keys are the source's key columns in order: a view's keys, or a stream's PRIMARY KEY,
	// event_time and event_id.
	keys  []string
	items int  // per row: SPLIT
	sync  bool // reads are strongly consistent
	// combos is the number of key combinations, or the most of them when exact is false.
	combos int
	exact  bool
}

func (e *explainer) queryReport(q *catalog.Query) error {
	if q.Source.Kind == catalog.SourceStream {
		return e.streamQuery(q)
	}
	v, _ := e.cat.View(q.Source.Name)
	vv, ok := v.Current()
	if !ok {
		return fmt.Errorf("view %s has no current version", v.Name)
	}
	s, _ := e.cat.Stream(vv.Stream)
	est, err := e.estimate(s)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(est.views, func(vp viewPlan) bool { return vp.version == vv })
	e.header(fmt.Sprintf("query %s :%s, from view %s", q.Name, q.Cardinality, v.Name), est.flags(e.w, i))
	r := newReadPlan(q, keyNamesOf(vv.Keys), vv.Split)
	r.sync = vv.Mode == catalog.ModeSync
	e.b.WriteString("\n")
	rowKB := est.cost.Views[i].RowKB
	if len(q.Keys.Partition)+len(q.Keys.SortPrefix) == len(vv.Keys) {
		e.pointRead(&r, rowKB)
	} else {
		e.rangeRead(&r, rowKB)
	}
	e.queryTail(&r)
	return nil
}

func (e *explainer) streamQuery(q *catalog.Query) error {
	s, _ := e.cat.Stream(q.Source.Name)
	e.header(fmt.Sprintf("query %s :%s, from stream %s", q.Name, q.Cardinality, s.Name), []string{"--event-kb " + flagValue(e.w.EventKB)})
	r := newReadPlan(q, append(slices.Clone(s.PrimaryKey), catalog.ColumnEventTime, catalog.ColumnEventID), s.Shards)
	e.b.WriteString("\n")
	e.rangeRead(&r, e.w.EventKB)
	if p, ok := s.LogRetention.Period(); ok {
		older := "in the archive"
		if s.Archive == nil {
			older = "gone: the stream isn't archived"
		}
		e.line("window", fmt.Sprintf("only events still in the log, LOG RETENTION %s; older ones are %s", p, older))
	}
	e.queryTail(&r)
	return nil
}

func newReadPlan(q *catalog.Query, keys []string, items int) readPlan {
	r := readPlan{q: q, keys: keys, items: items, combos: 1, exact: true}
	for _, b := range slices.Concat(q.Keys.Partition, q.Keys.SortPrefix) {
		distinct := map[[3]string]bool{}
		for _, v := range b.Values {
			if v.Type.Array {
				r.exact = false
			}
			distinct[[3]string{string(v.Op), v.Name, v.Value}] = true
		}
		r.combos *= len(distinct)
	}
	if !r.exact {
		r.combos = catalog.MaxKeyCombinations
	}
	return r
}

func keyNamesOf(keys []catalog.NamedExpr) []string {
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = k.Name
	}
	return names
}

// pointRead writes a read of whole keys, item by item.
func (e *explainer) pointRead(r *readPlan, rowKB float64) {
	op := "BatchGetItem"
	if r.exact && r.combos*r.items == 1 {
		op = "GetItem"
	}
	e.line("read", op+" of "+bindingsText(r.q.Keys.Partition, r.q.Keys.SortPrefix))
	e.arrayKeys(r)
	items := r.count(r.items, "item", "items")
	if r.items > 1 {
		items += fmt.Sprintf(": SPLIT %d stores each row as %d items", r.items, r.items)
	}
	e.line("items", items)
	e.line("read cost", fmt.Sprintf("%s KB per item, each rounded up to %d KB: %s",
		num(rowKB), readUnitKB, r.readUnits(math.Ceil(rowKB/readUnitKB), "item")))
}

// rangeRead writes a Query per key combination, and per shard of a stream, over a range of
// the next key column. kb is the size of what it reads: a row of a view, or an event.
func (e *explainer) rangeRead(r *readPlan, kb float64) {
	k, stream := r.q.Keys, r.q.Source.Kind == catalog.SourceStream
	read := "Query of " + bindingsText(k.Partition, k.SortPrefix)
	if k.Range != nil {
		read += " AND " + rangeText(k.Range)
	}
	e.line("read", read)
	e.arrayKeys(r)
	perCombo, unit, calls := 1, "row", "one per key combination"
	if stream {
		perCombo, unit = r.items, "event"
		if r.items > 1 {
			calls = fmt.Sprintf("one per key combination and shard (SHARDS %d)", r.items)
		}
	}
	e.line("calls", r.count(perCombo, "Query", "Queries")+", "+calls)
	items := "every " + unit + " of a key combination"
	if k.Range != nil {
		items = "every " + unit + " in the range"
	}
	if !stream && r.items > 1 {
		items += fmt.Sprintf(", %d items per row (SPLIT %d)", r.items, r.items)
		unit = "item"
	}
	e.line("items", items)
	e.line("read cost", fmt.Sprintf("%s KB per %s, and a Query rounds its total up to %d KB: %s",
		num(kb), unit, readUnitKB, r.readUnits(kb/readUnitKB, unit)))
	if r.q.Limit == nil {
		return
	}
	limit := "LIMIT " + valueText(*r.q.Limit)
	if r.limitBounds() {
		e.line("limit", limit+" stops each Query after that many rows")
	} else {
		e.line("limit", limit+" applies after the read, so it doesn't bound the items read")
	}
}

// readUnits says what reading one item costs, from its strongly consistent read units.
func (r *readPlan) readUnits(strong float64, item string) string {
	switch {
	case r.sync:
		return fmt.Sprintf("~%s read %s per %s, strongly consistent as the view is SYNC", num(strong), plural(strong, "unit", "units"), item)
	case r.q.Source.Kind == catalog.SourceStream:
		return fmt.Sprintf("~%s read %s per %s eventually consistent, ~%s strongly consistent",
			num(strong*eventualRead), plural(strong*eventualRead, "unit", "units"), item, num(strong))
	}
	return fmt.Sprintf("~%s read %s per %s eventually consistent, ~%s with WaitFor, which reads with strong consistency",
		num(strong*eventualRead), plural(strong*eventualRead, "unit", "units"), item, num(strong))
}

// arrayKeys notes that Array parameters set the number of key combinations.
func (e *explainer) arrayKeys(r *readPlan) {
	if !r.exact {
		e.line("keys", fmt.Sprintf("up to %d key combinations: the Array parameters set them at call time", catalog.MaxKeyCombinations))
	}
}

// count says how many of something a call reads, perCombo for each key combination.
func (r *readPlan) count(perCombo int, one, many string) string {
	n := r.combos * perCombo
	if !r.exact {
		return fmt.Sprintf("up to %d %s per call", n, many)
	}
	return fmt.Sprintf("%d %s per call", n, plural(float64(n), one, many))
}

// queryTail writes the steps of a query after its read.
func (e *explainer) queryTail(r *readPlan) {
	if r.q.Filter != nil {
		e.line("filter", "the WHERE terms that bind no key run on the items read: they don't reduce the read or its cost")
	}
	if a := r.q.Aggregation; a != nil {
		var aggs []string
		for _, qa := range a.Aggregates {
			aggs = append(aggs, queryAggregateText(qa))
		}
		group := "one row"
		if len(a.GroupBy) > 0 {
			group = "GROUP BY " + keyNames(a.GroupBy)
		}
		e.line("combine", group+": "+strings.Join(aggs, ", "))
	}
}

func queryAggregateText(qa catalog.QueryAggregate) string {
	a := qa.Aggregate
	if qa.Merge {
		a.Fn += "Merge"
	}
	return aggregateText(a)
}

// limitBounds reports whether the read stops after LIMIT rows per key combination: the
// query neither filters nor aggregates, and OrderBy, tie-breakers included, is the key
// order or its reverse once the columns that = or IN bind are left out.
func (r *readPlan) limitBounds() bool {
	if r.q.Filter != nil || r.q.Aggregation != nil {
		return false
	}
	bound := map[string]bool{}
	for _, b := range slices.Concat(r.q.Keys.Partition, r.q.Keys.SortPrefix) {
		bound[b.Column] = true
	}
	var sorted []string
	var desc []bool
	for _, o := range r.q.OrderBy {
		if o.Expr.Op != catalog.OpColumn {
			return false
		}
		if !bound[o.Expr.Name] {
			sorted = append(sorted, o.Expr.Name)
			desc = append(desc, o.Desc)
		}
	}
	rest := slices.DeleteFunc(slices.Clone(r.keys), func(k string) bool { return bound[k] })
	mixed := slices.Contains(desc, true) && slices.Contains(desc, false)
	return !mixed && slices.Equal(sorted, rest)
}

func bindingsText(groups ...[]catalog.KeyBinding) string {
	var terms []string
	for _, bs := range groups {
		for _, b := range bs {
			terms = append(terms, bindingText(b))
		}
	}
	return strings.Join(terms, " AND ")
}

func bindingText(b catalog.KeyBinding) string {
	if len(b.Values) == 1 && !b.Values[0].Type.Array {
		return b.Column + " = " + valueText(b.Values[0])
	}
	values := make([]string, len(b.Values))
	for i, v := range b.Values {
		values[i] = valueText(v)
	}
	if len(values) == 1 {
		return b.Column + " IN " + values[0]
	}
	return b.Column + " IN (" + strings.Join(values, ", ") + ")"
}

func rangeText(r *catalog.KeyRange) string {
	var terms []string
	if r.Lower != nil {
		op := " > "
		if r.Lower.Inclusive {
			op = " >= "
		}
		terms = append(terms, r.Column+op+valueText(r.Lower.Value))
	}
	if r.Upper != nil {
		op := " < "
		if r.Upper.Inclusive {
			op = " <= "
		}
		terms = append(terms, r.Column+op+valueText(r.Upper.Value))
	}
	return strings.Join(terms, " AND ")
}

func valueText(e catalog.Expr) string {
	if e.Op != catalog.OpLiteral {
		return exprText(e)
	}
	if e.Type.Kind == catalog.KindString {
		return stringLiteral(e.Value)
	}
	return e.Value
}

// stringLiteral quotes s as SQL that reads back as s. Characters that aren't graphic, such
// as ESC and line breaks, print as \xHH escapes so that they can't act on a terminal.
func stringLiteral(s string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for _, r := range s {
		switch {
		case r == '\'':
			b.WriteString("''")
		case r == '\\':
			b.WriteString(`\\`)
		case strconv.IsGraphic(r):
			b.WriteRune(r)
		default:
			for _, c := range []byte(string(r)) {
				fmt.Fprintf(&b, `\x%02x`, c)
			}
		}
	}
	b.WriteByte('\'')
	return b.String()
}

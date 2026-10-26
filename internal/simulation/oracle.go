// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"cmp"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// RowID names a view row: its view version and its key.
type RowID struct{ View, Key string }

// Row maps a row's columns to their finalized values as text.
type Row map[string]string

// Rows are the rows of a region's views.
type Rows map[RowID]Row

// RecordID names one change-stream record of a region.
type RecordID struct{ Unit, Seq string }

// State is what a region's views hold, as the read path returns it.
type State struct {
	Rows   Rows
	Broken map[string]RecordID // view → the record whose evaluation opened its breaker
}

// Oracle computes views naively, from each INSERT record of a region once and without
// incremental state, as the reference for P1 and P3. Exact numbers read as
// [big.Rat.RatString] prints them, and Float64 values as [strconv.FormatFloat] does with
// format 'g'. Sketches are the exception: their reference is agg's state built from every
// input in one pass, which checks that merging states loses and repeats nothing.
type Oracle struct {
	rows     map[RowID]bool
	counts   map[cell]int64
	sums     map[cell]*big.Rat
	floats   map[cell]*big.Rat // exact sums of Float64 values
	avgs     map[cell]*mean
	mins     map[cell]*big.Rat
	maxes    map[cell]*big.Rat
	texts    map[cell]string // the largest String
	picks    map[cell]*picked
	sets     map[cell]map[string]bool
	sketches map[cell]agg.State
	poison   map[string]map[RecordID]bool
}

type mean struct {
	sum big.Rat
	n   int64
}

// Tie orders an input row of argMax, argMin and any after its value: its event's time and
// ID, then its position in the ARRAY JOIN array (sql.md Dev 8).
type Tie struct {
	Time  int64
	ID    string
	Index int
}

type picked struct {
	fn  catalog.AggFunc
	arg string
	val *big.Rat // nil for any
	tie Tie
}

type cell struct {
	row RowID
	col string
}

func newOracle() *Oracle {
	return &Oracle{
		rows:     map[RowID]bool{},
		counts:   map[cell]int64{},
		sums:     map[cell]*big.Rat{},
		floats:   map[cell]*big.Rat{},
		avgs:     map[cell]*mean{},
		mins:     map[cell]*big.Rat{},
		maxes:    map[cell]*big.Rat{},
		texts:    map[cell]string{},
		picks:    map[cell]*picked{},
		sets:     map[cell]map[string]bool{},
		sketches: map[cell]agg.State{},
		poison:   map[string]map[RecordID]bool{},
	}
}

// Reach records that a source row reached row, which exists from then on, even when no
// aggregate input reached it.
func (o *Oracle) Reach(row RowID) { o.rows[row] = true }

// Count adds n to the count column col of row; with n = 0, as for a countIf whose
// condition doesn't hold, the column reads 0.
func (o *Oracle) Count(row RowID, col string, n int64) {
	o.rows[row] = true
	o.counts[cell{row, col}] += n
}

// Sum adds v to the sum column col of row.
func (o *Oracle) Sum(row RowID, col string, v *big.Rat) {
	o.rows[row] = true
	c := cell{row, col}
	if o.sums[c] == nil {
		o.sums[c] = new(big.Rat)
	}
	o.sums[c].Add(o.sums[c], v)
}

// Min keeps the smallest v of the column col of row.
func (o *Oracle) Min(row RowID, col string, v *big.Rat) {
	o.rows[row] = true
	if c := (cell{row, col}); o.mins[c] == nil || v.Cmp(o.mins[c]) < 0 {
		o.mins[c] = v
	}
}

// Max keeps the largest v of the column col of row.
func (o *Oracle) Max(row RowID, col string, v *big.Rat) {
	o.rows[row] = true
	if c := (cell{row, col}); o.maxes[c] == nil || v.Cmp(o.maxes[c]) > 0 {
		o.maxes[c] = v
	}
}

// SumFloat adds x to the Float64 sum column col of row, exactly.
func (o *Oracle) SumFloat(row RowID, col string, x float64) {
	o.rows[row] = true
	c := cell{row, col}
	if o.floats[c] == nil {
		o.floats[c] = new(big.Rat)
	}
	o.floats[c].Add(o.floats[c], new(big.Rat).SetFloat64(x))
}

// Avg adds v to the avg column col of row.
func (o *Oracle) Avg(row RowID, col string, v *big.Rat) {
	o.rows[row] = true
	c := cell{row, col}
	if o.avgs[c] == nil {
		o.avgs[c] = &mean{}
	}
	o.avgs[c].sum.Add(&o.avgs[c].sum, v)
	o.avgs[c].n++
}

// MaxString keeps the largest s, byte by byte, of the column col of row.
func (o *Oracle) MaxString(row RowID, col, s string) {
	o.rows[row] = true
	c := cell{row, col}
	if cur, ok := o.texts[c]; !ok || s > cur {
		o.texts[c] = s
	}
}

// Pick keeps the arg that the argMax, argMin or any column col of row returns: from the
// input with the largest (val, tie) for argMax, the smallest for argMin, and the smallest tie
// for any, whose val is nil.
func (o *Oracle) Pick(row RowID, col string, fn catalog.AggFunc, arg string, val *big.Rat, tie Tie) {
	o.rows[row] = true
	c, in := cell{row, col}, &picked{fn: fn, arg: arg, val: val, tie: tie}
	if cur := o.picks[c]; cur == nil || in.before(cur) {
		o.picks[c] = in
	}
}

// before reports whether p wins over q.
func (p *picked) before(q *picked) bool {
	d := 0
	if p.val != nil {
		d = p.val.Cmp(q.val)
	}
	d = cmp.Or(d, cmp.Compare(p.tie.Time, q.tie.Time), strings.Compare(p.tie.ID, q.tie.ID), cmp.Compare(p.tie.Index, q.tie.Index))
	if p.fn == catalog.AggArgMax {
		return d > 0
	}
	return d < 0
}

// Sketch adds v to the uniq or quantile column col of row, whose function f is.
func (o *Oracle) Sketch(row RowID, col string, f *agg.Func, v expr.Value) error {
	o.rows[row] = true
	c := cell{row, col}
	if o.sketches[c] == nil {
		o.sketches[c] = f.New()
	}
	return o.sketches[c].Add(agg.Input{Args: []expr.Value{v}})
}

// UniqExact adds value to the distinct-count column col of row.
func (o *Oracle) UniqExact(row RowID, col, value string) {
	o.rows[row] = true
	c := cell{row, col}
	if o.sets[c] == nil {
		o.sets[c] = map[string]bool{}
	}
	o.sets[c][value] = true
}

// Poison records that rec fails its evaluation in view, which must open view's breaker.
func (o *Oracle) Poison(view string, rec RecordID) {
	if o.poison[view] == nil {
		o.poison[view] = map[RecordID]bool{}
	}
	o.poison[view][rec] = true
}

func (o *Oracle) result() Rows {
	rows := Rows{}
	for id := range o.rows {
		rows[id] = Row{}
	}
	for c, n := range o.counts {
		rows[c.row][c.col] = strconv.FormatInt(n, 10)
	}
	for _, m := range []map[cell]*big.Rat{o.sums, o.mins, o.maxes} {
		for c, v := range m {
			rows[c.row][c.col] = v.RatString()
		}
	}
	for c, set := range o.sets {
		rows[c.row][c.col] = strconv.Itoa(len(set))
	}
	for c, v := range o.floats {
		f, _ := v.Float64()
		rows[c.row][c.col] = FormatFloat(f)
	}
	for c, m := range o.avgs {
		f, _ := m.sum.Float64()
		rows[c.row][c.col] = FormatFloat(f / float64(m.n))
	}
	for c, s := range o.texts {
		rows[c.row][c.col] = s
	}
	for c, p := range o.picks {
		rows[c.row][c.col] = p.arg
	}
	for c, s := range o.sketches {
		rows[c.row][c.col] = sketchText(s)
	}
	return rows
}

// FormatValue formats a value of type t as the oracle does; a time as RFC 3339 in UTC.
func FormatValue(v expr.Value, t catalog.Type) string {
	switch v.Kind() { //nolint:exhaustive // the kinds of the fixture's keys and columns
	case expr.KindUint:
		return strconv.FormatUint(v.AsUint(), 10)
	case expr.KindDecimal:
		return new(big.Rat).SetFrac(v.AsDecimal(), expr.Pow10(t.Scale)).RatString()
	case expr.KindMicros:
		return time.UnixMicro(v.AsMicros()).UTC().Format(time.RFC3339Nano)
	case expr.KindString:
		return v.AsString()
	case expr.KindFloat:
		return FormatFloat(v.AsFloat())
	case expr.KindNull:
		return "NULL"
	}
	return fmt.Sprintf("a value of kind %d", v.Kind())
}

// FormatFloat formats a Float64 value as the oracle does.
func FormatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

func sketchText(s agg.State) string {
	v, err := s.Final()
	switch {
	case err != nil:
		return "error: " + err.Error()
	case v.Kind() == expr.KindNull:
		return "NULL"
	case v.Kind() == expr.KindFloat:
		return FormatFloat(v.AsFloat())
	case v.Kind() == expr.KindUint:
		return strconv.FormatUint(v.AsUint(), 10)
	}
	return fmt.Sprintf("a value of kind %d", v.Kind())
}

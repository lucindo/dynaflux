// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"math/big"
	"strconv"
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
// incremental state, as the reference for P1 and P3.
type Oracle struct {
	rows   map[RowID]bool
	counts map[cell]int64
	sums   map[cell]*big.Rat
	sets   map[cell]map[string]bool
	poison map[string]map[RecordID]bool
}

type cell struct {
	row RowID
	col string
}

func newOracle() *Oracle {
	return &Oracle{
		rows:   map[RowID]bool{},
		counts: map[cell]int64{},
		sums:   map[cell]*big.Rat{},
		sets:   map[cell]map[string]bool{},
		poison: map[string]map[RecordID]bool{},
	}
}

// Count adds one to the count column col of row.
func (o *Oracle) Count(row RowID, col string) {
	o.rows[row] = true
	o.counts[cell{row, col}]++
}

// Sum adds v to the sum column col of row. Sums read as [big.Rat.RatString] prints them.
func (o *Oracle) Sum(row RowID, col string, v *big.Rat) {
	o.rows[row] = true
	c := cell{row, col}
	if o.sums[c] == nil {
		o.sums[c] = new(big.Rat)
	}
	o.sums[c].Add(o.sums[c], v)
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
	for c, s := range o.sums {
		rows[c.row][c.col] = s.RatString()
	}
	for c, set := range o.sets {
		rows[c.row][c.col] = strconv.Itoa(len(set))
	}
	return rows
}

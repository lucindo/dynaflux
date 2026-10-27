// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

import (
	"fmt"
	"math"
	"strconv"

	"github.com/lucindo/dynaflux/internal/catalog"
)

const (
	// tinyBatch is the average batch under which a view's writes rarely combine.
	tinyBatch = 2
	// concurrencyLimit is Lambda's default concurrency quota per account and region.
	concurrencyLimit = 1000
	// partitionReads is the read units per second that one partition takes.
	partitionReads = 3000
	// minSplit is the smallest SPLIT clause.
	minSplit = 2
)

func (est *estimate) streamFindings(w Workload) []finding {
	s, m, c := est.stream, &est.model, &est.cost
	var fs []finding
	if m.Forever {
		perDay := m.Rate * (m.EventKB + itemOverheadKB) * secondsPerDay / kbPerGB / partitionGB
		fs = append(fs, finding{
			subject: s.Name,
			message: fmt.Sprintf("LOG RETENTION FOREVER keeps every event in DynamoDB: at this rate the log gains ~%s partitions a day, "+
				"so batches shrink and projection costs more every day; this report is for -horizon %s",
				num(perDay), FormatHorizon(w.Horizon)),
			hint: "keep a short log (the default is 2 DAY), and use the archive as the permanent record",
		})
	}
	if c.Batch < tinyBatch {
		fs = append(fs, finding{
			subject: s.Name,
			message: fmt.Sprintf("batches average %s %s over %s shards, so view writes rarely combine: "+
				"most events cost a row write in every view", num(c.Batch), plural(c.Batch, "event", "events"), grouped(int64(c.Partitions))),
			hint: "shorten LOG RETENTION, which means fewer shards, or raise runtime.batching_window_seconds",
		})
	}
	window := max(est.store.BatchingWindow.Seconds(), minPollSeconds)
	if perShard := m.Rate / float64(c.Partitions); perShard*window > float64(est.store.BatchSize) {
		fs = append(fs, finding{
			subject: s.Name,
			message: fmt.Sprintf("at ~%s events/s per shard, a %s s batching window would gather ~%s events, but a batch holds at most %d "+
				"(the event source mapping's BatchSize): batches fill before the window ends",
				num(perShard), num(window), num(perShard*window), est.store.BatchSize),
			hint: "a longer runtime.batching_window_seconds won't make batches bigger at this rate",
		})
	}
	if c.Partitions > concurrencyLimit {
		fs = append(fs, finding{
			subject: s.Name,
			message: fmt.Sprintf("~%s shards per region: after an outage, the projector catches up with one invocation per shard, "+
				"over Lambda's default quota of %s concurrent executions, which all of the account's functions in a region share",
				grouped(int64(c.Partitions)), grouped(concurrencyLimit)),
			hint: "shorten LOG RETENTION, or raise the account's Lambda concurrency quota",
		})
	}
	return fs
}

func (est *estimate) viewFindings(i int) []finding {
	m, v := est.model.Views[i], est.cost.Views[i]
	if v.OverItemLimit() {
		hint := "shorten LOG RETENTION on " + est.stream.Name + " (fewer shards), or runtime.prune_window_seconds"
		if n, ok := est.split(i, func(rowKB float64) bool { return rowKB <= itemLimitKB }); ok {
			hint += "; SPLIT " + strconv.Itoa(n) + " keeps each item under the limit"
		}
		return []finding{{
			err:     true,
			subject: v.Name,
			message: fmt.Sprintf("rows reach %s KB, over DynamoDB's %d KB item limit: every write to them would fail", num(v.RowKB), itemLimitKB),
			hint:    hint,
		}}
	}
	var fs []finding
	if f, ok := est.marksFinding(i); ok {
		fs = append(fs, f)
	}
	if !m.Shared {
		return fs
	}
	if writes, reads := est.perItem(i); writes > partitionWrites || reads > partitionReads {
		fs = append(fs, finding{
			subject: v.Name,
			message: fmt.Sprintf("each row takes ~%s write and %s read units per second, over the %s and %s that a DynamoDB partition takes",
				num(writes), num(reads), grouped(partitionWrites), grouped(partitionReads)),
			hint: "GROUP BY a finer key, or SPLIT the rows",
		})
	}
	if m.Sync {
		fs = append(fs, finding{
			subject: v.Name,
			message: fmt.Sprintf("MODE SYNC on shared rows: ~%s appends a second update each row in a transaction in the accepting region, "+
				"and concurrent ones conflict and retry", num(m.Pass*est.model.Rate/float64(est.store.Regions)/m.ActiveRows)),
			hint: "use MODE ASYNC, or add the PRIMARY KEY of " + est.stream.Name + " to GROUP BY",
		})
	}
	return fs
}

// marksFinding warns when high-water marks make a row of views[i] cost more write units
// than its other attributes do.
func (est *estimate) marksFinding(i int) (finding, bool) {
	m, v, stream := est.model.Views[i], est.cost.Views[i], est.stream.Name
	minUnits := math.Ceil(m.BaseKB + float64(m.Uniqs)*hllKB)
	if float64(v.WriteUnits) <= minUnits || (!m.Shared && v.HWMEntries <= 1) {
		return finding{}, false
	}
	why := fmt.Sprintf("one for each shard that wrote the row within the prune window (the stream has %s shards)",
		grouped(int64(est.cost.Partitions)))
	fewer := "shorten LOG RETENTION on " + stream + " (fewer shards), or runtime.prune_window_seconds"
	if !m.Shared {
		why = fmt.Sprintf("one for each change-stream shard that SHARDS %d spreads an entity's events over", est.stream.Shards)
		fewer = "use fewer SHARDS on " + stream
	}
	hint := fewer
	if n, ok := est.split(i, func(rowKB float64) bool { return math.Ceil(rowKB) <= minUnits }); ok {
		hint = fmt.Sprintf("SPLIT %d keeps each item at %s %s, saving ~%s a month per region; queries then read %d items per row",
			n, num(minUnits), plural(minUnits, "write unit", "write units"), dollars(v.Dollars-est.splitDollars(i, n)), n)
		if !m.Shared {
			hint += "; or " + fewer
		}
	}
	return finding{
		subject: v.Name,
		message: fmt.Sprintf("rows reach %s KB with ~%s high-water marks, %s; every write costs %d write units",
			num(v.RowKB), num(v.HWMEntries), why, v.WriteUnits),
		hint: hint,
	}, true
}

// perItem returns the average write and read units per second that one item of a shared
// view takes in a region.
func (est *estimate) perItem(i int) (float64, float64) {
	m, v := est.model.Views[i], est.cost.Views[i]
	items := m.ActiveRows * float64(m.Split)
	return v.Rows.Write / items, v.Rows.Read / items
}

// split returns the smallest SPLIT, over the view's own, under which a row of views[i]
// fits. MODE SYNC can't be split.
func (est *estimate) split(i int, fits func(rowKB float64) bool) (int, bool) {
	if est.model.Views[i].Sync {
		return 0, false
	}
	for n := max(minSplit, est.model.Views[i].Split+1); n <= catalog.MaxSplit; n++ {
		if fits(est.splitRowKB(i, n)) {
			return n, true
		}
	}
	return 0, false
}

// splitRowKB returns the size of an item of views[i] under SPLIT n: its high-water marks
// shrink in proportion, down to the one mark of an entity's row.
func (est *estimate) splitRowKB(i, n int) float64 {
	m, v := est.model.Views[i], est.cost.Views[i]
	marks := v.HWMEntries * float64(m.Split) / float64(n)
	if !m.Shared {
		marks = max(1, marks)
	}
	return m.BaseKB + float64(m.Uniqs)*hllKB + est.store.HWMEntryKB*marks
}

// splitDollars returns the monthly cost of views[i] in a region under SPLIT n. A row's
// units scale with the units of its write and its read.
func (est *estimate) splitDollars(i, n int) float64 {
	m, v := est.model.Views[i], est.cost.Views[i]
	rowKB := est.splitRowKB(i, n)
	writes := v.Rows.Write * math.Ceil(rowKB) / float64(v.WriteUnits)
	reads := v.Rows.Read * math.Ceil(rowKB/readUnitKB) / float64(v.ReadUnits)
	return m.Capacity.monthly(Units{Write: writes + v.Markers.Write, Read: reads + v.Markers.Read})
}

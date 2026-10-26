// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

import (
	"fmt"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
)

const (
	conflictPercent = (conflictFactor - 1) * percent
	// attributeLabels are the columns of a row attribute that aren't numbers: its name and
	// what it holds.
	attributeLabels = 2
)

func (e *explainer) viewReport(v *catalog.View) error {
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
	e.header("view "+v.Name+", from stream "+s.Name, est.flags(e.w, i))
	e.viewLayout(&est, i)
	e.rowSection(&est, i)
	e.viewCost(&est, i)
	e.viewNotes(est.views[i].version)
	e.findings = append(e.findings, est.viewFindings(i)...)
	return nil
}

func (e *explainer) viewLayout(est *estimate, i int) {
	vp, m := &est.views[i], est.model.Views[i]
	e.section(regionsTitle("layout", e.cfg.Regions))
	e.line("table", e.cfg.Naming().ViewTable(vp.view, vp.version.Number)+": regional, "+capacityText(m.Capacity))
	mode := "ASYNC: the projector applies events in batches"
	if m.Sync {
		mode = "SYNC: in the region that accepts an append, its rows change in the append's transaction, " +
			"at twice the write units; other regions project it in batches"
	}
	e.line("mode", mode)
	group := "GROUP BY (" + keyNames(vp.version.Keys) + ")"
	pk := strings.Join(est.stream.PrimaryKey, ", ")
	if m.Shared {
		e.line("rows", fmt.Sprintf("shared: %s leaves out %s, the PRIMARY KEY of %s, so any shard can write any row; ~%s active",
			group, pk, est.stream.Name, whole(m.ActiveRows)))
	} else {
		e.line("rows", fmt.Sprintf("a row per entity: %s holds %s, the PRIMARY KEY of %s", group, pk, est.stream.Name))
	}
	if m.Split > 1 {
		e.line("split", fmt.Sprintf("SPLIT %d: each row is %d items, chosen by shard; a query reads all %d", m.Split, m.Split, m.Split))
	}
	e.line("backfill", backfillText(vp.version))
}

func backfillText(vv *catalog.ViewVersion) string {
	var additive []string
	for _, a := range vv.Aggregates {
		if !a.Fn.Idempotent() && !slices.Contains(additive, string(a.Fn)) {
			additive = append(additive, string(a.Fn))
		}
	}
	if len(additive) == 0 {
		return "no fence: every aggregate is idempotent"
	}
	return "fenced handoff: " + andList(additive) + " " + plural(float64(len(additive)), "is", "are") + " additive"
}

// rowSection writes the attributes of a row of views[i] and their sizes.
func (e *explainer) rowSection(est *estimate, i int) {
	vp, v := &est.views[i], est.cost.Views[i]
	e.section(fmt.Sprintf("row, estimated (key and state encodings aren't final yet; String values are taken as %d bytes)", unknownLenBytes))
	t := table{left: attributeLabels}
	for _, a := range vp.layout {
		what, size := a.what, bytesText(float64(a.bytes))
		switch a.state {
		case plainState:
		case hllState:
			size = num(hllKB) + " KB at most"
		case quantileState:
			size = "not estimated"
		}
		if a.name == catalog.AttrHighWaterMarks {
			what = fmt.Sprintf("~%s high-water %s of ~%s B", num(v.HWMEntries), plural(v.HWMEntries, "mark", "marks"), whole(hwmEntryBytes))
			size = bytesText(float64(a.bytes) + hwmEntryBytes*v.HWMEntries)
		}
		t.add("  "+a.name, what, size)
	}
	t.add("  total", fmt.Sprintf("%d write %s, %d read %s; the item limit is %d KB",
		v.WriteUnits, plural(float64(v.WriteUnits), "unit", "units"),
		v.ReadUnits, plural(float64(v.ReadUnits), "unit", "units"), itemLimitKB), bytesText(v.RowKB*bytesPerKB))
	t.write(&e.b)
}

// bytesText formats a size in bytes, in KB from 1 KB up.
func bytesText(n float64) string {
	if n < bytesPerKB {
		return whole(n) + " B"
	}
	return num(n/bytesPerKB) + " KB"
}

// viewCost writes the units and dollars of views[i].
func (e *explainer) viewCost(est *estimate, i int) {
	m, v, rate := est.model.Views[i], est.cost.Views[i], est.model.Rate
	e.section("per event, per region")
	batch := fmt.Sprintf("a batch of %s %s updates %s %s", num(est.cost.Batch), plural(est.cost.Batch, "event", "events"),
		num(v.RowsPerBatch), plural(v.RowsPerBatch, "row", "rows"))
	if m.Sync {
		batch += ", as the projector skips the appends that this region accepted"
	}
	e.line("rows", fmt.Sprintf("%s write units and %s read units: %s, and %s%% of writes conflict and retry",
		units(v.Rows.Write/rate), units(v.Rows.Read/rate), batch, num(conflictPercent)))
	if m.UniqExacts > 0 {
		e.line("markers", fmt.Sprintf("%s write units and %s read units: a marker read per uniqExact per source row, and a write per new value (%s%%)",
			units(v.Markers.Write/rate), units(v.Markers.Read/rate), num(m.UniqNew*percent)))
	}
	if m.Shared {
		writes, reads := est.perItem(i)
		e.line("per row", fmt.Sprintf("~%s write and %s read units per second on average; a partition takes %s and %s",
			num(writes), num(reads), grouped(partitionWrites), grouped(partitionReads)))
	}
	if v.OverItemLimit() {
		e.line("monthly", fmt.Sprintf("none: writes to rows over %d KB fail", itemLimitKB))
		return
	}
	regions := float64(est.store.Regions)
	e.line("monthly", fmt.Sprintf("DynamoDB requests %s per region, %s for %d %s",
		dollars(v.Dollars), dollars(v.Dollars*regions), est.store.Regions, plural(regions, "region", "regions")))
}

// viewNotes writes what a reader of the view should know about its values and size.
func (e *explainer) viewNotes(vv *catalog.ViewVersion) {
	var notes []string
	for _, a := range vv.Aggregates {
		switch {
		case (a.Fn == catalog.AggSum || a.Fn == catalog.AggAvg) && a.Args[0].Type.Kind == catalog.KindFloat64:
			notes = append(notes, a.Name+": a Float64 "+string(a.Fn)+" converges up to floating-point rounding: its last digits can differ "+
				"between regions, and between live projection and a backfill. Use Decimal for exact totals.")
		case a.Fn == catalog.AggQuantile || a.Fn == catalog.AggQuantileDD:
			notes = append(notes, a.Name+": the row size leaves out its sketch, whose size isn't fixed yet.")
		}
	}
	if len(notes) > 0 {
		e.b.WriteString("\n")
	}
	for _, n := range notes {
		e.b.WriteString("note " + n + "\n")
	}
}

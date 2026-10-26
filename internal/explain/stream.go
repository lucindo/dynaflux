// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
)

const (
	defaultTarget  = 0.7 // the autoscaling target utilization that dynaflux.yaml defaults to
	secondsPerHour = 3600
)

func (e *explainer) streamReport(s *catalog.Stream) error {
	est, err := e.estimate(s)
	if err != nil {
		return err
	}
	e.header("stream "+s.Name, est.flags(e.w, -1))
	e.streamLayout(&est)
	e.perEvent(&est)
	if err = e.monthly(&est); err != nil {
		return err
	}
	e.findings = append(e.findings, est.streamFindings(e.w)...)
	for i := range est.views {
		e.findings = append(e.findings, est.viewFindings(i)...)
	}
	return nil
}

func (e *explainer) streamLayout(est *estimate) {
	s, c := est.stream, &est.cost
	e.section(regionsTitle("layout", e.cfg.Regions))
	e.line("table", e.cfg.Naming().StreamTable(s.Name)+": Global Table, "+capacityText(est.model.Capacity))
	pk := strings.Join(s.PrimaryKey, ", ")
	if s.Shards > 1 {
		pk += " and a shard of " + strconv.Itoa(s.Shards)
	}
	e.line("key", "pk "+pk+"; sk "+catalog.ColumnEventTime+", "+catalog.ColumnEventID)
	log := "LOG RETENTION " + retentionText(s.LogRetention)
	if est.model.Forever {
		log += ", evaluated at --horizon " + FormatHorizon(e.w.Horizon)
	}
	e.line("log", fmt.Sprintf("%s: ~%s GB in ~%s partitions, each a change-stream shard",
		log, num(c.StoredGB), grouped(int64(c.Partitions))))
	e.line("projector", fmt.Sprintf("batching window %s s, batches of at most %d events: ~%s invocations/s, %s %s each on average",
		strconv.Itoa(e.cfg.Runtime.BatchingWindowSeconds), est.store.BatchSize, num(c.Invocations), num(c.Batch), plural(c.Batch, "event", "events")))
	e.line("marks", fmt.Sprintf("a view row keeps a shard's high-water mark for %d s after the shard's progress passes it (runtime.prune_window_seconds)",
		e.cat.PruneWindowSeconds))
	e.line("archive", archiveText(s.Archive))
}

// regionsTitle titles a section that holds in every region.
func regionsTitle(title string, regions []string) string {
	if len(regions) == 1 {
		return title + ", in " + regions[0]
	}
	return fmt.Sprintf("%s, in each of %d regions (%s)", title, len(regions), strings.Join(regions, ", "))
}

func archiveText(a *catalog.Archive) string {
	switch {
	case a == nil:
		return "off"
	case a.Events == catalog.ArchiveEventsLocal:
		return "Firehose to Iceberg; each region archives the events it accepted"
	}
	return "Firehose to Iceberg in " + strings.Join(a.Regions, ", ")
}

// perEvent writes the request units of an event in one region, line by line.
func (e *explainer) perEvent(est *estimate) {
	c, rate := &est.cost, est.model.Rate
	t := table{left: 1}
	t.add("per event, per region", "write units", "read units")
	put := "  event put, local or replicated"
	if slices.ContainsFunc(est.model.Views, func(v View) bool { return v.Sync }) {
		put += "; a transaction where accepted (SYNC views)"
	}
	t.add(put, units(c.EventWrites/rate), "")
	if c.TTLWrites > 0 {
		t.add("  TTL delete, replicated from the region that expires it", units(c.TTLWrites/rate), "")
	}
	for i, v := range c.Views {
		m := est.model.Views[i]
		t.add("  "+v.Name+", "+classText(m), units(v.Rows.Write/rate), units(v.Rows.Read/rate))
		if m.UniqExacts > 0 {
			t.add("    uniqExact markers", units(v.Markers.Write/rate), units(v.Markers.Read/rate))
		}
	}
	t.add("  progress item, one per batch", units(c.ProgressWrites/rate), "")
	u := c.Units()
	t.add("  total", units(u.Write/rate), units(u.Read/rate))
	t.add("  per second, all events", whole(u.Write), whole(u.Read))
	e.b.WriteString("\n")
	t.write(&e.b)
}

func classText(m View) string {
	text := "a row per entity"
	if m.Shared {
		text = "shared rows, " + whole(m.ActiveRows) + " active"
	}
	if m.Split > 1 {
		text += ", SPLIT " + strconv.Itoa(m.Split)
	}
	if m.Sync {
		text += ", SYNC"
	}
	return text
}

// monthly writes the monthly cost by line, per region and for every region.
func (e *explainer) monthly(est *estimate) error {
	c := &est.cost
	regions := float64(est.store.Regions)
	t := table{left: 1}
	t.add("monthly", "per region", fmt.Sprintf("%d %s", est.store.Regions, plural(regions, "region", "regions")))
	add := func(label string, x float64) { t.add(label, dollars(x), dollars(x*regions)) }
	// A view whose writes fail costs nothing, and the error below reports it.
	failing := 0.0
	for _, v := range c.Views {
		if v.OverItemLimit() {
			failing += v.Dollars
		}
	}
	requests := c.Monthly.Requests - failing
	add("  DynamoDB requests", requests)
	add("    stream and meta tables", c.StreamDollars+c.MetaDollars)
	for _, v := range c.Views {
		if v.OverItemLimit() {
			t.add("    "+v.Name, "fails", "fails")
		} else {
			add("    "+v.Name, v.Dollars)
		}
	}
	add("  DynamoDB storage of the log", c.Monthly.Storage)
	add("  Lambda, the projector", c.Monthly.Lambda)
	add("  Firehose", c.Monthly.Firehose)
	add("  S3 archive, year-1 average", c.Monthly.S3)
	add("  replication transfer", c.Monthly.Transfer)
	add("  total", c.Monthly.Sum()-failing)
	e.b.WriteString("\n")
	t.write(&e.b)
	if err := e.capacityAdvice(est, requests); err != nil {
		return err
	}
	e.b.WriteString("  not modeled: query reads, view and marker storage, backfills, archive compaction, CloudWatch\n")
	return nil
}

// capacityAdvice compares the monthly requests of a region with what provisioned capacity
// would cost.
func (e *explainer) capacityAdvice(est *estimate, requests float64) error {
	provisioned, err := est.provisionedRequests()
	if err != nil || provisioned >= requests {
		return err
	}
	fmt.Fprintf(&e.b, "  provisioned capacity at a %s%% target would cut requests to %s per region (-%s%%), "+
		"but costs more than on-demand if it must cover peaks over %s times the average\n",
		num(defaultTarget*percent), dollars(provisioned), whole((1-provisioned/requests)*percent), num(breakEven(defaultTarget)))
	return nil
}

// provisionedRequests returns the monthly cost of requests in a region with every
// on-demand table provisioned at the default target.
func (est *estimate) provisionedRequests() (float64, error) {
	p := Capacity{Provisioned: true, ReadTarget: defaultTarget, WriteTarget: defaultTarget}
	store, s := est.store, est.model
	provision := func(c *Capacity) {
		if !c.Provisioned {
			*c = p
		}
	}
	provision(&store.Meta)
	provision(&s.Capacity)
	s.Views = slices.Clone(s.Views)
	for i := range s.Views {
		provision(&s.Views[i].Capacity)
	}
	c, err := Estimate(store, s)
	if err != nil {
		return 0, err
	}
	requests := c.Monthly.Requests
	for i, v := range est.cost.Views {
		if v.OverItemLimit() {
			requests -= c.Views[i].Dollars
		}
	}
	return requests, nil
}

// breakEven is the peak-to-average traffic ratio at which provisioned capacity at target
// costs as much as on-demand: autoscaling keeps capacity for the peak.
func breakEven(target float64) float64 {
	return wruPrice * secondsPerHour * target / wcuHour
}

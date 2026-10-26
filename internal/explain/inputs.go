// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

import (
	"slices"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/infra"
)

const (
	day          = 24 * time.Hour
	daysPerWeek  = 7
	daysPerYear  = 365
	monthsPerQtr = 3
	percent      = 100
)

// streamPlan is a stream with the model inputs it maps to.
type streamPlan struct {
	stream *catalog.Stream
	store  Store
	model  Stream
	// views are in the order of model.Views.
	views []viewPlan
}

// viewPlan is a view version that a stream feeds.
type viewPlan struct {
	view    string
	version *catalog.ViewVersion
	layout  []attribute
}

func newStreamPlan(cfg *config.Config, cat *catalog.Catalog, w Workload, s *catalog.Stream) streamPlan {
	retention, forever := w.Horizon, true
	if p, ok := s.LogRetention.Period(); ok {
		retention, forever = duration(p), false
	}
	p := streamPlan{
		stream: s,
		store: Store{
			Regions:        len(cat.Regions),
			BatchingWindow: time.Duration(cfg.Runtime.BatchingWindowSeconds) * time.Second,
			BatchSize:      infra.ProjectorBatchSize,
			HWMWindow:      time.Duration(cat.PruneWindowSeconds) * time.Second,
			MemoryMB:       cfg.Runtime.MemoryMB,
			Arch:           Arch(cfg.Runtime.Arch),
			Meta:           capacity(cfg.Defaults.Capacity),
		},
		model: Stream{
			Rate:          w.Rate,
			EventKB:       w.EventKB,
			Retention:     retention,
			Forever:       forever,
			Shards:        s.Shards,
			ArchiveCopies: archiveCopies(s.Archive),
			Capacity:      tableCapacity(cfg, s.Name),
		},
	}
	for i := range cat.Views {
		for j := range cat.Views[i].Versions {
			if cat.Views[i].Versions[j].Stream == s.Name {
				p.addView(cfg, w, &cat.Views[i], &cat.Views[i].Versions[j])
			}
		}
	}
	return p
}

func (p *streamPlan) addView(cfg *config.Config, w Workload, v *catalog.View, vv *catalog.ViewVersion) {
	layout := rowLayout(vv)
	m := View{
		Name:    v.Name,
		Shared:  shared(p.stream, vv),
		BaseKB:  float64(rowBytes(layout)) / bytesPerKB,
		UniqNew: w.uniqNew(v.Name),
		Pass:    w.pass(v.Name),
		Split:   vv.Split,
		Sync:    vv.Mode == catalog.ModeSync,
		// A table override for a view applies to the table of every version.
		Capacity: tableCapacity(cfg, v.Name),
	}
	if len(v.Versions) > 1 {
		m.Name = catalog.VersionID(v.Name, vv.Number)
	}
	if m.Shared {
		m.ActiveRows = w.rows(v.Name)
	}
	for _, a := range vv.Aggregates {
		if a.Fn == catalog.AggUniq {
			m.Uniqs++
		}
		if a.Fn == catalog.AggUniqExact {
			m.UniqExacts++
		}
	}
	p.views = append(p.views, viewPlan{view: v.Name, version: vv, layout: layout})
	p.model.Views = append(p.model.Views, m)
}

// shared reports whether any delivery unit can write any row of vv: its keys leave out a
// column of the stream's PRIMARY KEY. Otherwise each row belongs to one entity, whose
// events all come through one delivery unit, or one per SHARDS shard.
func shared(s *catalog.Stream, vv *catalog.ViewVersion) bool {
	return slices.ContainsFunc(s.PrimaryKey, func(col string) bool {
		return !slices.ContainsFunc(vv.Keys, func(k catalog.NamedExpr) bool {
			return k.Expr.Op == catalog.OpColumn && k.Expr.Name == col
		})
	})
}

// duration returns how long i lasts, with the 730-hour months and 365-day years of the
// price model.
func duration(i catalog.Interval) time.Duration {
	switch i.Unit {
	case catalog.UnitSecond:
		return time.Duration(i.N) * time.Second
	case catalog.UnitMinute:
		return time.Duration(i.N) * time.Minute
	case catalog.UnitHour:
		return time.Duration(i.N) * time.Hour
	case catalog.UnitDay:
		return time.Duration(i.N) * day
	case catalog.UnitWeek:
		return time.Duration(i.N*daysPerWeek) * day
	case catalog.UnitMonth:
		return time.Duration(i.N*monthHours) * time.Hour
	case catalog.UnitQuarter:
		return time.Duration(i.N*monthsPerQtr*monthHours) * time.Hour
	case catalog.UnitYear:
		return time.Duration(i.N*daysPerYear) * day
	}
	return 0
}

func archiveCopies(a *catalog.Archive) int {
	switch {
	case a == nil:
		return 0
	case a.Events == catalog.ArchiveEventsLocal:
		return 1
	}
	return len(a.Regions)
}

// tableCapacity returns the capacity of the table of the stream or view called name.
func tableCapacity(cfg *config.Config, name string) Capacity {
	if i := slices.IndexFunc(cfg.Tables, func(t config.Table) bool { return t.Name == name }); i >= 0 {
		return capacity(cfg.Tables[i].Capacity)
	}
	return capacity(cfg.Defaults.Capacity)
}

func capacity(c config.Capacity) Capacity {
	if c.Mode != config.CapacityProvisioned {
		return Capacity{}
	}
	return Capacity{
		Provisioned: true,
		ReadTarget:  float64(c.Autoscaling.Read.TargetPercent) / percent,
		WriteTarget: float64(c.Autoscaling.Write.TargetPercent) / percent,
	}
}

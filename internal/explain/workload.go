// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
)

// Defaults of the per-view workload flags, which are costs.py's.
const (
	DefaultActiveRows = 1000 // --rows
	DefaultUniqNew    = 0.3  // --uniq-new
	DefaultPass       = 1    // --pass
)

// maxRate bounds --rate far above what a DynamoDB table takes, which keeps the counts the
// model derives from it, such as partitions, within int.
const maxRate = 1e7

// Workload is the traffic that explain assumes, which neither the SQL nor dynaflux.yaml
// says. Its fields are the flags of dynaflux explain.
type Workload struct {
	// Rate is --rate: the average number of events appended per second to the explained
	// stream, all regions together.
	Rate float64
	// EventKB is --event-kb: the average size of an event item, in KB of 1,024 bytes.
	EventKB float64
	// Rows is --rows view=N: how many rows a shared view writes to at a time, such as the
	// pages visited today for GROUP BY page, day. Views it leaves out get DefaultActiveRows.
	Rows map[string]float64
	// UniqNew is --uniq-new view=F: the fraction of source rows that bring a uniqExact
	// aggregate of the view a new value. Views it leaves out get DefaultUniqNew.
	UniqNew map[string]float64
	// Pass is --pass view=F: the source rows per event, which is the fraction of events that
	// WHERE keeps times the ARRAY JOIN fan-out. Views it leaves out get DefaultPass.
	Pass map[string]float64
	// Horizon is --horizon: the age at which to evaluate a stream kept FOREVER.
	Horizon time.Duration
}

// DefaultWorkload returns the workload without flags: costs.py's defaults.
func DefaultWorkload() Workload {
	const rate, eventKB, horizonDays = 1000, 1, 365
	return Workload{Rate: rate, EventKB: eventKB, Horizon: horizonDays * day}
}

func (w Workload) rows(view string) float64 {
	return valueOr(w.Rows, view, DefaultActiveRows)
}

func (w Workload) uniqNew(view string) float64 {
	return valueOr(w.UniqNew, view, DefaultUniqNew)
}

func (w Workload) pass(view string) float64 {
	return valueOr(w.Pass, view, DefaultPass)
}

func valueOr(m map[string]float64, key string, fallback float64) float64 {
	if v, ok := m[key]; ok {
		return v
	}
	return fallback
}

// check reports a flag value that is out of range or names a view that can't take it.
func (w Workload) check(cat *catalog.Catalog) error {
	switch {
	case !(w.Rate > 0 && w.Rate <= maxRate):
		return fmt.Errorf("--rate must be over 0 and at most %s events per second, not %v", grouped(maxRate), w.Rate)
	case !finite(w.EventKB) || w.EventKB <= 0 || w.EventKB > itemLimitKB:
		return fmt.Errorf("--event-kb must be over 0 and at most %d, the DynamoDB item limit, not %v", itemLimitKB, w.EventKB)
	case w.Horizon <= 0:
		return fmt.Errorf("--horizon must be positive, not %v", w.Horizon)
	}
	flags := []struct {
		name   string
		values map[string]float64
		check  func(*catalog.Stream, *catalog.ViewVersion, float64) string
	}{
		{"--rows", w.Rows, checkRows},
		{"--uniq-new", w.UniqNew, checkUniqNew},
		{"--pass", w.Pass, checkPass},
	}
	for _, f := range flags {
		for _, name := range slices.Sorted(maps.Keys(f.values)) {
			if err := checkViewFlag(cat, f.name, name, f.values[name], f.check); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkViewFlag checks name=value of a per-view flag against every version of the view.
func checkViewFlag(cat *catalog.Catalog, flag, name string, value float64,
	check func(*catalog.Stream, *catalog.ViewVersion, float64) string,
) error {
	v, ok := cat.View(name)
	if !ok {
		msg := fmt.Sprintf("%s %s=%v: no view named %s", flag, name, value, name)
		var views []string
		for _, cv := range cat.Views {
			views = append(views, cv.Name)
		}
		if s, found := diag.Suggest(name, views); found {
			msg += "; did you mean " + s + "?"
		}
		return errors.New(msg)
	}
	for i := range v.Versions {
		vv := &v.Versions[i]
		s, _ := cat.Stream(vv.Stream)
		if problem := check(s, vv, value); problem != "" {
			return fmt.Errorf("%s %s=%v: %s", flag, name, value, problem)
		}
	}
	return nil
}

func checkRows(s *catalog.Stream, vv *catalog.ViewVersion, rows float64) string {
	if !shared(s, vv) {
		return "the view has a row per entity of " + s.Name + "; --rows is for views whose GROUP BY leaves out the stream's PRIMARY KEY"
	}
	if !finite(rows) || rows < 1 {
		return "the number of active rows must be at least 1"
	}
	return ""
}

func checkUniqNew(_ *catalog.Stream, vv *catalog.ViewVersion, fraction float64) string {
	if !slices.ContainsFunc(vv.Aggregates, func(a catalog.ViewAggregate) bool { return a.Fn == catalog.AggUniqExact }) {
		return "the view has no uniqExact aggregate"
	}
	if !(fraction >= 0 && fraction <= 1) {
		return "the fraction must be from 0 to 1"
	}
	return ""
}

func checkPass(_ *catalog.Stream, vv *catalog.ViewVersion, pass float64) string {
	switch {
	case vv.Where == nil && vv.ArrayJoin == nil:
		return "the view has no WHERE or ARRAY JOIN, so each event is one source row"
	case vv.ArrayJoin == nil && !(pass > 0 && pass <= 1):
		return "the source rows per event must be over 0 and at most 1 without ARRAY JOIN"
	case !(pass > 0 && pass <= catalog.MaxArrayLen):
		return fmt.Sprintf("the source rows per event must be over 0 and at most %d, the most elements an array holds", catalog.MaxArrayLen)
	}
	return ""
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain_test

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/explain"
)

// referencePath holds every scenario of costs.py with its intermediates; capture.py, next to
// it, regenerates it.
const referencePath = "testdata/reference.json"

type reference struct {
	Scenarios []scenario `json:"scenarios"`
}

type scenario struct {
	Name          string             `json:"name"`
	Inputs        inputs             `json:"inputs"`
	Intermediates map[string]float64 `json:"intermediates"`
	PerRegion     map[string]float64 `json:"per_region"`
	Total         float64            `json:"total"`
	OverItemLimit bool               `json:"over_item_limit"`
}

// inputs are the arguments of costs.py's scenario().
type inputs struct {
	EventsS         float64 `json:"events_s"`
	EventKB         float64 `json:"event_kb"`
	Regions         int     `json:"regions"`
	RetentionDays   int     `json:"retention_days"`
	ArchiveRegions  *int    `json:"archive_regions"`
	WindowS         float64 `json:"window_s"`
	Prune           string  `json:"prune"`
	Provisioned     bool    `json:"provisioned"`
	Compression     float64 `json:"compression"`
	HotRows         float64 `json:"hot_rows"`
	UniqNewFraction float64 `json:"uniq_new_fraction"`
	RowSplit        int     `json:"row_split"`
	LifetimeS       float64 `json:"lifetime_s"`
}

// scenarioA is costs.py's scenario A: its defaults with a 2-day log.
func scenarioA() inputs {
	return inputs{
		EventsS: 1000, EventKB: 1, Regions: 3, RetentionDays: 2, WindowS: 1, Prune: "progress",
		Compression: 6, HotRows: 1000, UniqNewFraction: 0.3, RowSplit: 1, LifetimeS: 120,
	}
}

// model expresses a costs.py scenario as model input. Its three typical views are a shared
// count per page per day, a per-user argMax, and a per-user-per-day count with a uniqExact.
// costs.py bills the per-user rows one write unit each, which any base size up to 0.97 KB
// gives.
func model(t *testing.T, in inputs) (explain.Store, explain.Stream) {
	t.Helper()
	hwmWindow := time.Duration(in.LifetimeS * float64(time.Second))
	switch in.Prune {
	case "progress":
	case "25h":
		hwmWindow = 25 * time.Hour
	default:
		t.Fatalf("unknown prune %q", in.Prune)
	}
	if in.Compression != 6 {
		t.Fatalf("compression %v: the model has 6", in.Compression)
	}
	archive := in.Regions
	if in.ArchiveRegions != nil {
		archive = *in.ArchiveRegions
	}
	capacity := explain.Capacity{Provisioned: in.Provisioned, ReadTarget: 0.7, WriteTarget: 0.7}
	store := explain.Store{
		Regions:        in.Regions,
		BatchingWindow: time.Duration(in.WindowS * float64(time.Second)),
		BatchSize:      100,
		HWMWindow:      hwmWindow,
		MemoryMB:       1024,
		Arch:           explain.ArchARM64,
		Meta:           capacity,
	}
	stream := explain.Stream{
		Rate:          in.EventsS,
		EventKB:       in.EventKB,
		Retention:     time.Duration(in.RetentionDays) * 24 * time.Hour,
		Shards:        1,
		ArchiveCopies: archive,
		Capacity:      capacity,
		Views: []explain.View{
			{Name: "page_daily", Shared: true, ActiveRows: in.HotRows, BaseKB: 0.3, Pass: 1, Split: in.RowSplit, Capacity: capacity},
			{Name: "user_latest", BaseKB: 0.3, Pass: 1, Split: 1, Capacity: capacity},
			{Name: "user_daily", BaseKB: 0.3, UniqExacts: 1, UniqNew: in.UniqNewFraction, Pass: 1, Split: 1, Capacity: capacity},
		},
	}
	return store, stream
}

func estimate(t *testing.T, store explain.Store, stream explain.Stream) explain.StreamCost {
	t.Helper()
	c, err := explain.Estimate(store, stream)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}
	return c
}

func loadReference(t *testing.T) []scenario {
	t.Helper()
	data, err := os.ReadFile(referencePath)
	if err != nil {
		t.Fatal(err)
	}
	var ref reference
	if err = json.Unmarshal(data, &ref); err != nil {
		t.Fatalf("%s: %v", referencePath, err)
	}
	if len(ref.Scenarios) == 0 {
		t.Fatalf("%s has no scenarios", referencePath)
	}
	return ref.Scenarios
}

// near fails unless got is within a relative tolerance of want.
func near(t *testing.T, name string, got, want, tolerance float64) {
	t.Helper()
	if !(math.Abs(got-want) <= tolerance*math.Abs(want)) { // so that NaN fails
		t.Errorf("%s = %v, want %v (relative error %.2g)", name, got, want, math.Abs(got-want)/math.Abs(want))
	}
}

// nearAll checks each value of got against the reference value of the same name.
func nearAll(t *testing.T, got, want map[string]float64, tolerance float64) {
	t.Helper()
	for name, value := range got {
		w, ok := want[name]
		if !ok {
			t.Fatalf("%q isn't in %s", name, referencePath)
		}
		near(t, name, value, w, tolerance)
	}
}

func TestEstimateMatchesCostsPy(t *testing.T) {
	t.Parallel()
	for _, sc := range loadReference(t) {
		t.Run(sc.Name, func(t *testing.T) {
			t.Parallel()
			store, stream := model(t, sc.Inputs)
			got := estimate(t, store, stream)
			hot := got.Views[0]
			var rowsPerBatch float64
			var rows, markers explain.Units
			for _, v := range got.Views {
				rowsPerBatch += v.RowsPerBatch
				rows.Write += v.Rows.Write
				rows.Read += v.Rows.Read
				markers.Write += v.Markers.Write
				markers.Read += v.Markers.Read
			}
			units := got.Units()
			// The tolerance absorbs last-bit differences from libm and fused multiply-add. It
			// holds for the dollars too, so that a drifted price or constant fails.
			const tolerance = 1e-9
			intermediates := map[string]float64{
				"stored_gb": got.StoredGB, "partitions": float64(got.Partitions),
				"invocations": got.Invocations, "batch": got.Batch,
				"v1_rows": hot.RowsPerBatch, "rows_per_batch": rowsPerBatch,
				"hwm_entries": hot.HWMEntries, "hot_row_kb": hot.RowKB,
				"hot_wru": float64(hot.WriteUnits), "hot_rru": float64(hot.ReadUnits),
				"w_events": got.EventWrites, "w_ttl": got.TTLWrites, "w_progress": got.ProgressWrites,
				"w_views": rows.Write, "r_views": rows.Read,
				"w_markers": markers.Write, "r_markers": markers.Read,
				"wps": units.Write, "rps": units.Read,
			}
			nearAll(t, intermediates, sc.Intermediates, tolerance)
			lines := map[string]float64{
				"DynamoDB requests":     got.Monthly.Requests,
				"DynamoDB storage":      got.Monthly.Storage,
				"Lambda (projector)":    got.Monthly.Lambda,
				"Firehose":              got.Monthly.Firehose,
				"S3 archive (yr-1 avg)": got.Monthly.S3,
				"Replication transfer":  got.Monthly.Transfer,
			}
			nearAll(t, lines, sc.PerRegion, tolerance)
			near(t, "total", got.Total, sc.Total, tolerance)
			if hot.OverItemLimit() != sc.OverItemLimit {
				t.Errorf("OverItemLimit() = %v for a %.1f KB row, want %v", hot.OverItemLimit(), hot.RowKB, sc.OverItemLimit)
			}
		})
	}
}

// Each table is billed for its own units, by its own capacity mode. The figures are the model
// design's for scenario A.
func TestEstimateBillsEachTable(t *testing.T) {
	t.Parallel()
	store, stream := model(t, scenarioA())
	got := estimate(t, store, stream)
	dollars := map[string]float64{"stream and meta tables": got.StreamDollars + got.MetaDollars}
	for _, v := range got.Views {
		dollars[v.Name] = v.Dollars
	}
	want := map[string]float64{"stream and meta tables": 2770, "page_daily": 2116, "user_latest": 2168, "user_daily": 2989}
	for name, w := range want {
		if math.Round(dollars[name]) != w {
			t.Errorf("%s: $%.2f a month, want $%.0f", name, dollars[name], w)
		}
	}
}

// A SYNC view moves the accepting region's share of the work, a third in scenario A, into
// the append's transaction, where writes cost twice the units.
func TestEstimateSyncView(t *testing.T) {
	t.Parallel()
	t.Run("shared view", func(t *testing.T) {
		t.Parallel()
		store, stream := model(t, scenarioA())
		async := estimate(t, store, stream)
		stream.Views[0].Sync = true
		sync := estimate(t, store, stream)
		// A worked example from the model's design.
		writes, reads := sync.Units().Write-async.Units().Write, sync.Units().Read-async.Units().Read
		if math.Round(writes) != 715 || math.Round(reads) != 15 {
			t.Errorf("SYNC adds %.1f write and %.1f read units per second, want 715 and 15", writes, reads)
		}
		near(t, "added monthly requests", sync.Monthly.Requests-async.Monthly.Requests, 1180, 0.01)
	})
	t.Run("uniqExact markers", func(t *testing.T) {
		t.Parallel()
		store, stream := model(t, scenarioA())
		stream.Views[2].Sync = true
		markers := estimate(t, store, stream).Views[2].Markers
		// 300 new values per second: 200 put by the projector, 100 in transactions at 2 units.
		near(t, "marker writes", markers.Write, 400, 1e-12)
		near(t, "marker reads", markers.Read, 1000, 1e-12)
	})
}

func TestEstimateRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(*explain.Stream)
		view   int // the view to check, in model's order
		// Scenario A has 20 partitions and batches of 50 events.
		hwmEntries, rowsPerBatch, markerReads float64
		writeUnits                            int
	}{
		{
			name:       "per-entity row written from one delivery unit",
			change:     func(*explain.Stream) {},
			view:       1,
			hwmEntries: 1, rowsPerBatch: 50, writeUnits: 1,
		},
		{
			name:       "SHARDS spreads an entity over as many delivery units",
			change:     func(s *explain.Stream) { s.Shards = 8 },
			view:       1,
			hwmEntries: 8, rowsPerBatch: 50, writeUnits: 1,
		},
		{
			name:       "SHARDS beyond the partitions",
			change:     func(s *explain.Stream) { s.Shards = 64; s.Views[1].BaseKB = 0.5 },
			view:       1,
			hwmEntries: 20, rowsPerBatch: 50, writeUnits: 2,
		},
		{
			name:       "SPLIT divides an entity's delivery units",
			change:     func(s *explain.Stream) { s.Shards = 8; s.Views[1].Split = 4 },
			view:       1,
			hwmEntries: 2, rowsPerBatch: 50, writeUnits: 1,
		},
		{
			name:       "a uniq state adds 3 KB",
			change:     func(s *explain.Stream) { s.Views[1].Uniqs = 1 },
			view:       1,
			hwmEntries: 1, rowsPerBatch: 50, writeUnits: 4,
		},
		{
			name:       "ARRAY JOIN fan-out multiplies rows and marker reads",
			change:     func(s *explain.Stream) { s.Views[2].Pass = 2 },
			view:       2,
			hwmEntries: 1, rowsPerBatch: 100, markerReads: 2000, writeUnits: 1,
		},
		{
			name:       "WHERE keeps a fraction of the events",
			change:     func(s *explain.Stream) { s.Views[2].Pass = 0.1 },
			view:       2,
			hwmEntries: 1, rowsPerBatch: 5, markerReads: 100, writeUnits: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, stream := model(t, scenarioA())
			tt.change(&stream)
			v := estimate(t, store, stream).Views[tt.view]
			near(t, "HWMEntries", v.HWMEntries, tt.hwmEntries, 1e-12)
			near(t, "RowsPerBatch", v.RowsPerBatch, tt.rowsPerBatch, 1e-12)
			near(t, "Markers.Read", v.Markers.Read, tt.markerReads, 1e-12)
			if v.WriteUnits != tt.writeUnits {
				t.Errorf("WriteUnits = %d for a %.3f KB row, want %d", v.WriteUnits, v.RowKB, tt.writeUnits)
			}
		})
	}
}

func TestEstimateForeverAtHorizon(t *testing.T) {
	t.Parallel()
	var b scenario
	for _, sc := range loadReference(t) {
		if strings.HasPrefix(sc.Name, "B. ") {
			b = sc
		}
	}
	if b.Name == "" {
		t.Fatalf("no scenario B in %s", referencePath)
	}
	store, stream := model(t, b.Inputs) // a year of events
	stream.Forever = true
	got := estimate(t, store, stream)
	if got.Partitions != 3469 || got.TTLWrites != 0 {
		t.Errorf("a year into FOREVER: %d partitions and %v TTL writes, want 3469 and 0", got.Partitions, got.TTLWrites)
	}
	near(t, "write units", got.Units().Write, b.Intermediates["wps"]-b.Intermediates["w_ttl"], 1e-9)
}

func TestEstimateDeployment(t *testing.T) {
	t.Parallel()
	t.Run("x86_64 with 2 GB", func(t *testing.T) {
		t.Parallel()
		store, stream := model(t, scenarioA())
		store.Arch, store.MemoryMB = explain.ArchX86, 2048
		// 20 invocations/s of 0.13 s for a 730-hour month: 2,628,000 s × 20 × ($0.20/M + 0.13 s × 2 GB × $0.0000166667).
		near(t, "Lambda", estimate(t, store, stream).Monthly.Lambda, 238.27245552, 1e-9)
	})
	t.Run("batching window 0 polls every 0.25 s", func(t *testing.T) {
		t.Parallel()
		store, stream := model(t, scenarioA())
		store.BatchingWindow = 0
		got := estimate(t, store, stream)
		// 20 partitions of 50 events/s: 20 × (1 - e^-12.5) / 0.25 invocations.
		near(t, "Invocations", got.Invocations, 79.99970186774624, 1e-9)
		near(t, "Batch", got.Batch, 12.50004658333825, 1e-9)
	})
	t.Run("batch size caps batches before the window ends", func(t *testing.T) {
		t.Parallel()
		store, stream := model(t, scenarioA())
		store.BatchSize = 10
		got := estimate(t, store, stream)
		// 1,000 events/s in batches of 10.
		near(t, "Invocations", got.Invocations, 100, 1e-12)
		near(t, "Batch", got.Batch, 10, 1e-12)
	})
	t.Run("provisioned stream table only", func(t *testing.T) {
		t.Parallel()
		store, stream := model(t, scenarioA())
		onDemand := estimate(t, store, stream)
		stream.Capacity = explain.Capacity{Provisioned: true, ReadTarget: 0.7, WriteTarget: 0.7}
		got := estimate(t, store, stream)
		// 1,000 puts and 666.7 TTL deletes per second at 70%: 2,381 WCU × $0.00065 × 730 h.
		near(t, "stream requests", got.StreamDollars, 1129.7619047619, 1e-9)
		near(t, "other requests", got.Monthly.Requests-got.StreamDollars, onDemand.Monthly.Requests-onDemand.StreamDollars, 1e-12)
	})
}

func TestEstimateRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(*explain.Store, *explain.Stream)
		want   string
	}{
		{"no regions", func(s *explain.Store, _ *explain.Stream) { s.Regions = 0 }, "no regions"},
		{"no high-water-mark window", func(s *explain.Store, _ *explain.Stream) { s.HWMWindow = 0 }, "high-water-mark"},
		{"no batch size", func(s *explain.Store, _ *explain.Stream) { s.BatchSize = 0 }, "batch size"},
		{"batch size over the maximum", func(s *explain.Store, _ *explain.Stream) { s.BatchSize = 10001 }, "batch size"},
		{"no projector memory", func(s *explain.Store, _ *explain.Stream) { s.MemoryMB = 0 }, "memory"},
		{"unknown arch", func(s *explain.Store, _ *explain.Stream) { s.Arch = "" }, "architecture"},
		{"no events", func(_ *explain.Store, s *explain.Stream) { s.Rate = 0 }, "rate"},
		{"NaN rate", func(_ *explain.Store, s *explain.Stream) { s.Rate = math.NaN() }, "rate"},
		{"infinite rate", func(_ *explain.Store, s *explain.Stream) { s.Rate = math.Inf(1) }, "rate"},
		{"empty events", func(_ *explain.Store, s *explain.Stream) { s.EventKB = 0 }, "event size"},
		{"NaN event size", func(_ *explain.Store, s *explain.Stream) { s.EventKB = math.NaN() }, "event size"},
		{"event over the item limit", func(_ *explain.Store, s *explain.Stream) { s.EventKB = 401 }, "event size"},
		{"no retention", func(_ *explain.Store, s *explain.Stream) { s.Retention = 0 }, "retention"},
		{"no shards", func(_ *explain.Store, s *explain.Stream) { s.Shards = 0 }, "shard"},
		{"more archive copies than regions", func(_ *explain.Store, s *explain.Stream) { s.ArchiveCopies = 4 }, "archive"},
		{"meta table without targets", func(s *explain.Store, _ *explain.Stream) { s.Meta = explain.Capacity{Provisioned: true} }, "meta table"},
		{"stream table with a NaN target", func(_ *explain.Store, s *explain.Stream) {
			s.Capacity = explain.Capacity{Provisioned: true, ReadTarget: 0.7, WriteTarget: math.NaN()}
		}, "stream table"},
		{"view table without targets", func(_ *explain.Store, s *explain.Stream) {
			s.Views[1].Capacity = explain.Capacity{Provisioned: true}
		}, "view user_latest: autoscaling"},
		{"shared view without rows", func(_ *explain.Store, s *explain.Stream) { s.Views[0].ActiveRows = 0 }, "active rows"},
		{"shared view with NaN rows", func(_ *explain.Store, s *explain.Stream) { s.Views[0].ActiveRows = math.NaN() }, "active rows"},
		{"shared view with infinite rows", func(_ *explain.Store, s *explain.Stream) { s.Views[0].ActiveRows = math.Inf(1) }, "active rows"},
		{"negative base size", func(_ *explain.Store, s *explain.Stream) { s.Views[1].BaseKB = -1 }, "base row size"},
		{"negative uniqs", func(_ *explain.Store, s *explain.Stream) { s.Views[1].Uniqs = -3 }, "negative"},
		{"negative uniqExacts", func(_ *explain.Store, s *explain.Stream) { s.Views[2].UniqExacts = -1 }, "negative"},
		{"new-value fraction above 1", func(_ *explain.Store, s *explain.Stream) { s.Views[2].UniqNew = 1.5 }, "uniqExact"},
		{"NaN new-value fraction", func(_ *explain.Store, s *explain.Stream) { s.Views[2].UniqNew = math.NaN() }, "uniqExact"},
		{"no source rows", func(_ *explain.Store, s *explain.Stream) { s.Views[2].Pass = 0 }, "source rows"},
		{"NaN source rows", func(_ *explain.Store, s *explain.Stream) { s.Views[2].Pass = math.NaN() }, "source rows"},
		{"no split", func(_ *explain.Store, s *explain.Stream) { s.Views[1].Split = 0 }, "split"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, stream := model(t, scenarioA())
			tt.change(&store, &stream)
			_, err := explain.Estimate(store, stream)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Estimate error = %v, want one about %s", err, tt.want)
			}
		})
	}
}

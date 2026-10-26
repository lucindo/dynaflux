// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package explain estimates what a stream and its views cost to run: request units, row
// sizes, projector invocations and monthly dollars, per region.
//
// The model generalizes the one in testdata/costs.py from its three typical views to any
// list of views. It's rough (±30%) and uses us-east-1 list prices: a way to compare designs,
// not a quote. [Explain] maps a compiled project onto it and writes the reports of
// dynaflux explain.
package explain

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

const (
	monthHours      = 730
	monthSeconds    = monthHours * 3600
	monthsPerYear   = 12
	hoursPerDay     = 24
	secondsPerDay   = 86400
	kbPerGB         = 1e6
	mbPerGB         = 1024
	partitionGB     = 10   // widely cited; DynamoDB doesn't document it
	partitionWrites = 1000 // write units per second that one partition takes
	itemOverheadKB  = 0.1  // stored per item; AWS now says 100 bytes, plus 48 in a global table
	readUnitKB      = 4    // one strongly consistent read unit reads up to 4 KB
	itemLimitKB     = 400

	shardLifeSeconds  = 4 * 3600 // a change-stream shard is replaced after at most 4 hours
	minPollSeconds    = 0.25     // Lambda polls each shard 4 times a second
	maxBatchSize      = 10000    // the event source mapping's largest BatchSize
	hwmEntryKB        = 0.029    // costs.py's figure for a ~29-byte mark; rows are sized in KB of 1,024 bytes, so a mark counts 29.7 B
	hllKB             = 3        // dense HyperLogLog: 2^12 registers of 6 bits
	conflictFactor    = 1.1      // conditional writes that lose a race and retry
	txnFactor         = 2        // a transactional write costs twice the units
	invocationSeconds = 0.03     // projector duration: this, plus perEventSeconds per event
	perEventSeconds   = 0.002
	archiveOverhead   = 1.2 // archived bytes per event byte
	compression       = 6.0 // of the archive's Parquet files
)

// Arch is a Lambda architecture.
type Arch string

// Lambda architectures, spelled as runtime.arch spells them.
const (
	ArchARM64 Arch = "arm64"
	ArchX86   Arch = "x86_64"
)

// Capacity is how a table is billed.
type Capacity struct {
	// Provisioned bills the provisioned capacity that autoscaling keeps for the average load
	// at the targets. Otherwise the table is on-demand.
	Provisioned bool
	// ReadTarget and WriteTarget are the autoscaling target utilizations, as fractions.
	ReadTarget, WriteTarget float64
}

// Store holds the store-wide settings that costs depend on.
type Store struct {
	Regions int
	// BatchingWindow is how long the projector gathers a batch, and BatchSize the most
	// events a batch holds. Lambda delivers a batch when either is reached.
	BatchingWindow time.Duration
	BatchSize      int
	// HWMWindow is how long a view row keeps a delivery unit's high-water mark after that
	// unit's last write to it: the prune window, runtime.prune_window_seconds.
	HWMWindow time.Duration
	// MemoryMB and Arch configure the projector function.
	MemoryMB int
	Arch     Arch
	// Meta is the capacity of the meta table, where the projector records its progress.
	Meta Capacity
}

// Stream is a stream, its traffic and the views it feeds.
type Stream struct {
	// Rate is the average number of events appended per second, all regions together.
	Rate float64
	// EventKB is the average size of an event item in KB of 1,024 bytes, as DynamoDB sizes
	// items.
	EventKB float64
	// Retention is the LOG RETENTION period. With Forever, the table never stops growing,
	// and Retention is the age at which to evaluate it.
	Retention time.Duration
	Forever   bool
	// Shards is the SHARDS count, 1 without the clause.
	Shards int
	// ArchiveCopies is how many times the archive holds each event, all regions together:
	// the number of archiving regions, 1 if each region archives only the events it
	// accepted, or 0 if the stream isn't archived.
	ArchiveCopies int
	Capacity      Capacity
	Views         []View
}

// View is what the cost of a view version depends on.
type View struct {
	Name string
	// Shared is set when any delivery unit can write any row, because GROUP BY leaves out a
	// column of the stream's PRIMARY KEY. Otherwise each row belongs to one entity.
	Shared bool
	// ActiveRows is how many rows a shared view writes to at a time, such as the pages
	// visited today for GROUP BY page, day.
	ActiveRows float64
	// BaseKB is the row size without high-water marks and uniq states, in KB of 1,024 bytes.
	BaseKB float64
	// Uniqs is the number of uniq aggregates. Each keeps a dense HyperLogLog in the row.
	Uniqs int
	// UniqExacts is the number of uniqExact aggregates. Each reads a marker item per source
	// row, and writes one per new value.
	UniqExacts int
	// UniqNew is the fraction of source rows that bring a uniqExact aggregate a new value.
	UniqNew float64
	// Pass is the number of source rows per event: the fraction of events that WHERE keeps,
	// times the ARRAY JOIN fan-out.
	Pass float64
	// Split is the SPLIT count, 1 without the clause.
	Split int
	// Sync is MODE SYNC.
	Sync     bool
	Capacity Capacity
}

// Units are request units per second in one region.
type Units struct {
	Write, Read float64
}

// StreamCost is what a stream and its views cost per region, averaged over the regions.
type StreamCost struct {
	// StoredGB is the size of the stream table.
	StoredGB float64
	// Partitions is the number of stream table partitions. Each is a delivery unit: a shard
	// of the table's change stream.
	Partitions int
	// Invocations is the number of projector invocations per second, and Batch the average
	// number of events each one gets.
	Invocations, Batch float64
	// EventWrites, TTLWrites and ProgressWrites are write units per second: of event puts,
	// local and replicated; of deletes of expired events, replicated from the region that
	// expired them; and of the projector's progress items in the meta table.
	EventWrites, TTLWrites, ProgressWrites float64
	// StreamDollars and MetaDollars are the monthly cost of those writes, to the stream table
	// and to the meta table.
	StreamDollars, MetaDollars float64
	Views                      []ViewCost
	Monthly                    Monthly
	// Total is the monthly cost of all regions.
	Total float64
}

// ViewCost is what a view costs in each region.
type ViewCost struct {
	Name string
	// RowsPerBatch is the number of rows a projector invocation updates.
	RowsPerBatch float64
	// HWMEntries is the number of high-water marks a row holds, or an item of a split row.
	HWMEntries float64
	// RowKB is the size of a row in KB of 1,024 bytes, and WriteUnits and ReadUnits the cost
	// of writing it and of reading it with strong consistency.
	RowKB                 float64
	WriteUnits, ReadUnits int
	// Rows are the units of the rows' read-modify-writes, and Markers those of uniqExact's
	// marker items.
	Rows, Markers Units
	// Dollars is the monthly cost of those units.
	Dollars float64
}

// Monthly is the cost of a month in dollars, by line, averaged over the regions.
type Monthly struct {
	Requests float64 // DynamoDB requests to every table
	Storage  float64 // DynamoDB storage of the stream table
	Lambda   float64 // the projector
	Firehose float64
	S3       float64 // the archive, averaged over its first year
	Transfer float64 // replication between regions
}

// flow is what a stream's views see of its projection in one region.
type flow struct {
	events      float64 // appended per second
	batch       float64 // events per invocation
	invocations float64 // per second
	perUnit     float64 // invocations per second of one delivery unit
	units       float64 // delivery units
	local       float64 // the share of appends a region accepts, assuming an even spread
	hwmWindow   float64 // seconds
	shards      float64 // SHARDS
}

// Estimate returns what s and its views cost in the regions of store.
func Estimate(store Store, s Stream) (StreamCost, error) {
	if err := check(store, s); err != nil {
		return StreamCost{}, err
	}
	events := s.Rate
	regions := float64(store.Regions)
	eventUnits := math.Ceil(s.EventKB)
	days := s.Retention.Hours() / hoursPerDay

	var c StreamCost
	c.StoredGB = events * (s.EventKB + itemOverheadKB) * days * secondsPerDay / kbPerGB
	c.Partitions = int(max(1, math.Ceil(c.StoredGB/partitionGB), math.Ceil(events*eventUnits/partitionWrites)))
	units := float64(c.Partitions)
	window := max(store.BatchingWindow.Seconds(), minPollSeconds)
	// Poisson arrivals: a delivery unit invokes the projector for each window with an event,
	// or sooner when the batch fills. Expm1 keeps the precision that 1 - Exp loses at low
	// rates. costs.py has no BatchSize; its batches stay under 100 events.
	perUnit := max(-math.Expm1(-(events/units)*window)/window, events/units/float64(store.BatchSize))
	c.Invocations = units * perUnit
	c.Batch = events / c.Invocations

	f := flow{
		events: events, batch: c.Batch, invocations: c.Invocations, perUnit: perUnit, units: units,
		local: 1 / regions, hwmWindow: store.HWMWindow.Seconds(), shards: float64(s.Shards),
	}
	c.Views = make([]ViewCost, len(s.Views))
	for i, v := range s.Views {
		c.Views[i] = f.view(v)
	}

	c.EventWrites = events * eventUnits
	if slices.ContainsFunc(s.Views, func(v View) bool { return v.Sync }) {
		c.EventWrites *= 1 + f.local // the accepting region's put is transactional
	}
	if !s.Forever {
		// Expiring an item is free, but the delete replicates to the other regions as a write.
		c.TTLWrites = events * eventUnits * (regions - 1) / regions
	}
	c.ProgressWrites = c.Invocations
	c.StreamDollars = s.Capacity.monthly(Units{Write: c.EventWrites + c.TTLWrites})
	c.MetaDollars = store.Meta.monthly(Units{Write: c.ProgressWrites})
	c.Monthly = c.monthly(store, s)
	c.Total = c.Monthly.Sum() * regions
	return c, nil
}

// Units returns the request units of every table.
func (c *StreamCost) Units() Units {
	u := Units{Write: c.EventWrites + c.TTLWrites + c.ProgressWrites}
	for _, v := range c.Views {
		u.Write += v.Rows.Write + v.Markers.Write
		u.Read += v.Rows.Read + v.Markers.Read
	}
	return u
}

func (c *StreamCost) monthly(store Store, s Stream) Monthly {
	events, regions := s.Rate, float64(store.Regions)
	m := Monthly{
		Requests: c.StreamDollars + c.MetaDollars,
		Storage:  c.StoredGB * storageGBMonth,
	}
	for _, v := range c.Views {
		m.Requests += v.Dollars
	}
	duration := invocationSeconds + perEventSeconds*c.Batch
	gbSecond := lambdaGBsARM64
	if store.Arch == ArchX86 {
		gbSecond = lambdaGBsX86
	}
	m.Lambda = c.Invocations * monthSeconds * (lambdaRequest + duration*float64(store.MemoryMB)/mbPerGB*gbSecond)
	archiveGB := events * s.EventKB * archiveOverhead * monthSeconds / kbPerGB
	copies := float64(s.ArchiveCopies)
	m.Firehose = archiveGB * firehoseGB * copies / regions
	// The archive grows all year, so its first-year average is half a year's data.
	m.S3 = archiveGB / compression * monthsPerYear * s3GBMonth / 2 * copies / regions
	m.Transfer = events * s.EventKB * monthSeconds / kbPerGB * (regions - 1) / regions * transferGB
	return m
}

// OverItemLimit reports whether a row outgrows DynamoDB's 400 KB item limit, which fails
// every write to it.
func (v ViewCost) OverItemLimit() bool {
	return v.RowKB > itemLimitKB
}

// Sum returns the cost of all lines.
func (m Monthly) Sum() float64 {
	return m.Requests + m.Storage + m.Lambda + m.Firehose + m.S3 + m.Transfer
}

func (f flow) view(v View) ViewCost {
	c := ViewCost{Name: v.Name}
	projected := v.Pass * f.batch // source rows per invocation
	if v.Sync {
		projected *= 1 - f.local // the accepting region applies its own appends
	}
	split := float64(v.Split)
	if v.Shared {
		k := v.ActiveRows
		// The expected number of distinct rows among projected uniform picks of k,
		// k(1 - (1-1/k)^projected), in a form that keeps its precision for large k.
		if projected > 0 { // with k = 1 the log is -Inf, and 0 × -Inf is NaN
			c.RowsPerBatch = -k * math.Expm1(projected*math.Log1p(-1/k))
		}
		touches := f.perUnit * c.RowsPerBatch / k
		// A row keeps a mark for each shard that wrote it within the window, and every
		// partition's shard is replaced every few hours.
		c.HWMEntries = f.units / split * (1 + f.hwmWindow/shardLifeSeconds) * -math.Expm1(-touches*f.hwmWindow)
	} else {
		c.RowsPerBatch = projected
		// An entity's events come through one delivery unit, or one per SHARDS shard.
		c.HWMEntries = max(1, min(f.shards, f.units)/split)
	}
	c.RowKB = v.BaseKB + float64(v.Uniqs)*hllKB + hwmEntryKB*c.HWMEntries
	c.WriteUnits = int(math.Ceil(c.RowKB))
	c.ReadUnits = int(math.Ceil(c.RowKB / readUnitKB))
	write, read := float64(c.WriteUnits), float64(c.ReadUnits)
	c.Rows = Units{
		Write: f.invocations * (c.RowsPerBatch * write) * conflictFactor,
		Read:  f.invocations * (c.RowsPerBatch * read) * conflictFactor,
	}
	sourceRows := v.Pass * f.events
	markers := float64(v.UniqExacts) * sourceRows
	c.Markers = Units{Write: markers * v.UniqNew, Read: markers}
	if v.Sync {
		txn := sourceRows * f.local
		c.Rows.Write += txn * txnFactor * write * conflictFactor
		c.Rows.Read += txn * read * conflictFactor
		c.Markers.Write *= 1 + f.local // the local share is transactional
	}
	c.Dollars = v.Capacity.monthly(Units{Write: c.Rows.Write + c.Markers.Write, Read: c.Rows.Read + c.Markers.Read})
	return c
}

func (c Capacity) monthly(u Units) float64 {
	if c.Provisioned {
		return (u.Write/c.WriteTarget*wcuHour + u.Read/c.ReadTarget*rcuHour) * monthHours
	}
	return (u.Write*wruPrice + u.Read*rruPrice) * monthSeconds
}

func (c Capacity) check() error {
	if c.Provisioned && (!fraction(c.ReadTarget) || !fraction(c.WriteTarget)) {
		return fmt.Errorf("autoscaling targets %v and %v are not in (0, 1]", c.ReadTarget, c.WriteTarget)
	}
	return nil
}

func check(store Store, s Stream) error {
	switch {
	case store.Regions < 1:
		return errors.New("no regions")
	case store.HWMWindow <= 0:
		return errors.New("the high-water-mark window must be positive")
	case store.BatchSize < 1 || store.BatchSize > maxBatchSize:
		return fmt.Errorf("the batch size must be from 1 to %d, not %d", maxBatchSize, store.BatchSize)
	case store.MemoryMB < 1:
		return errors.New("the projector memory must be positive")
	case store.Arch != ArchARM64 && store.Arch != ArchX86:
		return fmt.Errorf("unknown Lambda architecture %q", store.Arch)
	case !finite(s.Rate) || s.Rate <= 0:
		return fmt.Errorf("the event rate must be a positive, finite number, not %v", s.Rate)
	case !finite(s.EventKB) || s.EventKB <= 0 || s.EventKB > itemLimitKB:
		return fmt.Errorf("the event size must be over 0 and at most %d KB, not %v", itemLimitKB, s.EventKB)
	case s.Retention <= 0:
		return errors.New("the retention must be positive")
	case s.Shards < 1:
		return errors.New("the stream needs at least 1 shard")
	case s.ArchiveCopies < 0 || s.ArchiveCopies > store.Regions:
		return fmt.Errorf("%d archive copies in %d regions", s.ArchiveCopies, store.Regions)
	}
	if err := store.Meta.check(); err != nil {
		return fmt.Errorf("meta table: %w", err)
	}
	if err := s.Capacity.check(); err != nil {
		return fmt.Errorf("stream table: %w", err)
	}
	for _, v := range s.Views {
		if err := v.check(); err != nil {
			return fmt.Errorf("view %s: %w", v.Name, err)
		}
	}
	return nil
}

func (v View) check() error {
	switch {
	case v.Shared && (!finite(v.ActiveRows) || v.ActiveRows < 1):
		return fmt.Errorf("a shared view needs a finite number of active rows, at least 1, not %v", v.ActiveRows)
	case !finite(v.BaseKB) || v.BaseKB < 0:
		return fmt.Errorf("the base row size must be a finite number of KB, 0 or more, not %v", v.BaseKB)
	case v.Uniqs < 0 || v.UniqExacts < 0:
		return errors.New("the uniq and uniqExact counts can't be negative")
	case !finite(v.UniqNew) || v.UniqNew < 0 || v.UniqNew > 1:
		return fmt.Errorf("the uniqExact new-value fraction %v is not between 0 and 1", v.UniqNew)
	case !finite(v.Pass) || v.Pass <= 0:
		return fmt.Errorf("the source rows per event must be a positive, finite number, not %v", v.Pass)
	case v.Split < 1:
		return errors.New("split must be at least 1")
	}
	return v.Capacity.check()
}

// finite is false for NaN and ±Inf, which [strconv.ParseFloat] accepts as "nan" and "inf".
func finite(x float64) bool {
	return !math.IsNaN(x) && !math.IsInf(x, 0)
}

// fraction reports whether x is in (0, 1]. NaN fails every comparison, so it isn't.
func fraction(x float64) bool {
	return x > 0 && x <= 1
}

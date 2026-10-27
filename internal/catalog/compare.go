// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import "slices"

// StreamChangeKind is a kind of difference between a deployed stream and its local
// definition. sql.md §8.9 allows some kinds and rejects the others.
type StreamChangeKind string

// Kinds of stream changes that sql.md §8.9 allows.
const (
	StreamAdded         StreamChangeKind = "stream added"
	StreamDropped       StreamChangeKind = "stream dropped" // Its table and archive stay.
	ColumnAdded         StreamChangeKind = "column added"   // Nullable or with a DEFAULT.
	LogRetentionChanged StreamChangeKind = "log retention changed"
	AcceptChanged       StreamChangeKind = "accept changed"
	GoTypeChanged       StreamChangeKind = "go type changed"
	ArchiveChanged      StreamChangeKind = "archive changed"
	// ArchiveStopped is a region that archived every event of the stream and no longer does,
	// which leaves a hole in its archive.
	ArchiveStopped StreamChangeKind = "archive stopped"
)

// Kinds of stream changes that sql.md §8.9 rejects.
const (
	ColumnRemoved     StreamChangeKind = "column removed" // Removed or renamed.
	ColumnRetyped     StreamChangeKind = "column retyped"
	DefaultChanged    StreamChangeKind = "default changed"
	ColumnRequired    StreamChangeKind = "column required" // A new column that is neither Nullable nor defaulted.
	PrimaryKeyChanged StreamChangeKind = "primary key changed"
	DedupByChanged    StreamChangeKind = "dedup by changed"
	ShardsChanged     StreamChangeKind = "shards changed"
)

// StreamChange is one difference between a deployed stream and its local definition.
type StreamChange struct {
	Kind StreamChangeKind
	// Column is the column of a column change.
	Column string
	// Region is the region of an ArchiveStopped change.
	Region string
}

// CompareStreams returns how local, the definition that the SQL files give a stream,
// differs from deployed, the same stream in a deployed catalog: stream changes first, then
// the deployed columns in order, the new columns in order, and the regions whose archive
// stops, sorted. A nil deployed is a new stream, and a nil local a dropped one.
func CompareStreams(deployed, local *Stream) []StreamChange {
	switch {
	case deployed == nil && local == nil:
		return nil
	case deployed == nil:
		return []StreamChange{{Kind: StreamAdded}}
	case local == nil:
		return []StreamChange{{Kind: StreamDropped}}
	}
	var changes []StreamChange
	add := func(changed bool, kind StreamChangeKind) {
		if changed {
			changes = append(changes, StreamChange{Kind: kind})
		}
	}
	add(!slices.Equal(deployed.PrimaryKey, local.PrimaryKey), PrimaryKeyChanged)
	add(!slices.Equal(deployed.DedupBy, local.DedupBy), DedupByChanged)
	add(deployed.Shards != local.Shards, ShardsChanged)
	for _, d := range deployed.Columns {
		i := slices.IndexFunc(local.Columns, func(c Column) bool { return c.Name == d.Name })
		switch {
		case i < 0:
			changes = append(changes, StreamChange{Kind: ColumnRemoved, Column: d.Name})
		case local.Columns[i].Type != d.Type:
			changes = append(changes, StreamChange{Kind: ColumnRetyped, Column: d.Name})
		case !sameExpr(local.Columns[i].Default, d.Default):
			changes = append(changes, StreamChange{Kind: DefaultChanged, Column: d.Name})
		}
	}
	for _, c := range local.Columns {
		if slices.ContainsFunc(deployed.Columns, func(d Column) bool { return d.Name == c.Name }) {
			continue
		}
		kind := ColumnRequired
		if c.Type.Nullable || c.Default != nil {
			kind = ColumnAdded
		}
		changes = append(changes, StreamChange{Kind: kind, Column: c.Name})
	}
	add(deployed.LogRetention != local.LogRetention, LogRetentionChanged)
	add(!samePtr(deployed.Accept.Past, local.Accept.Past) || deployed.Accept.Future != local.Accept.Future, AcceptChanged)
	add(deployed.GoType != local.GoType, GoTypeChanged)
	add(!sameArchive(deployed.Archive, local.Archive), ArchiveChanged)
	for _, r := range slices.Sorted(slices.Values(allEventsRegions(deployed.Archive))) {
		if !slices.Contains(allEventsRegions(local.Archive), r) {
			changes = append(changes, StreamChange{Kind: ArchiveStopped, Region: r})
		}
	}
	return changes
}

// allEventsRegions returns the regions that archive every event, local and replicated.
func allEventsRegions(a *Archive) []string {
	if a == nil || a.Events != ArchiveEventsAll {
		return nil
	}
	return a.Regions
}

func sameArchive(a, b *Archive) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Events == b.Events && slices.Equal(slices.Sorted(slices.Values(a.Regions)), slices.Sorted(slices.Values(b.Regions)))
}

func samePtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// sameExpr reports whether a and b are the same expression.
func sameExpr(a, b *Expr) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Op == b.Op && a.Type == b.Type && a.Value == b.Value && a.Name == b.Name && a.Index == b.Index &&
		slices.EqualFunc(a.Args, b.Args, func(x, y Expr) bool { return sameExpr(&x, &y) })
}

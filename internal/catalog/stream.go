// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import "slices"

// Names of the implicit columns that every stream has.
const (
	ColumnEventID      = "event_id"
	ColumnEventTime    = "event_time"
	ColumnIngestTime   = "ingest_time"
	ColumnOriginRegion = "origin_region"
)

// MaxShards is the largest SHARDS count.
const MaxShards = 64

// Stream is a CREATE STREAM with its defaults resolved.
type Stream struct {
	Name string `json:"name"`
	// GoType is the -- go:type override of the generated event type, or empty.
	GoType string `json:"go_type,omitzero"`
	// Columns are the declared columns, in declaration order. Generated code passes values
	// in this order. [ImplicitColumns] lists the others.
	Columns []Column `json:"columns,omitempty"`
	// PrimaryKey names the partition key columns, in order.
	PrimaryKey []string `json:"primary_key,omitempty"`
	// DedupBy names the DEDUP BY columns, in order. With them, event_id is a hash of the
	// event's PRIMARY KEY values, event_time and DEDUP BY values instead of a ULID that the
	// writer makes (sql.md §8.5).
	DedupBy      []string  `json:"dedup_by,omitempty"`
	LogRetention Retention `json:"log_retention"`
	Accept       Accept    `json:"accept"`
	// Shards is the SHARDS count: 1 without the clause, and at most [MaxShards].
	Shards int `json:"shards"`
	// Archive places the stream's archive; nil when the stream isn't archived.
	Archive       *Archive `json:"archive,omitzero"`
	StorageFormat int      `json:"storage_format"`
}

// ImplicitColumns returns the columns every stream has besides its declared ones.
func ImplicitColumns() []Column {
	str := Type{Kind: KindString}
	micros := Type{Kind: KindDateTime64, Precision: DateTime64Micros}
	return []Column{
		{Name: ColumnEventID, Type: str},
		{Name: ColumnEventTime, Type: micros},
		{Name: ColumnIngestTime, Type: micros},
		{Name: ColumnOriginRegion, Type: str},
	}
}

// Column returns the declared or implicit column named name.
func (s *Stream) Column(name string) (Column, bool) {
	byName := func(c Column) bool { return c.Name == name }
	if i := slices.IndexFunc(s.Columns, byName); i >= 0 {
		return s.Columns[i], true
	}
	implicit := ImplicitColumns()
	if i := slices.IndexFunc(implicit, byName); i >= 0 {
		return implicit[i], true
	}
	return Column{}, false
}

// Column is a declared stream column.
type Column struct {
	Name string `json:"name"`
	Type Type   `json:"type"`
	// Default is the DEFAULT value, computed by the compiler: an OpLiteral of the column's
	// type, an OpArray of them, or nil. Events carry no schema version, so an event stored
	// before the column existed reads as Default, or as NULL for a Nullable column. A
	// Nullable column's DEFAULT can only be NULL, stored as a nil Default, so a missing
	// value and a stored NULL read the same, in DynamoDB and in the archive. A deployed
	// column's Default never changes (sql.md §8.9).
	Default *Expr `json:"default,omitzero"`
}

// Accept is the ACCEPT window on event_time, relative to the writer's clock at append time.
type Accept struct {
	// Past bounds event_time from below at now() - Past; nil means no lower bound, and a
	// bare now() is "0 SECOND".
	Past *Interval `json:"past,omitzero"`
	// Future bounds event_time from above at now() + Future. A bare now() is "0 SECOND".
	Future Interval `json:"future"`
}

// Archive is where a stream is archived.
type Archive struct {
	// Regions are the archiving regions.
	Regions []string `json:"regions,omitempty"`
	// Events says which events each archiving region archives.
	Events ArchiveEvents `json:"events"`
}

// ArchiveEvents says which events an archiving region archives.
type ArchiveEvents string

// Archive event sets.
const (
	ArchiveEventsAll   ArchiveEvents = "all"   // Every event, local and replicated.
	ArchiveEventsLocal ArchiveEvents = "local" // Only the events the region accepted.
)

func (e ArchiveEvents) known() bool {
	switch e {
	case ArchiveEventsAll, ArchiveEventsLocal:
		return true
	}
	return false
}

// UnmarshalText implements [encoding.TextUnmarshaler]; it rejects unknown values.
func (e *ArchiveEvents) UnmarshalText(text []byte) error {
	return decodeEnum(e, text, "archive events")
}

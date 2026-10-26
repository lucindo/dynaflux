// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package archive is the schema of the event archive: in each archiving region, an Iceberg
// table per archived stream, which Firehose writes, and an Athena view of it with one row
// per event. The template builder, the setup function, the projector's schema guard and
// the Firehose writer all take the schema from here, so they can't disagree
// (docs/spec/infra.md §7, ADR 0008).
package archive

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Bookkeeping columns, which follow a stream's columns in its table. Stream column names
// start with a letter, so they can't clash with these.
const (
	// ColumnShard is the DynamoDB stream shard the record came from.
	ColumnShard = "_shard"
	// ColumnSeq is the record's sequence number, zero-padded to [catalog.SequenceWidth]
	// digits so that string order is numeric order.
	ColumnSeq = "_seq"
	// ColumnArrival is the record's ApproximateCreationDateTime, which every copy of the
	// record shares.
	ColumnArrival = "_arrival"
)

// partitionTransform is the Iceberg transform of every archive table's partition field.
const partitionTransform = "day"

// Partitioning is the partition spec of every archive table, the day of event_time, in the
// notation of the setup function's Glue reader.
const Partitioning = partitionTransform + "(" + catalog.ColumnEventTime + ")"

// Schema is the archive of one region: a table per stream archived there, sorted by name.
type Schema struct {
	Tables []Table
}

// Table is the Iceberg table of an archived stream.
type Table struct {
	// Name is the table name, which is the stream name.
	Name string
	// Columns are the implicit columns, the declared columns in declaration order, then
	// [ColumnShard], [ColumnSeq] and [ColumnArrival]. Every column is optional.
	Columns []Column
}

// Column is a column of an archive table.
type Column struct {
	Name string
	// Type is the column type in Athena DDL, such as "decimal(20,0)" or "array<string>".
	Type string
	// Default is the column's DEFAULT as an Athena SQL literal, which the dedup view reads
	// in rows archived before the column existed. It is nil without a DEFAULT, and so for
	// every Nullable column.
	Default *string
}

// SchemaFor returns the archive schema of region: the tables of the streams in cat that
// region archives.
func SchemaFor(cat *catalog.Catalog, region string) Schema {
	var s Schema
	for i := range cat.Streams {
		st := &cat.Streams[i]
		if st.Archive != nil && slices.Contains(st.Archive.Regions, region) {
			s.Tables = append(s.Tables, table(st))
		}
	}
	// The compiler keeps declaration order and catalog.Marshal sorts, so the fingerprint
	// must not depend on the order.
	slices.SortFunc(s.Tables, func(a, b Table) int { return strings.Compare(a.Name, b.Name) })
	return s
}

// Fingerprint returns "sha256:" and the lowercase hex SHA-256 of the schema: every table's
// name and its columns' names, types and defaults, in order. It is the same for every
// prefix, since nothing in a [Schema] depends on one.
func (s Schema) Fingerprint() string {
	var b strings.Builder
	for _, t := range s.Tables {
		for _, c := range t.Columns {
			fmt.Fprintf(&b, "%q %q %q", t.Name, c.Name, c.Type)
			if c.Default != nil {
				fmt.Fprintf(&b, " %q", *c.Default)
			}
			b.WriteByte('\n')
		}
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Location returns the location of a stream's table under warehouse, which has no
// trailing slash. The final "iceberg/" keeps one table's location from being a key prefix
// of another's, as page's would be of page_views': a tool that removes a table's orphan
// files could otherwise delete the other table's files.
func Location(warehouse, db, stream string) string {
	return warehouse + "/" + db + "/" + stream + "/iceberg/"
}

// The forms of names, types and DEFAULT literals that [SchemaFor] writes. Arrays don't nest.
const (
	scalarType    = `(?:int|bigint|decimal\([0-9]{1,2},[0-9]{1,2}\)|double|boolean|date|timestamp|string)`
	stringLiteral = `(?:U&)?'(?:[^']|'')*'`
	scalarLiteral = `(?:(?:(?:INTEGER|BIGINT|DECIMAL|DOUBLE|DATE|TIMESTAMP) )?` + stringLiteral + `|true|false)`
)

var (
	namePattern    = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	typePattern    = regexp.MustCompile(`^(?:` + scalarType + `|array<` + scalarType + `>)$`)
	literalPattern = regexp.MustCompile(`^(?:` + scalarLiteral + `|ARRAY\[(?:` + scalarLiteral + `(?:, ` + scalarLiteral + `)*)?\])$`)
)

// Check fails unless t has the form that [SchemaFor] gives a table: lowercase names, the
// column types of the type map and the DEFAULT literals that the dedup view reads. Its
// names, types and defaults become Athena statements.
func (t Table) Check() error {
	if !namePattern.MatchString(t.Name) {
		return fmt.Errorf("table name %q isn't lowercase letters, digits and _", t.Name)
	}
	for _, c := range t.Columns {
		switch {
		case !namePattern.MatchString(c.Name):
			return fmt.Errorf("table %s: column name %q isn't lowercase letters, digits and _", t.Name, c.Name)
		case !typePattern.MatchString(c.Type):
			return fmt.Errorf("table %s: column %s has type %q, which isn't an archive column type", t.Name, c.Name, c.Type)
		case c.Default != nil && !literalPattern.MatchString(*c.Default):
			return fmt.Errorf("table %s: column %s has DEFAULT %q, which isn't an archive literal", t.Name, c.Name, *c.Default)
		}
	}
	return nil
}

func table(s *catalog.Stream) Table {
	str := catalog.Type{Kind: catalog.KindString}
	bookkeeping := []catalog.Column{
		{Name: ColumnShard, Type: str},
		{Name: ColumnSeq, Type: str},
		{Name: ColumnArrival, Type: catalog.Type{Kind: catalog.KindDateTime64, Precision: catalog.DateTime64Micros}},
	}
	t := Table{Name: catalog.IcebergTable(s.Name)}
	for _, c := range slices.Concat(catalog.ImplicitColumns(), s.Columns, bookkeeping) {
		col := Column{Name: c.Name, Type: columnType(c.Type)}
		if c.Default != nil {
			col.Default = new(literal(c.Default))
		}
		t.Columns = append(t.Columns, col)
	}
	return t
}

// columnType maps a Dynaflux type to an Athena DDL type. Nullable(T) maps as T: every
// column is optional.
func columnType(t catalog.Type) string {
	var s string
	switch t.Kind {
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindInt8, catalog.KindInt16, catalog.KindInt32:
		s = "int"
	case catalog.KindUInt32, catalog.KindInt64:
		s = "bigint"
	case catalog.KindUInt64:
		// A bigint can't hold 2^64 - 1.
		s = "decimal(20,0)"
	case catalog.KindDecimal:
		s = "decimal(" + strconv.Itoa(t.Precision) + "," + strconv.Itoa(t.Scale) + ")"
	case catalog.KindFloat64:
		s = "double"
	case catalog.KindBool:
		s = "boolean"
	case catalog.KindDate:
		s = "date"
	case catalog.KindDateTime, catalog.KindDateTime64:
		s = "timestamp"
	case catalog.KindString, catalog.KindUUID:
		s = "string"
	}
	if t.Array {
		return "array<" + s + ">"
	}
	return s
}

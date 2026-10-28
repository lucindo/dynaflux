// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// CreateTable returns the Athena DDL that creates t in database db at location, which holds
// no quote: an Iceberg table of Parquet files compressed with zstd, partitioned by the day
// of event_time.
func CreateTable(db, location string, t Table) string {
	return "CREATE TABLE " + ddlIdent(db) + "." + ddlIdent(t.Name) + " (\n  " + columnDefs(t.Columns, ",\n  ") + ")\n" +
		"PARTITIONED BY (" + partitionTransform + "(" + ddlIdent(catalog.ColumnEventTime) + "))\n" +
		"LOCATION '" + location + "'\n" +
		"TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet', 'write_compression' = 'zstd')"
}

// AddColumns returns the Athena DDL that adds cols, which must not be empty, to t in
// database db.
func AddColumns(db string, t Table, cols []Column) string {
	return "ALTER TABLE " + ddlIdent(db) + "." + ddlIdent(t.Name) + " ADD COLUMNS (" + columnDefs(cols, ", ") + ")"
}

// DedupView returns the Athena statement that creates or replaces the view of t in
// database db with one row per event: of an event's copies, the first to arrive. A column
// with a DEFAULT reads as its DEFAULT in rows archived before the column was added. The
// view leaves out the bookkeeping columns.
func DedupView(db string, t Table) string {
	var cols []string
	for _, c := range t.Columns {
		name := dmlIdent(c.Name)
		switch {
		case c.Name == ColumnShard || c.Name == ColumnSeq || c.Name == ColumnArrival:
		case c.Default != nil:
			cols = append(cols, "coalesce("+name+", "+*c.Default+") AS "+name)
		default:
			cols = append(cols, name)
		}
	}
	// Copies of an event share event_time, so partitioning the window by it too lets a
	// filter on event_time prune day partitions.
	return fmt.Sprintf(`CREATE OR REPLACE VIEW %s.%s AS
SELECT
  %s
FROM (
  SELECT *, row_number() OVER (
    PARTITION BY %s, %s ORDER BY %s, %s, %s) AS "_dedup_rn"
  FROM %s.%s)
WHERE "_dedup_rn" = 1`,
		dmlIdent(db), dmlIdent(catalog.DedupView(t.Name)), strings.Join(cols, ",\n  "),
		dmlIdent(catalog.ColumnEventTime), dmlIdent(catalog.ColumnEventID),
		dmlIdent(ColumnArrival), dmlIdent(ColumnShard), dmlIdent(ColumnSeq), dmlIdent(db), dmlIdent(t.Name))
}

// Unload returns the Athena statement that exports the records of s's table in database db
// whose event_time is in [from, to) to dest, an S3 prefix, as gzip JSON files (ADR 0016
// §5). It reads the columns of the binary's catalog, _arrival too, by which a backfill leaves
// out the records of an older stream table. 64-bit integers, decimals, dates and times are
// cast to text, whose form the decoder knows, since Athena doesn't document the JSON it
// writes for them.
func Unload(db string, s *catalog.Stream, from, to time.Time, dest string) string {
	var cols []string
	for _, c := range columns(s) {
		name := dmlIdent(c.Name)
		switch {
		case !castToText(c.Type.Kind):
			cols = append(cols, name)
		case c.Type.Array:
			cols = append(cols, "transform("+name+", x -> CAST(x AS varchar)) AS "+name)
		default:
			cols = append(cols, "CAST("+name+" AS varchar) AS "+name)
		}
	}
	at := dmlIdent(catalog.ColumnEventTime)
	return fmt.Sprintf(`UNLOAD (
  SELECT
    %s
  FROM %s.%s
  WHERE %s >= TIMESTAMP '%s' AND %s < TIMESTAMP '%s')
TO %s
WITH (format = 'JSON')`,
		strings.Join(cols, ",\n    "), dmlIdent(db), dmlIdent(catalog.IcebergTable(s.Name)),
		at, from.UTC().Format(timestampLayout), at, to.UTC().Format(timestampLayout), sqlString(dest))
}

// PartitionsQuery returns the Athena query of the day partitions of s's table in database
// db: each day as text, then its record count and the bytes of its data files.
func PartitionsQuery(db string, s *catalog.Stream) string {
	// Iceberg names a partition field after its source column and transform.
	day := dmlIdent(catalog.ColumnEventTime + "_" + partitionTransform)
	return `SELECT CAST("partition".` + day + ` AS varchar) AS "day", "record_count", "total_size"` + "\n" +
		"FROM " + dmlIdent(db) + "." + dmlIdent(catalog.IcebergTable(s.Name)+"$partitions")
}

// timestampLayout writes a timestamp(6) literal.
const timestampLayout = "2006-01-02 15:04:05.000000"

// castToText reports whether an export casts a column of kind k to text: the bigint,
// decimal, date and timestamp columns.
func castToText(k catalog.Kind) bool {
	switch k {
	case catalog.KindUInt32, catalog.KindInt64, catalog.KindUInt64, catalog.KindDecimal,
		catalog.KindDate, catalog.KindDateTime, catalog.KindDateTime64:
		return true
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindInt8, catalog.KindInt16, catalog.KindInt32,
		catalog.KindFloat64, catalog.KindBool, catalog.KindString, catalog.KindUUID:
	}
	return false
}

func columnDefs(cols []Column, sep string) string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = ddlIdent(c.Name) + " " + c.Type
	}
	return strings.Join(defs, sep)
}

// ddlIdent quotes a name for DDL, and dmlIdent for queries. Every name is quoted: Athena
// needs it for names that start with "_" and for reserved words such as date, which are
// valid column names.
func ddlIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func dmlIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// literal writes a column DEFAULT, an OpLiteral or an OpArray of them, as an Athena
// literal. Typed literals keep every value exact, such as a UInt64 above the bigint range
// or a DateTime64(6) with its microseconds.
func literal(e *catalog.Expr) string {
	if e.Op == catalog.OpArray {
		elems := make([]string, len(e.Args))
		for i := range e.Args {
			elems[i] = literal(&e.Args[i])
		}
		return "ARRAY[" + strings.Join(elems, ", ") + "]"
	}
	var typ string
	switch e.Type.Kind {
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindInt8, catalog.KindInt16, catalog.KindInt32:
		typ = "INTEGER "
	case catalog.KindUInt32, catalog.KindInt64:
		typ = "BIGINT "
	case catalog.KindUInt64, catalog.KindDecimal:
		typ = "DECIMAL "
	case catalog.KindFloat64:
		typ = "DOUBLE "
	case catalog.KindBool:
		return e.Value
	case catalog.KindDate:
		typ = "DATE "
	case catalog.KindDateTime, catalog.KindDateTime64:
		typ = "TIMESTAMP "
	case catalog.KindString, catalog.KindUUID:
	}
	return typ + sqlString(e.Value)
}

// sqlString quotes s as an Athena string literal. Control characters become Unicode
// escapes, so no statement carries one through templates, JSON and the Athena API.
func sqlString(s string) string {
	if !strings.ContainsFunc(s, unicode.IsControl) {
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
	var b strings.Builder
	b.WriteString("U&'")
	for _, r := range s {
		switch {
		case r == '\'':
			b.WriteString("''")
		case r == '\\':
			b.WriteString(`\\`)
		case unicode.IsControl(r):
			fmt.Fprintf(&b, `\%04X`, r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

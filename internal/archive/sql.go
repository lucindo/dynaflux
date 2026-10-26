// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"fmt"
	"strings"
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

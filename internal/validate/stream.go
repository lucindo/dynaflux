// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/sql"
)

const (
	minShards   = 2
	dedupSuffix = "_dedup"
)

// streamInfo is a compiled stream.
type streamInfo struct {
	stream catalog.Stream
	// badColumns are the declared columns with an invalid name or type: what reads them
	// is reported already.
	badColumns map[string]bool
	// inherited is set when LOG RETENTION comes from defaults.log_retention.
	inherited bool
}

// compileStream checks a CREATE STREAM (sql.md §8) and compiles it.
func (p *project) compileStream(d *decl, s *sql.CreateStream) {
	st := d.st
	info := &streamInfo{badColumns: map[string]bool{}}
	cs := catalog.Stream{Name: s.Name.Name, GoType: goType(s), StorageFormat: catalog.StorageFormatVersion}
	if strings.HasSuffix(cs.Name, dedupSuffix) {
		st.errorf(s.Name.Pos, "DF2101", "the archive uses "+cs.Name+" for its deduplicated Athena view",
			"stream name %s ends with %s", cs.Name, dedupSuffix)
	}
	cs.Columns = streamColumns(st, s, info.badColumns)
	if s.PrimaryKey == nil {
		st.errorf(s.Pos, "DF2102", "add PRIMARY KEY ("+s.Columns[0].Name.Name+"): the columns that group events for reading",
			"stream has no PRIMARY KEY")
	} else {
		cs.PrimaryKey = keyColumns(st, s.PrimaryKey, "PRIMARY KEY", cs.Columns, info.badColumns)
	}
	if s.DedupBy != nil {
		cs.DedupBy = keyColumns(st, s.DedupBy, "DEDUP BY", cs.Columns, info.badColumns)
	}
	info.inherited = s.LogRetention == nil
	cs.LogRetention = logRetention(st, s.LogRetention, p.cfg.Defaults.LogRetention)
	cs.Accept = acceptWindow(st, s.Accept)
	cs.Shards = shards(st, s.Shards)
	if s.Consistency != nil {
		st.errorf(s.Consistency.Pos, "DF2108", "remove it; streams are eventually consistent across regions",
			"CONSISTENCY is reserved for consistent streams (v3)")
	}
	cs.Archive = p.archive(cs.Name)
	info.stream = cs
	if d.earlier == nil {
		d.stream = info
	}
}

// goType returns the valid -- go:type of a stream, or "".
func goType(s *sql.CreateStream) string {
	for _, a := range s.Annotations {
		if a.Key == sql.AnnotationGoType && !a.Bad && isGoName(a.Name) {
			return a.Name
		}
	}
	return ""
}

func streamColumns(st *stmt, s *sql.CreateStream, bad map[string]bool) []catalog.Column {
	reserved := []string{
		catalog.ColumnEventID, catalog.ColumnEventTime, catalog.ColumnIngestTime, catalog.ColumnOriginRegion,
		catalog.AttrPK, catalog.AttrSK,
	}
	var cols []catalog.Column
	seen := map[string]bool{}
	for _, col := range s.Columns {
		name := col.Name.Name
		valid := checkName(st, col.Name, reserved...)
		dup := seen[name]
		if dup {
			st.errorf(col.Name.Pos, "DF2001", "", "duplicate column %s", name)
		}
		seen[name] = true
		t, typeOK := resolveType(st, col.Type)
		if !dup && (!valid || !typeOK) {
			bad[name] = true
		}
		for _, o := range col.Options {
			st.errorf(o.Pos, "DF2002", "columns take only DEFAULT; for NULL, use Nullable("+st.text(col.Type)+")",
				"column option %s is not supported", o.Name)
		}
		if col.PII != nil {
			st.errorf(col.PII.Pos, "DF2005", "remove the tag", "PII column tags are reserved for v2")
		}
		def := columnDefault(st, col, t, typeOK)
		if valid && typeOK && !dup {
			cols = append(cols, catalog.Column{Name: name, Type: t, Default: def})
		}
	}
	return cols
}

// keyColumns checks a PRIMARY KEY or DEDUP BY list (sql.md §8.4, §8.5).
func keyColumns(st *stmt, list *sql.NameList, clause string, cols []catalog.Column, bad map[string]bool) []string {
	var names []string
	for _, id := range list.Names {
		name := id.Name
		i := slices.IndexFunc(cols, func(c catalog.Column) bool { return c.Name == name })
		switch {
		case qualified(st, id):
		case slices.Contains(names, name):
			st.errorf(id.Pos, "DF2106", "", "%s appears twice in %s", name, clause)
		case bad[name]:
		case i < 0:
			st.errorf(id.Pos, "DF2103", "implicit columns can't be keys",
				"%s column %s is not a declared column of the stream", clause, name)
		case cols[i].Type.Nullable:
			st.errorf(id.Pos, "DF2104", "declare it without Nullable", "key column %s is Nullable", name)
		case !isKeyType(cols[i].Type):
			st.errorf(id.Pos, "DF2105", "keys use String, integers, Decimal, Bool, UUID, Date or DateTime64",
				"key column %s has type %s, which can't be part of a key", name, cols[i].Type)
		}
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	return names
}

func shards(st *stmt, c *sql.IntClause) int {
	if c == nil {
		return 1
	}
	if c.Value.Int < minShards || c.Value.Int > catalog.MaxShards {
		st.errorf(c.Pos, "DF2107", "omit SHARDS for one shard", "SHARDS must be between %d and %d", minShards, catalog.MaxShards)
		return 1
	}
	return int(c.Value.Int)
}

// archive places a stream's archive, or returns nil when it isn't archived.
func (p *project) archive(stream string) *catalog.Archive {
	a := p.cfg.Archive
	enabled := a.Enabled
	if i := slices.IndexFunc(a.Streams, func(s config.ArchiveStream) bool { return s.Name == stream }); i >= 0 {
		enabled = a.Streams[i].Enabled
	}
	if !enabled {
		return nil
	}
	return &catalog.Archive{Regions: a.Regions, Events: a.Events}
}

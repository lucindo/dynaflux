// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// maxIDLen is the longest CloudFormation logical ID.
const maxIDLen = 255

const (
	scopeGlobal   = "global"
	scopeRegional = "regional"

	kindTable            = "table"
	kindBucket           = "bucket"
	kindLogGroup         = "log group"
	kindLogStream        = "log stream"
	kindRole             = "role"
	kindFunction         = "function"
	kindStreamLookup     = "stream lookup"
	kindMapping          = "mapping"
	kindArchiveWorkGroup = "archive workgroup"
	kindArchiveSchema    = "archive schema"
	kindDeliveryStream   = "delivery stream"
	kindAlarm            = "alarm"
	kindDashboard        = "dashboard"
)

// Check verifies the structural invariants that every manifest from [Build] meets. Values
// that Build fixes are pinned by the template goldens instead.
//
//  1. IDs are unique across both scopes and are an uppercase letter followed by at most
//     254 letters and digits.
//  2. Every reference names an existing resource and an attribute its kind provides, and
//     refers across scopes only from a regional resource to a global table.
//  3. Texts hold only the substitutions that [Text] lists.
//  4. A required field of a resource that isn't archive-only never refers to one that is:
//     the field would be missing in the regions that don't archive. Nor do a delivery
//     stream's Schema, which must name an archive schema, and an archive schema's After,
//     which name resources of its scope to wait for. Archive-only resources and statements
//     exist only when ArchiveRegions is set.
//  5. Prefix.MaxLen is at least the length of Prefix.Default, so every name fits with the
//     configured prefix.
//  6. Every archive table location ends in "/iceberg/", lies under the location of the
//     schema's database, and without its final "/" isn't a prefix of another table's
//     location.
//  7. A dashboard body holds only ${prefix}, ${region}, ${account} and ${partition}: it
//     can't wait for a resource.
func (m *Manifest) Check() error {
	es := m.entries()
	var errs []error
	byID := map[string]entry{}
	for _, e := range es {
		if !validID(e.id) {
			errs = append(errs, fmt.Errorf("ID %q isn't an uppercase letter followed by at most %d letters and digits", e.id, maxIDLen-1))
		}
		if _, dup := byID[e.id]; dup {
			errs = append(errs, fmt.Errorf("ID %s is used twice", e.id))
		}
		byID[e.id] = e
	}
	for _, e := range es {
		for _, f := range e.fields {
			errs = append(errs, checkText(e, f, byID)...)
		}
		if e.archiveOnly && len(m.ArchiveRegions) == 0 {
			errs = append(errs, fmt.Errorf("%s is archive-only, but the manifest has no archive regions", e.id))
		}
	}
	archives := len(m.ArchiveRegions) > 0
	errs = append(errs, m.Global.checkArchive(byID, scopeGlobal, archives)...)
	errs = append(errs, m.Regional.checkArchive(byID, scopeRegional, archives)...)
	for _, d := range slices.Concat(m.Global.Dashboards, m.Regional.Dashboards) {
		if err := checkDashboardBody(d.Body); err != nil {
			errs = append(errs, fmt.Errorf("%s body: %w", d.ID, err))
		}
	}
	if m.Prefix.MaxLen < len(m.Prefix.Default) {
		errs = append(errs, fmt.Errorf("some name allows a prefix of at most %d characters, shorter than the prefix %q",
			m.Prefix.MaxLen, m.Prefix.Default))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("infra: the manifest breaks its invariants:\n%w", err)
	}
	return nil
}

// entry is what Check needs of one resource.
type entry struct {
	scope       string
	kind        string
	id          string
	archiveOnly bool
	name        Text     // empty for kinds without a name
	attrs       []string // the attributes it provides to references
	fields      []field
}

// field is a text field of a resource. A required field can't be left out in the regions
// where a resource it refers to doesn't exist.
type field struct {
	name     string
	text     Text
	required bool
}

func required(name string, text Text) field {
	return field{name: name, text: text, required: true}
}

func (m *Manifest) entries() []entry {
	return append(m.Global.entries(scopeGlobal), m.Regional.entries(scopeRegional)...)
}

// IDs returns the IDs of the resources in s.
func (s *Scope) IDs() []string {
	var ids []string
	for _, e := range s.entries("") {
		ids = append(ids, e.id)
	}
	return ids
}

// ArchiveOnlyIDs returns the IDs of the archive-only resources in s.
func (s *Scope) ArchiveOnlyIDs() []string {
	var ids []string
	for _, e := range s.entries("") {
		if e.archiveOnly {
			ids = append(ids, e.id)
		}
	}
	return ids
}

func (s *Scope) entries(scope string) []entry {
	var es []entry
	add := func(kind string, c Common, name Text, attrs []string, fields ...field) {
		es = append(es, entry{scope: scope, kind: kind, id: c.ID, archiveOnly: c.ArchiveOnly, name: name, attrs: attrs, fields: fields})
	}
	named := []string{AttrName, AttrARN}
	for _, t := range s.Tables {
		attrs := named
		if t.Role == TableStream {
			attrs = []string{AttrName, AttrARN, AttrStreamARNPattern}
		}
		add(kindTable, t.Common, t.Name, attrs, required("name", t.Name))
	}
	for _, x := range s.Buckets {
		add(kindBucket, x.Common, x.Name, named, required("name", x.Name))
	}
	for _, x := range s.LogGroups {
		add(kindLogGroup, x.Common, x.Name, named, required("name", x.Name))
	}
	for _, x := range s.LogStreams {
		add(kindLogStream, x.Common, Text(x.Name), []string{AttrName}, required("group", x.Group))
	}
	for _, x := range s.Roles {
		add(kindRole, x.Common, "", []string{AttrARN}, statementFields(x.Statements)...)
	}
	for _, x := range s.Functions {
		add(kindFunction, x.Common, x.Name, named, functionFields(x)...)
	}
	for _, x := range s.StreamLookups {
		add(kindStreamLookup, x.Common, "", []string{AttrStreamARN}, required("handler", x.Handler), required("table", x.Table))
	}
	for _, x := range s.Mappings {
		add(kindMapping, x.Common, "", []string{AttrUUID},
			required("function", x.Function), required("source", x.Source), required("on_failure", x.OnFailure))
	}
	for _, x := range s.ArchiveWorkGroups {
		add(kindArchiveWorkGroup, x.Common, x.Name, []string{AttrName}, required("name", x.Name))
	}
	for _, x := range s.ArchiveSchemas {
		add(kindArchiveSchema, x.Common, "", []string{AttrFingerprint}, schemaFields(x)...)
	}
	for _, x := range s.DeliveryStreams {
		add(kindDeliveryStream, x.Common, x.Name, named, required("name", x.Name), required("catalog", x.Catalog),
			required("database", x.Database), required("role", x.Role), required("bucket", x.Bucket),
			required("log_group", x.LogGroup), required("log_stream", x.LogStream))
	}
	for _, x := range s.Alarms {
		fs := []field{required("name", x.Name)}
		for _, m := range x.Metrics {
			fs = append(fs, dimensionFields(m.Dimensions)...)
		}
		add(kindAlarm, x.Common, x.Name, nil, fs...)
	}
	for _, x := range s.Dashboards {
		add(kindDashboard, x.Common, x.Name, nil, required("name", x.Name))
	}
	return es
}

func functionFields(x Function) []field {
	fs := []field{required("name", x.Name), required("role", x.Role), required("log_group", x.LogGroup)}
	for _, k := range slices.Sorted(maps.Keys(x.Env)) {
		fs = append(fs, field{name: "env " + k, text: x.Env[k]})
	}
	return fs
}

func schemaFields(x ArchiveSchema) []field {
	fs := []field{
		required("handler", x.Handler), required("database", x.Database), required("database location", x.DatabaseLocation),
		required("workgroup", x.WorkGroup), required("optimizer role", x.Optimizers.Role),
	}
	for _, t := range x.Tables {
		fs = append(fs, required("location of "+t.Name, t.Location))
	}
	return fs
}

// statementFields returns the texts of statements. Those of an archive-only statement aren't
// required: the statement is left out where the resources they refer to don't exist.
func statementFields(statements []Statement) []field {
	var fs []field
	for _, st := range statements {
		texts := slices.Clone(st.Resources)
		for _, c := range st.Conditions {
			texts = append(texts, c.Values...)
		}
		for _, t := range texts {
			fs = append(fs, field{name: "statement " + st.Sid, text: t, required: !st.ArchiveOnly})
		}
	}
	return fs
}

func dimensionFields(ds []Dimension) []field {
	fs := make([]field, 0, len(ds))
	for _, d := range ds {
		fs = append(fs, required("dimension "+d.Name, d.Value))
	}
	return fs
}

// checkArchive checks invariants 4 and 6 for the archive resources of s, in scope.
func (s *Scope) checkArchive(byID map[string]entry, scope string, archiveRegions bool) []error {
	errs := s.checkOrder(byID, scope)
	for _, r := range s.Roles {
		for _, st := range r.Statements {
			if st.ArchiveOnly && !archiveRegions {
				errs = append(errs, fmt.Errorf("%s statement %s is archive-only, but the manifest has no archive regions", r.ID, st.Sid))
			}
		}
	}
	return append(errs, s.checkLocations()...)
}

// checkOrder checks that each resource that a delivery stream or an archive schema waits for
// exists in its scope wherever it does, and that a delivery stream waits for an archive schema.
func (s *Scope) checkOrder(byID map[string]entry, scope string) []error {
	var errs []error
	check := func(c Common, field, id string) {
		t, ok := byID[id]
		switch {
		case !ok || t.scope != scope:
			errs = append(errs, fmt.Errorf("%s %s %s isn't a resource of its scope", c.ID, field, id))
		case !c.ArchiveOnly && t.archiveOnly:
			errs = append(errs, fmt.Errorf("%s %s %s exists only in archive regions", c.ID, field, id))
		}
	}
	for _, x := range s.DeliveryStreams {
		if t, ok := byID[x.Schema]; ok && t.kind != kindArchiveSchema {
			errs = append(errs, fmt.Errorf("%s schema %s isn't an archive schema", x.ID, x.Schema))
		}
		check(x.Common, "schema", x.Schema)
	}
	for _, x := range s.ArchiveSchemas {
		for _, id := range x.After {
			check(x.Common, "after", id)
		}
	}
	return errs
}

// checkLocations checks invariant 6: anything that lists a table's files, such as orphan
// file removal, could otherwise reach another table's.
func (s *Scope) checkLocations() []error {
	var errs []error
	for _, x := range s.ArchiveSchemas {
		dbLocation := string(x.DatabaseLocation)
		for i, t := range x.Tables {
			loc := string(t.Location)
			if !strings.HasSuffix(loc, "/iceberg/") || !strings.HasPrefix(loc, dbLocation) {
				errs = append(errs, fmt.Errorf("%s table %s is at %s, which isn't an iceberg/ folder under its database's location %s",
					x.ID, t.Name, loc, dbLocation))
			}
			for j, u := range x.Tables {
				if i != j && strings.HasPrefix(string(u.Location), strings.TrimSuffix(loc, "/")) {
					errs = append(errs, fmt.Errorf("%s table %s is at %s, a key prefix of table %s at %s", x.ID, t.Name, loc, u.Name, u.Location))
				}
			}
		}
	}
	return errs
}

func checkText(e entry, f field, byID map[string]entry) []error {
	segs, err := f.text.Segments()
	if err != nil {
		return []error{fmt.Errorf("%s %s: %w", e.id, f.name, err)}
	}
	var errs []error
	for _, s := range segs {
		r := s.Ref
		if r.ID == "" {
			continue
		}
		t, ok := byID[r.ID]
		switch {
		case !ok:
			errs = append(errs, fmt.Errorf("%s %s refers to %s, which doesn't exist", e.id, f.name, r.ID))
		case !slices.Contains(t.attrs, r.Attr):
			errs = append(errs, fmt.Errorf("%s %s refers to %s.%s, but a %s has only %s", e.id, f.name, r.ID, r.Attr, t.kind, strings.Join(t.attrs, ", ")))
		case t.scope != e.scope && (e.scope != scopeRegional || t.kind != kindTable):
			errs = append(errs, fmt.Errorf("%s %s refers to %s across scopes; only regional resources may refer to global tables", e.id, f.name, r.ID))
		case f.required && !e.archiveOnly && t.archiveOnly:
			errs = append(errs, fmt.Errorf("%s %s refers to %s, which exists only in archive regions", e.id, f.name, r.ID))
		}
	}
	return errs
}

func validID(id string) bool {
	if id == "" || len(id) > maxIDLen || id[0] < 'A' || id[0] > 'Z' {
		return false
	}
	return !strings.ContainsFunc(id, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9')
	})
}

// checkDashboardBody checks invariant 7 for a dashboard's body.
func checkDashboardBody(body Text) error {
	segs, err := body.Segments()
	if err != nil {
		return err
	}
	for _, s := range segs {
		if s.Ref.ID != "" || s.Placeholder == PlaceholderPrefixUnderscore {
			return fmt.Errorf("has %s, but a dashboard may hold only ${%s}, ${%s}, ${%s} and ${%s}", substitution(s),
				PlaceholderPrefix, PlaceholderRegion, PlaceholderAccount, PlaceholderPartition)
		}
	}
	return nil
}

// substitution returns a placeholder or reference segment as a text spells it.
func substitution(s Segment) string {
	if s.Ref.ID != "" {
		return string(ref(s.Ref.ID, s.Ref.Attr))
	}
	return placeholder(s.Placeholder)
}

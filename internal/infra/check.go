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
	scopeQuery    = "query"

	kindTable            = "table"
	kindBucket           = "bucket"
	kindLogGroup         = "log group"
	kindLogStream        = "log stream"
	kindRole             = "role"
	kindManagedPolicy    = "managed policy"
	kindFunction         = "function"
	kindFunctionURL      = "function URL"
	kindPermission       = "permission"
	kindAPI              = "REST or HTTP API"
	kindStage            = "API stage"
	kindAuthorizer       = "authorizer"
	kindUsagePlan        = "usage plan"
	kindCertificate      = "certificate"
	kindDomain           = "custom domain"
	kindHealthCheck      = "health check"
	kindLatencyRecord    = "latency record"
	kindOutput           = "output"
	kindStreamLookup     = "stream lookup"
	kindMapping          = "mapping"
	kindCatalog          = "catalog registration"
	kindStateMachine     = "state machine"
	kindArchiveWorkGroup = "archive workgroup"
	kindArchiveSchema    = "archive schema"
	kindDeliveryStream   = "delivery stream"
	kindAlarm            = "alarm"
	kindDashboard        = "dashboard"
)

// Check verifies the structural invariants that every manifest from [Build] meets. Values
// that Build fixes are pinned by the template goldens instead.
//
//  1. IDs are unique across the scopes and are an uppercase letter followed by at most 254
//     letters and digits.
//  2. Every reference, outputs' included, names an existing resource and an attribute its
//     kind provides, and refers across scopes only to a table: from a regional resource to
//     a global one, and from a query resource to either.
//  3. Texts hold only the substitutions that [Text] lists.
//  4. A required field of a resource that isn't archive-only never refers to one that is:
//     the field would be missing in the regions that don't archive. Nor do a delivery
//     stream's Schema, which must name an archive schema, and the After of an archive schema
//     or a catalog registration, which name resources of its scope to wait for.
//     Archive-only resources and statements exist only when ArchiveRegions is set.
//  5. Prefix.MaxLen is at least the length of Prefix.Default, so every name fits with the
//     configured prefix.
//  6. Every archive table location ends in "/iceberg/", lies under the location of the
//     schema's database, and without its final "/" isn't a prefix of another table's
//     location.
//  7. A dashboard body holds only ${prefix}, ${region}, ${account} and ${partition}: it
//     can't wait for a resource.
//  8. A catalog registration waits for every function of its scope but its handler, and
//     every managed policy: the catalog becomes current once each function runs it with
//     all its grants, and a managed policy attaches to its role, not to the function.
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
	for _, s := range m.scopes() {
		errs = append(errs, s.Scope.checkArchive(byID, s.name, archives)...)
		for _, o := range s.Outputs {
			errs = append(errs, checkText(entry{scope: s.name, kind: kindOutput, id: o.Name}, required("value", o.Value), byID)...)
		}
	}
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

// namedScope is a scope of a manifest and its name.
type namedScope struct {
	*Scope

	name string
}

func (m *Manifest) scopes() []namedScope {
	return []namedScope{{&m.Global, scopeGlobal}, {&m.Regional, scopeRegional}, {&m.Query, scopeQuery}}
}

func (m *Manifest) entries() []entry {
	var es []entry
	for _, s := range m.scopes() {
		es = append(es, s.entries(s.name)...)
	}
	return es
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
	for _, x := range s.Catalogs {
		add(kindCatalog, x.Common, "", nil, required("handler", x.Handler), required("meta table", x.MetaTable))
	}
	for _, x := range s.StateMachines {
		add(kindStateMachine, x.Common, x.Name, []string{AttrARN}, required("name", x.Name), required("role", x.Role),
			required("log_group", x.LogGroup), required("function", x.Function))
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
	return slices.Concat(es, s.managedEntries(scope), s.doorEntries(scope))
}

// doorEntries returns the entries of s's front doors: function URLs, permissions, APIs with
// their stages and authorizers, usage plans and custom domains.
func (s *Scope) doorEntries(scope string) []entry {
	var es []entry
	add := func(kind string, c Common, name Text, attrs []string, fields ...field) {
		es = append(es, entry{scope: scope, kind: kind, id: c.ID, archiveOnly: c.ArchiveOnly, name: name, attrs: attrs, fields: fields})
	}
	id := []string{AttrID}
	for _, x := range s.FunctionURLs {
		add(kindFunctionURL, x.Common, "", []string{AttrURL}, required("function", x.Function))
	}
	for _, x := range s.Permissions {
		add(kindPermission, x.Common, "", nil, required("function", x.Function), required("source ARN", x.SourceARN))
	}
	for _, x := range s.APIs {
		add(kindAPI, x.Common, x.Name, id, required("name", x.Name), required("function", x.Function))
		add(kindStage, Common{ID: x.Stage.ID, ArchiveOnly: x.ArchiveOnly}, Text(x.Stage.Name), []string{AttrName},
			required("log group", x.Stage.LogGroup))
		if a := x.Authorizer; a != nil {
			add(kindAuthorizer, Common{ID: a.ID, ArchiveOnly: x.ArchiveOnly}, "", id, required("function", a.Function))
		}
	}
	for _, x := range s.UsagePlans {
		add(kindUsagePlan, x.Common, x.Name, id, required("name", x.Name), required("API", x.API), required("stage", x.Stage))
	}
	for _, x := range s.Certificates {
		add(kindCertificate, x.Common, "", []string{AttrARN}, required("domain", x.Domain), required("hosted zone", x.HostedZone))
	}
	for _, x := range s.Domains {
		add(kindDomain, x.Common, x.Name, []string{AttrRegionalName, AttrRegionalZone}, required("name", x.Name),
			required("certificate", x.Certificate), required("API", x.API), required("stage", x.Stage))
	}
	for _, x := range s.HealthChecks {
		add(kindHealthCheck, x.Common, "", id, required("host", x.Host))
	}
	for _, x := range s.LatencyRecords {
		add(kindLatencyRecord, x.Common, x.Name, nil, required("name", x.Name), required("hosted zone", x.HostedZone),
			required("target", x.Target), required("target zone", x.TargetZone), required("health check", x.HealthCheck))
	}
	return es
}

// managedEntries returns the entries of the managed policies of s's roles.
func (s *Scope) managedEntries(scope string) []entry {
	var es []entry
	for _, r := range s.Roles {
		for _, p := range r.Managed {
			es = append(es, entry{scope: scope, kind: kindManagedPolicy, id: p.ID, archiveOnly: r.ArchiveOnly, fields: statementFields(p.Statements)})
		}
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

// checkOrder checks that each resource that a delivery stream, an archive schema or a
// catalog registration waits for exists in its scope wherever it does, and that a delivery
// stream waits for an archive schema.
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
	for _, x := range s.Catalogs {
		for _, id := range x.After {
			check(x.Common, "after", id)
		}
		errs = append(errs, s.checkCatalogWaits(x)...)
	}
	return errs
}

// checkCatalogWaits checks invariant 8 for the catalog registration x of s.
func (s *Scope) checkCatalogWaits(x CatalogRegistration) []error {
	var waits []string
	for _, f := range s.Functions {
		if x.Handler != ref(f.ID, AttrARN) {
			waits = append(waits, f.ID)
		}
	}
	var errs []error
	for _, id := range append(waits, s.managedPolicyIDs()...) {
		if !slices.Contains(x.After, id) {
			errs = append(errs, fmt.Errorf("%s is registered before %s", x.ID, id))
		}
	}
	return errs
}

// managedPolicyIDs returns the IDs of the managed policies of s's roles.
func (s *Scope) managedPolicyIDs() []string {
	var ids []string
	for _, r := range s.Roles {
		for _, p := range r.Managed {
			ids = append(ids, p.ID)
		}
	}
	return ids
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
		case t.scope != e.scope && !crossScope(e.scope, t):
			errs = append(errs, fmt.Errorf("%s %s refers to %s across scopes; only a regional resource may refer to a global "+
				"table, and a query resource to a global or regional one", e.id, f.name, r.ID))
		case f.required && !e.archiveOnly && t.archiveOnly:
			errs = append(errs, fmt.Errorf("%s %s refers to %s, which exists only in archive regions", e.id, f.name, r.ID))
		}
	}
	return errs
}

// crossScope reports whether a resource of scope may refer to t, of another scope: the
// regional and query stacks name the tables of the stacks deployed before them.
func crossScope(scope string, t entry) bool {
	switch scope {
	case scopeRegional:
		return t.kind == kindTable && t.scope == scopeGlobal
	case scopeQuery:
		return t.kind == kindTable
	}
	return false
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
	allowed := []string{PlaceholderPrefix, PlaceholderRegion, PlaceholderAccount, PlaceholderPartition}
	for _, s := range segs {
		if s.Ref.ID != "" || s.Placeholder != "" && !slices.Contains(allowed, s.Placeholder) {
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

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra_test

import (
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/infra"
)

// validManifest meets every invariant. Table names carry most references under test: Meta
// refers across scopes to the stream table, which is allowed, as the query role's reference
// to it is.
func validManifest() *infra.Manifest {
	return &infra.Manifest{
		Store:          "analytics",
		Regions:        []string{"us-east-1", "us-west-1"},
		ArchiveRegions: []string{"us-east-1"},
		Prefix:         infra.Prefix{Default: "prod-", MaxLen: 5},
		Global: infra.Scope{Tables: []infra.Table{
			{Common: infra.Common{ID: "StreamPageViews"}, Name: "${prefix}analytics-stream-page_views", Role: infra.TableStream},
		}},
		Regional: infra.Scope{
			Tables: []infra.Table{
				{Common: infra.Common{ID: "Meta"}, Name: "${ref:StreamPageViews.stream_arn_pattern}", Role: infra.TableMeta},
				{Common: infra.Common{ID: "Archived", ArchiveOnly: true}, Name: "${prefix_}x", Role: infra.TableView},
				{Common: infra.Common{ID: "AlsoArchived", ArchiveOnly: true}, Name: "${ref:Archived.arn}", Role: infra.TableView},
			},
			// An archive-only statement is left out where its resource isn't.
			Roles: []infra.Role{{Common: infra.Common{ID: "RoleSetup"}, Statements: []infra.Statement{
				{Sid: "Archive", ArchiveOnly: true, Resources: []infra.Text{"${ref:Archived.arn}"}},
			}, Managed: []infra.ManagedPolicy{{ID: "RoleSetupPolicy1", Statements: []infra.Statement{
				{Sid: "Tables", Resources: []infra.Text{"${ref:StreamPageViews.arn}"}},
			}}}}},
			StateMachines: []infra.StateMachine{{
				Common: infra.Common{ID: "BackfillStateMachine"}, Name: "${prefix}analytics-backfill", Role: "${ref:RoleSetup.arn}",
				LogGroup: "logs", Function: "${ref:Setup.arn}",
			}},
			Functions: []infra.Function{
				{Common: infra.Common{ID: "Setup"}, Name: "setup", Role: "${ref:RoleSetup.arn}", LogGroup: "logs"},
				{
					Common: infra.Common{ID: "ProjectorPageViews"}, Name: "projector", Mode: catalog.EnvModeProjector, Role: "${ref:RoleSetup.arn}",
					LogGroup: "logs", After: []string{"ArchiveMode"},
				},
			},
			ArchiveModes: []infra.ArchiveMode{{
				Common: infra.Common{ID: "ArchiveMode"}, Handler: "${ref:Setup.arn}", MetaTable: "${prefix}analytics-meta",
				Streams: map[string]string{"page_views": "all"},
			}},
			StreamLookups: []infra.StreamLookup{
				{Common: infra.Common{ID: "LookupPageViews"}, Handler: "${ref:Setup.arn}", Table: "${ref:StreamPageViews.name}"},
			},
			Catalogs: []infra.CatalogRegistration{{
				Common: infra.Common{ID: "Catalog"}, Handler: "${ref:Setup.arn}", MetaTable: "${prefix}analytics-meta",
				After: []string{"LookupPageViews", "ProjectorPageViews", "RoleSetupPolicy1"},
			}},
			LogStreams: []infra.LogStream{{Common: infra.Common{ID: "LogStreamPage", ArchiveOnly: true}, Group: "logs", Name: "page"}},
			ArchiveSchemas: []infra.ArchiveSchema{{
				Common: infra.Common{ID: "ArchiveSchema", ArchiveOnly: true}, Handler: "${ref:Setup.arn}",
				Database: "db", DatabaseLocation: "s3://b/warehouse/db/", WorkGroup: "wg", After: []string{"LookupPageViews"},
				Optimizers: infra.Optimizers{Role: "role"},
				Tables: []infra.ArchiveTable{
					{Name: "page", Location: "s3://b/warehouse/db/page/iceberg/"},
					{Name: "page_views", Location: "s3://b/warehouse/db/page_views/iceberg/"},
				},
			}},
			DeliveryStreams: []infra.DeliveryStream{{
				Common: infra.Common{ID: "FirehosePage", ArchiveOnly: true}, Name: "firehose", Schema: "ArchiveSchema",
				Catalog: "catalog", Database: "db", Role: "role", Bucket: "bucket", LogGroup: "logs", LogStream: "${ref:LogStreamPage.name}",
			}},
			Alarms: []infra.Alarm{{
				Common: infra.Common{ID: "AlarmErrorsSetup"}, Name: "${prefix}analytics-setup-errors",
				Metrics: []infra.Metric{{Dimensions: []infra.Dimension{{Name: "FunctionName", Value: "${ref:Setup.name}"}}}},
			}},
			Dashboards: []infra.Dashboard{{
				Common: infra.Common{ID: "Dashboard"}, Name: "${prefix}analytics-${region}",
				Body: `{"widgets":[{"type":"metric","properties":{"region":"${region}","title":"${prefix}analytics"}}]}`,
			}},
		},
		Query: infra.Scope{
			Roles: []infra.Role{{Common: infra.Common{ID: "RoleQuery"}, Statements: []infra.Statement{
				{Sid: "Tables", Resources: []infra.Text{"${ref:StreamPageViews.arn}"}},
			}}},
			Functions:    []infra.Function{{Common: infra.Common{ID: "Query"}, Name: "query", Role: "${ref:RoleQuery.arn}", LogGroup: "logs"}},
			FunctionURLs: []infra.FunctionURL{{Common: infra.Common{ID: "QueryUrl"}, Function: "${ref:Query.arn}"}},
			Permissions: []infra.Permission{{
				Common: infra.Common{ID: "PermissionUrl"}, Function: "${ref:Query.name}",
				SourceARN: "arn:${partition}:execute-api:${region}:${account}:${ref:Api.id}/authorizers/${ref:Authorizer.id}",
			}},
			APIs: []infra.API{{
				Common: infra.Common{ID: "Api"}, Name: "api", Function: "${ref:Query.arn}", Authorizer: &infra.Authorizer{ID: "Authorizer"},
				Stage: infra.Stage{ID: "Stage", Name: "api", LogGroup: "logs"},
			}},
			Domains: []infra.Domain{{
				Common: infra.Common{ID: "Domain"}, Name: "${domain}", Certificate: "certificate", API: "${ref:Api.id}", Stage: "${ref:Stage.name}",
			}},
			LatencyRecords: []infra.LatencyRecord{{
				Common: infra.Common{ID: "LatencyRecord"}, Name: "${domain}", HostedZone: "${hosted_zone}",
				Target: "${ref:Domain.regional_name}", TargetZone: "${ref:Domain.regional_zone}", HealthCheck: "check",
			}},
			Outputs: []infra.Output{{Name: "QueryEndpoint", Value: "${ref:QueryUrl.url}"}},
		},
	}
}

func TestCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(m *infra.Manifest)
		want   string // a part of the error; empty for none
	}{
		{"valid", func(*infra.Manifest) {}, ""},
		{
			"projector before the archive mode", func(m *infra.Manifest) { m.Regional.Functions[1].After = nil },
			"ProjectorPageViews, a function in mode projector, waits for an archive mode: false",
		},
		{
			"another function after the archive mode", func(m *infra.Manifest) { m.Regional.Functions[0].After = []string{"ArchiveMode"} },
			"Setup, a function in mode , waits for an archive mode: true",
		},
		{
			"function waits for a missing resource", func(m *infra.Manifest) { m.Regional.Functions[1].After = []string{"ArchiveMode", "Mode"} },
			"ProjectorPageViews after Mode isn't a resource of its scope",
		},
		{"ID of 255 characters", func(m *infra.Manifest) { m.Regional.Tables[0].ID = "M" + strings.Repeat("x", 254) }, ""},
		{
			"ID of 256 characters", func(m *infra.Manifest) { m.Regional.Tables[0].ID = "M" + strings.Repeat("x", 255) },
			"isn't an uppercase letter followed by at most 254 letters and digits",
		},
		{
			"ID with an underscore", func(m *infra.Manifest) { m.Regional.Tables[0].ID = "Meta_1" },
			`ID "Meta_1" isn't an uppercase letter`,
		},
		{
			"ID in both scopes", func(m *infra.Manifest) { m.Regional.Tables[0].ID = "StreamPageViews" },
			"ID StreamPageViews is used twice",
		},
		{
			"managed policy with a resource's ID", func(m *infra.Manifest) { m.Regional.Roles[0].Managed[0].ID = "Meta" },
			"ID Meta is used twice",
		},
		{
			"managed policy refers to a missing resource",
			func(m *infra.Manifest) { m.Regional.Roles[0].Managed[0].Statements[0].Resources[0] = "${ref:View.arn}" },
			"RoleSetupPolicy1 statement Tables refers to View, which doesn't exist",
		},
		{
			"state machine runs a missing function", func(m *infra.Manifest) { m.Regional.StateMachines[0].Function = "${ref:Backfill.arn}" },
			"BackfillStateMachine function refers to Backfill, which doesn't exist",
		},
		{
			"reference to a missing resource", func(m *infra.Manifest) { m.Regional.Tables[0].Name = "${ref:Bucket.arn}/x" },
			"Meta name refers to Bucket, which doesn't exist",
		},
		{
			"attribute the kind lacks", func(m *infra.Manifest) { m.Regional.Tables[0].Name = "${ref:StreamPageViews.uuid}" },
			"refers to StreamPageViews.uuid, but a table has only name, arn, stream_arn_pattern",
		},
		{
			"stream attribute of a view table", func(m *infra.Manifest) { m.Regional.Tables[0].Name = "${ref:Archived.stream_arn_pattern}" },
			"refers to Archived.stream_arn_pattern, but a table has only name, arn",
		},
		{
			"global resource refers to a regional one", func(m *infra.Manifest) { m.Global.Tables[0].Name = "${ref:Meta.name}" },
			"StreamPageViews name refers to Meta across scopes",
		},
		{
			"query role refers to a regional table", func(m *infra.Manifest) { m.Query.Roles[0].Statements[0].Resources[0] = "${ref:Meta.arn}" },
			"",
		},
		{
			"query role refers to a regional function",
			func(m *infra.Manifest) { m.Query.Roles[0].Statements[0].Resources[0] = "${ref:Setup.arn}" },
			"RoleQuery statement Tables refers to Setup across scopes",
		},
		{
			"regional alarm watches the query function",
			func(m *infra.Manifest) { m.Regional.Alarms[0].Metrics[0].Dimensions[0].Value = "${ref:Query.name}" },
			"AlarmErrorsSetup dimension FunctionName refers to Query across scopes",
		},
		{
			"output refers to a missing resource", func(m *infra.Manifest) { m.Query.Outputs[0].Value = "${ref:Gateway.url}" },
			"QueryEndpoint value refers to Gateway, which doesn't exist",
		},
		{
			"output reads an attribute its resource lacks", func(m *infra.Manifest) { m.Query.Outputs[0].Value = "${ref:Query.url}" },
			"QueryEndpoint value refers to Query.url, but a function has only name, arn",
		},
		{
			"permission for a missing function", func(m *infra.Manifest) { m.Query.Permissions[0].Function = "${ref:Reader.name}" },
			"PermissionUrl function refers to Reader, which doesn't exist",
		},
		{
			"permission for a missing authorizer", func(m *infra.Manifest) { m.Query.APIs[0].Authorizer = nil },
			"PermissionUrl source ARN refers to Authorizer, which doesn't exist",
		},
		{
			"API stage with a resource's ID", func(m *infra.Manifest) { m.Query.APIs[0].Stage.ID = "Query" },
			"ID Query is used twice",
		},
		{
			"latency record targets the API", func(m *infra.Manifest) { m.Query.LatencyRecords[0].Target = "${ref:Api.regional_name}" },
			"LatencyRecord target refers to Api.regional_name, but a REST or HTTP API has only id",
		},
		{
			"unknown substitution", func(m *infra.Manifest) { m.Global.Tables[0].Name = "${stage}analytics" },
			`StreamPageViews name: "${stage}analytics": unknown substitution ${stage}`,
		},
		{
			"required field refers to an archive-only resource", func(m *infra.Manifest) { m.Regional.Tables[0].Name = "${ref:Archived.name}" },
			"Meta name refers to Archived, which exists only in archive regions",
		},
		{
			"attribute a role lacks", func(m *infra.Manifest) { m.Regional.Functions[0].Role = "${ref:RoleSetup.name}" },
			"Setup role refers to RoleSetup.name, but a role has only arn",
		},
		{
			"function refers to an archive-only role", func(m *infra.Manifest) { m.Regional.Roles[0].ArchiveOnly = true },
			"Setup role refers to RoleSetup, which exists only in archive regions",
		},
		{
			"prefix longer than the names allow", func(m *infra.Manifest) { m.Prefix.MaxLen = 4 },
			`some name allows a prefix of at most 4 characters, shorter than the prefix "prod-"`,
		},
		{
			"statement everywhere refers to an archive-only resource",
			func(m *infra.Manifest) { m.Regional.Roles[0].Statements[0].ArchiveOnly = false },
			"RoleSetup statement Archive refers to Archived, which exists only in archive regions",
		},
		{
			"archive-only resource without archive regions", func(m *infra.Manifest) { m.ArchiveRegions = nil },
			"Archived is archive-only, but the manifest has no archive regions",
		},
		{
			"archive-only statement without archive regions",
			func(m *infra.Manifest) { m.ArchiveRegions, m.Regional.Tables[1].ArchiveOnly = nil, false },
			"RoleSetup statement Archive is archive-only, but the manifest has no archive regions",
		},
		{
			"delivery stream schema isn't an archive schema", func(m *infra.Manifest) { m.Regional.DeliveryStreams[0].Schema = "Setup" },
			"FirehosePage schema Setup isn't an archive schema",
		},
		{
			"delivery stream everywhere after an archive-only schema",
			func(m *infra.Manifest) { m.Regional.DeliveryStreams[0].ArchiveOnly = false },
			"FirehosePage schema ArchiveSchema exists only in archive regions",
		},
		{
			"table location without iceberg/",
			func(m *infra.Manifest) {
				m.Regional.ArchiveSchemas[0].Tables[1].Location = "s3://b/warehouse/db/page_views/"
			},
			"ArchiveSchema table page_views is at s3://b/warehouse/db/page_views/, which isn't an iceberg/ folder under its database's location s3://b/warehouse/db/",
		},
		{
			"table location outside its database",
			func(m *infra.Manifest) {
				m.Regional.ArchiveSchemas[0].Tables[1].Location = "s3://b/warehouse/other/page_views/iceberg/"
			},
			"ArchiveSchema table page_views is at s3://b/warehouse/other/page_views/iceberg/, which isn't an iceberg/ folder",
		},
		{
			// Without the iceberg/ folder, page's location is a key prefix of page_views'.
			"table locations overlap",
			func(m *infra.Manifest) {
				m.Regional.ArchiveSchemas[0].Tables[0].Location = "s3://b/warehouse/db/page/"
				m.Regional.ArchiveSchemas[0].Tables[1].Location = "s3://b/warehouse/db/page_views/"
			},
			"ArchiveSchema table page is at s3://b/warehouse/db/page/, a key prefix of table page_views at s3://b/warehouse/db/page_views/",
		},
		{
			"schema waits for a resource that doesn't exist", func(m *infra.Manifest) { m.Regional.ArchiveSchemas[0].After = []string{"LookupClicks"} },
			"ArchiveSchema after LookupClicks isn't a resource of its scope",
		},
		{
			// DependsOn names resources of the same template.
			"schema waits for a global resource", func(m *infra.Manifest) { m.Regional.ArchiveSchemas[0].After = []string{"StreamPageViews"} },
			"ArchiveSchema after StreamPageViews isn't a resource of its scope",
		},
		{
			"schema everywhere waits for an archive-only resource",
			func(m *infra.Manifest) {
				m.Regional.ArchiveSchemas[0].ArchiveOnly = false
				m.Regional.ArchiveSchemas[0].After = []string{"Archived"}
			},
			"ArchiveSchema after Archived exists only in archive regions",
		},
		{
			"catalog waits for a resource that doesn't exist", func(m *infra.Manifest) { m.Regional.Catalogs[0].After = []string{"Projector"} },
			"Catalog after Projector isn't a resource of its scope",
		},
		{
			"catalog waits for an archive-only resource", func(m *infra.Manifest) { m.Regional.Catalogs[0].After = []string{"Archived"} },
			"Catalog after Archived exists only in archive regions",
		},
		{
			"catalog registered before a managed policy", func(m *infra.Manifest) { m.Regional.Catalogs[0].After = []string{"LookupPageViews"} },
			"Catalog is registered before RoleSetupPolicy1",
		},
		{
			"catalog registered before a function", func(m *infra.Manifest) {
				m.Regional.Functions = append(m.Regional.Functions, infra.Function{Common: infra.Common{ID: "Projector"}, Name: "p",
					Role: "${ref:RoleSetup.arn}", LogGroup: "logs"})
			},
			"Catalog is registered before Projector",
		},
		{
			"alarm everywhere watches an archive-only resource",
			func(m *infra.Manifest) { m.Regional.Alarms[0].Metrics[0].Dimensions[0].Value = "${ref:Archived.name}" },
			"AlarmErrorsSetup dimension FunctionName refers to Archived, which exists only in archive regions",
		},
		{
			"dashboard refers to a resource",
			func(m *infra.Manifest) { m.Regional.Dashboards[0].Body = `{"region":"${ref:Setup.name}"}` },
			"Dashboard body: has ${ref:Setup.name}, but a dashboard may hold only ${prefix}, ${region}, ${account} and ${partition}",
		},
		{
			"dashboard uses ${prefix_}",
			func(m *infra.Manifest) { m.Regional.Dashboards[0].Body = `{"region":"${prefix_}"}` },
			"Dashboard body: has ${prefix_}",
		},
		{
			"dashboard uses the deploy-time domain",
			func(m *infra.Manifest) { m.Regional.Dashboards[0].Body = `{"title":"${domain}"}` },
			"Dashboard body: has ${domain}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := validManifest()
			tt.change(m)
			err := m.Check()
			switch {
			case tt.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
				t.Fatalf("got error %v, want one containing %q", err, tt.want)
			}
		})
	}
}

func TestSegments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		text infra.Text
		want []infra.Segment
	}{
		{"", nil},
		{"analytics-meta", []infra.Segment{{Literal: "analytics-meta"}}},
		{"${prefix}analytics-${region}", []infra.Segment{{Placeholder: "prefix"}, {Literal: "analytics-"}, {Placeholder: "region"}}},
		{"${ref:Bucket.arn}/aws/lambda/*", []infra.Segment{{Ref: infra.Ref{ID: "Bucket", Attr: "arn"}}, {Literal: "/aws/lambda/*"}}},
		{"$5 ${account}${partition}${prefix_}", []infra.Segment{
			{Literal: "$5 "}, {Placeholder: "account"}, {Placeholder: "partition"}, {Placeholder: "prefix_"},
		}},
	}
	for _, tt := range tests {
		got, err := tt.text.Segments()
		if err != nil {
			t.Errorf("%q: %v", tt.text, err)
			continue
		}
		if len(got) != len(tt.want) {
			t.Errorf("%q: got %+v, want %+v", tt.text, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("%q: got %+v, want %+v", tt.text, got, tt.want)
				break
			}
		}
	}
}

func TestSegmentsRejects(t *testing.T) {
	t.Parallel()
	for text, want := range map[infra.Text]string{
		"${prefix":          "has a ${ without its }",
		"${Prefix}":         "unknown substitution ${Prefix}",
		"${}":               "unknown substitution ${}",
		"${ref:Bucket}":     "${ref:Bucket} isn't a reference of the form ${ref:ID.ATTR}",
		"${ref:.arn}":       "${ref:.arn} isn't a reference",
		"x${ref:Bucket.}/y": "${ref:Bucket.} isn't a reference",
		"${prefix}${region": "has a ${ without its }",
	} {
		if _, err := text.Segments(); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: got error %v, want one containing %q", text, err, want)
		}
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awssetup_test

import (
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/setup/awssetup"
	"github.com/lucindo/dynaflux/internal/validate"
)

// grantsDeployment is where the test reads the setup role, as IAM would.
func grantsDeployment() infra.Deployment {
	return infra.Deployment{Prefix: "prod-", Partition: "aws", Account: "123456789012", Region: "us-east-1"}
}

// grantsManifest is the manifest of a store with two archived streams.
func grantsManifest(t *testing.T, store string) *infra.Manifest {
	t.Helper()
	cfg, cat, ds := validate.Load(fstest.MapFS{
		"dynaflux.yaml": {Data: []byte("store: " + store + "\nregions: [us-east-1]\narchive: {enabled: true}\n")},
		"dynaflux/streams.sql": {Data: []byte("CREATE STREAM page_views (k String) PRIMARY KEY (k);\n" +
			"CREATE STREAM clicks (k String) PRIMARY KEY (k);\n")},
	}, "/project")
	if cat == nil {
		t.Fatalf("the project doesn't compile: %v", ds)
	}
	m, err := infra.Build(cfg, cat, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func resolve(t *testing.T, m *infra.Manifest, x infra.Text, d infra.Deployment) string {
	t.Helper()
	s, err := m.Resolve(x, d)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// setupCalls is every call the adapters can send for m's stream lookups and archive schema
// in deployment d, by IAM action, with the ARNs that IAM authorizes it on. "Actions on a
// table, partition, or table version require permission on the table, database, and
// catalog" (Glue's ARN documentation); creating a database needs the catalog and the
// database. Every optimizer call goes both ways: an existing one is updated, and a canceled
// query is stopped. The optimizer calls pass the optimizer role to Glue.
func setupCalls(t *testing.T, m *infra.Manifest, d infra.Deployment) map[string][]string {
	t.Helper()
	arn := func(service, resource string) string {
		return "arn:" + d.Partition + ":" + service + ":" + d.Region + ":" + d.Account + ":" + resource
	}
	var streamTables []string
	for _, l := range m.Regional.StreamLookups {
		streamTables = append(streamTables, arn("dynamodb", "table/"+resolve(t, m, l.Table, d)))
	}
	schema := m.Regional.ArchiveSchemas[0]
	db := resolve(t, m, schema.Database, d)
	glueCatalog, glueDatabase := arn("glue", "catalog"), arn("glue", "database/"+db)
	glueTables := []string{glueCatalog, glueDatabase}
	for _, tb := range schema.Tables {
		glueTables = append(glueTables, arn("glue", "table/"+db+"/"+tb.Name))
	}
	workgroup := []string{arn("athena", "workgroup/"+resolve(t, m, schema.WorkGroup, d))}
	return map[string][]string{
		"dynamodb:DescribeTable":     streamTables,
		"glue:CreateDatabase":        {glueCatalog, glueDatabase},
		"glue:GetTable":              glueTables,
		"glue:CreateTableOptimizer":  glueTables,
		"glue:UpdateTableOptimizer":  glueTables,
		"athena:StartQueryExecution": workgroup,
		"athena:GetQueryExecution":   workgroup,
		"athena:StopQueryExecution":  workgroup,
		"iam:PassRole":               {resolve(t, m, schema.Optimizers.Role, d)},
	}
}

// requestKeys returns the condition keys of a call of action.
func requestKeys(action string) map[string]string {
	if action == "iam:PassRole" {
		return map[string]string{"iam:PassedToService": infra.ServiceGlue}
	}
	return nil
}

// foreignCalls is what the same calls name for another store and for the store under
// another prefix, along with a pass of each other role of m, less what the calls of
// m's deployment name. The setup role must allow none of it.
func foreignCalls(t *testing.T, m *infra.Manifest) map[string][]string {
	t.Helper()
	d := grantsDeployment()
	own := setupCalls(t, m, d)
	foreign := map[string][]string{}
	add := func(action, arn string) {
		if !slices.Contains(own[action], arn) && !slices.Contains(foreign[action], arn) {
			foreign[action] = append(foreign[action], arn)
		}
	}
	dev := d
	dev.Prefix = "dev-"
	for _, calls := range []map[string][]string{setupCalls(t, m, dev), setupCalls(t, grantsManifest(t, "billing"), d)} {
		for action, arns := range calls {
			for _, arn := range arns {
				add(action, arn)
			}
		}
	}
	for _, r := range m.Regional.Roles {
		add("iam:PassRole", resolve(t, m, infra.Text("${ref:"+r.ID+"."+infra.AttrARN+"}"), d))
	}
	return foreign
}

// forOthers reports whether a grant of the setup role, its Sid and action, serves what
// others do with the role's credentials, which no adapter call shows: Lambda writes the
// logs, and Athena reads and writes the catalog and the warehouse while it runs the
// statements. Only the AWS end-to-end suite can confirm these.
func forOthers(grant string) bool {
	switch grant {
	case "Logs logs:CreateLogStream", "Logs logs:PutLogEvents",
		"ArchiveAthenaCatalog glue:CreateDatabase", "ArchiveAthenaCatalog glue:GetDatabase", "ArchiveAthenaCatalog glue:GetDatabases",
		"ArchiveCatalog glue:CreateTable", "ArchiveCatalog glue:UpdateTable", "ArchiveCatalog glue:GetPartitions",
		"ArchiveCatalog glue:GetTables", "ArchiveCatalog glue:GetDatabases", "ArchiveCatalog glue:GetDatabase",
		"ArchiveFiles s3:GetObject", "ArchiveFiles s3:PutObject", "ArchiveList s3:ListBucket",
		"ArchiveBucketLocation s3:GetBucketLocation":
		return true
	}
	return false
}

// checkGrants compares calls with role both ways: a call that no statement allows would be
// AccessDenied on AWS, while a statement that allows a call of foreign, or an action that
// no call uses unless others use it as the role, grants too much.
func checkGrants(m *infra.Manifest, role infra.Role, calls, foreign map[string][]string) error {
	var errs []error
	allowing := func(action, arn string) []string {
		sids, err := m.Allowing(role, grantsDeployment(), action, arn, requestKeys(action))
		if err != nil {
			errs = append(errs, err)
		}
		return sids
	}
	used := map[string]bool{} // a statement's Sid and action
	for action, arn := range each(calls) {
		sids := allowing(action, arn)
		if len(sids) == 0 {
			errs = append(errs, fmt.Errorf("no statement allows %s on %s", action, arn))
		}
		for _, sid := range sids {
			used[sid+" "+action] = true
		}
	}
	for action, arn := range each(foreign) {
		for _, sid := range allowing(action, arn) {
			errs = append(errs, fmt.Errorf("statement %s allows %s on %s, another store's or deployment's", sid, action, arn))
		}
	}
	for _, st := range role.Statements {
		for _, action := range st.Actions {
			if !used[st.Sid+" "+action] && !forOthers(st.Sid+" "+action) {
				errs = append(errs, fmt.Errorf("statement %s allows %s, which no call uses", st.Sid, action))
			}
		}
	}
	return errors.Join(errs...)
}

// each yields every action of calls, in order, with each of its ARNs.
func each(calls map[string][]string) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for _, action := range slices.Sorted(maps.Keys(calls)) {
			for _, arn := range calls[action] {
				if !yield(action, arn) {
					return
				}
			}
		}
	}
}

func setupRole(t *testing.T, m *infra.Manifest) infra.Role {
	t.Helper()
	role, err := m.FunctionRole(catalog.EnvModeSetup)
	if err != nil {
		t.Fatal(err)
	}
	return role
}

// The setup role grants what the adapters can call, on the resources they name, and
// nothing else but what others do with its credentials.
func TestSetupGrants(t *testing.T) {
	t.Parallel()
	m := grantsManifest(t, "analytics")
	calls := setupCalls(t, m, grantsDeployment())
	ops := []string{"iam:PassRole"}
	for service, names := range awssetup.Operations() {
		for _, name := range names {
			ops = append(ops, service+":"+name)
		}
	}
	if got, want := slices.Sorted(maps.Keys(calls)), slices.Sorted(slices.Values(ops)); !slices.Equal(got, want) {
		t.Errorf("the table lists %q, but the adapters can send %q", got, want)
	}
	if err := checkGrants(m, setupRole(t, m), calls, foreignCalls(t, m)); err != nil {
		t.Error(err)
	}
}

// Each control breaks the role one way, and the check must name it.
func TestSetupGrantsControls(t *testing.T) {
	t.Parallel()
	m := grantsManifest(t, "analytics")
	calls, foreign := setupCalls(t, m, grantsDeployment()), foreignCalls(t, m)
	const passOptimizer = "no statement allows iam:PassRole on arn:aws:iam::123456789012:role/RoleArchiveOptimizer"
	tests := []struct {
		name, sid string
		change    func(st *infra.Statement)
		want      string
	}{
		{"an action no call uses", "ArchiveCatalog", func(st *infra.Statement) {
			st.Actions = append(slices.Clone(st.Actions), "glue:GetTableOptimizer")
		}, "statement ArchiveCatalog allows glue:GetTableOptimizer, which no call uses"},
		{"an action others don't use", "ArchiveFiles", func(st *infra.Statement) {
			st.Actions = append(slices.Clone(st.Actions), "s3:DeleteObject")
		}, "statement ArchiveFiles allows s3:DeleteObject, which no call uses"},
		{"no action a call uses", "ArchiveQueries", func(st *infra.Statement) {
			st.Actions = slices.DeleteFunc(slices.Clone(st.Actions), func(a string) bool { return a == "athena:StopQueryExecution" })
		}, "no statement allows athena:StopQueryExecution on arn:aws:athena:us-east-1:123456789012:workgroup/prod-analytics"},
		{"the tables without their database", "ArchiveCatalog", func(st *infra.Statement) {
			st.Resources = slices.DeleteFunc(slices.Clone(st.Resources), func(r infra.Text) bool {
				return strings.Contains(string(r), ":database/")
			})
		}, "no statement allows glue:GetTable on arn:aws:glue:us-east-1:123456789012:database/prod_analytics"},
		{"another stream table", "StreamTables", func(st *infra.Statement) { st.Resources = st.Resources[:1] },
			"no statement allows dynamodb:DescribeTable on arn:aws:dynamodb:us-east-1:123456789012:table/prod-analytics-stream-page_views"},
		{"every table", "StreamTables", func(st *infra.Statement) { st.Resources = []infra.Text{"*"} },
			"statement StreamTables allows dynamodb:DescribeTable on arn:aws:dynamodb:us-east-1:123456789012:table/dev-analytics-stream-page_views"},
		{"another store's tables", "ArchiveCatalog", func(st *infra.Statement) {
			st.Resources = append(slices.Clone(st.Resources), "arn:${partition}:glue:${region}:${account}:table/${prefix_}*/*")
		}, "statement ArchiveCatalog allows glue:GetTable on arn:aws:glue:us-east-1:123456789012:table/prod_billing/page_views"},
		{"another prefix's workgroup", "ArchiveQueries", func(st *infra.Statement) {
			st.Resources = []infra.Text{"arn:${partition}:athena:${region}:${account}:workgroup/*analytics"}
		}, "statement ArchiveQueries allows athena:StartQueryExecution on arn:aws:athena:us-east-1:123456789012:workgroup/dev-analytics"},
		{"no pass of the optimizer role", "ArchivePassOptimizerRole", func(st *infra.Statement) { st.Actions = nil }, passOptimizer},
		{"the optimizer role passed to another service", "ArchivePassOptimizerRole", func(st *infra.Statement) {
			st.Conditions = []infra.Condition{{Operator: "StringEquals", Key: "iam:PassedToService", Values: []infra.Text{"lambda.amazonaws.com"}}}
		}, passOptimizer},
		{"a pass of every role", "ArchivePassOptimizerRole", func(st *infra.Statement) { st.Resources = []infra.Text{"*"} },
			"statement ArchivePassOptimizerRole allows iam:PassRole on arn:aws:iam::123456789012:role/RoleSetup"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			role := setupRole(t, m)
			role.Statements = slices.Clone(role.Statements)
			k := slices.IndexFunc(role.Statements, func(st infra.Statement) bool { return st.Sid == tt.sid })
			if k < 0 {
				t.Fatalf("the role has no statement %s", tt.sid)
			}
			tt.change(&role.Statements[k])
			if err := checkGrants(m, role, calls, foreign); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("got %v, want an error with %q", err, tt.want)
			}
		})
	}
}

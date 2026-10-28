// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/validate"
)

// build builds the manifest of a project with the given SQL.
func build(t *testing.T, sql string) (*infra.Manifest, error) {
	t.Helper()
	return buildWith(t, "store: analytics\nregions: [us-east-1]\n", sql, infra.Options{})
}

func buildWith(t *testing.T, config, sql string, opts infra.Options) (*infra.Manifest, error) {
	t.Helper()
	cfg, cat, ds := validate.Load(fstest.MapFS{
		"dynaflux.yaml":        {Data: []byte(config)},
		"dynaflux/streams.sql": {Data: []byte(sql)},
	}, "/project")
	if cat == nil {
		t.Fatalf("the project doesn't compile: %v", ds)
	}
	return infra.Build(cfg, cat, opts)
}

// IDs come from the stream name alone. A name whose PascalCase form another name could
// share (a_1 and a1) gets "X" and 8 hex digits of its SHA-256.
func TestBuildStreamIDs(t *testing.T) {
	t.Parallel()
	var sql strings.Builder
	for _, name := range []string{"page_views", "a_1", "a__b", "a_"} {
		sql.WriteString("CREATE STREAM " + name + " (k String) PRIMARY KEY (k);\n")
	}
	m, err := build(t, sql.String())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, tb := range m.Global.Tables {
		got = append(got, tb.ID)
	}
	want := []string{"StreamA1X1426674c", "StreamABX63e5c1c4", "StreamAX571fb0e3", "StreamPageViews"}
	if !slices.Equal(got, want) {
		t.Errorf("got IDs %q, want %q", got, want)
	}
}

// Without a query annotated -- expose: http, no door is built, nor a custom domain, whatever
// http says.
func TestBuildQueryWithoutExposed(t *testing.T) {
	t.Parallel()
	const apiDoor = "store: analytics\nregions: [us-east-1]\n" +
		"http: {frontend: http_api, auth: lambda, authorizer: {function: auth}, domain: {name: flux.example.com}}\n"
	m, err := buildWith(t, apiDoor, "CREATE STREAM page_views (user_id String, page String) PRIMARY KEY (user_id);\n", infra.Options{})
	if err != nil || len(m.Query.IDs()) != 0 || m.Domain != "" {
		t.Errorf("query scope %q, domain %q, error %v; want none", m.Query.IDs(), m.Domain, err)
	}
}

func TestBuildWithoutStreams(t *testing.T) {
	t.Parallel()
	_, err := build(t, "")
	if err == nil || err.Error() != "infra: the store has no stream, so there is nothing to deploy" {
		t.Errorf("got error %v", err)
	}
}

// With the longest prefix that the names allow, a projector role that writes 100 view
// tables is over IAM's 10,240 characters. With a projector per stream, each role writes 25.
func TestBuildPolicySize(t *testing.T) {
	t.Parallel()
	var sql strings.Builder
	for s := range 4 {
		fmt.Fprintf(&sql, "CREATE STREAM s%d (k String) PRIMARY KEY (k);\n", s)
		for v := range 25 {
			fmt.Fprintf(&sql, "CREATE MATERIALIZED VIEW v_%d_%02d ORDER BY (k) PRIMARY KEY (k) AS SELECT k, count() AS n FROM s%d GROUP BY k;\n", s, v, s)
		}
	}
	const config = "store: analytics\nregions: [us-east-1]\n"
	_, err := buildWith(t, config, sql.String(), infra.Options{})
	const want = "infra: the policies of role RoleProjector can reach "
	if err == nil || !strings.HasPrefix(err.Error(), want) || !strings.HasSuffix(err.Error(),
		"over IAM's limit of 10240 per role; set runtime.split: stream, which gives each stream's projector a role of its own, or retire old view versions") {
		t.Errorf("split: store: got error %v, want one starting with %q", err, want)
	}
	if _, err = buildWith(t, config+"runtime: {split: stream}\n", sql.String(), infra.Options{}); err != nil {
		t.Errorf("split: stream: %v", err)
	}
}

func TestBuildUnprotectRegion(t *testing.T) {
	t.Parallel()
	const config = "store: analytics\nregions: [us-east-1, us-west-2]\n"
	const sql = "CREATE STREAM page_views (k String) PRIMARY KEY (k);\n"
	m, err := buildWith(t, config, sql, infra.Options{UnprotectRegion: "us-west-2"})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Global.Tables[0].UnprotectedRegion; got != "us-west-2" {
		t.Errorf("unprotected region %q, want us-west-2", got)
	}
}

// One regional.yaml serves every region, so every archive region must archive the same
// streams; dynaflux.yaml can't express anything else.
func TestBuildArchivePlacement(t *testing.T) {
	t.Parallel()
	cfg, cat, ds := validate.Load(fstest.MapFS{
		"dynaflux.yaml":        {Data: []byte("store: analytics\nregions: [us-east-1, us-west-2]\n")},
		"dynaflux/streams.sql": {Data: []byte("CREATE STREAM a (k String) PRIMARY KEY (k);\nCREATE STREAM b (k String) PRIMARY KEY (k);\n")},
	}, "/project")
	if cat == nil {
		t.Fatalf("the project doesn't compile: %v", ds)
	}
	cat.Streams[1].Archive.Regions = []string{"us-west-2"}
	_, err := infra.Build(cfg, cat, infra.Options{})
	const want = "infra: streams a and b archive in different regions, and one template serves every region"
	if err == nil || err.Error() != want {
		t.Errorf("got error %v, want %q", err, want)
	}
}

// sam build compiles the functions' code from runtime.out, relative to the templates in
// infra.out.
func TestBuildFunctionCode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		outputs string
		want    string
	}{
		{"", "../../cmd/dynaflux-runtime"},
		{"infra: {out: deploy}\nruntime: {out: cmd/rt}\n", "../cmd/rt"},
		{"infra: {out: infra/dynaflux}\nruntime: {out: infra/rt}\n", "../rt"},
		{"infra: {out: deploy}\nruntime: {out: deploy/rt}\n", "rt"},
	}
	for _, tt := range tests {
		m, err := buildWith(t, "store: analytics\nregions: [us-east-1]\n"+tt.outputs, "CREATE STREAM s (k String) PRIMARY KEY (k);\n", infra.Options{})
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range m.Regional.Functions {
			if f.Code != tt.want {
				t.Errorf("%q: %s has code %q, want %q", tt.outputs, f.ID, f.Code, tt.want)
			}
		}
	}
}

// The backfill role names every stream and view table, so its statements that grow go in
// managed policies, as many as fit IAM's 6,144 characters each with the longest prefix, in
// order. Past IAM's 10 managed policies per role, Build fails.
func TestBuildBackfillPolicies(t *testing.T) {
	t.Parallel()
	views := func(streams, perStream int) string {
		var sql strings.Builder
		for s := range streams {
			fmt.Fprintf(&sql, "CREATE STREAM s%d (k String) PRIMARY KEY (k);\n", s)
			for v := range perStream {
				fmt.Fprintf(&sql, "CREATE MATERIALIZED VIEW v_%d_%02d ORDER BY (k) PRIMARY KEY (k) AS SELECT k, count() AS n FROM s%d GROUP BY k;\n", s, v, s)
			}
		}
		return sql.String()
	}
	const config = "store: analytics\nregions: [us-east-1, us-west-2]\nruntime: {split: stream}\n"
	m, err := buildWith(t, config, views(4, 25), infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	role, err := m.FunctionRole("backfill")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	resources := map[string][]infra.Text{}
	for _, p := range role.Managed {
		ids = append(ids, p.ID)
		for _, st := range p.Statements {
			resources[st.Sid] = append(resources[st.Sid], st.Resources...)
		}
	}
	if want := []string{"RoleBackfillPolicy1", "RoleBackfillPolicy2", "RoleBackfillPolicy3"}; !slices.Equal(ids, want) {
		t.Errorf("managed policies %q, want %q", ids, want)
	}
	if got := resources["Views"]; len(got) != 100 || got[0] != "${ref:ViewV000Xb7ded8d1V1.arn}" || got[99] != "${ref:ViewV324Xe1311f8bV1.arn}" {
		t.Errorf("Views names %d tables, from %s to %s; want the 100 view tables in order", len(got), got[0], got[len(got)-1])
	}
	if got := resources["StreamTables"]; len(got) != 8 ||
		got[0] != "arn:${partition}:dynamodb:us-east-1:${account}:table/${ref:StreamS0.name}" ||
		got[4] != "arn:${partition}:dynamodb:us-west-2:${account}:table/${ref:StreamS0.name}" {
		t.Errorf("StreamTables names %q; want each stream table in us-east-1 and us-west-2", got)
	}
	_, err = buildWith(t, config, views(8, 75), infra.Options{})
	if want := "infra: role RoleBackfill needs 12 managed policies, and IAM attaches at most 10 to a role by default; " +
		"retire old view versions, or split the streams across stores"; err == nil || err.Error() != want {
		t.Errorf("600 view versions: got error %v, want %q", err, want)
	}
}

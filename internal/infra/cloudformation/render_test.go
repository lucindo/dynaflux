// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/infra/cloudformation"
	"github.com/lucindo/dynaflux/internal/txtar"
)

// The golden cases use only ${prefix}; the other placeholders become pseudo parameters,
// and ${prefix_} a !Sub variable.
func TestRenderPlaceholders(t *testing.T) {
	t.Parallel()
	m := &infra.Manifest{
		Store:    "analytics",
		Regions:  []string{"us-east-1"},
		Regional: infra.Scope{Tables: []infra.Table{{Common: infra.Common{ID: "Meta"}, Name: "${prefix_}x-${account}-${region}-${partition}"}}},
	}
	files, err := cloudformation.Render(m, "v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	want := `
      TableName: !Sub
      - ${PrefixUnderscore}x-${AWS::AccountId}-${AWS::Region}-${AWS::Partition}
      - {PrefixUnderscore: !Join [_, !Split ["-", !Ref Prefix]]}
`
	if got := string(files[cloudformation.RegionalTemplate]); !strings.Contains(got, want) {
		t.Errorf("regional.yaml:\n%s\nwant it to contain:%s", got, want)
	}
}

// A column DEFAULT is a plain string in the archive schema's properties, never !Sub, so the
// "${" of one stays as it is.
func TestRenderDefaultIsPlain(t *testing.T) {
	t.Parallel()
	m := &infra.Manifest{Store: "analytics", Regions: []string{"us-east-1"}, Regional: infra.Scope{
		ArchiveSchemas: []infra.ArchiveSchema{{
			Common: infra.Common{ID: "ArchiveSchema"}, Handler: "h", Database: "db", DatabaseLocation: "s3://b/db/",
			WorkGroup: "wg", Optimizers: infra.Optimizers{Role: "r"},
			Tables: []infra.ArchiveTable{{
				Name: "page", Location: "s3://b/db/page/iceberg/",
				Columns: []archive.Column{{Name: "note", Type: "string", Default: new("'it''s ${x}'")}},
			}},
		}},
	}}
	files, err := cloudformation.Render(m, "v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	want := `
        - Name: note
          Type: string
          Default: "'it''s ${x}'"
`
	if got := string(files[cloudformation.RegionalTemplate]); !strings.Contains(got, want) {
		t.Errorf("regional.yaml:\n%s\nwant it to contain:%s", got, want)
	}
}

// A template holds at most 500 resources.
func TestRenderResourceLimit(t *testing.T) {
	t.Parallel()
	tables := func(n int) *infra.Manifest {
		m := &infra.Manifest{Store: "analytics", Regions: []string{"us-east-1"}}
		for i := range n {
			m.Regional.Tables = append(m.Regional.Tables, infra.Table{Common: infra.Common{ID: fmt.Sprintf("View%dV1", i)}, Name: "t"})
		}
		return m
	}
	if _, err := cloudformation.Render(tables(500), "v1.2.3"); err != nil {
		t.Errorf("500 resources: %v", err)
	}
	_, err := cloudformation.Render(tables(501), "v1.2.3")
	const want = "cloudformation: regional.yaml has 501 resources, and a CloudFormation template can have at most 500"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("501 resources: got error %v, want one starting with %q", err, want)
	}
}

// SAM CLI builds functions together only when their Metadata is equal, so every function
// of a golden case must have the same, for sam build to compile the binary once.
func TestFunctionsShareMetadata(t *testing.T) {
	t.Parallel()
	for name, regional := range goldenTemplates(t, cloudformation.RegionalTemplate) {
		var first []byte
		functions := 0
		resources := templateResources(t, name, regional)
		for i := 1; i < len(resources.Content); i += 2 {
			r := resources.Content[i]
			if value(r, "Type").Value != "AWS::Serverless::Function" {
				continue
			}
			functions++
			metadata, err := yaml.Marshal(value(r, "Metadata"))
			if err != nil {
				t.Fatal(err)
			}
			if first == nil {
				first = metadata
			} else if string(metadata) != string(first) {
				t.Errorf("%s: %s has Metadata\n%s\nthe first function has\n%s", name, resources.Content[i-1].Value, metadata, first)
			}
		}
		if functions < 2 {
			t.Errorf("%s: %d functions, want at least the projector and the setup function", name, functions)
		}
	}
}

// CloudFormation leaves out a resource that depends on one whose condition is false, so a
// resource may depend only on resources that exist wherever it does (cfn-lint E3005).
func TestDependsOnlyOnResourcesThatExist(t *testing.T) {
	t.Parallel()
	for name, regional := range goldenTemplates(t, cloudformation.RegionalTemplate) {
		resources := templateResources(t, name, regional)
		conditions := map[string]string{}
		for i := 0; i+1 < len(resources.Content); i += 2 {
			conditions[resources.Content[i].Value] = value(resources.Content[i+1], "Condition").Value
		}
		for i := 0; i+1 < len(resources.Content); i += 2 {
			id, r := resources.Content[i].Value, resources.Content[i+1]
			deps := value(r, "DependsOn")
			if deps.Kind == yaml.ScalarNode {
				deps = &yaml.Node{Content: []*yaml.Node{deps}}
			}
			for _, d := range deps.Content {
				if c := conditions[d.Value]; c != "" && c != conditions[id] {
					t.Errorf("%s: %s, with condition %q, depends on %s, which has condition %s", name, id, conditions[id], d.Value, c)
				}
			}
		}
	}
}

// AWS::Glue::TableOptimizer exists in a few regions, and CloudFormation checks resource
// types before conditions, so one would fail the template in every other region.
func TestNoTableOptimizerResource(t *testing.T) {
	t.Parallel()
	for _, file := range []string{cloudformation.GlobalTemplate, cloudformation.RegionalTemplate} {
		for name, data := range goldenTemplates(t, file) {
			resources := templateResources(t, name, data)
			for i := 1; i < len(resources.Content); i += 2 {
				if typ := value(resources.Content[i], "Type").Value; typ == "AWS::Glue::TableOptimizer" {
					t.Errorf("%s: %s %s is an %s", name, file, resources.Content[i-1].Value, typ)
				}
			}
		}
	}
}

// templateResources returns the Resources mapping of a template.
func templateResources(t *testing.T, name string, data []byte) *yaml.Node {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return value(doc.Content[0], "Resources")
}

// goldenTemplates returns the want/<file> section of each golden case, by case name: those
// of ../testdata/cases, and of ../testdata/doors, which hold only query.yaml.
func goldenTemplates(t *testing.T, file string) map[string][]byte {
	t.Helper()
	cases, err := filepath.Glob("../testdata/cases/*.txtar")
	if err != nil || len(cases) == 0 {
		t.Fatalf("no cases in ../testdata/cases: %v", err)
	}
	doors, err := filepath.Glob("../testdata/doors/*.txtar")
	if err != nil || len(doors) == 0 {
		t.Fatalf("no cases in ../testdata/doors: %v", err)
	}
	paths := slices.Concat(cases, doors)
	templates := map[string][]byte{}
	for _, path := range paths {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		for _, f := range txtar.Parse(data).Files {
			if f.Name == "want/"+file {
				templates[filepath.Base(path)] = f.Data
			}
		}
	}
	return templates
}

// value returns the value of key in mapping n, or an empty node.
func value(n *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return &yaml.Node{}
}

// generate's guards read back what Render wrote: the replica regions and the view tables.
func TestReadInventory(t *testing.T) {
	t.Parallel()
	inv, err := cloudformation.ReadInventory(
		goldenTemplates(t, cloudformation.GlobalTemplate)["page_views.txtar"],
		goldenTemplates(t, cloudformation.RegionalTemplate)["page_views.txtar"],
	)
	if err != nil {
		t.Fatal(err)
	}
	want := cloudformation.Inventory{
		Regions:   []string{"us-east-1", "us-west-2"},
		Protected: []string{"us-east-1", "us-west-2"},
		Views: []cloudformation.ViewTable{
			{View: "page_daily", Version: 1, Fingerprint: "1242f0a6792f3ab9edc9e9f6a438396eb1d3ff79c2876c0f18f39bce4ca88ed5"},
			{View: "page_daily", Version: 2, Fingerprint: "5415ee58c09f7a46b4b06ff6d09435b15ad5e43a3d107104edab77f77341bc99"},
		},
	}
	if !reflect.DeepEqual(inv, want) {
		t.Errorf("got %+v\nwant %+v", inv, want)
	}
}

func TestReadInventoryRejects(t *testing.T) {
	t.Parallel()
	for name, regional := range map[string]string{
		"not YAML":       "Resources: [",
		"not a template": "- a\n",
		"bad version":    "Resources:\n  V:\n    Type: AWS::DynamoDB::Table\n    Properties:\n      Tags:\n      - {Key: dynaflux:kind, Value: view}\n      - {Key: dynaflux:version, Value: two}\n",
	} {
		if _, err := cloudformation.ReadInventory(nil, []byte(regional)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

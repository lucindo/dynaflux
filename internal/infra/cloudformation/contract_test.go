// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/setup"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/txtar"
)

// The custom resources of every golden regional.yaml, sent as CloudFormation sends them,
// are requests that the setup function answers, with every attribute the template reads.
// Neither package can rename a type, a property or an attribute alone, and the catalog's
// registration names the catalog that generate embeds in the runtime binary.
func TestCustomResourceContract(t *testing.T) {
	t.Parallel()
	for name, regional := range goldenTemplates(t, "regional.yaml") {
		replies := customResources(t, name, regional)
		for _, attr := range regexp.MustCompile(`\$\{(\w+)\.(\w+)\}|!GetAtt (\w+)\.(\w+)`).FindAllStringSubmatch(string(regional), -1) {
			id, key := attr[1]+attr[3], attr[2]+attr[4]
			if resp, ok := replies[id]; ok {
				if _, ok = resp.Data[key]; !ok {
					t.Errorf("%s: the template reads %s.%s, and the setup function replies with %v", name, id, key, resp.Data)
				}
			}
		}
	}
}

// customResources sends each custom resource of a template to the setup function, and
// returns the replies by ID.
func customResources(t *testing.T, name string, template []byte) map[string]setup.Response {
	t.Helper()
	resources := templateResources(t, name, template)
	replies := map[string]setup.Response{}
	for i := 0; i+1 < len(resources.Content); i += 2 {
		id, r := resources.Content[i].Value, resources.Content[i+1]
		typ := value(r, "Type").Value
		if !strings.HasPrefix(typ, "Custom::") {
			continue
		}
		props, ok := sent(value(r, "Properties")).(map[string]any)
		if !ok {
			t.Fatalf("%s: %s has no properties", name, id)
		}
		h := setup.Handler{DynamoDB: acceptAll{}, Athena: acceptAll{}, Glue: acceptAll{}, Meta: mem.New(), Catalog: caseCatalog(t, name)}
		resp, err := h.Handle(t.Context(), setup.Request{Type: typ, Action: setup.ActionCreate, Props: props})
		if err != nil {
			t.Errorf("%s: %s: %v", name, id, err)
		}
		replies[id] = resp
	}
	return replies
}

// caseCatalog returns the catalog of the golden case in file name, as generate embeds it.
func caseCatalog(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("../testdata/cases", name))
	if err != nil {
		t.Fatal(err)
	}
	fsys := fstest.MapFS{}
	for _, f := range txtar.Parse(data).Files {
		if !strings.HasPrefix(f.Name, "want/") {
			fsys[f.Name] = &fstest.MapFile{Data: f.Data}
		}
	}
	return catalogtest.MarshalFS(t, fsys)
}

// sent returns a template value as CloudFormation sends it to a custom resource: scalars as
// strings, and intrinsic functions resolved with sample values.
func sent(n *yaml.Node) any {
	switch {
	case n.Kind == yaml.MappingNode:
		m := map[string]any{}
		for i := 0; i+1 < len(n.Content); i += 2 {
			m[n.Content[i].Value] = sent(n.Content[i+1])
		}
		return m
	case n.Tag == "!Sub" && n.Kind == yaml.SequenceNode:
		return substitute(n.Content[0].Value)
	case n.Tag == "!Sub":
		return substitute(n.Value)
	case n.Tag == "!Ref" || n.Tag == "!GetAtt":
		return sample(n.Value)
	case n.Tag == "!If":
		// As a region whose condition holds.
		return sent(n.Content[1])
	case n.Kind == yaml.SequenceNode:
		var s []any
		for _, c := range n.Content {
			s = append(s, sent(c))
		}
		return s
	}
	return n.Value
}

func substitute(s string) string {
	return regexp.MustCompile(`\$\{([^}]+)\}`).ReplaceAllStringFunc(s, func(v string) string {
		return sample(v[2 : len(v)-1])
	})
}

// sample returns a value of a parameter, a pseudo parameter, a !Sub variable or an
// attribute.
func sample(name string) string {
	switch name {
	case "Prefix":
		return "prod-"
	case "PrefixUnderscore":
		return "prod_"
	case "AWS::Partition":
		return "aws"
	case "AWS::Region":
		return "us-east-1"
	case "AWS::AccountId":
		return "123456789012"
	case "Bucket":
		return "prod-analytics-123456789012-us-east-1-an"
	}
	return "sample-" + strconv.Quote(name)
}

// acceptAll is DynamoDB, Athena and Glue with ready stream tables and no archive tables.
type acceptAll struct{}

func (acceptAll) DescribeTable(_ context.Context, name string) (setup.StreamTable, bool, error) {
	return setup.StreamTable{
		Status: "ACTIVE", StreamARN: "arn:aws:dynamodb:us-east-1:123456789012:table/" + name + "/stream/2026-10-26T12:00:00.000",
		StreamView: "NEW_AND_OLD_IMAGES",
	}, true, nil
}

func (acceptAll) Exec(context.Context, string, string) error { return nil }

func (acceptAll) CreateDatabase(context.Context, string, string) error { return nil }

func (acceptAll) Table(context.Context, string, string) (setup.GlueTable, bool, error) {
	return setup.GlueTable{}, false, nil
}

func (acceptAll) PutOptimizer(context.Context, string, string, setup.Optimizer) error { return nil }

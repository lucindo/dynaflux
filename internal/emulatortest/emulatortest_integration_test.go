// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package emulatortest_test

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/validate"
)

// table is what DynamoDB Local reports of a table.
type table struct {
	Keys    []string // name:type:role
	Billing string
	Stream  string // view type; empty without a stream
	TTL     string // attribute:status
}

func describe(t *testing.T, c *dynamodb.Client, name string) table {
	t.Helper()
	d, err := c.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: &name})
	if err != nil {
		t.Fatalf("DescribeTable %s: %v", name, err)
	}
	ttl, err := c.DescribeTimeToLive(t.Context(), &dynamodb.DescribeTimeToLiveInput{TableName: &name})
	if err != nil {
		t.Fatalf("DescribeTimeToLive %s: %v", name, err)
	}
	var got table
	types := map[string]string{}
	for _, a := range d.Table.AttributeDefinitions {
		types[*a.AttributeName] = string(a.AttributeType)
	}
	for _, k := range d.Table.KeySchema {
		got.Keys = append(got.Keys, *k.AttributeName+":"+types[*k.AttributeName]+":"+string(k.KeyType))
	}
	if b := d.Table.BillingModeSummary; b != nil {
		got.Billing = string(b.BillingMode)
	}
	if s := d.Table.StreamSpecification; s != nil && *s.StreamEnabled {
		got.Stream = string(s.StreamViewType)
	}
	if d := ttl.TimeToLiveDescription; d != nil && d.AttributeName != nil {
		got.TTL = *d.AttributeName + ":" + string(d.TimeToLiveStatus)
	}
	return got
}

// manifest is the manifest of a store with one stream and one view.
func manifest(t *testing.T) *infra.Manifest {
	t.Helper()
	cfg, cat, ds := validate.Load(fstest.MapFS{
		"dynaflux.yaml": {Data: []byte("store: analytics\nregions: [us-east-1]\n")},
		"dynaflux/streams.sql": {Data: []byte(`CREATE STREAM page_views (k String) PRIMARY KEY (k);
CREATE MATERIALIZED VIEW daily ORDER BY (k) PRIMARY KEY (k) AS SELECT k, count() AS n FROM page_views GROUP BY k;`)},
	}, "/project")
	if cat == nil {
		t.Fatalf("the fixture doesn't compile: %v", ds)
	}
	m, err := infra.Build(cfg, cat, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestTables(t *testing.T) {
	t.Parallel()
	awsCfg := emulatortest.Config(t, nil)
	n := emulatortest.Tables(t, awsCfg, manifest(t))
	c := dynamodb.NewFromConfig(awsCfg)

	keys := []string{"pk:S:HASH", "sk:S:RANGE"}
	want := map[string]table{
		n.StreamTable("page_views"): {Keys: keys, Billing: "PAY_PER_REQUEST", Stream: "NEW_AND_OLD_IMAGES", TTL: "_ttl:ENABLED"},
		n.MetaTable():               {Keys: keys, Billing: "PAY_PER_REQUEST", TTL: "_ttl:ENABLED"},
		n.ViewTable("daily", 1):     {Keys: keys, Billing: "PAY_PER_REQUEST", TTL: "_ttl:ENABLED"},
	}
	for name, w := range want {
		if got := describe(t, c, name); !reflect.DeepEqual(got, w) {
			t.Errorf("table %s:\n got  %+v\n want %+v", name, got, w)
		}
	}
	var mine []string
	pages := dynamodb.NewListTablesPaginator(c, &dynamodb.ListTablesInput{})
	for pages.HasMorePages() {
		page, perr := pages.NextPage(t.Context())
		if perr != nil {
			t.Fatalf("ListTables: %v", perr)
		}
		for _, name := range page.TableNames {
			if strings.HasPrefix(name, n.Prefix) {
				mine = append(mine, name)
			}
		}
	}
	slices.Sort(mine)
	if wantNames := slices.Sorted(maps.Keys(want)); !slices.Equal(mine, wantNames) {
		t.Errorf("tables with the prefix: %q, want %q", mine, wantNames)
	}
}

// Every run shares one in-memory DynamoDB Local, so the tables a test creates are deleted
// when it ends.
func TestTablesDeletedAtEnd(t *testing.T) {
	t.Parallel()
	awsCfg := emulatortest.Config(t, nil)
	c := dynamodb.NewFromConfig(awsCfg)
	var names []string
	// t.Context is canceled before cleanups run.
	ctx := context.WithoutCancel(t.Context())
	// Registered first, so it runs after the cleanups that delete the tables.
	t.Cleanup(func() {
		for _, name := range names {
			var missing *types.ResourceNotFoundException
			if _, err := c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &name}); !errors.As(err, &missing) {
				t.Errorf("DescribeTable %s after the test: %v, want ResourceNotFoundException", name, err)
			}
		}
	})
	n := emulatortest.Tables(t, awsCfg, manifest(t))
	names = []string{n.StreamTable("page_views"), n.MetaTable(), n.ViewTable("daily", 1), emulatortest.Table(t, awsCfg)}
}

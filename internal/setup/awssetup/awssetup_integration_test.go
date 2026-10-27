// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package awssetup_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/setup"
	"github.com/lucindo/dynaflux/internal/setup/awssetup"
)

// lookup runs the setup function's stream lookup of table, as CloudFormation asks for it.
func lookup(t *testing.T, h *setup.Handler, table string) (setup.Response, error) {
	t.Helper()
	return h.Handle(t.Context(), setup.Request{Type: setup.TypeStreamARN, Action: setup.ActionCreate, Props: map[string]any{
		"ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:analytics-setup", "TableName": table,
	}})
}

// S2 needs both images of every record: the lookup accepts the stream tables that the
// templates create, returning their stream's ARN, and refuses a stream of another view.
func TestS2StreamViewChecked(t *testing.T) {
	t.Parallel()
	cfg := emulatortest.Config(t, nil)
	h := awssetup.New(cfg)
	naming := emulatortest.Tables(t, cfg, grantsManifest(t, "analytics"))
	c := dynamodb.NewFromConfig(cfg)
	for _, stream := range []string{"page_views", "clicks"} {
		table := naming.StreamTable(stream)
		resp, err := lookup(t, h, table)
		out, derr := c.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: &table})
		if derr != nil {
			t.Fatal(derr)
		}
		arn := *out.Table.LatestStreamArn
		if err != nil || resp.PhysicalID != arn || len(resp.Data) != 1 || resp.Data["StreamArn"] != arn {
			t.Errorf("lookup of %s: %+v, %v; want physical ID and StreamArn %s", table, resp, err, arn)
		}
	}

	keysOnly := naming.Prefix + "keys-only"
	createTable(t, c, keysOnly, &types.StreamSpecification{StreamEnabled: new(true), StreamViewType: types.StreamViewTypeKeysOnly})
	_, err := lookup(t, h, keysOnly)
	if want := "the stream of stream table " + keysOnly + " is KEYS_ONLY, not NEW_AND_OLD_IMAGES: " +
		"deploy the global.yaml that Dynaflux generates"; err == nil || err.Error() != want {
		t.Errorf("got %v\nwant %s", err, want)
	}
}

// DescribeTable reads what DynamoDB Local reports: no Replicas for a table that isn't a
// Global Table, and no stream for one without.
func TestDescribeTableOnDynamoDBLocal(t *testing.T) {
	t.Parallel()
	cfg := emulatortest.Config(t, nil)
	h := awssetup.New(cfg)
	c := dynamodb.NewFromConfig(cfg)
	plain := emulatortest.Table(t, cfg)
	got, found, err := h.DynamoDB.DescribeTable(t.Context(), plain)
	if want := (setup.StreamTable{Status: "ACTIVE"}); got != want || !found || err != nil {
		t.Errorf("table without a stream: %+v, %v, %v; want %+v", got, found, err, want)
	}

	streamed := plain + "-streamed"
	createTable(t, c, streamed, &types.StreamSpecification{StreamEnabled: new(true), StreamViewType: types.StreamViewTypeNewAndOldImages})
	got, found, err = h.DynamoDB.DescribeTable(t.Context(), streamed)
	if got.Status != "ACTIVE" || got.StreamARN == "" || got.StreamView != "NEW_AND_OLD_IMAGES" || got.ReplicaStatus != "" || !found || err != nil {
		t.Errorf("table with a stream: %+v, %v, %v; want ACTIVE with a NEW_AND_OLD_IMAGES stream and no replica", got, found, err)
	}

	got, found, err = h.DynamoDB.DescribeTable(t.Context(), plain+"-missing")
	if got != (setup.StreamTable{}) || found || err != nil {
		t.Errorf("missing table: %+v, %v, %v; want not found", got, found, err)
	}
}

// createTable creates a table with Dynaflux's keys and stream, deleted when the test ends.
func createTable(t *testing.T, c *dynamodb.Client, name string, stream *types.StreamSpecification) {
	t.Helper()
	_, err := c.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName:   &name,
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: new("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: new("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: new("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: new("sk"), KeyType: types.KeyTypeRange},
		},
		StreamSpecification: stream,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		if _, derr := c.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: &name}); derr != nil {
			t.Errorf("delete table %s: %v", name, derr)
		}
	})
}

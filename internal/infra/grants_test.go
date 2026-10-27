// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra_test

import (
	"slices"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/infra"
)

func TestFunctionRole(t *testing.T) {
	t.Parallel()
	m, err := buildWith(t, "store: analytics\nregions: [us-east-1]\n", "CREATE STREAM page_views (k String) PRIMARY KEY (k);", infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for mode, want := range map[string]string{catalog.EnvModeSetup: "RoleSetup", catalog.EnvModeProjector: "RoleProjector"} {
		if r, rerr := m.FunctionRole(mode); rerr != nil || r.ID != want {
			t.Errorf("FunctionRole(%s) = %s, %v; want %s", mode, r.ID, rerr, want)
		}
	}
	if _, err = m.FunctionRole("reader"); err == nil || err.Error() != "infra: no function runs in mode reader" {
		t.Errorf("FunctionRole(reader) fails with %v, want no such function", err)
	}
}

// IAM's * matches any run of characters, / included, and ? any one; every other character
// matches itself. A condition holds when the request has its key with one of its values.
func TestAllowing(t *testing.T) {
	t.Parallel()
	m, err := buildWith(t, "store: analytics\nregions: [us-east-1]\n", "CREATE STREAM page_views (k String) PRIMARY KEY (k);", infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	const table = "arn:${partition}:dynamodb:${region}:${account}:table/${prefix}"
	role := infra.Role{Statements: []infra.Statement{
		{Sid: "Tables", Actions: []string{"dynamodb:GetItem"}, Resources: []infra.Text{table + "a-?", table + "x.y"}},
		{Sid: "Prefixed", Actions: []string{"dynamodb:PutItem"}, Resources: []infra.Text{table + "*"}},
		{Sid: "Both", Actions: []string{"dynamodb:GetItem", "dynamodb:PutItem"}, Resources: []infra.Text{table + "a-1"}},
		{
			Sid: "Pass", Actions: []string{"iam:PassRole"}, Resources: []infra.Text{"${ref:RoleSetup.arn}"},
			Conditions: []infra.Condition{{
				Operator: "StringEquals", Key: "iam:PassedToService", Values: []infra.Text{"glue.amazonaws.com", "lambda.amazonaws.com"},
			}},
		},
		{
			Sid: "Listed", Actions: []string{"s3:ListBucket"}, Resources: []infra.Text{"arn:${partition}:s3:::b"},
			Conditions: []infra.Condition{{Operator: "StringLike", Key: "s3:prefix", Values: []infra.Text{"w/*"}}},
		},
		{Sid: "Objects", Actions: []string{"s3:GetObject"}, Resources: []infra.Text{"${ref:Bucket.arn}/*"}},
	}}
	d := infra.Deployment{Prefix: "prod-", Partition: "aws", Account: "123456789012", Region: "us-east-1"}
	const arn = "arn:aws:dynamodb:us-east-1:123456789012:table/prod-"
	const setupRole = "arn:aws:iam::123456789012:role/RoleSetup"
	tests := []struct {
		action, arn string
		keys        map[string]string
		want        []string
		wantErr     string
	}{
		{action: "dynamodb:GetItem", arn: arn + "a-1", want: []string{"Tables", "Both"}},
		{action: "dynamodb:GetItem", arn: arn + "a-12"},
		{action: "dynamodb:GetItem", arn: arn + "x.y", want: []string{"Tables"}},
		{action: "dynamodb:GetItem", arn: arn + "xzy"},
		{action: "dynamodb:PutItem", arn: arn + "a-1/index/k", want: []string{"Prefixed"}},
		{action: "dynamodb:PutItem", arn: "arn:aws:dynamodb:us-east-1:123456789012:table/dev-a-1"},
		{action: "iam:PassRole", arn: setupRole, keys: map[string]string{"iam:PassedToService": "lambda.amazonaws.com"}, want: []string{"Pass"}},
		{action: "iam:PassRole", arn: setupRole, keys: map[string]string{"iam:PassedToService": "ec2.amazonaws.com"}},
		{action: "iam:PassRole", arn: setupRole},
		{action: "s3:ListBucket", arn: "arn:aws:s3:::b", wantErr: "infra: statement Listed: condition operator StringLike isn't supported"},
		{
			action: "s3:GetObject", arn: "arn:aws:s3:::b/k",
			wantErr: "infra: ${ref:Bucket.arn}: Resolve resolves references to tables, delivery streams, archive workgroups and roles only",
		},
	}
	for _, tt := range tests {
		got, aerr := m.Allowing(role, d, tt.action, tt.arn, tt.keys)
		var gotErr string
		if aerr != nil {
			gotErr = aerr.Error()
		}
		if !slices.Equal(got, tt.want) || gotErr != tt.wantErr {
			t.Errorf("Allowing(%s, %s, %v) = %q, %q; want %q, %q", tt.action, tt.arn, tt.keys, got, gotErr, tt.want, tt.wantErr)
		}
	}
}

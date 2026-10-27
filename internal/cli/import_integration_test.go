// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cli_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// TestImportEndpoint imports into a store on DynamoDB Local with -endpoint, its clock
// checked by DynamoDB Local's Date, and imports again: with DEDUP BY, every event is
// already present.
func TestImportEndpoint(t *testing.T) {
	t.Parallel()
	dir := writeProject(t, "import")
	res := compile(t, dir)
	m, err := infra.Build(res.Config, res.Catalog, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := emulatortest.Config(t, nil)
	cfg.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: cli.LocalAccessKey, SecretAccessKey: "fake"}, nil
	})
	naming := emulatortest.Tables(t, cfg, m)
	data, err := catalog.Marshal(res.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = meta.New(dynamo.New(dynamodb.NewFromConfig(cfg)), naming.MetaTable()).RegisterCatalog(t.Context(), data, "v0.5.0"); err != nil {
		t.Fatal(err)
	}

	args := []string{"import", "payments", "-file", "input/payments.jsonl", "-yes",
		"-region", emulatortest.Region, "-endpoint", *cfg.BaseEndpoint, "-prefix", naming.Prefix}
	stderr := "store shop  prefix " + naming.Prefix + "  endpoint " + *cfg.BaseEndpoint + "  region us-east-1\n" +
		"plan: import 2 events of input/payments.jsonl into payments in us-east-1, at most 500 a second; an import can't be undone\n"
	for i, want := range []result{
		{0, "2 events: 2 stored, 0 already present, 0 conflicting\n", stderr},
		{0, "2 events: 0 stored, 2 already present, 0 conflicting\n", stderr},
	} {
		var stdout, errOut strings.Builder
		status := cli.Run(t.Context(), args, nil, &stdout, &errOut, dir)
		if got := (result{status, stdout.String(), errOut.String()}); got != want {
			t.Errorf("run %d:\ngot  %+v\nwant %+v", i+1, got, want)
		}
	}
}

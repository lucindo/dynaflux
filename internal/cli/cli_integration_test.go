// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cli_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// TestEndpoint runs query and inspect with -endpoint on a store deployed to DynamoDB Local,
// its catalog registered in the meta table.
func TestEndpoint(t *testing.T) {
	t.Parallel()
	dir := writeProject(t, "deployed")
	res := compile(t, dir)
	m, err := infra.Build(res.Config, res.Catalog, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// The tables belong to the access key that -endpoint sends: DynamoDB Local keeps each
	// key's tables apart.
	cfg := emulatortest.Config(t, nil)
	cfg.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: cli.LocalAccessKey, SecretAccessKey: "fake"}, nil
	})
	naming := emulatortest.Tables(t, cfg, m)
	deploy(t, dir, dynamo.New(dynamodb.NewFromConfig(cfg)), naming)

	local := []string{"-region", emulatortest.Region, "-endpoint", *cfg.BaseEndpoint, "-prefix", naming.Prefix}
	head := "store shop  prefix " + naming.Prefix + "  endpoint " + *cfg.BaseEndpoint + "  region us-east-1\n"
	tests := []struct {
		name string
		args []string
		want result
	}{
		{
			"query of a view", []string{"query", "PageStats", "-param", "page=/home", "-param", "from=1999-12-30", "-param", "to=1999-12-31"},
			result{0, "day                   views  visitors  avg_ms\n" +
				"1999-12-30T00:00:00Z  1      1         300\n" +
				"1999-12-31T00:00:00Z  2      2         150\n", head},
		},
		{
			"query of a stream", []string{"query", "UserEvents", "-json", "-param", "users=['u2']"},
			result{0, `{"event_time":"1999-12-31T11:00:00.000000Z","page":"/home","tags":[]}` + "\n", head},
		},
		{
			"query of a version that isn't live", []string{"query", "UserViews", "-param", "users=['u1']"},
			result{1, "", head + "dynaflux query: view version not ready: user_pages@v1 is pending_backfill in us-east-1\n" +
				"  hint: dynaflux dev -endpoint " + *cfg.BaseEndpoint + " fills it; wait for \"user_pages@v1: live\" in its output, or read it anyway with -allow-partial\n"},
		},
		{
			"a prefix without tables", []string{"query", "PageStats", "-prefix", "none-"},
			result{1, "", "store shop  prefix none-  endpoint " + *cfg.BaseEndpoint + "  region us-east-1\n" +
				"dynaflux query: us-east-1: no catalog is deployed in none-shop-meta\n  hint: deploy regional.yaml in us-east-1\n"},
		},
		{
			"inspect of a view's partition", []string{"inspect", "page_daily", "-key", "page=/home"},
			result{0, "page   day                   views  visitors  avg_ms\n" +
				"/home  1999-12-30T00:00:00Z  1      1         300\n" +
				"/home  1999-12-31T00:00:00Z  2      2         150\n", head},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr strings.Builder
			status := cli.Run(t.Context(), slices.Concat(tt.args[:1], local, tt.args[1:]), nil, &stdout, &stderr, dir)
			if got := (result{status, stdout.String(), stderr.String()}); got != tt.want {
				t.Errorf("got  %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

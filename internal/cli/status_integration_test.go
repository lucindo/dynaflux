// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cli_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// localStore is testdata/status.txtar deployed to DynamoDB Local in tables of the access
// key that -endpoint sends: page_counts@v1 live, page_seen@v1 broken, plan_counts@v1
// pending, and gone@v1, which the catalog doesn't define, live on page_views.
type localStore struct {
	dir      string
	endpoint string
	naming   catalog.Naming
	b        store.Backend
	meta     *meta.Meta
}

func newLocalStore(t *testing.T) *localStore {
	t.Helper()
	ctx := t.Context()
	dir := writeProject(t, "status")
	res := compile(t, dir)
	m, err := infra.Build(res.Config, res.Catalog, infra.Options{})
	must(t, err)
	cfg := emulatortest.Config(t, nil)
	cfg.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: cli.LocalAccessKey, SecretAccessKey: "fake"}, nil
	})
	s := &localStore{dir: dir, endpoint: *cfg.BaseEndpoint, naming: emulatortest.Tables(t, cfg, m)}
	s.b = dynamo.New(dynamodb.NewFromConfig(cfg))
	s.meta = meta.New(s.b, s.naming.MetaTable())
	data, err := catalog.Marshal(res.Catalog)
	must(t, err)
	_, err = s.meta.RegisterCatalog(ctx, data, "v0.5.0")
	must(t, err)
	for _, v := range []string{"page_counts@v1", "page_seen@v1", "gone@v1"} {
		_, err = s.meta.Activate(ctx, v, "page_views", meta.SplitNone, meta.DefaultBounds)
		must(t, err)
		must(t, s.meta.MakeLive(ctx, v, time.Time{}))
	}
	markBroken(t, s.meta, "page_seen@v1", "page_views", meta.Failure{Cause: meta.CauseRecord, EventID: "e1", Error: "division by zero", At: time.Now()})
	return s
}

// status reads the store's meta-derived findings from DynamoDB Local, with -endpoint and,
// for a job's executions, with a fake Step Functions.
func TestStatusOnDynamoDBLocal(t *testing.T) {
	t.Parallel()
	s := newLocalStore(t)
	var stdout, stderr strings.Builder
	status := cli.Run(t.Context(), []string{"status", "-endpoint", s.endpoint, "-prefix", s.naming.Prefix, "-verbose"}, nil, &stdout, &stderr, s.dir)
	for _, want := range []string{
		"us-east-1: error DF7201: page_seen@v1 broke at ",
		"us-east-1: warning DF7202: plan_counts@v1 needs a backfill\n  hint: dynaflux backfill plan_counts@v1 -region us-east-1 -prefix " + s.naming.Prefix + "\n",
		"us-east-1: error DF7209: the projector of stream page_views fails every batch: gone@v1 is live, " +
			"and the deployed catalog neither defines nor retires it; records are lost 24 hours after they were written",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr lacks %q:\n%s", want, stderr.String())
		}
	}
	for _, want := range []string{"gone@v1         live\n", "page_seen@v1    broken\n", "plan_counts@v1  pending_backfill\n", "  stream signups:\n    unit shardId-"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("stdout lacks %q:\n%s", want, stdout.String())
		}
	}
	if status != 1 {
		t.Errorf("status %d, want 1", status)
	}

	steps := &fakeSFN{status: map[string]types.ExecutionStatus{}}
	arn := "arn:aws:states:" + emulatortest.Region + ":" + account + ":execution:" + s.naming.BackfillStateMachine() + ":plan-counts-v1-0a1b2c-a1"
	_, err := backfill.Start(t.Context(), s.meta, backfill.Request{
		Version: "plan_counts@v1", Stream: "signups", Name: "plan-counts-v1-0a1b2c", Execution: func(int) string { return arn }, Split: meta.SplitTime,
	})
	must(t, err)
	steps.status[arn] = types.ExecutionStatusFailed
	stderr.Reset()
	cli.RunEnv(t.Context(), []string{"status", "-prefix", s.naming.Prefix, "-region", emulatortest.Region}, nil, &strings.Builder{}, &stderr, s.dir, cli.Env{
		Region: emulatortest.Region, Account: account, Backends: map[string]store.Backend{emulatortest.Region: s.b},
		StepFunctions: map[string]cli.StepFunctions{emulatortest.Region: steps},
		AWS:           map[string]cli.AWS{emulatortest.Region: healthyAWS(s.naming, emulatortest.Region, compile(t, s.dir).Catalog)},
	})
	if want := "us-east-1: warning DF7216: the backfill of plan_counts@v1 is interrupted: its execution " + arn + " ended without finishing\n"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr lacks %q:\n%s", want, stderr.String())
	}
}

// diff compares the project with its catalog on DynamoDB Local, and with the region's
// states there.
func TestDiffOnDynamoDBLocal(t *testing.T) {
	t.Parallel()
	s := newLocalStore(t)
	run := func() result {
		var stdout, stderr strings.Builder
		status := cli.Run(t.Context(), []string{"diff", "-endpoint", s.endpoint, "-prefix", s.naming.Prefix}, nil, &stdout, &stderr, s.dir)
		return result{status, stdout.String(), stderr.String()}
	}
	data, err := catalog.Marshal(compile(t, s.dir).Catalog)
	must(t, err)
	head := "store shop  prefix " + s.naming.Prefix + "  endpoint " + s.endpoint + "  region us-east-1\n"
	same := "us-east-1: catalog " + catalog.Hash(data)[:12] + ", the same as the local one\n"
	if got, want := run(), (result{0, same, head}); got != want {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
	_, err = s.meta.Retire(t.Context(), "plan_counts@v1", "signups")
	must(t, err)
	want := result{1, same, head + "us-east-1: error DF7114: dynaflux.lock keeps plan_counts@v1, which is retired in us-east-1\n" +
		"  hint: give the view a new version, by changing it or with dynaflux generate -rebuild plan_counts, then dynaflux retire plan_counts@v1\n"}
	if got := run(); got != want {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
}

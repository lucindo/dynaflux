// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// backfill starts, leaves, cancels and resumes a job whose items are on DynamoDB Local, with
// a fake Step Functions.
func TestBackfillOnDynamoDBLocal(t *testing.T) {
	t.Parallel()
	dir := writeProject(t, "backfill")
	res := compile(t, dir)
	lock, err := catalog.MarshalLock(res.Lock)
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, catalog.LockFileName), lock, 0o644)
	}
	if err != nil {
		t.Fatal(err)
	}
	m, err := infra.Build(res.Config, res.Catalog, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := emulatortest.Config(t, nil)
	naming := emulatortest.Tables(t, cfg, m)
	b := dynamo.New(dynamodb.NewFromConfig(cfg))
	data, err := catalog.Marshal(res.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	mt := meta.New(b, naming.MetaTable())
	if _, err = mt.RegisterCatalog(t.Context(), data, "v0.5.0"); err != nil {
		t.Fatal(err)
	}
	cat, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	e := engine.New(b, engine.Options{Region: emulatortest.Region, Prefix: naming.Prefix})
	if _, err = e.Append(t.Context(), cat, "page_views", [][]expr.Value{{expr.String("u1"), expr.String("/home"), expr.Null()}}, engine.AppendMode{}); err != nil {
		t.Fatal(err)
	}
	steps := &fakeSFN{status: map[string]types.ExecutionStatus{}}
	tables := &fakeReplicas{born: map[string]time.Time{naming.StreamTable("page_views"): time.Now().Add(-72 * time.Hour)}}
	run := func(args ...string) result {
		t.Helper()
		var stdout, stderr strings.Builder
		status := cli.RunEnv(t.Context(), append([]string{"backfill", "-region", emulatortest.Region, "-prefix", naming.Prefix}, args...),
			nil, &stdout, &stderr, dir, cli.Env{
				Region: emulatortest.Region, Account: account, Backends: map[string]store.Backend{emulatortest.Region: b},
				StepFunctions: map[string]cli.StepFunctions{emulatortest.Region: steps}, Tables: map[string]backfill.Tables{emulatortest.Region: tables},
				AWS: map[string]cli.AWS{emulatortest.Region: healthyAWS(naming, emulatortest.Region, res.Catalog)},
			})
		return result{status, stdout.String(), stderr.String()}
	}
	job := func(state meta.JobState, attempt int) {
		t.Helper()
		j, ok, jerr := mt.Job(t.Context(), "page_seen@v1")
		if jerr != nil || !ok || j.State != state || j.Attempt != attempt || j.Split != meta.SplitNone {
			t.Fatalf("job %+v, %v, %v; want %s at attempt %d, not split", j, ok, jerr, state, attempt)
		}
	}
	if got := run("page_seen", "-yes"); got.status != 0 || !strings.Contains(got.stdout, "page_seen@v1: started in execution page_seen-v1-") {
		t.Fatalf("start: %+v", got)
	}
	job(meta.JobRunning, 1)
	if got := run("page_seen"); got.status != 0 || !strings.Contains(got.stdout, "page_seen@v1  running") || len(steps.started) != 1 {
		t.Errorf("a running job: %+v, started %q", got, steps.started)
	}
	if got := run("page_seen", "-cancel"); got.status != 0 || len(steps.stopped) != 1 {
		t.Errorf("cancel: %+v, stopped %q", got, steps.stopped)
	}
	job(meta.JobCancelled, 1)
	// It would start the other versions' jobs, which it asks about first.
	if got := run("-all"); got.status != 2 || strings.Contains(got.stdout, "page_seen@v1") || !strings.Contains(got.stdout, "plan_counts@v1  start") {
		t.Errorf("-all with the job cancelled: %+v", got)
	}
	if got := run("page_seen"); got.status != 0 || !strings.Contains(got.stdout, "page_seen@v1: resumed as attempt 2 in execution page_seen-v1-") {
		t.Errorf("resume: %+v", got)
	}
	job(meta.JobRunning, 2)
}

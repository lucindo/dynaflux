// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package fluxruntime_test

import (
	"bytes"
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"

	"github.com/lucindo/dynaflux/fluxruntime"
	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
)

const functionARN = "arn:aws:lambda:" + region + ":123456789012:function:app-backfill"

type longAgo struct{}

func (longAgo) Born(context.Context, string) (time.Time, bool, error) {
	return time.Now().Add(-48 * time.Hour), false, nil
}

type noProjectorLag struct{}

func (noProjectorLag) Lag(context.Context) (time.Duration, error) { return 0, nil }

type noUnits struct{}

func (noUnits) Units(context.Context, string) ([]string, error) { return nil, nil }

// backfillRig is a backfill function of store app in us-east-1, with its catalog
// registered, over an in-memory store whose events a pending rig appends.
type backfillRig struct {
	*rig

	m        *meta.Meta
	runs     *backfill.MemRuns
	buckets  []string // the bucket of each invocation's runs
	archives []string // and of its archive
	logs     *bytes.Buffer
	invoke   func(context.Context, []byte) ([]byte, error)
}

func newBackfillRig(t *testing.T) *backfillRig {
	t.Helper()
	r := &backfillRig{rig: newPendingRig(t, compile(t, false), ""), runs: backfill.NewMemRuns(), logs: &bytes.Buffer{}}
	r.m = meta.New(r.mem, catalog.Naming{Store: "app"}.MetaTable())
	data, err := catalog.Marshal(r.cat.Catalog())
	if err == nil {
		_, err = r.m.RegisterCatalog(t.Context(), data, "v0.5.0")
	}
	if err != nil {
		t.Fatal(err)
	}
	r.invoke, err = fluxruntime.NewBackfill(r.cat, fluxruntime.Env{Region: region, Memory: 64 << 20}, fluxruntime.BackfillParts{
		Store: r.mem, Units: noUnits{}, Tables: longAgo{}, Lag: noProjectorLag{},
		Runs: func(bucket string) backfill.Runs {
			r.buckets = append(r.buckets, bucket)
			return r.runs
		},
		Archive: func(bucket string) fluxruntime.ArchiveReader {
			r.archives = append(r.archives, bucket)
			return awsbackfill.NewArchive(awsbackfill.Clients{}, catalog.Naming{Store: "app"}, r.cat.Catalog(), bucket)
		},
	}, r.logs)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// step invokes the function as the state machine does, with Lambda's 900 s deadline.
func (r *backfillRig) step(t *testing.T, event, arn string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 900*time.Second)
	defer cancel()
	ctx = lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{AwsRequestID: "req-1", InvokedFunctionArn: arn})
	out, err := r.invoke(ctx, []byte(event))
	return string(out), err
}

// Each invocation steps the job until it waits or ends, and tells the state machine so: a
// time split waits until every event at or before the split can have landed, then the job
// reads the table and fills the version. The staged runs and the archive are in the
// account's bucket.
func TestBackfillSteps(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newBackfillRig(t)
		r.click(t, "u1", "/a", 100)
		r.click(t, "u2", "/a", 200)
		if _, err := backfill.Start(t.Context(), r.m, backfill.Request{
			Version: "page_totals@v1", Stream: "clicks", Name: "page_totals-v1-0a1b2c", Execution: func(int) string { return "arn:x" }, Split: meta.SplitTime,
		}); err != nil {
			t.Fatal(err)
		}
		const event = `{"job":"page_totals@v1","attempt":1,"execution":"arn:aws:states:us-east-1:123456789012:execution:app-backfill:page_totals-v1-0a1b2c-a1"}`
		// The listing delay, ε, ε and L, and the clock margin: 90 s + 5 + 5 + 15 + 1 minutes.
		if out, err := r.step(t, event, functionARN); err != nil || out != `{"done":false,"wait_seconds":1650}` {
			t.Fatalf("first step: %s, %v", out, err)
		}
		time.Sleep(1650 * time.Second)
		if out, err := r.step(t, event, functionARN); err != nil || out != `{"done":true,"wait_seconds":0}` {
			t.Fatalf("second step: %s, %v", out, err)
		}
		states, err := r.m.ViewStates(t.Context())
		if err != nil || states["page_totals@v1"].State != meta.Live {
			t.Errorf("page_totals@v1 is %+v, %v; want live", states["page_totals@v1"], err)
		}
		if got := r.row(t, "page_totals@v1", "/a"); got != "/a 2" {
			t.Errorf("row %q, want /a 2", got)
		}
		j, _, err := r.m.Job(t.Context(), "page_totals@v1")
		if err != nil || j.State != meta.JobDone || j.Execution != "arn:aws:states:us-east-1:123456789012:execution:app-backfill:page_totals-v1-0a1b2c-a1" {
			t.Errorf("job %+v, %v; want done by the execution", j, err)
		}
		if want := []string{"app-123456789012-us-east-1-an", "app-123456789012-us-east-1-an"}; !slices.Equal(r.buckets, want) ||
			!slices.Equal(r.archives, want) {
			t.Errorf("runs in buckets %q and the archive in %q, want %q", r.buckets, r.archives, want)
		}
	})
}

// An invocation that can't step fails, and its log line says why.
func TestBackfillRefuses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, event, arn, err string
	}{
		{"not JSON", `{"job":`, functionARN, "fluxruntime: decode the event: unexpected end of JSON input"},
		{"no attempt", `{"job":"page_totals@v1","execution":"arn:x"}`, functionARN,
			`fluxruntime: the event {"job":"page_totals@v1","execution":"arn:x"} lacks a job, an attempt or an execution`},
		{"no account", `{"job":"page_totals@v1","attempt":1,"execution":"arn:x"}`, "app-backfill",
			`fluxruntime: the invoked function's ARN "app-backfill" names no account`},
		{"no job", `{"job":"page_totals@v1","attempt":1,"execution":"arn:x"}`, functionARN,
			"backfill: page_totals@v1: meta: the version has no job"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := newBackfillRig(t)
			if _, err := r.step(t, tt.event, tt.arn); err == nil || err.Error() != tt.err {
				t.Errorf("step: %v, want %q", err, tt.err)
			}
			lines := jsonLines(t, r.logs.String())
			if len(lines) != 1 || lines[0]["level"] != "ERROR" || lines[0]["msg"] != "step failed" || lines[0]["error"] != tt.err ||
				lines[0]["requestId"] != "req-1" {
				t.Errorf("log lines %v, want the failure at ERROR", lines)
			}
		})
	}
}

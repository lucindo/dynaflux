// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package runtime_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill/awsbackfilltest"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/meta"
)

// stepAPI is a Lambda Runtime API that hands the child one event at a time and returns its
// reply, as the state machine's Task does.
type stepAPI struct {
	srv     *httptest.Server
	events  chan string
	replies chan string // the body of a response, or "error: " and the body of an error
}

func newStepAPI(t *testing.T) *stepAPI {
	t.Helper()
	a := &stepAPI{events: make(chan string), replies: make(chan string)}
	n := 0
	a.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			select {
			case ev := <-a.events:
				n++
				w.Header().Set("Lambda-Runtime-Aws-Request-Id", "req-"+strconv.Itoa(n))
				w.Header().Set("Lambda-Runtime-Deadline-Ms", strconv.FormatInt(time.Now().Add(900*time.Second).UnixMilli(), 10))
				w.Header().Set("Lambda-Runtime-Invoked-Function-Arn", "arn:aws:lambda:"+emulatortest.Region+":123456789012:function:sim-backfill")
				io.WriteString(w, ev)
			case <-r.Context().Done():
			}
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		w.WriteHeader(http.StatusAccepted)
		reply := string(body)
		if strings.HasSuffix(r.URL.Path, "/error") {
			reply = "error: " + reply
		}
		a.replies <- reply
	}))
	t.Cleanup(a.srv.Close)
	return a
}

// step sends an event and waits for the child's reply.
func (a *stepAPI) step(t *testing.T, c *child, event string) string {
	t.Helper()
	select {
	case a.events <- event:
	case <-time.After(30 * time.Second):
		t.Fatalf("the child didn't ask for an invocation; stderr:\n%s", &c.stderr)
	}
	select {
	case reply := <-a.replies:
		return reply
	case <-time.After(120 * time.Second):
		t.Fatalf("the child didn't reply; stderr:\n%s", &c.stderr)
	}
	return ""
}

// The runtime binary in backfill mode fills a version from the stream table on DynamoDB
// Local, step by step as a Go loop plays the state machine: the first step activates it and
// asks to wait for the listing delay, which the loop skips by moving the job's read_after;
// the next stages the table in S3, applies it, makes the version live and deletes the
// staged runs.
func TestRunStepsBackfill(t *testing.T) {
	t.Parallel()
	const version = "page_users@v1"
	r := newLocalRig(t, version)
	r.pageView("/a", "u1", 100)
	r.pageView("/a", "u2", 200)
	r.pageView("/b", "u1", 300)
	data, err := catalog.Marshal(rules.cat.Catalog())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.meta.RegisterCatalog(t.Context(), data, "v0.5.0"); err != nil {
		t.Fatal(err)
	}
	const execution = "arn:aws:states:" + emulatortest.Region + ":123456789012:execution:sim-backfill:page_users-v1-0a1b2c-a1"
	if _, err = backfill.Start(t.Context(), r.meta, backfill.Request{
		Version: version, Stream: pageViews, Name: "page_users-v1-0a1b2c", Execution: func(int) string { return execution }, Split: meta.SplitNone,
	}); err != nil {
		t.Fatal(err)
	}
	s3 := awsbackfilltest.NewS3()
	s3Server := httptest.NewServer(s3)
	t.Cleanup(s3Server.Close)
	metrics := httptest.NewServer(http.HandlerFunc(awsbackfilltest.CloudWatch))
	t.Cleanup(metrics.Close)
	api := newStepAPI(t)
	c := startChildAt(t, childRun, api.srv.URL, map[string]string{
		catalogVar:                        string(data),
		"DYNAFLUX_MODE":                   "backfill",
		"DYNAFLUX_STORE":                  rules.cat.Catalog().Store,
		"DYNAFLUX_PREFIX":                 r.naming.Prefix,
		"AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "512",
		"AWS_ENDPOINT_URL_DYNAMODB":       aws.ToString(r.cfg.BaseEndpoint),
		"AWS_ENDPOINT_URL_S3":             s3Server.URL,
		"AWS_ENDPOINT_URL_CLOUDWATCH":     metrics.URL,
	})
	const event = `{"job":"` + version + `","attempt":1,"execution":"` + execution + `"}`
	if reply := api.step(t, c, event); reply != `{"done":false,"wait_seconds":90}` {
		t.Fatalf("first step: %s; stderr:\n%s", reply, &c.stderr)
	}
	if _, err = r.meta.UpdateJob(t.Context(), version, func(j *meta.Job) (bool, error) {
		j.ReadAfter = time.Now().Add(-time.Second)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if reply := api.step(t, c, event); reply != `{"done":true,"wait_seconds":0}` {
		t.Fatalf("second step: %s; stderr:\n%s", reply, &c.stderr)
	}
	states, err := r.meta.ViewStates(t.Context())
	if err != nil || states[version].State != meta.Live {
		t.Errorf("%s is %+v, %v; want live", version, states[version], err)
	}
	r.checkRow("/a u1", version, "/a", "u1")
	r.checkRow("/a u2", version, "/a", "u2")
	r.checkRow("/b u1", version, "/b", "u1")
	bucket := r.naming.Bucket("123456789012", emulatortest.Region)
	if keys := s3.Keys(bucket, ""); len(keys) != 0 {
		t.Errorf("staged objects %q are left in %s", keys, bucket)
	}
	j, _, err := r.meta.Job(t.Context(), version)
	if err != nil || j.State != meta.JobDone || j.Staged != 3 || j.Applied != 3 {
		t.Errorf("job %+v, %v; want done, 3 staged and applied", j, err)
	}
}

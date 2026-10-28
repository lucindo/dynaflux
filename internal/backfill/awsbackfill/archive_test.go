// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfill_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill/awsbackfilltest"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/validate"
)

// archiveCatalog is a store that archives page_views.
func archiveCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	_, cat, ds := validate.Load(fstest.MapFS{
		"dynaflux.yaml":      {Data: []byte("store: analytics\nregions: [us-east-1]\n")},
		"dynaflux/store.sql": {Data: []byte("CREATE STREAM page_views (page String, ms UInt32) PRIMARY KEY (page);")},
	}, "/project")
	if cat == nil {
		t.Fatalf("the store doesn't compile: %v", ds)
	}
	return cat
}

// answer is an HTTP response of a recorded body.
type answer struct {
	status int
	body   string
}

func ok(body string) answer { return answer{http.StatusOK, body} }

func refused(code, message string) answer {
	return answer{http.StatusBadRequest, `{"__type":"` + code + `","Message":"` + message + `"}`}
}

// recorded answers the operations of a JSON protocol in order from queues of recorded
// bodies, the last one again once its queue is down to it, and keeps each request's body.
type recorded struct {
	mu      sync.Mutex
	answers map[string][]answer
	bodies  map[string][]string
}

func newRecorded(answers map[string][]answer) *recorded {
	return &recorded{answers: answers, bodies: map[string][]string{}}
}

func (f *recorded) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	_, op, _ := strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	f.mu.Lock()
	f.bodies[op] = append(f.bodies[op], string(body))
	queue := f.answers[op]
	if len(queue) == 0 {
		f.mu.Unlock()
		http.Error(w, "unexpected "+op, http.StatusNotImplemented)
		return
	}
	a := queue[0]
	if len(queue) > 1 {
		f.answers[op] = queue[1:]
	}
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	if a.status != http.StatusOK {
		w.Header().Set("X-Amzn-Errortype", strings.SplitN(a.body, `"`, 5)[3])
	}
	w.WriteHeader(a.status)
	io.WriteString(w, a.body)
}

// regionClients holds the clients of a region whose services the tests fake.
type regionClients struct {
	athena   http.Handler
	s3       *awsbackfilltest.S3
	metrics  *awsbackfilltest.Metrics
	firehose *awsbackfilltest.Firehose
	lambda   *awsbackfilltest.Lambda
	ddb      awsbackfill.DynamoDB
}

// archiveOf returns the archive whose calls go to the fakes of rc, in the bucket, at the
// clock now.
func archiveOf(t *testing.T, rc regionClients, now time.Time) *awsbackfill.Archive {
	t.Helper()
	c := awsbackfill.Clients{DynamoDB: rc.ddb}
	if rc.athena != nil {
		c.Athena = athena.NewFromConfig(config(rc.athena))
	}
	if rc.s3 != nil {
		c.S3 = s3.NewFromConfig(config(rc.s3), func(o *s3.Options) { o.UsePathStyle = true })
	}
	if rc.metrics != nil {
		cw := cloudwatch.NewFromConfig(config(rc.metrics))
		c.CloudWatch, c.Metrics = cw, cw
	}
	if rc.firehose != nil {
		c.Firehose = firehose.NewFromConfig(config(rc.firehose))
	}
	if rc.lambda != nil {
		c.Lambda = lambda.NewFromConfig(config(rc.lambda))
	}
	a := awsbackfill.NewArchive(c, catalog.Naming{Prefix: "prod-", Store: "analytics"}, archiveCatalog(t), bucket)
	a.Now = func() time.Time { return now }
	return a
}

const queryID = "0f5e9c1d-7a2b-4c3d-8e9f-a0b1c2d3e4f5"

// A query starts in the backfill workgroup with the job's token and package archive's
// statement; an export writes under its destination in the bucket.
func TestArchiveStart(t *testing.T) {
	t.Parallel()
	f := newRecorded(map[string][]answer{"StartQueryExecution": {ok(`{"QueryExecutionId":"` + queryID + `"}`)}})
	a := archiveOf(t, regionClients{athena: f}, time.Time{})
	from, to := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	queries := []backfill.Query{
		{Kind: backfill.QueryPartitions, Stream: "page_views"},
		{Kind: backfill.QueryExport, Stream: "page_views", From: from, To: to, Dest: "backfill/page_daily@v2/1/unload/plan-1-ab/1-2/1/"},
	}
	for i, q := range queries {
		id, err := a.Start(t.Context(), q, "token-"+string(rune('a'+i)))
		if err != nil || id != queryID {
			t.Fatalf("Start(%+v) = %q, %v; want %s", q, id, err, queryID)
		}
	}
	s, _ := archiveCatalog(t).Stream("page_views")
	want := []string{archive.PartitionsQuery("prod_analytics", s),
		archive.Unload("prod_analytics", s, from, to, "s3://"+bucket+"/backfill/page_daily@v2/1/unload/plan-1-ab/1-2/1/")}
	for i, body := range f.bodies["StartQueryExecution"] {
		var in struct {
			QueryString        string `json:"QueryString"`
			ClientRequestToken string `json:"ClientRequestToken"`
			WorkGroup          string `json:"WorkGroup"`
		}
		if err := json.Unmarshal([]byte(body), &in); err != nil {
			t.Fatal(err)
		}
		if in.QueryString != want[i] || in.ClientRequestToken != "token-"+string(rune('a'+i)) || in.WorkGroup != "prod-analytics-backfill" {
			t.Errorf("request %d: %+v\nwant the statement %q, its token and the backfill workgroup", i, in, want[i])
		}
	}
}

// Athena's refusal for the account's query concurrency or its request rate is ErrThrottled,
// for the job to start the query again a minute later with the same token; another refusal
// fails with Athena's message.
func TestArchiveStartRefused(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		answer    answer
		throttled bool
		want      string
	}{
		{"concurrency", refused("TooManyRequestsException", "Rate exceeded"), true, "Rate exceeded"},
		{"request rate", refused("ThrottlingException", "Rate exceeded"), true, "Rate exceeded"},
		{"invalid", refused("InvalidRequestException", "line 1:8: mismatched input"), false, "line 1:8: mismatched input"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := archiveOf(t, regionClients{athena: newRecorded(map[string][]answer{"StartQueryExecution": {tt.answer}})}, time.Time{})
			_, err := a.Start(t.Context(), backfill.Query{Kind: backfill.QueryPartitions, Stream: "page_views"}, "token")
			if err == nil || errors.Is(err, backfill.ErrThrottled) != tt.throttled || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Start: %v; want throttled %v with %q", err, tt.throttled, tt.want)
			}
		})
	}
}

// execution is GetQueryExecution's body for a query in state, with status fields such as a
// StateChangeReason and an AthenaError, and statistics.
func execution(state, status, statistics string) answer {
	if status != "" {
		status = "," + status
	}
	return ok(`{"QueryExecution":{"QueryExecutionId":"` + queryID + `","StatementType":"DML","WorkGroup":"prod-analytics-backfill",` +
		`"Status":{"State":"` + state + `","SubmissionDateTime":1.761480000E9` + status + `},"Statistics":{` + statistics + `}}}`)
}

func failedWith(typ, reason string) answer {
	return execution("FAILED", `"StateChangeReason":"`+reason+`","AthenaError":{"ErrorCategory":2,"ErrorType":`+typ+`,"Retryable":false,`+
		`"ErrorMessage":"`+reason+`"}`, `"DataScannedInBytes":2048`)
}

// A query's state, and how it failed, by Athena's error type (ADR 0016 §6).
func TestArchiveStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		answer answer
		want   backfill.QueryStatus
	}{
		{"queued", execution("QUEUED", "", ""), backfill.QueryStatus{State: backfill.QueryRunning}},
		{"running", execution("RUNNING", "", `"DataScannedInBytes":1024`), backfill.QueryStatus{State: backfill.QueryRunning, Scanned: 1024}},
		{"succeeded", execution("SUCCEEDED", `"CompletionDateTime":1.76148003E9`, `"DataScannedInBytes":5368709120`),
			backfill.QueryStatus{State: backfill.QuerySucceeded, Scanned: 5368709120}},
		{"timed out", failedWith("206", "Query timeout"),
			backfill.QueryStatus{State: backfill.QueryFailed, Failure: backfill.FailureTimeout, Reason: "Query timeout", Scanned: 2048}},
		{"exhausted", failedWith("4", "Query exhausted resources at this scale factor"), backfill.QueryStatus{
			State: backfill.QueryFailed, Failure: backfill.FailureTimeout, Reason: "Query exhausted resources at this scale factor", Scanned: 2048}},
		{"throttled", failedWith("1403", "Please reduce your request rate."), backfill.QueryStatus{
			State: backfill.QueryFailed, Failure: backfill.FailureRetryable, Reason: "Please reduce your request rate.", Scanned: 2048}},
		{"denied", failedWith("1501", "Access denied when writing to location"), backfill.QueryStatus{
			State: backfill.QueryFailed, Failure: backfill.FailurePermission, Reason: "Access denied when writing to location", Scanned: 2048}},
		{"other", failedWith("100", "INTERNAL_ERROR_QUERY_ENGINE"), backfill.QueryStatus{
			State: backfill.QueryFailed, Failure: backfill.FailureOther, Reason: "INTERNAL_ERROR_QUERY_ENGINE", Scanned: 2048}},
		{"cancelled", execution("CANCELLED", `"StateChangeReason":"Query cancelled by user"`, ""),
			backfill.QueryStatus{State: backfill.QueryCancelled, Reason: "Query cancelled by user"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := archiveOf(t, regionClients{athena: newRecorded(map[string][]answer{"GetQueryExecution": {tt.answer}})}, time.Time{})
			if got, err := a.Status(t.Context(), queryID); err != nil || got != tt.want {
				t.Errorf("Status = %+v, %v; want %+v", got, err, tt.want)
			}
		})
	}
}

// The partitions are the query's rows past its header, page after page, each day a date or
// days since 1970.
func TestArchivePartitions(t *testing.T) {
	t.Parallel()
	meta := `"ResultSetMetadata":{"ColumnInfo":[{"Name":"day","Type":"varchar"},{"Name":"record_count","Type":"bigint"},` +
		`{"Name":"total_size","Type":"bigint"}]}`
	row := func(cells ...string) string {
		return `{"Data":[{"VarCharValue":"` + strings.Join(cells, `"},{"VarCharValue":"`) + `"}]}`
	}
	f := newRecorded(map[string][]answer{"GetQueryResults": {
		ok(`{"ResultSet":{` + meta + `,"Rows":[` + row("day", "record_count", "total_size") + `,` + row("2026-10-01", "1500", "73400320") + `]},` +
			`"NextToken":"page-2","UpdateCount":0}`),
		ok(`{"ResultSet":{` + meta + `,"Rows":[` + row("20363", "7", "4096") + `]},"UpdateCount":0}`),
	}})
	got, err := archiveOf(t, regionClients{athena: f}, time.Time{}).Partitions(t.Context(), queryID)
	want := []backfill.Partition{
		{From: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC), Records: 1500, Bytes: 73400320},
		{From: time.Date(2025, 10, 2, 0, 0, 0, 0, time.UTC), To: time.Date(2025, 10, 3, 0, 0, 0, 0, time.UTC), Records: 7, Bytes: 4096},
	}
	if err != nil || !slices.Equal(got, want) {
		t.Errorf("Partitions = %+v, %v\nwant %+v", got, err, want)
	}
	if len(f.bodies["GetQueryResults"]) != 2 || !strings.Contains(f.bodies["GetQueryResults"][1], `"NextToken":"page-2"`) {
		t.Errorf("requests %q, want the second page by its token", f.bodies["GetQueryResults"])
	}
}

// An export's files are those its data manifest lists. A manifest gone with the lifecycle
// rule is ErrStagingMissing, so the job stages again.
func TestArchiveFiles(t *testing.T) {
	t.Parallel()
	manifest := `"DataManifestLocation":"s3://` + bucket + `/athena/backfill/` + queryID + `-manifest.csv","DataScannedInBytes":10`
	unload := "backfill/page_daily@v2/1/unload/plan-1-ab/1-2/1/"
	tests := []struct {
		name     string
		manifest string // the object's content; "" for none
		want     []string
		err      string
	}{
		{"listed", "s3://" + bucket + "/" + unload + "a_00000.gz\ns3://" + bucket + "/" + unload + "a_00001.gz\n",
			[]string{unload + "a_00000.gz", unload + "a_00001.gz"}, ""},
		{"empty", "", nil, backfill.ErrStagingMissing.Error()},
		{"another bucket", "s3://other-bucket/" + unload + "a_00000.gz\n", nil, `"s3://other-bucket/` + unload + `a_00000.gz" isn't an object of s3://` + bucket},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := awsbackfilltest.NewS3()
			if tt.manifest != "" {
				fake.Put(bucket, "athena/backfill/"+queryID+"-manifest.csv", []byte(tt.manifest))
			}
			a := archiveOf(t, regionClients{s3: fake, athena: newRecorded(map[string][]answer{
				"GetQueryExecution": {execution("SUCCEEDED", "", manifest)},
			})}, time.Time{})
			got, err := a.Files(t.Context(), queryID)
			if tt.err == "" && (err != nil || !slices.Equal(got, tt.want)) || tt.err != "" && (err == nil || !strings.Contains(err.Error(), tt.err)) {
				t.Errorf("Files = %q, %v; want %q, %q", got, err, tt.want, tt.err)
			}
		})
	}
}

// The error objects are the stream's under its error prefix, after the last one staged, in
// key order, past a listing's page, and written since the stream table's creation.
func TestArchiveErrorObjects(t *testing.T) {
	t.Parallel()
	fake := awsbackfilltest.NewS3()
	born := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var keys []string
	for i := range 1200 {
		key := fmt.Sprintf("errors/firehose/page_views/2026/10/01/%04d", i)
		fake.PutAt(bucket, key, []byte("x"), born.Add(time.Duration(i)*time.Millisecond))
		keys = append(keys, key)
	}
	fake.PutAt(bucket, "errors/firehose/page_views/2026/09/30/older", []byte("x"), born.Add(-time.Millisecond))
	fake.PutAt(bucket, "errors/firehose/page_views2/2026/10/01/0000", []byte("x"), born)
	a := archiveOf(t, regionClients{s3: fake}, time.Time{})
	got, err := a.ErrorObjects(t.Context(), "page_views", keys[99], born)
	if err != nil || !slices.Equal(got, keys[100:]) {
		t.Errorf("ErrorObjects = %d keys from %q, %v; want %d from %q", len(got), first(got), err, len(keys)-100, keys[100])
	}
	if all, aerr := a.ErrorObjects(t.Context(), "page_views", "", born); aerr != nil || !slices.Equal(all, keys) {
		t.Errorf("ErrorObjects from the start = %d keys, %v; want the 1200 written since %v", len(all), aerr, born)
	}
}

func first(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

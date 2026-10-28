// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awssetup_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/setup"
)

const (
	workgroup = "prod-analytics"
	statement = "ALTER TABLE `prod_analytics`.`page_views` ADD COLUMNS (`amount` decimal(20,0))"
	queryID   = "0f5e9c1d-7a2b-4c3d-8e9f-a0b1c2d3e4f5"
	started   = `{"QueryExecutionId":"` + queryID + `"}`
)

// execution is GetQueryExecution's answer for a query in state, with status fields such
// as a StateChangeReason and an AthenaError.
func execution(state, status string) answer {
	if status != "" {
		status = "," + status
	}
	return ok(`{"QueryExecution":{"EngineVersion":{"EffectiveEngineVersion":"Athena engine version 3",` +
		`"SelectedEngineVersion":"Athena engine version 3"},"Query":` + quote(statement) + `,"QueryExecutionId":"` + queryID + `",` +
		`"StatementType":"DDL","Status":{"State":"` + state + `","SubmissionDateTime":1.761480000E9` + status + `},` +
		`"WorkGroup":"` + workgroup + `"}}`)
}

// failed is a FAILED query with reason and Athena's error type typ.
func failed(typ int, retryable bool, reason string) answer {
	return execution("FAILED", fmt.Sprintf(`"StateChangeReason":%s,"AthenaError":{"ErrorCategory":2,"ErrorType":%d,`+
		`"Retryable":%t,"ErrorMessage":%s}`, quote(reason), typ, retryable, quote(reason)))
}

func quote(s string) string {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// Exec submits the statement in the workgroup, then reads its state after 0.5 s, and at
// intervals doubling up to 5 s, until it succeeds.
func TestExecPolls(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		running := execution("RUNNING", "")
		f := newFakeAWS(t, map[string][]answer{
			"StartQueryExecution": {ok(started)},
			"GetQueryExecution": {
				execution("QUEUED", ""), running, running, running, running, running, execution("SUCCEEDED", `"CompletionDateTime":1.761480030E9`),
			},
		})
		start := time.Now()
		if err := handler(f).Athena.Exec(t.Context(), workgroup, statement); err != nil {
			t.Fatal(err)
		}
		calls := f.recorded()
		var got []string
		for _, c := range calls {
			got = append(got, fmt.Sprintf("%s %v", c.op, c.at.Sub(start)))
		}
		want := []string{
			"StartQueryExecution 0s", "GetQueryExecution 500ms", "GetQueryExecution 1.5s", "GetQueryExecution 3.5s",
			"GetQueryExecution 7.5s", "GetQueryExecution 12.5s", "GetQueryExecution 17.5s", "GetQueryExecution 22.5s",
		}
		if !slices.Equal(got, want) {
			t.Errorf("calls %q\nwant %q", got, want)
		}
		var in struct {
			QueryString        string `json:"QueryString"`
			WorkGroup          string `json:"WorkGroup"`
			ClientRequestToken string `json:"ClientRequestToken"`
		}
		if err := json.Unmarshal([]byte(calls[0].body), &in); err != nil || in.QueryString != statement ||
			in.WorkGroup != workgroup || in.ClientRequestToken == "" {
			t.Errorf("StartQueryExecution %s, want the statement, the workgroup and a request token", calls[0].body)
		}
		if calls[1].body != `{"QueryExecutionId":"`+queryID+`"}` {
			t.Errorf("GetQueryExecution %s, want the query's ID", calls[1].body)
		}
	})
}

// A query that fails is classified by its error type and reason, and submitted again only
// when Athena says it was throttled or may succeed again.
func TestExecFailures(t *testing.T) {
	t.Parallel()
	const commit = "ICEBERG_COMMIT_ERROR: Failed to commit Iceberg update to table prod_analytics.page_views"
	tests := []struct {
		name    string
		answer  answer
		err     error // the error wraps it; nil for any error that wraps neither of setup's
		submits int   // StartQueryExecution calls: 2 when the query is submitted again
	}{
		{"permission", failed(1500, false, "Insufficient permissions to execute the query."), setup.ErrAccessDenied, 1},
		{"S3 permission", failed(1501, false, "Access denied when writing to location: s3://bucket/warehouse/"), setup.ErrAccessDenied, 1},
		{"commit conflict", failed(233, false, commit), setup.ErrConflict, 1},
		{
			"created meanwhile", failed(1003, false, "FAILED: Execution Error, return code 1 from DDLTask. "+
				"AlreadyExistsException(message:Table page_views already exists)"), setup.ErrConflict, 1,
		},
		{"too many table versions", failed(1402, true, "Query failed due to too many table versions in AWS Glue."), nil, 1},
		{"syntax", failed(1006, false, "line 1:8: mismatched input 'TABLE'"), nil, 1},
		{"canceled", execution("CANCELLED", `"StateChangeReason":"Query cancelled by user"`), nil, 1},
		{"throttling", failed(1400, false, "Query failed due to throttling."), nil, 2},
		{"Glue throttling", failed(1401, false, "Rate exceeded (Service: AWSGlue; Status Code: 400)"), nil, 2},
		{"S3 throttling", failed(1403, false, "Please reduce your request rate. (Service: Amazon S3; Status Code: 503)"), nil, 2},
		{"Athena throttling", failed(1406, false, "Query failed due to Amazon Athena throttling."), nil, 2},
		{"past the throttling types", failed(1407, false, "Query failed."), nil, 1},
		{"retryable", failed(100, true, "Internal service error."), nil, 2},
		{"retryable timeout", failed(206, true, "Query timeout"), nil, 2},
		{"retryable exhaustion", failed(4, true, "Query exhausted resources at this scale factor"), nil, 2},
		{"timeout", failed(206, false, "Query timeout"), nil, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFakeAWS(t, map[string][]answer{
					"StartQueryExecution": {ok(started)},
					"GetQueryExecution":   {tt.answer, execution("SUCCEEDED", "")},
				})
				err := handler(f).Athena.Exec(t.Context(), workgroup, statement)
				switch {
				case tt.submits == 2 && err != nil:
					t.Errorf("got %v, want success when submitted again", err)
				case tt.submits == 1 && (err == nil || !strings.Contains(err.Error(), "query "+queryID+" ")):
					t.Errorf("got %v, want an error naming the query", err)
				case tt.err != nil && !errors.Is(err, tt.err):
					t.Errorf("got %v, want %v", err, tt.err)
				case tt.err == nil && (errors.Is(err, setup.ErrAccessDenied) || errors.Is(err, setup.ErrConflict)):
					t.Errorf("got %v, which the handler would retry", err)
				}
				if n := len(slices.DeleteFunc(f.ops(), func(op string) bool { return op != "StartQueryExecution" })); n != tt.submits {
					t.Errorf("submitted %d times, want %d: %q", n, tt.submits, f.ops())
				}
			})
		})
	}
}

// A query that ends with the caller's context is stopped, even though the context is gone.
func TestExecStopsOnDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFakeAWS(t, map[string][]answer{
			"StartQueryExecution": {ok(started)},
			"GetQueryExecution":   {execution("RUNNING", "")},
			"StopQueryExecution":  {ok(`{}`)},
		})
		start := time.Now()
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
		defer cancel()
		err := handler(f).Athena.Exec(ctx, workgroup, statement)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want the context's deadline", err)
		}
		calls := f.recorded()
		last := calls[len(calls)-1]
		if last.op != "StopQueryExecution" || last.body != `{"QueryExecutionId":"`+queryID+`"}` || last.at.Sub(start) != 4*time.Second {
			t.Errorf("last call %+v, want StopQueryExecution of the query at the deadline", last)
		}
	})
}

// The call that stops the query gets 10 s of its own after the caller's deadline.
func TestExecStopBounded(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFakeAWS(t, map[string][]answer{
			"StartQueryExecution": {ok(started)},
			"GetQueryExecution":   {execution("RUNNING", "")},
			"StopQueryExecution":  {hang},
		})
		start := time.Now()
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
		defer cancel()
		err := handler(f).Athena.Exec(ctx, workgroup, statement)
		if took := time.Since(start); !errors.Is(err, context.DeadlineExceeded) || took != 14*time.Second {
			t.Errorf("got %v after %v, want the deadline after 14 s", err, took)
		}
	})
}

// A query whose state can't be read is stopped, so that a retry doesn't run the statement
// twice at once.
func TestExecStopsOnPollError(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFakeAWS(t, map[string][]answer{
			"StartQueryExecution": {ok(started)},
			"GetQueryExecution": {awsError(http.StatusBadRequest, "AccessDeniedException",
				"User: arn:aws:sts::123456789012:assumed-role/prod-analytics-setup/prod-analytics-setup is not authorized to perform: "+
					"athena:GetQueryExecution on resource: arn:aws:athena:us-east-1:123456789012:workgroup/prod-analytics")},
			"StopQueryExecution": {ok(`{}`)},
		})
		err := handler(f).Athena.Exec(t.Context(), workgroup, statement)
		calls := f.recorded()
		if !errors.Is(err, setup.ErrAccessDenied) || !slices.Equal(f.ops(), []string{"StartQueryExecution", "GetQueryExecution", "StopQueryExecution"}) ||
			calls[2].body != `{"QueryExecutionId":"`+queryID+`"}` {
			t.Errorf("got %v after %+v, want access denied after the query was stopped", err, calls)
		}
	})
}

// A submission that Athena throttles is submitted again, past the SDK's three attempts.
func TestExecStartThrottled(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		throttled := awsError(http.StatusBadRequest, "TooManyRequestsException",
			"You have exceeded the limit for the number of queries you can run concurrently. "+
				"Reduce the number of concurrent queries submitted by this account.")
		f := newFakeAWS(t, map[string][]answer{
			"StartQueryExecution": {throttled, throttled, throttled, ok(started)},
			"GetQueryExecution":   {execution("SUCCEEDED", "")},
		})
		err := handler(f).Athena.Exec(t.Context(), workgroup, statement)
		want := []string{"StartQueryExecution", "StartQueryExecution", "StartQueryExecution", "StartQueryExecution", "GetQueryExecution"}
		if err != nil || !slices.Equal(f.ops(), want) {
			t.Errorf("got %v after %q, want success after %q", err, f.ops(), want)
		}
	})
}

// Throttling that outlasts the context fails with the context's error and the last
// failure.
func TestExecThrottledUntilDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFakeAWS(t, map[string][]answer{
			"StartQueryExecution": {ok(started)},
			"GetQueryExecution":   {failed(1404, false, "Query failed due to Amazon Athena throttling.")},
		})
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		err := handler(f).Athena.Exec(ctx, workgroup, statement)
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "Amazon Athena throttling") {
			t.Errorf("got %v, want the deadline and the throttling", err)
		}
		// Each round is 0.5 s of polling, then 1 s, 2 s, ... 30 s before the next submit.
		if n := len(slices.DeleteFunc(f.ops(), func(op string) bool { return op != "StartQueryExecution" })); n != 6 {
			t.Errorf("submitted %d times in a minute, want 6: %q", n, f.ops())
		}
	})
}

func TestExecRefused(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFakeAWS(t, map[string][]answer{"StartQueryExecution": {awsError(http.StatusBadRequest, "AccessDeniedException",
			"User: arn:aws:sts::123456789012:assumed-role/prod-analytics-setup/prod-analytics-setup is not authorized to perform: "+
				"athena:StartQueryExecution on resource: arn:aws:athena:us-east-1:123456789012:workgroup/prod-analytics")}})
		err := handler(f).Athena.Exec(t.Context(), workgroup, statement)
		if !errors.Is(err, setup.ErrAccessDenied) || !slices.Equal(f.ops(), []string{"StartQueryExecution"}) {
			t.Errorf("got %v after %q, want access denied after one call", err, f.ops())
		}
	})
}

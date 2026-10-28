// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-lambda-go/cfn"
	"github.com/aws/aws-lambda-go/lambdacontext"

	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/setup"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/runtime"
)

const (
	stackID   = "arn:aws:cloudformation:us-east-1:123456789012:stack/prod-analytics-regional/5b6c7d80-b2c4-11f0-9e1a-0a1b2c3d4e5f"
	requestID = "c2a1e8f0-4d3b-4a5c-9e7f-1b2c3d4e5f60"
	// archiveSchemaProps are ArchiveSchema's properties as CloudFormation sends them.
	archiveSchemaProps = `{
    "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:prod-analytics-setup",
    "ServiceTimeout": "960",
    "Database": "prod_analytics",
    "DatabaseLocation": "s3://prod-analytics-123456789012-us-east-1-an/warehouse/prod_analytics/",
    "WorkGroup": "prod-analytics",
    "OptimizerRoleArn": "arn:aws:iam::123456789012:role/prod-analytics-regional-RoleArchiveOptimizer-1A2B3C4D5E6F",
    "Optimizers": {"Compaction": "binpack", "SnapshotRetentionDays": "5", "SnapshotsToKeep": "1"},
    "Fingerprint": "sha256:3f1c",
    "Tables": [{
      "Name": "clicks",
      "Location": "s3://prod-analytics-123456789012-us-east-1-an/warehouse/prod_analytics/clicks/iceberg/",
      "Columns": [{"Name": "event_id", "Type": "string"}, {"Name": "event_time", "Type": "timestamp"}]
    }]
  }`
	lookupProps = `{
    "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:prod-analytics-setup",
    "ServiceTimeout": "300",
    "TableName": "` + streamTable + `",
    "LookupGeneration": "1"
  }`
)

// cfnEvent returns a custom resource request as CloudFormation sends it, replying to url.
// An Update or a Delete carries the resource's physical ID.
func cfnEvent(t *testing.T, requestType, resourceType, logicalID, physicalID, props, url string) cfn.Event {
	t.Helper()
	var physical string
	if physicalID != "" {
		physical = `"PhysicalResourceId": "` + physicalID + `", "OldResourceProperties": ` + props + `,`
	}
	data := `{
  "RequestType": "` + requestType + `",
  "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:prod-analytics-setup",
  "ResponseURL": "` + url + `",
  "StackId": "` + stackID + `",
  "RequestId": "` + requestID + `",
  "LogicalResourceId": "` + logicalID + `",
  "ResourceType": "` + resourceType + `",` + physical + `
  "ResourceProperties": ` + props + `
}`
	var ev cfn.Event
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		t.Fatalf("%v in %s", err, data)
	}
	return ev
}

// setupAWS answers the setup handler as AWS would for a stream table that exists and an
// archive without tables. With block, DescribeTable waits for its context; with err, it
// fails; with panics, it panics.
type setupAWS struct {
	block  bool
	err    error
	panics bool
}

func (f *setupAWS) DescribeTable(ctx context.Context, _ string) (setup.StreamTable, bool, error) {
	switch {
	case f.block:
		<-ctx.Done()
		return setup.StreamTable{}, false, ctx.Err()
	case f.panics:
		panic("boom")
	case f.err != nil:
		return setup.StreamTable{}, false, f.err
	}
	return setup.StreamTable{Status: "ACTIVE", StreamARN: streamARN, StreamView: "NEW_AND_OLD_IMAGES"}, true, nil
}

func (*setupAWS) CreateDatabase(context.Context, string, string) error { return nil }

func (*setupAWS) Table(context.Context, string, string) (setup.GlueTable, bool, error) {
	return setup.GlueTable{}, false, nil
}

func (*setupAWS) PutOptimizer(context.Context, string, string, setup.Optimizer) error { return nil }

func (*setupAWS) Exec(context.Context, string, string) error { return nil }

// reply is a reply as the response URL received it.
type reply struct {
	method, contentType, body string
	at                        time.Time
}

// replies is a response URL that keeps every reply.
type replies struct {
	srv *httptest.Server
	mu  sync.Mutex
	got []reply
}

func newReplies(t *testing.T) *replies {
	t.Helper()
	r := &replies{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		r.got = append(r.got, reply{req.Method, strings.Join(req.Header.Values("Content-Type"), ","), string(body), time.Now()})
	}))
	t.Cleanup(r.srv.Close)
	return r
}

// url is a presigned reply URL on the server, as CloudFormation gives it.
func (r *replies) url() string {
	return r.srv.URL + "/cloudformation-custom-resource-response-useast1/arn%3Aaws%3Acloudformation%3Aus-east-1%3A123456789012" +
		"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20261026T120000Z&X-Amz-Expires=7200&X-Amz-SignedHeaders=host" +
		"&X-Amz-Credential=AKIA6ODU5DHT7EXAMPLE%2F20261026%2Fus-east-1%2Fs3%2Faws4_request" +
		"&X-Amz-Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924aEXAMPLE"
}

func (r *replies) all() []reply {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]reply(nil), r.got...)
}

// Each request of the templates' custom resources gets one reply: a PUT without a
// Content-Type, of the fields CloudFormation reads, with no Data for a Delete.
func TestSetupReplies(t *testing.T) {
	t.Parallel()
	const (
		lookupID = "LookupClicks"
		schemaID = "ArchiveSchema"
		reply    = `{"Status":"SUCCESS","RequestId":"` + requestID + `","LogicalResourceId":"`
		stack    = `","StackId":"` + stackID + `","PhysicalResourceId":"`
	)
	tests := []struct {
		name                                    string
		requestType, resourceType, logical, old string
		props                                   string
		want                                    string
	}{
		{"lookup create", "Create", setup.TypeStreamARN, lookupID, "", lookupProps,
			reply + lookupID + stack + streamARN + `","Data":{"StreamArn":"` + streamARN + `"}}`},
		{"lookup update", "Update", setup.TypeStreamARN, lookupID, streamARN, lookupProps,
			reply + lookupID + stack + streamARN + `","Data":{"StreamArn":"` + streamARN + `"}}`},
		{"lookup delete", "Delete", setup.TypeStreamARN, lookupID, streamARN, lookupProps, reply + lookupID + stack + streamARN + `"}`},
		{"schema create", "Create", setup.TypeArchiveSchema, schemaID, "", archiveSchemaProps,
			reply + schemaID + stack + `prod_analytics","Data":{"Fingerprint":"sha256:3f1c"}}`},
		{"schema update", "Update", setup.TypeArchiveSchema, schemaID, "prod_analytics", archiveSchemaProps,
			reply + schemaID + stack + `prod_analytics","Data":{"Fingerprint":"sha256:3f1c"}}`},
		{"schema delete", "Delete", setup.TypeArchiveSchema, schemaID, "prod_analytics", archiveSchemaProps,
			reply + schemaID + stack + `prod_analytics"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			aws := &setupAWS{}
			fn := runtime.NewSetup(&setup.Handler{DynamoDB: aws, Glue: aws, Athena: aws}, io.Discard)
			r := newReplies(t)
			reason, err := fn(t.Context(), cfnEvent(t, tt.requestType, tt.resourceType, tt.logical, tt.old, tt.props, r.url()))
			if reason != "" || err != nil {
				t.Errorf("got %q, %v; want the reply sent", reason, err)
			}
			if got := r.all(); len(got) != 1 || got[0].method != http.MethodPut || got[0].contentType != "" || got[0].body != tt.want {
				t.Errorf("replies %+v\nwant one PUT without Content-Type of\n%s", got, tt.want)
			}
		})
	}
}

// An update of ArchiveMode compares the modes it sends with the old ones: a stream that
// starts archiving gets a hole.
func TestSetupArchiveModeReadsTheOldProperties(t *testing.T) {
	t.Parallel()
	const props = `{"ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:prod-analytics-setup", "ServiceTimeout": "300",
    "MetaTable": "prod-analytics-meta", "Streams": {"clicks": "all"}}`
	b := mem.New()
	m := meta.New(b, "prod-analytics-meta")
	if err := m.ApplyArchiveModes(t.Context(), nil, map[string]meta.ArchiveMode{"clicks": meta.ArchiveOff}, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	fn := runtime.NewSetup(&setup.Handler{Meta: b}, io.Discard)
	r := newReplies(t)
	ev := cfnEvent(t, "Update", setup.TypeArchiveMode, "ArchiveMode", "prod-analytics-meta/archive-mode", props, r.url())
	ev.OldResourceProperties["Streams"] = map[string]any{"clicks": "off"}
	if reason, err := fn(t.Context(), ev); reason != "" || err != nil {
		t.Fatalf("got %q, %v; want the reply sent", reason, err)
	}
	s, _, err := m.Stream(t.Context(), "clicks")
	if err != nil || s.Mode != meta.ArchiveAll || len(s.Holes) != 1 || s.Holes[0].From != meta.ArchiveOff {
		t.Errorf("stream item %+v, %v; want mode all after a hole from off", s, err)
	}
}

// A failed Create has no physical ID of its own, so the reply carries the request's: the
// rollback's Delete then names it.
func TestSetupFailedCreate(t *testing.T) {
	t.Parallel()
	aws := &setupAWS{err: errors.New("describe stream table: connection reset")}
	fn := runtime.NewSetup(&setup.Handler{DynamoDB: aws}, io.Discard)
	r := newReplies(t)
	if reason, err := fn(t.Context(), cfnEvent(t, "Create", setup.TypeStreamARN, "LookupClicks", "", lookupProps, r.url())); reason != "" || err != nil {
		t.Errorf("got %q, %v; want the reply sent", reason, err)
	}
	want := `{"Status":"FAILED","RequestId":"` + requestID + `","LogicalResourceId":"LookupClicks","StackId":"` + stackID +
		`","PhysicalResourceId":"` + requestID + `","Reason":"describe stream table ` + streamTable +
		`: describe stream table: connection reset"}`
	if got := r.all(); len(got) != 1 || got[0].body != want {
		t.Errorf("replies %+v\nwant\n%s", got, want)
	}
}

// CloudFormation refuses a reply over 4096 bytes, so a long error is cut, on a rune
// boundary and counting JSON's escapes, to the most that fits, with the physical ID the
// reply carries: the old one, or on a Create the request's. The log keeps all of it.
func TestSetupReplyFits(t *testing.T) {
	t.Parallel()
	const lambdaRequestID = "9d3b7a10-6c2e-4f8b-a1d4-5e6f708192a3"
	long := strings.Repeat("column <é> differs; ", 512) // 10 KB, of which < and > take 6 bytes in JSON
	full := "describe stream table " + streamTable + ": " + long
	tests := []struct {
		requestType, old string
		physicalID       string // in the reply
	}{
		{"Create", "", requestID},
		{"Update", streamARN, streamARN},
	}
	for _, tt := range tests {
		t.Run(tt.requestType, func(t *testing.T) {
			t.Parallel()
			aws := &setupAWS{err: errors.New(long)}
			var logs bytes.Buffer
			fn := runtime.NewSetup(&setup.Handler{DynamoDB: aws}, &logs)
			r := newReplies(t)
			ctx := lambdacontext.NewContext(t.Context(), &lambdacontext.LambdaContext{AwsRequestID: lambdaRequestID})
			if _, err := fn(ctx, cfnEvent(t, tt.requestType, setup.TypeStreamARN, "LookupClicks", tt.old, lookupProps, r.url())); err != nil {
				t.Fatal(err)
			}
			got := r.all()
			if len(got) != 1 {
				t.Fatalf("replies %+v, want one", got)
			}
			var sent cfn.Response
			if err := json.Unmarshal([]byte(got[0].body), &sent); err != nil {
				t.Fatal(err)
			}
			cut := strings.TrimSuffix(sent.Reason, " …")
			// One more rune of the reason, escaped, would take up to 6 more bytes.
			if n := len(got[0].body); n > 4096 || n <= 4096-6 || sent.Status != cfn.StatusFailed || sent.PhysicalResourceID != tt.physicalID ||
				cut == sent.Reason || !strings.HasPrefix(full, cut) || !utf8.ValidString(sent.Reason) {
				t.Errorf("reply of %d bytes: %s %s, reason %q; want at most 4096 bytes, FAILED, %s, "+
					"and the start of the error with an ellipsis", n, sent.Status, sent.PhysicalResourceID, sent.Reason, tt.physicalID)
			}
			lines := jsonLines(t, logs.String())
			if len(lines) != 1 || lines[0]["level"] != "ERROR" || lines[0]["error"] != full || lines[0]["requestId"] != lambdaRequestID ||
				lines[0]["cfnRequestId"] != requestID || lines[0]["logicalResourceId"] != "LookupClicks" {
				t.Errorf("log lines %v, want the whole error at ERROR, with Lambda's and CloudFormation's request IDs", lines)
			}
		})
	}
}

// A request still unfinished 30 s before Lambda's deadline replies FAILED, so the stack
// doesn't wait out ServiceTimeout.
func TestSetupRepliesBeforeDeadline(t *testing.T) {
	t.Parallel()
	aws := &setupAWS{block: true}
	fn := runtime.NewSetup(&setup.Handler{DynamoDB: aws}, io.Discard)
	r := newReplies(t)
	deadline := time.Now().Add(30*time.Second + 200*time.Millisecond)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	if _, err := fn(ctx, cfnEvent(t, "Create", setup.TypeStreamARN, "LookupClicks", "", lookupProps, r.url())); err != nil {
		t.Fatal(err)
	}
	got := r.all()
	var sent cfn.Response
	if len(got) != 1 || json.Unmarshal([]byte(got[0].body), &sent) != nil || sent.Status != cfn.StatusFailed ||
		!strings.HasSuffix(sent.Reason, "context deadline exceeded") || deadline.Sub(got[0].at) < 29*time.Second {
		t.Errorf("replies %+v, want FAILED with the deadline about 30 s before Lambda's", got)
	}
}

// When the reply can't be sent, the function fails, so that Lambda runs the request
// again. Its error leaves out the reply URL's query, which holds the signature.
func TestSetupRedactsReplyURL(t *testing.T) {
	t.Parallel()
	r := newReplies(t)
	url := r.url()
	r.srv.Close()
	aws := &setupAWS{}
	fn := runtime.NewSetup(&setup.Handler{DynamoDB: aws}, io.Discard)
	reason, err := fn(t.Context(), cfnEvent(t, "Create", setup.TypeStreamARN, "LookupClicks", "", lookupProps, url))
	path, _, _ := strings.Cut(url, "?")
	if err == nil || reason != err.Error() || !strings.Contains(reason, `"`+path+`"`) {
		t.Fatalf("got %q, %v; want an error naming %s", reason, err, path)
	}
	for _, secret := range []string{"X-Amz-", "AKIA6ODU5DHT7EXAMPLE", "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924aEXAMPLE"} {
		if strings.Contains(reason, secret) {
			t.Errorf("reason %q holds %s", reason, secret)
		}
	}
}

// A panic replies FAILED with the request's ID as the physical ID of a Create, then goes
// on, so aws-lambda-go reports it and the process exits.
func TestSetupPanic(t *testing.T) {
	t.Parallel()
	aws := &setupAWS{panics: true}
	fn := runtime.NewSetup(&setup.Handler{DynamoDB: aws}, io.Discard)
	r := newReplies(t)
	func() {
		defer func() {
			if v := recover(); v != "boom" {
				t.Errorf("recovered %v, want the handler's panic", v)
			}
		}()
		_, err := fn(t.Context(), cfnEvent(t, "Create", setup.TypeStreamARN, "LookupClicks", "", lookupProps, r.url()))
		t.Errorf("returned %v, want a panic", err)
	}()
	want := `{"Status":"FAILED","RequestId":"` + requestID + `","LogicalResourceId":"LookupClicks","StackId":"` + stackID +
		`","PhysicalResourceId":"` + requestID + `","Reason":"Function panicked, see log stream for details"}`
	if got := r.all(); len(got) != 1 || got[0].contentType != "" || got[0].body != want {
		t.Errorf("replies %+v\nwant\n%s", got, want)
	}
}

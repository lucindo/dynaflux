// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/internal/catalog"
)

// Scenarios 34 and 35 run as the example's application. On AWS its credentials are
// E2E_APP_ROLE's, limited by a session policy to the policy in the example's README.

// appRoleVar names the role that scenarios 34 and 35 assume on AWS.
const appRoleVar = "E2E_APP_ROLE"

// application is the example's application in one region.
type application struct {
	client *flux.Client
	ddb    *dynamodb.Client
	tx     *txLog
}

// application returns the application in a region, with appCredentials on AWS.
func (h *harness) application(t *testing.T, reg *region) *application {
	t.Helper()
	cfg := reg.cfg.Copy()
	if h.aws != nil {
		cfg.Credentials = h.aws.appCredentials(t, reg.cfg)
	}
	tx := &txLog{next: cfg.HTTPClient, reasons: map[string]int{}}
	cfg.HTTPClient = tx
	s, err := dynaflux.Open(dynaflux.Config{AWS: cfg, Prefix: reg.naming.Prefix})
	if err != nil {
		t.Fatal(err)
	}
	return &application{client: flux.New(s), ddb: dynamodb.NewFromConfig(cfg), tx: tx}
}

// appCredentials returns the credentials of E2E_APP_ROLE with the README's policy, for this
// run, as the session policy: the session can do only what that policy allows.
func (a *awsTarget) appCredentials(t *testing.T, cfg aws.Config) aws.CredentialsProvider {
	t.Helper()
	role := os.Getenv(appRoleVar)
	if !strings.HasPrefix(role, "arn:aws:iam::"+a.account+":role/") {
		t.Fatalf("%s is %q; set it to the ARN of a role in E2E_ACCOUNT that you can assume (docs/runbooks/e2e.md §1)", appRoleVar, role)
	}
	policy := readmePolicy(t, a.account, "e2e-"+a.run+"-")
	c := sts.NewFromConfig(cfg)
	return aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		out, err := c.AssumeRole(ctx, &sts.AssumeRoleInput{RoleArn: &role, RoleSessionName: new("dynaflux-e2e-" + a.run), Policy: &policy})
		if err != nil {
			return aws.Credentials{}, err
		}
		cr := out.Credentials
		return aws.Credentials{
			AccessKeyID: aws.ToString(cr.AccessKeyId), SecretAccessKey: aws.ToString(cr.SecretAccessKey), SessionToken: aws.ToString(cr.SessionToken),
			CanExpire: true, Expires: aws.ToTime(cr.Expiration), Source: appRoleVar,
		}, nil
	}))
}

// readmePolicy returns the application policy in the example's README, its one JSON block,
// with account and prefix in place of <account> and demo-.
func readmePolicy(t *testing.T, account, prefix string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(exampleDir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, rest, found := strings.Cut(string(data), "```json\n")
	policy, _, closed := strings.Cut(rest, "```")
	if !found || !closed {
		t.Fatal("the example's README has no JSON block")
	}
	policy = strings.ReplaceAll(strings.ReplaceAll(policy, "<account>", account), "demo-", prefix)
	var b bytes.Buffer
	if err = json.Compact(&b, []byte(policy)); err != nil {
		t.Fatalf("the README's policy: %v", err)
	}
	return b.String()
}

// txLog passes requests on, counting the TransactWriteItems calls that DynamoDB answers,
// and the reasons of those it cancels by code and by whether the reason holds the item.
type txLog struct {
	next aws.HTTPClient

	mu      sync.Mutex
	calls   int
	reasons map[string]int
}

func (l *txLog) Do(req *http.Request) (*http.Response, error) {
	res, err := l.next.Do(req)
	if err != nil || req.Header.Get("X-Amz-Target") != "DynamoDB_20120810.TransactWriteItems" {
		return res, err
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	res.Body = io.NopCloser(bytes.NewReader(body))
	var out struct {
		Reasons []struct {
			Code string         `json:"Code"`
			Item map[string]any `json:"Item"`
		} `json:"CancellationReasons"`
	}
	if err = json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("TransactWriteItems response %q: %w", body, err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	for _, r := range out.Reasons {
		code := r.Code
		if r.Item != nil {
			code += " with Item"
		}
		l.reasons[code]++
	}
	return res, nil
}

func (l *txLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var parts []string
	for _, code := range slices.Sorted(maps.Keys(l.reasons)) {
		parts = append(parts, fmt.Sprintf("%s: %d", code, l.reasons[code]))
	}
	return fmt.Sprintf("%d TransactWriteItems calls; cancellation reasons {%s}", l.calls, strings.Join(parts, ", "))
}

// Scenario 34's load: workers in each region charge one account at once, the hot row.
const (
	syncWorkers = 4
	syncCharges = 25 // per worker
)

// Scenario 34: SYNC appends to one quota_usage row, from both regions at once, as the
// application. Each region reads its own charges at once, a retried charge counts once,
// the policy refuses writes to the view outside a transaction, and the views converge to
// the oracle. It logs the append latencies, the transactions sent and why DynamoDB
// cancelled them.
//
//nolint:thelper // a scenario: its failures point into it
func testSync(t *testing.T, h *harness) {
	r := newRun(t, h, "s34")
	account := r.key("acme")
	r.accounts[account] = true
	since := time.Now()
	loads := map[*region]*syncLoad{}
	for _, reg := range h.regions {
		loads[reg] = newSyncLoad(t, h, reg, account, r.at)
	}
	var wg sync.WaitGroup
	for _, l := range loads {
		wg.Go(func() { l.run(t.Context()) })
	}
	wg.Wait()
	for _, reg := range h.regions {
		loads[reg].check(t, r, reg)
	}
	r.converged(h.regions...)
	if h.aws == nil {
		return
	}
	for _, reg := range h.regions {
		group := "/aws/lambda/" + reg.naming.ProjectorFunction()
		lines, err := h.aws.logLines(t.Context(), reg, group, `{ $.msg = "rows failed" }`, since)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %d rows failed lines in %s since the scenario began", reg.name, len(lines), group)
	}
}

// syncLoad is scenario 34's charges in one region, a list per worker, and what became of
// them.
type syncLoad struct {
	app     *application
	account string
	at      time.Time
	work    [][]flux.APICall

	mu      sync.Mutex
	took    []time.Duration
	retried int
	errs    []error
}

func newSyncLoad(t *testing.T, h *harness, reg *region, account string, at time.Time) *syncLoad {
	t.Helper()
	l := &syncLoad{app: h.application(t, reg), account: account, at: at, work: make([][]flux.APICall, syncWorkers)}
	for w := range l.work {
		for i := range syncCharges {
			l.work[w] = append(l.work[w], flux.APICall{
				AccountID: account, Endpoint: "/search", Units: uint32(1 + (w+i)%5), RequestID: newUUID(t), EventTime: at,
			})
		}
	}
	return l
}

// run makes the charges, each worker's one after another, all workers at once.
func (l *syncLoad) run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, calls := range l.work {
		wg.Go(func() {
			for _, call := range calls {
				start := time.Now()
				n, err := charge(ctx, l.app.client, call)
				l.mu.Lock()
				l.took = append(l.took, time.Since(start))
				l.retried += n - 1
				if err != nil {
					l.errs = append(l.errs, fmt.Errorf("charge %s: %w", call.RequestID, err))
				}
				l.mu.Unlock()
			}
		})
	}
	wg.Wait()
}

// check checks the region after its charges, gives them to the oracles, and logs what they
// took.
func (l *syncLoad) check(t *testing.T, r *run, reg *region) {
	t.Helper()
	if err := errors.Join(l.errs...); err != nil {
		t.Fatalf("%s: %v", reg.name, err)
	}
	var own flux.QuotaUsedRow
	for _, calls := range l.work {
		for _, call := range calls {
			own.Calls++
			own.UsedUnits += uint64(call.Units)
		}
		r.feed(reg, func(ctx context.Context, c *flux.Client) error {
			_, err := c.AppendAPICallBatch(ctx, calls)
			return err
		})
	}
	// Before any projector runs here, the row holds the region's own charges, and on AWS
	// maybe some of the other region's.
	usage, err := l.app.client.QuotaUsed(t.Context(), flux.QuotaUsedParams{AccountID: l.account, At: l.at})
	if err != nil {
		t.Fatalf("%s: QuotaUsed: %v", reg.name, err)
	}
	if usage.Calls < own.Calls || usage.UsedUnits < own.UsedUnits || !r.h.replicated && usage != own {
		t.Errorf("%s: QuotaUsed right after the charges = %+v, want the region's own %+v", reg.name, usage, own)
	}
	retry := l.work[0][0]
	if _, err = l.app.client.AppendAPICall(t.Context(), retry); !errors.Is(err, dynaflux.ErrDuplicate) {
		t.Errorf("%s: charge %s again: %v, want ErrDuplicate", reg.name, retry.RequestID, err)
	}
	if r.h.aws != nil {
		checkRefusals(t, reg, l.app)
	}
	t.Logf("%s: %d charges, %s; %d retried by the application; %s", reg.name, len(l.took), latencies(l.took), l.retried, l.app.tx)
}

// charge appends call as an application does, sending the same call again after an error,
// at most 3 times in all: DEDUP BY stores it once. It returns how many appends it sent.
func charge(ctx context.Context, c *flux.Client, call flux.APICall) (int, error) {
	for n := 1; ; n++ {
		_, err := c.AppendAPICall(ctx, call)
		switch {
		case err == nil, n > 1 && errors.Is(err, dynaflux.ErrDuplicate):
			return n, nil
		case n == 3:
			return n, err
		}
	}
}

// checkRefusals checks the README policy's conditions: the application can't write a
// quota_usage row outside a transaction, nor query a meta partition other than the view
// states.
func checkRefusals(t *testing.T, reg *region, app *application) {
	t.Helper()
	// The condition never holds, so a write that IAM let through would change nothing.
	_, err := app.ddb.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: new(reg.naming.ViewTable("quota_usage", 1)),
		Item: map[string]types.AttributeValue{
			catalog.AttrPK: &types.AttributeValueMemberS{Value: "e2e-refused"},
			catalog.AttrSK: &types.AttributeValueMemberS{Value: "!"},
		},
		ConditionExpression: new("attribute_exists(pk)"),
	})
	checkAccessDenied(t, reg.name+": PutItem on quota_usage outside a transaction", err)
	_, err = app.ddb.Query(t.Context(), &dynamodb.QueryInput{
		TableName:                 new(reg.naming.MetaTable()),
		KeyConditionExpression:    new("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "catalog"}},
	})
	checkAccessDenied(t, reg.name+": Query of the meta table's catalog partition", err)
}

func checkAccessDenied(t *testing.T, what string, err error) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "AccessDeniedException" {
		t.Errorf("%s: %v, want AccessDeniedException", what, err)
	}
}

// Scenario 35's load, and how long each query may wait.
const (
	waitViews   = 20 // per region
	waitTimeout = 10 * time.Second
)

// Scenario 35: in each region, the application records page views of a page of its own with
// RecordPageView, which appends each with Waitable and waits for it: each returns the
// page's stats with every view so far. It logs how long each took, against the batching
// window.
//
//nolint:thelper // a scenario: its failures point into it
func testWait(t *testing.T, h *harness) {
	r := newRun(t, h, "s35")
	for _, reg := range h.regions {
		app := h.application(t, reg)
		page := r.key(reg.name + "/p")
		r.pages[page] = true
		var took []time.Duration
		for i := range waitViews {
			view := flux.PageView{UserID: r.key(fmt.Sprint("u", i)), Page: page, DurationMS: 10, EventTime: r.at}
			stop := settling(t, reg)
			start := time.Now()
			stats, err := analytics.RecordPageView(t.Context(), app.client, view, waitTimeout)
			took = append(took, time.Since(start))
			stop()
			if err != nil {
				t.Fatalf("%s: RecordPageView %d: %v", reg.name, i, err)
			}
			if len(stats) != 1 || stats[0].Views != uint64(i+1) {
				t.Errorf("%s: RecordPageView %d = %+v, want one day with %d views", reg.name, i, stats, i+1)
			}
			r.feed(reg, func(ctx context.Context, c *flux.Client) error {
				_, aerr := c.AppendPageView(ctx, view)
				return aerr
			})
		}
		t.Logf("%s: %d waits, %s; batching window %d s", reg.name, len(took), latencies(took), h.p.cfg.Runtime.BatchingWindowSeconds)
	}
	r.converged(h.regions...)
}

// settling settles reg again and again until the function it returns is called, so that on
// the emulator the projector applies what a query waits for. A settle runs to its end: one
// that stopped halfway would lose the records it had read.
func settling(t *testing.T, reg *region) func() {
	t.Helper()
	quit, done := make(chan struct{}), make(chan error, 1)
	go func() {
		for {
			if err := reg.settle(t.Context()); err != nil {
				done <- err
				return
			}
			select {
			case <-quit:
				done <- nil
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	return func() {
		t.Helper()
		close(quit)
		if err := <-done; err != nil {
			t.Errorf("%s: settle: %v", reg.name, err)
		}
	}
}

// latencies returns the median, 90th percentile and maximum of ds.
func latencies(ds []time.Duration) string {
	s := slices.Sorted(slices.Values(ds))
	at := func(p int) time.Duration { return s[(len(s)-1)*p/100] }
	return fmt.Sprintf("p50 %v, p90 %v, max %v", at(50), at(90), s[len(s)-1])
}

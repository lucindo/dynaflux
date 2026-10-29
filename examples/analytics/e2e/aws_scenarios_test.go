// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/fis"
	fistypes "github.com/aws/aws-sdk-go-v2/service/fis/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation"
	lftypes "github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
)

// mapping is what scenario 6 compares of an event source mapping.
type mapping struct {
	Function, State, StartingPosition, OnFailure string
	BatchSize, BatchingWindow, Parallelization   int32
	MaxRetries, MaxRecordAge, TumblingWindow     int32
	Bisect, Filtered                             bool
	ResponseTypes, Metrics                       string
}

// Scenario 6: each stream table has exactly one mapping in each region, to the projector,
// with the fixed values of docs/spec/infra.md §8.
//
//nolint:thelper // a scenario: its failures point into it
func testMappings(t *testing.T, h *harness) {
	for _, reg := range h.regions {
		c := h.aws.clients[reg].lambda
		fn := reg.naming.ProjectorFunction()
		if all := mappings(t, c, &lambda.ListEventSourceMappingsInput{FunctionName: &fn}); len(all) != len(h.p.def.Streams) {
			t.Errorf("%s: %s has %d mappings, want one per stream, %d", reg.name, fn, len(all), len(h.p.def.Streams))
		}
		want := mapping{
			Function: fn, State: "Enabled", StartingPosition: "TRIM_HORIZON",
			OnFailure: "arn:aws:s3:::" + reg.naming.Bucket(h.aws.account, reg.name),
			BatchSize: 100, BatchingWindow: int32(h.p.cfg.Runtime.BatchingWindowSeconds), Parallelization: 1,
			MaxRetries: -1, MaxRecordAge: -1, TumblingWindow: 900, Bisect: true,
			ResponseTypes: "ReportBatchItemFailures", Metrics: "EventCount",
		}
		for _, st := range h.p.def.Streams {
			ms := mappings(t, c, &lambda.ListEventSourceMappingsInput{EventSourceArn: new(streamARN(t, reg, st.Name))})
			if len(ms) != 1 {
				t.Errorf("%s: %d mappings on the stream of %s, want 1", reg.name, len(ms), st.Name)
				continue
			}
			if got := mappingOf(ms[0]); got != want {
				t.Errorf("%s: the mapping of %s is\n%+v\nwant\n%+v", reg.name, st.Name, got, want)
			}
		}
	}
}

func mappings(t *testing.T, c *lambda.Client, in *lambda.ListEventSourceMappingsInput) []lambdatypes.EventSourceMappingConfiguration {
	t.Helper()
	var all []lambdatypes.EventSourceMappingConfiguration
	pages := lambda.NewListEventSourceMappingsPaginator(c, in)
	for pages.HasMorePages() {
		page, err := pages.NextPage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.EventSourceMappings...)
	}
	return all
}

func mappingOf(m lambdatypes.EventSourceMappingConfiguration) mapping {
	got := mapping{
		Function:         aws.ToString(m.FunctionArn)[strings.LastIndex(aws.ToString(m.FunctionArn), ":")+1:],
		State:            aws.ToString(m.State),
		StartingPosition: string(m.StartingPosition),
		BatchSize:        aws.ToInt32(m.BatchSize), BatchingWindow: aws.ToInt32(m.MaximumBatchingWindowInSeconds),
		Parallelization: aws.ToInt32(m.ParallelizationFactor), MaxRetries: aws.ToInt32(m.MaximumRetryAttempts),
		MaxRecordAge: aws.ToInt32(m.MaximumRecordAgeInSeconds), TumblingWindow: aws.ToInt32(m.TumblingWindowInSeconds),
		Bisect: aws.ToBool(m.BisectBatchOnFunctionError), Filtered: m.FilterCriteria != nil,
	}
	if m.DestinationConfig != nil && m.DestinationConfig.OnFailure != nil {
		got.OnFailure = aws.ToString(m.DestinationConfig.OnFailure.Destination)
	}
	for _, rt := range m.FunctionResponseTypes {
		got.ResponseTypes += string(rt)
	}
	if m.MetricsConfig != nil {
		for _, mt := range m.MetricsConfig.Metrics {
			got.Metrics += string(mt)
		}
	}
	return got
}

// Scenario 18: Lake Formation leaves the archive to IAM: its defaults give
// IAM_ALLOWED_PRINCIPALS every permission on new databases and tables, IAM lets the Firehose
// role read each archive table, and no table is registered with Lake Formation.
//
//nolint:thelper // a scenario: its failures point into it
func testLakeFormation(t *testing.T, h *harness) {
	for _, reg := range h.regions {
		checkLakeFormationDefaults(t, h, reg)
		for _, st := range h.p.def.Streams {
			checkFirehoseReads(t, h, reg, st.Name)
		}
	}
}

func checkLakeFormationDefaults(t *testing.T, h *harness, reg *region) {
	t.Helper()
	out, err := h.aws.clients[reg].lakeformation.GetDataLakeSettings(t.Context(), &lakeformation.GetDataLakeSettingsInput{})
	if err != nil {
		t.Fatal(err)
	}
	iamAll := func(p lftypes.PrincipalPermissions) bool {
		return p.Principal != nil && aws.ToString(p.Principal.DataLakePrincipalIdentifier) == "IAM_ALLOWED_PRINCIPALS" &&
			slices.Contains(p.Permissions, lftypes.PermissionAll)
	}
	settings := out.DataLakeSettings
	for what, perms := range map[string][]lftypes.PrincipalPermissions{
		"databases": settings.CreateDatabaseDefaultPermissions, "tables": settings.CreateTableDefaultPermissions,
	} {
		if !slices.ContainsFunc(perms, iamAll) {
			t.Errorf("%s: Lake Formation's default permissions on new %s are %+v; want ALL for IAM_ALLOWED_PRINCIPALS", reg.name, what, perms)
		}
	}
}

func checkFirehoseReads(t *testing.T, h *harness, reg *region, stream string) {
	t.Helper()
	c, db := h.aws.clients[reg], reg.naming.ArchiveDatabase()
	ds, err := c.firehose.DescribeDeliveryStream(t.Context(), &firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: new(reg.naming.ArchiveStream(stream)),
	})
	if err != nil {
		t.Fatal(err)
	}
	role := aws.ToString(ds.DeliveryStreamDescription.Destinations[0].IcebergDestinationDescription.RoleARN)
	arn := "arn:aws:glue:" + reg.name + ":" + h.aws.account + ":"
	for _, resource := range []string{arn + "catalog", arn + "database/" + db, arn + "table/" + db + "/" + stream} {
		out, serr := c.iam.SimulatePrincipalPolicy(t.Context(), &iam.SimulatePrincipalPolicyInput{
			PolicySourceArn: &role, ActionNames: []string{"glue:GetTable"}, ResourceArns: []string{resource},
		})
		if serr != nil {
			t.Fatal(serr)
		}
		if d := out.EvaluationResults[0].EvalDecision; d != iamtypes.PolicyEvaluationDecisionTypeAllowed {
			t.Errorf("%s: glue:GetTable on %s by the Firehose role: %s, want allowed", reg.name, resource, d)
		}
	}
	tbl, err := c.glue.GetTable(t.Context(), &glue.GetTableInput{DatabaseName: &db, Name: &stream})
	if err != nil {
		t.Fatal(err)
	}
	if tbl.Table.IsRegisteredWithLakeFormation {
		t.Errorf("%s: table %s.%s is registered with Lake Formation; the archive uses IAM only", reg.name, db, stream)
	}
}

// accessDenied matches the access denials of the services the store calls. A term matches
// a whole word, so AccessDeniedException needs its own.
const accessDenied = `?AccessDenied ?AccessDeniedException ?"not authorized" ?"Access Denied" ?"Access was denied"`

// Scenario 7: no function or Firehose log holds an access denial, other than scenario 23's
// own when an earlier test run on the store ran it, and the optimizers that the setup
// function created (iam:PassRole included) exist.
//
//nolint:thelper // a scenario: its failures point into it
func testLeastPrivilege(t *testing.T, h *harness) {
	for _, reg := range h.regions {
		projector := "/aws/lambda/" + reg.naming.ProjectorFunction()
		for _, g := range []string{projector, "/aws/lambda/" + reg.naming.SetupFunction(), reg.naming.ArchiveLogGroup()} {
			msgs, err := h.aws.logLines(t.Context(), reg, g, accessDenied, time.UnixMilli(0))
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range msgs {
				if g != projector || !denied(h, reg, m) {
					t.Errorf("%s: %s: %s", reg.name, g, m)
				}
			}
		}
		checkOptimizers(t, h, reg)
	}
}

// denied reports whether a projector log message is a denial by scenario 23's policy: in the
// home region, from the role's own policy, on the table that scenario 23 names. A deny from
// an SCP, a resource policy or a permissions boundary is a finding.
func denied(h *harness, reg *region, msg string) bool {
	if reg != h.regions[0] || !strings.Contains(msg, "with an explicit deny in an identity-based policy") {
		return false
	}
	for _, table := range []string{reg.naming.ViewTable("page_daily", 1), reg.naming.MetaTable()} {
		if strings.Contains(msg, ":table/"+table+" ") {
			return true
		}
	}
	return false
}

func checkOptimizers(t *testing.T, h *harness, reg *region) {
	t.Helper()
	db := reg.naming.ArchiveDatabase()
	for _, st := range h.p.def.Streams {
		for _, typ := range []gluetypes.TableOptimizerType{gluetypes.TableOptimizerTypeCompaction, gluetypes.TableOptimizerTypeRetention} {
			_, err := h.aws.clients[reg].glue.GetTableOptimizer(t.Context(), &glue.GetTableOptimizerInput{
				CatalogId: &h.aws.account, DatabaseName: &db, TableName: new(st.Name), Type: typ,
			})
			if err != nil {
				t.Errorf("%s: the %s optimizer of %s: %v", reg.name, typ, st.Name, err)
			}
		}
	}
}

// Scenario 2: one dedup key appended in both regions at once. With the same payload, the
// views count it once; with different payloads, each region counts one of them once, and the
// projector reports a divergent duplicate.
//
//nolint:thelper // a scenario: its failures point into it
func testDedup(t *testing.T, h *harness) {
	r := newRun(t, h, "s2")
	since := time.Now()
	const pairs = 10
	same := appendSame(t, r, pairs)
	raced := 0
	diverged := make([]string, pairs)
	for i := range pairs {
		a := flux.APICall{AccountID: r.key("diverge-" + strconv.Itoa(i)), Endpoint: "/search", Units: 3, RequestID: newUUID(t), EventTime: r.at}
		b := a
		b.Units = 4
		if appendBoth(t, h, a, b) {
			raced++
		}
		diverged[i] = a.AccountID
	}
	r.converged(h.regions...)
	for _, reg := range h.regions {
		h.await(t, h.patience, reg.name+": divergent keys counted once", func(ctx context.Context) error {
			return countedOnce(ctx, reg, r.at, diverged)
		})
	}
	if raced == 0 {
		t.Fatalf("none of %d divergent pairs was stored in both regions before replication: no divergent duplicate to see", pairs)
	}
	h.await(t, h.patience, "DivergentDuplicates in the projector logs", func(ctx context.Context) error {
		for _, reg := range h.regions {
			msgs, err := h.aws.logLines(ctx, reg, "/aws/lambda/"+reg.naming.ProjectorFunction(), `{ $.DivergentDuplicates > 0 }`, since)
			if err != nil || len(msgs) > 0 {
				return err
			}
		}
		return fmt.Errorf("no line with DivergentDuplicates in either region after %d pairs stored in both", raced)
	})
	if len(same) == 0 {
		t.Fatalf("none of %d same-payload pairs was stored in both regions before replication: no replicated MODIFY to count", pairs)
	}
	for _, reg := range h.regions {
		t.Logf("%s: %d MODIFY records with equal payloads for the %d same-payload keys that both regions stored (record it in the results)",
			reg.name, payloadModifies(t, reg, same), len(same))
	}
}

// appendSame appends pairs of events, each in both regions at once with the same payload,
// and returns the keys of those that both regions stored.
func appendSame(t *testing.T, r *run, pairs int) []string {
	t.Helper()
	var stored []string
	for i := range pairs {
		ev := flux.APICall{AccountID: r.key("same-" + strconv.Itoa(i)), Endpoint: "/search", Units: 5, RequestID: newUUID(t), EventTime: r.at}
		if appendBoth(t, r.h, ev, ev) {
			stored = append(stored, ev.AccountID)
		}
		r.accounts[ev.AccountID] = true
		for _, o := range r.oracle {
			if _, err := flux.New(o).AppendAPICall(t.Context(), ev); err != nil {
				t.Fatal(err)
			}
		}
	}
	return stored
}

// countedOnce checks that each account's quota row counts one call, of 3 or 4 units.
func countedOnce(ctx context.Context, reg *region, at time.Time, accounts []string) error {
	for _, account := range accounts {
		row, err := reg.client.QuotaUsed(ctx, flux.QuotaUsedParams{AccountID: account, At: at})
		if err != nil {
			return fmt.Errorf("%s: %w", account, err)
		}
		if row.Calls != 1 || (row.UsedUnits != 3 && row.UsedUnits != 4) {
			return fmt.Errorf("%s: %+v, want 1 call of 3 or 4 units", account, row)
		}
	}
	return nil
}

// appendBoth appends a in the first region and b in the second at once, and reports whether
// both stored their event: neither saw the other's replicated first.
func appendBoth(t *testing.T, h *harness, a, b flux.APICall) bool {
	t.Helper()
	var errs [2]error
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, ev := range []flux.APICall{a, b} {
		wg.Go(func() {
			<-start
			_, errs[i] = h.regions[i].client.AppendAPICall(t.Context(), ev)
		})
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil && !errors.Is(err, dynaflux.ErrDuplicate) {
			t.Fatal(err)
		}
	}
	return errs[0] == nil && errs[1] == nil
}

// payloadModifies counts the MODIFY records of the accounts' api_calls items whose old and
// new images hold the same payload: whether replication writes them is S2's open question.
// Each region's copy of an event has its own _r, _it and _ttl (docs/spec/encoding.md §6),
// so these are left out. It fails unless the read saw the INSERT of every account.
func payloadModifies(t *testing.T, reg *region, accounts []string) int {
	t.Helper()
	arn := streamARN(t, reg, "api_calls")
	inserted, n := map[string]bool{}, 0
	for _, shard := range streamShards(t, reg, "api_calls") {
		for _, rec := range shardRecords(t, reg, arn, shard) {
			account, ok := rec.Dynamodb.NewImage["account_id"].(*streamtypes.AttributeValueMemberS)
			if !ok || !slices.Contains(accounts, account.Value) {
				continue
			}
			switch rec.EventName {
			case streamtypes.OperationTypeInsert:
				inserted[account.Value] = true
			case streamtypes.OperationTypeModify:
				if reflect.DeepEqual(payload(rec.Dynamodb.NewImage), payload(rec.Dynamodb.OldImage)) {
					n++
				}
			case streamtypes.OperationTypeRemove:
			}
		}
	}
	if len(inserted) != len(accounts) {
		t.Fatalf("%s: the stream read ended before the scenario's records: it saw the INSERT of %d of %d accounts", reg.name, len(inserted), len(accounts))
	}
	return n
}

func payload(item map[string]streamtypes.AttributeValue) map[string]streamtypes.AttributeValue {
	item = maps.Clone(item)
	maps.DeleteFunc(item, func(name string, _ streamtypes.AttributeValue) bool { return strings.HasPrefix(name, "_") })
	return item
}

// emptyPages is how many empty GetRecords pages in a row end the read of an open shard:
// pages can be empty before the records of a shard (DynamoDB Streams API reference).
const emptyPages = 20

// shardRecords reads a shard from TRIM_HORIZON to its end, or, while it is open, to
// emptyPages empty pages in a row.
func shardRecords(t *testing.T, reg *region, arn, shard string) []streamtypes.Record {
	t.Helper()
	it, err := reg.streams.GetShardIterator(t.Context(), &dynamodbstreams.GetShardIteratorInput{
		StreamArn: &arn, ShardId: &shard, ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatal(err)
	}
	var recs []streamtypes.Record
	for next, empty := it.ShardIterator, 0; next != nil && empty < emptyPages; {
		page, gerr := reg.streams.GetRecords(t.Context(), &dynamodbstreams.GetRecordsInput{ShardIterator: next})
		if gerr != nil {
			t.Fatal(gerr)
		}
		if empty++; len(page.Records) > 0 {
			empty = 0
		}
		recs = append(recs, page.Records...)
		next = page.NextShardIterator
	}
	return recs
}

// pauseDuration is the duration of fis.yaml's Pause action.
const pauseDuration = 10 * time.Minute

// Scenario 3: with replication paused by AWS FIS, each region shows only its own appends;
// after the pause, both converge.
//
//nolint:thelper // a scenario: its failures point into it
func testReplicationPause(t *testing.T, h *harness) {
	fisRegion := h.regions[1] // where scripts/e2e/deploy.sh deploys fis.yaml
	c := h.aws.clients[fisRegion].fis
	id := pauseTemplate(t, c, h.aws.run)
	exp, err := c.StartExperiment(t.Context(), &fis.StartExperimentInput{ExperimentTemplateId: &id})
	if err != nil {
		t.Fatal(err)
	}
	expID := exp.Experiment.Id
	t.Cleanup(func() { stopExperiment(t, c, expID) })
	started := awaitExperiment(t, h, c, expID, 5*time.Minute, func(e *fistypes.Experiment) bool {
		return e.Actions["Pause"].StartTime != nil
	})
	start := *started.Actions["Pause"].StartTime
	// Replication can go on for up to 5 minutes after the action starts (AWS FIS).
	if err = sleep(t.Context(), time.Until(start.Add(5*time.Minute+30*time.Second))); err != nil {
		t.Fatal(err)
	}
	r := newRun(t, h, "s3")
	r.paused = true
	for _, reg := range h.regions {
		r.pageViews(reg, flux.PageView{UserID: r.key(reg.name), Page: r.key("/p"), Tags: []string{r.key("t")}, DurationMS: 25, EventTime: r.at})
	}
	// Before the pause ends, each region shows its own appends alone, and its stream table
	// holds them but not the other region's.
	h.await(t, time.Until(start.Add(pauseDuration-30*time.Second)), "views during the pause", func(ctx context.Context) error {
		for _, reg := range h.regions {
			if cerr := r.compare(ctx, reg); cerr != nil {
				return fmt.Errorf("%s: %w", reg.name, cerr)
			}
		}
		return nil
	})
	for _, reg := range h.regions {
		for _, from := range h.regions {
			evs, rerr := readPageViews(t.Context(), reg.client, r.key(from.name))
			if rerr != nil {
				t.Fatal(rerr)
			}
			want := 0
			if from == reg {
				want = 1
			}
			if len(evs) != want {
				t.Fatalf("%s: %d events appended in %s during the pause, want %d", reg.name, len(evs), from.name, want)
			}
		}
	}
	awaitExperiment(t, h, c, expID, 10*time.Minute, func(e *fistypes.Experiment) bool {
		return e.State.Status == fistypes.ExperimentStatusCompleted
	})
	r.resume()
	r.converged(h.regions...)
}

// stopExperiment stops the experiment if the scenario failed before the pause ended.
func stopExperiment(t *testing.T, c *fis.Client, id *string) {
	t.Helper()
	ctx := context.WithoutCancel(t.Context())
	out, err := c.GetExperiment(ctx, &fis.GetExperimentInput{Id: id})
	active := []fistypes.ExperimentStatus{
		fistypes.ExperimentStatusPending, fistypes.ExperimentStatusInitiating, fistypes.ExperimentStatusRunning,
	}
	if err == nil && slices.Contains(active, out.Experiment.State.Status) {
		_, err = c.StopExperiment(ctx, &fis.StopExperimentInput{Id: id})
	}
	if err != nil {
		t.Errorf("stop experiment %s: %v", aws.ToString(id), err)
	}
}

// pauseTemplate returns the ID of fis.yaml's experiment template for run.
func pauseTemplate(t *testing.T, c *fis.Client, run string) string {
	t.Helper()
	pages := fis.NewListExperimentTemplatesPaginator(c, &fis.ListExperimentTemplatesInput{})
	for pages.HasMorePages() {
		page, err := pages.NextPage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, tmpl := range page.ExperimentTemplates {
			if tmpl.Tags["dynaflux:e2e-run"] == run {
				return aws.ToString(tmpl.Id)
			}
		}
	}
	t.Fatalf("no experiment template tagged dynaflux:e2e-run=%s: deploy fis.yaml with scripts/e2e/deploy.sh", run)
	return ""
}

// awaitExperiment waits until done holds for the experiment, and returns it.
func awaitExperiment(t *testing.T, h *harness, c *fis.Client, id *string, patience time.Duration, done func(*fistypes.Experiment) bool) *fistypes.Experiment {
	t.Helper()
	var exp *fistypes.Experiment
	h.await(t, patience, "experiment "+aws.ToString(id), func(ctx context.Context) error {
		out, err := c.GetExperiment(ctx, &fis.GetExperimentInput{Id: id})
		if err != nil {
			return err
		}
		exp = out.Experiment
		failed := []fistypes.ExperimentStatus{
			fistypes.ExperimentStatusFailed, fistypes.ExperimentStatusStopped, fistypes.ExperimentStatusCancelled,
		}
		if st := exp.State; slices.Contains(failed, st.Status) {
			t.Fatalf("experiment %s: %s: %s", aws.ToString(id), st.Status, aws.ToString(st.Reason))
		}
		if !done(exp) {
			return fmt.Errorf("status %s", exp.State.Status)
		}
		return nil
	})
	return exp
}

// denyPolicy is the inline policy that scenarios 23 and 24 add to the projector and backfill
// roles, and that scripts/e2e/teardown.sh removes if a run left it.
const denyPolicy = "dynaflux-e2e-deny"

// Scenario 23: under the 900-second tumbling window, with steady appends, row failures (a
// denied view table: partial replies) and function errors (a denied meta table: retries
// with bisection) end with the views equal to the oracle.
//
//nolint:thelper // a scenario: its failures point into it
func testPartialFailures(t *testing.T, h *harness) {
	home := h.regions[0]
	c := h.aws.clients[home]
	role := functionRole(t, h, home, home.naming.ProjectorFunction())
	t.Cleanup(func() {
		if err := removeDeny(context.WithoutCancel(t.Context()), c.iam, role); err != nil {
			t.Error(err)
		}
	})
	table := "arn:aws:dynamodb:" + home.name + ":" + h.aws.account + ":table/"
	r := newRun(t, h, "s23")
	since, n := time.Now(), 0
	appendFor := func(d time.Duration) {
		for end := time.Now().Add(d); time.Now().Before(end); n++ {
			r.pageViews(home, flux.PageView{
				UserID: r.key("u"), Page: r.key("/p"), DurationMS: 15, EventTime: r.at.Add(time.Duration(n) * time.Millisecond),
			})
			if err := sleep(t.Context(), 5*time.Second); err != nil {
				t.Fatal(err)
			}
		}
	}
	deny(t, c.iam, role, "dynamodb:PutItem", table+home.naming.ViewTable("page_daily", 1))
	appendFor(2 * time.Minute)
	deny(t, c.iam, role, "dynamodb:*", table+home.naming.MetaTable())
	appendFor(2 * time.Minute)
	if err := removeDeny(t.Context(), c.iam, role); err != nil {
		t.Fatal(err)
	}
	appendFor(30 * time.Second)
	h.await(t, 2*awsPatience, "views after the failures", func(ctx context.Context) error {
		for _, reg := range h.regions {
			if cerr := r.compare(ctx, reg); cerr != nil {
				return fmt.Errorf("%s: %w", reg.name, cerr)
			}
		}
		return nil
	})
	group := "/aws/lambda/" + home.naming.ProjectorFunction()
	for _, msg := range []string{"rows failed", "batch failed"} {
		lines, lerr := h.aws.logLines(t.Context(), home, group, `{ $.msg = "`+msg+`" }`, since)
		if lerr != nil {
			t.Fatal(lerr)
		}
		if len(lines) == 0 {
			t.Errorf("%s: no %q line since the first deny", group, msg)
		}
	}
}

// functionRole returns the name of a function's execution role in reg.
func functionRole(t *testing.T, h *harness, reg *region, function string) string {
	t.Helper()
	fn, err := h.aws.clients[reg].lambda.GetFunctionConfiguration(t.Context(), &lambda.GetFunctionConfigurationInput{FunctionName: &function})
	if err != nil {
		t.Fatal(err)
	}
	arn := aws.ToString(fn.Role)
	return arn[strings.LastIndex(arn, "/")+1:]
}

// removeDeny removes denyPolicy from role, if it is there.
func removeDeny(ctx context.Context, c *iam.Client, role string) error {
	_, err := c.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{RoleName: &role, PolicyName: new(denyPolicy)})
	var gone *iamtypes.NoSuchEntityException
	if err != nil && !errors.As(err, &gone) {
		return fmt.Errorf("remove %s from %s: %w", denyPolicy, role, err)
	}
	return nil
}

// deny puts denyPolicy on role, denying action on resource, and waits for IAM to apply it.
func deny(t *testing.T, c *iam.Client, role, action, resource string) {
	t.Helper()
	doc, err := json.Marshal(map[string]any{
		"Version":   "2012-10-17",
		"Statement": []map[string]string{{"Effect": "Deny", "Action": action, "Resource": resource}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.PutRolePolicy(t.Context(), &iam.PutRolePolicyInput{RoleName: &role, PolicyName: new(denyPolicy), PolicyDocument: new(string(doc))})
	if err != nil {
		t.Fatal(err)
	}
	if err = sleep(t.Context(), 15*time.Second); err != nil {
		t.Fatal(err)
	}
}

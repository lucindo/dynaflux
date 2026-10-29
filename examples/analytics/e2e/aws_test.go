// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/fis"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// awsTarget is the deployed e2e store of run E2E_RUN in account E2E_ACCOUNT.
type awsTarget struct {
	run     string
	account string
	clients map[*region]*awsClients
}

type awsClients struct {
	athena        *athena.Client
	logs          *cloudwatchlogs.Client
	alarms        *cloudwatch.Client
	lambda        *lambda.Client
	glue          *glue.Client
	firehose      *firehose.Client
	iam           *iam.Client
	fis           *fis.Client
	lakeformation *lakeformation.Client
}

// How long a deployed region may take: replication and the batching window for the views,
// Firehose's 300-second buffer for the archive.
const (
	awsPatience        = 10 * time.Minute
	awsArchivePatience = 20 * time.Minute
)

// newAWS opens the stacks of scripts/e2e/deploy.sh, with the credentials of the default
// chain. deploy.sh's backfill -all made the view versions live.
func newAWS(t *testing.T, p *project) *harness {
	t.Helper()
	run, account := os.Getenv("E2E_RUN"), os.Getenv("E2E_ACCOUNT")
	if !regexp.MustCompile(`^[a-z0-9]{1,8}$`).MatchString(run) {
		t.Fatalf("E2E_RUN is %q; set it to the run that scripts/e2e/deploy.sh deployed", run)
	}
	if !regexp.MustCompile(`^[0-9]{12}$`).MatchString(account) {
		t.Fatalf("E2E_ACCOUNT is %q; set it to the ID of the sandbox account", account)
	}
	a := &awsTarget{run: run, account: account, clients: map[*region]*awsClients{}}
	h := &harness{p: p, replicated: true, patience: awsPatience, archivePatience: awsArchivePatience, aws: a}
	for _, name := range p.def.Regions {
		cfg, err := config.LoadDefaultConfig(t.Context(), config.WithRegion(name))
		if err != nil {
			t.Fatal(err)
		}
		// The default chain can point at any account.
		id, err := sts.NewFromConfig(cfg).GetCallerIdentity(t.Context(), &sts.GetCallerIdentityInput{})
		if err != nil {
			t.Fatal(err)
		}
		if got := aws.ToString(id.Account); got != account {
			t.Fatalf("the AWS credentials are for account %s, not E2E_ACCOUNT=%s", got, account)
		}
		r := &region{
			name: name, cfg: cfg, naming: catalog.Naming{Prefix: "e2e-" + run + "-", Store: p.def.Store},
			ddb: dynamodb.NewFromConfig(cfg),
			// Scenario 2 reads a stream beside the mapping, and DynamoDB Streams throttles
			// a second reader of a Global Table with LimitExceededException, which the
			// SDK retries.
			streams: dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) { o.RetryMaxAttempts = 10 }),
			settle:  func(context.Context) error { return nil },
		}
		r.meta = meta.New(dynamo.New(r.ddb), r.naming.MetaTable())
		if r.store, err = dynaflux.Open(dynaflux.Config{AWS: cfg, Prefix: r.naming.Prefix}); err != nil {
			t.Fatal(err)
		}
		r.client = flux.New(r.store)
		c := &awsClients{
			athena: athena.NewFromConfig(cfg), logs: cloudwatchlogs.NewFromConfig(cfg), alarms: cloudwatch.NewFromConfig(cfg),
			lambda: lambda.NewFromConfig(cfg), glue: glue.NewFromConfig(cfg), firehose: firehose.NewFromConfig(cfg),
			iam: iam.NewFromConfig(cfg), fis: fis.NewFromConfig(cfg), lakeformation: lakeformation.NewFromConfig(cfg),
		}
		a.clients[r] = c
		a.checkDeployed(t, p, r)
		r.archive = func(ctx context.Context, stream, key, prefix string) (archived, error) {
			return a.archived(ctx, p, r, stream, key, prefix)
		}
		h.regions = append(h.regions, r)
	}
	return h
}

// assume returns the credentials of role, limited by policy unless it is nil. source names
// the variable that gave the role.
func (a *awsTarget) assume(cfg aws.Config, role, source string, policy *string) aws.CredentialsProvider {
	c := sts.NewFromConfig(cfg)
	return aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		out, err := c.AssumeRole(ctx, &sts.AssumeRoleInput{RoleArn: &role, RoleSessionName: new("dynaflux-e2e-" + a.run), Policy: policy})
		if err != nil {
			return aws.Credentials{}, err
		}
		cr := out.Credentials
		return aws.Credentials{
			AccessKeyID: aws.ToString(cr.AccessKeyId), SecretAccessKey: aws.ToString(cr.SecretAccessKey), SessionToken: aws.ToString(cr.SessionToken),
			CanExpire: true, Expires: aws.ToTime(cr.Expiration), Source: source,
		}, nil
	}))
}

// checkDeployed fails the test unless the region was deployed from the sources the suite
// compiles, the e2e view included.
func (a *awsTarget) checkDeployed(t *testing.T, p *project, r *region) {
	t.Helper()
	fn, err := a.clients[r].lambda.GetFunctionConfiguration(t.Context(), &lambda.GetFunctionConfigurationInput{
		FunctionName: new(r.naming.ProjectorFunction()),
	})
	if err != nil {
		t.Fatalf("%s: %v; deploy with scripts/e2e/deploy.sh first", r.name, err)
	}
	if got, want := fn.Environment.Variables[catalog.EnvArchiveSchema], p.fingerprint(r.name); got != want {
		t.Fatalf("%s: the projector's %s is %q, but this checkout's e2e store gives %q: deploy and test from the same checkout",
			r.name, catalog.EnvArchiveSchema, got, want)
	}
	for _, v := range p.def.Views {
		for _, vv := range v.Versions {
			table := r.naming.ViewTable(v.Name, vv.Number)
			if _, err = r.ddb.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: &table}); err != nil {
				t.Fatalf("%s: view table %s: %v; deploy with scripts/e2e/deploy.sh from this checkout", r.name, table, err)
			}
		}
	}
}

// archived reads a stream's archive in a region through Athena: the events from the dedup
// view, and the stream record of each copy from the table.
func (a *awsTarget) archived(ctx context.Context, p *project, r *region, stream, key, prefix string) (archived, error) {
	st, ok := p.def.Stream(stream)
	if !ok {
		return archived{}, fmt.Errorf("no stream %s", stream)
	}
	cols := tableColumns(st)
	exprs := make([]string, len(cols))
	for i, c := range cols {
		var err error
		if exprs[i], err = athenaText(c); err != nil {
			return archived{}, err
		}
	}
	db := `"` + r.naming.ArchiveDatabase() + `"`
	where := fmt.Sprintf(` WHERE "%s" LIKE '%s%%'`, key, prefix)
	rows, err := a.query(ctx, r, "SELECT "+strings.Join(exprs, ", ")+" FROM "+db+`."`+catalog.DedupView(stream)+`"`+where)
	if err != nil {
		return archived{}, err
	}
	var out archived
	for _, row := range rows {
		ev := map[string]string{}
		for i, v := range row {
			if v != nil {
				ev[cols[i].Name] = *v
			}
		}
		out.events = append(out.events, ev)
	}
	rows, err = a.query(ctx, r, fmt.Sprintf(`SELECT "%s", "%s", "%s", CAST("%s" AS varchar) FROM %s."%s"%s`,
		catalog.ColumnEventID, archive.ColumnShard, archive.ColumnSeq, archive.ColumnArrival, db, stream, where))
	if err != nil {
		return archived{}, err
	}
	for _, row := range rows {
		c := copyRecord{event: aws.ToString(row[0]), shard: aws.ToString(row[1]), seq: aws.ToString(row[2])}
		if row[3] != nil {
			if c.arrival, err = time.Parse(timestampLayout, *row[3]); err != nil {
				return archived{}, fmt.Errorf("%s of %s: %w", archive.ColumnArrival, c.event, err)
			}
		}
		out.copies = append(out.copies, c)
	}
	return out, nil
}

// query runs a statement in the store's workgroup, which keeps the results, and returns its
// rows without the header; a nil value is NULL.
func (a *awsTarget) query(ctx context.Context, r *region, sql string) ([][]*string, error) {
	c := a.clients[r].athena
	start, err := c.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: &sql, WorkGroup: new(r.naming.ArchiveWorkGroup()),
	})
	if err != nil {
		return nil, fmt.Errorf("athena: %w", err)
	}
	for done := false; !done; {
		q, qerr := c.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: start.QueryExecutionId})
		if qerr != nil {
			return nil, fmt.Errorf("athena: %w", qerr)
		}
		switch st := q.QueryExecution.Status; st.State {
		case athenatypes.QueryExecutionStateSucceeded:
			done = true
		case athenatypes.QueryExecutionStateFailed, athenatypes.QueryExecutionStateCancelled:
			return nil, fmt.Errorf("athena: %s: %s: %s", st.State, aws.ToString(st.StateChangeReason), sql)
		case athenatypes.QueryExecutionStateQueued, athenatypes.QueryExecutionStateRunning:
			if err = sleep(ctx, time.Second); err != nil {
				return nil, err
			}
		}
	}
	var rows [][]*string
	pages := athena.NewGetQueryResultsPaginator(c, &athena.GetQueryResultsInput{QueryExecutionId: start.QueryExecutionId})
	for header := true; pages.HasMorePages(); {
		page, perr := pages.NextPage(ctx)
		if perr != nil {
			return nil, fmt.Errorf("athena: %w", perr)
		}
		for _, row := range page.ResultSet.Rows {
			if header {
				header = false
				continue
			}
			vals := make([]*string, len(row.Data))
			for i, d := range row.Data {
				vals[i] = d.VarCharValue
			}
			rows = append(rows, vals)
		}
	}
	return rows, nil
}

// logLines returns the messages of a log group's events since a time that match a
// CloudWatch Logs filter pattern.
func (a *awsTarget) logLines(ctx context.Context, r *region, group, pattern string, since time.Time) ([]string, error) {
	var msgs []string
	pages := cloudwatchlogs.NewFilterLogEventsPaginator(a.clients[r].logs, &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName: &group, FilterPattern: &pattern, StartTime: new(since.UnixMilli()),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", group, err)
		}
		for _, ev := range page.Events {
			msgs = append(msgs, aws.ToString(ev.Message))
		}
	}
	return msgs, nil
}

// alarmFired returns nil once an alarm has gone into ALARM since a time. Its history keeps
// the change after the alarm is back to OK.
func (a *awsTarget) alarmFired(ctx context.Context, r *region, name string, since time.Time) error {
	pages := cloudwatch.NewDescribeAlarmHistoryPaginator(a.clients[r].alarms, &cloudwatch.DescribeAlarmHistoryInput{
		AlarmName: &name, HistoryItemType: cwtypes.HistoryItemTypeStateUpdate, StartDate: &since,
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, item := range page.AlarmHistoryItems {
			var data struct {
				NewState struct {
					StateValue string `json:"stateValue"`
				} `json:"newState"`
			}
			if err = json.Unmarshal([]byte(aws.ToString(item.HistoryData)), &data); err != nil {
				return fmt.Errorf("alarm history %s: %w", aws.ToString(item.HistoryData), err)
			}
			if data.NewState.StateValue == string(cwtypes.StateValueAlarm) {
				return nil
			}
		}
	}
	return fmt.Errorf("no change to ALARM since %s", since.Format(time.RFC3339))
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Fixed values of the backfill (docs/spec/infra.md §6.5, ADR 0011).
const (
	backfillTimeoutSeconds = 900
	backfillMemoryMB       = 3008
	// backfillMaxSteps keeps an execution's history, about 10 events a step, well under Step
	// Functions' 25,000.
	backfillMaxSteps = 1000
	// backfillStaging is where jobs stage their runs in the bucket: backfill.Start names each
	// job's staging prefix under it.
	backfillStaging       = "backfill/"
	stagingExpiryDays     = 30
	stagingNoncurrentDays = 1
)

// addBackfill adds the backfill function and the state machine that drives it to s, each
// with its log group and role.
func (b *builder) addBackfill(s *Scope) {
	b.addFunction(s, b.backfill())
	name := b.naming.BackfillStateMachine()
	logs := LogGroup{
		Common:        Common{ID: logsID(idBackfillMachine), Tags: b.tags(TagKindBackfill, "")},
		Name:          b.name(catalog.ResourceLogGroup, "/aws/vendedlogs/states/"+name),
		RetentionDays: logRetentionDays,
	}
	function := ref(idBackfill, AttrARN)
	role := Role{
		Common:  Common{ID: roleID(idBackfillMachine), Tags: b.tags(TagKindBackfill, "")},
		Service: ServiceStates,
		Trust:   []Condition{{Operator: condStringEquals, Key: "aws:SourceAccount", Values: []Text{Text(placeholder(PlaceholderAccount))}}},
		Statements: []Statement{
			{Sid: "Step", Actions: []string{"lambda:InvokeFunction"}, Resources: []Text{function, function + ":*"}},
			// The machine's own ARN, by name: a reference to it would be a cycle.
			{Sid: "Continue", Actions: []string{"states:StartExecution"}, Resources: []Text{regionalARN("states", Text("stateMachine:"+name))}},
			// What Step Functions needs to deliver logs; these actions have no resource type.
			{Sid: sidLogs, Actions: []string{
				"logs:CreateLogDelivery", "logs:DeleteLogDelivery", "logs:DescribeLogGroups", "logs:DescribeResourcePolicies",
				"logs:GetLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:UpdateLogDelivery",
			}, Resources: []Text{"*"}},
		},
	}
	s.LogGroups = append(s.LogGroups, logs)
	s.Roles = append(s.Roles, role)
	s.StateMachines = append(s.StateMachines, StateMachine{
		Common:   Common{ID: idBackfillMachine, Tags: b.tags(TagKindBackfill, "")},
		Name:     b.name(catalog.ResourceBackfillStateMachine, name),
		Role:     ref(role.ID, AttrARN),
		LogGroup: ref(logs.ID, AttrARN),
		Function: function,
		MaxSteps: backfillMaxSteps,
	})
}

// backfill returns the function that steps backfill jobs. It reads the stream tables, the
// meta table and the projectors' metrics, stages in the bucket, and writes the view tables.
// The statements that grow with the store go in managed policies.
func (b *builder) backfill() function {
	bucket := ref(idBucket, AttrARN)
	statements := []Statement{
		metaBatchStatement(),
		// GetMetricData has no resource type.
		{Sid: "Metrics", Actions: []string{"cloudwatch:GetMetricData"}, Resources: []Text{"*"}},
		{Sid: "Staging", Actions: []string{s3DeleteObject, s3GetObject, s3PutObject}, Resources: []Text{bucket + "/" + backfillStaging + "*"}},
		{Sid: "StagingList", Actions: []string{s3ListBucket}, Resources: []Text{bucket},
			Conditions: []Condition{{Operator: condStringLike, Key: "s3:prefix", Values: []Text{backfillStaging + "*"}}}},
	}
	var scan, describe, streams, views []Text
	for _, st := range b.cat.Streams {
		id := streamID(st.Name)
		scan = append(scan, ref(id, AttrARN))
		streams = append(streams, ref(id, AttrStreamARNPattern))
		// Every replica's, for its creation time.
		for _, region := range b.regions {
			describe = append(describe, Text("arn:"+placeholder(PlaceholderPartition)+":dynamodb:"+region+":"+
				placeholder(PlaceholderAccount)+":table/")+ref(id, AttrName))
		}
	}
	for _, v := range b.cat.Views {
		for _, vv := range v.Versions {
			views = append(views, ref(viewID(v.Name, vv.Number), AttrARN))
		}
	}
	for _, rs := range [][]Text{scan, describe, streams, views} {
		slices.Sort(rs)
	}
	managed := []Statement{
		{Sid: "StreamScan", Actions: []string{"dynamodb:Scan"}, Resources: scan},
		{Sid: "StreamTables", Actions: []string{"dynamodb:DescribeTable"}, Resources: describe},
		{Sid: "Streams", Actions: []string{"dynamodb:DescribeStream"}, Resources: streams},
	}
	if len(views) > 0 {
		managed = append(managed, Statement{Sid: sidViews, Actions: []string{dynamoBatchGetItem, dynamoGetItem, dynamoPutItem}, Resources: views})
	}
	return function{
		id: idBackfill, kind: catalog.ResourceBackfillFunction, name: b.naming.BackfillFunction(), mode: catalog.EnvModeBackfill,
		tags: b.tags(TagKindBackfill, ""), timeoutSeconds: backfillTimeoutSeconds, memoryMB: backfillMemoryMB,
		statements: statements, managed: managed,
	}
}

// backfillAlarm returns the alarm on the executions of the backfill state machine sm that
// fail.
func (b *builder) backfillAlarm(sm StateMachine) Alarm {
	return b.alarm(sm.ID, b.naming.BackfillStateMachine(), b.tags(TagKindBackfill, ""), rule{"Failed", SeverityCritical, 1, 1,
		"a backfill execution failed after its retries, so its job stopped. The backfill function's log group says why; " +
			"dynaflux backfill resumes the job from its last chunk."},
		Metric{Namespace: namespaceStates, Name: "ExecutionsFailed", Statistic: statSum,
			Dimensions: []Dimension{{Name: dimStateMachine, Value: ref(sm.ID, AttrARN)}}})
}

// backfillGraph returns the dashboard's graph of the backfill executions. A dashboard can't
// refer to a resource, so it names the machine's ARN.
func (b *builder) backfillGraph() graph {
	arn := string(regionalARN("states", Text("stateMachine:"+b.naming.BackfillStateMachine())))
	return graph{Title: "Backfill executions", Stat: statSum, Metrics: lines(namespaceStates, dimStateMachine, []string{arn},
		"ExecutionsStarted", "ExecutionsSucceeded", "ExecutionsFailed")}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"maps"
	"slices"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
)

// Fixed values of the query function (ADR 0018).
const (
	// queryTimeoutSeconds stays under API Gateway's 29 seconds, so a slow query is the
	// handler's 504 rather than the door's.
	queryTimeoutSeconds = 25
	// metaViewPartition is the meta table's partition of view states (encoding.md §8.2), the
	// only one the query function reads.
	metaViewPartition = "view"
)

// condForAllValuesStringEquals holds when every value of a multivalued condition key is
// listed, and when the request has no value.
const condForAllValuesStringEquals = "ForAllValues:StringEquals"

// IDs and output names of the query scope.
const (
	idQuery               = "Query"
	idQueryURL            = "QueryUrl"
	idPermissionURL       = "PermissionUrl"
	idPermissionURLInvoke = "PermissionUrlInvoke"
	outputQueryEndpoint   = "QueryEndpoint"
)

// query returns the query scope: the function that serves the queries annotated
// -- expose: http, with its log group, role and alarms, and the front door that
// http.frontend selects. It is empty when no query is exposed.
func (b *builder) query() Scope {
	if !slices.ContainsFunc(b.cat.Queries, func(q catalog.Query) bool { return q.ExposeHTTP }) {
		return Scope{}
	}
	var s Scope
	b.addFunction(&s, b.queryFunction(b.exposedTables()))
	f := s.Functions[0]
	s.Alarms = b.queryAlarms(f)
	endpoint := ref(idQueryURL, AttrURL)
	if b.cfg.HTTP.Frontend == config.FrontendFunctionURL {
		b.addFunctionURL(&s)
	} else {
		endpoint = b.addAPI(&s, f)
	}
	s.Outputs = append([]Output{{
		Name:        outputQueryEndpoint,
		Description: "The base URL of the exposed queries in this region: POST /queries/{name} and GET /health (docs/spec/http.md).",
		Value:       endpoint,
	}}, s.Outputs...)
	sortByID(s.LogGroups)
	sortByID(s.Permissions)
	sortByID(s.Alarms)
	return s
}

// addFunctionURL adds the function URL of the query function to s, with the permissions
// that a public one needs.
func (b *builder) addFunctionURL(s *Scope) {
	auth := URLAuthIAM
	if b.cfg.HTTP.Auth == config.AuthNone {
		auth = URLAuthNone
		fn := ref(idQuery, AttrName)
		s.Permissions = []Permission{
			{Common: Common{ID: idPermissionURL}, Function: fn, Action: "lambda:InvokeFunctionUrl", Principal: "*", URLAuth: URLAuthNone},
			{Common: Common{ID: idPermissionURLInvoke}, Function: fn, Action: lambdaInvokeFunction, Principal: "*", ViaURL: true},
		}
	}
	s.FunctionURLs = []FunctionURL{{Common: Common{ID: idQueryURL}, Function: ref(idQuery, AttrARN), Auth: auth}}
}

// exposedTables returns the IDs of the tables that the exposed queries read, sorted: the
// current version's of each view, then each stream's.
func (b *builder) exposedTables() ([]string, []string) {
	var views, streams []string
	for v, vv := range b.cat.ExposedViews() {
		views = append(views, viewID(v.Name, vv.Number))
	}
	for _, q := range b.cat.Queries {
		if q.ExposeHTTP && q.Source.Kind == catalog.SourceStream {
			streams = append(streams, streamID(q.Source.Name))
		}
	}
	return slices.Sorted(slices.Values(views)), slices.Compact(slices.Sorted(slices.Values(streams)))
}

// queryFunction returns the query function. Its role reads the view states, by the meta
// table's view partition only, and the tables of the exposed queries; it writes nothing.
func (b *builder) queryFunction(views, streams []string) function {
	statements := []Statement{{
		Sid: sidMeta, Actions: []string{dynamoQuery}, Resources: []Text{ref(idMeta, AttrARN)},
		Conditions: []Condition{{Operator: condForAllValuesStringEquals, Key: "dynamodb:LeadingKeys", Values: []Text{metaViewPartition}}},
	}}
	arns := func(ids []string) []Text {
		var ts []Text
		for _, id := range ids {
			ts = append(ts, ref(id, AttrARN))
		}
		return ts
	}
	if len(views) > 0 {
		statements = append(statements, Statement{Sid: sidViews, Actions: []string{dynamoBatchGetItem, dynamoQuery}, Resources: arns(views)})
	}
	if len(streams) > 0 {
		statements = append(statements, Statement{Sid: "Streams", Actions: []string{dynamoQuery}, Resources: arns(streams)})
	}
	return function{
		id: idQuery, kind: catalog.ResourceQueryFunction, name: b.naming.QueryFunction(), mode: catalog.EnvModeQuery,
		tags: b.tags(TagKindQuery, ""), timeoutSeconds: queryTimeoutSeconds, reserved: b.cfg.HTTP.MaxConcurrency,
		env: map[string]Text{catalog.EnvCatalogHash: Text(b.catalogHash)}, statements: statements,
	}
}

// queryAlarms returns the alarms on the query function f: its crashes, its throttles and its
// failed answers.
func (b *builder) queryAlarms(f Function) []Alarm {
	lambda := func(name string) Metric {
		return Metric{Namespace: namespaceLambda, Name: name, Statistic: statSum,
			Dimensions: []Dimension{{Name: dimFunctionName, Value: ref(f.ID, AttrName)}}}
	}
	alarm := func(r rule, m Metric) Alarm {
		return b.alarm(f.ID, string(f.Name), maps.Clone(f.Tags), r, m)
	}
	return []Alarm{
		alarm(rule{"Errors", SeverityWarning, 1, 1,
			"the query function crashed, timed out, or got an event from no door it serves, so callers got the door's own " +
				"error. Read its log group."}, lambda(metricErrors)),
		alarm(rule{"Failures", SeverityWarning, 1, sustained,
			"the query function has answered 500 or 504 for 15 minutes. Its log group gives each request's code and cause."},
			Metric{Namespace: catalog.MetricNamespace, Name: catalog.MetricQueryFailures, Statistic: statSum,
				Dimensions: []Dimension{{Name: catalog.MetricDimensionStore, Value: Text(b.naming.Base())}}}),
		alarm(rule{"Throttles", SeverityWarning, 1, sustained,
			"Lambda has throttled the query function for 15 minutes, so callers got errors. Raise http.max_concurrency, " +
				"or the account's concurrent executions quota."}, lambda(metricThrottles)),
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
)

// Fixed values of the API Gateway doors and the custom domain (ADR 0018).
const (
	// httpStage serves an HTTP API's root. A REST API's stage, restStage, is the first part
	// of its paths.
	httpStage = "$default"
	restStage = "api"
	// Route 53's checkers call a health check about once every 2 seconds in all, so the
	// stage's throttle of GET /health leaves them room and keeps anyone else from draining
	// the function's concurrency through it.
	healthRate  = 5
	healthBurst = 10
	// healthCheckIntervalSeconds and healthCheckFailures take a region out of the custom
	// domain about 90 seconds after its views stop being ready.
	healthCheckIntervalSeconds = 30
	healthCheckFailures        = 3
	// authorizerTTLSeconds is how long API Gateway keeps a Lambda authorizer's answer for
	// one Authorization header.
	authorizerTTLSeconds = 300
	// ACM renews a DNS-validated certificate from 60 days before it expires, so one with
	// fewer than certificateExpiryDays left failed to renew. ACM publishes DaysToExpiry once
	// a day.
	certificateExpiryDays    = 45
	certificatePeriodSeconds = 24 * 60 * 60
	// maxAPIName is the longest HTTP API name; a REST API takes longer ones.
	maxAPIName        = 128
	apiLogGroupPrefix = "/aws/vendedlogs/apigateway/"
)

const (
	lambdaInvokeFunction        = "lambda:InvokeFunction"
	serviceAPIGateway           = "apigateway.amazonaws.com"
	namespaceCertificateManager = "AWS/CertificateManager"
	statMinimum                 = "Minimum"
)

// IDs and output names of the API Gateway doors.
const (
	idAPI                  = "Api"
	idStage                = "Stage"
	idAuthorizer           = "Authorizer"
	idUsagePlan            = "UsagePlan"
	idCertificate          = "Certificate"
	idDomain               = "Domain"
	idHealthCheck          = "HealthCheck"
	idLatencyRecord        = "LatencyRecord"
	idPermissionAPI        = "PermissionApi"
	idPermissionAuthorizer = "PermissionAuthorizer"
	outputQueryUsagePlanID = "QueryUsagePlanId"
)

// addAPI adds the door of http.frontend http_api or rest_api to s: the API, its access log
// group, its authorizer or usage plan when http.auth has one, the permission to invoke the
// query function f, and the custom domain of http.domain. It returns the API's base URL in
// the region.
func (b *builder) addAPI(s *Scope, f Function) Text {
	h := b.cfg.HTTP
	protocol, stage, path := APIProtocolHTTP, httpStage, "/"
	if h.Frontend == config.FrontendRESTAPI {
		protocol, stage, path = APIProtocolREST, restStage, "/"+restStage+"/"
	}
	name := b.naming.QueryFunction()
	logs := LogGroup{
		Common:        Common{ID: logsID(idAPI), Tags: b.tags(TagKindQuery, "")},
		Name:          b.name(catalog.ResourceLogGroup, apiLogGroupPrefix+name),
		RetentionDays: logRetentionDays,
	}
	api := API{
		Common:   Common{ID: idAPI, Tags: b.tags(TagKindQuery, "")},
		Name:     b.fit(maxAPIName, name),
		Protocol: protocol,
		Function: ref(idQuery, AttrARN),
		Auth:     APIAuth(h.Auth), // the values of http.auth that APIs take are APIAuth's
		Stage: Stage{
			ID: idStage, Name: stage, LogGroup: ref(logs.ID, AttrARN),
			HealthThrottle: Throttle{Rate: healthRate, Burst: healthBurst},
		},
	}
	b.authorize(&api, s)
	s.APIs = []API{api}
	s.LogGroups = append(s.LogGroups, logs)
	s.Permissions = append(s.Permissions, Permission{
		Common: Common{ID: idPermissionAPI}, Function: ref(idQuery, AttrName), Action: lambdaInvokeFunction,
		Principal: serviceAPIGateway, SourceARN: executeAPI("/*"),
	})
	if h.Auth == config.AuthAPIKey {
		b.addUsagePlan(s, name)
	}
	host := ref(idAPI, AttrID) + ".execute-api." + Text(placeholder(PlaceholderRegion)+"."+placeholder(PlaceholderURLSuffix))
	if h.Domain != nil {
		b.addDomain(s, f, host, path)
	}
	return "https://" + host + Text(path)
}

// authorize sets the authorizer and scopes that http.auth asks of api's queries route, and
// adds the permission that a Lambda authorizer needs to s.
func (b *builder) authorize(api *API, s *Scope) {
	h := b.cfg.HTTP
	a := &Authorizer{ID: idAuthorizer, Name: string(h.Auth)}
	switch h.Auth {
	case config.AuthJWT:
		a.Issuer, a.Audience, api.Scopes = h.JWT.Issuer, h.JWT.Audience, h.JWT.Scopes
	case config.AuthLambda:
		a.Function, a.TTLSeconds = regionalARN("lambda", Text("function:"+h.Authorizer.Function)), authorizerTTLSeconds
		s.Permissions = append(s.Permissions, Permission{
			Common: Common{ID: idPermissionAuthorizer}, Function: a.Function, Action: lambdaInvokeFunction,
			Principal: serviceAPIGateway, SourceARN: executeAPI("/authorizers/" + ref(idAuthorizer, AttrID)),
		})
	case config.AuthCognito:
		a.UserPool, a.Clients, api.Scopes = h.Cognito.UserPoolARN, h.Cognito.AppClients, h.Cognito.Scopes
	case config.AuthIAM, config.AuthAPIKey, config.AuthNone:
		return
	}
	api.Authorizer = a
}

// executeAPI returns the ARN of what path names in the API: "/*" for all of it.
func executeAPI(path Text) Text {
	return regionalARN("execute-api", ref(idAPI, AttrID)+path)
}

// addUsagePlan adds the usage plan of auth api_key to s, called name, with the limits of
// http.usage_plan, and the output that gives its ID: an operator adds each API key to it.
func (b *builder) addUsagePlan(s *Scope, name string) {
	plan := UsagePlan{
		Common: Common{ID: idUsagePlan, Tags: b.tags(TagKindQuery, "")}, Name: Text(name),
		API: ref(idAPI, AttrID), Stage: ref(idStage, AttrName),
	}
	if p := b.cfg.HTTP.UsagePlan; p != nil {
		plan.Throttle, plan.QuotaPerDay = Throttle{Rate: p.Rate, Burst: p.Burst}, p.QuotaPerDay
	}
	s.UsagePlans = []UsagePlan{plan}
	s.Outputs = append(s.Outputs, Output{
		Name:        outputQueryUsagePlanID,
		Description: "The ID of the usage plan that each API key must belong to in this region (docs/spec/http.md §8.4).",
		Value:       ref(idUsagePlan, AttrID),
	})
}

// addDomain adds the custom domain to s: its certificate, with an alarm on its renewal named
// after the query function f, the domain mapped to the API's stage, and the region's latency
// record, which Route 53 answers with while a health check of path + "health" at host, the
// API's own host in the region, passes. A check of the domain itself would follow the latency
// record to another region.
func (b *builder) addDomain(s *Scope, f Function, host Text, path string) {
	domain, zone := Text(placeholder(PlaceholderDomain)), Text(placeholder(PlaceholderHostedZone))
	s.Certificates = []Certificate{{Common: Common{ID: idCertificate, Tags: b.tags(TagKindQuery, "")}, Domain: domain, HostedZone: zone}}
	s.Domains = []Domain{{
		Common: Common{ID: idDomain, Tags: b.tags(TagKindQuery, "")}, Name: domain,
		Certificate: ref(idCertificate, AttrARN), API: ref(idAPI, AttrID), Stage: ref(idStage, AttrName),
	}}
	s.HealthChecks = []HealthCheck{{
		Common: Common{ID: idHealthCheck, Tags: b.tags(TagKindQuery, "")}, Host: host, Path: path + "health",
		IntervalSeconds: healthCheckIntervalSeconds, FailureThreshold: healthCheckFailures,
	}}
	s.LatencyRecords = []LatencyRecord{{
		Common: Common{ID: idLatencyRecord}, Name: domain, HostedZone: zone, Target: ref(idDomain, AttrRegionalName),
		TargetZone: ref(idDomain, AttrRegionalZone), HealthCheck: ref(idHealthCheck, AttrID),
	}}
	s.Alarms = append(s.Alarms, b.certificateAlarm(f))
}

// certificateAlarm returns the alarm on the renewal of the custom domain's certificate,
// named after the query function f.
func (b *builder) certificateAlarm(f Function) Alarm {
	a := b.alarm(f.ID, string(f.Name), b.tags(TagKindQuery, ""), rule{"Certificate", SeverityWarning, certificateExpiryDays, 1,
		"the custom domain's certificate expires in less than 45 days, so ACM, which renews it from 60 days ahead, " +
			"couldn't. Check that its DNS validation record is still in the hosted zone."},
		Metric{Namespace: namespaceCertificateManager, Name: "DaysToExpiry", Statistic: statMinimum,
			Dimensions: []Dimension{{Name: "CertificateArn", Value: ref(idCertificate, AttrARN)}}})
	a.PeriodSeconds, a.Comparison = certificatePeriodSeconds, ComparisonBelow
	return a
}

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
	// GET /health takes anyone, so the stage's throttle keeps them from draining the
	// function's concurrency through it. The custom domain's health check never calls it.
	healthRate  = 5
	healthBurst = 10
	// The query function checks readiness every minute, and readinessPeriods minutes in a
	// row not ready take the region out of the custom domain. Route 53 follows no alarm
	// that needs only some of its periods.
	readinessPeriodSeconds = 60
	readinessPeriods       = 2
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
	serviceEvents               = "events.amazonaws.com"
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
	idSchedule             = "Schedule"
	idLatencyRecord        = "LatencyRecord"
	idPermissionAPI        = "PermissionApi"
	idPermissionAuthorizer = "PermissionAuthorizer"
	idPermissionSchedule   = "PermissionSchedule"
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
		b.addDomain(s, f)
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
// record, which Route 53 answers with while the region's views are ready. f checks them on a
// schedule, and the health check follows the alarm on its checks.
func (b *builder) addDomain(s *Scope, f Function) {
	domain, zone := Text(placeholder(PlaceholderDomain)), Text(placeholder(PlaceholderHostedZone))
	s.Certificates = []Certificate{{Common: Common{ID: idCertificate, Tags: b.tags(TagKindQuery, "")}, Domain: domain, HostedZone: zone}}
	s.Domains = []Domain{{
		Common: Common{ID: idDomain, Tags: b.tags(TagKindQuery, "")}, Name: domain,
		Certificate: ref(idCertificate, AttrARN), API: ref(idAPI, AttrID), Stage: ref(idStage, AttrName),
	}}
	notReady := b.notReadyAlarm(f)
	s.Schedules = []Schedule{{Common: Common{ID: idSchedule, Tags: b.tags(TagKindQuery, "")}, Function: ref(idQuery, AttrARN)}}
	s.Permissions = append(s.Permissions, Permission{
		Common: Common{ID: idPermissionSchedule}, Function: ref(idQuery, AttrName), Action: lambdaInvokeFunction,
		Principal: serviceEvents, SourceARN: ref(idSchedule, AttrARN),
	})
	s.HealthChecks = []HealthCheck{{Common: Common{ID: idHealthCheck, Tags: b.tags(TagKindQuery, "")}, Alarm: ref(notReady.ID, AttrName)}}
	s.LatencyRecords = []LatencyRecord{{
		Common: Common{ID: idLatencyRecord}, Name: domain, HostedZone: zone, Target: ref(idDomain, AttrRegionalName),
		TargetZone: ref(idDomain, AttrRegionalZone), HealthCheck: ref(idHealthCheck, AttrID),
	}}
	s.Alarms = append(s.Alarms, b.certificateAlarm(f), notReady)
}

// notReadyAlarm returns the alarm on the readiness checks of the query function f, which the
// custom domain's health check follows. A check whose line is lost leaves no data, which
// doesn't breach: only a check that finds a view not ready takes the region out.
func (b *builder) notReadyAlarm(f Function) Alarm {
	a := b.alarm(f.ID, string(f.Name), b.tags(TagKindQuery, ""), rule{"ViewsNotReady", SeverityWarning, 1, readinessPeriods,
		"a view that an exposed query reads isn't live in this region, or the query function can't read the view states, " +
			"so the custom domain sends no queries here. Run dynaflux status."},
		Metric{Namespace: catalog.MetricNamespace, Name: catalog.MetricViewsNotReady, Statistic: statMaximum,
			Dimensions: []Dimension{{Name: catalog.MetricDimensionStore, Value: Text(b.naming.Base())}}})
	a.PeriodSeconds = readinessPeriodSeconds
	return a
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

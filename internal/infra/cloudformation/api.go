// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/infra"
)

// IDs of the resources that an API and a custom domain are rendered with, besides those of
// the manifest.
const (
	idIntegration     = "Integration"
	idRouteQueries    = "RouteQueries"
	idRouteHealth     = "RouteHealth"
	idResourceQueries = "ResourceQueries"
	idResourceQuery   = "ResourceQuery"
	idResourceHealth  = "ResourceHealth"
	idMethodQuery     = "MethodQuery"
	idMethodHealth    = "MethodHealth"
	idMapping         = "Mapping"
	// idDeployment is followed by 8 hex digits of what the REST API's deployment holds.
	idDeployment = "Deployment"
)

const (
	routeHealth = "GET /health"
	// httpIntegrationMillis is the most an HTTP API waits for the function.
	httpIntegrationMillis = 30_000
	httpsPort             = 443
	// authorizationHeader is where every authorizer finds the caller's credentials.
	authorizationHeader  = "Authorization"
	deploymentHashDigits = 8
)

// The access log line of each kind of stage, one JSON object per request.
const (
	httpAccessLog = `{"requestId":"$context.requestId","ip":"$context.identity.sourceIp","route":"$context.routeKey",` +
		`"path":"$context.path","status":"$context.status","latency":"$context.responseLatency",` +
		`"integrationError":"$context.integrationErrorMessage","authorizerError":"$context.authorizer.error"}`
	restAccessLog = `{"requestId":"$context.requestId","ip":"$context.identity.sourceIp","apiKeyId":"$context.identity.apiKeyId",` +
		`"path":"$context.path","status":"$context.status","latency":"$context.responseLatency",` +
		`"integrationError":"$context.integrationErrorMessage"}`
)

// addDoors adds the API Gateway doors of s: its APIs, usage plans and custom domains, with
// their health checks and latency records.
func addDoors(rs *resources, r *resolver, s infra.Scope) {
	for _, a := range s.APIs {
		if a.Protocol == infra.APIProtocolREST {
			addRESTAPI(rs, r, a)
		} else {
			addHTTPAPI(rs, r, a)
		}
	}
	for _, u := range s.UsagePlans {
		rs.add(u.ID, func() *yaml.Node { return usagePlan(r, u) })
	}
	for _, c := range s.Certificates {
		rs.add(c.ID, func() *yaml.Node { return certificate(r, c) })
	}
	for _, d := range s.Domains {
		addDomain(rs, r, d)
	}
	for _, x := range s.Schedules {
		rs.add(x.ID, func() *yaml.Node { return schedule(r, x) })
	}
	for _, h := range s.HealthChecks {
		rs.add(h.ID, func() *yaml.Node { return healthCheck(r, h) })
	}
	for _, l := range s.LatencyRecords {
		rs.add(l.ID, func() *yaml.Node { return latencyRecord(r, l) })
	}
}

// addHTTPAPI adds an HTTP API: one integration with the function, the two routes, the
// authorizer and the $default stage, which deploys every change by itself.
func addHTTPAPI(rs *resources, r *resolver, a infra.API) {
	rs.add(a.ID, func() *yaml.Node {
		return resource("AWS::ApiGatewayV2::Api", mapping(
			kv("Name", r.value(a.Name)),
			kv("ProtocolType", str("HTTP")),
			kv("Tags", tagMap(a.Tags)),
		))
	})
	rs.add(idIntegration, func() *yaml.Node {
		return resource("AWS::ApiGatewayV2::Integration", mapping(
			kv("ApiId", ref(a.ID)),
			kv("IntegrationType", str("AWS_PROXY")),
			kv("IntegrationUri", r.value(a.Function)),
			kv("PayloadFormatVersion", str("2.0")),
			kv("TimeoutInMillis", num(httpIntegrationMillis)),
		))
	})
	var authorizer *yaml.Node
	if az := a.Authorizer; az != nil {
		rs.add(az.ID, func() *yaml.Node { return httpAuthorizer(r, a) })
		authorizer = ref(az.ID)
	}
	rs.add(idRouteQueries, func() *yaml.Node {
		return httpRoute(a.ID, "POST /queries/{name}", authorizationType(a.Auth), authorizer, scopes(a))
	})
	rs.add(idRouteHealth, func() *yaml.Node { return httpRoute(a.ID, routeHealth, "NONE", nil, nil) })
	health := a.Stage.HealthThrottle
	// A stage's route settings fail while their route doesn't exist.
	rs.add(a.Stage.ID, func() *yaml.Node {
		return resource("AWS::ApiGatewayV2::Stage", mapping(
			kv("ApiId", ref(a.ID)),
			kv("StageName", str(a.Stage.Name)),
			kv("AutoDeploy", boolean(true)),
			kv("AccessLogSettings", mapping(kv("DestinationArn", r.value(a.Stage.LogGroup)), kv("Format", str(httpAccessLog)))),
			kv("RouteSettings", mapping(kv(routeHealth, mapping(
				kv("ThrottlingBurstLimit", num(health.Burst)),
				kv("ThrottlingRateLimit", num(health.Rate)),
			)))),
			kv("Tags", tagMap(a.Tags)),
		), dependsOn(idRouteHealth))
	})
}

// httpRoute returns the route key of the API with ID api, which sends requests to the
// integration once auth and, when not nil, authorizer and scopes admit them.
func httpRoute(api, key, auth string, authorizer, scopes *yaml.Node) *yaml.Node {
	return resource("AWS::ApiGatewayV2::Route", mapping(
		kv("ApiId", ref(api)),
		kv("RouteKey", str(key)),
		kv("AuthorizationType", str(auth)),
		kv("AuthorizerId", authorizer),
		kv("AuthorizationScopes", scopes),
		kv("Target", intrinsic("Sub", str("integrations/${"+idIntegration+"}"))),
	))
}

func httpAuthorizer(r *resolver, a infra.API) *yaml.Node {
	az := a.Authorizer
	props := []pair{
		kv("ApiId", ref(a.ID)),
		kv("Name", str(az.Name)),
		kv("IdentitySource", seq(str("$request.header."+authorizationHeader))),
	}
	if a.Auth == infra.APIAuthJWT {
		props = append(props,
			kv("AuthorizerType", str("JWT")),
			kv("JwtConfiguration", mapping(kv("Issuer", str(az.Issuer)), kv("Audience", strs(az.Audience)))),
		)
	} else {
		props = append(props,
			kv("AuthorizerType", str("REQUEST")),
			kv("AuthorizerUri", invocationURI(r, az.Function)),
			kv("AuthorizerPayloadFormatVersion", str("2.0")),
			kv("EnableSimpleResponses", boolean(true)),
			kv("AuthorizerResultTtlInSeconds", num(az.TTLSeconds)),
		)
	}
	return resource("AWS::ApiGatewayV2::Authorizer", mapping(props...))
}

// addRESTAPI adds a REST API: the resources of the two routes, a proxy method on each, the
// authorizer, a deployment of them, and the stage that serves the deployment. CloudFormation
// never deploys a REST API again by itself, so the deployment's ID ends in a hash of what it
// holds: a change makes a new deployment, which the stage then serves.
func addRESTAPI(rs *resources, r *resolver, a infra.API) {
	rs.add(a.ID, func() *yaml.Node {
		return resource("AWS::ApiGateway::RestApi", mapping(
			kv("Name", r.value(a.Name)),
			kv("EndpointConfiguration", mapping(kv("Types", seq(str("REGIONAL"))))),
			kv("ApiKeySourceType", str("HEADER")),
			kv("Tags", tags(a.Tags)),
		))
	})
	var deployed []pair
	add := func(id string, n *yaml.Node) {
		deployed = append(deployed, kv(id, n))
		rs.add(id, func() *yaml.Node { return n })
	}
	var authorizer *yaml.Node
	if az := a.Authorizer; az != nil {
		add(az.ID, restAuthorizer(r, a))
		authorizer = ref(az.ID)
	}
	root := intrinsic("GetAtt", str(a.ID+".RootResourceId"))
	add(idResourceQueries, restResource(a.ID, root, "queries"))
	add(idResourceQuery, restResource(a.ID, ref(idResourceQueries), "{name}"))
	add(idResourceHealth, restResource(a.ID, root, "health"))
	add(idMethodQuery, resource("AWS::ApiGateway::Method", mapping(
		kv("RestApiId", ref(a.ID)),
		kv("ResourceId", ref(idResourceQuery)),
		kv("HttpMethod", str("POST")),
		kv("AuthorizationType", str(authorizationType(a.Auth))),
		kv("AuthorizerId", authorizer),
		kv("AuthorizationScopes", scopes(a)),
		kv("ApiKeyRequired", boolean(a.Auth == infra.APIAuthAPIKey)),
		kv("Integration", restIntegration(r, a.Function)),
	)))
	add(idMethodHealth, resource("AWS::ApiGateway::Method", mapping(
		kv("RestApiId", ref(a.ID)),
		kv("ResourceId", ref(idResourceHealth)),
		kv("HttpMethod", str("GET")),
		kv("AuthorizationType", str("NONE")),
		kv("ApiKeyRequired", boolean(false)),
		kv("Integration", restIntegration(r, a.Function)),
	), checkovSkip("CKV_AWS_59", "GET /health is public by design: Route 53 health checks can't sign requests or send keys.")))
	deployment := idDeployment + deploymentHash(r, deployed)
	rs.add(deployment, func() *yaml.Node {
		return resource("AWS::ApiGateway::Deployment", mapping(kv("RestApiId", ref(a.ID))), dependsOn(idMethodHealth, idMethodQuery))
	})
	rs.add(a.Stage.ID, func() *yaml.Node { return restStage(r, a, deployment) })
}

func restAuthorizer(r *resolver, a infra.API) *yaml.Node {
	az := a.Authorizer
	props := []pair{
		kv("RestApiId", ref(a.ID)),
		kv("Name", str(az.Name)),
		kv("IdentitySource", str("method.request.header."+authorizationHeader)),
	}
	if a.Auth == infra.APIAuthCognito {
		var clients *yaml.Node
		if len(az.Clients) > 0 {
			// API Gateway matches a Cognito ID token's aud against it.
			quoted := make([]string, len(az.Clients))
			for i, c := range az.Clients {
				quoted[i] = regexp.QuoteMeta(c)
			}
			clients = str("^(" + strings.Join(quoted, "|") + ")$")
		}
		props = append(props,
			kv("Type", str("COGNITO_USER_POOLS")),
			kv("ProviderARNs", seq(str(az.UserPool))),
			kv("IdentityValidationExpression", clients),
		)
	} else {
		props = append(props,
			kv("Type", str("REQUEST")),
			kv("AuthorizerUri", invocationURI(r, az.Function)),
			kv("AuthorizerResultTtlInSeconds", num(az.TTLSeconds)),
		)
	}
	return resource("AWS::ApiGateway::Authorizer", mapping(props...))
}

func restResource(api string, parent *yaml.Node, part string) *yaml.Node {
	return resource("AWS::ApiGateway::Resource", mapping(
		kv("RestApiId", ref(api)),
		kv("ParentId", parent),
		kv("PathPart", str(part)),
	))
}

// restIntegration returns a method's proxy integration with the function of ARN fn.
func restIntegration(r *resolver, fn infra.Text) *yaml.Node {
	return mapping(
		kv("Type", str("AWS_PROXY")),
		kv("IntegrationHttpMethod", str("POST")),
		kv("Uri", invocationURI(r, fn)),
	)
}

// restStage returns a REST API's stage, which serves deployment. Its method settings
// throttle GET /health; ~1 is the "/" of the resource path.
func restStage(r *resolver, a infra.API, deployment string) *yaml.Node {
	t := a.Stage.HealthThrottle
	health := mapping(
		kv("ResourcePath", str("/~1health")),
		kv("HttpMethod", str("GET")),
		kv("ThrottlingBurstLimit", num(t.Burst)),
		kv("ThrottlingRateLimit", num(t.Rate)),
	)
	return resource("AWS::ApiGateway::Stage", mapping(
		kv("RestApiId", ref(a.ID)),
		kv("DeploymentId", ref(deployment)),
		kv("StageName", str(a.Stage.Name)),
		kv("TracingEnabled", boolean(true)),
		kv("AccessLogSetting", mapping(kv("DestinationArn", r.value(a.Stage.LogGroup)), kv("Format", str(restAccessLog)))),
		kv("MethodSettings", seq(health)),
		kv("Tags", tags(a.Tags)),
	), checkovSkip("CKV_AWS_120", "No cache: it is billed by the hour, and would answer with rows older than the view's."))
}

// deploymentHash returns the hex digits that end the ID of the deployment of what the nodes
// render.
func deploymentHash(r *resolver, nodes []pair) string {
	data, err := dump(&yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{mapping(nodes...)}})
	if err != nil {
		r.fail(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])[:deploymentHashDigits]
}

func usagePlan(r *resolver, u infra.UsagePlan) *yaml.Node {
	var limits, quota *yaml.Node
	if u.Throttle != (infra.Throttle{}) {
		limits = mapping(kv("BurstLimit", positive(u.Throttle.Burst)), kv("RateLimit", positive(u.Throttle.Rate)))
	}
	if u.QuotaPerDay > 0 {
		quota = mapping(kv("Limit", num(u.QuotaPerDay)), kv("Period", str("DAY")))
	}
	return resource("AWS::ApiGateway::UsagePlan", mapping(
		kv("UsagePlanName", r.value(u.Name)),
		kv("ApiStages", seq(mapping(kv("ApiId", r.value(u.API)), kv("Stage", r.value(u.Stage))))),
		kv("Throttle", limits),
		kv("Quota", quota),
		kv("Tags", tags(u.Tags)),
	))
}

func certificate(r *resolver, c infra.Certificate) *yaml.Node {
	return resource("AWS::CertificateManager::Certificate", mapping(
		kv("DomainName", r.value(c.Domain)),
		kv("ValidationMethod", str("DNS")),
		kv("DomainValidationOptions", seq(mapping(kv("DomainName", r.value(c.Domain)), kv("HostedZoneId", r.value(c.HostedZone))))),
		kv("Tags", tags(c.Tags)),
	))
}

// addDomain adds a custom domain and its mapping to the API's stage, which a REST API's too
// takes from API Gateway v2.
func addDomain(rs *resources, r *resolver, d infra.Domain) {
	rs.add(d.ID, func() *yaml.Node {
		return resource("AWS::ApiGatewayV2::DomainName", mapping(
			kv("DomainName", r.value(d.Name)),
			kv("DomainNameConfigurations", seq(mapping(
				kv("CertificateArn", r.value(d.Certificate)),
				kv("EndpointType", str("REGIONAL")),
				kv("SecurityPolicy", str("TLS_1_2")),
			))),
			kv("Tags", tagMap(d.Tags)),
		))
	})
	rs.add(idMapping, func() *yaml.Node {
		return resource("AWS::ApiGatewayV2::ApiMapping", mapping(
			kv("ApiId", r.value(d.API)),
			kv("DomainName", ref(d.ID)),
			kv("Stage", r.value(d.Stage)),
		))
	})
}

func schedule(r *resolver, s infra.Schedule) *yaml.Node {
	return resource("AWS::Events::Rule", mapping(
		kv("ScheduleExpression", str("rate(1 minute)")),
		kv("State", str("ENABLED")),
		kv("Targets", seq(mapping(kv("Arn", r.value(s.Function)), kv("Id", str("Function"))))),
		kv("Tags", tags(s.Tags)),
	))
}

func healthCheck(r *resolver, h infra.HealthCheck) *yaml.Node {
	return resource("AWS::Route53::HealthCheck", mapping(
		kv("HealthCheckConfig", mapping(
			kv("Type", str("CLOUDWATCH_METRIC")),
			kv("AlarmIdentifier", mapping(kv("Name", r.value(h.Alarm)), kv("Region", ref("AWS::Region")))),
			kv("InsufficientDataHealthStatus", str("Healthy")),
		)),
		kv("HealthCheckTags", tags(h.Tags)),
	))
}

func latencyRecord(r *resolver, l infra.LatencyRecord) *yaml.Node {
	return resource("AWS::Route53::RecordSet", mapping(
		kv("HostedZoneId", r.value(l.HostedZone)),
		kv("Name", r.value(l.Name)),
		kv("Type", str("A")),
		kv("SetIdentifier", ref("AWS::Region")),
		kv("Region", ref("AWS::Region")),
		kv("HealthCheckId", r.value(l.HealthCheck)),
		kv("AliasTarget", mapping(
			kv("DNSName", r.value(l.Target)),
			kv("HostedZoneId", r.value(l.TargetZone)),
			kv("EvaluateTargetHealth", boolean(false)),
		)),
	))
}

// authorizationType returns API Gateway's authorization type of the queries route of an API
// with auth a.
func authorizationType(a infra.APIAuth) string {
	switch a {
	case infra.APIAuthIAM:
		return "AWS_IAM"
	case infra.APIAuthJWT:
		return "JWT"
	case infra.APIAuthLambda:
		return "CUSTOM"
	case infra.APIAuthCognito:
		return "COGNITO_USER_POOLS"
	case infra.APIAuthAPIKey: // the method requires the key instead
	}
	return "NONE"
}

// invocationURI returns the URI by which API Gateway invokes the function of ARN fn.
func invocationURI(r *resolver, fn infra.Text) *yaml.Node {
	return r.value("arn:${" + infra.PlaceholderPartition + "}:apigateway:${" + infra.PlaceholderRegion +
		"}:lambda:path/2015-03-31/functions/" + fn + "/invocations")
}

func ref(id string) *yaml.Node {
	return intrinsic("Ref", str(id))
}

func strs(ss []string) *yaml.Node {
	n := seq()
	for _, s := range ss {
		n.Content = append(n.Content, str(s))
	}
	return n
}

// scopes returns the AuthorizationScopes of api's queries route, or nothing without scopes.
func scopes(a infra.API) *yaml.Node {
	if len(a.Scopes) == 0 {
		return nil
	}
	return strs(a.Scopes)
}

// positive returns n, or nothing when it is 0.
func positive(n int) *yaml.Node {
	if n == 0 {
		return nil
	}
	return num(n)
}

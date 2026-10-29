// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation_test

import (
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/infra/cloudformation"
)

// The invariants of query.yaml (ADR 0018, ADR 0020) hold in every golden case and the
// example's: the query role writes nothing, names no resource by a wildcard, and queries the
// meta table's view partition only; on API doors, GET /health has no auth and a stage
// throttle; every health check follows an alarm, so no one can flood what it calls, and
// stays healthy without data; and a permission for anyone to invoke the function holds only
// through a function URL. Each invariant applies somewhere in them.
func TestQueryInvariants(t *testing.T) {
	t.Parallel()
	templates := goldenTemplates(t, cloudformation.QueryTemplate)
	example, err := os.ReadFile("../../../examples/analytics/infra/dynaflux/" + cloudformation.QueryTemplate)
	if err != nil {
		t.Fatal(err)
	}
	templates["example"] = example
	applied := map[string]int{}
	for name, data := range templates {
		resources := templateResources(t, name, data)
		for i := 0; i+1 < len(resources.Content); i += 2 {
			id, r := resources.Content[i].Value, resources.Content[i+1]
			for _, broken := range queryInvariants(resources, value(r, "Type").Value, value(r, "Properties"), applied) {
				t.Errorf("%s: %s %s", name, id, broken)
			}
		}
	}
	for _, inv := range []string{"role", "meta", "health route", "stage throttle", "health check", "public invoke"} {
		if applied[inv] == 0 {
			t.Errorf("no query.yaml has a resource that the %s invariant applies to", inv)
		}
	}
}

// queryInvariants returns how a resource of type typ with props, in a query.yaml with
// resources, breaks an invariant, and counts in applied each invariant that applies to it.
func queryInvariants(resources *yaml.Node, typ string, props *yaml.Node, applied map[string]int) []string {
	readOnly := []string{"dynamodb:BatchGetItem", "dynamodb:GetItem", "dynamodb:Query", "logs:CreateLogStream", "logs:PutLogEvents"}
	var broken []string
	check := func(inv string, ok bool, problem string) {
		applied[inv]++
		if !ok {
			broken = append(broken, problem)
		}
	}
	switch typ {
	case "AWS::IAM::Role":
		for _, st := range value(value(value(props, "Policies").Content[0], "PolicyDocument"), "Statement").Content {
			actions, names := scalars(value(st, "Action")), scalars(value(st, "Resource"))
			check("role", !slices.ContainsFunc(actions, func(a string) bool { return !slices.Contains(readOnly, a) }),
				"allows "+strings.Join(actions, ", ")+", not only reads")
			check("role", !slices.ContainsFunc(names, func(n string) bool { return strings.Contains(n, "*") }),
				"names "+strings.Join(names, ", ")+", with a wildcard")
			if slices.ContainsFunc(names, func(n string) bool { return strings.HasSuffix(n, "-meta") }) {
				keys := value(value(value(st, "Condition"), "ForAllValues:StringEquals"), "dynamodb:LeadingKeys")
				check("meta", slices.Equal(scalars(keys), []string{"view"}), "reads the meta table outside its view partition")
			}
		}
	case "AWS::ApiGatewayV2::Route":
		if value(props, "RouteKey").Value == "GET /health" {
			check("health route", value(props, "AuthorizationType").Value == "NONE", "authenticates GET /health")
		}
	case "AWS::ApiGateway::Method":
		parent := value(value(value(resources, value(props, "ResourceId").Value), "Properties"), "PathPart").Value
		if parent == "health" {
			check("health route", value(props, "AuthorizationType").Value == "NONE" && value(props, "ApiKeyRequired").Value != "true",
				"authenticates GET /health")
		}
	case "AWS::ApiGatewayV2::Stage":
		check("stage throttle", throttled(value(value(props, "RouteSettings"), "GET /health")), "doesn't throttle GET /health")
	case "AWS::ApiGateway::Stage":
		i := slices.IndexFunc(value(props, "MethodSettings").Content, func(s *yaml.Node) bool {
			return value(s, "ResourcePath").Value == "/~1health" && value(s, "HttpMethod").Value == "GET"
		})
		check("stage throttle", i >= 0 && throttled(value(props, "MethodSettings").Content[i]), "doesn't throttle GET /health")
	case "AWS::Route53::HealthCheck":
		c := value(props, "HealthCheckConfig")
		check("health check", value(c, "Type").Value == "CLOUDWATCH_METRIC" && value(c, "InsufficientDataHealthStatus").Value == "Healthy",
			"doesn't follow an alarm, or is unhealthy without data")
	case "AWS::Lambda::Permission":
		if value(props, "Principal").Value == "*" && value(props, "Action").Value == "lambda:InvokeFunction" {
			check("public invoke", value(props, "InvokedViaFunctionUrl").Value == "true", "lets anyone invoke the function directly")
		}
	}
	return broken
}

// throttled reports whether settings limit both the rate and the bursts.
func throttled(settings *yaml.Node) bool {
	rate, rateErr := strconv.Atoi(value(settings, "ThrottlingRateLimit").Value)
	burst, burstErr := strconv.Atoi(value(settings, "ThrottlingBurstLimit").Value)
	return rateErr == nil && burstErr == nil && rate > 0 && burst > 0
}

// scalars returns the values of a scalar or a sequence of scalars.
func scalars(n *yaml.Node) []string {
	if n.Kind == yaml.ScalarNode {
		return []string{n.Value}
	}
	var vs []string
	for _, c := range n.Content {
		vs = append(vs, c.Value)
	}
	return vs
}

// A stage serves a REST API's changes only through a new deployment, which CloudFormation
// never makes by itself: the deployment's ID changes with the resources, methods and
// authorizer it deploys, and only with them.
func TestRESTDeploymentID(t *testing.T) {
	t.Parallel()
	deployment := func(change func(*infra.API)) string {
		t.Helper()
		a := infra.API{
			Common: infra.Common{ID: "Api"}, Name: "api", Protocol: infra.APIProtocolREST, Function: "${ref:Query.arn}",
			Auth: infra.APIAuthCognito, Authorizer: &infra.Authorizer{ID: "Authorizer", Name: "cognito", UserPool: "pool-a"},
			Stage: infra.Stage{ID: "Stage", Name: "api", LogGroup: "logs-a", HealthThrottle: infra.Throttle{Rate: 5, Burst: 10}},
		}
		change(&a)
		m := &infra.Manifest{Store: "analytics", Regions: []string{"us-east-1"}, Query: infra.Scope{
			Functions: []infra.Function{{Common: infra.Common{ID: "Query"}, Name: "query"}},
			APIs:      []infra.API{a},
		}}
		files, err := cloudformation.Render(m, "v1.2.3")
		if err != nil {
			t.Fatal(err)
		}
		ids := regexp.MustCompile(`(?m)^  (Deployment\w*):$`).FindAllStringSubmatch(string(files[cloudformation.QueryTemplate]), -1)
		if len(ids) != 1 {
			t.Fatalf("query.yaml has deployments %q, want one", ids)
		}
		return ids[0][1]
	}
	first := deployment(func(*infra.API) {})
	if !regexp.MustCompile(`^Deployment[0-9a-f]{8}$`).MatchString(first) {
		t.Fatalf("deployment ID %s isn't Deployment and 8 hex digits", first)
	}
	tests := []struct {
		name   string
		change func(*infra.API)
		same   bool
	}{
		{"another user pool", func(a *infra.API) { a.Authorizer.UserPool = "pool-b" }, false},
		{"scopes", func(a *infra.API) { a.Scopes = []string{"dynaflux/query"} }, false},
		{"IAM auth", func(a *infra.API) { a.Auth, a.Authorizer = infra.APIAuthIAM, nil }, false},
		{"another function", func(a *infra.API) { a.Function = "${ref:Query.name}" }, false},
		{"another log group", func(a *infra.API) { a.Stage.LogGroup = "logs-b" }, true},
		{"another health throttle", func(a *infra.API) { a.Stage.HealthThrottle.Rate = 1 }, true},
	}
	for _, tt := range tests {
		if got := deployment(tt.change); (got == first) != tt.same {
			t.Errorf("%s: deployment %s, and %s before; want the same ID: %t", tt.name, got, first, tt.same)
		}
	}
}

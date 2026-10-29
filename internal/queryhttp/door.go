// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-lambda-go/events"
)

// door is the way a request came in.
type door int

const (
	doorDirect door = iota
	doorFunctionURL
	doorHTTPAPI
	doorRESTAPI
	// doorSchedule is the EventBridge schedule of the custom domain's health check, whose
	// event asks for the readiness metric.
	doorSchedule
)

func (d door) String() string {
	return [...]string{"direct", "function_url", "http_api", "rest_api", "schedule"}[d]
}

// request is an event, decoded.
type request struct {
	door door
	// method and path are an HTTP request's route, with the query's name in place of
	// {name}: the function URL's own path, or the API route's.
	method, path string
	query        string // a direct invoke's query
	body         []byte // the parameters object
	bad          string // why the event can't be served, before routing; "" when it can
}

// probe holds the top-level keys that tell the doors apart. API Gateway and function URLs
// always send requestContext; direct invoke has no reason to. A schedule's event has source
// and detail-type.
type probe struct {
	Source         string          `json:"source"`
	DetailType     string          `json:"detail-type"`
	Version        string          `json:"version"`
	HTTPMethod     json.RawMessage `json:"httpMethod"`
	Resource       json.RawMessage `json:"resource"`
	RequestContext *struct {
		HTTP json.RawMessage `json:"http"`
	} `json:"requestContext"`
}

// parse tells the door of payload by its top-level keys, never by authorizer content, and
// decodes it. An HTTP-shaped event reaches the same queries as a direct one, and a schedule's
// event carries nothing that the handler trusts, so a caller that forges one gains nothing.
func parse(payload []byte) (request, error) {
	p, isAPI := apiEvent(payload)
	switch {
	case !isAPI && p.Source == "aws.events" && p.DetailType == "Scheduled Event":
		return request{door: doorSchedule}, nil
	case !isAPI:
		return parseDirect(payload), nil
	}
	switch {
	case p.Version == "2.0" && p.RequestContext.HTTP != nil:
		var ev events.APIGatewayV2HTTPRequest
		if err := json.Unmarshal(payload, &ev); err != nil {
			return request{}, fmt.Errorf("queryhttp: decode a payload 2.0 event: %w", err)
		}
		r := request{door: doorHTTPAPI}
		if method, path, ok := strings.Cut(ev.RouteKey, " "); ok {
			r.method, r.path = method, strings.Replace(path, "{name}", ev.PathParameters["name"], 1)
		} else {
			// A function URL's event has the route $default, or none.
			r.door, r.method, r.path = doorFunctionURL, ev.RequestContext.HTTP.Method, ev.RawPath
		}
		r.body, r.bad = decodeBody(ev.Body, ev.IsBase64Encoded)
		return r, nil
	case p.HTTPMethod != nil && p.Resource != nil:
		var ev events.APIGatewayProxyRequest
		if err := json.Unmarshal(payload, &ev); err != nil {
			return request{}, fmt.Errorf("queryhttp: decode a REST API event: %w", err)
		}
		r := request{door: doorRESTAPI, method: ev.HTTPMethod, path: strings.Replace(ev.Resource, "{name}", ev.PathParameters["name"], 1)}
		r.body, r.bad = decodeBody(ev.Body, ev.IsBase64Encoded)
		return r, nil
	}
	return request{}, errors.New("queryhttp: the event is from neither a function URL, an API Gateway HTTP or REST API, " +
		"nor a direct invoke: the function serves only the doors that dynaflux generate configures")
}

// apiEvent returns the keys of payload that tell the doors apart, and whether it is an
// object with requestContext.
func apiEvent(payload []byte) (probe, bool) {
	var p probe
	err := json.Unmarshal(payload, &p)
	return p, err == nil && p.RequestContext != nil
}

// parseDirect decodes a direct invoke's payload, {"query": <name>, "params": {…}}.
func parseDirect(payload []byte) request {
	r := request{door: doorDirect}
	raw, err := object(payload, []string{"query", "params"})
	switch u, unknown := errors.AsType[*unknownKeyError](err); {
	case unknown:
		r.bad = fmt.Sprintf(`unknown key %q: a direct invoke has "query" and "params"`, u.key)
	case err != nil:
		r.bad = `the payload must be one JSON object, {"query": <name>, "params": {…}}: ` + err.Error()
	case raw[0] != nil && json.Unmarshal(raw[0], &r.query) != nil:
		r.bad = `"query" must be a string`
	case r.query == "":
		r.bad = `the payload has no "query"`
	}
	r.body = raw[1]
	return r
}

func decodeBody(body string, b64 bool) ([]byte, string) {
	if !b64 {
		return []byte(body), ""
	}
	data, err := base64.StdEncoding.DecodeString(body)
	if err != nil {
		return nil, "the body isn't valid base64: " + err.Error()
	}
	return data, ""
}

// encode returns the payload that answers r through d: a direct invoke gets the body itself.
func (h *Handler) encode(ctx context.Context, d door, r reply) ([]byte, error) {
	body := r.body
	if body == nil {
		body = errorBody(r, requestID(ctx))
	}
	headers := map[string]string{
		"content-type":      "application/json",
		"cache-control":     "no-store",
		"x-dynaflux-region": h.region,
	}
	if r.code == codeThrottled {
		headers["retry-after"] = "1"
	}
	if r.allow != "" {
		headers["allow"] = r.allow
	}
	var v any
	switch d {
	case doorDirect, doorSchedule:
		return body, nil
	case doorFunctionURL:
		v = events.LambdaFunctionURLResponse{StatusCode: r.status, Headers: headers, Body: string(body)}
	case doorHTTPAPI:
		v = events.APIGatewayV2HTTPResponse{StatusCode: r.status, Headers: headers, Body: string(body)}
	case doorRESTAPI:
		v = events.APIGatewayProxyResponse{StatusCode: r.status, Headers: headers, Body: string(body)}
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false) // an HTML escape would make each <, > and & of the body six bytes
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("queryhttp: encode the response: %w", err)
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

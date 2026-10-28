// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"math"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

// API Gateway's limits on a JWT authorizer.
const (
	maxAudience = 50
	maxScopes   = 10
)

var (
	functionNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	userPoolARNPattern  = regexp.MustCompile(`^arn:aws[a-z-]*:cognito-idp:[a-z0-9-]+:[0-9]{12}:userpool/[\w-]+_[0-9A-Za-z]+$`)
)

const hintIssuer = "use the issuer's https:// URL, such as https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc"

func (d *decoder) http(top object, c *Config) {
	h := &c.HTTP
	*h = HTTP{Frontend: FrontendFunctionURL, Auth: AuthIAM}
	o, ok := d.object("http", top.vals["http"], top.at("http"), "frontend", "auth", "allow_public",
		"jwt", "authorizer", "cognito", "usage_plan", "max_concurrency", "domain")
	if !ok {
		return
	}
	if top.has("http") {
		h.Pos = top.at("http")
	}
	frontendOK := enum(d, o, "frontend", &h.Frontend, FrontendFunctionURL, FrontendHTTPAPI, FrontendRESTAPI) ||
		!o.has("frontend")
	authOK := enum(d, o, "auth", &h.Auth, AuthIAM, AuthNone, AuthJWT, AuthLambda, AuthAPIKey, AuthCognito) ||
		!o.has("auth")
	d.boolean(o, "allow_public", &h.AllowPublic)
	d.integer(o, "max_concurrency", &h.MaxConcurrency, bounds{1, math.MaxInt,
		"it reserves that many of the account's concurrent executions in each region for the query function"})
	// The selected auth's settings are required only when the front door supports it.
	selected := Auth("")
	if frontendOK && authOK && slices.Contains(authsFor(h.Frontend), h.Auth) {
		selected = h.Auth
	}
	h.JWT = d.jwt(o, selected == AuthJWT)
	h.Authorizer = d.authorizer(o, selected == AuthLambda)
	h.Cognito = d.cognito(o, selected == AuthCognito)
	h.UsagePlan = d.usagePlan(o, selected == AuthAPIKey)
	h.Domain = d.domain(o)
	if frontendOK && authOK {
		d.auth(o, h)
	}
	if authOK {
		d.unusedSettings(o, h.Auth)
	}
	if authOK && h.Auth != AuthNone && h.AllowPublic {
		d.warnf(o.at("allow_public"), "DF6604", "remove allow_public",
			"http.allow_public: true has no effect, because http.auth is %s, not none", h.Auth)
	}
	switch {
	case !frontendOK || !o.has("domain"):
	case h.Frontend == FrontendFunctionURL:
		d.errorf(o.at("domain"), "DF6605",
			"function URLs have no custom domain option; use frontend: http_api for one hostname across regions",
			"http.domain is set, but http.frontend is function_url")
	case authOK && h.Auth == AuthIAM:
		d.errorf(o.at("domain"), "DF6608",
			"use another auth, such as jwt or lambda, or remove http.domain and call each region's own endpoint",
			"http.domain is set, but http.auth is iam: a SigV4 signature names one region, and latency routing can send "+
				"the request to another")
	}
}

// auth checks http.auth against the front door, allow_public and max_concurrency.
func (d *decoder) auth(o object, h *HTTP) {
	supported := authsFor(h.Frontend)
	switch {
	case !slices.Contains(supported, h.Auth):
		d.errorf(o.at("auth"), "DF6601", fmt.Sprintf("%s supports auth %s", h.Frontend, orList(supported)),
			"http.auth is %s, which http.frontend %s doesn't support", h.Auth, h.Frontend)
	case h.Auth == AuthNone && !h.AllowPublic:
		d.errorf(o.at("auth"), "DF6602",
			"set allow_public: true to confirm that anyone may call the exposed queries, or use auth: iam",
			"http.auth is none, but http.allow_public isn't true")
	case h.Auth == AuthNone:
		d.warnf(o.at("auth"), "DF6603", "use auth: iam unless the queries must be public",
			"http.auth is none: anyone with the URL can call every query annotated -- expose: http")
		if !o.has("max_concurrency") {
			d.warnf(o.at("auth"), "DF6615", "set http.max_concurrency, such as 50, to cap the query function's concurrency",
				"http.auth is none without http.max_concurrency: anyone with the URL can take the account's concurrent "+
					"executions in the region, which the projectors share")
		}
	case h.Auth == AuthAPIKey:
		d.warnf(o.at("auth"), "DF6607", "use iam, lambda or cognito to control access",
			"http.auth is api_key; AWS advises against API keys for authentication or authorization")
	}
}

func authsFor(f Frontend) []Auth {
	switch f {
	case FrontendFunctionURL:
		return []Auth{AuthIAM, AuthNone}
	case FrontendHTTPAPI:
		return []Auth{AuthIAM, AuthJWT, AuthLambda}
	case FrontendRESTAPI:
		return []Auth{AuthIAM, AuthAPIKey, AuthLambda, AuthCognito}
	}
	return nil
}

// unusedSettings warns about the settings of each auth that auth isn't (DF6609).
func (d *decoder) unusedSettings(o object, auth Auth) {
	for _, s := range []struct {
		key  string
		auth Auth
	}{{"jwt", AuthJWT}, {"authorizer", AuthLambda}, {"cognito", AuthCognito}, {"usage_plan", AuthAPIKey}} {
		if o.has(s.key) && auth != s.auth {
			d.warnf(o.at(s.key), "DF6609", "remove http."+s.key,
				"http.%s has no effect, because http.auth is %s, not %s", s.key, auth, s.auth)
		}
	}
}

// settings reads the mapping at k, with the given keys. It reports DF6008 when required and
// k isn't set, and returns false when k isn't a mapping that is set.
func (d *decoder) settings(o object, k, example string, required bool, keys ...string) (object, bool) {
	if required {
		d.require(o, k, example)
	}
	if !o.has(k) {
		return object{}, false
	}
	return d.object(o.key(k), o.vals[k], o.at(k), keys...)
}

func (d *decoder) jwt(o object, selected bool) *JWT {
	m, ok := d.settings(o, "jwt", "{issuer: https://issuer.example.com, audience: [app-client-id]}", selected,
		"issuer", "audience", "scopes")
	if !ok {
		return nil
	}
	j := &JWT{}
	if selected {
		d.require(m, "issuer", "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc")
		d.require(m, "audience", "[app-client-id]")
	}
	if s, set := d.str(m, "issuer", hintIssuer); set {
		if u, err := url.Parse(s); err == nil && strings.HasPrefix(s, "https://") && u.Host != "" {
			j.Issuer = s
		} else {
			d.errorf(m.at("issuer"), "DF6610", hintIssuer, "http.jwt.issuer is %q, which isn't an https:// URL", s)
		}
	}
	j.Audience = d.stringList(m, "audience", "[app-client-id]")
	j.Scopes = d.stringList(m, "scopes", "[dynaflux/query]")
	d.atMost(m, "audience", len(j.Audience), maxAudience)
	d.atMost(m, "scopes", len(j.Scopes), maxScopes)
	if selected && !m.has("scopes") {
		d.warnf(m.pos, "DF6613", "set http.jwt.scopes, and give callers access tokens that carry one",
			"http.jwt has no scopes: any ID or access token for the audience passes, since API Gateway can't tell them apart")
	}
	return j
}

func (d *decoder) authorizer(o object, selected bool) *Authorizer {
	m, ok := d.settings(o, "authorizer", "{function: flux-authorizer}", selected, "function")
	if !ok {
		return nil
	}
	a := &Authorizer{}
	if selected {
		d.require(m, "function", "flux-authorizer")
	}
	if s, set := d.str(m, "function", "for example function: flux-authorizer"); set {
		if functionNamePattern.MatchString(s) {
			a.Function = s
		} else {
			d.errorf(m.at("function"), "DF6611", "use the name of a function in every region: 1 to 64 letters, digits, - and _",
				"http.authorizer.function is %q, which isn't a Lambda function name", s)
		}
	}
	return a
}

func (d *decoder) cognito(o object, selected bool) *Cognito {
	const example = "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_abc"
	m, ok := d.settings(o, "cognito", "{user_pool_arn: "+example+"}", selected, "user_pool_arn", "scopes", "app_clients")
	if !ok {
		return nil
	}
	cg := &Cognito{}
	if selected {
		d.require(m, "user_pool_arn", example)
	}
	if s, set := d.str(m, "user_pool_arn", "for example user_pool_arn: "+example); set {
		if userPoolARNPattern.MatchString(s) {
			cg.UserPoolARN = s
		} else {
			d.errorf(m.at("user_pool_arn"), "DF6611", "use the ARN of a Cognito user pool, such as "+example,
				"http.cognito.user_pool_arn is %q, which isn't a user pool ARN", s)
		}
	}
	cg.Scopes = d.stringList(m, "scopes", "[dynaflux/query]")
	cg.AppClients = d.stringList(m, "app_clients", "[app-client-id]")
	switch {
	case !selected:
	case !m.has("scopes") && !m.has("app_clients"):
		d.warnf(m.pos, "DF6614", "set http.cognito.scopes or http.cognito.app_clients",
			"http.cognito has neither scopes nor app_clients: every user of the pool, from any app client, is authorized")
	case m.has("scopes") && m.has("app_clients"):
		// Unconfirmed on AWS (infra.md §16); the warning goes when the end-to-end suite settles it.
		d.warnf(m.pos, "DF6616", "set scopes or app_clients, not both: the resource server's app client settings "+
			"already decide which clients get a scope",
			"http.cognito has both scopes and app_clients: with scopes, API Gateway takes only access tokens, which "+
				"carry no aud for app_clients to match, so it may refuse every caller")
	}
	return cg
}

func (d *decoder) usagePlan(o object, selected bool) *UsagePlan {
	m, ok := d.settings(o, "usage_plan", "", false, "rate", "burst", "quota_per_day")
	if !ok {
		return nil
	}
	if selected && (m.has("rate") || m.has("burst")) {
		// API Gateway reads a missing limit as 0, and a burst or rate of 0 refuses requests.
		d.require(m, "rate", "100")
		d.require(m, "burst", "200")
	}
	p := &UsagePlan{}
	const hint = "API Gateway takes positive limits; leave a key out for no limit"
	d.integer(m, "rate", &p.Rate, bounds{1, math.MaxInt, hint})
	d.integer(m, "burst", &p.Burst, bounds{1, math.MaxInt, hint})
	d.integer(m, "quota_per_day", &p.QuotaPerDay, bounds{1, math.MaxInt, hint})
	return p
}

// atMost reports DF6610 when the list at k of http.jwt has n entries, more than limit.
func (d *decoder) atMost(m object, k string, n, limit int) {
	if n > limit {
		d.errorf(m.at(k), "DF6610", fmt.Sprintf("list at most %d", limit),
			"%s has %d entries; API Gateway takes at most %d", m.key(k), n, limit)
	}
}

// stringList returns the entries of the list at k, without repeats.
func (d *decoder) stringList(o object, k, example string) []string {
	var out []string
	seen := map[string]bool{}
	for _, e := range d.list(o, k, example, false) {
		if !d.repeated(seen, o.key(k), e.Value, e) {
			out = append(out, e.Value)
		}
	}
	return out
}

func (d *decoder) domain(o object) *Domain {
	if !o.has("domain") {
		return nil
	}
	m, ok := d.object("http.domain", o.vals["domain"], o.pos, "name", "routing")
	if !ok {
		return nil
	}
	dom := &Domain{Routing: RoutingLatency}
	d.require(m, "name", "flux.example.com")
	if s, set := d.str(m, "name", "for example name: flux.example.com"); set {
		if isDomainName(s) {
			dom.Name = s
		} else {
			d.errorf(m.at("name"), "DF6606", "use lowercase labels of letters, digits and -, such as flux.example.com",
				"http.domain.name is %q, which isn't a valid DNS name", s)
		}
	}
	enum(d, m, "routing", &dom.Routing, RoutingLatency)
	return dom
}

// isDomainName reports whether s is a lowercase DNS name of at least two labels.
func isDomainName(s string) bool {
	labels := strings.Split(s, ".")
	if len(s) > maxDomainName || len(labels) < 2 {
		return false
	}
	return !slices.ContainsFunc(labels, func(l string) bool {
		return l == "" || len(l) > maxDomainLabel || l[0] == '-' || l[len(l)-1] == '-' || !consists(l, lower+digits+"-")
	})
}

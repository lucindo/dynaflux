<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0018: One query handler serves every front door, from a third template deployed with the application

- Status: Accepted
- Amended by [ADR 0020](0020-health-checks-follow-a-readiness-alarm.md): the health checks
  follow a readiness alarm instead of calling `/health`, which anyone could flood.
- Date: 2026-10-27

## Context

Plan D2, D12, D17 and §13.2 promise named queries over HTTP for Python and other non-Go
clients: a Lambda function URL (default, `iam` or `none`), an API Gateway HTTP API (`iam`,
`jwt`, `lambda`) or REST API (`iam`, `api_key`, `lambda`, `cognito`), a custom domain with
Route 53 latency routing across regions, and direct Lambda invoke, which always works. Only
queries annotated `-- expose: http` are served over HTTP, and `auth: none` needs
`allow_public: true` ([config.md](../spec/config.md#http)). The JSON forms are fixed by
catalog requirement Q21: 64-bit integers and decimals as strings, RFC 3339 times with up to
6 fractional digits, `YYYY-MM-DD` dates. M5 built `internal/valuetext` for them.

Plan §13 places "the optional query Lambda" in `regional.yaml`. But `regional.yaml` also
holds the view tables, and a view change deploys its new version and the query function's
new catalog together. The engine reads the catalog's current version and refuses it until
it is `live` ([lifecycle.md §6](../spec/lifecycle.md#6-queries)), so every HTTP query over a
changed view would answer "not ready" for the length of its backfill: hours at the plan's
ceiling. lifecycle.md §2 already avoids this for applications by deploying them after the
backfill.

Other facts that shape the handler ([function URL auth][urlauth], [Lambda quotas][lq],
[HTTP API quotas][hq], [REST quotas][rq], [SigV4][sigv4]):

- A synchronous response is at most 6 MB, and the payload 2.0 envelope escapes the body.
- API Gateway gives up after 29 s (REST) or 30 s (HTTP API).
- A function URL with `AuthType: NONE` needs both `lambda:InvokeFunctionUrl` and
  `lambda:InvokeFunction` for `*`; without `lambda:InvokedViaFunctionUrl` on the second,
  anyone could invoke the function directly.
- A SigV4 signature names one region, and latency routing picks the region after the
  client signed.

## Decision

1. **A third template, `query.yaml`**, generated when a query is annotated
   `-- expose: http` and deployed in every region **with the application, after the
   backfill**. It holds the
   query function (`{prefix}{store}-query`, `DYNAFLUX_MODE=query`, the same binary), its
   role, log group and alarms, the front door `http` selects when a query is exposed, and
   one output, `QueryEndpoint` (and `QueryUsagePlanId` with API keys). The function's role
   reads the meta table's `view` partition only (`dynamodb:LeadingKeys`) and the tables its
   exposed queries read, by exact ARN; it writes nothing.
2. **One handler** (`internal/queryhttp`, a `lambda.Handler` that owns the response bytes)
   tells the events apart by their top-level keys: payload 2.0 (function URL, HTTP API),
   payload 1.0 (REST API), or Dynaflux's direct shape `{"query", "params"}`. An event with
   `requestContext` from no door it serves is a function error; any other payload is a
   direct invoke, and a malformed one answers 400. It never reads authorizer content.
3. **Routes:** `POST /queries/{name}` with the parameters object as the body, and
   `GET /health`. Every door, direct invoke included, serves the same exposed queries; an
   unknown or unexposed name is the same 404.
4. **Parameters are strict:** one JSON object, no duplicate, unknown, missing or `null`
   key, each value through `valuetext.ParseJSON`.
5. **Responses** have ClickHouse's `FORMAT JSON` shape: `{"status", "meta", "data", "rows"}`,
   with `meta` giving each column's SQL type and values in Q21's forms. A `:one` query
   without a row returns 200 with `rows: 0`. Bodies are byte-identical across doors.
6. **Request failures are responses, never function errors:** 400 `bad_request`,
   `invalid_params`, `too_many_keys`, `result_too_large` (over 6,291,456 bytes); 404
   `not_found`; 405; 503 `view_not_ready` and `throttled` (with `Retry-After`); 504
   `timeout` (the handler stops 1 s before Lambda's deadline; the function's timeout is a
   fixed 25 s); 500 `evaluation_error` and `internal`. Only a crash, a Lambda timeout or an
   unknown event shape is a function error, which the `Errors` alarm counts.
7. **Health:** 200 while the current version of every view an exposed query reads is
   `live` in the region, else 503 with only the status and code (version names go to the
   log). Unauthenticated on API doors and throttled at the stage (rate 5, burst 10; Route 53
   needs about one request every 2 seconds per check), so health checks take a region out
   of DNS while its views aren't ready and anonymous callers can't use it to drain the
   account's Lambda concurrency. On a function URL it follows the URL's `AuthType`.
8. **Doors.** Function URL: `AWS_IAM` or `NONE` (with `InvokedViaFunctionUrl` on the public
   `InvokeFunction` permission). HTTP API: one `AWS_PROXY` integration, payload 2.0, IAM
   routes, a JWT authorizer (a warning without `scopes`: API Gateway can't tell ID tokens
   from access tokens), or a `REQUEST` Lambda authorizer. REST API: `REGIONAL`, proxy
   methods, IAM, API keys with a usage plan (keys created outside the stack, generated by
   API Gateway in one region and imported into the others), a `REQUEST` Lambda authorizer,
   or a Cognito authorizer (`scopes` as method scopes, `app_clients` as the `aud`
   expression; a warning without both, since then any ID token of the pool passes); a
   deployment whose logical ID carries a hash of the methods. Custom domain (API doors):
   an ACM certificate validated in the given hosted zone with an alarm on its
   `DaysToExpiry`, a regional `AWS::ApiGatewayV2::DomainName`, an API mapping, an HTTPS
   health check of the region's own endpoint with `EnableSNI: true` (the shared
   execute-api host needs SNI, and no default is documented), and a latency alias record
   per region.
9. **`auth: iam` with `http.domain` is an error** (DF6608): use `jwt`, `lambda` or each
   region's own endpoint.
10. **A lagging query stack is the application's case.** `query.yaml` deploys with the
    application and lags `regional.yaml` as the application does, so `retire`'s warning to
    deploy the application first names `query.yaml` too. A lagging function answers 503 for
    a retired view. The function checks its `DYNAFLUX_CATALOG_HASH` against its binary at
    Init.

### Rejected

- **The query function in `regional.yaml`** (plan §13 as written): 503 during every view
  backfill.
- **An engine that falls back to an older live version:** queries compiled per version,
  and answers that differ by region.
- **Direct invoke serving every named query:** a third stack for stores whose only
  clients are Go applications, two exposure rules to test, and `lambda:InvokeFunction`
  (which function URL `iam` callers need anyway) reaching queries that aren't exposed.
- **`retire` reading the query function's catalog and refusing:** machinery for an
  availability failure that lifecycle.md already handles for applications with a warning.
- **A route per exposed query:** one or two resources per query and a REST redeploy per
  change, for per-query throttles nobody asked for.
- **404 for a `:one` without a row:** indistinguishable from API Gateway's own route 404.
- **Wait tokens and `AllowPartial` over HTTP in M6:** tokens are region-local, and a
  custom domain can route a request to another region than the append's.
- **CORS and OpenAPI in M6:** the clients the plan names are services; `meta` plus a wire
  format page ([http.md](../spec/http.md)) let a generic client decode any query. Plan §4 puts a
  generated Python client in v2.
- **API keys as template parameters:** secrets in parameters and CI logs, and one value
  can't span regions.

## Consequences

- Plan §13's two templates become three; the manifest gains a query scope; lifecycle.md §2
  deploys `query.yaml` with the application; deleting a store deletes `query.yaml` first.
- The handler runs in on-demand Lambda or Lambda Managed Instances: the engine is safe for
  concurrent use.
- A public function URL has no throttling and shares the account's regional concurrency
  with the projector; DF6603's hint points public endpoints at `http_api`. The optional
  `http.max_concurrency` reserves the query function's concurrency, and DF6615 warns when
  `auth: none` has none. It has no default: a reservation can fail deploys in accounts with
  a low concurrency quota ([infra.md §14](../spec/infra.md#14-deliberate-omissions)).
- The `/health` throttle is shared: anyone who floods a region's `/health` makes Route 53's
  checkers get `429` too, which takes the region out of the domain while the flood lasts
  (Route 53 answers with every region when all are out). End-to-end experiment Q1 decides
  whether that needs a health route that only Route 53 knows.
- Authentication admits callers but doesn't choose rows: any caller that passes `auth` reads
  every row an exposed query reaches ([http.md §8](../spec/http.md#8-authentication)).
- REST API access logs need the account's per-region API Gateway CloudWatch role; a
  `rest_api` deploy fails loudly where it isn't set (a documented prerequisite).
- Each regional query stack owns its certificate and latency record. Whether deleting one
  stack's certificate removes the validation record the others share is an end-to-end item;
  the `DaysToExpiry` alarm (below 45 days) catches a renewal that fails because of it.
- infra.md §14's reason for having no dead-letter queues changes: the query function is
  invoked synchronously.
- Unverified, for the end-to-end suite: each door's status for a crashed invocation, a
  Cognito pool in another region, that API Gateway refuses SigV4a.

[urlauth]: https://docs.aws.amazon.com/lambda/latest/dg/urls-auth.html
[lq]: https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html
[hq]: https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-quotas.html
[rq]: https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-execution-service-limits-table.html
[sigv4]: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-troubleshooting.html

<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# HTTP queries

This page is the wire format of the query function: how a client calls a named query
annotated `-- expose: http` over HTTP or by direct Lambda invoke, what it gets back, and
what each kind of authentication asks of the caller. Python and other non-Go clients need
nothing else to call a query and decode its rows. Related: the `http` settings in
[config.md](config.md#http), the query rules in [sql.md §10](sql.md#10-named-queries), and
readiness in [lifecycle.md §6](lifecycle.md#6-queries).

## Contents

1. [Where to call](#1-where-to-call)
2. [Routes](#2-routes)
3. [Requests](#3-requests)
4. [Responses](#4-responses)
5. [Errors](#5-errors)
6. [Health](#6-health)
7. [Limits](#7-limits)
8. [Authentication](#8-authentication)
9. [A Python client](#9-a-python-client)

## 1. Where to call

`dynaflux generate` writes a third template, `query.yaml`, when at least one query is
annotated `-- expose: http`. It deploys in every region with the application, after the
backfill (lifecycle.md §2), and holds the query function, `{prefix}{store}-query`, and the
front door that `http.frontend` selects. Its output `QueryEndpoint` is the region's base
URL:

| `http.frontend` | `QueryEndpoint` |
|-----------------|-----------------|
| `function_url` | `https://<url-id>.lambda-url.<region>.on.aws/` |
| `http_api` | `https://<api-id>.execute-api.<region>.amazonaws.com/` |
| `rest_api` | `https://<api-id>.execute-api.<region>.amazonaws.com/api/` |

With `http.domain`, `https://<domain>/` reaches the nearest region whose views are ready
([§6](#6-health)). Direct Lambda invoke of the query function always works, with the same
queries and the same answers as HTTP.

Each region answers from its own tables. A view version must be `live` in the region that
answers ([lifecycle.md §6](lifecycle.md#6-queries)); the response header
`x-dynaflux-region` names that region.

## 2. Routes

| Method | Path | Does |
|--------|------|------|
| `POST` | `/queries/{name}` | Runs the query named `{name}`, the `-- name:` identifier, case-sensitive. |
| `GET` | `/health` | Says whether the region can serve every exposed query ([§6](#6-health)). |

- A name that isn't in the deployed catalog, or isn't annotated `-- expose: http`, gets the
  same `404`, on every door and on direct invoke, so a caller can't learn which queries
  exist.
- A function URL passes every request to the function, which answers another method on
  these paths with `405` and an `Allow` header, and any other path with `404`. API Gateway
  answers requests for routes it doesn't have itself, with its own status and body.
- The query string and `content-type` are ignored.

A direct invoke sends `{"query": "<name>", "params": {…}}` as the payload. `params` may be
left out when the query has no parameters; any other key is a `400`.

## 3. Requests

The body is one JSON object with one member per query parameter, named as in the SQL:

```http
POST /queries/TagTotals
content-type: application/json

{"tags": ["go", "dynamodb"], "since": "2026-10-01T00:00:00Z"}
```

- An empty body is the empty object. A body sent base64-encoded (`isBase64Encoded`) is
  decoded first.
- Every parameter must be given once. A missing, unknown or repeated key is a `400`, and
  so is `null`: parameters are never `Nullable` (sql.md §10.3). The function stops reading
  at the first unknown or repeated key, so that key's error comes before any later one.
- Each value has the JSON form of its type below, and the value restrictions of
  [sql.md §4.3](sql.md#43-value-restrictions) apply. A value that doesn't fit is a `400`;
  nothing wraps or rounds, so `300` for a `UInt8` fails where ClickHouse would store 44.

| Parameter type | JSON form |
|----------------|-----------|
| `String` | a string |
| `UInt8` … `UInt64`, `Int8` … `Int64` | a number with a whole value, or a string of a base-10 integer: `42`, `4.2e1`, `"-42"` |
| `Float64` | a number |
| `Decimal(P, S)` | a number, or a string without an exponent, with no nonzero digit past the S-th after the point: `1234.5`, `"1234.50"` |
| `Bool` | `true` or `false` |
| `UUID` | a string of 8-4-4-4-12 hex digits, in either case |
| `Date` | a string `YYYY-MM-DD` |
| `DateTime64(3)`, `DateTime64(6)` | an RFC 3339 string with any UTC offset and at most 6 fractional digits, cut to the type's precision: `"2026-10-01T12:00:00.250+02:00"` |
| `Array(T)` | an array of T's form, at most 1,000 elements |

A 64-bit integer sent as a JSON number is read exactly, but some clients, such as
JavaScript's `JSON.stringify`, round large numbers before sending them. Send 64-bit
integers and decimals as strings when the client might.

## 4. Responses

A query answers `200` with ClickHouse's `FORMAT JSON` shape, without `statistics`:

```json
{"status":200,
 "meta":[{"name":"tag","type":"String"},{"name":"total_views","type":"UInt64"}],
 "data":[{"tag":"go","total_views":"1520"}],
 "rows":1}
```

- `status` repeats the HTTP status, so a direct invoke's payload is complete on its own.
- `meta` lists the output columns in `SELECT` order, each with its SQL type as sql.md writes
  it: `String`, `Decimal(18, 2)`, `Nullable(Decimal(18, 2))`, `Array(String)`,
  `DateTime64(6)`, `DateTime`.
- `data` has one object per row, its members in `SELECT` order. `rows` is their number.
- A `:one` query returns at most one row. Without one it answers `200` with `"rows":0`,
  not `404`, which a client couldn't tell from API Gateway's own.
- Every door, and direct invoke, returns the same body, byte for byte. Strings are not
  HTML-escaped.
- Headers: `content-type: application/json`, `cache-control: no-store`, and
  `x-dynaflux-region: <region>`. A direct invoke's payload is the body itself.

| Column type | JSON form |
|-------------|-----------|
| `String` | a string |
| `UInt8` … `UInt32`, `Int8` … `Int32` | a number |
| `UInt64`, `Int64` | a string: `"18446744073709551615"` |
| `Float64` | a number, as Go's `encoding/json` writes it: `0.25`, `-1e-7` |
| `Decimal(P, S)` | a string with exactly S digits after the point: `"1234.50"` |
| `Bool` | `true` or `false` |
| `UUID` | a lowercase 8-4-4-4-12 string |
| `Date` | `"YYYY-MM-DD"` |
| `DateTime` | RFC 3339 in UTC, whole seconds: `"2026-10-01T12:00:00Z"` |
| `DateTime64(p)` | RFC 3339 in UTC with exactly p fractional digits: `"2026-10-01T13:00:00.000Z"` |
| `NULL` | `null` |
| `Array(T)` | an array of T's form |

## 5. Errors

A request the function can't serve gets an error status and this body:

```json
{"status":503,"error":{"code":"view_not_ready","message":"view version tag_hourly@v2 is activating in us-east-1","request_id":"8f1c…"}}
```

`request_id` is Lambda's request ID, which the function's log line for the request
carries. The log names the query's parameters that the request gave, but never logs their
values or other keys.

| Status | `code` | Cause | What to do |
|--------|--------|-------|------------|
| 400 | `bad_request` | The body isn't one JSON object, repeats a key, or isn't valid base64; a direct invoke's payload has another key than `query` and `params`. | Fix the request. |
| 400 | `invalid_params` | A parameter is missing, unknown or `null`, or its value doesn't fit its type ([§3](#3-requests)). | Fix the parameters; the message names the parameter. |
| 400 | `too_many_keys` | The `IN` arrays make more than 100 key combinations (sql.md §10.4). | Split the call. |
| 400 | `result_too_large` | The response would pass Lambda's limit ([§7](#7-limits)). | Narrow the parameters, or add a `LIMIT` to the query. |
| 404 | `not_found` | No exposed query has the name, or no route has the path. | Check the name and its `-- expose: http`. |
| 405 | `method_not_allowed` | A known path with another method. | Use the method that `Allow` names. |
| 503 | `view_not_ready` | The view version the query reads isn't `live` in this region. A broken version's failure goes to the log only. | Retry later, or in another region. |
| 503 | `throttled` | DynamoDB throttled the reads after the SDK's retries. Comes with `Retry-After: 1`. | Retry after a second. |
| 504 | `timeout` | The query didn't finish 1 second before the function's 25-second timeout. | Narrow the parameters. |
| 500 | `evaluation_error` | The query's definition fails on the rows it read, such as an integer division by zero (sql.md §5.8). | Change the query. |
| 500 | `internal` | Anything else, such as a missing permission. The details are in the log. | Report the `request_id`. |

Every row above is a normal response. Only a crash, a Lambda timeout or an event from a door
the templates don't configure fails the invocation: the door then answers with its own
`5xx`, and a direct invoke's response has `FunctionError` set. The function's `Errors`
alarm counts these, and the `QueryFailures` metric counts `500` and `504` answers.

## 6. Health

`GET /health` answers `200` with `{"status":200,"region":"<region>"}` while the current
version of every view that an exposed query reads is `live` in the region, and otherwise
`503` with only `{"status":503,"error":{"code":"view_not_ready"}}`. When it can't read
the states, it answers as [§5](#5-errors) does, again with only the status and the code.
It never names a version: on API Gateway doors `/health` has no authentication, and the
log names the versions that aren't ready. A store whose exposed queries read only streams is always
healthy.

With `http.domain`, DNS stops sending clients to a region while its views aren't ready.
Each region's Route 53 health check follows an alarm rather than calling `/health`:

- Every minute, a schedule makes the query function check the views as `/health` does. It
  logs `ViewsNotReady`: 0, or 1 when a view isn't ready or the states can't be read.
- After 2 minutes in a row of 1, the region leaves the domain, until a check finds the
  views ready again. CloudWatch's delay and the time resolvers keep the old answer come on
  top.
- A check that leaves no data, such as one whose log line a raised log level drops, keeps
  the region in. When every region is out, Route 53 answers with all of them.

On API Gateway doors, the stage throttles `/health` to 5 requests a second with bursts of
10, so no one can drain the function's concurrency through it. No health check calls it, so
a flood of it can't take a region out of the domain. On a function URL, `/health` has the
URL's authentication.

## 7. Limits

| Limit | Value |
|-------|-------|
| Response | 6,291,456 bytes, Lambda's limit on a synchronous response, counted on the whole payload: an HTTP door's envelope escapes the body's quotes, so there the body must be smaller |
| Request | 6 MB (Lambda) |
| Time | the function times out after 25 seconds and answers `504` 1 second before; API Gateway gives up after 29 (REST) or 30 seconds (HTTP API) |
| Key combinations | 100 per call (sql.md §10.4) |
| Array parameter | 1,000 elements |

There is no pagination in v1 (sql.md §10.1).

## 8. Authentication

`http.frontend` and `http.auth` choose the door and how callers prove who they are
([config.md](config.md#http)). Every door serves the same exposed queries, and the function
never reads the authorizer's result, so an authenticated caller can call every exposed
query.

Authentication decides who can call, not which rows they read: the parameters choose the
rows. Any caller that passes `auth` can read every row an exposed query reaches, so with
`WHERE user_id = {user:String}`, every signed-in user can read every other user's rows.
Expose only queries whose rows every caller may see, and serve per-user reads through your
own service.

| Door and `auth` | The caller needs |
|-----------------|------------------|
| direct invoke (always) | `lambda:InvokeFunction` on the query function. |
| `function_url`, `iam` | Requests signed with SigV4 for service `lambda`, and both `lambda:InvokeFunctionUrl` and `lambda:InvokeFunction` on the function: AWS checks both. |
| `function_url`, `none` | Nothing: anyone with the URL can call (DF6602, DF6603). A function URL has no throttling; `http.max_concurrency` caps the function's concurrent executions (DF6615). |
| `http_api` or `rest_api`, `iam` | Requests signed with SigV4 for service `execute-api` and the region of the host, and `execute-api:Invoke` on the query route. A signature names one region, so `iam` can't be combined with `http.domain` (DF6608). |
| `http_api`, `jwt` | A JWT from `jwt.issuer`, in the `Authorization` header. |
| `http_api` or `rest_api`, `lambda` | What the authorizer function allows, from the `Authorization` header. |
| `rest_api`, `api_key` | A key of the usage plan, in the `x-api-key` header. |
| `rest_api`, `cognito` | A token of the Cognito user pool, in the `Authorization` header. |

### 8.1 IAM

A caller of an `iam` function URL needs both actions; they reach the same exposed queries:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["lambda:InvokeFunctionUrl", "lambda:InvokeFunction"],
    "Resource": "arn:aws:lambda:*:123456789012:function:prod-analytics-query"
  }]
}
```

On API Gateway, allow `execute-api:Invoke` on
`arn:aws:execute-api:<region>:<account>:<api-id>/$default/POST/queries/*` (HTTP API) or
`…/<api-id>/api/POST/queries/*` (REST API), and `GET/health` if the caller checks health.

### 8.2 JWT

API Gateway accepts a token whose issuer is `jwt.issuer` and whose `aud` claim, or
`client_id` when the token has no `aud`, is in `jwt.audience`. With `jwt.scopes`, the token
must also carry one of them. Without `jwt.scopes`, any ID or access token for the audience
passes, since API Gateway can't tell them apart (DF6613).

- Only RSA-signed tokens are accepted.
- API Gateway caches the issuer's public keys for up to 2 hours. When you rotate a signing
  key, keep publishing the old key for 2 hours after the new one signs tokens.
- A Cognito issuer, `https://cognito-idp.<region>.amazonaws.com/<pool-id>`, works from
  every region.

### 8.3 Lambda authorizer

`authorizer.function` names a function that must exist under the same name in every
region. API Gateway caches its answer for 300 seconds per `Authorization` header value. On an
HTTP API the function gets the version 2.0 payload and returns a simple response,
`{"isAuthorized": true}` or `false`. On a REST API it returns an IAM policy; it must allow
`arn:aws:execute-api:<region>:<account>:<api-id>/api/POST/queries/*`, not one query's ARN:
the cached policy also decides the caller's next queries.

### 8.4 API keys

Keys are created outside the stack, so they stay out of templates, parameters and CI logs.
Let API Gateway generate a key in one region, then import its value into the usage plan
`{prefix}{store}-query` of every other region (output `QueryUsagePlanId`), so one key works
behind `http.domain`:

```sh
aws apigateway create-api-key --name client-a --enabled --region us-east-1
aws apigateway create-usage-plan-key --usage-plan-id <QueryUsagePlanId> \
    --key-id <key id> --key-type API_KEY --region us-east-1
# In each other region: the same value, then the plan.
aws apigateway create-api-key --name client-a --enabled --value <value> --region eu-west-1
```

The usage plan's rate, burst and daily quota are applied on a best-effort basis. They
aren't a cost or access control: keep the key secret, and use another `auth` when callers
must be told apart reliably.

### 8.5 Cognito

- With `cognito.scopes`, the caller sends an access token that carries one of them.
- With `cognito.app_clients`, the caller sends an ID token whose `aud` is one of them.
- With neither, any ID token of the pool passes, from any of its app clients (DF6614).
- With both, the method takes access tokens only, which carry no `aud`, so API Gateway may
  refuse every token; `validate` warns (DF6616). Which tokens pass is to be confirmed on AWS
  ([infra.md §16](infra.md#16-to-be-confirmed-on-aws)). The resource server's app client
  settings already decide which clients can get a scope.

## 9. A Python client

`meta` is enough to decode any query. This helper turns values into Python types:

```python
import datetime
import decimal
import uuid


def decode(value, sql_type):
    """Decodes one value of a response by its meta type."""
    if value is None:
        return None
    if sql_type.startswith("Nullable("):
        return decode(value, sql_type[len("Nullable("):-1])
    if sql_type.startswith("Array("):
        return [decode(v, sql_type[len("Array("):-1]) for v in value]
    if sql_type in ("Int64", "UInt64"):
        return int(value)
    if sql_type.startswith("Decimal("):
        return decimal.Decimal(value)
    if sql_type == "UUID":
        return uuid.UUID(value)
    if sql_type == "Date":
        return datetime.date.fromisoformat(value)
    if sql_type == "DateTime" or sql_type.startswith("DateTime64("):
        return datetime.datetime.fromisoformat(value)  # Python 3.11 or later reads the Z
    return value  # String, Bool, Float64 and the smaller integers


def rows(out):
    """Returns a response's rows as dictionaries of Python values."""
    if out["status"] != 200:
        raise RuntimeError(out["error"]["code"], out["error"].get("message"))
    return [{c["name"]: decode(row[c["name"]], c["type"]) for c in out["meta"]} for row in out["data"]]
```

Direct invoke, with `lambda:InvokeFunction`:

```python
import json

import boto3

resp = boto3.client("lambda", region_name="us-east-1").invoke(
    FunctionName="prod-analytics-query",
    Payload=json.dumps({"query": "TagTotals", "params": {"tags": ["go"], "since": "2026-10-01T00:00:00Z"}}),
)
if "FunctionError" in resp:  # a crash or a timeout, not a request error
    raise RuntimeError(resp["Payload"].read())
print(rows(json.load(resp["Payload"])))
```

A signed HTTP call: service `lambda` for a function URL, `execute-api` for an API Gateway
door. An error status still carries the JSON body of [§5](#5-errors), except when API
Gateway answers by itself.

```python
import json
import urllib.error
import urllib.request

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest


def query(endpoint, region, name, params, service="lambda"):
    req = AWSRequest(method="POST", url=endpoint.rstrip("/") + "/queries/" + name,
                     data=json.dumps(params).encode(), headers={"content-type": "application/json"})
    SigV4Auth(boto3.Session().get_credentials(), service, region).add_auth(req)
    p = req.prepare()
    try:
        with urllib.request.urlopen(urllib.request.Request(p.url, data=p.body, headers=dict(p.headers))) as resp:
            return rows(json.load(resp))
    except urllib.error.HTTPError as e:
        return rows(json.load(e))
```

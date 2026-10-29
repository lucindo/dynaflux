<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0020: The custom domain's health checks follow a readiness alarm, not `/health`

- Status: Accepted. Amends [ADR 0018](0018-query-handler-and-front-doors.md), whose
  decision stands except for how Route 53 learns that a region's views are ready.
- Date: 2026-10-29

## Context

ADR 0018 had each region's Route 53 health check call the region's own `GET /health`. On
the API doors, `/health` takes anyone, since Route 53 can't sign requests, so the stage
throttles it to 5 requests a second. Route 53's checkers share that throttle with everyone
else. Anyone who sends more than 5 requests a second makes the checkers get `429` too, and
the region leaves the domain until the flood stops. ADR 0018 accepted this and left it to
end-to-end experiment Q1. A security review found it worth fixing before v1.0: the attack
needs no credentials and costs the attacker almost nothing.

A health check can instead follow a CloudWatch alarm (`CLOUDWATCH_METRIC`). Route 53 then
calls nothing, so there is no endpoint to flood. Route 53 takes only alarms on one
standard-resolution metric, with a basic statistic, without metric math, and alarming on
every period it evaluates rather than "M out of N"
([Route 53 health check types](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/health-checks-types.html)).

Something must publish the metric when no one queries. Operators may raise the functions'
application log level to `ERROR` ([ADR 0010](0010-runtime-entry-point.md), e2e.md B12),
which drops every log line below it, and with it any embedded metric that the line holds.

## Decision

1. With `http.domain`, an EventBridge rule sends the query function a scheduled event every
   minute. The function checks the views as `/health` does and logs `ViewsNotReady` in the
   `Dynaflux` namespace: 0 at `INFO`, or 1 at `ERROR` when a view an exposed query reads
   isn't `live` in the region or the states can't be read.
2. An alarm, `{query function}-views-not-ready`, is in `ALARM` when the maximum of
   `ViewsNotReady` is 1 for 2 one-minute periods in a row. It treats missing data as not
   breaching.
3. The health check is `CLOUDWATCH_METRIC` on that alarm, with
   `InsufficientDataHealthStatus: Healthy`.
4. `/health` stays, throttled, for people and probes. No health check calls it.

Only a 1 takes a region out. A raised log level drops the 0s, which then count as no data,
and keeps the 1s. A tick that never runs also leaves no data. Both keep the region in the
domain rather than letting a logging setting, or a starved function, pull it out.

## Consequences

- Flooding `/health` gets `429`s and nothing else.
- A region whose views stop being ready leaves the domain after about 2 minutes plus
  CloudWatch's delay, slower than the 90 seconds of ADR 0018. End-to-end experiment Q1
  measures it.
- A region whose query function can't run at all stays in the domain, where the old check
  would have taken it out. The health check follows the views, as `http.md` §6 always
  said; the function's own failures have their alarms.
- With `http.domain`, each region adds one invocation a minute, one custom metric and one
  alarm.
- The query function serves a fifth event shape. It carries nothing the function trusts, so
  a forged one, which needs `lambda:InvokeFunction` anyway, only adds a true datapoint.

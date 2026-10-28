-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- Few accounts and endpoints, so appends meet on rows. Events expire a day after their
-- append; a run expires some early and appends them again.
CREATE STREAM api_calls (
    account_id String,
    endpoint   String,
    request_id String,
    units      UInt32
)
PRIMARY KEY (account_id)
DEDUP BY (request_id)
LOG RETENTION 1 DAY;

-- dynaflux.lock holds version 1, without rate, which keeps running while this version 2
-- is backfilled. A poison event, of no units, fails version 2.
CREATE MATERIALIZED VIEW quota_usage
MODE SYNC
ORDER BY (account_id)
AS SELECT account_id, count() AS calls, sum(units) AS used, sum(intDiv(1000, units)) AS rate
FROM api_calls
GROUP BY account_id;

-- A row that every account shares (DF3501): concurrent appends conflict on it.
CREATE MATERIALIZED VIEW endpoint_calls
MODE SYNC
ORDER BY (endpoint)
AS SELECT endpoint, count() AS calls, uniqExact(account_id) AS accounts
FROM api_calls
GROUP BY endpoint;

-- Calls of more than 50 units only. In the variant catalog that keeps this view alone SYNC,
-- an append of a smaller call names it in _sync and puts the event alone.
CREATE MATERIALIZED VIEW heavy_calls
MODE SYNC
ORDER BY (account_id)
AS SELECT account_id, count() AS calls, sum(units) AS used
FROM api_calls
WHERE units > 50
GROUP BY account_id;

CREATE MATERIALIZED VIEW account_daily
ORDER BY (account_id, day)
PRIMARY KEY (account_id)
AS SELECT account_id, toStartOfDay(event_time) AS day, count() AS calls, sum(units) AS used
FROM api_calls
GROUP BY account_id, day;

-- Each query reads every row of one view's current version, its keys first.

-- name: QuotaUsage :many
SELECT account_id, calls, used, rate FROM quota_usage
WHERE account_id IN {accounts:Array(String)};

-- name: EndpointCalls :many
SELECT endpoint, calls, accounts FROM endpoint_calls
WHERE endpoint IN {endpoints:Array(String)};

-- name: HeavyCalls :many
SELECT account_id, calls, used FROM heavy_calls
WHERE account_id IN {accounts:Array(String)};

-- name: AccountDaily :many
SELECT account_id, day, calls, used FROM account_daily
WHERE account_id IN {accounts:Array(String)};

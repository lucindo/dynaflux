-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- A stream with idempotent appends, a SYNC view that sees its own writes, and a :one check.
CREATE STREAM api_calls (
    account_id String,
    endpoint   LowCardinality(String),
    units      UInt32,
    request_id UUID
)
PRIMARY KEY (account_id)
DEDUP BY (request_id);

CREATE MATERIALIZED VIEW quota_usage
MODE SYNC
ORDER BY (account_id, month)
PRIMARY KEY (account_id)
TTL month + INTERVAL 13 MONTH
AS SELECT
    account_id,
    toStartOfMonth(event_time) AS month,
    sum(units)                 AS used_units,
    count()                    AS calls
FROM api_calls
GROUP BY account_id, month;

-- name: QuotaUsed :one
SELECT used_units, calls
FROM quota_usage
WHERE account_id = {account_id:String}
  AND month = toStartOfMonth({at:DateTime64(6)});

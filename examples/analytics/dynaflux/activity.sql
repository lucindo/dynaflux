-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- Per-member latest state, a daily rollup, and queries that combine rows.
CREATE STREAM member_events (
    workspace_id String,
    member_id    String,
    action       LowCardinality(String),
    duration_ms  UInt32
)
PRIMARY KEY (workspace_id, member_id);

CREATE MATERIALIZED VIEW member_latest
ORDER BY (workspace_id, member_id)
PRIMARY KEY (workspace_id)
AS SELECT
    workspace_id,
    member_id,
    argMax(action, event_time) AS last_action,
    max(event_time)            AS last_seen,
    count()                    AS actions
FROM member_events
GROUP BY workspace_id, member_id;

CREATE MATERIALIZED VIEW workspace_daily
ORDER BY (workspace_id, day)
PRIMARY KEY (workspace_id)
TTL day + INTERVAL 400 DAY
AS SELECT
    workspace_id,
    toDate(event_time)        AS day,
    uniq(member_id)           AS active_members,
    countIf(action = 'login') AS logins,
    avg(duration_ms)          AS avg_duration_ms
FROM member_events
GROUP BY workspace_id, day;

-- name: WorkspaceSummary :one
SELECT
    count()                                     AS members,
    countIf(last_seen >= {since:DateTime64(6)}) AS recently_active,
    countIf(last_action = 'logout')             AS logged_out
FROM member_latest
WHERE workspace_id = {workspace_id:String};

-- name: WorkspaceActiveMembers :one
SELECT uniqMerge(active_members) AS members
FROM workspace_daily
WHERE workspace_id = {workspace_id:String}
  AND day BETWEEN {from:Date} AND {to:Date};

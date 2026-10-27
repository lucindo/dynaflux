-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- Read by the end-to-end suite only (docs/runbooks/e2e.md): the view columns that the
-- example's queries leave out, so that comparing a region with its oracle covers every column.

-- name: E2EPageTotals :many
SELECT day, total_ms, last_view
FROM page_daily
WHERE page = {page:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;

-- name: E2EWorkspaceDays :many
SELECT day, logins, avg_duration_ms
FROM workspace_daily
WHERE workspace_id = {workspace_id:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;

-- name: E2EMembers :many
SELECT member_id, last_action, last_seen, actions
FROM member_latest
WHERE workspace_id = {workspace_id:String}
ORDER BY member_id;

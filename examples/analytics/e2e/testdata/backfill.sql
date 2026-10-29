-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- Added to the deployed end-to-end store by scenarios 24 and 36 (docs/runbooks/e2e.md),
-- which backfill its views under traffic: e2e_page_count is additive, so a table source
-- splits it by time, and e2e_page_latest is idempotent, so a table source needs no split.
-- The queries read every column of both, for the comparison with the oracle.
CREATE MATERIALIZED VIEW e2e_page_count
ORDER BY (page)
AS SELECT
    page,
    count()          AS views,
    sum(duration_ms) AS total_ms
FROM page_views
GROUP BY page;

CREATE MATERIALIZED VIEW e2e_page_latest
ORDER BY (page)
AS SELECT
    page,
    max(event_time) AS last_view,
    uniq(user_id)   AS users
FROM page_views
GROUP BY page;

-- name: E2EPageCount :many
SELECT views, total_ms
FROM e2e_page_count
WHERE page = {page:String};

-- name: E2EPageLatest :many
SELECT last_view, users
FROM e2e_page_latest
WHERE page = {page:String};

-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- Daily totals per page. day is a DateTime, so the TTL uses a key column of a temporal type.
CREATE MATERIALIZED VIEW page_daily
MODE ASYNC
ORDER BY (page, day)
PRIMARY KEY (page)
TTL day + INTERVAL 2 YEAR
AS SELECT
    page,
    toStartOfDay(event_time) AS day,
    count()                  AS views,
    uniqExact(user_id)       AS visitors,
    sum(duration_ms)         AS total_ms,
    max(event_time)          AS last_view
FROM page_views
WHERE page != ''
GROUP BY page, day;

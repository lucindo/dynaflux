-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- name: PageStats :many
SELECT day, views, visitors
FROM page_daily
WHERE page = {page:String}
  AND day BETWEEN {from:Date} AND {to:Date}
ORDER BY day;

-- name: TagTotals :many
-- expose: http
SELECT tag, sum(views) AS total_views, uniqMerge(users) AS total_users
FROM tag_hourly
WHERE tag IN {tags:Array(String)}
  AND hour >= {since:DateTime64(6)}
GROUP BY tag
ORDER BY total_views DESC
LIMIT 10;

-- name: RecentPageViews :many
SELECT event_time, page, duration_ms
FROM page_views
WHERE user_id = {user:String}
  AND event_time >= {since:DateTime64(6)}
ORDER BY event_time DESC
LIMIT {n:UInt32};

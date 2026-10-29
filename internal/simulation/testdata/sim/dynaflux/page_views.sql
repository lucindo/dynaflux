-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- Every aggregate, -If, ARRAY JOIN, TTL and SPLIT. A run ends long before any view's TTL,
-- so no row expires; events expire a day after their append.
CREATE STREAM page_views (
    site    String,
    page    String,
    user_id String,
    ms      UInt32,
    price   Decimal(10, 2),
    score   Float64,
    label   String,
    tags    Array(String)
)
PRIMARY KEY (page)
LOG RETENTION 1 DAY;

-- Every additive aggregate. dynaflux.lock holds version 1, without slow_ms, which keeps
-- running while this version 2 is backfilled.
CREATE MATERIALIZED VIEW page_hourly
ORDER BY (page, hour)
PRIMARY KEY (page)
TTL hour + INTERVAL 7 DAY
AS SELECT page, toStartOfHour(event_time) AS hour, count() AS views, sum(ms) AS total_ms,
    sum(price) AS revenue, sum(score) AS total_score, avg(ms) AS avg_ms, min(ms) AS fastest,
    max(ms) AS slowest, uniqExact(user_id) AS users, quantile(0.9)(ms) AS p90_ms,
    quantileDD(0.01, 0.99)(ms) AS p99_ms, sumIf(ms, ms > 2500) AS slow_ms
FROM page_views
GROUP BY page, hour;

CREATE MATERIALIZED VIEW page_users
ORDER BY (page, user_id)
PRIMARY KEY (page)
AS SELECT page, user_id
FROM page_views
GROUP BY page, user_id;

-- No event is that slow, so each row holds a count of 0.
CREATE MATERIALIZED VIEW slow_pages
ORDER BY (page)
AS SELECT page, countIf(ms > 60000) AS slow
FROM page_views
GROUP BY page;

-- Few sites: every unit writes each row.
CREATE MATERIALIZED VIEW site_totals
ORDER BY (site)
AS SELECT site, count() AS views, uniqExact(user_id) AS users
FROM page_views
GROUP BY site;

-- The same hot rows, each stored as 4 items.
CREATE MATERIALIZED VIEW site_daily
ORDER BY (site, day)
PRIMARY KEY (site)
SPLIT 4
AS SELECT site, toDate(event_time) AS day, count() AS views, uniqExact(user_id) AS users,
    sum(price) AS revenue
FROM page_views
GROUP BY site, day;

-- Ties in ms go by event. A label over 4 KB is left out of max(label); a poison event's ms,
-- 0, fails the evaluation. Idempotent only, but uniqExact takes the fenced handoff.
CREATE MATERIALIZED VIEW user_latest
ORDER BY (user_id)
AS SELECT user_id, argMax(page, ms) AS slowest_page, argMin(page, ms) AS fastest_page,
    any(page) AS first_page, max(label) AS top_label, max(intDiv(1000, ms)) AS top_rate,
    uniqExact(page) AS pages
FROM page_views
GROUP BY user_id;

-- An event's tags repeat: uniqExact counts a tag once per record and row.
CREATE MATERIALIZED VIEW page_tags
ORDER BY (page)
AS SELECT page, count() AS tag_rows, uniqExact(tag) AS distinct_tags, uniq(tag) AS approx_tags
FROM page_views
ARRAY JOIN tags AS tag
GROUP BY page;

-- A poison event, whose ms is 0, fails the evaluation.
CREATE MATERIALIZED VIEW page_rates
ORDER BY (page)
AS SELECT page, sum(intDiv(1000, ms)) AS rate
FROM page_views
GROUP BY page;

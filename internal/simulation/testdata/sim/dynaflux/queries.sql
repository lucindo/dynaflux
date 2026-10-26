-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- Each query reads every row of one view's current version, its keys first.

-- name: PageHourly :many
SELECT page, hour, views, total_ms, revenue, total_score, avg_ms, fastest, slowest, users, p90_ms,
    p99_ms, slow_ms
FROM page_hourly
WHERE page IN {pages:Array(String)};

-- name: PageUsers :many
SELECT page, user_id FROM page_users
WHERE page IN {pages:Array(String)};

-- name: SlowPages :many
SELECT page, slow FROM slow_pages
WHERE page IN {pages:Array(String)};

-- name: SiteTotals :many
SELECT site, views, users FROM site_totals
WHERE site IN {sites:Array(String)};

-- name: SiteDaily :many
SELECT site, day, views, users, revenue FROM site_daily
WHERE site IN {sites:Array(String)};

-- name: UserLatest :many
SELECT user_id, slowest_page, fastest_page, first_page, top_label, pages FROM user_latest
WHERE user_id IN {users:Array(String)};

-- name: PageTags :many
SELECT page, tag_rows, distinct_tags, approx_tags FROM page_tags
WHERE page IN {pages:Array(String)};

-- name: PageRates :many
SELECT page, rate FROM page_rates
WHERE page IN {pages:Array(String)};

-- name: QuotaByAccount :many
SELECT account, requests, used FROM quota_by_account
WHERE account IN {accounts:Array(String)};

-- name: QuotaByOrigin :many
SELECT account, origin_region, requests, used FROM quota_by_origin
WHERE account IN {accounts:Array(String)};

-- name: ChargeTotals :many
SELECT customer, charges, total_cents FROM charge_totals
WHERE customer IN {customers:Array(String)};

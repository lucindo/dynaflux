-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- One row per tag and hour, from the tags array.
CREATE MATERIALIZED VIEW tag_hourly
ORDER BY (tag, hour)
PRIMARY KEY (tag)
TTL hour + INTERVAL 30 DAY
AS SELECT
    tag,
    toStartOfHour(event_time) AS hour,
    count()                   AS views,
    uniq(user_id)             AS users
FROM page_views
ARRAY JOIN tags AS tag
GROUP BY tag, hour;

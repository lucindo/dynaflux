-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

CREATE STREAM page_views (
    user_id     String,
    page        String,
    referrer    Nullable(String),
    tags        Array(String),
    duration_ms UInt32,
    country     LowCardinality(String) DEFAULT ''
)
PRIMARY KEY (user_id)
LOG RETENTION 2 DAY
ACCEPT event_time BETWEEN now() - INTERVAL 30 DAY AND now() + INTERVAL 5 MINUTE;

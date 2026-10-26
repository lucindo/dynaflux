-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- A client that fails over appends a request in a second region too, with the same payload.
CREATE STREAM quota_events (
    account String,
    request String,
    units   UInt32
)
PRIMARY KEY (account)
DEDUP BY (request)
LOG RETENTION 1 DAY;

CREATE MATERIALIZED VIEW quota_by_account
ORDER BY (account)
AS SELECT account, count() AS requests, sum(units) AS used
FROM quota_events
GROUP BY account;

-- A request appended in two regions has another origin_region in each (DF3504), but each
-- region counts it once: an account's rows add up to the same totals everywhere.
CREATE MATERIALIZED VIEW quota_by_origin
ORDER BY (account, origin_region)
PRIMARY KEY (account)
AS SELECT account, origin_region, count() AS requests, sum(units) AS used
FROM quota_events
GROUP BY account, origin_region;

-- A client that retries a charge with another amount: regions may keep different payloads.
CREATE STREAM charges (
    customer String,
    charge   String,
    cents    UInt32
)
PRIMARY KEY (customer)
DEDUP BY (charge)
LOG RETENTION 1 DAY;

CREATE MATERIALIZED VIEW charge_totals
ORDER BY (customer)
AS SELECT customer, count() AS charges, sum(cents) AS total_cents
FROM charges
GROUP BY customer;

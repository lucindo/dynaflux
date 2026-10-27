-- Copyright 2026 The Dynaflux Authors
-- SPDX-License-Identifier: Apache-2.0

-- Added to the example for the end-to-end suite only (docs/runbooks/e2e.md): a page view with
-- duration_ms = 0 breaks this view, and the scenario of the poison event checks that the
-- other views and the shard go on.
CREATE MATERIALIZED VIEW e2e_ratio
ORDER BY (page)
AS SELECT
    page,
    sum(intDiv(1000, duration_ms)) AS per_second
FROM page_views
GROUP BY page;

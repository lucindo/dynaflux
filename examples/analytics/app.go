// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package analytics is application code that uses the example's generated client.
package analytics

import (
	"context"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
)

// RecordPageView appends a page view and returns the stats of its page on its UTC day,
// which count it. page_daily is ASYNC, so the append asks for a receipt and the query waits
// for this region's projector to apply the view, for at most timeout: about a second when
// deployed. A zero EventTime is the time of the call.
func RecordPageView(ctx context.Context, c *flux.Client, view flux.PageView, timeout time.Duration) ([]flux.PageStatsRow, error) {
	if view.EventTime.IsZero() {
		view.EventTime = time.Now()
	}
	tok, err := c.AppendPageView(ctx, view, dynaflux.Waitable())
	if err != nil {
		return nil, err
	}
	day := flux.PageStatsParams{Page: view.Page, From: view.EventTime, To: view.EventTime}
	return c.PageStats(ctx, day, dynaflux.WaitFor(tok, timeout))
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package analytics_test

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/fluxtest"
)

func requestID(t *testing.T, s string) dynaflux.UUID {
	t.Helper()
	id, err := dynaflux.ParseUUID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestChargeCountsRetryOnce(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		charge := flux.APICall{
			AccountID: "acme", Endpoint: "/search", Units: 5,
			RequestID: requestID(t, "6f1c2b8e-4d3a-4b5c-9e7f-0a1b2c3d4e5f"), EventTime: start,
		}
		first, err := c.AppendAPICall(t.Context(), charge)
		if err != nil {
			t.Fatal(err)
		}
		// The caller retries with the same request_id and event_time: a duplicate of the
		// stored event, which keeps counting once.
		again, err := c.AppendAPICall(t.Context(), charge)
		if !errors.Is(err, dynaflux.ErrDuplicate) || again != first {
			t.Errorf("retry = %v, %v; want ErrDuplicate with the first append's token %v", again, err, first)
		}
		other := flux.APICall{
			AccountID: "acme", Endpoint: "/export", Units: 3,
			RequestID: requestID(t, "0b9f7c6e-1a2d-4e3f-8a9b-c0d1e2f3a4b5"), EventTime: start.Add(time.Minute),
		}
		if _, err = c.AppendAPICall(t.Context(), other); err != nil {
			t.Fatal(err)
		}
		got, err := c.QuotaUsed(t.Context(), flux.QuotaUsedParams{AccountID: "acme", At: start.Add(time.Hour)})
		if want := (flux.QuotaUsedRow{UsedUnits: 8, Calls: 2}); err != nil || got != want {
			t.Errorf("QuotaUsed = %+v, %v; want %+v", got, err, want)
		}
	})
}

func TestQuotaUsedNotFound(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := flux.New(fluxtest.NewStore(t, flux.Catalog))
		december := start.Add(-time.Hour)
		_, err := c.AppendAPICall(t.Context(), flux.APICall{
			AccountID: "acme", Endpoint: "/search", Units: 5,
			RequestID: requestID(t, "6f1c2b8e-4d3a-4b5c-9e7f-0a1b2c3d4e5f"), EventTime: december,
		})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.QuotaUsed(t.Context(), flux.QuotaUsedParams{AccountID: "acme", At: december})
		if want := (flux.QuotaUsedRow{UsedUnits: 5, Calls: 1}); err != nil || got != want {
			t.Errorf("QuotaUsed in December = %+v, %v; want %+v", got, err, want)
		}
		// Before an account's first call of the month, its row doesn't exist.
		for _, p := range []flux.QuotaUsedParams{{AccountID: "acme", At: start}, {AccountID: "globex", At: december}} {
			if _, err = c.QuotaUsed(t.Context(), p); !errors.Is(err, dynaflux.ErrNotFound) {
				t.Errorf("QuotaUsed(%s, %v): got %v, want ErrNotFound", p.AccountID, p.At, err)
			}
		}
	})
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation_test

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/simulation"
	"github.com/lucindo/dynaflux/internal/store"
)

// callsFixture is the store of the SYNC scenarios: testdata/sync.
//
//go:embed testdata/sync
var callsFixture embed.FS

const (
	callsStore    = "sync"
	callAccounts  = 3
	callEndpoints = 2
	maxCallUnits  = 100
	rateUnits     = 1000 // quota_usage@v2's rate sums intDiv(rateUnits, units)
	heavyUnits    = 50   // heavy_calls takes the calls of more units
)

// The view versions of the SYNC fixture.
const (
	quotaUsageV1  = "quota_usage@v1" // retained, by dynaflux.lock
	quotaUsage    = "quota_usage@v2"
	endpointCalls = "endpoint_calls@v1"
	heavyCalls    = "heavy_calls@v1"
	accountDaily  = "account_daily@v1"
)

// apiCalls is the SYNC fixture's store, appended to with SYNC views and projected by the
// real core. Seeds may share it.
type apiCalls struct {
	cat   *engine.Catalog
	async *engine.Catalog // cat with every view ASYNC
	heavy *engine.Catalog // cat with every view but heavy_calls ASYNC
	calls *projector.Stream
}

func newAPICalls(t *testing.T) *apiCalls {
	t.Helper()
	root, err := fs.Sub(callsFixture, "testdata/sync")
	if err != nil {
		t.Fatal(err)
	}
	data := catalogtest.MarshalFS(t, root)
	s := &apiCalls{
		cat:   compileData(t, data, window),
		async: compileData(t, catalogtest.Async(t, data), window),
		heavy: compileData(t, catalogtest.Async(t, data, "heavy_calls"), window),
	}
	s.calls, _ = s.cat.Projector().Stream("api_calls")
	return s
}

// Config backfills quota_usage's version 2 while version 1, which the scenarios with
// Builds.Retire retire, runs. Writers append with a variant now and then: every view ASYNC,
// or all but heavy_calls.
func (s *apiCalls) Config() simulation.Config {
	return simulation.Config{
		Catalog:  s.cat,
		Naming:   catalog.Naming{Store: callsStore},
		Backfill: []string{quotaUsage},
		Retire:   quotaUsageV1,
		Variants: []*engine.Catalog{s.async, s.heavy},
	}
}

// Event returns an API call, half of them with a divergent payload. A poison event has no
// units, which fails quota_usage@v2.
func (*apiCalls) Event(r *rand.Rand, n int, poison bool) simulation.Event {
	at := time.Now().Add(-time.Duration(r.Int64N(int64(spread))))
	units := 1 + r.Uint64N(maxCallUnits)
	if poison {
		units = 0
	}
	ev := simulation.Event{Stream: "api_calls", Vals: []expr.Value{
		value("a", r.IntN(callAccounts)),
		value("e", r.IntN(callEndpoints)),
		value("q", n),
		expr.Uint(units),
		expr.Micros(at.UnixMicro()),
	}}
	if r.IntN(2) == 0 {
		ev.Divergent = slices.Clone(ev.Vals)
		ev.Divergent[3] = expr.Uint(units + 1 + r.Uint64N(maxCallUnits))
	}
	return ev
}

func (s *apiCalls) NewCore(region string, b store.Backend, a simulation.Archiver, cat *projector.Compiled) simulation.Core {
	if cat == nil {
		cat = s.cat.Projector()
	}
	return projector.New(cat, b, a, projector.Config{Naming: catalog.Naming{Store: callsStore}, Region: region})
}

func (s *apiCalls) Expect(o *simulation.Oracle, _, unit string, rec projector.Record) error {
	vals, bad := s.calls.Decode(rec.New)
	if err := errors.Join(bad...); err != nil {
		return fmt.Errorf("record %s: %w", rec.Seq, err)
	}
	n := len(s.calls.Columns)
	account, endpoint := vals[0].AsString(), vals[1].AsString()
	units := new(big.Rat).SetUint64(vals[3].AsUint())
	// event_id, then event_time, follow the declared columns.
	id := simulation.RecordID{Unit: unit, Seq: rec.Seq, EventID: vals[n].AsString()}
	for _, version := range []string{quotaUsageV1, quotaUsage} {
		row := simulation.RowID{View: version, Key: account}
		o.Count(row, "calls", 1)
		o.Sum(row, "used", units)
	}
	if units.Sign() == 0 {
		o.Poison(quotaUsage, id)
	} else {
		o.Sum(simulation.RowID{View: quotaUsage, Key: account}, "rate", new(big.Rat).SetUint64(rateUnits/vals[3].AsUint()))
	}
	calls := simulation.RowID{View: endpointCalls, Key: endpoint}
	o.Count(calls, "calls", 1)
	o.UniqExact(calls, "accounts", account)
	if vals[3].AsUint() > heavyUnits {
		heavy := simulation.RowID{View: heavyCalls, Key: account}
		o.Count(heavy, "calls", 1)
		o.Sum(heavy, "used", units)
	}
	day := time.UnixMicro(vals[n+1].AsMicros()).UTC().Truncate(24 * time.Hour)
	daily := simulation.RowID{View: accountDaily, Key: account + " " + rfc3339(day)}
	o.Count(daily, "calls", 1)
	o.Sum(daily, "used", units)
	return nil
}

func (s *apiCalls) Read(ctx context.Context, b store.Backend) (simulation.State, error) {
	return readState(ctx, b, s.cat, map[string]expr.Value{
		"accounts":  every("a", callAccounts),
		"endpoints": every("e", callEndpoints),
	})
}

// systems returns the system of each scenario: the SYNC fixture's for the sync scenarios,
// the page views' otherwise.
func systems(t *testing.T) func(simulation.Scenario) simulation.System {
	t.Helper()
	views, calls := newPageViews(t), newAPICalls(t)
	return func(sc simulation.Scenario) simulation.System {
		if strings.HasPrefix(sc.Name, "sync") {
			return calls
		}
		return views
	}
}

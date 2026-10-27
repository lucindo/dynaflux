// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// The builds a region runs (Builds), as indexes of run.builds.
const (
	buildFull     = iota // the system's catalog
	buildOld             // without the versions Config.Backfill names
	buildRetiring        // without Config.Retire, listing it as retired
	builds
)

// Shares of the regions of a run with Builds.Retire.
const (
	neverRetired = 1.0 / 3 // get no tombstone from the retire actor, as when a deploy rolled back
	keepOlder    = 0.5     // keep the older build for good once they return to it
)

// compileBuilds compiles the catalogs of the builds the run's scenario runs into r.builds, by
// index; nil for the full build and for those it doesn't run.
func (r *run) compileBuilds() error {
	sc, cfg := r.sc, r.cfg
	r.builds = make([]*projector.Compiled, builds)
	def := cfg.Catalog.Catalog()
	var err error
	if sc.Builds.Switch > 0 || sc.Builds.Flap > 0 {
		if r.builds[buildOld], err = variant(def, cfg.Backfill, nil); err != nil {
			return err
		}
	}
	if sc.Builds.Retire > 0 {
		if _, vv, ok := cfg.Catalog.Projector().Version(cfg.Retire); !ok || slices.Contains(cfg.Backfill, cfg.Retire) || !vv.FencedHandoff() {
			return fmt.Errorf("retiring %q needs an additive version that is live from the start", cfg.Retire)
		}
		retire := []string{cfg.Retire}
		if r.builds[buildRetiring], err = variant(def, retire, retire); err != nil {
			return err
		}
	}
	return nil
}

// variant compiles def without the view versions in drop, listing retired as retired.
func variant(def *catalog.Catalog, drop, retired []string) (*projector.Compiled, error) {
	v := *def
	v.Views, v.Retired = nil, retired
	for _, view := range def.Views {
		view.Versions = slices.DeleteFunc(slices.Clone(view.Versions), func(vv catalog.ViewVersion) bool {
			return slices.Contains(drop, catalog.VersionID(view.Name, vv.Number))
		})
		if len(view.Versions) > 0 {
			v.Views = append(v.Views, view)
		}
	}
	return projector.Compile(&v)
}

// deploy moves the build that the region's new containers run, as Builds says.
func (rg *region) deploy() {
	r := rg.run
	b := r.sc.Builds
	id := rg.name + " deploy"
	rng := r.actorRNG(id)
	defer func() {
		r.sched.gate(id, "done")
		r.settle(1)
	}()
	to := func(after time.Duration, build int) bool {
		time.Sleep(after)
		r.sched.gate(id, "build "+strconv.Itoa(build))
		if r.stopped() {
			return false
		}
		rg.build = build
		r.count(&r.stats.Deploys)
		return true
	}
	switch {
	case b.Flap > 0:
		if to(between(rng, 0, b.Flap), buildOld) {
			to(between(rng, 1, b.Flap), buildFull)
		}
	case b.Retire > 0:
		if to(between(rng, 0, b.Retire), buildRetiring) && to(between(rng, 0, b.Retire), buildFull) && !chance(rng, keepOlder) {
			to(between(rng, 1, b.Retire), buildRetiring)
		}
	case b.Switch > 0:
		if !to(between(rng, 0, b.Switch), buildFull) || b.Rollback == 0 {
			return
		}
		for rg.backfilling > 0 {
			time.Sleep(backfillPoll)
			r.sched.gate(id, "backfills")
			if r.stopped() {
				return
			}
		}
		if to(between(rng, 0, b.Rollback), buildOld) {
			to(between(rng, 1, b.Rollback), buildFull)
		}
	}
}

// retire writes the region's tombstone of Config.Retire at a time up to Builds.Retire, as
// dynaflux retire or a deploy does, unless the region is one that doesn't get it that way.
func (rg *region) retire() {
	r := rg.run
	id := rg.name + " retire"
	rng := r.actorRNG(id)
	defer func() {
		r.sched.gate(id, "done")
		r.settle(1)
	}()
	if chance(rng, neverRetired) {
		return
	}
	time.Sleep(between(rng, 0, r.sc.Builds.Retire))
	r.sched.gate(id, "tombstone")
	if r.stopped() {
		return
	}
	_, vv, _ := r.cfg.Catalog.Projector().Version(r.cfg.Retire)
	if _, err := meta.New(rg.mem, r.meta).Retire(context.Background(), r.cfg.Retire, vv.Stream); err != nil {
		r.violatef("retire: %s: %v", rg.name, err)
		return
	}
	rg.tombstoned()
}

// noteTombstone notes a write of the meta table that retires Config.Retire, as a core of the
// retiring build makes before it skips the version. Only gated code calls it.
func (rg *region) noteTombstone(it store.Item) {
	r := rg.run
	if r.sc.Builds.Retire > 0 && it[catalog.AttrPK].Str == "view" && it[catalog.AttrSK].Str == r.cfg.Retire &&
		it["state"].Str == string(meta.Retired) {
		rg.tombstoned()
	}
}

// tombstoned records the first time the region's tombstone of Config.Retire was stored.
func (rg *region) tombstoned() {
	if rg.retiredAt.IsZero() {
		rg.retiredAt = time.Now()
		rg.run.count(&rg.run.stats.Tombstones)
	}
}

// checkRetired checks P1 on a write to table, a view's: once the region's tombstone of
// Config.Retire could be cached, no invocation that starts writes its table. Only gated code
// calls it.
func (rg *region) checkRetired(ctx context.Context, table string) {
	r := rg.run
	if rg.retiredAt.IsZero() || r.views[table] != r.cfg.Retire {
		return
	}
	if inv := invocationOf(ctx); !inv.start.Before(rg.retiredAt.Add(meta.MaxViewStateAge)) {
		r.violatef("P1: %s: %s wrote retired %s, starting %v after its tombstone", rg.name, inv.id, r.cfg.Retire, inv.start.Sub(rg.retiredAt))
	}
}

// retiredIn reports whether view is the version the run retires and region stores its
// tombstone, which P1 and P2 leave out. A region without the tombstone serves the version
// as live, so it must be exact there.
func (r *run) retiredIn(name, view string) bool {
	if r.sc.Builds.Retire == 0 || view != r.cfg.Retire {
		return false
	}
	i := slices.IndexFunc(r.regions, func(rg *region) bool { return rg.name == name })
	return i >= 0 && !r.regions[i].retiredAt.IsZero()
}

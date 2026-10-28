// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// History shapes the history of each region's archive (ADR 0017): deploys that stop and
// start archiving a stream, origins that a CLI records, their clocks, and projector stalls.
type History struct {
	// Deployed is how long after the deploy that created the run's tables and registered its
	// catalog the writers, the backfills, the CLI and the archive's deploys start.
	Deployed time.Duration
	// Origins, when set, leaves each region's origins to a CLI that tries to record them
	// from a time up to Origins on, about once a minute, as backfill -all does; otherwise
	// each region records them when the run starts.
	Origins time.Duration
	// Toggle is the chance that a region's deploys stop archiving a stream at a time up to
	// ToggleWithin, and start it again up to ToggleWithin later. A deploy takes up to
	// Register from changing the functions to registering its catalog. Rollback is the chance
	// that the deploy that stops archiving fails once the functions changed, and
	// CloudFormation rolls it back up to ToggleWithin later: the functions and ArchiveMode,
	// with the properties swapped, in either order, since CloudFormation doesn't document
	// it, and up to rollbackGap apart; no catalog is registered.
	Toggle       float64
	ToggleWithin time.Duration
	Register     time.Duration
	Rollback     float64
	// Skew is how far the clocks of the CLI and of the setup function are off, either way.
	Skew time.Duration
	// Stall is the chance that a region's mappings stop delivering, at a time up to
	// StallWithin, for StallFor and up to an hour more.
	Stall       float64
	StallWithin time.Duration
	StallFor    time.Duration
}

func (h History) check() error {
	switch {
	case h.Deployed < 0 || h.Origins < 0 || h.ToggleWithin < 0 || h.Register < 0 || h.Skew < 0 || h.StallWithin < 0 || h.StallFor < 0:
		return fmt.Errorf("history %+v: a time is negative", h)
	case h.Toggle < 0 || h.Toggle > 1 || h.Rollback < 0 || h.Rollback > 1 || h.Stall < 0 || h.Stall > 1:
		return fmt.Errorf("history %+v: needs toggle, rollback and stall chances from 0 to 1", h)
	case h.Toggle > 0 && h.ToggleWithin == 0, h.Stall > 0 && h.StallFor == 0:
		return fmt.Errorf("history %+v: a toggle or a stall needs its times", h)
	}
	return nil
}

// day is how long a change stream keeps a record: progress that passes a record older than
// that passes one that DynamoDB may have trimmed.
const day = 24 * time.Hour

// registration is a catalog that a region registered, and the streams it doesn't archive.
type registration struct {
	at  time.Time
	off map[string]bool
}

// archivedEverywhere returns def with every stream archived, with every event, in regions.
func archivedEverywhere(def *catalog.Catalog, regions []string) *catalog.Catalog {
	out := *def
	out.Streams = slices.Clone(def.Streams)
	for i := range out.Streams {
		out.Streams[i].Archive = &catalog.Archive{Regions: regions, Events: catalog.ArchiveEventsAll}
	}
	return &out
}

// regionCatalog returns the catalog that region registers when its functions don't archive
// the streams in off, and notes them by its hash.
func (r *run) regionCatalog(region string, off map[string]bool) ([]byte, error) {
	def := archivedEverywhere(r.cfg.Catalog.Catalog(), r.regionNames)
	for i := range def.Streams {
		if s := &def.Streams[i]; off[s.Name] {
			s.Archive.Regions = slices.DeleteFunc(slices.Clone(s.Archive.Regions), func(n string) bool { return n == region })
		}
	}
	data, err := catalog.Marshal(def)
	if err == nil {
		r.exactOff[catalog.Hash(data)] = maps.Clone(off)
	}
	return data, err
}

// openHistory records what a first deploy and, without a CLI, backfill -all record in a new
// region (ADR 0017): each stream's mode, all, and its origin.
func (rg *region) openHistory(ctx context.Context) error {
	r := rg.run
	m := meta.New(rg.mem, r.meta)
	now := time.Now()
	modes := map[string]meta.ArchiveMode{}
	for _, s := range rg.order {
		modes[s.name] = meta.ArchiveAll
	}
	if err := m.ApplyArchiveModes(ctx, nil, modes, true, now); err != nil {
		return err
	}
	rg.registered = []registration{{at: now, off: map[string]bool{}}}
	if r.sc.History.Origins > 0 {
		return nil
	}
	for _, s := range rg.order {
		item, _, err := m.Stream(ctx, s.name)
		if err == nil {
			o := meta.Origin{At: now, Hash: r.catalogHash, RegisteredAt: now, Born: r.started, ARN: rg.streamARN(s.table)}
			_, err = m.RecordOrigin(ctx, item, o)
		}
		if err != nil {
			return err
		}
		rg.originAt[s.name] = now
	}
	return nil
}

func (rg *region) streamARN(table string) string { return "arn:sim:" + rg.name + ":" + table }

// witness is what AWS shows of the region's streams, behind the gates: their ARNs, a mapping
// on each, and no gap.
type witness struct{ rg *region }

func (w witness) StreamARN(ctx context.Context, table string) (string, error) {
	lat, f := w.rg.enter(ctx, kindRead, "describe stream "+table)
	defer time.Sleep(lat)
	if f == faultTransient {
		return "", errTransient
	}
	return w.rg.streamARN(table), nil
}

func (w witness) Facts(ctx context.Context, table string, _ meta.StreamItem) (backfill.Facts, error) {
	lat, f := w.rg.enter(ctx, kindRead, "history "+table)
	defer time.Sleep(lat)
	if f == faultTransient {
		return backfill.Facts{}, errTransient
	}
	return backfill.Facts{ARN: w.rg.streamARN(table), Mapping: true}, nil
}

// archiving is the region's archive as a container's functions use it: they don't archive
// the streams in off.
type archiving struct {
	a   Archiver
	off map[string]bool
}

func (a archiving) Archive(ctx context.Context, stream, unit string, recs []projector.Record) error {
	if a.off[stream] {
		return nil
	}
	return a.a.Archive(ctx, stream, unit, recs)
}

// toggleArchive runs the region's deploys that stop archiving a stream, then start it again,
// as History.Toggle says.
func (rg *region) toggleArchive() {
	r := rg.run
	h := r.sc.History
	id := rg.name + " archive deploy"
	rng := r.actorRNG(id)
	defer func() {
		r.sched.gate(id, "done")
		r.settle(1)
	}()
	if !chance(rng, h.Toggle) {
		return
	}
	streams := map[string]bool{}
	for _, v := range r.cfg.Backfill {
		_, vv, _ := r.cfg.Catalog.Projector().Version(v)
		streams[vv.Stream] = true
	}
	choice := slices.Sorted(maps.Keys(streams))
	stream := choice[rng.IntN(len(choice))]
	skew := between(rng, -h.Skew, h.Skew)
	time.Sleep(h.Deployed + between(rng, 0, h.ToggleWithin))
	if rg.deployArchive(id, rng, skew, stream, false) {
		time.Sleep(between(rng, 1, h.ToggleWithin))
		rg.deployArchive(id, rng, skew, stream, true)
	}
}

// deployArchive is a deploy that stops or starts archiving stream (ADR 0017 §2): ArchiveMode
// records the modes, a hole among them, before the functions change; then the deploy
// registers its catalog, or one that stops archiving may roll back. It reports whether the
// deploy ended registered. clock is the setup function's offset.
func (rg *region) deployArchive(id string, rng *rand.Rand, clock time.Duration, stream string, archive bool) bool {
	r := rg.run
	ctx := withInvocation(context.Background(), &invocation{id: id, stallAt: -1})
	old, off := rg.off, maps.Clone(rg.off)
	if archive {
		delete(off, stream)
	} else {
		off[stream] = true
	}
	r.sched.gate(id, "deploy "+stream)
	if r.stopped() {
		return false
	}
	if !rg.recordModes(ctx, old, off, clock) {
		return false
	}
	rg.changeFunctions(stream, off)
	if h := r.sc.History; !archive && chance(rng, h.Rollback) {
		time.Sleep(between(rng, 1, h.ToggleWithin))
		rg.rollBack(ctx, id, rng, stream, old, off, clock)
		return false
	}
	time.Sleep(between(rng, 0, r.sc.History.Register))
	r.sched.gate(id, "register")
	data, err := r.regionCatalog(rg.name, off)
	if err != nil {
		r.violatef("harness: %s: %v", id, err)
		return false
	}
	for !r.stopped() {
		_, err = meta.New(rg.store, r.meta).RegisterCatalog(ctx, data, "sim")
		if err == nil {
			r.count(&r.stats.Toggles)
			return true
		}
		if !injected(err) {
			r.violatef("harness: %s: register: %v", id, err)
			return false
		}
		time.Sleep(stepRetry)
	}
	return false
}

// rollbackGap is how long apart CloudFormation may roll back ArchiveMode and the functions, and
// rollbackModesFirst the chance that it rolls back ArchiveMode first.
const (
	rollbackGap        = 2 * time.Minute
	rollbackModesFirst = 0.5
)

// rollBack rolls back a deploy that stopped archiving stream, from off to old: the functions
// and ArchiveMode, in either order.
func (rg *region) rollBack(ctx context.Context, id string, rng *rand.Rand, stream string, old, off map[string]bool, clock time.Duration) {
	r := rg.run
	modesFirst := chance(rng, rollbackModesFirst)
	gap := between(rng, 1, rollbackGap)
	r.sched.gate(id, "roll back")
	if !modesFirst {
		rg.changeFunctions(stream, old)
		time.Sleep(gap)
		r.sched.gate(id, "roll back archive modes")
	}
	if !rg.recordModes(ctx, off, old, clock) {
		return
	}
	r.count(&r.stats.Toggles)
	if modesFirst {
		time.Sleep(gap)
		r.sched.gate(id, "roll back functions")
		rg.changeFunctions(stream, old)
	}
}

// recordModes sends ArchiveMode's update from the functions' archiving old to next until it
// succeeds, as the setup function's retries and CloudFormation's do.
func (rg *region) recordModes(ctx context.Context, old, next map[string]bool, clock time.Duration) bool {
	r := rg.run
	modes := func(off map[string]bool) map[string]meta.ArchiveMode {
		out := map[string]meta.ArchiveMode{}
		for _, s := range rg.order {
			out[s.name] = meta.ArchiveAll
			if off[s.name] {
				out[s.name] = meta.ArchiveOff
			}
		}
		return out
	}
	for !r.stopped() {
		err := meta.New(rg.store, r.meta).ApplyArchiveModes(ctx, modes(old), modes(next), false, time.Now().Add(clock))
		if err == nil {
			return true
		}
		if !injected(err) {
			r.violatef("harness: %s: archive modes: %v", rg.name, err)
			return false
		}
		time.Sleep(stepRetry)
	}
	return false
}

// changeFunctions makes the region's new containers archive all but the streams in off, and
// checks P14: the stream item already holds a mode hole after its origin.
func (rg *region) changeFunctions(stream string, off map[string]bool) {
	r := rg.run
	rg.off, rg.gen = off, rg.gen+1
	rg.changes[stream] = append(rg.changes[stream], time.Now())
	item, _, err := meta.New(rg.mem, r.meta).Stream(context.Background(), stream)
	switch {
	case err != nil:
		r.violatef("harness: %s: read the stream item of %s: %v", rg.name, stream, err)
	case item.Origin != nil && !slices.ContainsFunc(item.After(), modeHole):
		r.violatef("P14: %s: the functions changed archiving %s, with no hole after its origin", rg.name, stream)
	}
}

func modeHole(h meta.Hole) bool { return h.Cause == meta.HoleUpdate || h.Cause == meta.HoleCreate }

// recordOrigins is the region's CLI, which tries to record each stream's origin about once a
// minute, as backfill -all does, by a clock of its own.
func (rg *region) recordOrigins() {
	r := rg.run
	h := r.sc.History
	id := rg.name + " cli"
	rng := r.actorRNG(id)
	defer func() {
		r.sched.gate(id, "done")
		r.settle(1)
	}()
	ctx := withInvocation(context.Background(), &invocation{id: id, stallAt: -1})
	env := rg.backfillEnv()
	skew := between(rng, -h.Skew, h.Skew)
	env.Now = func() time.Time { return time.Now().Add(skew) }
	time.Sleep(h.Deployed + between(rng, 0, h.Origins))
	for range originTries {
		r.sched.gate(id, "backfill -all")
		if r.stopped() {
			return
		}
		for _, s := range rg.order {
			st, err := backfill.RecordOrigin(ctx, &env, s.name)
			switch {
			case st.State == backfill.OriginRecorded:
				r.count(&r.stats.Origins)
			case err != nil && !injected(err):
				r.violatef("harness: %s: record the origin of %s: %v", id, s.name, err)
			}
		}
		time.Sleep(between(rng, originGapLeast, originGapMost))
	}
}

// The CLI runs backfill -all originTries times, each originGapLeast to originGapMost after the
// one before.
const (
	originTries    = 8
	originGapLeast = 30 * time.Second
	originGapMost  = 90 * time.Second
)

// stalled returns how long the region's mappings stay stalled from now.
func (rg *region) stalled() time.Duration {
	if now := time.Now(); now.Before(rg.stallFrom) {
		return 0
	}
	return max(0, time.Until(rg.stallTo))
}

// noteHistory notes a write of the meta table that replaced cur with next, for P14. Only
// gated code calls it.
func (rg *region) noteHistory(ctx context.Context, cur, next store.Item) {
	switch k := store.KeyOf(next); {
	case strings.HasPrefix(k.PK, streamPrefix):
		rg.noteStreamItem(cur, next)
	case k == store.Key{PK: "catalog", SK: "current"}:
		rg.noteRegistration(next)
	case k.PK != "job" || next["split"].Str != string(meta.SplitFence):
	case cur["phase"].Str == string(meta.PhaseStage) && next["phase"].Str == string(meta.PhaseApply):
		rg.checkSelect(ctx, next["stream"].Str, k.SK)
	case !errorsDone(cur) && errorsDone(next):
		rg.checkErrors(ctx, next["stream"].Str, k.SK)
	}
}

// errorsDone reports whether a job item's stage holds the error prefix read.
func errorsDone(it store.Item) bool { return it["stage"].Map["errors"].Map["done"].Bool }

// checkErrors checks P14 on a write that marked the error prefix of a job of stream, split by
// fences, read: no commit failure's object had landed when its step last listed the prefix.
func (rg *region) checkErrors(ctx context.Context, stream, version string) {
	actor := invocationOf(ctx).id
	at, listed := rg.listed[actor+" "+stream]
	if !listed {
		rg.run.violatef("P14: %s: %s marked the error prefix of the job of %s read without listing it", rg.name, actor, version)
		return
	}
	for name, o := range rg.archive.objects {
		if o.stream == stream && o.failed && !o.due.After(at) {
			rg.run.violatef("P14: %s: %s read the error prefix of %s for %s past %s, a commit that failed", rg.name, actor, stream, version, name)
			return
		}
	}
}

// noteStreamItem notes a write of a stream item: an origin lands only while the region's
// functions archive the stream (P14). An invocation of older functions may still run, but
// the stream held no event when the origin's scan was sent.
func (rg *region) noteStreamItem(cur, next store.Item) {
	o, ok := next["origin"]
	if !ok || reflect.DeepEqual(cur["origin"], o) {
		return
	}
	stream := strings.TrimPrefix(next[catalog.AttrPK].Str, streamPrefix)
	rg.originAt[stream] = time.Now()
	if rg.off[stream] {
		rg.run.violatef("P14: %s: the origin of %s landed while the functions didn't archive it", rg.name, stream)
	}
}

// streamPrefix starts the partition key of a stream item (encoding.md §8.5).
const streamPrefix = "stream#"

// noteRegistration notes a write of the current catalog, and which streams it doesn't
// archive in the region.
func (rg *region) noteRegistration(it store.Item) {
	off, ok := rg.run.exactOff[it["hash"].Str]
	if !ok {
		rg.run.violatef("harness: %s: registered catalog %s isn't one the run made", rg.name, it["hash"].Str)
	}
	rg.registered = append(rg.registered, registration{at: time.Now(), off: off})
}

// checkLag checks P14 on a write that raised a unit's progress from "from" to to: when it
// passed a record that entered the stream a day before, a gap the model made, the stream item
// holds a lag after its origin. Only gated code calls it.
func (rg *region) checkLag(pk, from, to string) {
	r := rg.run
	stream, key, _ := strings.Cut(strings.TrimPrefix(pk, unitPrefix), "#")
	u := rg.units[key]
	if u == nil {
		return
	}
	old := time.Now().Add(-day)
	passed := slices.ContainsFunc(u.recs, func(rec projector.Record) bool {
		seq, err := keys.PadSeq(rec.Seq)
		return err == nil && seq > from && seq <= to && !rec.Arrival.After(old)
	})
	if !passed {
		return
	}
	rg.lagged[stream] = append(rg.lagged[stream], time.Now())
	r.count(&r.stats.Lags)
	item, ok, err := meta.New(rg.mem, r.meta).Stream(context.Background(), stream)
	switch {
	case err != nil:
		r.violatef("harness: %s: read the stream item of %s: %v", rg.name, stream, err)
	case ok && !slices.ContainsFunc(item.After(), func(h meta.Hole) bool { return h.Cause == meta.HoleLag }):
		r.violatef("P14: %s: progress of %s passed a record a day old, with no lag after the origin of %s", rg.name, u.id, stream)
	}
}

// checkSelect checks P14 on a write that moved a job of stream, split by fences, from stage
// to apply: its step read the stream item when, by the model, the archive was complete.
func (rg *region) checkSelect(ctx context.Context, stream, version string) {
	actor := invocationOf(ctx).id
	at, read := rg.reads[actor+" "+stream]
	if !read {
		rg.run.violatef("P14: %s: %s moved the job of %s to apply without reading the stream item", rg.name, actor, version)
		return
	}
	if why := rg.incomplete(stream, at); why != "" {
		rg.run.violatef("P14: %s: %s moved the job of %s to apply, but when it read the stream item %s", rg.name, actor, version, why)
	}
}

// incomplete says why, by the model, the region's archive of stream wasn't complete at t, ""
// when it was: no origin had landed, the registered catalog didn't archive the stream, or
// since the origin, the functions changed archiving it or progress passed a record a day old.
func (rg *region) incomplete(stream string, t time.Time) string {
	o, ok := rg.originAt[stream]
	if !ok || o.After(t) {
		return "no origin had landed"
	}
	i := slices.IndexFunc(rg.registered, func(reg registration) bool { return reg.at.After(t) })
	if i < 0 {
		i = len(rg.registered)
	}
	if i == 0 || rg.registered[i-1].off[stream] {
		return "the registered catalog didn't archive it"
	}
	after := func(ts []time.Time) bool {
		return slices.ContainsFunc(ts, func(c time.Time) bool { return c.After(o) && !c.After(t) })
	}
	switch {
	case after(rg.changes[stream]):
		return "the functions had changed archiving it since its origin"
	case after(rg.lagged[stream]):
		return "progress had passed a record a day old since its origin"
	}
	return ""
}

// checkIncomplete checks P14 on the jobs of a region at quiescence: a job that ended broken
// left its version broken, and one that failed names an archive rule and left its version
// short of live. It returns the versions of the failed jobs, whose archive wasn't complete.
func (r *run) checkIncomplete(ctx context.Context, rg *region, states map[string]meta.ViewState) map[string]bool {
	jobs, err := meta.New(rg.mem, r.meta).Jobs(ctx)
	if err != nil {
		r.violatef("P14: %s: read the jobs: %v", rg.name, err)
		return nil
	}
	out := map[string]bool{}
	for _, j := range jobs {
		vs := states[j.Version].State
		switch {
		case j.State == meta.JobBroken && vs != meta.Broken:
			r.violatef("P14: %s: the job of %s ended broken, but its version is %s: %+v", rg.name, j.Version, vs, j.Error)
		case j.State != meta.JobFailed || vs == meta.Broken:
		case j.Error == nil || !strings.Contains(j.Error.Message, backfill.ErrIncomplete.Error()) || vs == meta.Live:
			r.violatef("P14: %s: the job of %s failed, but not on an archive rule, or its version is live: %+v", rg.name, j.Version, j.Error)
		default:
			out[j.Version] = true
			r.count(&r.stats.Incomplete)
		}
	}
	return out
}

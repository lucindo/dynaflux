// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package simulation

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
)

const (
	stepRetry     = time.Second // before the state machine repeats a step that failed
	backfillPoll  = 10 * time.Second
	backfillBytes = 2560 << 10 // a step's memory: runs of a few dozen records, merged in passes
	stepRequests  = 8          // what a step sends for, as a batch sends for its records
	crashWithin   = 200        // a crashed step dies at one of its first crashWithin requests
	twinSteps     = 5          // the most steps a duplicate worker takes
	execution     = "local"
)

// driver backfills one view version in one region as dynaflux backfill and the state machine
// do: backfill.Start, then backfill.Step in a loop, each step an invocation of a backfill
// worker on a container of the full build, honoring the wait it returns. A step that fails is
// repeated. The driver adds a production run's faults: a step that dies at a random request,
// after which the job is claimed for a new attempt; a duplicate worker that steps the same
// attempt for a while, with another deadline; and staged runs lost before apply.
type driver struct {
	rg      *region
	id      string
	version string
	stream  *stream
	rng     *rand.Rand
	attempt int
	steps   int
	twins   int
	kept    bool         // whether the job keeps its staged runs from now on: it lost them, or never will
	env     backfill.Env // each step's container gives its Core, and its name its Entropy
}

// drivers returns the region's drivers, one per version the run backfills.
func (rg *region) drivers() []*driver {
	r := rg.run
	var out []*driver
	for _, v := range r.cfg.Catalog.Catalog().Views {
		for i := range v.Versions {
			vv := &v.Versions[i]
			id := catalog.VersionID(v.Name, vv.Number)
			if !r.backfills(id) {
				continue
			}
			actor := rg.name + " backfill " + id
			s := rg.streams[r.cfg.Naming.StreamTable(vv.Stream)]
			out = append(out, &driver{rg: rg, id: actor, version: id, stream: s, rng: r.actorRNG(actor), env: rg.backfillEnv()})
		}
	}
	return out
}

// backfillEnv returns a step's environment in the region: its store, archive and staging
// behind the gates.
func (rg *region) backfillEnv() backfill.Env {
	r := rg.run
	return backfill.Env{
		Naming:  r.cfg.Naming,
		Catalog: r.cfg.Catalog.Catalog(),
		Hash:    r.catalogHash,
		Meta:    meta.New(rg.store, r.meta),
		Store:   rg.store,
		Units:   lister{rg},
		Tables:  tables{rg},
		Archive: archiveReader{rg},
		Runs:    gatedRuns{rg},
		Lag:     lagger{rg},
		Bounds:  meta.DefaultBounds,
		Memory:  backfillBytes,
		Chunk:   r.sc.Backfill.Chunk,
		Log:     slog.New(slog.DiscardHandler),
	}
}

func (d *driver) run() {
	r := d.rg.run
	defer func() {
		r.sched.gate(d.id, "done")
		d.rg.backfilling--
		r.settle(1)
	}()
	start := between(d.rng, 0, r.sc.Backfill.Within)
	if r.sc.Backfill.Together {
		start = d.rg.backfillStart
	}
	time.Sleep(start)
	if !d.start() {
		return
	}
	d.kept = !chance(d.rng, r.sc.Backfill.Lose)
	for !r.stopped() {
		d.lose()
		if chance(d.rng, r.sc.Backfill.Twin) {
			d.twin()
		}
		crashAt := -1
		if chance(d.rng, r.sc.Backfill.Crash) {
			crashAt = d.rng.IntN(crashWithin)
		}
		d.steps++
		wait, done, err := d.step(d.id+" #"+strconv.Itoa(d.steps), d.rng, d.attempt, r.sc.Backfill.Step, crashAt)
		switch {
		case err == nil && done:
			d.finish()
			return
		case err == nil:
			time.Sleep(wait)
		case crashAt >= 0 && errors.Is(err, errCrashed):
			r.count(&r.stats.Resumed)
			if !d.start() {
				return
			}
		case d.failed(err):
			return
		default:
			time.Sleep(stepRetry)
		}
	}
}

// start creates the job, or claims it for a new attempt, as dynaflux backfill does. The job
// reads the archive, split at fences, or with Backfill.Table the stream table: an additive
// version split by time, an idempotent one not split.
func (d *driver) start() bool {
	r := d.rg.run
	ctx := withInvocation(context.Background(), &invocation{id: d.id, stallAt: -1})
	split := meta.SplitFence
	switch {
	case r.sc.Backfill.Table && r.fenced[d.version]:
		split = meta.SplitTime
	case r.sc.Backfill.Table:
		split = meta.SplitNone
	}
	req := backfill.Request{Version: d.version, Stream: d.stream.name, Name: d.version, Execution: func(int) string { return execution }, Split: split, Rate: r.sc.Backfill.Rate}
	for !r.stopped() {
		j, err := backfill.Start(ctx, d.env.Meta, req)
		switch {
		case err == nil:
			d.attempt = j.Attempt
			return true
		case errors.Is(err, meta.ErrTransition):
			// The version broke or was retired while the job ran: dynaflux backfill refuses it
			// (DF7502).
			return false
		case errors.Is(err, meta.ErrJobExists):
			// The step that died, or a duplicate worker, ended the job.
			d.finish()
			return false
		case d.failed(err):
			return false
		}
		time.Sleep(stepRetry)
	}
	return false
}

// step runs one step of attempt as an invocation of a backfill worker that times out after
// timeout and, unless crashAt is negative, dies at that request.
func (d *driver) step(id string, rng *rand.Rand, attempt int, timeout time.Duration, crashAt int) (time.Duration, bool, error) {
	var wait time.Duration
	var done bool
	o := d.rg.invoke(id, rng, invocationSpec{n: stepRequests, worker: true, timeout: timeout, crashAt: crashAt},
		func(ctx context.Context, core Core) (projector.Result, error) {
			env := d.env
			env.Core = core
			seed := sha256.Sum256(fmt.Appendf(nil, "%d %s", d.rg.run.seed, id))
			env.Entropy = rand.NewChaCha8(seed)
			var err error
			wait, done, err = backfill.Step(ctx, &env, d.version, attempt, execution)
			return projector.Result{}, err
		})
	if o.err != nil {
		// A step that timed out may still run: only a finished one has set wait and done.
		return 0, false, o.err
	}
	return wait, done, nil
}

// failed reports, as a violation, an error that no fault explains.
func (d *driver) failed(err error) bool {
	for _, injected := range []error{errTransient, errAmbiguous, errTimeout, errCrashed, context.DeadlineExceeded} {
		if errors.Is(err, injected) {
			return false
		}
	}
	d.rg.run.violatef("backfill: %s: %v", d.id, err)
	return true
}

// twin starts a duplicate worker of the current attempt, as a step that Step Functions
// repeated while the first still ran, or a continuation that overlaps its predecessor. It
// takes a few steps, each with a deadline of its own, so its runs have other extents.
func (d *driver) twin() {
	r := d.rg.run
	r.count(&r.stats.Twins)
	d.twins++
	id := d.id + " twin " + strconv.Itoa(d.twins)
	attempt, steps := d.attempt, 1+d.rng.IntN(twinSteps)
	timeout := between(d.rng, r.sc.Backfill.Step/2+time.Minute/2, r.sc.Backfill.Step)
	r.sched.goActor(func() {
		rng := r.actorRNG(id)
		for i := range steps {
			if r.stopped() {
				break
			}
			wait, done, err := d.step(id+" #"+strconv.Itoa(i+1), rng, attempt, timeout, -1)
			if done || err != nil && d.failed(err) {
				break
			}
			time.Sleep(max(wait, stepRetry))
		}
		r.sched.gate(id, "done")
	})
}

// lose deletes the job's staged runs once it applies them, as their lifecycle rule would,
// unless the job lost them already or is not to.
func (d *driver) lose() {
	r := d.rg.run
	if d.kept {
		return
	}
	r.sched.gate(d.id, "lose")
	j, ok, err := meta.New(d.rg.mem, r.meta).Job(context.Background(), d.version)
	if err != nil || !ok || j.Phase != meta.PhaseApply {
		return
	}
	d.kept = true
	r.count(&r.stats.Lost)
	if err = d.rg.runs.DeletePrefix(context.Background(), j.Staging); err != nil {
		r.violatef("harness: %s: delete the staged runs: %v", d.id, err)
	}
}

// finish counts what the job did.
func (d *driver) finish() {
	r := d.rg.run
	j, _, err := meta.New(d.rg.mem, r.meta).Job(context.Background(), d.version)
	if err != nil {
		r.violatef("harness: %s: read the job: %v", d.id, err)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats.Chunks += j.Chunks
	if j.State == meta.JobDone {
		r.stats.Backfills++
	}
	if j.Fresh {
		r.stats.Fresh++
	}
}

// lister lists a stream's units as DescribeStream does: all but those that every mapping
// finished delivering, which stand for units past the stream's retention.
type lister struct{ rg *region }

func (l lister) Units(ctx context.Context, table string) ([]string, error) {
	lat, f := l.rg.enter(ctx, kindRead, "list "+table)
	defer time.Sleep(lat)
	if f == faultTransient {
		return nil, errTransient
	}
	s := l.rg.streams[table]
	s.listed = true
	var out []string
	for _, u := range s.units {
		if u.finished.IsZero() {
			out = append(out, u.id)
		}
	}
	return out, nil
}

// tables describes the stream tables, which every region created when the run started.
type tables struct{ rg *region }

func (t tables) Born(ctx context.Context, table string) (time.Time, bool, error) {
	lat, f := t.rg.enter(ctx, kindRead, "describe "+table)
	defer time.Sleep(lat)
	if f == faultTransient {
		return time.Time{}, false, errTransient
	}
	return t.rg.run.started, false, nil
}

// lagger reports the region's projector lag: the age of the oldest record a started mapping
// hasn't delivered.
type lagger struct{ rg *region }

func (l lagger) Lag(ctx context.Context) (time.Duration, error) {
	lat, f := l.rg.enter(ctx, kindRead, "lag")
	defer time.Sleep(lat)
	if f == faultTransient {
		return 0, errTransient
	}
	now, lag := time.Now(), time.Duration(0)
	for _, s := range l.rg.order {
		for _, u := range s.units {
			for _, ln := range u.lanes {
				if !ln.m.start.After(now) && ln.acked < len(ln.at) {
					lag = max(lag, now.Sub(ln.at[ln.acked]))
				}
			}
		}
	}
	return lag, nil
}

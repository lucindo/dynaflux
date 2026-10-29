// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
)

func backfillCommand() command {
	return command{
		name:    "backfill",
		args:    "[flags] <view>[@v<N>]... | -all",
		summary: "fill view versions in the deployed store, so they go live",
		doc: `Backfill fills view versions in each region of the deployed store, with a job that
the region's backfill state machine runs (docs/spec/lifecycle.md §5). A view
without @v<N> names the version that dynaflux.lock makes current. -all names every
version of each region's deployed catalog that isn't live, broken or retired, and
whose job wasn't cancelled.

First, in each region that archives a stream with events: all, backfill records
the stream's origin while the stream has no events yet, so that the region's
archive of it is complete from then on (docs/spec/lifecycle.md §5.5): -all for
every stream, and otherwise for the streams of the versions it names. It prints a
line for each origin it records, and with -all warns about a stream table less
than 23 hours old that still has none.

Before it starts jobs, backfill prints what it will do in each region: start a
job, resume an interrupted or cancelled one, leave a running one, or nothing for a
live version. A new job activates its version, which can't be undone, so backfill
asks first; -yes answers for it. A new job reads the region's archive when it is
complete; otherwise the stream table, split by time for a version that counts or
sums. -source chooses.

-rate limits the records each job applies a second, and changes it on a running
job. -cancel stops the jobs, and the Athena query a job runs on the archive; the
versions stay activating. -watch follows the jobs until they end; interrupting it
leaves them running. With -json, -watch prints a JSON line with "format": 1, then
one for each change of a job, and the plan goes to stderr.

Exit status: 0 when nothing was refused, and with -watch every version is live; 1
when a version was refused, or with -watch one broke, its job failed, or the
watch was interrupted; and 2 for usage, I/O and AWS access problems.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			o := backfillOptions{}
			fs.Var(&o.prefix, "prefix", "the `prefix` of the environment's resource names (default prefix in dynaflux.yaml)")
			fs.Var(&o.regions, "region", "a `region` to fill, one of regions in dynaflux.yaml; repeat it for each one (default all of them)")
			fs.BoolVar(&o.all, "all", false, "fill every version that isn't live, broken or retired")
			fs.StringVar((*string)(&o.source), "source", "", "the `source` of a new job: table or archive (default chosen in each region)")
			fs.Var(&o.rate, "rate", "apply at most `n` records a second in each job; 0 for no limit")
			fs.BoolVar(&o.watch, "watch", false, "follow the jobs until they end")
			fs.BoolVar(&o.json, "json", false, "with -watch, print JSON lines")
			fs.BoolVar(&o.cancel, "cancel", false, "stop the jobs and their archive queries")
			fs.BoolVar(&o.yes, "yes", false, "start jobs without asking")
			return func(c *cli, args []string) int { return c.backfill(o, args) }
		},
	}
}

type backfillOptions struct {
	prefix  optionalString
	regions repeated
	all     bool
	source  backfill.Source
	rate    optionalInt
	watch   bool
	json    bool
	cancel  bool
	yes     bool
}

func (c *cli) backfill(o backfillOptions, args []string) int {
	if status := c.checkBackfillUsage(o, args); status != exitOK {
		return status
	}
	targets, status := c.backfillTargets(o, args)
	if status != exitOK {
		return status
	}
	cfg, status := c.loadConfig()
	if status != exitOK {
		return status
	}
	f, status := c.openFleet(c.ctx, "backfill", cfg, o.prefix, o.regions, "")
	if status != exitOK {
		return status
	}
	plans, status := f.plan(c, o, targets)
	status = max(status, c.printPlan(plans, o))
	starts := 0
	for _, p := range plans {
		for _, it := range p.items {
			switch {
			case it.usage:
				return exitUsage
			case it.act == actStart:
				starts++
			}
		}
	}
	if starts > 0 {
		c.errorf("A new job activates its version, which can't be undone: its source and split are fixed for its life.")
		if s := c.confirm("backfill", fmt.Sprintf("start %d backfill %s?", starts, plural(starts, "job", "jobs")), o.yes); s != exitOK {
			return s
		}
	}
	status = max(status, c.act(f, o, plans))
	if o.watch {
		status = max(status, c.watch(f, plans, o.json))
	}
	return status
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

func (c *cli) checkBackfillUsage(o backfillOptions, args []string) int {
	switch {
	case o.all == (len(args) > 0):
		return c.usageError("backfill", "name the view versions to fill, or -all")
	case o.source != "" && o.source != backfill.SourceTable && o.source != backfill.SourceArchive:
		return c.usageError("backfill", fmt.Sprintf("-source %s: want table or archive", o.source))
	case o.cancel && (o.rate.set || o.source != "" || o.watch):
		return c.usageError("backfill", "-cancel takes no -rate, -source or -watch")
	case o.json && !o.watch:
		return c.usageError("backfill", "-json goes with -watch")
	}
	return exitOK
}

// backfillTargets returns the version IDs that args name, a bare view by dynaflux.lock, or
// none for -all. A version that dynaflux.lock doesn't hold is a usage error, unless there is
// no lock.
func (c *cli) backfillTargets(o backfillOptions, args []string) ([]string, int) {
	if o.all {
		return nil, exitOK
	}
	lock, status := c.readLock()
	if status != exitOK {
		return nil, status
	}
	var ids []string
	for _, a := range args {
		if strings.Contains(a, "@") {
			view, n, ok := parseVersionID(a)
			switch {
			case !ok:
				return nil, c.usageError("backfill", fmt.Sprintf("%s isn't a view version; name one such as page_daily@v1", a))
			case lock != nil && !slices.ContainsFunc(lock.Views[view], func(vv catalog.ViewVersion) bool { return vv.Number == n }):
				return nil, c.usageError("backfill", fmt.Sprintf("%s has no %s; dynaflux status lists the deployed versions", catalog.LockFileName, a))
			}
			ids = append(ids, a)
			continue
		}
		if lock == nil {
			c.errorf("dynaflux backfill: there is no %s\n  hint: run dynaflux generate, or name the versions, such as page_daily@v1",
				catalog.LockFileName)
			return nil, exitUsage
		}
		i := slices.IndexFunc(lock.Views[a], func(vv catalog.ViewVersion) bool { return vv.State == catalog.StateCurrent })
		if i < 0 {
			return nil, c.usageError("backfill", fmt.Sprintf("%s has no current version in %s; name a version, such as %s@v1",
				a, catalog.LockFileName, a))
		}
		ids = append(ids, catalog.VersionID(a, lock.Views[a][i].Number))
	}
	return slices.Compact(slices.Sorted(slices.Values(ids))), exitOK
}

// parseVersionID splits a version ID, <view>@v<N>, into its view and number.
func parseVersionID(id string) (string, int, bool) {
	view, num, ok := strings.Cut(id, "@v")
	n, err := strconv.Atoi(num)
	return view, n, ok && err == nil && view != "" && n > 0 && catalog.VersionID(view, n) == id
}

// readLock reads the project's dynaflux.lock; nil when there is none.
func (c *cli) readLock() (*catalog.Lock, int) {
	root, status := c.projectRoot()
	if status != exitOK {
		return nil, status
	}
	defer root.Close()
	data, err := root.ReadFile(catalog.LockFileName)
	if errors.Is(err, os.ErrNotExist) {
		return nil, exitOK
	}
	if err != nil {
		c.errorf("dynaflux backfill: %v", err)
		return nil, exitUsage
	}
	lock, err := catalog.ParseLock(data)
	if err != nil {
		c.errorf("dynaflux backfill: %s: %v", catalog.LockFileName, err)
		return nil, exitUsage
	}
	return lock, exitOK
}

// action is what a run does with a version in a region.
type action string

// Actions.
const (
	actLive    action = "live"    // nothing: the version is live
	actRefuse  action = "refused" // nothing: the version can't be filled
	actRunning action = "running" // the job runs; with -rate, its rate changes
	actResume  action = "resume"  // the job, interrupted or cancelled, runs again as its next attempt
	actStart   action = "start"   // a new job
	actCancel  action = "cancel"  // the job stops
	actNone    action = "none"    // nothing: the job is already cancelled
)

// planItem is what a run does with a version in a region, and why.
type planItem struct {
	version, stream string
	act             action
	job             meta.Job // the job read, when there is one
	split           meta.Split
	detail          string   // what the plan line says after the action
	code, hint      string   // a refusal's diagnostic; no code for a usage refusal
	stop            []string // cancel: the ARNs of the job's running executions
	usage           bool     // a usage refusal
	failed          bool     // acting on it failed
}

// regionPlan is what a run does in a region.
type regionPlan struct {
	region   string
	origins  []string // the streams whose origins the run recorded
	items    []*planItem
	findings []finding // warnings about the region's archive
	err      error     // the region couldn't be read
	// problems are what couldn't be read of the region's archive, which the plan goes without.
	problems []error
}

// plan records the origins it can, and works out, otherwise with reads only, what the run
// does in each region it can work in, the regions at once. It reports each region it can't
// work in, and returns the status that calls for.
func (f *fleet) plan(c *cli, o backfillOptions, targets []string) ([]*regionPlan, int) {
	status := exitOK
	plans := make([]*regionPlan, len(f.regions))
	var wg sync.WaitGroup
	for i, d := range f.regions {
		plans[i] = &regionPlan{region: d.region}
		if s := c.refused("backfill", d); s != exitOK {
			status = max(status, s)
			continue
		}
		wg.Go(func() { plans[i].err = f.planRegion(c, d, o, targets, plans[i]) })
	}
	wg.Wait()
	return plans, status
}

// planRegion records the origins it can, then plans each version that targets name, or with
// -all each that needs a job, into rp.
func (f *fleet) planRegion(c *cli, d *deployment, o backfillOptions, targets []string, rp *regionPlan) error {
	if o.all {
		for id := range d.cat.Versions() {
			if j, ok := d.job(id); allNames(o.cancel, d.states[id].State, j, ok) {
				targets = append(targets, id)
			}
		}
		slices.Sort(targets)
	}
	p := &regionPlanner{f: f, c: c, d: d, o: o, rp: rp, env: f.backfillEnv(d), archives: map[string]*streamArchive{},
		origins: map[string]backfill.OriginStatus{}}
	if !o.cancel {
		p.recordOrigins(targets)
	}
	for _, id := range targets {
		j, ok := d.job(id)
		it, err := p.item(id, d.states[id], j, ok)
		if err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
		rp.items = append(rp.items, it)
	}
	if o.all && !o.cancel {
		p.missingOrigins()
	}
	return nil
}

// allNames reports whether -all names a version in state whose job, when ok, is j: with
// -cancel, one whose job runs; otherwise one that isn't live, broken or retired, and whose
// job wasn't cancelled.
func allNames(cancel bool, state meta.State, j meta.Job, ok bool) bool {
	if cancel {
		return ok && j.State == meta.JobRunning
	}
	return state != meta.Live && state != meta.Broken && state != meta.Retired && (!ok || j.State != meta.JobCancelled)
}

type regionPlanner struct {
	f        *fleet
	c        *cli
	d        *deployment
	o        backfillOptions
	rp       *regionPlan
	env      *backfill.Env
	archives map[string]*streamArchive        // by stream
	origins  map[string]backfill.OriginStatus // by stream: what recordOrigins did
}

// streamArchive is what the plan read of a stream in the region: its table's birth, and the
// completeness of the region's archive of it, once a choice needed it.
type streamArchive struct {
	born    time.Time
	verdict *backfill.Verdict
}

// recordOrigins records the origin of each stream that the region archives with events: all,
// with -all, or else of the streams of targets, where ADR 0017 §3 allows it now. One that
// can't be recorded is a problem of the region that the plan goes without: the jobs that the
// region's archive can't serve read its tables.
func (p *regionPlanner) recordOrigins(targets []string) {
	for _, s := range p.d.cat.Streams {
		named := p.o.all || slices.ContainsFunc(targets, func(id string) bool {
			vv, ok := p.d.cat.Version(id)
			return ok && vv.Stream == s.Name
		})
		if !named || !p.d.cat.ExactHandoff(p.d.region, s.Name) {
			continue
		}
		st, err := backfill.RecordOrigin(p.c.ctx, p.env, s.Name)
		if err != nil {
			p.rp.problems = append(p.rp.problems, fmt.Errorf("record the origin of stream %s: %w", s.Name, err))
			continue
		}
		p.origins[s.Name] = st
		if st.State == backfill.OriginRecorded {
			p.rp.origins = append(p.rp.origins, s.Name)
		}
	}
}

// missingOrigins adds DF7218 for each stream that the region archives with events: all and
// whose table is less than 23 hours old, where recordOrigins found no origin and recorded none.
func (p *regionPlanner) missingOrigins() {
	for _, s := range p.d.cat.Streams {
		st, tried := p.origins[s.Name]
		if !tried || st.State == backfill.OriginRecorded || st.State == backfill.OriginHeld {
			continue
		}
		a, err := p.archive(s.Name)
		if err != nil {
			p.rp.problems = append(p.rp.problems, err)
			continue
		}
		if time.Since(a.born) < backfill.FreshAge {
			p.rp.findings = append(p.rp.findings, noOrigin(p.d.region, s.Name, a.born, st, prefixFlag(p.o.prefix)))
		}
	}
}

// archive returns the birth of stream's table, reading it the first time.
func (p *regionPlanner) archive(stream string) (*streamArchive, error) {
	if a, ok := p.archives[stream]; ok {
		return a, nil
	}
	table := p.d.naming.StreamTable(stream)
	born, _, err := p.d.clients.tables.Born(p.c.ctx, table)
	if err != nil {
		return nil, fmt.Errorf("describe %s: %w", table, err)
	}
	a := &streamArchive{born: born}
	p.archives[stream] = a
	return a, nil
}

// verdict returns the completeness of the region's archive of stream, reading it the first
// time.
func (p *regionPlanner) verdict(a *streamArchive, stream string) (backfill.Verdict, error) {
	if a.verdict != nil {
		return *a.verdict, nil
	}
	v := backfill.Verdict{Rule: backfill.RuleOrigin, Reason: p.d.region + " doesn't archive it with events: all"}
	if p.d.cat.ExactHandoff(p.d.region, stream) {
		e, err := backfill.Gather(p.c.ctx, p.env, stream)
		if err != nil {
			return backfill.Verdict{}, err
		}
		v = backfill.Completeness(e)
	}
	a.verdict = &v
	return v, nil
}

// item plans version id, whose state is vs and whose job, when ok, is j.
func (p *regionPlanner) item(id string, vs meta.ViewState, j meta.Job, ok bool) (*planItem, error) {
	vv, defined := p.d.cat.Version(id)
	it := &planItem{version: id, job: j}
	if defined {
		it.stream = vv.Stream
	}
	switch {
	case !defined && vs.State != meta.Retired:
		return it.refuse("DF7501", fmt.Sprintf("%s isn't in the deployed catalog of %s", id, p.d.region),
			"deploy regional.yaml in "+p.d.region+" first"), nil
	case p.o.cancel:
		return p.cancelItem(it, ok)
	case vs.State == meta.Live:
		it.act = actLive
		return it, nil
	case vs.State == meta.Retired:
		return it.refuse("DF7502", fmt.Sprintf("%s is retired in %s", id, p.d.region), "fill the view's current version instead"), nil
	case vs.State == meta.Broken:
		view, _, _ := strings.Cut(id, "@")
		return it.refuse("DF7502", fmt.Sprintf("%s is broken in %s", id, p.d.region),
			"rebuild it with dynaflux generate -rebuild "+view+", then deploy and backfill the new version"), nil
	case vs.State == meta.Activating && p.o.source != "" && backfill.SourceOf(vs.Split) != p.o.source:
		return it.usageRefusal(fmt.Sprintf("-source %s: %s is activating in %s with the %s source, which can't change",
			p.o.source, id, p.d.region, backfill.SourceOf(vs.Split))), nil
	case ok:
		return p.jobItem(it, vv, vs)
	case vs.State == meta.Activating:
		it.act, it.split = actStart, vs.Split
		it.detail = describeSplit(vs.Split) + ", as activated"
		return it, nil
	}
	split, notes, err := p.chooseSplit(it, vv)
	if err != nil || it.act == actRefuse {
		return it, err
	}
	it.act, it.split, it.detail = actStart, split, describeSplit(split)+notes+p.rateNote()
	return it, nil
}

// rateNote is what a plan line says of -rate.
func (p *regionPlanner) rateNote() string {
	if !p.o.rate.set {
		return ""
	}
	return "; rate " + rateText(p.o.rate.value)
}

func (it *planItem) refuse(code, msg, hint string) *planItem {
	it.act, it.code, it.detail, it.hint = actRefuse, code, msg, hint
	return it
}

func (it *planItem) usageRefusal(msg string) *planItem {
	it.act, it.usage, it.detail = actRefuse, true, msg
	return it
}

// jobItem plans a version that has a job, whose state is vs.
func (p *regionPlanner) jobItem(it *planItem, vv *catalog.ViewVersion, vs meta.ViewState) (*planItem, error) {
	j := it.job
	switch j.State {
	case meta.JobDone:
		it.act = actLive // made live meanwhile
		return it, nil
	case meta.JobBroken:
		return it.refuse("DF7502", fmt.Sprintf("%s's job broke in %s", it.version, p.d.region),
			"read its error with dynaflux status, then rebuild the view"), nil
	case meta.JobFailed:
		return it.refuse("DF7506", fmt.Sprintf("%s's job failed in %s on an archive of %s that wasn't complete, so the version can't go live",
			it.version, p.d.region, vv.Stream), "read its error with dynaflux status, then rebuild the view"), nil
	case meta.JobCancelled, meta.JobRunning:
	}
	var running []string
	if j.State == meta.JobRunning {
		var err error
		if running, err = p.f.running(p.c.ctx, p.d, j); err != nil {
			return nil, err
		}
	}
	if len(running) > 0 {
		if p.o.source != "" && backfill.SourceOf(j.Split) != p.o.source {
			return it.usageRefusal(fmt.Sprintf("-source %s: the job of %s runs in %s with the %s source", p.o.source, it.version, p.d.region,
				backfill.SourceOf(j.Split))), nil
		}
		it.act, it.detail = actRunning, progress(j)+p.rateNote()
		return it, nil
	}
	reason := statusInterrupted
	if j.State == meta.JobCancelled {
		reason = string(meta.JobCancelled)
	}
	it.act, it.split = actResume, j.Split
	notes := ""
	if vs.State != meta.Activating {
		// Not activated yet: the job takes the split that the region serves now.
		var err error
		if it.split, notes, err = p.chooseSplit(it, vv); err != nil || it.act == actRefuse {
			return it, err
		}
	}
	it.detail = fmt.Sprintf("%s; attempt %d, %s%s%s", reason, j.Attempt+1, describeSplit(it.split), notes, p.rateNote())
	return it, nil
}

// cancelItem plans -cancel of a version's job.
func (p *regionPlanner) cancelItem(it *planItem, ok bool) (*planItem, error) {
	j := it.job
	switch {
	case !ok:
		return it.refuse("DF7504", fmt.Sprintf("%s has no backfill job in %s", it.version, p.d.region), "nothing to cancel"), nil
	case j.State == meta.JobCancelled:
		it.act, it.detail = actNone, "already cancelled"
		return it, nil
	case j.State != meta.JobRunning:
		return it.refuse("DF7504", fmt.Sprintf("%s's job is %s in %s", it.version, j.State, p.d.region), "nothing to cancel"), nil
	}
	running, err := p.f.running(p.c.ctx, p.d, j)
	if err != nil {
		return nil, err
	}
	it.act, it.stop, it.detail = actCancel, running, progress(j)
	return it, nil
}

// chooseSplit returns the split of a job of vv that isn't activated yet, from the source the
// region serves now (lifecycle.md §5.2), and the notes that the plan line adds to it. The
// archive source that -source asks for, of an archive that isn't complete, refuses it, as
// DF7503. With -source table, an archive whose completeness can't be read is a problem of the
// region, not of the job: a table rebuild is how a gap is repaired.
func (p *regionPlanner) chooseSplit(it *planItem, vv *catalog.ViewVersion) (meta.Split, string, error) {
	a, err := p.archive(vv.Stream)
	if err != nil {
		return "", "", err
	}
	v, err := p.verdict(a, vv.Stream)
	switch {
	case err != nil && p.o.source == backfill.SourceTable:
		p.rp.problems = append(p.rp.problems, fmt.Errorf("the completeness of the archive of stream %s: %w", vv.Stream, err))
	case err != nil:
		return "", "", fmt.Errorf("the completeness of the archive of stream %s, which -source table doesn't need: %w", vv.Stream, err)
	}
	split, err := backfill.Choose(p.d.cat, p.d.region, it.version, v, p.o.source)
	if errors.Is(err, backfill.ErrIncomplete) {
		it.refuse("DF7503", fmt.Sprintf("the archive of %s in %s isn't complete: %s", vv.Stream, p.d.region, v.Reason),
			"fill it with -source table")
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	var notes string
	if since := partialSince(p.d.cat, vv.Stream, split, a.born); !since.IsZero() {
		notes += "; partial since about " + since.UTC().Format(time.DateOnly)
	}
	s, _ := p.d.cat.Stream(vv.Stream)
	if r, expires := s.LogRetention.Period(); expires && v.Complete && split != meta.SplitFence {
		notes += "; the archive is complete, and a table source covers only the last " + r.String()
	}
	return split, notes, nil
}

// partialSince returns about when a table source that reads now holds events from, or zero
// when it holds every event: the stream table is younger than its LOG RETENTION.
func partialSince(cat *catalog.Catalog, stream string, split meta.Split, born time.Time) time.Time {
	s, _ := cat.Stream(stream)
	r, expires := s.LogRetention.Period()
	if !expires || split == meta.SplitFence {
		return time.Time{}
	}
	start, err := expr.AddInterval(time.Now().UnixMicro(), catalog.Interval{N: -r.N, Unit: r.Unit})
	if err != nil || born.UnixMicro() > start {
		return time.Time{}
	}
	return time.UnixMicro(start)
}

// describeSplit says how a split reads and divides a version's events.
func describeSplit(s meta.Split) string {
	switch s {
	case meta.SplitFence:
		return "archive source, split by fences"
	case meta.SplitTime:
		return "table source, split by time"
	case meta.SplitNone:
	}
	return "table source, no split"
}

// progress describes a running job.
func progress(j meta.Job) string {
	return fmt.Sprintf("attempt %d, phase %s, %d of %d records applied", j.Attempt, j.Phase, j.Applied, j.Staged)
}

func rateText(rate int) string {
	if rate == 0 {
		return "no limit"
	}
	return strconv.Itoa(rate) + " records/s"
}

// executionName returns the name of a new job's executions, without -a<attempt>:
// <view>-v<N>-<h>, where h is 6 hex digits of a hash of the version and the time the job is
// created, so a job made again never meets a name of the last 90 days. A hash of the version
// replaces the view when the name, with a 3-digit attempt and a 5-digit continuation, would
// pass Step Functions' 80 characters.
func executionName(version string, created time.Time) string {
	view, n, _ := strings.Cut(version, "@v")
	sum := sha256.Sum256([]byte(version + " " + created.UTC().Format(time.RFC3339Nano)))
	tail := "-v" + n + "-" + hex.EncodeToString(sum[:])[:6]
	const longest = 80 - len("-a999-c99999")
	if len(view)+len(tail) > longest {
		id := sha256.Sum256([]byte(version))
		view = hex.EncodeToString(id[:])[:12]
	}
	return view + tail
}

// printPlan prints what the run does in each region, on stdout or with -json on stderr, and
// the findings of the versions it refuses. It returns exitErrors when it refuses one, and
// exitUsage for a usage refusal or a region that couldn't be read.
func (c *cli) printPlan(plans []*regionPlan, o backfillOptions) int {
	out := &c.out
	if o.json {
		out = &c.errOut
	}
	status, items := exitOK, 0
	for _, p := range plans {
		for _, err := range p.problems {
			c.errorf("dynaflux backfill: %s: %v", p.region, err)
			status = exitUsage
		}
		if p.err != nil {
			c.errorf("dynaflux backfill: %s: %v", p.region, p.err)
			status = exitUsage
			continue
		}
		items += len(p.items)
		p.print(out)
	}
	if items == 0 && status == exitOK {
		what := "fill"
		if o.cancel {
			what = "cancel"
		}
		fmt.Fprintf(out, "nothing to %s\n", what)
	}
	for _, p := range plans {
		for _, it := range p.items {
			switch {
			case it.usage:
				c.errorf("dynaflux backfill: %s", it.detail)
				status = exitUsage
			case it.act == actRefuse:
				c.finding(p.region, diag.SeverityError, it.code, it.detail, it.hint)
				status = max(status, exitErrors)
			}
		}
		for _, f := range p.findings {
			f.write(&c.errOut)
		}
	}
	return status
}

// print prints the region's plan lines, after the origins the run recorded.
func (p *regionPlan) print(out *strings.Builder) {
	if len(p.items) == 0 && len(p.origins) == 0 {
		return
	}
	fmt.Fprintf(out, "%s:\n", p.region)
	for _, s := range p.origins {
		fmt.Fprintf(out, "  stream %s: origin recorded; the region's archive of it is complete from now on\n", s)
	}
	width := 0
	for _, it := range p.items {
		width = max(width, len(it.version))
	}
	for _, it := range p.items {
		line := fmt.Sprintf("  %-*s  %-7s", width, it.version, it.act)
		if it.detail != "" && it.act != actRefuse {
			line += "  " + it.detail
		}
		fmt.Fprintln(out, strings.TrimRight(line, " "))
	}
}

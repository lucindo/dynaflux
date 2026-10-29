// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
)

// gapMargin is how long before a version's activation status still reads Lambda's records:
// a record's key holds the time Lambda wrote it, by a clock other than the activation's.
const gapMargin = time.Hour

func statusCommand() command {
	return command{
		name:    "status",
		args:    "[flags]",
		summary: "show the state of the deployed store, and what needs an action",
		doc: `Status reads each region of the deployed store at once. It shows the state of every
view version (docs/spec/lifecycle.md §3) as a table of versions by region, then
for each region its catalog, the backfill jobs, and for each stream the
projector's lag, the event source mappings, how far the archive is delivered, and
whether it is complete (§5.5). Last, on stderr, it lists what needs an action,
each with the command that takes it: a broken version, a version that needs a
backfill, a live version that is no longer current, an interrupted backfill, a
gap Lambda recorded in the bucket, a suspected gap or an incomplete archive that
affects a version, a young stream without an origin, projectors blocked on a
version their catalog doesn't know, projector lag, another reader of a stream, a
mapping that isn't enabled, and a stale archive. The commands repeat -endpoint
and -prefix as given. It reads the newest 100 of Lambda's records, and none
written before every version's activation. -verbose adds each delivery unit's
progress and fences. -json prints one JSON document instead, with "format": 1.

With -endpoint it reads DynamoDB Local, where mappings, metrics, gaps and the
jobs' executions aren't available, and judges an archive by the holes its meta
table records. Its commands repeat -region there too, and name dynaflux dev
where a backfill would run on AWS.

Exit status: 0 when nothing needs an action or only warnings do, 1 when an error
does, and 2 for usage and AWS access problems, and when a region, or part of
one, couldn't be read.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			o := statusOptions{}
			addStoreFlags(fs, &o.storeFlags)
			fs.BoolVar(&o.verbose, "verbose", false, "also show each delivery unit's progress and fences")
			return func(c *cli, args []string) int { return c.status(o, args) }
		},
	}
}

type statusOptions struct {
	storeFlags

	verbose bool
}

// regionStatus is what status read of a region.
type regionStatus struct {
	*deployment

	local bool      // on DynamoDB Local: no executions, mappings, metrics or gaps
	now   time.Time // when status started
	// execs is what runs each running job on AWS; a job whose execution couldn't be read is
	// absent.
	execs   map[string]deployed.Execution
	streams []streamStatus
	// olderGaps and cappedGaps count the on-failure records not read: written before every
	// version's activation, and past the newest deployed.MaxGaps.
	olderGaps, cappedGaps int
	gapsUnread            bool
	// leftOut is how many source rows and aggregate inputs the views left out in the last
	// deployed.CountWindow, for a value too long to store.
	leftOut int64
	unread  []error // the parts of the region that couldn't be read
}

// streamStatus is how a region delivers a stream's events.
type streamStatus struct {
	name          string
	arn           string
	mapped        bool // its change stream and mappings were read
	mappings      []deployed.Mapping
	lag           *time.Duration // the IteratorAge of its projector; nil without data
	lagUnread     bool
	archive       string // the delivery stream that archives it in the region; "" for none
	freshness     *time.Duration
	delivered     *time.Time // Firehose had delivered every record it took before then
	failed        int64
	archiveUnread bool
	check         *archiveCheck // with events: all in the region; nil locally without an origin
	// suspects are the gaps suspected in the views applied live (lifecycle.md §7.2), from the
	// stream item and, with an origin, AWS's facts.
	suspects []backfill.Verdict
	gaps     []deployed.Gap
	units    []meta.Unit // with -verbose
}

// archiveCheck is what status read of the completeness of the region's archive of a stream
// that it archives with events: all.
type archiveCheck struct {
	since     time.Time // the origin's time; zero without one
	born      time.Time // the stream table's; zero locally
	verdict   backfill.Verdict
	noOrigin  bool      // no origin, or one that the stream table voids
	unchecked time.Time // hours since the origin before it went unchecked
	unread    bool
}

func (c *cli) status(o statusOptions, args []string) int {
	if len(args) > 0 {
		return c.usageError("status", "unexpected arguments: "+strings.Join(args, " "))
	}
	if status := o.check(c, "status"); status != exitOK {
		return status
	}
	cfg, status := c.loadConfig()
	if status != exitOK {
		return status
	}
	ctx, cancel := context.WithTimeout(c.ctx, o.timeout)
	defer cancel()
	f, status := c.openFleet(ctx, "status", cfg, o.prefix, o.regions, o.endpoint)
	if status != exitOK {
		return status
	}
	now := time.Now()
	rs := make([]*regionStatus, len(f.regions))
	var wg sync.WaitGroup
	for i, d := range f.regions {
		rs[i] = &regionStatus{deployment: d, local: f.endpoint != "", now: now}
		if d.err == nil && d.cat != nil {
			wg.Go(func() { rs[i].read(ctx, f, o.verbose, now) })
		}
	}
	wg.Wait()
	var fs []finding
	for _, r := range rs {
		fs = append(fs, r.findings(f, hintFlags{endpoint: o.endpoint, prefix: o.prefix, regions: o.regions}, now)...)
	}
	if o.json {
		status = c.writeJSON(statusJSONOf(f, rs, fs))
	} else {
		c.printStatus(f, rs, o.verbose)
		for _, fd := range fs {
			fd.write(&c.errOut)
		}
	}
	for _, r := range rs {
		for _, err := range append([]error{r.err}, r.unread...) {
			if err != nil {
				c.errorf("dynaflux status: %s: %s", r.region, cellLines(err.Error()))
				status = exitUsage
			}
		}
	}
	return max(status, worst(fs))
}

// read reads, besides the region's state, what runs its jobs, its streams' units with
// verbose, and on AWS their delivery. A part that can't be read is recorded in r.unread, and
// the others are still read.
func (r *regionStatus) read(ctx context.Context, f *fleet, verbose bool, now time.Time) {
	if !r.local {
		r.readExecutions(ctx, f)
	}
	for _, s := range r.cat.Streams {
		ss := streamStatus{name: s.Name}
		if s.Archive != nil && slices.Contains(s.Archive.Regions, r.region) {
			ss.archive = f.naming.ArchiveStream(s.Name)
		}
		if verbose {
			var err error
			if ss.units, err = readUnits(ctx, r.deployment, s.Name); err != nil {
				r.unread = append(r.unread, fmt.Errorf("the units of stream %s couldn't be read: %w", s.Name, err))
			}
		}
		if err := r.readArchive(ctx, f, &ss); err != nil {
			r.unread = append(r.unread, fmt.Errorf("the history of stream %s couldn't be read: %w", s.Name, err))
			if r.cat.ExactHandoff(r.region, s.Name) {
				ss.check = &archiveCheck{unread: true}
			}
		}
		r.streams = append(r.streams, ss)
	}
	if !r.local {
		r.readDelivery(ctx, f, now)
	}
}

// readArchive reads the gaps suspected in the views of s's stream, and, where the region
// archives it with events: all, the completeness of its archive: on AWS by every rule; locally,
// where AWS's facts aren't available, by the holes of the stream item alone, which needs an
// origin for a check.
func (r *regionStatus) readArchive(ctx context.Context, f *fleet, s *streamStatus) error {
	exact := r.cat.ExactHandoff(r.region, s.name)
	if r.local {
		item, _, err := r.meta.Stream(ctx, s.name)
		if err != nil {
			return err
		}
		e := backfill.Evidence{Stream: item, Exact: exact}
		if o := item.Origin; o != nil {
			e.Facts = backfill.Facts{ARN: o.ARN, Mapping: true}
			if exact {
				s.check = &archiveCheck{since: o.At, verdict: backfill.Completeness(e)}
			}
		}
		s.suspects = backfill.Suspected(e)
		return nil
	}
	e, err := backfill.Gather(ctx, f.backfillEnv(r.deployment), s.name)
	if err != nil {
		return err
	}
	s.suspects = backfill.Suspected(e)
	if !exact {
		return nil
	}
	table := f.naming.StreamTable(s.name)
	born, _, err := r.clients.tables.Born(ctx, table)
	if err != nil {
		return fmt.Errorf("describe %s: %w", table, err)
	}
	a := &archiveCheck{born: born, verdict: backfill.Completeness(e), noOrigin: true, unchecked: e.Unchecked}
	if o := e.Stream.Origin; o != nil {
		a.since, a.noOrigin = o.At, o.Void(e.ARN)
	}
	s.check = a
	return nil
}

// readExecutions reads what runs each running job.
func (r *regionStatus) readExecutions(ctx context.Context, f *fleet) {
	r.execs = map[string]deployed.Execution{}
	for _, j := range r.jobs {
		if j.State != meta.JobRunning {
			continue
		}
		e, err := deployed.Running(ctx, r.clients.sfn, f.machine(r.region), j)
		if err != nil {
			r.unread = append(r.unread, fmt.Errorf("the execution of the backfill of %s couldn't be read: %w", j.Version, err))
			continue
		}
		r.execs[j.Version] = e
	}
}

// readUnits reads the items of a stream's delivery units, in the order of their IDs.
func readUnits(ctx context.Context, d *deployment, stream string) ([]meta.Unit, error) {
	ids, err := d.clients.units.Units(ctx, d.naming.StreamTable(stream))
	if err != nil {
		return nil, err
	}
	unitKeys := make([]string, len(ids))
	for i, id := range ids {
		unitKeys[i] = keys.UnitKey(id)
	}
	byKey, err := d.meta.Units(ctx, stream, unitKeys, false)
	if err != nil {
		return nil, err
	}
	units := make([]meta.Unit, 0, len(ids))
	for i, id := range ids {
		u, ok := byKey[unitKeys[i]]
		if !ok {
			u = meta.Unit{Stream: stream, ID: id, Key: unitKeys[i]}
		}
		units = append(units, u)
	}
	slices.SortFunc(units, func(a, b meta.Unit) int { return strings.Compare(a.ID, b.ID) })
	return units, nil
}

// readDelivery reads, on AWS, each stream's mappings, the projectors' lag, the archive's
// metrics, and the gaps in the bucket.
func (r *regionStatus) readDelivery(ctx context.Context, f *fleet, now time.Time) {
	var archives []string
	for i := range r.streams {
		s := &r.streams[i]
		if s.archive != "" {
			archives = append(archives, s.archive)
		}
		if err := s.readMappings(ctx, f.naming, r.clients); err != nil {
			r.unread = append(r.unread, fmt.Errorf("the mappings of stream %s couldn't be read: %w", s.name, err))
		}
	}
	ages, err := awsbackfill.IteratorAges(ctx, r.clients.metrics, awsbackfill.ProjectorFunctions(f.naming, r.cat), now.Add(-deployed.Recent), now)
	if err != nil {
		r.unread = append(r.unread, fmt.Errorf("the projectors' lag couldn't be read: %w", err))
	}
	m, merr := deployed.ReadMetrics(ctx, r.clients.metrics, f.naming.Base(), archives, now)
	if merr != nil {
		r.unread = append(r.unread, fmt.Errorf("the archive's and the views' metrics couldn't be read: %w", merr))
	}
	r.leftOut = m.LeftOut
	for i := range r.streams {
		s := &r.streams[i]
		s.lagUnread, s.archiveUnread = err != nil, merr != nil && s.archive != ""
		if lag, found := s.projectorLag(f.naming, ages); found {
			s.lag = &lag
		}
		if fresh, ok := m.Freshness[s.archive]; ok {
			delivered := m.Delivered[s.archive]
			s.freshness, s.delivered = &fresh, &delivered
		}
		s.failed = m.FailedRows[s.archive]
	}
	r.readGaps(ctx, f, now)
}

// readMappings reads the stream's change stream and the event source mappings that read it.
func (s *streamStatus) readMappings(ctx context.Context, naming catalog.Naming, cl regionClients) error {
	arn, err := awsbackfill.StreamARN(ctx, cl.describe, naming.StreamTable(s.name))
	if err != nil {
		return err
	}
	if arn != "" {
		if s.mappings, err = deployed.Mappings(ctx, cl.lambda, arn); err != nil {
			return err
		}
	}
	s.arn, s.mapped = arn, true
	return nil
}

// readGaps reads the on-failure records that a version may miss: those Lambda wrote since
// the earliest activation of a version neither retired nor broken.
func (r *regionStatus) readGaps(ctx context.Context, f *fleet, now time.Time) {
	since := now
	for _, vs := range r.states {
		if vs.State != meta.Retired && vs.State != meta.Broken && !vs.ActivatedAt.IsZero() && vs.ActivatedAt.Before(since) {
			since = vs.ActivatedAt
		}
	}
	bucket := f.naming.Bucket(f.account, r.region)
	scan, err := deployed.Gaps(ctx, r.clients.bucket, bucket, since.Add(-gapMargin))
	if err != nil {
		r.gapsUnread = true
		r.unread = append(r.unread, fmt.Errorf("the gaps couldn't be read: %w", err))
		return
	}
	if len(scan.Bad) > 0 {
		r.unread = append(r.unread, fmt.Errorf("%d of Lambda's records under s3://%s/%s couldn't be read, such as: %w",
			len(scan.Bad), bucket, deployed.GapPrefix, scan.Bad[0]))
	}
	r.olderGaps, r.cappedGaps = scan.Older, scan.Capped
	for i := range r.streams {
		s := &r.streams[i]
		for _, g := range scan.Gaps {
			if g.Table == f.naming.StreamTable(s.name) {
				s.gaps = append(s.gaps, g)
			}
		}
	}
}

// projector returns the stream's mapping to its projector: the store's or the stream's.
func (s *streamStatus) projector(naming catalog.Naming) (deployed.Mapping, bool) {
	i := slices.IndexFunc(s.mappings, func(m deployed.Mapping) bool { return isProjector(naming, s.name, m.Function) })
	if i < 0 {
		return deployed.Mapping{}, false
	}
	return s.mappings[i], true
}

// projectorLag returns the IteratorAge of the stream's projector in ages: its mapping's
// function, or, when the mappings couldn't be read, the stream's projector or else the
// store's, as runtime.split deployed one of them.
func (s *streamStatus) projectorLag(naming catalog.Naming, ages map[string]time.Duration) (time.Duration, bool) {
	if mp, ok := s.projector(naming); ok || s.mapped {
		lag, found := ages[mp.Function]
		return lag, found
	}
	if lag, found := ages[naming.StreamProjectorFunction(s.name)]; found {
		return lag, true
	}
	lag, found := ages[naming.ProjectorFunction()]
	return lag, found
}

func isProjector(naming catalog.Naming, stream, function string) bool {
	return function == naming.ProjectorFunction() || function == naming.StreamProjectorFunction(stream)
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/smithy-go"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
)

// The throughput benchmark's flags (docs/runbooks/benchmarks.md).
//
//nolint:gochecknoglobals // test flags
var (
	throughputOut = flag.String("throughput.out", "",
		"the throughput benchmark's result file; unset, TestThroughput and TestThroughputReport skip")
	throughputRate     = flag.Float64("throughput.rate", 1000, "events per second, all regions together")
	throughputWarmup   = flag.Duration("throughput.warmup", 10*time.Minute, "how long to load before the measured window")
	throughputDuration = flag.Duration("throughput.duration", time.Hour, "the measured window")
	throughputWorkers  = flag.Int("throughput.workers", 128, "appends in flight in each region")
)

// The workload is uniform over fixed pools, so that page_daily has about loadPages active
// rows and tag_hourly loadTags, as the report's explain -rows assume.
const (
	loadMaxTags   = 3 // tags per event: 0 to 3, uniformly
	maxDurationMS = 5000
	loadTick      = 10 * time.Millisecond

	appendTimeout = 10 * time.Second
	probeEvery    = 2 * time.Second
	probePoll     = 200 * time.Millisecond
	probePatience = 5 * time.Minute
	// maxUnknown bounds the appends whose outcome is unknown, since each widens what the
	// oracle accepts.
	maxUnknown   = 100
	minAchieved  = 0.95 // of the target rate
	checkWorkers = 16   // queries in flight while checking a region's views
	// reportDelay is how late Lambda's and DynamoDB's datapoints land in CloudWatch.
	reportDelay = 5 * time.Minute

	pageViews = "page_views"
)

// TestThroughput loads the e2e store at -throughput.rate events per second, spread evenly
// over the regions. It measures how long an append takes to show in each region's views, and
// checks that every region's views count exactly what was appended. It writes what the
// report needs to -throughput.out as it goes.
//
//nolint:paralleltest // it loads the whole store
func TestThroughput(t *testing.T) {
	if *throughputOut == "" {
		t.Skip("set -throughput.out to run the throughput benchmark (docs/runbooks/benchmarks.md)")
	}
	checkLoadFlags(t)
	p := loadProject(t)
	var h *harness
	switch *target {
	case targetEmulator:
		h = newEmulator(t, p)
	case targetAWS:
		h = newAWS(t, p)
	default:
		t.Fatalf("-target is %q; want %s or %s (docs/runbooks/benchmarks.md)", *target, targetEmulator, targetAWS)
	}
	l := newLoad(t, h)
	res := l.result(t)
	saveResult(t, res)
	l.run(t)
	l.loaded(t, res)
	saveResult(t, res)
	res.Failures = append(res.Failures, l.converge(t)...)
	res.Phase = "done"
	saveResult(t, res)
	for _, f := range res.Failures {
		t.Error(f)
	}
}

func checkLoadFlags(t *testing.T) {
	t.Helper()
	switch {
	case !(*throughputRate > 0):
		t.Fatalf("-throughput.rate is %v; want events per second, over 0", *throughputRate)
	case *throughputWarmup < 0 || *throughputDuration <= 0:
		t.Fatalf("-throughput.warmup is %v and -throughput.duration %v; want a warm-up of 0 or more and a positive duration",
			*throughputWarmup, *throughputDuration)
	case *throughputWorkers < 1:
		t.Fatalf("-throughput.workers is %d; want 1 or more", *throughputWorkers)
	case *target == targetAWS && *throughputDuration%time.Minute != 0:
		t.Fatalf("-throughput.duration is %v; on AWS it must be whole minutes, as CloudWatch's datapoints are", *throughputDuration)
	}
}

// load is a run of the throughput benchmark on a harness.
type load struct {
	h     *harness
	users *agg.Func // tag_hourly's uniq(user_id)
	// The load runs from start to windowEnd; the report measures from windowStart.
	start, windowStart, windowEnd time.Time
	gens                          []*generator // in the harness's region order

	mu sync.Mutex // guards the fields below and each generator's results
	// seen holds every region's (page, user) pairs by day; newPairs counts the window's
	// appends that brought one, windowEvents its appends and windowTags their tags.
	seen                               map[int64]pairSet
	newPairs, windowEvents, windowTags int64
	probes                             []probeResult
	failures                           []string
}

// generator sends one region's share of the load through the example's client, one event
// per call, as an application does.
type generator struct {
	r      *region
	client *flux.Client // on a store whose HTTP pool keeps a connection per worker
	rate   float64
	rng    *rand.Rand // the scheduler's
	jobs   chan pageEvent

	// Guarded by load.mu. sent counts the run's appends that succeeded, appended and missed
	// the window's.
	tally                           *tally
	sent, appended, missed, unknown int64
	errors                          map[string]int64
	samples                         map[string]string
	latency                         []float64 // ms
}

func newLoad(t *testing.T, h *harness) *load {
	t.Helper()
	l := &load{h: h, users: uniqUsers(t, h.p.def), seen: map[int64]pairSet{}}
	for i, r := range h.regions {
		l.gens = append(l.gens, &generator{
			r: r, client: loadClient(t, r), rate: *throughputRate / float64(len(h.regions)),
			rng:  rand.New(rand.NewPCG(uint64(i), 1)),
			jobs: make(chan pageEvent, *throughputWorkers), tally: newTally(l.users),
			errors: map[string]int64{}, samples: map[string]string{},
		})
	}
	l.start = time.Now()
	l.windowStart = l.start.Add(*throughputWarmup)
	if h.aws != nil {
		// CloudWatch's datapoints start on whole minutes.
		if rounded := l.windowStart.Truncate(time.Minute); rounded.Before(l.windowStart) {
			l.windowStart = rounded.Add(time.Minute)
		}
	}
	l.windowEnd = l.windowStart.Add(*throughputDuration)
	return l
}

// uniqUsers compiles tag_hourly's users column as the projector does.
func uniqUsers(t *testing.T, cat *catalog.Catalog) *agg.Func {
	t.Helper()
	v, ok := cat.View("tag_hourly")
	if !ok {
		t.Fatal("the e2e store has no view tag_hourly")
	}
	vv, ok := v.Current()
	if !ok {
		t.Fatal("tag_hourly has no current version")
	}
	i := slices.IndexFunc(vv.Aggregates, func(a catalog.ViewAggregate) bool { return a.Name == "users" })
	if i < 0 {
		t.Fatal("tag_hourly has no column users")
	}
	a := vv.Aggregates[i]
	f, err := agg.Compile(agg.Spec{Agg: a.Aggregate, ArgTypes: []catalog.Type{a.Args[0].Type}})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// loadClient opens the region's store with an HTTP pool that keeps a connection for each
// worker: the SDK's keeps 10 per host, and the others would each make a TLS handshake per
// append.
func loadClient(t *testing.T, r *region) *flux.Client {
	t.Helper()
	cfg := r.cfg.Copy()
	var bc *awshttp.BuildableClient
	switch c := cfg.HTTPClient.(type) {
	case nil:
		bc = awshttp.NewBuildableClient()
	case *awshttp.BuildableClient:
		bc = c
	default:
		t.Fatalf("%s: the AWS config's HTTP client is a %T, whose pool the load can't size", r.name, c)
	}
	cfg.HTTPClient = bc.WithTransportOptions(func(tr *http.Transport) { tr.MaxIdleConnsPerHost = *throughputWorkers })
	s, err := dynaflux.Open(dynaflux.Config{AWS: cfg, Prefix: r.naming.Prefix})
	if err != nil {
		t.Fatal(err)
	}
	return flux.New(s)
}

// result starts the result file with what the report reads of each region.
func (l *load) result(t *testing.T) *throughputResult {
	t.Helper()
	res := &throughputResult{
		Target: *target, Phase: "loading", Base: l.h.regions[0].naming.Base(),
		Workload: loadWorkload{Rate: *throughputRate, Workers: *throughputWorkers, Users: loadUsers, Pages: loadPages, Tags: loadTags},
		Start:    l.start.UTC(), WindowStart: l.windowStart.UTC(), WindowEnd: l.windowEnd.UTC(),
	}
	for _, r := range l.h.regions {
		res.Regions = append(res.Regions, r.name)
		rr := regionResult{
			Name: r.name, StreamTable: r.naming.StreamTable(pageViews), MetaTable: r.naming.MetaTable(),
			ViewTables: map[string]string{}, Function: r.naming.ProjectorFunction(), Archive: r.naming.ArchiveStream(pageViews),
		}
		for i := range l.h.p.def.Views {
			v := &l.h.p.def.Views[i]
			if vv, ok := v.Current(); ok && vv.Stream == pageViews {
				rr.ViewTables[v.Name] = r.naming.ViewTable(v.Name, vv.Number)
			}
		}
		if l.h.aws != nil {
			ms := mappings(t, l.h.aws.clients[r].lambda, &lambda.ListEventSourceMappingsInput{
				FunctionName: new(rr.Function), EventSourceArn: new(streamARN(t, r, pageViews)),
			})
			if len(ms) != 1 {
				t.Fatalf("%s: %d event source mappings of %s on %s, want 1", r.name, len(ms), rr.StreamTable, rr.Function)
			}
			rr.Mapping = aws.ToString(ms[0].UUID)
		}
		res.PerRegion = append(res.PerRegion, rr)
	}
	return res
}

// saveResult replaces -throughput.out with res, so that the file is whole if the run stops.
func saveResult(t *testing.T, res *throughputResult) {
	t.Helper()
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	tmp := *throughputOut + ".tmp"
	if err = os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(tmp, *throughputOut); err != nil {
		t.Fatal(err)
	}
}

// run loads the store until the window ends, and waits for every probe. On the emulator, a
// loop per region drains the streams meanwhile.
func (l *load) run(t *testing.T) {
	t.Helper()
	var stops []func()
	if l.h.aws == nil {
		for _, r := range l.h.regions {
			stops = append(stops, settling(t, r))
		}
	}
	done := make(chan struct{})
	var load, probes, watch sync.WaitGroup
	watch.Go(func() { l.progress(t, done) })
	for _, g := range l.gens {
		load.Go(func() { l.schedule(t.Context(), g) })
		for range *throughputWorkers {
			load.Go(func() { l.work(t.Context(), g) })
		}
		load.Go(func() { l.sendProbes(t.Context(), g, &probes) })
	}
	load.Wait()
	close(done)
	watch.Wait()
	probes.Wait()
	for _, stop := range stops {
		stop()
	}
}

// progress logs each region's appends every minute until done, so that a long run shows
// whether the client keeps up.
func (l *load) progress(t *testing.T, done <-chan struct{}) {
	t.Helper()
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	last := make([]int64, len(l.gens))
	for {
		select {
		case <-done:
			return
		case <-tick.C:
		}
		l.mu.Lock()
		for i, g := range l.gens {
			t.Logf("%s: %d appended, %.0f/s in the last minute; in the window, %d missed; %d unknown",
				g.r.name, g.sent, float64(g.sent-last[i])/time.Minute.Seconds(), g.missed, g.unknown)
			last[i] = g.sent
		}
		l.mu.Unlock()
	}
}

func (l *load) inWindow(at time.Time) bool {
	return !at.Before(l.windowStart) && at.Before(l.windowEnd)
}

// schedule hands the generator's workers an event at each of its scheduled times. When
// every worker is busy, it counts the event as missed instead of sending it late, so that a
// slow client shows as a lower rate rather than a burst.
func (l *load) schedule(ctx context.Context, g *generator) {
	defer close(g.jobs)
	tick := time.NewTicker(loadTick)
	defer tick.Stop()
	for n := 0; ; {
		var now time.Time
		select {
		case <-ctx.Done():
			return
		case now = <-tick.C:
		}
		for ; ; n++ {
			at := l.start.Add(time.Duration(float64(n) * float64(time.Second) / g.rate))
			if !at.Before(l.windowEnd) {
				return
			}
			if at.After(now) {
				break
			}
			select {
			case g.jobs <- g.next(at):
			default:
				if l.inWindow(at) {
					l.mu.Lock()
					g.missed++
					l.mu.Unlock()
				}
			}
		}
	}
}

func (g *generator) next(at time.Time) pageEvent {
	return pageEvent{
		user: g.rng.IntN(loadUsers), page: g.rng.IntN(loadPages), at: at,
		durationMS: 1 + g.rng.Uint32N(maxDurationMS), // never 0, which breaks e2e_ratio
		tags:       g.rng.Perm(loadTags)[:g.rng.IntN(loadMaxTags+1)],
	}
}

func pageName(p int) string  { return fmt.Sprintf("/%s/p%d", nonce, p) }
func tagName(tag int) string { return fmt.Sprintf("%s-t%d", nonce, tag) }

func (ev pageEvent) pageView() flux.PageView {
	tags := make([]string, len(ev.tags))
	for i, tag := range ev.tags {
		tags[i] = tagName(tag)
	}
	return flux.PageView{
		UserID: userName(ev.user), Page: pageName(ev.page), Tags: tags, DurationMS: ev.durationMS, Country: "BR", EventTime: ev.at,
	}
}

func (l *load) work(ctx context.Context, g *generator) {
	for ev := range g.jobs {
		actx, cancel := context.WithTimeout(ctx, appendTimeout)
		_, err := g.client.AppendPageView(actx, ev.pageView())
		cancel()
		l.record(g, ev, err, time.Since(ev.at))
	}
}

// record tallies an append. A failed one may still be stored, after a timeout or an
// attempt that the SDK retried, so its outcome is unknown.
func (l *load) record(g *generator, ev pageEvent, err error, latency time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		class := errorClass(err)
		g.unknown++
		g.errors[class]++
		if _, ok := g.samples[class]; !ok {
			g.samples[class] = err.Error()
		}
		g.tally.unknown = append(g.tally.unknown, ev)
		return
	}
	g.sent++
	if err = g.tally.add(ev); err != nil {
		l.failures = append(l.failures, fmt.Sprintf("%s: tally: %v", g.r.name, err))
	}
	day := dayOf(ev.at)
	if l.seen[day] == nil {
		l.seen[day] = newPairSet()
	}
	isNew := l.seen[day].add(ev.page, ev.user)
	if l.inWindow(ev.at) {
		g.appended++
		g.latency = append(g.latency, float64(latency)/float64(time.Millisecond))
		l.windowEvents++
		l.windowTags += int64(len(ev.tags))
		if isNew {
			l.newPairs++
		}
	}
}

func errorClass(err error) string {
	var ae smithy.APIError
	switch {
	case errors.As(err, &ae):
		return ae.ErrorCode()
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	return "other"
}

func throttled(class string) bool {
	return strings.Contains(class, "Throttl") || class == "ProvisionedThroughputExceededException" || class == "RequestLimitExceeded"
}

// sendProbes appends a probe in the generator's region every probeEvery until the window
// ends.
func (l *load) sendProbes(ctx context.Context, g *generator, probes *sync.WaitGroup) {
	tick := time.NewTicker(probeEvery)
	defer tick.Stop()
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			if !now.Before(l.windowEnd) {
				return
			}
		}
		probes.Go(func() { l.probe(ctx, g, n) })
	}
}

// probe appends a page view of a page of its own, which makes a new page_daily row, and
// times how long each region the append reaches takes to show the row.
func (l *load) probe(ctx context.Context, g *generator, n int) {
	page := fmt.Sprintf("/%s/probe-%s-%d", nonce, g.r.name, n)
	at := time.Now()
	_, err := g.client.AppendPageView(ctx, flux.PageView{UserID: page, Page: page, DurationMS: 1, EventTime: at})
	sent := time.Now()
	var observers sync.WaitGroup
	for _, obs := range l.h.regions {
		if obs != g.r && !l.h.replicated {
			continue
		}
		observers.Go(func() {
			p := probeResult{Origin: g.r.name, Observer: obs.name, Sent: sent.UTC(), Missed: true}
			if err == nil {
				p.LagMS, p.Missed = awaitRow(ctx, obs, page, at, sent)
			}
			l.mu.Lock()
			l.probes = append(l.probes, p)
			l.mu.Unlock()
		})
	}
	observers.Wait()
}

// awaitRow polls a region's page_daily for a page's row, and returns the time since sent
// at which it saw the row, or true for missed when it gave up.
func awaitRow(ctx context.Context, obs *region, page string, at, sent time.Time) (float64, bool) {
	for {
		rows, err := obs.client.PageStats(ctx, flux.PageStatsParams{Page: page, From: at, To: at})
		if err == nil && len(rows) > 0 {
			return float64(time.Since(sent)) / float64(time.Millisecond), false
		}
		if time.Since(sent) > probePatience || sleep(ctx, probePoll) != nil {
			return 0, true
		}
	}
}

// loaded records what the load did, and why it fails the run.
func (l *load) loaded(t *testing.T, res *throughputResult) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	res.Phase = "checking"
	var appended, unknown int64
	for i, g := range l.gens {
		rr := &res.PerRegion[i]
		rr.Appended, rr.Missed, rr.Unknown = g.appended, g.missed, g.unknown
		rr.Errors, rr.Samples, rr.Latency = g.errors, g.samples, newPercentiles(g.latency)
		rr.Shards = openShards(t, g.r)
		appended += g.appended
		unknown += g.unknown
		for class, n := range g.errors {
			if throttled(class) {
				l.failures = append(l.failures, fmt.Sprintf("%s: %d appends throttled (%s)", g.r.name, n, class))
			}
		}
	}
	res.Probes = l.probes
	if l.windowEvents > 0 {
		res.TagsPerEvent = float64(l.windowTags) / float64(l.windowEvents)
		res.UniqNew = float64(l.newPairs) / float64(l.windowEvents)
	}
	achieved := float64(appended) / l.windowEnd.Sub(l.windowStart).Seconds()
	if achieved < minAchieved**throughputRate {
		l.failures = append(l.failures, fmt.Sprintf("%.1f events/s appended in the window, short of the target %v: "+
			"the client can't keep up (docs/runbooks/benchmarks.md)", achieved, *throughputRate))
	}
	if unknown > maxUnknown {
		l.failures = append(l.failures, fmt.Sprintf("%d appends with an unknown outcome, more than %d", unknown, maxUnknown))
	}
	if i := slices.IndexFunc(l.probes, func(p probeResult) bool { return p.Missed }); i >= 0 {
		missed := 0
		for _, p := range l.probes {
			if p.Missed {
				missed++
			}
		}
		p := l.probes[i]
		l.failures = append(l.failures, fmt.Sprintf("%d probes never showed, the first from %s at %s in %s",
			missed, p.Origin, p.Sent.Format(time.RFC3339), p.Observer))
	}
	res.Failures = slices.Clone(l.failures)
}

// openShards counts the open shards of the region's stream of page_views.
func openShards(t *testing.T, r *region) int {
	t.Helper()
	in := &dynamodbstreams.DescribeStreamInput{StreamArn: new(streamARN(t, r, pageViews))}
	n := 0
	for {
		out, err := r.streams.DescribeStream(t.Context(), in)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range out.StreamDescription.Shards {
			if s.SequenceNumberRange == nil || s.SequenceNumberRange.EndingSequenceNumber == nil {
				n++
			}
		}
		if out.StreamDescription.LastEvaluatedShardId == nil {
			return n
		}
		in.ExclusiveStartShardId = out.StreamDescription.LastEvaluatedShardId
	}
}

// converge waits, for at most the harness's patience, until each region's views count what
// the generators appended to it, and returns how they fail to.
func (l *load) converge(t *testing.T) []string {
	t.Helper()
	all := newTally(l.users)
	for _, g := range l.gens {
		if err := all.merge(g.tally); err != nil {
			t.Fatal(err)
		}
	}
	var failures []string
	for i, obs := range l.h.regions {
		want := l.gens[i].tally
		if l.h.replicated {
			want = all
		}
		deadline := time.Now().Add(l.h.patience)
		for {
			err := l.h.settle(t.Context())
			if err == nil {
				err = l.check(t.Context(), obs, want)
			}
			if err == nil {
				break
			}
			if time.Now().After(deadline) || sleep(t.Context(), pollInterval) != nil {
				failures = append(failures, fmt.Sprintf("%s: views against the generators: %v", obs.name, err))
				break
			}
		}
	}
	return failures
}

func (l *load) check(ctx context.Context, obs *region, want *tally) error {
	states, err := obs.meta.ViewStates(ctx)
	if err != nil {
		return err
	}
	for v, s := range states {
		if s.Stream == pageViews && s.Failure != nil {
			return fmt.Errorf("view version %s broke: %+v", v, *s.Failure)
		}
	}
	if err = l.checkPages(ctx, obs, want); err != nil {
		return err
	}
	return l.checkTags(ctx, obs, want)
}

// checkPages compares each page's page_daily rows, through PageStats, with want.
func (l *load) checkPages(ctx context.Context, obs *region, want *tally) error {
	byPage := map[int]map[int64]pageDayWant{}
	for pd, w := range want.pageDays() {
		if byPage[pd.page] == nil {
			byPage[pd.page] = map[int64]pageDayWant{}
		}
		byPage[pd.page][pd.day] = w
	}
	pages := make(chan int)
	var mu sync.Mutex
	var errs []error
	var workers sync.WaitGroup
	for range checkWorkers {
		workers.Go(func() {
			for p := range pages {
				if err := checkPage(ctx, obs, p, byPage[p], l.start, l.windowEnd); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		})
	}
	for p := range byPage {
		pages <- p
	}
	close(pages)
	workers.Wait()
	return firstErrors(errs)
}

func dayText(day int64) string { return time.Unix(day*secondsPerDay, 0).UTC().Format(time.DateOnly) }

func checkPage(ctx context.Context, obs *region, p int, want map[int64]pageDayWant, from, to time.Time) error {
	rows, err := obs.client.PageStats(ctx, flux.PageStatsParams{Page: pageName(p), From: from, To: to})
	if err != nil {
		return fmt.Errorf("PageStats %s: %w", pageName(p), err)
	}
	got := map[int64]flux.PageStatsRow{}
	for _, row := range rows {
		if _, ok := want[dayOf(row.Day)]; !ok {
			return fmt.Errorf("page_daily of %s has a row for %s, where nothing was appended", pageName(p), dayText(dayOf(row.Day)))
		}
		got[dayOf(row.Day)] = row
	}
	for day, w := range want {
		row, ok := got[day]
		switch {
		case !ok && w.views.lo > 0:
			return fmt.Errorf("page_daily of %s has no row for %s, want %s views", pageName(p), dayText(day), w.views)
		case ok && (!w.views.holds(row.Views) || !w.visitors.holds(row.Visitors)):
			return fmt.Errorf("page_daily of %s on %s: %d views and %d visitors, want %s and %s",
				pageName(p), dayText(day), row.Views, row.Visitors, w.views, w.visitors)
		}
	}
	return nil
}

// checkTags compares each tag's TagTotals row with want. TagTotals returns at most 10 rows,
// so it asks for 10 tags at a time.
func (l *load) checkTags(ctx context.Context, obs *region, want *tally) error {
	tags, err := want.tagWants()
	if err != nil {
		return err
	}
	const perQuery = 10
	var errs []error
	for chunk := range slices.Chunk(slices.Sorted(maps.Keys(tags)), perQuery) {
		names := make([]string, len(chunk))
		for i, tag := range chunk {
			names[i] = tagName(tag)
		}
		rows, qerr := obs.client.TagTotals(ctx, flux.TagTotalsParams{Tags: names, Since: l.start.Truncate(time.Hour)})
		if qerr != nil {
			return fmt.Errorf("TagTotals: %w", qerr)
		}
		got := map[string]flux.TagTotalsRow{}
		for _, row := range rows {
			got[row.Tag] = row
		}
		for _, tag := range chunk {
			w, row := tags[tag], got[tagName(tag)]
			if !w.views.holds(row.TotalViews) || !w.users.holds(row.TotalUsers) {
				errs = append(errs, fmt.Errorf("tag_hourly of %s: %d views and %d users, want %s and %s",
					tagName(tag), row.TotalViews, row.TotalUsers, w.views, w.users))
			}
		}
	}
	return firstErrors(errs)
}

func (b bounds) holds(v uint64) bool { return b.lo <= v && v <= b.hi }

// firstErrors joins the first few errors.
func firstErrors(errs []error) error {
	const most = 5
	if len(errs) > most {
		errs = append(errs[:most], fmt.Errorf("and %d more", len(errs)-most))
	}
	return errors.Join(errs...)
}

// TestThroughputReport reads -throughput.out and CloudWatch, prints the report, and writes
// the metrics it read beside the result, in a file ending .metrics.json. It starts no
// harness, so it runs after teardown too: CloudWatch keeps 1-minute datapoints for 15 days.
//
//nolint:paralleltest // it may wait minutes for CloudWatch
func TestThroughputReport(t *testing.T) {
	if *throughputOut == "" {
		t.Skip("set -throughput.out to report on a throughput run (docs/runbooks/benchmarks.md)")
	}
	if *target != targetAWS {
		t.Skip("the report reads CloudWatch, so it runs with -target=aws only")
	}
	var res throughputResult
	readJSON(t, *throughputOut, &res)
	period := res.WindowEnd.Sub(res.WindowStart)
	if res.Target != targetAWS || period < time.Minute || period%time.Minute != 0 || !res.WindowStart.Equal(res.WindowStart.Truncate(time.Minute)) {
		t.Fatalf("%s holds a run on %s from %s to %s; the report needs a run on aws, in whole minutes",
			*throughputOut, res.Target, res.WindowStart, res.WindowEnd)
	}
	cfg, cat := compile(t, e2eSQL, res.Regions)
	if wait := time.Until(res.WindowEnd.Add(reportDelay)); wait > 0 {
		t.Logf("waiting %v for CloudWatch's datapoints", wait.Round(time.Second))
		if err := sleep(t.Context(), wait); err != nil {
			t.Fatal(err)
		}
	}
	metrics := map[string]map[string]float64{}
	for i := range res.PerRegion {
		metrics[res.PerRegion[i].Name] = readMetrics(t, &res, &res.PerRegion[i])
	}
	data, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	// The metrics with the result can replace the report test's fixture.
	if err = os.WriteFile(strings.TrimSuffix(*throughputOut, ".json")+".metrics.json", append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	text, failures, err := throughputReport(&res, metrics, cfg, cat)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + text)
	for _, f := range failures {
		t.Error(f)
	}
}

// readMetrics reads every statistic the report needs of a region, each over the window as
// one datapoint, with the default credential chain.
func readMetrics(t *testing.T, res *throughputResult, rr *regionResult) map[string]float64 {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(t.Context(), awsconfig.WithRegion(rr.Name))
	if err != nil {
		t.Fatal(err)
	}
	period := int32(res.WindowEnd.Sub(res.WindowStart) / time.Second)
	var queries []cwtypes.MetricDataQuery
	for _, q := range metricQueries(res, rr) {
		dims := make([]cwtypes.Dimension, len(q.Dims))
		for i, d := range q.Dims {
			dims[i] = cwtypes.Dimension{Name: new(d.Name), Value: new(d.Value)}
		}
		queries = append(queries, cwtypes.MetricDataQuery{
			Id: new(q.ID),
			MetricStat: &cwtypes.MetricStat{
				Metric: &cwtypes.Metric{Namespace: new(q.Namespace), MetricName: new(q.Name), Dimensions: dims},
				Period: new(period), Stat: new(q.Stat),
			},
		})
	}
	out := map[string]float64{}
	pages := cloudwatch.NewGetMetricDataPaginator(cloudwatch.NewFromConfig(cfg), &cloudwatch.GetMetricDataInput{
		MetricDataQueries: queries, StartTime: new(res.WindowStart), EndTime: new(res.WindowEnd),
	})
	for pages.HasMorePages() {
		page, perr := pages.NextPage(t.Context())
		if perr != nil {
			t.Fatalf("%s: %v", rr.Name, perr)
		}
		for _, r := range page.MetricDataResults {
			switch len(r.Values) {
			case 0:
			case 1:
				out[aws.ToString(r.Id)] = r.Values[0]
			default:
				t.Fatalf("%s: %s has %d datapoints over the window, want 1", rr.Name, aws.ToString(r.Id), len(r.Values))
			}
		}
	}
	return out
}

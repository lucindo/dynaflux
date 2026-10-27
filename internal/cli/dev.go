// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/esmlocal"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/localstore"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

const (
	defaultDevEndpoint = "http://127.0.0.1:8000"
	devPoll            = time.Second // between reads of the streams
	// A job's step works for devStepWork, and a chunk it started by then, of at most devChunk
	// rows, still finishes; dev projects after each step, well within the time split's late
	// bound.
	devStepWork  = time.Second
	devChunk     = 200
	devStepRetry = 5 * time.Second
	devMemory    = 64 << 20 // the bytes a step holds in sort buffers and merge blocks
	devExecution = "local"
	// devSkew and devLanding are the time split's bounds on one host with one clock and no
	// replication: an additive version goes live about 7 seconds after its activation.
	devSkew    = time.Second
	devLanding = 5 * time.Second
)

func devCommand() command {
	return command{
		name:    "dev",
		args:    "[-endpoint url] [-reset]",
		summary: "run the store on DynamoDB Local for an application under development",
		doc: `Dev runs the store on DynamoDB Local, in the first of regions, for an application
under development. It creates the tables that are missing, registers the catalog
compiled from the SQL files, and projects each stream as the projector functions
do. It then fills each view version that isn't live from its stream table, as
dynaflux backfill does on AWS: an idempotent version is live in about a second, an
additive one in about 7 seconds. It prints the environment that points the
application at DynamoDB Local on standard output, then a line for each batch it
projects, until it is interrupted.

Dev runs what dynaflux generate wrote: it refuses to run while generate would change
dynaflux.lock. Run again, it reads the streams from their start and skips what it
projected before. -reset deletes the store's tables first, with their events.

Start DynamoDB Local with

  docker run --rm -p 127.0.0.1:8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -inMemory

Exit status: 0 when interrupted; 1 for errors in the project, an out-of-date
dynaflux.lock and a batch that fails; and 2 for usage and I/O problems, and
DynamoDB Local not answering.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			var o devOptions
			fs.StringVar(&o.endpoint, "endpoint", defaultDevEndpoint, "DynamoDB Local's loopback `URL`")
			fs.BoolVar(&o.reset, "reset", false, "delete the store's tables first")
			return func(c *cli, args []string) int { return c.dev(o, args) }
		},
	}
}

type devOptions struct {
	endpoint string
	reset    bool
}

func (c *cli) dev(o devOptions, args []string) int {
	if len(args) > 0 {
		return c.usageError("dev", "unexpected arguments: "+strings.Join(args, " "))
	}
	if err := checkEndpoint(o.endpoint); err != nil {
		return c.usageError("dev", "-endpoint "+o.endpoint+": "+err.Error())
	}
	p, ds, status := c.load()
	if status = c.diagnostics(ds, status); status != exitOK {
		return status
	}
	if len(p.lockChanges) > 0 {
		c.errorf("dynaflux dev: dynaflux.lock is out of date:")
		for _, ch := range p.lockChanges {
			c.errorf("  %s", ch)
		}
		c.errorf("  hint: run dynaflux generate, which the application needs too, then dynaflux dev")
		return exitErrors
	}
	staging, err := os.MkdirTemp("", "dynaflux-dev-")
	if err != nil {
		c.errorf("dynaflux dev: %v", err)
		return exitUsage
	}
	defer func() {
		if rerr := os.RemoveAll(staging); rerr != nil {
			c.errorf("dynaflux dev: %v", rerr)
		}
	}()
	d, status := c.openDev(p, o, staging)
	if status != exitOK {
		return status
	}
	defer d.close()
	return d.serve()
}

// devRun is a run of dev: the store on DynamoDB Local, its projector and its jobs.
type devRun struct {
	c        *cli
	endpoint string
	naming   catalog.Naming
	region   string
	states   *meta.StateCache // the projector's, read again after each step of a job
	core     *projector.Core
	timeout  time.Duration // each batch's
	streams  []devStream
	env      *backfill.Env
	runs     *dirRuns
	jobs     []*devJob
}

type devStream struct {
	name   string
	reader *esmlocal.Reader
}

// devJob is a job that dev steps, at next.
type devJob struct {
	version string
	attempt int
	next    time.Time
}

// openDev reaches DynamoDB Local, creates the tables that are missing, deleting them first
// with -reset, and registers the catalog.
func (c *cli) openDev(p *project, o devOptions, staging string) (*devRun, int) {
	naming := p.cfg.Naming()
	region := p.cfg.Regions[0] // DynamoDB Local serves any region
	cfg := localConfig(o.endpoint, region)
	ddb := dynamodb.NewFromConfig(cfg)
	c.header(naming, "endpoint "+o.endpoint, []string{region})
	once := func(opts *dynamodb.Options) { opts.RetryMaxAttempts = 1 } // a missing emulator is reported at once
	if _, err := ddb.ListTables(c.ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(1)}, once); err != nil {
		c.errorf("dynaflux dev: DynamoDB Local doesn't answer at %s: %v\n  hint: start it with %s", o.endpoint, err, dockerRun(o.endpoint))
		return nil, exitUsage
	}
	if status := c.createDevTables(p, o.reset, ddb); status != exitOK {
		return nil, status
	}
	b := dynamo.New(ddb)
	m := meta.New(b, naming.MetaTable())
	data, status := c.registerDevCatalog(m, p, region)
	if status != exitOK {
		return nil, status
	}
	compiled, err := projector.Compile(p.cat)
	if err != nil {
		c.errorf("dynaflux dev: %v", err)
		return nil, exitErrors
	}
	runs, err := openDirRuns(staging)
	if err != nil {
		c.errorf("dynaflux dev: %v", err)
		return nil, exitUsage
	}
	states := meta.NewStateCache(m)
	core := projector.New(compiled, b, nil, projector.Config{Naming: naming, States: states})
	d := &devRun{
		c: c, endpoint: o.endpoint, naming: naming, region: region, states: states, core: core,
		timeout: time.Duration(p.cfg.Runtime.TimeoutSeconds) * time.Second, runs: runs,
		env: &backfill.Env{
			Naming: naming, Catalog: p.cat, Hash: catalog.Hash(data), Meta: m, Store: b, Core: core,
			Units: devNoArchive{}, Tables: devReplicas{ddb}, Runs: runs, Lag: devNoLag{},
			Bounds: meta.Bounds{Skew: devSkew, Landing: devLanding}, Memory: devMemory, Chunk: devChunk, Segments: 1,
			Log: slog.New(slog.NewTextHandler(&c.errOut, &slog.HandlerOptions{Level: slog.LevelWarn})),
		},
	}
	if err = d.openStreams(ddb, dynamodbstreams.NewFromConfig(cfg), p.cat); err != nil {
		d.close()
		c.errorf("dynaflux dev: %v", err)
		return nil, exitUsage
	}
	return d, exitOK
}

// dockerRun is the command that starts DynamoDB Local at endpoint.
func dockerRun(endpoint string) string {
	port := "80"
	if u, err := url.Parse(endpoint); err == nil && u.Port() != "" {
		port = u.Port()
	}
	return "docker run --rm -p 127.0.0.1:" + port + ":8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -inMemory"
}

// createDevTables creates the store's tables that are missing, after deleting them all with
// reset.
func (c *cli) createDevTables(p *project, reset bool, ddb *dynamodb.Client) int {
	m, err := infra.Build(p.cfg, p.cat, infra.Options{})
	var ts []localstore.Table
	if err == nil {
		ts, err = localstore.Tables(m, p.cfg.Naming().Prefix)
	}
	if err != nil {
		c.errorf("dynaflux dev: %v", err)
		return exitErrors
	}
	if reset {
		deleted, derr := localstore.Delete(c.ctx, ddb, ts)
		for _, name := range deleted {
			c.errorf("deleted table %s", name)
		}
		if derr != nil {
			c.errorf("dynaflux dev: %v", derr)
			return exitUsage
		}
	}
	created, err := localstore.Ensure(c.ctx, ddb, ts)
	for _, name := range created {
		c.errorf("created table %s", name)
	}
	if err != nil {
		c.errorf("dynaflux dev: %v", err)
		return exitUsage
	}
	return exitOK
}

// registerDevCatalog makes the project's catalog the region's current one, and returns its
// bytes.
func (c *cli) registerDevCatalog(m *meta.Meta, p *project, region string) ([]byte, int) {
	reg, ok, err := m.Current(c.ctx)
	if err != nil {
		c.errorf("dynaflux dev: read the registered catalog: %v", err)
		return nil, exitUsage
	}
	if ok {
		if status := c.checkFormat(region, reg); status != exitOK {
			return nil, status
		}
	}
	data, err := catalog.Marshal(p.cat)
	if err != nil {
		c.errorf("dynaflux dev: %v", err)
		return nil, exitErrors
	}
	bi, _ := debug.ReadBuildInfo() // A nil BuildInfo reads as (devel).
	if reg, err = m.RegisterCatalog(c.ctx, data, headerVersion(bi)); err != nil {
		c.errorf("dynaflux dev: register the catalog: %v", err)
		return nil, exitUsage
	}
	c.errorf("registered catalog %s", reg.Hash[:12])
	return data, exitOK
}

// openStreams starts a reader of each stream table's stream, from its start.
func (d *devRun) openStreams(ddb *dynamodb.Client, sc *dynamodbstreams.Client, cat *catalog.Catalog) error {
	for _, s := range cat.Streams {
		table := d.naming.StreamTable(s.Name)
		out, err := ddb.DescribeTable(d.c.ctx, &dynamodb.DescribeTableInput{TableName: &table})
		if err != nil {
			return fmt.Errorf("describe table %s: %w", table, err)
		}
		d.streams = append(d.streams, devStream{s.Name, esmlocal.NewReader(sc, aws.ToString(out.Table.LatestStreamArn))})
	}
	return nil
}

func (d *devRun) close() {
	if err := d.runs.close(); err != nil {
		d.c.errorf("dynaflux dev: %v", err)
	}
}

// serve prints the application's environment, projects every stream until it is caught up,
// then starts a job for every version that isn't live, broken or retired, and goes on
// projecting and stepping the jobs until it is interrupted.
func (d *devRun) serve() int {
	c := d.c
	fmt.Fprintf(&c.out, "AWS_ENDPOINT_URL_DYNAMODB=%s\nAWS_REGION=%s\nAWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n%s=%s\n",
		d.endpoint, d.region, localAccessKey, localSecretKey, catalog.EnvPrefix, d.naming.Prefix)
	c.flush()
	if err := d.project(); err != nil {
		return d.stopped(err)
	}
	c.errorf("caught up")
	if err := d.startJobs(); err != nil {
		return d.stopped(err)
	}
	c.flush()
	tick := time.NewTicker(devPoll)
	defer tick.Stop()
	for {
		if err := d.stepJobs(); err != nil {
			return d.stopped(err)
		}
		c.flush()
		select {
		case <-c.ctx.Done():
			return exitOK
		case <-tick.C:
		}
		if err := d.project(); err != nil {
			return d.stopped(err)
		}
		c.flush()
	}
}

// stopped returns the exit status of a run that err stopped: 0 when it was interrupted, and 2
// when DynamoDB Local stopped answering.
func (d *devRun) stopped(err error) int {
	if d.c.ctx.Err() != nil {
		return exitOK
	}
	if _, gone := errors.AsType[*net.OpError](err); gone {
		d.c.errorf("dynaflux dev: DynamoDB Local stopped answering at %s: %v\n  hint: start it with %s, then run dynaflux dev again",
			d.endpoint, err, dockerRun(d.endpoint))
		return exitUsage
	}
	d.c.errorf("dynaflux dev: %v\n  hint: fix the cause and run dynaflux dev again; it reads the streams from their start", err)
	return exitErrors
}

// project delivers each stream's new records to the projector, as Lambda does.
func (d *devRun) project() error {
	for _, s := range d.streams {
		if err := esmlocal.Drain(d.c.ctx, s.reader, d.timeout, d.function(s.name)); err != nil {
			return fmt.Errorf("project %s: %w", s.name, err)
		}
	}
	return nil
}

// startJobs creates or claims a job for every version that isn't live, broken or retired:
// the table source, split by time for an additive version and not split for an idempotent
// one.
func (d *devRun) startJobs() error {
	states, err := d.env.Meta.ViewStates(d.c.ctx)
	if err != nil {
		return err
	}
	for id, vv := range d.env.Catalog.Versions() {
		switch states[id].State {
		case meta.Live, meta.Broken, meta.Retired:
			continue
		case meta.PendingBackfill, meta.Activating:
		}
		split := meta.SplitNone
		if vv.FencedHandoff() {
			split = meta.SplitTime
		}
		j, serr := backfill.Start(d.c.ctx, d.env.Meta, backfill.Request{
			Version: id, Stream: vv.Stream, Name: id, Execution: func(int) string { return devExecution }, Split: split,
		})
		if serr != nil {
			return serr
		}
		d.c.errorf("%s: filling from the stream table, split %s", id, j.Split)
		d.jobs = append(d.jobs, &devJob{version: id, attempt: j.Attempt})
	}
	return nil
}

// stepJobs steps each job whose time came, for devStepWork each, and projects after each step,
// so the projector stays well within the time split's late bound. It reads the view states
// again after each step, so the projector applies a version from its activation on: dev's
// bounds leave no time for the cached states to expire.
func (d *devRun) stepJobs() error {
	left := d.jobs[:0]
	for _, j := range d.jobs {
		if time.Now().Before(j.next) {
			left = append(left, j)
			continue
		}
		ctx, cancel := context.WithTimeout(d.c.ctx, backfill.Reserve+devStepWork)
		wait, done, err := backfill.Step(ctx, d.env, j.version, j.attempt, devExecution)
		cancel()
		if d.c.ctx.Err() != nil {
			return d.c.ctx.Err()
		}
		states, serr := d.states.States(d.c.ctx, true)
		if serr != nil {
			return serr
		}
		switch {
		case err != nil:
			d.c.errorf("%s: backfill: %v", j.version, err)
			j.next = time.Now().Add(devStepRetry)
			left = append(left, j)
		case done:
			d.c.errorf("%s: %s", j.version, devOutcome(states[j.version]))
		default:
			j.next = time.Now().Add(wait)
			left = append(left, j)
		}
		if err = d.project(); err != nil {
			return err
		}
	}
	d.jobs = left
	return nil
}

// devOutcome says what state a version's job left it in.
func devOutcome(vs meta.ViewState) string {
	switch {
	case vs.State == meta.Live && !vs.PartialSince.IsZero():
		return "live, partial since " + vs.PartialSince.Format(time.RFC3339)
	case vs.State == meta.Broken && vs.Failure != nil:
		return "broken: " + vs.Failure.Error
	}
	return string(vs.State)
}

// devNoArchive lists no delivery units: only a job that reads the archive fences them, and
// dev reads stream tables.
type devNoArchive struct{}

func (devNoArchive) Units(context.Context, string) ([]string, error) {
	return nil, errors.New("dynaflux dev reads no archive")
}

// devNoLag reports no lag: dev projects between the steps of its jobs.
type devNoLag struct{}

func (devNoLag) Lag(context.Context) (time.Duration, error) { return 0, nil }

// devReplicas describes stream tables on DynamoDB Local, which holds one replica of each.
type devReplicas struct{ c *dynamodb.Client }

func (t devReplicas) Born(ctx context.Context, table string) (time.Time, bool, error) {
	out, err := t.c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
	if err != nil {
		return time.Time{}, false, err
	}
	return aws.ToTime(out.Table.CreationDateTime), out.Table.RestoreSummary != nil, nil
}

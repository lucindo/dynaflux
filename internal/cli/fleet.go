// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/meta"
)

// fleet is the regions of the deployed store that a run works in.
type fleet struct {
	naming    catalog.Naming
	partition string
	account   string        // "" on DynamoDB Local
	endpoint  string        // DynamoDB Local's; "" on AWS
	regions   []*deployment // in order
	names     []string      // the regions' names, in order
}

// deployment is one region of a deployed store, as a command reads it.
type deployment struct {
	regionState

	naming  catalog.Naming
	region  string
	clients regionClients
	meta    *meta.Meta
	err     error // the region couldn't be read
	// compiled and engine serve the commands that read one region's tables.
	compiled *engine.Catalog
	engine   *engine.Engine
}

// regionState is what every command reads of a region first.
type regionState struct {
	notDeployed bool // the region has no catalog
	reg         meta.Registered
	data        []byte                    // the deployed catalog
	cat         *catalog.Catalog          // nil when not deployed, unreadable or in a newer format
	states      map[string]meta.ViewState // with cat, for a command that reads the states
	jobs        []meta.Job                // likewise, in the order of their versions
}

// regionPicker returns the regions a run works in, given the AWS SDK's region, or a usage
// message.
type regionPicker func(sdk string) ([]string, string)

// someRegions picks the regions that -region names, or else every region of cfg; on DynamoDB
// Local, which serves every region from one database, the first.
func someRegions(cfg *config.Config, named repeated, endpoint string) regionPicker {
	return func(string) ([]string, string) {
		switch {
		case len(named) > 0:
		case endpoint != "":
			return cfg.Regions[:1], ""
		default:
			return cfg.Regions, ""
		}
		var names []string
		for _, r := range named {
			if msg := refusedRegion(r); msg != "" {
				return nil, "-region " + msg
			}
			if !slices.Contains(cfg.Regions, r) {
				return nil, fmt.Sprintf("-region %s isn't in regions (%s)", r, strings.Join(cfg.Regions, ", "))
			}
			if !slices.Contains(names, r) {
				names = append(names, r)
			}
		}
		return names, ""
	}
}

// oneRegion picks the region that -region names, or else the AWS SDK's, which must be one of
// cfg's; on DynamoDB Local, which serves any region, the first of cfg's.
func oneRegion(cfg *config.Config, named, endpoint string) regionPicker {
	return func(sdk string) ([]string, string) {
		if endpoint != "" {
			sdk = cfg.Regions[0]
		}
		r, msg := pickRegion(named, sdk, cfg.Regions)
		return []string{r}, msg
	}
}

// openStore starts a run of command name in cfg's store: it checks -endpoint and -prefix,
// connects to AWS, or to DynamoDB Local at endpoint, picks the regions, finds the account on
// AWS, and prints the header. Its deployments are unread.
func (c *cli) openStore(ctx context.Context, name string, cfg *config.Config, prefix optionalString, endpoint string,
	pick regionPicker,
) (*fleet, int) {
	if endpoint != "" {
		if err := checkEndpoint(endpoint); err != nil {
			return nil, c.usageError(name, "-endpoint "+endpoint+": "+err.Error())
		}
	}
	f := &fleet{naming: cfg.Naming(), endpoint: endpoint}
	if status := c.applyPrefix(name, prefix, &f.naming); status != exitOK {
		return nil, status
	}
	cl, err := c.connect(ctx, endpoint)
	if err != nil {
		c.errorf("dynaflux %s: %v", name, err)
		return nil, exitUsage
	}
	var msg string
	if f.names, msg = pick(cl.sdkRegion()); msg != "" {
		return nil, c.usageError(name, msg)
	}
	where := "endpoint " + endpoint
	if endpoint == "" {
		out, ierr := cl.clients(f.names[0]).identity.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		if ierr != nil {
			c.errorf("dynaflux %s: find the AWS account: %v\n  hint: set AWS_PROFILE, or log in with aws sso login", name, ierr)
			return nil, exitUsage
		}
		f.account = aws.ToString(out.Account)
		f.partition, _, _ = strings.Cut(strings.TrimPrefix(aws.ToString(out.Arn), "arn:"), ":")
		where = "account " + f.account
	}
	c.header(f.naming, where, f.names)
	for _, r := range f.names {
		d := &deployment{naming: f.naming, region: r, clients: cl.clients(r)}
		d.meta = meta.New(d.clients.data, f.naming.MetaTable())
		f.regions = append(f.regions, d)
	}
	return f, exitOK
}

// openFleet starts a run of command name in the regions of cfg's store that regions names, as
// openStore does, and reads each one's state, the regions at once. A region that can't be
// read keeps the error.
func (c *cli) openFleet(ctx context.Context, name string, cfg *config.Config, prefix optionalString, regions repeated,
	endpoint string,
) (*fleet, int) {
	f, status := c.openStore(ctx, name, cfg, prefix, endpoint, someRegions(cfg, regions, endpoint))
	if status != exitOK {
		return nil, status
	}
	var wg sync.WaitGroup
	for _, d := range f.regions {
		wg.Go(func() {
			if d.err = d.readCatalog(ctx); d.err == nil && d.cat != nil {
				d.err = d.readStates(ctx)
			}
		})
	}
	wg.Wait()
	return f, exitOK
}

// readCatalog reads the region's registered catalog and, unless it is in a newer format,
// decodes it. A missing meta table, as with a prefix that isn't deployed, is no catalog.
func (d *deployment) readCatalog(ctx context.Context) error {
	dc, err := deployed.ReadCatalog(ctx, d.meta)
	switch {
	case errors.Is(err, deployed.ErrNotDeployed):
		d.notDeployed = true
		return nil
	case err != nil:
		return fmt.Errorf("read the deployed catalog: %w", err)
	}
	d.reg, d.data = dc.Registered, dc.Data
	if _, newer := formatFinding(d.region, d.reg); newer {
		return nil
	}
	if d.cat, err = catalog.Unmarshal(dc.Data); err != nil {
		return fmt.Errorf("the deployed catalog %s: %w", dc.Hash, err)
	}
	return nil
}

// readStates reads the region's view states and jobs.
func (d *deployment) readStates(ctx context.Context) error {
	var err error
	if d.states, err = d.meta.ViewStates(ctx); err != nil {
		return fmt.Errorf("read the view states: %w", err)
	}
	if d.jobs, err = d.meta.Jobs(ctx); err != nil {
		return fmt.Errorf("read the jobs: %w", err)
	}
	return nil
}

// refused reports why command name, which writes, can't work in d, and the exit status it
// calls for: exitUsage when d couldn't be read, exitErrors when it has no catalog or one in a
// newer format (DF7001), and exitOK when it can work there. Commands that only read report a
// region without a catalog as a state of it instead.
func (c *cli) refused(name string, d *deployment) int {
	switch {
	case d.err != nil:
		c.errorf("dynaflux %s: %s: %v", name, d.region, d.err)
		return exitUsage
	case d.notDeployed:
		c.errorf("dynaflux %s: %s: no catalog is deployed in %s\n  hint: deploy regional.yaml in %s", name, d.region, d.naming.MetaTable(), d.region)
		return exitErrors
	case d.cat == nil:
		f, _ := formatFinding(d.region, d.reg)
		f.write(&c.errOut)
		return exitErrors
	}
	return exitOK
}

// state returns the state of version id; a version without an item is pending.
func (r *regionState) state(id string) meta.ViewState {
	vs, ok := r.states[id]
	if !ok {
		return meta.ViewState{Version: id, State: meta.PendingBackfill}
	}
	return vs
}

// job returns the job of version id, and whether it has one.
func (r *regionState) job(id string) (meta.Job, bool) {
	i := slices.IndexFunc(r.jobs, func(j meta.Job) bool { return j.Version == id })
	if i < 0 {
		return meta.Job{}, false
	}
	return r.jobs[i], true
}

// runs reports whether a backfill of version id runs: its job is running and doesn't wait.
func (r *regionState) runs(id string) bool {
	j, ok := r.job(id)
	return ok && j.State == meta.JobRunning && j.Waits == ""
}

// header prints the line on stderr that says where a command works: store, prefix, account
// or endpoint, and regions.
func (c *cli) header(naming catalog.Naming, where string, regions []string) {
	label := "regions"
	if len(regions) == 1 {
		label = "region"
	}
	c.errorf("store %s  prefix %s  %s  %s %s", naming.Store, cmp.Or(naming.Prefix, "(none)"), where, label, strings.Join(regions, ", "))
}

// running returns the ARNs of the job's executions that run in d's region: the one that ran
// its last step, or, when that one handed over, the continuation it started (ADR 0011).
func (f *fleet) running(ctx context.Context, d *deployment, j meta.Job) ([]string, error) {
	e, err := deployed.Running(ctx, d.clients.sfn, f.machine(d.region), j)
	if err != nil || e.ARN == "" {
		return nil, err
	}
	return []string{e.ARN}, nil
}

// machine returns the ARN of the backfill state machine in region.
func (f *fleet) machine(region string) string {
	return "arn:" + f.partition + ":states:" + region + ":" + f.account + ":stateMachine:" + f.naming.BackfillStateMachine()
}

// execution returns the ARN of the execution called name in region.
func (f *fleet) execution(region, name string) string {
	return "arn:" + f.partition + ":states:" + region + ":" + f.account + ":execution:" + f.naming.BackfillStateMachine() + ":" + name
}

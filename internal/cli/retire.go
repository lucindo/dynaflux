// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"cmp"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/meta"
)

// retireName is the command's name, and what its plan says it does in a region.
const retireName = "retire"

func retireCommand() command {
	return command{
		name:    retireName,
		args:    "[flags] <view>@v<N>",
		summary: "retire a view version that is no longer current",
		doc: `Retire removes a view version that dynaflux.lock keeps but no longer makes current,
in one environment (docs/spec/lifecycle.md §8). It first checks each region of the
deployed store: the region's deployed catalog doesn't make the version current,
the view's current version there is live (unless the retired version is broken),
and no backfill of the version runs. It lists every check that fails, and then
writes nothing.

When every check passes, it asks, then sets the version retired in each region,
which stops its projection and its queries, and marks it retired in
dynaflux.lock; -yes answers for it. Then run dynaflux generate and deploy
regional.yaml in every region, which deletes the version's table. Each other
environment that deploys the same dynaflux.lock needs retire too, with its
-prefix, before its deploy.

With -endpoint it works on DynamoDB Local.

Exit status: 0 when the version is retired, 1 when a check failed or the
question was declined, and 2 for usage, I/O and AWS access problems, and when a
region couldn't be read.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			o := retireOptions{}
			fs.Var(&o.prefix, "prefix", "the `prefix` of the environment's resource names (default prefix in dynaflux.yaml)")
			fs.Var(&o.regions, "region", "a `region` to retire the version in, one of regions in dynaflux.yaml; repeat it for each one (default all of them)")
			fs.StringVar(&o.endpoint, "endpoint", "",
				"work on DynamoDB Local at this loopback `URL`, such as http://127.0.0.1:8000, with fake credentials, in the first of regions unless -region names others")
			fs.BoolVar(&o.yes, "yes", false, "retire without asking")
			return func(c *cli, args []string) int { return c.retire(o, args) }
		},
	}
}

type retireOptions struct {
	prefix   optionalString
	regions  repeated
	endpoint string
	yes      bool
}

// retireItem is what retire does with the version in a region, and why.
type retireItem struct {
	write    bool // the region needs the version's tombstone
	stream   string
	detail   string    // what the plan line says after the action
	refusals []finding // the checks that failed
}

func (c *cli) retire(o retireOptions, args []string) int {
	if len(args) != 1 {
		return c.usageError(retireName, "name one view version, such as page_daily@v1")
	}
	id := args[0]
	if _, _, ok := parseVersionID(id); !ok {
		return c.usageError(retireName, id+" isn't a view version; name one such as page_daily@v1")
	}
	root, status := c.projectRoot()
	if status != exitOK {
		return status
	}
	defer root.Close()
	lock, entry, status := c.retiredEntry(root, id)
	if status != exitOK {
		return status
	}
	cfg, status := c.loadConfig()
	if status != exitOK {
		return status
	}
	f, status := c.openFleet(c.ctx, retireName, cfg, o.prefix, o.regions, o.endpoint)
	if status != exitOK {
		return status
	}
	// DynamoDB Local serves every region from one database.
	var unchecked []string
	if f.endpoint == "" {
		unchecked = slices.DeleteFunc(slices.Clone(cfg.Regions), func(r string) bool { return slices.Contains(f.names, r) })
	}
	items, status := f.planRetire(c, id, prefixFlag(o.prefix))
	if status = max(status, c.printRetirePlan(f, id, entry, items, unchecked)); status != exitOK {
		return status
	}
	if status = c.writeTombstones(f, id, items, o.yes); status != exitOK {
		return status
	}
	fmt.Fprintf(&c.out, "\nChecked prefix %s in %s.\n", cmp.Or(f.naming.Prefix, "(none)"), strings.Join(f.names, ", "))
	if len(unchecked) > 0 && entry.State != catalog.StateRetired {
		fmt.Fprintf(&c.out, "%s is unchanged: a deploy would retire %s in %s without these checks. "+
			"Run dynaflux retire %s%s without -region to check every region and mark it retired.\n",
			catalog.LockFileName, id, strings.Join(unchecked, ", "), id, prefixFlag(o.prefix))
		return exitOK
	}
	if entry.State != catalog.StateRetired {
		view, _, _ := strings.Cut(id, "@v")
		if status = c.writeRetiredLock(root, lock, view, entry.Number); status != exitOK {
			return status
		}
		fmt.Fprintf(&c.out, "%s: %s retired\n", catalog.LockFileName, id)
	}
	fmt.Fprintf(&c.out, "Next: run dynaflux generate, and deploy regional.yaml in every region, which deletes the table of %s.\n", id)
	fmt.Fprintf(&c.out, "Before you deploy another environment with this %s, run dynaflux retire %s -prefix <its prefix> there, with its credentials.\n",
		catalog.LockFileName, id)
	return exitOK
}

// writeTombstones asks, then sets version id retired in each region whose item needs it.
// When a write fails, dynaflux.lock stays as it is, so that a run again finishes the job.
func (c *cli) writeTombstones(f *fleet, id string, items []*retireItem, yes bool) int {
	var regions []string
	for i, it := range items {
		if it.write {
			regions = append(regions, f.names[i])
		}
	}
	if len(regions) == 0 {
		return exitOK
	}
	c.errorf("retire can't see application builds: one older than the deployed catalog may still query %s, "+
		"and gets ErrViewNotReady once it is retired. Deploy the application first.", id)
	if s := c.confirm(retireName, fmt.Sprintf("retire %s in %s?", id, strings.Join(regions, ", ")), yes); s != exitOK {
		return s
	}
	status := exitOK
	for i, it := range items {
		if !it.write {
			continue
		}
		if _, err := f.regions[i].meta.Retire(c.ctx, id, it.stream); err != nil {
			c.errorf("dynaflux retire: %s: %v", f.names[i], err)
			status = exitUsage
			continue
		}
		fmt.Fprintf(&c.out, "%s: %s retired\n", f.names[i], id)
	}
	if status != exitOK {
		c.errorf("dynaflux retire: %s is unchanged; run dynaflux retire again", catalog.LockFileName)
	}
	return status
}

// retiredEntry reads dynaflux.lock and returns it with version id's entry, which must be
// retained or retired (DF7301). A missing lock holds no version.
func (c *cli) retiredEntry(root *os.Root, id string) (*catalog.Lock, catalog.ViewVersion, int) {
	lock := &catalog.Lock{}
	data, err := root.ReadFile(catalog.LockFileName)
	if err == nil {
		lock, err = catalog.ParseLock(data)
	}
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		c.errorf("dynaflux retire: %s: %v", catalog.LockFileName, err)
		return nil, catalog.ViewVersion{}, exitUsage
	}
	view, _, _ := strings.Cut(id, "@v")
	i := slices.IndexFunc(lock.Views[view], func(vv catalog.ViewVersion) bool { return catalog.VersionID(view, vv.Number) == id })
	lockFinding := func(msg, hint string) (*catalog.Lock, catalog.ViewVersion, int) {
		f := finding{pos: &diag.Pos{File: catalog.LockFileName, Line: 1, Col: 1}, severity: diag.SeverityError, code: "DF7301", message: msg, hint: hint}
		f.write(&c.errOut)
		return nil, catalog.ViewVersion{}, exitErrors
	}
	switch {
	case i < 0:
		return lockFinding(fmt.Sprintf("%s doesn't hold %s", catalog.LockFileName, id),
			"name a version that "+catalog.LockFileName+" keeps; dynaflux status lists the deployed ones")
	case lock.Views[view][i].State == catalog.StateCurrent:
		return lockFinding(fmt.Sprintf("%s is the current version of %s in %s", id, view, catalog.LockFileName),
			"make another version current, or remove the view's SQL, and run dynaflux generate")
	}
	return lock, lock.Views[view][i], exitOK
}

// planRetire works out, with reads only, what retire does with version id in each region it
// can work in. It reports each region it can't work in, which gets no item, and returns the
// status that calls for.
func (f *fleet) planRetire(c *cli, id, prefix string) ([]*retireItem, int) {
	status := exitOK
	items := make([]*retireItem, len(f.regions))
	for i, d := range f.regions {
		if s := c.refused(retireName, d); s != exitOK {
			status = max(status, s)
			continue
		}
		items[i] = &retireItem{}
		items[i].plan(d, id, prefix)
	}
	return items, status
}

// plan runs retire's checks on version id in region d (lifecycle.md §8). A region that
// already holds the version retired, or never held it, needs nothing.
func (it *retireItem) plan(d *deployment, id, prefix string) {
	vv, defined := d.cat.Version(id)
	vs, held := d.states[id]
	switch {
	case held && vs.State == meta.Retired:
		it.detail = "already retired"
		return
	case !held && !defined:
		it.detail = "not deployed"
		return
	case !held:
		vs = meta.ViewState{Stream: vv.Stream, State: meta.PendingBackfill}
	}
	it.write, it.stream, it.detail = true, vs.Stream, string(vs.State)
	if defined && vv.State == catalog.StateCurrent {
		it.refuse(d.region, "DF7302", fmt.Sprintf("the deployed catalog of %s makes %s current, so deployed queries read it", d.region, id),
			"deploy regional.yaml in "+d.region+" first, from a dynaflux generate that makes another version current")
	}
	view, _, _ := strings.Cut(id, "@v")
	if n := currentNumber(d.cat, view); n > 0 && vs.State != meta.Broken {
		cur := catalog.VersionID(view, n)
		if s := d.state(cur).State; cur != id && s != meta.Live {
			hint := "fill it first: dynaflux backfill " + cur + " -region " + d.region + prefix
			if s == meta.Broken {
				hint = "rebuild it first with dynaflux generate -rebuild " + view + ", then deploy and backfill the new version"
			}
			it.refuse(d.region, "DF7303", fmt.Sprintf("%s, the view's current version in the deployed catalog, is %s in %s", cur, s, d.region), hint)
		}
	}
	if d.runs(id) {
		it.refuse(d.region, "DF7304", fmt.Sprintf("a backfill of %s runs in %s", id, d.region),
			"cancel it with dynaflux backfill -cancel "+id+" -region "+d.region+prefix)
	}
}

func (it *retireItem) refuse(region, code, msg, hint string) {
	it.refusals = append(it.refusals, regionFinding(region, diag.SeverityError, code, msg, hint))
}

// action is what the plan line says retire does in the region.
func (it *retireItem) action() string {
	switch {
	case len(it.refusals) > 0:
		return string(actRefuse)
	case it.write:
		return retireName
	}
	return string(actNone)
}

// printRetirePlan prints what retire does in each region and in dynaflux.lock, whose entry
// of version id is entry, and then every refusal. The lock stays when a region is
// unchecked. It returns exitErrors when a check failed.
func (c *cli) printRetirePlan(f *fleet, id string, entry catalog.ViewVersion, items []*retireItem, unchecked []string) int {
	width := len(catalog.LockFileName)
	for _, r := range f.names {
		width = max(width, len(r))
	}
	fmt.Fprintf(&c.out, "%s:\n", id)
	for i, it := range items {
		if it != nil {
			fmt.Fprintf(&c.out, "  %-*s  %-7s  %s\n", width, f.names[i], it.action(), it.detail)
		}
	}
	act, detail := retireName, string(catalog.StateRetained)
	switch {
	case entry.State == catalog.StateRetired:
		act, detail = string(actNone), "already retired"
	case len(unchecked) > 0:
		act, detail = string(actNone), "retained until a run checks "+strings.Join(unchecked, ", ")
	}
	fmt.Fprintf(&c.out, "  %-*s  %-7s  %s\n", width, catalog.LockFileName, act, detail)
	status := exitOK
	for _, it := range items {
		if it == nil {
			continue
		}
		for _, r := range it.refusals {
			r.write(&c.errOut)
		}
		status = max(status, worst(it.refusals))
	}
	return status
}

// writeRetiredLock marks version number of view retired in lock, and writes it as
// dynaflux.lock.
func (c *cli) writeRetiredLock(root *os.Root, lock *catalog.Lock, view string, number int) int {
	err := lock.Retire(view, number)
	var data []byte
	if err == nil {
		data, err = catalog.MarshalLock(lock)
	}
	if err == nil {
		err = writeFile(root, output{path: catalog.LockFileName, data: data})
	}
	if err != nil {
		c.errorf("dynaflux retire: %s: %v", catalog.LockFileName, err)
		return exitUsage
	}
	return exitOK
}

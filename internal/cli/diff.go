// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/sql"
)

func diffCommand() command {
	return command{
		name:    "diff",
		args:    "[flags]",
		summary: "compare the project with the deployed store",
		doc: `Diff compiles the project as generate does and compares it with the catalog
deployed in each region, reading the regions at once: streams by the rules of
docs/spec/sql.md §8.9, view versions by number and fingerprint and against the
states in each region's meta table, named queries, and
runtime.prune_window_seconds. Regions whose deployed catalogs are equal print
once. It lists the changes, then, on stderr, those that would lose data or need an
action before the deploy. It writes nothing. -json prints one JSON document
instead, with "format": 1.

Exit status: 0 when no difference is an error (warnings are allowed), 1 when one
is or the project has an error, and 2 for usage, I/O and AWS access problems, and
when a region couldn't be read.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			o := storeFlags{}
			addStoreFlags(fs, &o)
			return func(c *cli, args []string) int { return c.diff(o, args) }
		},
	}
}

// change is a difference that diff lists.
type change struct {
	subject string // such as stream page_views
	text    string
}

func (c *cli) diff(o storeFlags, args []string) int {
	if len(args) > 0 {
		return c.usageError("diff", "unexpected arguments: "+strings.Join(args, " "))
	}
	if status := o.check(c, "diff"); status != exitOK {
		return status
	}
	p, ds, status := c.load()
	if status != exitOK {
		return c.diagnostics(ds, status)
	}
	data, err := catalog.Marshal(p.cat)
	var local *catalog.Catalog
	if err == nil {
		local, err = catalog.Unmarshal(data) // the form a deployed catalog has
	}
	if err != nil {
		c.errorf("dynaflux diff: %v", err)
		return exitErrors
	}
	sources, status := c.streamSources(p)
	if status != exitOK {
		return status
	}
	ctx, cancel := context.WithTimeout(c.ctx, o.timeout)
	defer cancel()
	f, status := c.openFleet(ctx, "diff", p.cfg, o.prefix, o.regions, o.endpoint)
	if status != exitOK {
		return status
	}
	cm := &comparison{local: local, hash: catalog.Hash(data), sources: sources, prefix: prefixFlag(o.prefix)}
	groups := cm.group(f.regions)
	if o.json {
		status = c.writeJSON(cm.json(f, groups))
	} else {
		cm.print(&c.out, f, groups)
		for _, fd := range cm.findings {
			fd.write(&c.errOut)
		}
	}
	for _, d := range f.regions {
		if d.err != nil {
			c.errorf("dynaflux diff: %s: %v", d.region, d.err)
			status = exitUsage
		}
	}
	return max(status, worst(cm.findings))
}

// streamSource is where the SQL files define a stream, its columns and its key clauses.
type streamSource struct {
	stmt    diag.Pos
	columns map[string]diag.Pos
	clauses map[catalog.StreamChangeKind]diag.Pos
}

// streamSources finds where the project's SQL files define each stream.
func (c *cli) streamSources(p *project) (map[string]streamSource, int) {
	root, status := c.projectRoot()
	if status != exitOK {
		return nil, status
	}
	defer root.Close()
	paths, _ := p.cfg.SQLFiles(root.FS(), root.Name()) // load reported their problems already
	sources := map[string]streamSource{}
	for _, path := range paths {
		src, err := root.ReadFile(path)
		if err != nil {
			c.errorf("dynaflux diff: %v", err)
			return nil, exitUsage
		}
		f, _ := sql.Parse(path, src)
		for _, s := range f.Stmts {
			if cs, ok := s.(*sql.CreateStream); ok {
				if _, seen := sources[cs.Name.Name]; !seen {
					sources[cs.Name.Name] = sourceOf(cs)
				}
			}
		}
	}
	return sources, exitOK
}

func sourceOf(cs *sql.CreateStream) streamSource {
	ss := streamSource{stmt: cs.Pos, columns: map[string]diag.Pos{}, clauses: make(map[catalog.StreamChangeKind]diag.Pos)}
	for _, col := range cs.Columns {
		ss.columns[col.Name.Name] = col.Pos
	}
	if cs.PrimaryKey != nil {
		ss.clauses[catalog.PrimaryKeyChanged] = cs.PrimaryKey.Pos
	}
	if cs.DedupBy != nil {
		ss.clauses[catalog.DedupByChanged] = cs.DedupBy.Pos
	}
	if cs.Shards != nil {
		ss.clauses[catalog.ShardsChanged] = cs.Shards.Pos
	}
	return ss
}

// comparison compares the local catalog with the deployed ones, and collects the findings.
type comparison struct {
	local    *catalog.Catalog
	hash     string
	sources  map[string]streamSource
	prefix   string // the -prefix flag for hints, or ""
	findings []finding
}

// diffGroup is the regions that deploy one catalog, and its changes.
type diffGroup struct {
	regions []string
	cat     *catalog.Catalog
	hash    string
	changes []change
}

// group compares each distinct deployed catalog once, then checks each region's states.
func (cm *comparison) group(rs []*deployment) []*diffGroup {
	var groups []*diffGroup
	for _, r := range rs {
		if fd, newer := formatFinding(r.region, r.reg); newer && r.err == nil {
			cm.add(fd)
		}
		if r.cat == nil || r.err != nil {
			continue
		}
		i := slices.IndexFunc(groups, func(g *diffGroup) bool { return g.hash == r.reg.Hash })
		if i < 0 {
			groups = append(groups, &diffGroup{cat: r.cat, hash: r.reg.Hash})
			i = len(groups) - 1
		}
		groups[i].regions = append(groups[i].regions, r.region)
	}
	for _, g := range groups {
		g.changes = cm.compare(g)
	}
	for _, r := range rs {
		if r.cat != nil && r.err == nil {
			cm.checkStates(r)
		}
	}
	return groups
}

func (cm *comparison) add(fd finding) { cm.findings = append(cm.findings, fd) }

// compare lists how the local catalog differs from g's, and adds the findings that depend
// on the catalogs alone.
func (cm *comparison) compare(g *diffGroup) []change {
	if g.hash == cm.hash {
		return nil
	}
	changes := cm.compareStreams(g)
	changes = append(changes, cm.compareVersions(g)...)
	changes = append(changes, compareQueries(g.cat, cm.local)...)
	dep, loc := g.cat, cm.local
	if !slices.Equal(dep.Regions, loc.Regions) {
		changes = append(changes, change{"regions", strings.Join(dep.Regions, ", ") + " → " + strings.Join(loc.Regions, ", ")})
	}
	if dep.PruneWindowSeconds != loc.PruneWindowSeconds {
		changes = append(changes, change{"runtime.prune_window_seconds", fmt.Sprintf("%d → %d", dep.PruneWindowSeconds, loc.PruneWindowSeconds)})
	}
	if loc.PruneWindowSeconds < dep.PruneWindowSeconds {
		cm.add(finding{regions: g.regions, severity: diag.SeverityWarning, code: "DF7113",
			message: fmt.Sprintf("runtime.prune_window_seconds is %d, lower than the %d deployed", loc.PruneWindowSeconds, dep.PruneWindowSeconds),
			hint: "lower runtime.timeout_seconds in one deploy, and prune_window_seconds in a later one once the old timeout has passed " +
				"(docs/spec/config.md)"})
	}
	return changes
}

// print lists the changes of each group of regions, at its first region.
func (cm *comparison) print(b *strings.Builder, f *fleet, groups []*diffGroup) {
	printed := map[*diffGroup]bool{}
	for _, r := range f.regions {
		switch {
		case r.err != nil:
			fmt.Fprintf(b, "%s: couldn't be read\n", r.region)
			continue
		case r.notDeployed:
			fmt.Fprintf(b, "%s: not deployed in %s; everything is new\n", r.region, f.naming.MetaTable())
			continue
		case r.cat == nil:
			fmt.Fprintf(b, "%s: catalog %s, in format %d\n", r.region, r.reg.Hash[:hashText], r.reg.Format)
			continue
		}
		g := groups[slices.IndexFunc(groups, func(g *diffGroup) bool { return g.hash == r.reg.Hash })]
		if printed[g] {
			continue
		}
		printed[g] = true
		fmt.Fprintf(b, "%s: catalog %s", strings.Join(g.regions, ", "), g.hash[:hashText])
		if g.hash == cm.hash {
			b.WriteString(", the same as the local one\n")
			continue
		}
		fmt.Fprintf(b, ", local %s\n", cm.hash[:hashText])
		for _, ch := range g.changes {
			fmt.Fprintf(b, "  %s: %s\n", ch.subject, ch.text)
		}
	}
}

// diffJSON is diff's -json document.
type diffJSON struct {
	Format       int              `json:"format"`
	Store        string           `json:"store"`
	Prefix       string           `json:"prefix"`
	Account      string           `json:"account,omitzero"`
	Endpoint     string           `json:"endpoint,omitzero"`
	LocalCatalog string           `json:"local_catalog"`
	Regions      []diffRegionJSON `json:"regions"`
	Findings     []findingJSON    `json:"findings"`
}

type diffRegionJSON struct {
	Region   string       `json:"region"`
	Error    string       `json:"error,omitzero"`
	Deployed bool         `json:"deployed"`
	Catalog  string       `json:"catalog,omitzero"`
	Changes  []changeJSON `json:"changes,omitzero"`
}

type changeJSON struct {
	Subject string `json:"subject"`
	Change  string `json:"change"`
}

func (cm *comparison) json(f *fleet, groups []*diffGroup) diffJSON {
	doc := diffJSON{Format: 1, Store: f.naming.Store, Prefix: f.naming.Prefix, Account: f.account, Endpoint: f.endpoint,
		LocalCatalog: cm.hash, Regions: []diffRegionJSON{}, Findings: []findingJSON{}}
	for _, r := range f.regions {
		j := diffRegionJSON{Region: r.region, Deployed: !r.notDeployed && r.err == nil, Catalog: r.reg.Hash}
		if r.err != nil {
			j.Error = r.err.Error()
		}
		if i := slices.IndexFunc(groups, func(g *diffGroup) bool { return slices.Contains(g.regions, r.region) }); i >= 0 {
			for _, ch := range groups[i].changes {
				j.Changes = append(j.Changes, changeJSON{ch.subject, ch.text})
			}
		}
		doc.Regions = append(doc.Regions, j)
	}
	for _, fd := range cm.findings {
		doc.Findings = append(doc.Findings, fd.json())
	}
	return doc
}

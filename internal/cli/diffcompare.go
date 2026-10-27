// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/meta"
)

// compareStreams lists how each stream differs, by sql.md §8.9, and adds the findings for
// the changes it rejects (DF7101–DF7107).
func (cm *comparison) compareStreams(g *diffGroup) []change {
	var names []string
	for _, s := range slices.Concat(cm.local.Streams, g.cat.Streams) {
		if !slices.Contains(names, s.Name) {
			names = append(names, s.Name)
		}
	}
	var changes []change
	for _, name := range names {
		dep, _ := g.cat.Stream(name)
		loc, _ := cm.local.Stream(name)
		for _, ch := range catalog.CompareStreams(dep, loc) {
			text := string(ch.Kind)
			switch {
			case ch.Column != "":
				text += ": " + ch.Column
			case ch.Region != "":
				text += " in " + ch.Region
			}
			changes = append(changes, change{"stream " + name, text})
			cm.streamFinding(g, name, dep, loc, ch)
		}
	}
	return changes
}

// streamFinding adds the finding of a stream change, when it has one.
func (cm *comparison) streamFinding(g *diffGroup, name string, dep, loc *catalog.Stream, ch catalog.StreamChange) {
	src := cm.sources[name]
	in := strings.Join(g.regions, ", ")
	at := func(pos diag.Pos, ok bool) *diag.Pos {
		if !ok {
			pos = src.stmt
		}
		return &pos
	}
	fd := finding{regions: g.regions, severity: diag.SeverityError, pos: at(src.columns[ch.Column], ch.Column != "")}
	switch ch.Kind {
	case catalog.ColumnRemoved:
		fd.pos, fd.code = &src.stmt, "DF7101"
		fd.message = fmt.Sprintf("stream %s: column %s, deployed in %s, was removed or renamed", name, ch.Column, in)
		fd.hint = "put it back: a deployed stream only gains columns (docs/spec/sql.md §8.9)"
	case catalog.ColumnRetyped:
		d, _ := dep.Column(ch.Column)
		l, _ := loc.Column(ch.Column)
		fd.code = "DF7102"
		fd.message = fmt.Sprintf("stream %s: column %s is %s, and %s deployed in %s", name, ch.Column, l.Type, d.Type, in)
		fd.hint = "keep its type, and add a new column with the other one"
	case catalog.DefaultChanged:
		fd.code = "DF7103"
		fd.message = fmt.Sprintf("stream %s: the DEFAULT of column %s differs from the one deployed in %s", name, ch.Column, in)
		fd.hint = "keep the deployed DEFAULT: the events stored before the column existed read as it"
	case catalog.ColumnRequired:
		fd.code = "DF7105"
		fd.message = fmt.Sprintf("stream %s: new column %s is neither Nullable nor has a DEFAULT, so the events deployed in %s have no value for it",
			name, ch.Column, in)
		fd.hint = "make it Nullable, or give it a DEFAULT"
	case catalog.PrimaryKeyChanged, catalog.DedupByChanged, catalog.ShardsChanged:
		pos, ok := src.clauses[ch.Kind]
		fd.pos, fd.code = at(pos, ok), "DF7104"
		fd.message = fmt.Sprintf("stream %s: %s differs from the one deployed in %s", name, strings.ToUpper(strings.TrimSuffix(string(ch.Kind), " changed")), in)
		fd.hint = "keep it: a stream with another key is a new stream"
	case catalog.StreamDropped:
		fd = finding{regions: g.regions, severity: diag.SeverityWarning, code: "DF7106",
			message: "stream " + name + " is dropped; its table and archive stay",
			hint:    "stop writing to it, and deploy once the projector's lag and the archive's freshness have drained (docs/spec/infra.md §12)"}
	case catalog.ArchiveStopped:
		if !slices.Contains(g.regions, ch.Region) {
			return // another group's catalog judges it
		}
		fd = finding{regions: []string{ch.Region}, severity: diag.SeverityWarning, code: "DF7107",
			message: "stream " + name + " stops being archived with events: all; Firehose may drop one buffer, and the region's archive misses those events for good",
			hint:    "keep archive.events: all in " + ch.Region + ", or accept the hole (docs/spec/infra.md §12)"}
	case catalog.StreamAdded, catalog.ColumnAdded, catalog.LogRetentionChanged, catalog.AcceptChanged, catalog.GoTypeChanged,
		catalog.ArchiveChanged:
		return
	}
	cm.add(fd)
}

// compareVersions lists how the view versions differ, and adds DF7110 and DF7111.
func (cm *comparison) compareVersions(g *diffGroup) []change {
	dep, loc := g.cat, cm.local
	in := strings.Join(g.regions, ", ")
	var changes []change
	var views []string
	for _, v := range slices.Concat(loc.Views, dep.Views) {
		if !slices.Contains(views, v.Name) {
			views = append(views, v.Name)
		}
	}
	slices.Sort(views)
	for _, name := range views {
		if d, l := currentNumber(dep, name), currentNumber(loc, name); d != l {
			changes = append(changes, change{"view " + name, "current " + numberText(d) + " → " + numberText(l)})
		}
	}
	for id, vv := range dep.Versions() {
		lv, ok := loc.Version(id)
		switch {
		case ok && lv.Fingerprint != vv.Fingerprint:
			cm.add(finding{regions: g.regions, severity: diag.SeverityError, code: "DF7111",
				message: fmt.Sprintf("%s has another definition in dynaflux.lock than deployed in %s", id, in),
				hint:    "dynaflux.lock and the deployed catalog disagree, as after a reverted lock or two branches deployed: restore dynaflux.lock from the deployed build, then run dynaflux generate"})
		case ok && lv.Mode != vv.Mode:
			changes = append(changes, change{id, "MODE " + strings.ToUpper(string(vv.Mode)) + " → " + strings.ToUpper(string(lv.Mode))})
		case ok:
		case slices.Contains(loc.Retired, id):
			changes = append(changes, change{id, "retired; the deploy deletes its table"})
		default:
			cm.add(finding{regions: g.regions, severity: diag.SeverityError, code: "DF7110",
				message: fmt.Sprintf("%s is deployed in %s, and dynaflux.lock neither holds nor retires it; the deploy drops its table", id, in),
				hint:    "restore its entry in dynaflux.lock; to drop the version, then retire it with dynaflux retire " + id})
		}
	}
	for id := range loc.Versions() {
		if _, ok := dep.Version(id); !ok {
			changes = append(changes, change{id, "new; backfill it after the deploy"})
		}
	}
	return changes
}

// currentNumber returns the number of view's current version in c, or 0 without one.
func currentNumber(c *catalog.Catalog, view string) int {
	v, ok := c.View(view)
	if !ok {
		return 0
	}
	if cur, current := v.Current(); current {
		return cur.Number
	}
	return 0
}

func numberText(n int) string {
	if n == 0 {
		return "none"
	}
	return "v" + strconv.Itoa(n)
}

// compareQueries lists the named queries that are new, changed or removed.
func compareQueries(dep, loc *catalog.Catalog) []change {
	var names []string
	for _, q := range slices.Concat(loc.Queries, dep.Queries) {
		if !slices.Contains(names, q.Name) {
			names = append(names, q.Name)
		}
	}
	slices.Sort(names)
	find := func(c *catalog.Catalog, name string) *catalog.Query {
		if i := slices.IndexFunc(c.Queries, func(q catalog.Query) bool { return q.Name == name }); i >= 0 {
			return &c.Queries[i]
		}
		return nil
	}
	var changes []change
	for _, name := range names {
		d, l := find(dep, name), find(loc, name)
		switch {
		case d == nil:
			changes = append(changes, change{"query " + name, "new"})
		case l == nil:
			changes = append(changes, change{"query " + name, "removed"})
		case !reflect.DeepEqual(d, l):
			changes = append(changes, change{"query " + name, "changed"})
		}
	}
	return changes
}

// checkStates adds the findings that depend on r's view states (DF7112, DF7114, DF7115).
func (cm *comparison) checkStates(r *deployment) {
	for _, v := range cm.local.Views {
		cm.checkCurrent(r, &v)
		cm.checkKept(r, &v)
	}
	for _, id := range cm.local.Retired {
		cm.checkRetiring(r, id)
	}
}

// checkCurrent adds DF7112 when v's current version changes and isn't live in r.
func (cm *comparison) checkCurrent(r *deployment, v *catalog.View) {
	cur, ok := v.Current()
	if !ok || currentNumber(r.cat, v.Name) == cur.Number {
		return
	}
	id := catalog.VersionID(v.Name, cur.Number)
	if s := r.state(id).State; s != meta.Live {
		cm.add(regionFinding(r.region, diag.SeverityWarning, "DF7112",
			fmt.Sprintf("%s becomes the current version of %s, but it is %s in %s; its queries return ErrViewNotReady until it is live",
				id, v.Name, s, r.region),
			"deploy regional.yaml, run dynaflux backfill "+id+" -region "+r.region+cm.prefix+", and deploy the application once it is live"))
	}
}

// checkKept adds DF7114 for each version of v that the lock keeps and r has retired.
func (cm *comparison) checkKept(r *deployment, v *catalog.View) {
	for _, vv := range v.Versions {
		id := catalog.VersionID(v.Name, vv.Number)
		if r.state(id).State != meta.Retired {
			continue
		}
		hint := "mark it retired in dynaflux.lock with dynaflux retire " + id
		if vv.State == catalog.StateCurrent {
			hint = "give the view a new version, by changing it or with dynaflux generate -rebuild " + v.Name + ", then dynaflux retire " + id
		}
		cm.add(regionFinding(r.region, diag.SeverityError, "DF7114", fmt.Sprintf("dynaflux.lock keeps %s, which is retired in %s", id, r.region), hint))
	}
}

// checkRetiring adds DF7115 when the lock retires version id, which r's catalog defines,
// without the checks of retire: its backfill runs, or the view's current version isn't live.
func (cm *comparison) checkRetiring(r *deployment, id string) {
	if _, defined := r.cat.Version(id); !defined || r.state(id).State == meta.Retired {
		return
	}
	view, _, _ := strings.Cut(id, "@")
	cur := currentNumber(cm.local, view)
	curID := catalog.VersionID(view, cur)
	var why string
	switch {
	case r.runs(id):
		why = "its backfill is running"
	case cur > 0 && r.state(curID).State != meta.Live && r.state(id).State != meta.Broken:
		why = fmt.Sprintf("%s, the view's current version, is %s there", curID, r.state(curID).State)
	default:
		return
	}
	cm.add(regionFinding(r.region, diag.SeverityError, "DF7115",
		fmt.Sprintf("dynaflux.lock retires %s, which the deploy retires in %s without the checks of dynaflux retire, and %s", id, r.region, why),
		"run dynaflux retire "+id+cm.prefix+" for this environment before the deploy"))
}

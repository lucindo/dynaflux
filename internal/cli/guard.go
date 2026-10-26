// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/infra/cloudformation"
)

// checkPrevious compares templates, the new ones, with those that generate wrote last time
// in infra.out, and refuses changes that would lose data or fail when deployed: a view
// version's table that disappears or holds another definition (DF6016), a change of more
// than one replica region (DF6017), and the removal of a region whose replicas are still
// protected (DF6018). A template that isn't generated is left to write's check.
func (c *cli) checkPrevious(root *os.Root, p *project, templates []output) int {
	var old, current [2][]byte
	for i, name := range []string{cloudformation.GlobalTemplate, cloudformation.RegionalTemplate} {
		file := path.Join(p.cfg.Infra.Out, name)
		if j := slices.IndexFunc(templates, func(o output) bool { return o.path == file }); j >= 0 {
			current[i] = templates[j].data
		}
		data, err := root.ReadFile(file)
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			c.errorf("dynaflux generate: %v", err)
			return exitUsage
		case isGeneratedTemplate(data):
			old[i] = data
		}
	}
	before, err := cloudformation.ReadInventory(old[0], old[1])
	if err != nil {
		c.errorf("dynaflux generate: can't read the templates in %s that it wrote last time: %v; restore them, or delete them to start over",
			p.cfg.Infra.Out, err)
		return exitErrors
	}
	after, err := cloudformation.ReadInventory(current[0], current[1])
	if err != nil {
		c.errorf("dynaflux generate: %v", err)
		return exitErrors
	}
	ds := droppedVersions(before.Views, after.Views)
	// Without a global.yaml from last time, every region is new.
	if len(before.Regions) > 0 {
		ds = append(ds, changedRegions(before, after.Regions, p.cfg.RegionsPos)...)
	}
	if len(ds) == 0 {
		return exitOK
	}
	diag.Sort(ds)
	return c.diagnostics(ds, exitErrors)
}

// droppedVersions reports each view version whose table the last regional.yaml has and the
// new one lacks or gives another fingerprint. Only a lost, reverted or badly merged
// dynaflux.lock does that, and deploying would delete the table or write rows of another
// definition into it.
func droppedVersions(before, after []cloudformation.ViewTable) []diag.Diagnostic {
	const hint = "restore dynaflux.lock from version control and run dynaflux generate again; " +
		"to start over on purpose, delete the old templates first"
	var ds []diag.Diagnostic
	for _, v := range before {
		id := catalog.VersionID(v.View, v.Version)
		i := slices.IndexFunc(after, func(w cloudformation.ViewTable) bool { return w.View == v.View && w.Version == v.Version })
		var msg string
		switch {
		case i < 0:
			msg = fmt.Sprintf("%s has a table in the regional.yaml that generate wrote last time, but dynaflux.lock no longer "+
				"holds the version; deploying would delete the table", id)
		case after[i].Fingerprint != v.Fingerprint:
			msg = fmt.Sprintf("%s has another definition in dynaflux.lock than in the regional.yaml that generate wrote last "+
				"time; the projector would write rows of the new definition into the old version's table", id)
		default:
			continue
		}
		ds = append(ds, diag.Diagnostic{
			Pos: diag.Pos{File: catalog.LockFileName, Line: 1, Col: 1}, Severity: diag.SeverityError, Code: "DF6016",
			Message: msg, Hint: hint,
		})
	}
	return ds
}

// changedRegions reports regions that differ from the replica regions of the last
// global.yaml by more than one region, since a stack update adds or removes one replica,
// and a removed region whose replicas the last global.yaml protects from deletion.
func changedRegions(before cloudformation.Inventory, after []string, pos diag.Pos) []diag.Diagnostic {
	missing := func(from, in []string) []string {
		var out []string
		for _, r := range from {
			if !slices.Contains(in, r) {
				out = append(out, r)
			}
		}
		slices.Sort(out)
		return out
	}
	added, removed := missing(after, before.Regions), missing(before.Regions, after)
	if len(removed) == 1 && len(added) == 0 && slices.Contains(before.Protected, removed[0]) {
		return []diag.Diagnostic{{
			Pos: pos, Severity: diag.SeverityError, Code: "DF6018",
			Message: fmt.Sprintf("regions removes %s, whose stream table replicas have deletion protection in the global.yaml "+
				"that generate wrote last time", removed[0]),
			Hint: fmt.Sprintf("first run dynaflux generate -unprotect-region %s and deploy global.yaml, then remove the region",
				removed[0]),
		}}
	}
	if len(added)+len(removed) <= 1 {
		return nil
	}
	var changes []string
	if len(added) > 0 {
		changes = append(changes, "adds "+strings.Join(added, ", "))
	}
	if len(removed) > 0 {
		changes = append(changes, "removes "+strings.Join(removed, ", "))
	}
	return []diag.Diagnostic{{
		Pos: pos, Severity: diag.SeverityError, Code: "DF6017",
		Message: fmt.Sprintf("regions %s, compared with the global.yaml that generate wrote last time; "+
			"CloudFormation adds or removes one Global Table replica per stack update", strings.Join(changes, " and ")),
		Hint: "change one region, run dynaflux generate and deploy global.yaml, then change the next",
	}}
}

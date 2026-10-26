// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// generate refuses templates that would delete a view version's table or change its
// definition, or change more than one replica region, and then writes nothing.
func TestGenerateGuards(t *testing.T) {
	t.Parallel()
	const dropped = "dynaflux.lock:1:1: error DF6016: page_daily@v1 has a table in the regional.yaml that generate wrote last time, " +
		"but dynaflux.lock no longer holds the version; deploying would delete the table\n" +
		"  hint: restore dynaflux.lock from version control and run dynaflux generate again; " +
		"to start over on purpose, delete the old templates first\n"
	tests := []struct {
		name    string
		project string
		change  func(t *testing.T, dir string)
		stderr  string
	}{
		{"retained version dropped", "generate/changed", dropRetained, dropped},
		{
			"lock lost", "generate/changed", func(t *testing.T, dir string) {
				t.Helper()
				if err := os.Remove(filepath.Join(dir, "dynaflux.lock")); err != nil {
					t.Fatal(err)
				}
			},
			"dynaflux.lock:1:1: error DF6016: page_daily@v1 has another definition in dynaflux.lock than in the regional.yaml " +
				"that generate wrote last time; the projector would write rows of the new definition into the old version's table\n" +
				"  hint: restore dynaflux.lock from version control and run dynaflux generate again; " +
				"to start over on purpose, delete the old templates first\n" +
				"dynaflux.lock:1:1: error DF6016: page_daily@v2 has a table in the regional.yaml that generate wrote last time, " +
				"but dynaflux.lock no longer holds the version; deploying would delete the table\n" +
				"  hint: restore dynaflux.lock from version control and run dynaflux generate again; " +
				"to start over on purpose, delete the old templates first\n",
		},
		{
			"two regions added", "generate/new", setRegions("[us-east-1, us-west-2, eu-west-1, ap-south-1]"),
			"dynaflux.yaml:2:10: error DF6017: regions adds ap-south-1, eu-west-1, compared with the global.yaml that generate " +
				"wrote last time; CloudFormation adds or removes one Global Table replica per stack update\n" +
				"  hint: change one region, run dynaflux generate and deploy global.yaml, then change the next\n",
		},
		{
			"one region added and one removed", "generate/new", setRegions("[us-east-1, eu-west-1]"),
			"dynaflux.yaml:2:10: error DF6017: regions adds eu-west-1 and removes us-west-2, compared with the global.yaml " +
				"that generate wrote last time; CloudFormation adds or removes one Global Table replica per stack update\n" +
				"  hint: change one region, run dynaflux generate and deploy global.yaml, then change the next\n",
		},
		{
			"protected region removed", "generate/new", setRegions("[us-east-1]"),
			"dynaflux.yaml:2:10: error DF6018: regions removes us-west-2, whose stream table replicas have deletion protection " +
				"in the global.yaml that generate wrote last time\n" +
				"  hint: first run dynaflux generate -unprotect-region us-west-2 and deploy global.yaml, then remove the region\n",
		},
		{
			"unreadable template", "generate/new", func(t *testing.T, dir string) {
				t.Helper()
				p := filepath.Join(dir, "infra/dynaflux/regional.yaml")
				if err := os.WriteFile(p, []byte(yamlHeader+"Resources: [\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			"dynaflux generate: can't read the templates in infra/dynaflux that it wrote last time: regional.yaml: ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := writeProject(t, tt.project)
			if got := run(dir, "generate"); got.status != 0 {
				t.Fatalf("first run: %+v", got)
			}
			tt.change(t, dir)
			before := snapshot(t, dir)
			got := run(dir, "generate")
			if got.status != 1 || got.stdout != "" || !strings.HasPrefix(got.stderr, tt.stderr) {
				t.Errorf("got  %+v\nwant status 1 and stderr starting with\n%s", got, tt.stderr)
			}
			if after := snapshot(t, dir); !maps.EqualFunc(before, after, slices.Equal) {
				t.Error("a refused generate changed files")
			}
		})
	}
}

// A region at a time passes the guard.
func TestGenerateAddsOneRegion(t *testing.T) {
	t.Parallel()
	dir := writeProject(t, "generate/new")
	if got := run(dir, "generate"); got.status != 0 {
		t.Fatalf("first run: %+v", got)
	}
	setRegions("[us-east-1, us-west-2, eu-west-1]")(t, dir)
	if got := run(dir, "generate"); got != (result{0, "", ""}) {
		t.Errorf("got %+v", got)
	}
}

// A region whose replicas a run with -unprotect-region left unprotected can go.
func TestGenerateRemovesUnprotectedRegion(t *testing.T) {
	t.Parallel()
	dir := writeProject(t, "generate/new")
	if got := run(dir, "generate", "-unprotect-region", "us-west-2"); got.status != 0 {
		t.Fatalf("first run: %+v", got)
	}
	setRegions("[us-east-1]")(t, dir)
	if got := run(dir, "generate"); got != (result{0, "", ""}) {
		t.Errorf("got %+v", got)
	}
}

// -unprotect-region turns off deletion protection on one region's replicas for one run.
func TestGenerateUnprotectRegion(t *testing.T) {
	t.Parallel()
	dir := writeProject(t, "generate/new")
	protection := func() string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(dir, "infra/dynaflux/global.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var b strings.Builder
		for line := range strings.Lines(string(data)) {
			if strings.Contains(line, "- Region:") || strings.Contains(line, "DeletionProtectionEnabled") {
				b.WriteString(strings.TrimSpace(line) + "\n")
			}
		}
		return b.String()
	}
	if got := run(dir, "generate", "-unprotect-region", "us-west-2"); got.status != 0 {
		t.Fatalf("got %+v", got)
	}
	const unprotected = "- Region: us-east-1\nDeletionProtectionEnabled: true\n- Region: us-west-2\nDeletionProtectionEnabled: false\n"
	if got := protection(); got != unprotected {
		t.Errorf("with -unprotect-region us-west-2:\n%s\nwant:\n%s", got, unprotected)
	}
	if got := run(dir, "generate"); got.status != 0 {
		t.Fatalf("got %+v", got)
	}
	const protected = "- Region: us-east-1\nDeletionProtectionEnabled: true\n- Region: us-west-2\nDeletionProtectionEnabled: true\n"
	if got := protection(); got != protected {
		t.Errorf("without the flag:\n%s\nwant:\n%s", got, protected)
	}
	got := run(dir, "generate", "-unprotect-region", "eu-west-1")
	const want = "dynaflux generate: -unprotect-region eu-west-1 isn't in regions (us-east-1, us-west-2)\n"
	if got.status != 2 || !strings.HasPrefix(got.stderr, want) {
		t.Errorf("a region outside regions: got %+v, want status 2 and stderr starting with %q", got, want)
	}
}

// dropRetained removes the retained version 1 of page_daily from dynaflux.lock, as a bad
// merge could.
func dropRetained(t *testing.T, dir string) {
	t.Helper()
	p := filepath.Join(dir, "dynaflux.lock")
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := catalog.ParseLock(data)
	if err != nil {
		t.Fatal(err)
	}
	lock.Views["page_daily"] = slices.DeleteFunc(lock.Views["page_daily"], func(v catalog.ViewVersion) bool { return v.Number == 1 })
	if data, err = catalog.MarshalLock(lock); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func setRegions(regions string) func(t *testing.T, dir string) {
	return func(t *testing.T, dir string) {
		t.Helper()
		config := "store: analytics\nregions: " + regions + "\n"
		if err := os.WriteFile(filepath.Join(dir, "dynaflux.yaml"), []byte(config), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain_test

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"text/template"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/explain"
	"github.com/lucindo/dynaflux/internal/txtar"
	"github.com/lucindo/dynaflux/internal/validate"
)

const (
	// costsProjectPath is a project template with costs.py's three views.
	costsProjectPath = "testdata/costs_project.txtar"
	projectRoot      = "/project"
)

// costsProject is the data of the costs_project.txtar template.
type costsProject struct {
	Regions, ArchiveRegions string
	RetentionDays, Split    int
	Window, PruneWindow     int
	Provisioned             bool
}

// project expresses a costs.py scenario in dynaflux.yaml and SQL, and its traffic in a
// workload.
func project(t *testing.T, in inputs) (costsProject, explain.Workload) {
	t.Helper()
	if in.Compression != 6 || in.WindowS != float64(int(in.WindowS)) || in.LifetimeS != float64(int(in.LifetimeS)) {
		t.Fatalf("inputs %+v: the project can't express them", in)
	}
	regions := []string{"us-east-1", "us-west-2", "ap-northeast-1"}[:in.Regions]
	p := costsProject{
		Regions:        strings.Join(regions, ", "),
		ArchiveRegions: "all",
		RetentionDays:  in.RetentionDays,
		Split:          in.RowSplit,
		Window:         int(in.WindowS),
		PruneWindow:    int(in.LifetimeS),
		Provisioned:    in.Provisioned,
	}
	if in.ArchiveRegions != nil {
		p.ArchiveRegions = "[" + strings.Join(regions[:*in.ArchiveRegions], ", ") + "]"
	}
	if in.Prune == "25h" {
		p.PruneWindow = 25 * 3600
	}
	w := explain.DefaultWorkload()
	w.Rate, w.EventKB = in.EventsS, in.EventKB
	w.Rows = map[string]float64{"page_daily": in.HotRows}
	w.UniqNew = map[string]float64{"user_daily": in.UniqNewFraction}
	return p, w
}

// load compiles the project files of an archive, leaving out the sections named
// "explain …".
func load(t *testing.T, archive []byte) (*config.Config, *catalog.Catalog) {
	t.Helper()
	fsys := fstest.MapFS{}
	for _, f := range txtar.Parse(archive).Files {
		if !strings.HasPrefix(f.Name, "explain ") {
			fsys[f.Name] = &fstest.MapFile{Data: f.Data}
		}
	}
	cfg, cat, ds := validate.Load(fsys, projectRoot)
	if cfg == nil {
		t.Fatalf("the project doesn't compile:\n%v", ds)
	}
	return cfg, cat
}

// TestInputsMatchCostsPy builds a project for every costs.py scenario and checks the
// monthly costs of its stream against costs.py's. A view's row size is the one input that
// the project doesn't set exactly: costs.py takes 0.3 KB for every view.
func TestInputsMatchCostsPy(t *testing.T) {
	t.Parallel()
	text, err := os.ReadFile(costsProjectPath)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := template.Must(template.New(costsProjectPath).Option("missingkey=error").Parse(string(text)))
	for _, sc := range loadReference(t) {
		t.Run(sc.Name, func(t *testing.T) {
			t.Parallel()
			p, w := project(t, sc.Inputs)
			var archive bytes.Buffer
			if execErr := tmpl.Execute(&archive, p); execErr != nil {
				t.Fatal(execErr)
			}
			cfg, cat := load(t, archive.Bytes())
			store, stream, inErr := explain.Inputs(cfg, cat, w, "page_views")
			if inErr != nil {
				t.Fatal(inErr)
			}
			store.HWMEntryKB = 0.029 // costs.py's mark; Dynaflux's binary marks are smaller
			checkBaseSizes(t, stream.Views)
			got := estimate(t, store, stream)
			lines := map[string]float64{
				"DynamoDB requests":     got.Monthly.Requests,
				"DynamoDB storage":      got.Monthly.Storage,
				"Lambda (projector)":    got.Monthly.Lambda,
				"Firehose":              got.Monthly.Firehose,
				"S3 archive (yr-1 avg)": got.Monthly.S3,
				"Replication transfer":  got.Monthly.Transfer,
			}
			nearAll(t, lines, sc.PerRegion, 0.01)
			near(t, "total", got.Total, sc.Total, 0.01)
		})
	}
}

// checkBaseSizes checks that the estimated row sizes bill as costs.py's 0.3 KB do in every
// scenario. Past these bounds, a row's write units in some scenario change, and so do its
// costs, by more than 1%.
func checkBaseSizes(t *testing.T, views []explain.View) {
	t.Helper()
	bounds := map[string]float64{"page_daily": 0.371, "user_latest": 0.971, "user_daily": 0.971}
	if len(views) != len(bounds) {
		t.Fatalf("%d views, want %d", len(views), len(bounds))
	}
	for _, v := range views {
		if !(v.BaseKB > 0 && v.BaseKB <= bounds[v.Name]) {
			t.Errorf("%s: base row size %v KB, want over 0 and at most %v", v.Name, v.BaseKB, bounds[v.Name])
		}
	}
}

func TestInputsClassifyViews(t *testing.T) {
	t.Parallel()
	cfg, cat := load(t, []byte(`-- dynaflux.yaml --
store: analytics
regions: [us-east-1]
-- dynaflux/s.sql --
CREATE STREAM clicks (tenant String, user_id String, page String)
PRIMARY KEY (tenant, user_id);

-- Leaves out user_id.
CREATE MATERIALIZED VIEW by_tenant ORDER BY (tenant)
AS SELECT tenant, count() AS n FROM clicks GROUP BY tenant;

-- Holds the whole key under other keys.
CREATE MATERIALIZED VIEW by_user_page ORDER BY (page, tenant, user_id)
AS SELECT page, tenant, user_id, count() AS n FROM clicks GROUP BY page, tenant, user_id;

-- Holds an expression of user_id, not user_id.
CREATE MATERIALIZED VIEW by_lower_user ORDER BY (tenant, u)
AS SELECT tenant, lower(user_id) AS u, count() AS n FROM clicks GROUP BY tenant, u;
`))
	_, stream, err := explain.Inputs(cfg, cat, explain.DefaultWorkload(), "clicks")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, v := range stream.Views {
		got[v.Name] = v.Shared
	}
	want := map[string]bool{"by_tenant": true, "by_user_page": false, "by_lower_user": true}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("shared views %v, want %v", got, want)
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"errors"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// archivedCatalog is the fixture's store in us-east-1 and us-west-2, with archive.
func archivedCatalog(t *testing.T, archive string) *catalog.Catalog {
	t.Helper()
	cat, err := catalog.Unmarshal(catalogtest.MarshalFS(t, fstest.MapFS{
		"dynaflux.yaml": {Data: []byte("store: shop\nregions: [us-east-1, us-west-2]\n" +
			"archive: " + archive + "\n")},
		"dynaflux/store.sql": {Data: []byte(storeSQL)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

// A new job reads the archive whenever the region's archive of the stream is complete, unless
// the table is asked for. Otherwise it reads the table, split by time for an additive version
// and not at all for an idempotent one; asking for the archive names the rule that failed.
func TestChoose(t *testing.T) {
	t.Parallel()
	cat := archivedCatalog(t, "{enabled: true, regions: [us-east-1], events: all}")
	complete := backfill.Verdict{Complete: true}
	hole := backfill.Verdict{Rule: backfill.RuleHole, Reason: "a deploy changed its archive mode from all to off at 2000-01-01T00:00:00Z"}
	tests := []struct {
		name    string
		version string
		verdict backfill.Verdict
		want    backfill.Source
		split   meta.Split
		err     string
	}{
		{"complete", totals, complete, "", meta.SplitFence, ""},
		{"complete, idempotent", peaks, complete, "", meta.SplitFence, ""},
		{"complete, archive asked", totals, complete, backfill.SourceArchive, meta.SplitFence, ""},
		{"complete, table asked", totals, complete, backfill.SourceTable, meta.SplitTime, ""},
		{"complete, idempotent, table asked", peaks, complete, backfill.SourceTable, meta.SplitNone, ""},
		{"incomplete", totals, hole, "", meta.SplitTime, ""},
		{"incomplete, idempotent", peaks, hole, "", meta.SplitNone, ""},
		{"incomplete, markers", kinds, hole, "", meta.SplitTime, ""},
		{
			"incomplete, archive asked", totals, hole, backfill.SourceArchive, "",
			"backfill: the archive isn't complete: events in us-east-1: rule 2: a deploy changed its archive mode from all to off at 2000-01-01T00:00:00Z",
		},
		{"unknown version", "totals@v2", complete, "", "", "backfill: totals@v2 isn't in the catalog"},
	}
	for _, tt := range tests {
		split, err := backfill.Choose(cat, "us-east-1", tt.version, tt.verdict, tt.want)
		var got string
		if err != nil {
			got = err.Error()
		}
		if split != tt.split || got != tt.err || (tt.want == backfill.SourceArchive && tt.err != "") != errors.Is(err, backfill.ErrIncomplete) {
			t.Errorf("%s: Choose = %q, %q; want %q, %q", tt.name, split, got, tt.split, tt.err)
		}
	}
}

// Cancel stops a running job and keeps a cancelled one; SetRate changes the rate of either.
// Neither changes a job that ended.
func TestCancelAndSetRate(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m := meta.New(mem.New(), "meta")
	start := func(version string) {
		t.Helper()
		if _, err := backfill.Start(ctx, m, backfill.Request{Version: version, Stream: stream, Name: version, Execution: backfill.LocalExecution, Split: meta.SplitNone}); err != nil {
			t.Fatal(err)
		}
	}
	start(peaks)
	if err := backfill.SetRate(ctx, m, peaks, 500); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		j, err := backfill.Cancel(ctx, m, peaks)
		if err != nil || j.State != meta.JobCancelled || j.Rate != 500 || j.Attempt != 1 {
			t.Errorf("Cancel = %+v, %v; want attempt 1 cancelled at rate 500", j, err)
		}
	}
	if err := backfill.SetRate(ctx, m, peaks, 0); err != nil {
		t.Errorf("SetRate on a cancelled job: %v", err)
	}
	if j, _, err := m.Job(ctx, peaks); err != nil || j.Rate != 0 {
		t.Errorf("rate %d, %v; want 0", j.Rate, err)
	}
	if _, err := backfill.Cancel(ctx, m, totals); !errors.Is(err, meta.ErrNoJob) {
		t.Errorf("Cancel without a job: %v, want ErrNoJob", err)
	}
	start(totals)
	if _, err := m.UpdateJob(ctx, totals, func(j *meta.Job) (bool, error) {
		j.State = meta.JobDone
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := backfill.Cancel(ctx, m, totals); !errors.Is(err, backfill.ErrJobEnded) {
		t.Errorf("Cancel of a done job: %v, want ErrJobEnded", err)
	}
	if err := backfill.SetRate(ctx, m, totals, 10); !errors.Is(err, backfill.ErrJobEnded) {
		t.Errorf("SetRate of a done job: %v, want ErrJobEnded", err)
	}
	if err := backfill.SetRate(ctx, m, peaks, -1); err == nil {
		t.Error("a negative rate: no error")
	}
}

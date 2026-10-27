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

// A new job of an additive version reads the archive only where the fresh rule will most
// likely make it live without a read: a fresh stream that the region archives with events:
// all. Otherwise it reads the table, split by time for an additive version and not at all
// for an idempotent one, which reads the archive only when asked; asking for the archive
// where it can't serve says which condition failed.
func TestChoose(t *testing.T) {
	t.Parallel()
	all := archivedCatalog(t, "{enabled: true, regions: [us-east-1], events: all}")
	local := archivedCatalog(t, "{enabled: true, events: local}")
	tests := []struct {
		name    string
		cat     *catalog.Catalog
		region  string
		version string
		fresh   bool
		want    backfill.Source
		split   meta.Split
		err     string
	}{
		{"fresh, archived", all, "us-east-1", totals, true, "", meta.SplitFence, ""},
		{"fresh, archived, idempotent", all, "us-east-1", peaks, true, "", meta.SplitNone, ""},
		{"fresh, archived, idempotent, archive asked", all, "us-east-1", peaks, true, backfill.SourceArchive, meta.SplitFence, ""},
		{"fresh, archived, archive asked", all, "us-east-1", totals, true, backfill.SourceArchive, meta.SplitFence, ""},
		{"fresh, archived, table asked", all, "us-east-1", totals, true, backfill.SourceTable, meta.SplitTime, ""},
		{"not fresh", all, "us-east-1", totals, false, "", meta.SplitTime, ""},
		{"not fresh, idempotent", all, "us-east-1", peaks, false, "", meta.SplitNone, ""},
		{"not fresh, markers", all, "us-east-1", kinds, false, "", meta.SplitTime, ""},
		{"not archived here", all, "us-west-2", totals, true, "", meta.SplitTime, ""},
		{"local events", local, "us-east-1", peaks, true, "", meta.SplitNone, ""},
		{
			"archive asked, not fresh", all, "us-east-1", totals, false, backfill.SourceArchive, "",
			"backfill: the archive source isn't available: stream events isn't fresh in us-east-1, " +
				"and this release reads the archive only of a stream without events",
		},
		{
			"archive asked, not archived here", all, "us-west-2", totals, true, backfill.SourceArchive, "",
			"backfill: the archive source isn't available: us-west-2 doesn't archive stream events with events: all",
		},
		{
			"archive asked, local events", local, "us-east-1", totals, true, backfill.SourceArchive, "",
			"backfill: the archive source isn't available: us-east-1 doesn't archive stream events with events: all",
		},
		{"unknown version", all, "us-east-1", "totals@v2", true, "", "", "backfill: totals@v2 isn't in the catalog"},
	}
	for _, tt := range tests {
		split, err := backfill.Choose(tt.cat, tt.region, tt.version, tt.fresh, tt.want)
		var got string
		if err != nil {
			got = err.Error()
		}
		if split != tt.split || got != tt.err || (tt.want == backfill.SourceArchive && tt.err != "") != errors.Is(err, backfill.ErrArchiveSource) {
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

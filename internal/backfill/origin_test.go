// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"context"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/meta"
)

// newOriginFixture returns a fixture whose stream table an hour old holds no event, whose
// meta table has the stream item of a first deploy and no origin, and whose catalog, which
// archives every event of the stream, was registered now.
func newOriginFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	f.m = meta.New(f.b, "origin-meta")
	f.env.Meta = f.m
	f.tables.born = time.Now().Add(-time.Hour)
	if _, err := f.m.RegisterCatalog(t.Context(), catalogtest.Marshal(t, storeName, storeSQL), "test"); err != nil {
		t.Fatal(err)
	}
	if err := f.m.ApplyArchiveModes(t.Context(), nil, map[string]meta.ArchiveMode{stream: meta.ArchiveAll}, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) recordOriginNow() backfill.OriginStatus {
	f.t.Helper()
	st, err := backfill.RecordOrigin(f.t.Context(), f.env, stream)
	if err != nil {
		f.t.Fatal(err)
	}
	return st
}

func (f *fixture) origin() *meta.Origin {
	f.t.Helper()
	item, _, err := f.m.Stream(f.t.Context(), stream)
	if err != nil {
		f.t.Fatal(err)
	}
	return item.Origin
}

// An origin is recorded once, from a scan of a fresh stream, with the current catalog and the
// stream table's identity.
func TestRecordOrigin(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newOriginFixture(t)
		time.Sleep(time.Second)
		if st := f.recordOriginNow(); st.State != backfill.OriginRecorded {
			t.Fatalf("RecordOrigin = %+v, want recorded", st)
		}
		utc := func(t time.Time) time.Time { return t.UTC().Round(0) }
		want := meta.Origin{At: utc(time.Now()), Hash: f.hash, RegisteredAt: utc(time.Now().Add(-time.Second)), Born: utc(f.tables.born), ARN: streamARN}
		if got := f.origin(); got == nil || *got != want {
			t.Errorf("origin %+v, want %+v", got, want)
		}
		if st := f.recordOriginNow(); st.State != backfill.OriginHeld {
			t.Errorf("RecordOrigin = %+v over a valid origin, want held", st)
		}
	})
}

// No origin is recorded where the archive may already miss an event.
func TestRecordOriginRefuses(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		setup func(f *fixture)
		want  backfill.OriginState
	}{
		{"an event in the table", func(f *fixture) { f.putEvent() }, backfill.OriginTooLate},
		{"a table 23 hours old", func(f *fixture) { f.tables.born = time.Now().Add(-23 * time.Hour) }, backfill.OriginTooLate},
		{"archiving stopped", func(f *fixture) { f.stopArchive(f.t.Context()) }, backfill.OriginPending},
		{"a catalog that doesn't archive the stream", func(f *fixture) {
			data := catalogtest.MarshalFS(f.t, fstest.MapFS{
				"dynaflux.yaml":      {Data: []byte("store: " + storeName + "\nregions: [" + region + "]\narchive: {enabled: false}\n")},
				"dynaflux/store.sql": {Data: []byte(storeSQL)},
			})
			if _, err := f.m.RegisterCatalog(f.t.Context(), data, "test"); err != nil {
				f.t.Fatal(err)
			}
		}, backfill.OriginPending},
		{"no catalog", func(f *fixture) {
			f.m = meta.New(f.b, "empty-meta")
			f.env.Meta = f.m
			if err := f.m.ApplyArchiveModes(f.t.Context(), nil, map[string]meta.ArchiveMode{stream: meta.ArchiveAll}, true, time.Now()); err != nil {
				f.t.Fatal(err)
			}
		}, backfill.OriginPending},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newOriginFixture(t)
				tt.setup(f)
				if st := f.recordOriginNow(); st.State != tt.want || f.origin() != nil {
					t.Errorf("RecordOrigin = %+v, recorded %+v; want state %d and none", st, f.origin(), tt.want)
				}
			})
		})
	}
}

// Once a deploy changed the mode, an origin waits until no invocation of a build that didn't
// archive can still run: 16 minutes after the current catalog's registration.
func TestRecordOriginWaitsOutOldInvocations(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newOriginFixture(t)
		f.stopArchive(t.Context())
		err := f.m.ApplyArchiveModes(t.Context(), map[string]meta.ArchiveMode{stream: meta.ArchiveOff}, map[string]meta.ArchiveMode{stream: meta.ArchiveAll}, false, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(backfill.OriginWait - time.Second)
		if st := f.recordOriginNow(); st.State != backfill.OriginWaiting || !st.Until.Equal(time.Now().Add(time.Second)) {
			t.Fatalf("RecordOrigin = %+v within 16 minutes of the registration, want waiting a second more", st)
		}
		time.Sleep(time.Second)
		if st := f.recordOriginNow(); st.State != backfill.OriginRecorded || f.origin().Holes != 2 {
			t.Errorf("RecordOrigin = %+v, origin %+v; want one after both holes", st, f.origin())
		}
	})
}

// A rollback writes its holes and registers no catalog: the origin waits 16 minutes after its
// last hole, as after a registration, whatever order CloudFormation rolled back in.
func TestRecordOriginWaitsOutARollback(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newOriginFixture(t)
		time.Sleep(time.Hour)
		f.stopArchive(t.Context())
		err := f.m.ApplyArchiveModes(t.Context(), map[string]meta.ArchiveMode{stream: meta.ArchiveOff}, map[string]meta.ArchiveMode{stream: meta.ArchiveAll}, false, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if st := f.recordOriginNow(); st.State != backfill.OriginWaiting || !st.Until.Equal(time.Now().Add(backfill.OriginWait)) {
			t.Fatalf("RecordOrigin = %+v after a rollback, want waiting 16 minutes", st)
		}
		time.Sleep(backfill.OriginWait)
		if st := f.recordOriginNow(); st.State != backfill.OriginRecorded {
			t.Errorf("RecordOrigin = %+v, want recorded", st)
		}
	})
}

// A re-created stream table voids its origin, and a new one replaces it while it is fresh.
func TestRecordOriginReplacesAVoidOrigin(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newOriginFixture(t)
		f.recordOriginNow()
		f.tables.born = time.Now()
		f.witness.set(backfill.Facts{ARN: streamARN + "-2", Mapping: true})
		if st := f.recordOriginNow(); st.State != backfill.OriginRecorded || !f.origin().Born.Equal(time.Now()) || f.origin().ARN != streamARN+"-2" {
			t.Errorf("origin %+v, want the re-created table's", f.origin())
		}
	})
}

// meddling runs do once, after the item's first read, before the origin's write.
type meddling struct {
	backfill.Witness

	do func()
}

func (m *meddling) StreamARN(ctx context.Context, table string) (string, error) {
	if m.do != nil {
		m.do()
		m.do = nil
	}
	return m.Witness.StreamARN(ctx, table)
}

// A write that meets another starts over from the read of the stream item: a lag recorded
// meanwhile sits before the origin, and a deploy that stopped archiving meanwhile leaves none.
func TestRecordOriginStartsOver(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		do   func(f *fixture)
		want int // the origin's holes; -1 for no origin
	}{
		{"a lag", func(f *fixture) {
			if err := f.m.MarkLag(f.t.Context(), stream, time.Now()); err != nil {
				f.t.Fatal(err)
			}
		}, 1},
		{"a deploy that stops archiving", func(f *fixture) { f.stopArchive(f.t.Context()) }, -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newOriginFixture(t)
				f.env.Witness = &meddling{Witness: f.witness, do: func() { tt.do(f) }}
				ok := f.recordOriginNow().State == backfill.OriginRecorded
				switch o := f.origin(); {
				case tt.want < 0 && (ok || o != nil):
					t.Errorf("origin %+v, want none", o)
				case tt.want >= 0 && (!ok || o == nil || o.Holes != tt.want):
					t.Errorf("origin %+v, want one after %d holes", o, tt.want)
				}
			})
		})
	}
}

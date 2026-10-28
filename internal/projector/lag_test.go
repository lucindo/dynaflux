// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package projector_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// putLog records the partition key of every write, in order.
type putLog struct {
	store.Backend

	mu  sync.Mutex
	pks []string
}

func (p *putLog) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	p.mu.Lock()
	p.pks = append(p.pks, it[catalog.AttrPK].Str)
	p.mu.Unlock()
	return p.Backend.Put(ctx, table, it, c)
}

// withStreamItem gives clicks a stream item, as a deploy does.
func (f *fixture) withStreamItem(t *testing.T) {
	t.Helper()
	if err := f.meta.ApplyArchiveModes(t.Context(), nil, map[string]meta.ArchiveMode{"clicks": meta.ArchiveAll}, true, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) holes(t *testing.T) []meta.Hole {
	t.Helper()
	s, _, err := f.meta.Stream(t.Context(), "clicks")
	if err != nil {
		t.Fatal(err)
	}
	return s.Holes
}

// streamItemWrite returns the index in pks of the first write of the stream item, -1 for
// none, and whether every write before it is to a unit item: a fence.
func streamItemWrite(pks []string) (int, bool) {
	i := slices.Index(pks, "stream#clicks")
	return i, i >= 0 && !slices.ContainsFunc(pks[:i], func(p string) bool { return !strings.HasPrefix(p, "unit#") })
}

// arrived returns an INSERT of it that entered the change stream age ago.
func arrived(n int, it store.Item, age time.Duration) projector.Record {
	r := insert(n, it)
	r.Arrival = time.Now().Add(-age).Truncate(time.Second)
	return r
}

// A record above progress that arrived 23 hours 50 minutes before the batch started records
// a lag, before the batch archives or writes rows and progress; one a second younger
// doesn't.
func TestHandleRecordsALagFirst(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		age  time.Duration
		want int
	}{{23*time.Hour + 49*time.Minute + 59*time.Second, 0}, {23*time.Hour + 50*time.Minute, 1}} {
		t.Run(tt.age.String(), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				log := &putLog{}
				f := newFixture(t, func(b store.Backend) store.Backend { log.Backend = b; return log })
				f.withStreamItem(t)
				f.handle(t, arrived(1, f.click(t, "u1", "/a", 100), time.Minute), arrived(2, f.click(t, "u2", "/b", 100), tt.age))
				holes := f.holes(t)
				if len(holes) != tt.want {
					t.Fatalf("holes %+v, want %d", holes, tt.want)
				}
				if _, fencesFirst := streamItemWrite(log.pks); tt.want == 1 && (holes[0].Cause != meta.HoleLag || !holes[0].At.Equal(start) || !fencesFirst) {
					t.Errorf("hole %+v, writes %v; want a lag at %v, written after the fences only", holes[0], log.pks, start)
				}
			})
		})
	}
}

// One lag counts against an origin: a second old batch adds none, and a batch whose old
// records are at or below progress, as a mapping replaying from the trim horizon delivers,
// records nothing.
func TestHandleRecordsOneLagPerOrigin(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, nil)
		f.withStreamItem(t)
		a, b := f.click(t, "u1", "/a", 100), f.click(t, "u2", "/b", 100)
		f.handle(t, arrived(1, a, time.Minute))
		time.Sleep(24 * time.Hour)
		f.handle(t, arrived(1, a, 24*time.Hour+time.Minute))
		if holes := f.holes(t); len(holes) != 0 {
			t.Fatalf("a replay of handled records recorded %+v", holes)
		}
		f.handle(t, arrived(2, b, 24*time.Hour))
		f.handle(t, arrived(3, f.click(t, "u3", "/c", 100), 24*time.Hour))
		if holes := f.holes(t); len(holes) != 1 || holes[0].Cause != meta.HoleLag {
			t.Errorf("holes %+v, want one lag", holes)
		}
	})
}

// A lag that can't be recorded fails the batch before any row or progress is written.
func TestHandleFailsWhenALagCantBeRecorded(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		log := &putLog{}
		f := newFixture(t, func(b store.Backend) store.Backend {
			log.Backend = failingPuts{Backend: b, table: "app-meta", pk: "stream#clicks", err: errTransient}
			return log
		})
		f.withStreamItem(t)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		batch := projector.Batch{Stream: "clicks", Unit: unit, Records: []projector.Record{arrived(1, f.click(t, "u1", "/a", 100), 24*time.Hour)}}
		_, err := f.core.Handle(ctx, batch)
		u, uerr := f.meta.Unit(ctx, "clicks", unit)
		i, fencesFirst := streamItemWrite(log.pks)
		if !errors.Is(err, errTransient) || !fencesFirst || i != len(log.pks)-1 || len(f.archive.seqs()) != 0 || uerr != nil || u.Progress != "" {
			t.Errorf("Handle = %v after writes %v, archived %v, progress %q; want the lag's error, no row, no archive and no progress",
				err, log.pks, f.archive.seqs(), u.Progress)
		}
	})
}

// A batch that fails on the view states, as every batch does while the deployed build lacks
// an active version (G3), still records the lag of an old record first: DynamoDB trims the
// records behind it while it fails. A MODIFY meets G3 too.
func TestHandleRecordsALagBeforeTheViewStates(t *testing.T) {
	t.Parallel()
	for _, modify := range []bool{false, true} {
		t.Run(map[bool]string{false: "insert", true: "modify"}[modify], func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newPendingFixture(t, nil)
				f.withStreamItem(t)
				f.setState(t, meta.Activating)
				rec := arrived(1, f.click(t, "u1", "/a", 100), 23*time.Hour+50*time.Minute)
				if modify {
					rec = projector.Record{Op: projector.Modify, Seq: seq(1), Old: rec.New, New: f.click(t, "u1", "/b", 100), Arrival: rec.Arrival}
				}
				_, err := handleWith(t, f.build(t, []string{pageTotals}, nil), rec)
				checkUnknown(t, err, true, meta.Activating)
				if holes := f.holes(t); len(holes) != 1 || holes[0].Cause != meta.HoleLag || !holes[0].At.Equal(start) {
					t.Errorf("holes %+v, want a lag at %v", holes, start)
				}
			})
		})
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// No build tag: make test checks the throughput benchmark's oracle with synthetic appends,
// since a bug in it would show only as a false verdict on the maintainer's AWS run.

package e2e_test

import (
	"crypto/rand"
	"fmt"
	"maps"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/agg"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// The pools of the throughput benchmark's workload.
const (
	loadUsers = 10_000
	loadPages = 1_000
	loadTags  = 200
)

// nonce keeps the keys of this process apart from an earlier run's on the same AWS store.
//
//nolint:gochecknoglobals // one per process
var nonce = strings.ToLower(rand.Text()[:6])

func userName(u int) string { return fmt.Sprintf("%s-u%d", nonce, u) }

// pageEvent is a page view of the load, by its indices in the pools.
type pageEvent struct {
	user, page int
	tags       []int
	at         time.Time
	durationMS uint32
}

const secondsPerDay = 24 * 60 * 60

// dayOf numbers the UTC day of a time, as page_daily's toStartOfDay groups it.
func dayOf(at time.Time) int64 { return at.Unix() / secondsPerDay }

// bounds is the range a view value may take: exact for the confirmed appends, wider by the
// appends whose outcome is unknown.
type bounds struct{ lo, hi uint64 }

func (b bounds) String() string {
	if b.lo == b.hi {
		return strconv.FormatUint(b.lo, 10)
	}
	return fmt.Sprintf("%d to %d", b.lo, b.hi)
}

// tally is what a set of appends adds to page_daily and tag_hourly.
type tally struct {
	users    *agg.Func
	views    map[pageDay]uint64 // page_daily.views of the confirmed appends
	pairs    map[int64]pairSet  // by day: the (page, user) pairs of the confirmed appends
	tagViews [loadTags]uint64   // of the confirmed appends
	tagUsers [loadTags]agg.State
	unknown  []pageEvent // appends whose outcome is unknown
}

type pageDay struct {
	page int
	day  int64
}

type pageDayWant struct{ views, visitors bounds }

type tagWant struct{ views, users bounds }

func newTally(users *agg.Func) *tally {
	t := &tally{users: users, views: map[pageDay]uint64{}, pairs: map[int64]pairSet{}}
	for i := range t.tagUsers {
		t.tagUsers[i] = users.New()
	}
	return t
}

func (t *tally) add(ev pageEvent) error {
	day := dayOf(ev.at)
	t.views[pageDay{ev.page, day}]++
	if t.pairs[day] == nil {
		t.pairs[day] = newPairSet()
	}
	t.pairs[day].add(ev.page, ev.user)
	for _, tag := range ev.tags {
		t.tagViews[tag]++
		if err := t.tagUsers[tag].Add(agg.Input{Args: []expr.Value{expr.String(userName(ev.user))}}); err != nil {
			return err
		}
	}
	return nil
}

// merge adds o's appends to t.
func (t *tally) merge(o *tally) error {
	for pd, n := range o.views {
		t.views[pd] += n
	}
	for day, s := range o.pairs {
		if t.pairs[day] == nil {
			t.pairs[day] = newPairSet()
		}
		for i, w := range s {
			t.pairs[day][i] |= w
		}
	}
	for tag := range t.tagViews {
		t.tagViews[tag] += o.tagViews[tag]
		if err := t.tagUsers[tag].Merge(o.tagUsers[tag]); err != nil {
			return err
		}
	}
	t.unknown = append(t.unknown, o.unknown...)
	return nil
}

// pageDays returns what each page_daily row may hold.
func (t *tally) pageDays() map[pageDay]pageDayWant {
	out := map[pageDay]pageDayWant{}
	unknownUsers := map[pageDay][]int{}
	for _, ev := range t.unknown {
		pd := pageDay{ev.page, dayOf(ev.at)}
		unknownUsers[pd] = append(unknownUsers[pd], ev.user)
	}
	for pd := range unknownUsers {
		out[pd] = pageDayWant{}
	}
	for pd, n := range t.views {
		out[pd] = pageDayWant{views: bounds{n, n}}
	}
	for pd, w := range out {
		var row []uint64
		if s := t.pairs[pd.day]; s != nil {
			row = slices.Clone(s.row(pd.page))
		} else {
			row = make([]uint64, pairWords)
		}
		w.visitors.lo = countBits(row)
		for _, u := range unknownUsers[pd] {
			setBit(row, u)
		}
		w.views.hi = w.views.lo + uint64(len(unknownUsers[pd]))
		w.visitors.hi = countBits(row)
		out[pd] = w
	}
	return out
}

// tagWants returns what each tag's TagTotals row may hold. With no unknown outcome, the
// uniq sketch is the view's exactly, since a sketch ignores order and merges by register
// maximum; otherwise it lies between the estimates of the confirmed appends and of all of
// them (internal/agg's TestUniqEstimateNeverFalls).
func (t *tally) tagWants() (map[int]tagWant, error) {
	all := [loadTags]agg.State{}
	unknown := [loadTags]uint64{}
	for i := range all {
		all[i] = t.users.New()
		if err := all[i].Merge(t.tagUsers[i]); err != nil {
			return nil, err
		}
	}
	for _, ev := range t.unknown {
		for _, tag := range ev.tags {
			unknown[tag]++
			if err := all[tag].Add(agg.Input{Args: []expr.Value{expr.String(userName(ev.user))}}); err != nil {
				return nil, err
			}
		}
	}
	out := map[int]tagWant{}
	for tag := range all {
		if t.tagViews[tag] == 0 && unknown[tag] == 0 {
			continue
		}
		lo, err := estimate(t.tagUsers[tag])
		if err != nil {
			return nil, err
		}
		hi, err := estimate(all[tag])
		if err != nil {
			return nil, err
		}
		out[tag] = tagWant{views: bounds{t.tagViews[tag], t.tagViews[tag] + unknown[tag]}, users: bounds{lo, hi}}
	}
	return out, nil
}

// estimate is a uniq state's value; an empty one is 0.
func estimate(s agg.State) (uint64, error) {
	if s.Empty() {
		return 0, nil
	}
	v, err := s.Final()
	return v.AsUint(), err
}

// pairSet holds a day's (page, user) pairs, a bit each, each page's users in a row of words.
type pairSet []uint64

const pairWords = (loadUsers + 63) / 64

func newPairSet() pairSet { return make(pairSet, loadPages*pairWords) }

func (s pairSet) row(page int) []uint64 { return s[page*pairWords : (page+1)*pairWords] }

// add adds a pair and reports whether it is new.
func (s pairSet) add(page, user int) bool {
	row := s.row(page)
	isNew := row[user/64]&(1<<(user%64)) == 0
	setBit(row, user)
	return isNew
}

func setBit(row []uint64, i int) { row[i/64] |= 1 << (i % 64) }

func countBits(row []uint64) uint64 {
	n := 0
	for _, w := range row {
		n += bits.OnesCount64(w)
	}
	return uint64(n)
}

// TestTally checks what the oracle accepts of page_daily and tag_hourly, from confirmed
// appends split over two tallies and merged, as the load's generators are, and from appends
// whose outcome is unknown.
func TestTally(t *testing.T) {
	t.Parallel()
	users, err := agg.Compile(agg.Spec{
		Agg: catalog.Aggregate{Fn: catalog.AggUniq}, ArgTypes: []catalog.Type{{Kind: catalog.KindString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	d1 := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	d2 := d1.Add(24 * time.Hour)
	tests := []struct {
		name               string
		confirmed, unknown []pageEvent
		pages              map[pageDay]pageDayWant
		tags               map[int]tagWant
	}{
		{
			name: "confirmed only",
			confirmed: []pageEvent{
				{user: 1, page: 2, at: d1, tags: []int{5}, durationMS: 800},
				{user: 1, page: 2, at: d1, tags: []int{5, 7}},
				{user: 3, page: 2, at: d1},
				{user: 1, page: 4, at: d2, tags: []int{7}},
			},
			pages: map[pageDay]pageDayWant{
				{2, dayOf(d1)}: {views: bounds{3, 3}, visitors: bounds{2, 2}},
				{4, dayOf(d2)}: {views: bounds{1, 1}, visitors: bounds{1, 1}},
			},
			tags: map[int]tagWant{
				5: {views: bounds{2, 2}, users: bounds{1, 1}},
				7: {views: bounds{2, 2}, users: bounds{1, 1}},
			},
		},
		{
			name:      "unknown outcomes",
			confirmed: []pageEvent{{user: 1, page: 2, at: d1, tags: []int{5}}},
			unknown: []pageEvent{
				{user: 3, page: 2, at: d1, tags: []int{5}},
				{user: 1, page: 2, at: d1},
				{user: 4, page: 9, at: d1, tags: []int{8}},
			},
			pages: map[pageDay]pageDayWant{
				{2, dayOf(d1)}: {views: bounds{1, 3}, visitors: bounds{1, 2}},
				{9, dayOf(d1)}: {views: bounds{0, 1}, visitors: bounds{0, 1}},
			},
			tags: map[int]tagWant{
				5: {views: bounds{1, 2}, users: bounds{1, 2}},
				8: {views: bounds{0, 1}, users: bounds{0, 1}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tallyOf(t, users, tt.confirmed, tt.unknown)
			if pages := got.pageDays(); !maps.Equal(pages, tt.pages) {
				t.Errorf("pageDays = %v, want %v", pages, tt.pages)
			}
			tags, terr := got.tagWants()
			if terr != nil {
				t.Fatal(terr)
			}
			if !maps.Equal(tags, tt.tags) {
				t.Errorf("tagWants = %v, want %v", tags, tt.tags)
			}
		})
	}
}

// tallyOf adds the confirmed appends to two tallies in turn, and the unknown ones to the
// second, then merges the second into the first, as the load does with its generators'.
func tallyOf(t *testing.T, users *agg.Func, confirmed, unknown []pageEvent) *tally {
	t.Helper()
	first, second := newTally(users), newTally(users)
	for i, ev := range confirmed {
		into := first
		if i%2 == 1 {
			into = second
		}
		if err := into.add(ev); err != nil {
			t.Fatal(err)
		}
	}
	second.unknown = unknown
	if err := first.merge(second); err != nil {
		t.Fatal(err)
	}
	return first
}

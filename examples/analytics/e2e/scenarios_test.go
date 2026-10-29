// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
)

// Scenario 1: appends in every region; each region's views equal the oracle. On AWS, it
// first checks that the first deploy's backfill -all made every version live by the fresh rule.
//
//nolint:thelper // a scenario: its failures point into it
func testAppends(t *testing.T, h *harness) {
	if h.aws != nil {
		for _, reg := range h.regions {
			checkFresh(t, h, reg)
		}
	}
	r := newRun(t, h, "s1")
	for i, reg := range h.regions {
		user, member := r.key(reg.name+"-u"), r.key(reg.name+"-m")
		r.pageViews(reg,
			flux.PageView{UserID: user, Page: r.key("/home"), Tags: []string{r.key("landing")}, DurationMS: 120, EventTime: r.at},
			flux.PageView{
				UserID: user, Page: r.key("/pricing"), Referrer: new("https://example.com"), Tags: []string{r.key("landing"), r.key("ad")},
				DurationMS: 340, Country: "BR", EventTime: r.at.Add(time.Second),
			},
		)
		r.memberEvents(reg,
			flux.MemberEvent{WorkspaceID: r.key("ws"), MemberID: member, Action: "login", DurationMS: 30, EventTime: r.at},
			flux.MemberEvent{WorkspaceID: r.key("ws"), MemberID: member, Action: "logout", DurationMS: 50, EventTime: r.at.Add(time.Second)},
		)
		r.apiCalls(reg, flux.APICall{AccountID: r.key("acme"), Endpoint: "/search", Units: uint32(5 + i), RequestID: newUUID(t), EventTime: r.at})
	}
	r.converged(h.regions...)
}

// checkFresh checks what scripts/e2e/deploy.sh's backfill -all did on the new store, before
// any event: each job made its version live from the archive without reading it, by the
// fresh rule (docs/spec/lifecycle.md §4.3).
func checkFresh(t *testing.T, h *harness, reg *region) {
	t.Helper()
	states, err := reg.meta.ViewStates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := reg.meta.Jobs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for id := range h.p.def.Versions() {
		s := states[id]
		i := slices.IndexFunc(jobs, func(j meta.Job) bool { return j.Version == id })
		switch {
		case s.State != meta.Live || s.Split != meta.SplitFence:
			t.Errorf("%s: %s is %q with split %q, want live with split fence", reg.name, id, s.State, s.Split)
		case i < 0:
			t.Errorf("%s: %s has no job: deploy with scripts/e2e/deploy.sh, which runs backfill -all", reg.name, id)
		case jobs[i].State != meta.JobDone || !jobs[i].Fresh:
			t.Errorf("%s: the job of %s is %s with fresh %t, want done by the fresh rule", reg.name, id, jobs[i].State, jobs[i].Fresh)
		}
	}
}

// Scenario 4: a REMOVE record of a stream item leaves the views and the archive as they are.
//
//nolint:thelper // a scenario: its failures point into it
func testRemove(t *testing.T, h *harness) {
	r := newRun(t, h, "s4")
	home, user, page := h.regions[0], r.key("u"), r.key("/a")
	r.pageViews(home,
		flux.PageView{UserID: user, Page: page, DurationMS: 10, EventTime: r.at},
		flux.PageView{UserID: user, Page: page, DurationMS: 20, EventTime: r.at.Add(time.Second)},
	)
	r.converged(h.regions...)
	before, rerr := readPageViews(t.Context(), home.client, user)
	if rerr != nil {
		t.Fatal(rerr)
	}
	deleteStreamItem(t, home, "page_views", "user_id", user)
	// One partition key's records are in one shard, in order: once the views show this
	// event, the projector is past the REMOVE.
	r.pageViews(home, flux.PageView{UserID: user, Page: page, DurationMS: 30, EventTime: r.at.Add(2 * time.Second)})
	r.converged(h.regions...)
	for _, reg := range r.reached(home) {
		h.await(t, h.patience, reg.name+": the stream after the delete", func(ctx context.Context) error {
			evs, err := readPageViews(ctx, reg.client, user)
			if err == nil && len(evs) != 2 {
				err = fmt.Errorf("%d events of %s, want 2 of the 3 appended", len(evs), user)
			}
			return err
		})
	}
	after, rerr := readPageViews(t.Context(), home.client, user)
	if rerr != nil {
		t.Fatal(rerr)
	}
	texts := map[string]map[string]string{}
	for _, ev := range slices.Concat(before, after) {
		if texts[ev.EventID], rerr = pageViewText(ev); rerr != nil {
			t.Fatal(rerr)
		}
	}
	// The archive keeps the deleted event, and the REMOVE adds no copy.
	for _, reg := range r.reached(home) {
		checkArchive(t, r, reg, "page_views", "user_id", slices.Collect(maps.Values(texts)))
	}
}

// deleteStreamItem deletes the first item of a stream table whose column holds value.
func deleteStreamItem(t *testing.T, r *region, stream, column, value string) {
	t.Helper()
	table := r.naming.StreamTable(stream)
	pages := dynamodb.NewScanPaginator(r.ddb, &dynamodb.ScanInput{
		TableName:                 &table,
		FilterExpression:          new("#c = :v"),
		ExpressionAttributeNames:  map[string]string{"#c": column},
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberS{Value: value}},
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Items) > 0 {
			it := page.Items[0]
			key := map[string]types.AttributeValue{catalog.AttrPK: it[catalog.AttrPK], catalog.AttrSK: it[catalog.AttrSK]}
			if _, err = r.ddb.DeleteItem(t.Context(), &dynamodb.DeleteItemInput{TableName: &table, Key: key}); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("no item of %s with %s = %s", table, column, value)
}

// Scenario 5: a poison event breaks e2e_ratio; the other views stay exact, and the shard
// goes on.
//
//nolint:thelper // a scenario: its failures point into it
func testPoison(t *testing.T, h *harness) {
	r := newRun(t, h, "s5")
	home, user, page := h.regions[0], r.key("u"), r.key("/a")
	since := time.Now()
	r.pageViews(home, flux.PageView{UserID: user, Page: page, DurationMS: 0, EventTime: r.at})
	// After the poison event on its shard: the views show it only if the shard went on.
	r.pageViews(home, flux.PageView{UserID: user, Page: page, DurationMS: 40, EventTime: r.at.Add(time.Second)})
	r.converged(h.regions...)
	evs, err := readPageViews(t.Context(), home.client, user)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(evs, func(ev flux.PageView) bool { return ev.DurationMS == 0 })
	if i < 0 {
		t.Fatalf("no event with duration_ms 0 among %+v", evs)
	}
	for _, reg := range h.regions {
		broken := ""
		if slices.Contains(r.reached(home), reg) {
			broken = evs[i].EventID
		}
		h.await(t, h.patience, reg.name+": view states", func(ctx context.Context) error {
			return viewStates(ctx, reg, broken)
		})
	}
	if h.aws == nil {
		checkBreakerLog(t, home)
		return
	}
	for _, reg := range r.reached(home) {
		name := reg.naming.Base() + "-views-broken"
		h.await(t, h.patience, reg.name+": alarm "+name, func(ctx context.Context) error {
			return h.aws.alarmFired(ctx, reg, name, since)
		})
	}
}

// checkBreakerLog checks the projector's log on the emulator, where nothing fails but the
// poison event: its one WARN or ERROR line is the batch metrics line of the breaker it opened,
// the BreakerOpened metric behind the views-broken alarm.
func checkBreakerLog(t *testing.T, reg *region) {
	t.Helper()
	var lines []string
	for line := range strings.Lines(reg.logs()) {
		if strings.Contains(line, `"level":"WARN"`) || strings.Contains(line, `"level":"ERROR"`) {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 || !strings.Contains(lines[0], `"BreakerOpened":1`) || !strings.Contains(lines[0], `"broken":["`+ratioVersion+`"]`) {
		t.Errorf("%s: the projector's WARN and ERROR lines are\n%s\nwant one, with BreakerOpened 1 and %s broken",
			reg.name, strings.Join(lines, ""), ratioVersion)
	}
}

// viewStates checks that event broke ratioVersion, or that no event did when event is "",
// and that every other version is live.
func viewStates(ctx context.Context, reg *region, event string) error {
	states, err := reg.meta.ViewStates(ctx)
	if err != nil {
		return err
	}
	if _, ok := states[ratioVersion]; !ok {
		return fmt.Errorf("%s has no state", ratioVersion)
	}
	for v, s := range states {
		switch {
		case v == ratioVersion && event != "" && (s.State != meta.Broken || s.Failure.EventID != event):
			return fmt.Errorf("%s is %+v, want broken by event %s", v, s, event)
		case (v != ratioVersion || event == "") && s.State != meta.Live:
			return fmt.Errorf("%s is %+v, want live", v, s)
		}
	}
	return nil
}

// Scenario 8: each region's archive holds every event of its stream once, as the stream
// table holds it, under a real shard; and its unit items name real shards.
//
//nolint:thelper // a scenario: its failures point into it
func testArchive(t *testing.T, h *harness) {
	r := newRun(t, h, "s8")
	users, accounts := map[string]*region{}, map[string]*region{}
	for _, reg := range h.regions {
		user, account := r.key(reg.name+"-u"), r.key(reg.name+"-a")
		users[user], accounts[account] = reg, reg
		r.pageViews(reg,
			flux.PageView{UserID: user, Page: r.key("/a"), DurationMS: 10, EventTime: r.at},
			flux.PageView{
				UserID: user, Page: r.key("/b"), Referrer: new(`https://example.com/?q="é"`), Tags: []string{r.key("x"), r.key("y,z")},
				DurationMS: 4_000_000_000, Country: "BR", EventTime: r.at.Add(time.Second),
			},
		)
		r.apiCalls(reg, flux.APICall{AccountID: account, Endpoint: "/search", Units: 7, RequestID: newUUID(t), EventTime: r.at})
	}
	r.converged(h.regions...)
	for _, reg := range h.regions {
		pages := checkStream(t, r, reg, users, func(ctx context.Context, c *flux.Client, user string) ([]map[string]string, []flux.PageView, error) {
			evs, err := readPageViews(ctx, c, user)
			texts := make([]map[string]string, len(evs))
			for i, ev := range evs {
				if texts[i], err = pageViewText(ev); err != nil {
					return nil, nil, err
				}
				evs[i].EventID, evs[i].IngestTime, evs[i].OriginRegion = "", time.Time{}, ""
			}
			return texts, evs, err
		})
		calls := checkStream(t, r, reg, accounts, func(ctx context.Context, c *flux.Client, account string) ([]map[string]string, []flux.APICall, error) {
			var texts []map[string]string
			var evs []flux.APICall
			for ev, err := range c.ReadAPICalls(ctx, flux.APICallsKey{AccountID: account}) {
				if err != nil {
					return nil, nil, err
				}
				texts = append(texts, apiCallText(ev))
				ev.EventID, ev.IngestTime, ev.OriginRegion = "", time.Time{}, ""
				evs = append(evs, ev)
			}
			return texts, evs, nil
		})
		checkArchive(t, r, reg, "page_views", "user_id", pages)
		checkArchive(t, r, reg, "api_calls", "account_id", calls)
		checkUnits(t, reg, "page_views")
	}
}

// checkStream reads a region's events of the scenario's keys, checks that each holds what
// the oracle holds and the region it was appended in, and returns their archive text.
// read returns the events' text, and the events without what the writer sets.
func checkStream[E any](t *testing.T, r *run, reg *region, keys map[string]*region,
	read func(context.Context, *flux.Client, string) ([]map[string]string, []E, error),
) []map[string]string {
	t.Helper()
	var all []map[string]string
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		in := keys[key]
		if !slices.Contains(r.reached(in), reg) {
			continue
		}
		texts, got, err := read(t.Context(), reg.client, key)
		if err != nil {
			t.Fatal(err)
		}
		_, want, err := read(t.Context(), flux.New(r.oracle[reg]), key)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: events of %s\n%+v\nwant\n%+v", reg.name, key, got, want)
		}
		for _, text := range texts {
			if text[catalog.ColumnOriginRegion] != in.name {
				t.Errorf("%s: event %s has origin_region %s, want %s", reg.name, text[catalog.ColumnEventID], text[catalog.ColumnOriginRegion], in.name)
			}
		}
		all = append(all, texts...)
	}
	return all
}

// checkArchive waits until a region's archive holds want, the run's events of a stream, each
// event once, and checks the stream record of every copy. On the emulator, where nothing
// redelivers, each event has one copy.
func checkArchive(t *testing.T, r *run, reg *region, stream, key string, want []map[string]string) {
	t.Helper()
	// DynamoDB Local rounds ApproximateCreationDateTime down to the minute, and AWS's clock
	// can be behind ours.
	earliest := r.at.Truncate(time.Minute).Add(-time.Minute)
	r.h.await(t, r.h.archivePatience, reg.name+": the archive of "+stream, func(ctx context.Context) error {
		shards := streamShards(t, reg, stream) // a shard can close and split while Firehose buffers
		a, err := reg.archive(ctx, stream, key, r.tag)
		if err != nil {
			return err
		}
		if got := byEventID(a.events); !reflect.DeepEqual(got, byEventID(want)) {
			return fmt.Errorf("archived\n%v\nwant\n%v", got, byEventID(want))
		}
		if r.h.aws == nil && len(a.copies) != len(a.events) {
			return fmt.Errorf("%d copies of %d events", len(a.copies), len(a.events))
		}
		return checkCopies(a.copies, shards, earliest, time.Now().Add(time.Minute))
	})
}

// checkCopies checks the stream record of each copy: a shard of the stream, a sequence
// number of its own event in that shard, and an arrival time between earliest and latest.
func checkCopies(copies []copyRecord, shards []string, earliest, latest time.Time) error {
	seqs, events := map[[2]string]string{}, map[[2]string]string{}
	for _, c := range copies {
		switch {
		case !slices.Contains(shards, c.shard):
			return fmt.Errorf("a copy of %s has _shard %q, not one of the shards %q", c.event, c.shard, shards)
		case len(c.seq) != catalog.SequenceWidth || strings.Trim(c.seq, "0123456789") != "":
			return fmt.Errorf("a copy of %s has _seq %q, want %d digits", c.event, c.seq, catalog.SequenceWidth)
		case c.arrival.Before(earliest) || c.arrival.After(latest):
			return fmt.Errorf("a copy of %s has _arrival %s, want between %s and %s", c.event, c.arrival.UTC(), earliest.UTC(), latest.UTC())
		}
		bySeq, byEvent := [2]string{c.shard, c.seq}, [2]string{c.shard, c.event}
		if e, ok := events[bySeq]; ok && e != c.event {
			return fmt.Errorf("record %s of shard %s holds events %s and %s", c.seq, c.shard, e, c.event)
		}
		if s, ok := seqs[byEvent]; ok && s != c.seq {
			return fmt.Errorf("event %s has records %s and %s in shard %s", c.event, s, c.seq, c.shard)
		}
		events[bySeq], seqs[byEvent] = c.event, c.seq
	}
	return nil
}

// checkUnits checks that every unit item of a stream names a shard of its stream: the
// event's shardId is the stream's ShardId (docs/decisions/0003).
func checkUnits(t *testing.T, reg *region, stream string) {
	t.Helper()
	shards := streamShards(t, reg, stream)
	pages := dynamodb.NewScanPaginator(reg.ddb, &dynamodb.ScanInput{
		TableName:                 new(reg.naming.MetaTable()),
		FilterExpression:          new("begins_with(#pk, :p)"),
		ExpressionAttributeNames:  map[string]string{"#pk": catalog.AttrPK},
		ExpressionAttributeValues: map[string]types.AttributeValue{":p": &types.AttributeValueMemberS{Value: "unit#" + stream + "#"}},
	})
	units := 0
	for pages.HasMorePages() {
		page, err := pages.NextPage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range page.Items {
			units++
			id, ok := it["id"].(*types.AttributeValueMemberS)
			if !ok || !slices.Contains(shards, id.Value) {
				t.Errorf("%s: unit item %v doesn't name one of the shards %q", reg.name, it[catalog.AttrPK], shards)
			}
		}
	}
	if units == 0 {
		t.Errorf("%s: no unit item of %s", reg.name, stream)
	}
}

// streamShards returns the ShardIds that DescribeStream lists for a stream table.
func streamShards(t *testing.T, reg *region, stream string) []string {
	t.Helper()
	in := &dynamodbstreams.DescribeStreamInput{StreamArn: new(streamARN(t, reg, stream))}
	var ids []string
	for {
		out, err := reg.streams.DescribeStream(t.Context(), in)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range out.StreamDescription.Shards {
			ids = append(ids, aws.ToString(s.ShardId))
		}
		if out.StreamDescription.LastEvaluatedShardId == nil {
			return ids
		}
		in.ExclusiveStartShardId = out.StreamDescription.LastEvaluatedShardId
	}
}

// reached returns the regions an append in region in reaches.
func (r *run) reached(in *region) []*region {
	if r.h.replicated {
		return r.h.regions
	}
	return []*region{in}
}

func newUUID(t *testing.T) dynaflux.UUID {
	t.Helper()
	var u dynaflux.UUID
	if _, err := rand.Read(u[:]); err != nil {
		t.Fatal(err)
	}
	return u
}

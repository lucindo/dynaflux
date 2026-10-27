// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/esmlocal"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
)

// Each test checks one rule of the projector whose outcome depends on DynamoDB, on DynamoDB
// Local, through the handler that Run serves. The rules that don't depend on DynamoDB are
// unit tests.

const (
	putItem = "DynamoDB_20120810.PutItem"
	getItem = "DynamoDB_20120810.GetItem"
	query   = "DynamoDB_20120810.Query"
	// attrProgress is the unit item's progress (encoding.md §8.1): only a write that raises
	// progress has it.
	attrProgress = "progress"
)

// replica returns a change that makes a stream item the copy of its event that another
// region accepted, then applies payload to it.
func replica(payload func(store.Item)) func(store.Item) {
	return func(it store.Item) {
		it[catalog.AttrOriginRegion] = store.Value{Type: store.S, Str: "eu-west-1"}
		it[catalog.AttrIngestTime] = store.Value{Type: store.N, Str: strconv.FormatInt(time.Now().UnixMicro(), 10)}
		if payload != nil {
			payload(it)
		}
	}
}

// eventNames returns each record's eventName.
func eventNames(t *testing.T, recs []json.RawMessage) []string {
	t.Helper()
	out := make([]string, len(recs))
	for i, rec := range recs {
		out[i] = field(t, rec, "eventName")
	}
	return out
}

// The delivery unit is the event's shardId as delivered: the unit item, the row's high-water
// marks and the archive all name it (ADR 0003).
func TestWindowShardPassThrough(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.quota("acct-a", "req-1", 3)
	shard, recs := r.next(quotaEvents)
	r.deliver(r.fn, quotaEvents, shard, recs...)
	seq := storedSeq(t, recs[0])
	if u := r.unit(quotaEvents, shard); u.ID != shard || u.Progress != seq {
		t.Errorf("unit %q with progress %q, want %q with %q", u.ID, u.Progress, shard, seq)
	}
	if got, want := marks(t, r.item("quota_by_account", "acct-a")), map[string]string{keys.UnitKey(shard): seq}; !maps.Equal(got, want) {
		t.Errorf("high-water marks %v, want %v", got, want)
	}
	if got, want := r.fh.stored(), map[[2]string]int{{shard, seq}: 1}; !maps.Equal(got, want) {
		t.Errorf("archived (_shard, _seq) %v, want %v", got, want)
	}
}

// Events appended before the mapping starts reach the views: it reads from TRIM_HORIZON,
// and the projector applies INSERT records.
func TestS2InsertApplies(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.quota("acct-a", "req-1", 3)
	r.quota("acct-a", "req-2", 4)
	r.quota("acct-b", "req-3", 5)
	if err := esmlocal.Drain(t.Context(), r.stream(quotaEvents).r, functionTimeout, r.fn.invoke); err != nil {
		t.Fatal(err)
	}
	r.checkRow("acct-a 2 7", "quota_by_account@v1", "acct-a")
	r.checkRow("acct-b 1 5", "quota_by_account@v1", "acct-b")
	r.checkRow("acct-a us-west-2 2 7", "quota_by_origin@v1", "acct-a", appendRegion)
}

// A MODIFY whose payload equals its old image's changes nothing: a replica's copy of an
// event, with another origin and ingest_time, or a write that changed nothing, which
// DynamoDB Local doesn't record, so an INSERT stands in for it.
func TestS2ModifySamePayload(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	k := r.quota("acct-a", "req-1", 3)
	shard, inserts := r.next(quotaEvents)
	r.deliver(r.fn, quotaEvents, shard, inserts...)
	r.overwrite(quotaEvents, k, replica(nil))
	_, modifies := r.next(quotaEvents)
	if got := eventNames(t, modifies); !slices.Equal(got, []string{"MODIFY"}) {
		t.Fatalf("records %q, want one MODIFY", got)
	}
	unchanged := craft(t, inserts[0], func(rec, ddb map[string]any) {
		rec["eventName"] = "MODIFY"
		ddb["OldImage"] = ddb["NewImage"]
	})
	r.deliver(r.fn, quotaEvents, shard, modifies[0], unchanged)
	r.checkRow("acct-a 1 3", "quota_by_account@v1", "acct-a")
	r.checkRow("acct-a us-west-2 1 3", "quota_by_origin@v1", "acct-a", appendRegion)
	r.checkRow("", "quota_by_origin@v1", "acct-a", "eu-west-1")
	if n := len(r.fh.records()); n != 1 {
		t.Errorf("%d archived records, want the INSERT's", n)
	}
	if lines := r.metrics(); len(lines) != 0 {
		t.Errorf("metrics %v, want none", lines)
	}
}

// A MODIFY whose payload differs from its old image's is a DEDUP BY event appended in two
// regions with different values: counted, at WARN, and never applied.
func TestS2DivergentDuplicate(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	k := r.quota("acct-a", "req-1", 3)
	shard, inserts := r.next(quotaEvents)
	r.deliver(r.fn, quotaEvents, shard, inserts...)
	r.overwrite(quotaEvents, k, replica(func(it store.Item) { it["units"] = store.Value{Type: store.N, Str: "9"} }))
	_, modifies := r.next(quotaEvents)
	r.deliver(r.fn, quotaEvents, shard, modifies...)
	lines := r.metrics()
	if len(lines) != 1 || lines[0]["level"] != "WARN" || lines[0]["DivergentDuplicates"] != 1.0 {
		t.Errorf("metrics %v, want one WARN line with DivergentDuplicates 1", lines)
	}
	r.checkRow("acct-a 1 3", "quota_by_account@v1", "acct-a")
	if n := len(r.fh.records()); n != 1 {
		t.Errorf("%d archived records, want the INSERT's", n)
	}
}

// REMOVE records change nothing and cost no call, TTL deletes among them: DynamoDB Local
// never expires items, so a REMOVE with the userIdentity AWS documents stands in for one.
func TestS2RemoveSkipped(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	k := r.quota("acct-a", "req-1", 3)
	shard, inserts := r.next(quotaEvents)
	r.deliver(r.fn, quotaEvents, shard, inserts...)
	r.remove(quotaEvents, k)
	_, removes := r.next(quotaEvents)
	if got := eventNames(t, removes); !slices.Equal(got, []string{"REMOVE"}) {
		t.Fatalf("records %q, want one REMOVE", got)
	}
	ttl := craft(t, removes[0], func(rec, _ map[string]any) {
		rec["userIdentity"] = map[string]any{"type": "Service", "principalId": "dynamodb.amazonaws.com"}
	})
	for _, rec := range []json.RawMessage{removes[0], ttl} {
		before := len(r.fn.rec.Calls())
		r.deliver(r.fn, quotaEvents, shard, rec)
		if n := len(r.fn.rec.Calls()) - before; n != 0 {
			t.Errorf("%d DynamoDB or Firehose calls for %s, want none", n, rec)
		}
	}
	r.checkRow("acct-a 1 3", "quota_by_account@v1", "acct-a")
}

// origin_region is the item's _r: neither the record's awsRegion nor the projector's region.
func TestS2OriginFromItem(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.quota("acct-a", "req-1", 3)
	shard, recs := r.next(quotaEvents)
	if got := field(t, recs[0], "awsRegion"); got != "ddblocal" {
		t.Fatalf("the record's awsRegion is %q, want DynamoDB Local's ddblocal", got)
	}
	r.deliver(r.fn, quotaEvents, shard, recs...)
	r.checkRow("acct-a us-west-2 1 3", "quota_by_origin@v1", "acct-a", appendRegion)
	if got := r.fh.records(); len(got) != 1 || got[0][catalog.ColumnOriginRegion] != appendRegion {
		t.Errorf("archived %v, want one record from %s", got, appendRegion)
	}
}

// Records of several units write one row, whose high-water marks keep an entry per unit.
// DynamoDB Local's stream has one shard, so its records come as batches of four shardIds.
func TestS2ManyUnitsOneRow(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	for i := range 4 {
		r.quota("acct-a", fmt.Sprintf("req-%d", i), uint64(i+1))
	}
	shard, recs := r.next(quotaEvents)
	want := map[string]string{}
	for i, rec := range recs {
		unit := fmt.Sprintf("%s-%d", shard, i)
		r.deliver(r.fn, quotaEvents, unit, rec)
		want[keys.UnitKey(unit)] = storedSeq(t, rec)
	}
	r.checkRow("acct-a 4 10", "quota_by_account@v1", "acct-a")
	if got := marks(t, r.item("quota_by_account", "acct-a")); !maps.Equal(got, want) {
		t.Errorf("high-water marks %v, want %v", got, want)
	}
}

// rendezvous holds the row writes of its parties until each has reached its first one,
// having read every row it writes.
type rendezvous struct {
	parties int32
	arrived atomic.Int32
	all     chan struct{}
}

// party returns a function whose row writes wait at rv.
func (r *localRig) party(rv *rendezvous) *function {
	var once sync.Once
	return r.function(faults{put: func(ctx context.Context, table string, it store.Item) error {
		if !r.isRow(table, it) {
			return nil
		}
		once.Do(func() {
			if rv.arrived.Add(1) == rv.parties {
				close(rv.all)
			}
		})
		select {
		case <-rv.all:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
}

// lostRaces returns how many of fn's row writes failed their condition, and the tables of
// those that fn read after.
func (r *localRig) lostRaces(fn *function) (int, []string) {
	calls := fn.rec.Calls()
	lost := 0
	var reread []string
	for i, c := range calls {
		if c.Target != putItem || c.ErrorCode != "ConditionalCheckFailedException" || c.Resources[0] == r.naming.MetaTable() {
			continue
		}
		lost++
		if slices.ContainsFunc(calls[i+1:], func(later emulatortest.Call) bool {
			return later.Target == getItem && later.Resources[0] == c.Resources[0]
		}) {
			reread = append(reread, c.Resources[0])
		}
	}
	return lost, reread
}

// Two invocations of one batch, as Lambda may run a stale one beside its retry, race on each
// row's conditional write. The loser takes the stored row from the failed condition, with no
// read, finds its records applied, and both succeed.
func TestS1ConcurrentInvocations(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.quota("acct-a", "req-1", 3)
	shard, recs := r.next(quotaEvents)
	rv := &rendezvous{parties: 2, all: make(chan struct{})}
	fns := []*function{r.party(rv), r.party(rv)}
	event := [][]byte{r.event(quotaEvents, shard, recs...)}
	r.deliverAll(map[*function][][]byte{fns[0]: event, fns[1]: event})
	lost := 0
	for _, fn := range fns {
		races, reread := r.lostRaces(fn)
		lost += races
		if len(reread) > 0 {
			t.Errorf("read %q again after a row write lost the race", reread)
		}
	}
	if lost == 0 {
		t.Error("no row write lost the race")
	}
	r.checkRow("acct-a 1 3", "quota_by_account@v1", "acct-a")
	r.checkRow("acct-a us-west-2 1 3", "quota_by_origin@v1", "acct-a", appendRegion)
}

// The rows of a batch whose progress write failed hold its records in their high-water
// marks: the batch delivered again changes no row.
func TestS1ReapplySkipped(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.quota("acct-a", "req-1", 3)
	r.quota("acct-a", "req-2", 4)
	shard, recs := r.next(quotaEvents)
	var failed atomic.Bool
	fn := r.function(faults{put: func(_ context.Context, table string, it store.Item) error {
		if _, advance := it[attrProgress]; table == r.naming.MetaTable() && advance && failed.CompareAndSwap(false, true) {
			return errInjected
		}
		return nil
	}})
	event := r.event(quotaEvents, shard, recs...)
	if _, err := r.call(fn, event); !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want the failed progress write", err)
	}
	r.checkRow("acct-a 2 7", "quota_by_account@v1", "acct-a")
	if reply, err := r.call(fn, event); reply != success || err != nil {
		t.Fatalf("reply %s, %v; want %s", reply, err, success)
	}
	r.checkRow("acct-a 2 7", "quota_by_account@v1", "acct-a")
	r.checkRow("acct-a us-west-2 2 7", "quota_by_origin@v1", "acct-a", appendRegion)
	want := map[string]string{keys.UnitKey(shard): storedSeq(t, recs[1])}
	if got := marks(t, r.item("quota_by_account", "acct-a")); !maps.Equal(got, want) {
		t.Errorf("high-water marks %v, want %v", got, want)
	}
}

// A uniqExact value counts for the record that owns its marker: the retry of a row write that
// failed after its markers landed counts each value once, and a value the event's ARRAY JOIN
// repeats counts once.
func TestS1MarkerOwner(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.pageView("/p", "u1", 100, "t1", "t1", "t2")
	shard, recs := r.next(pageViews)
	table := r.naming.ViewTable("page_tags", 1)
	var failed atomic.Bool
	fn := r.function(faults{put: func(_ context.Context, tb string, it store.Item) error {
		if tb == table && r.isRow(tb, it) && failed.CompareAndSwap(false, true) {
			return errInjected
		}
		return nil
	}})
	event := r.event(pageViews, shard, recs...)
	if reply, err := r.call(fn, event); reply != partial(seqOf(t, recs[0])) || err != nil {
		t.Fatalf("reply %s, %v; want a partial failure", reply, err)
	}
	if reply, err := r.call(fn, event); reply != success || err != nil {
		t.Fatalf("reply %s, %v; want %s", reply, err, success)
	}
	r.checkRow("/p 2 2 3", "page_tags@v1", "/p")
}

// The SDK retries a row write whose response was lost after it landed. The retry fails its
// condition and finds the write done, so the batch succeeds and counts its record once.
func TestS1LostResponse(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.quota("acct-a", "req-1", 3)
	shard, recs := r.next(quotaEvents)
	table := r.naming.ViewTable("quota_by_account", 1)
	var lost atomic.Bool
	fn := r.function(faults{lose: func(target string, body []byte) bool {
		var in struct {
			TableName string `json:"TableName"`
		}
		return target == putItem && json.Unmarshal(body, &in) == nil && in.TableName == table && lost.CompareAndSwap(false, true)
	}})
	r.deliver(fn, quotaEvents, shard, recs...)
	var got []string
	for _, c := range fn.rec.Calls() {
		if c.Target == putItem && c.Resources[0] == table {
			got = append(got, strconv.Itoa(c.Status)+" "+c.ErrorCode)
		}
	}
	if want := []string{"0 ", "400 ConditionalCheckFailedException"}; !slices.Equal(got, want) {
		t.Errorf("row writes answered %q, want %q", got, want)
	}
	r.checkRow("acct-a 1 3", "quota_by_account@v1", "acct-a")
}

// A row that fails gives the lowest sequence number of its records, as delivered, and
// progress stays just below it. Lambda delivers the batch again from there until the row
// is written.
func TestS1PartialBatch(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	for i, account := range []string{"acct-a", "acct-b", "acct-a", "acct-b"} {
		r.quota(account, fmt.Sprintf("req-%d", i), uint64(i+1))
	}
	shard, recs := r.next(quotaEvents)
	table := r.naming.ViewTable("quota_by_account", 1)
	var fails atomic.Int32
	fn := r.function(faults{put: func(_ context.Context, tb string, it store.Item) error {
		if tb == table && it["account"].Str == "acct-b" && fails.Add(1) <= 2 {
			return errInjected
		}
		return nil
	}})
	for _, batch := range [][]json.RawMessage{recs, recs[1:]} {
		if reply, err := r.call(fn, r.event(quotaEvents, shard, batch...)); reply != partial(seqOf(t, recs[1])) || err != nil {
			t.Fatalf("reply %s, %v; want a partial failure from %s", reply, err, seqOf(t, recs[1]))
		}
		if u := r.unit(quotaEvents, shard); u.Progress != storedSeq(t, recs[0]) {
			t.Errorf("progress %q, want %q", u.Progress, storedSeq(t, recs[0]))
		}
	}
	r.deliver(fn, quotaEvents, shard, recs[1:]...)
	r.checkRow("acct-a 2 4", "quota_by_account@v1", "acct-a")
	r.checkRow("acct-b 2 6", "quota_by_account@v1", "acct-b")
}

// A batch that fails as a whole is a function error, which Lambda retries without limit;
// the retry that gets through applies the batch once.
func TestS1BatchErrorRetried(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.quota("acct-a", "req-1", 3)
	r.quota("acct-a", "req-2", 4)
	shard, recs := r.next(quotaEvents)
	var fails atomic.Int32
	fn := r.function(faults{query: func(table string) error {
		if table == r.naming.MetaTable() && fails.Add(1) <= 3 {
			return errInjected
		}
		return nil
	}})
	event := r.event(quotaEvents, shard, recs...)
	for range 3 {
		if reply, err := r.call(fn, event); reply != "" || !errors.Is(err, errInjected) {
			t.Fatalf("reply %q, %v; want the failed view-state read", reply, err)
		}
	}
	if reply, err := r.call(fn, event); reply != success || err != nil {
		t.Fatalf("reply %s, %v; want %s", reply, err, success)
	}
	if got := r.reasons(); !slices.Equal(got, []string{"other", "other", "other"}) {
		t.Errorf("reasons %q, want three of other", got)
	}
	r.checkRow("acct-a 2 7", "quota_by_account@v1", "acct-a")
}

// An event that a version can't evaluate opens that version's breaker, with the record that
// broke it, and every other version applies the batch. The function never writes the
// version again, even with the view states it cached before the breaker opened.
func TestS1Breaker(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.pageView("/a", "u1", 100)
	poison := r.pageView("/b", "u2", 0) // page_rates divides by ms
	shard, recs := r.next(pageViews)
	r.deliver(r.fn, pageViews, shard, recs...)
	_, eventID, err := keys.ParseStreamSK(poison.SK)
	if err != nil {
		t.Fatal(err)
	}
	states := r.checkStates("page_rates@v1")
	if f := states["page_rates@v1"].Failure; f == nil || f.Unit != shard || f.Seq != storedSeq(t, recs[1]) || f.EventID != eventID {
		t.Errorf("page_rates@v1 broke with %+v, want unit %s, seq %s, event %s", f, shard, storedSeq(t, recs[1]), eventID)
	}
	lines := r.metrics()
	if len(lines) != 1 || lines[0]["level"] != "ERROR" || lines[0]["BreakerOpened"] != 1.0 ||
		!reflect.DeepEqual(lines[0]["broken"], []any{"page_rates@v1"}) {
		t.Errorf("metrics %v, want one ERROR line with BreakerOpened 1 for page_rates@v1", lines)
	}
	r.checkRow("s1 2 2", "site_totals@v1", "s1")
	r.pageView("/c", "u3", 50)
	_, recs = r.next(pageViews)
	r.deliver(r.fn, pageViews, shard, recs...)
	r.checkRow("s1 3 3", "site_totals@v1", "s1")
	table := r.naming.ViewTable("page_rates", 1)
	for _, c := range r.fn.rec.Calls() {
		if c.Target == putItem && c.Resources[0] == table {
			t.Fatalf("wrote to %s, whose version is broken", table)
		}
	}
}

// fill fills a row's high-water marks with entries of units that have no item, so none is
// pruned, until one more entry for seq would take the item past DynamoDB's 400 KB.
func (r *localRig) fill(view, key, seq string) {
	r.t.Helper()
	it := r.item(view, key)
	one, err := keys.HWM{Units: map[string]string{keys.UnitKey("u"): seq}}.Encode()
	if err != nil {
		r.t.Fatal(err)
	}
	none := store.Value{Type: store.M, Map: map[string]store.Value{}}
	entry := store.ItemSize(store.Item{"m": one}) - store.ItemSize(store.Item{"m": none})
	h := keys.HWM{Units: marks(r.t, it)}
	for i := range (store.MaxItemSize - store.ItemSize(it)) / entry {
		h.Units[keys.UnitKey(fmt.Sprintf("filler-%d", i))] = seq
	}
	if it[catalog.AttrHighWaterMarks], err = h.Encode(); err == nil {
		err = r.b.Put(r.t.Context(), r.naming.ViewTable(view, 1), it, store.Cond{})
	}
	if err != nil {
		r.t.Fatal(err)
	}
}

// A row that would grow past DynamoDB's item limit fails, and RowTooLarge counts it, but
// neither the batch nor the version fails: Lambda delivers its records again.
func TestS1RowTooLarge(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.quota("acct-a", "req-1", 1)
	shard, recs := r.next(quotaEvents)
	r.deliver(r.fn, quotaEvents, shard, recs...)
	r.quota("acct-a", "req-2", 2)
	_, later := r.next(quotaEvents)
	r.fill("quota_by_account", "acct-a", storedSeq(t, later[0]))
	// From another unit, so the row needs one more high-water mark.
	if reply, err := r.call(r.fn, r.event(quotaEvents, shard+"-other", later...)); reply != partial(seqOf(t, later[0])) || err != nil {
		t.Fatalf("reply %s, %v; want a partial failure", reply, err)
	}
	lines := r.metrics()
	if len(lines) != 1 || lines[0]["level"] != "ERROR" || lines[0]["RowTooLarge"] != 1.0 {
		t.Errorf("metrics %v, want one ERROR line with RowTooLarge 1", lines)
	}
	states, err := r.meta.ViewStates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s := states["quota_by_account@v1"].State; s != meta.Live {
		t.Errorf("quota_by_account@v1 is %s, want live", s)
	}
	r.checkRow("acct-a 1 1", "quota_by_account@v1", "acct-a")
}

// dropTable deletes a table of the rig, and creates it again when the test ends, for the
// rig's cleanup to delete.
func (r *localRig) dropTable(table string) {
	r.t.Helper()
	if _, err := r.db.DeleteTable(r.t.Context(), &dynamodb.DeleteTableInput{TableName: &table}); err != nil {
		r.t.Fatal(err)
	}
	ctx := context.WithoutCancel(r.t.Context())
	r.t.Cleanup(func() {
		_, err := r.db.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:   &table,
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: new(catalog.AttrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: new(catalog.AttrSK), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: new(catalog.AttrPK), KeyType: types.KeyTypeHash},
				{AttributeName: new(catalog.AttrSK), KeyType: types.KeyTypeRange},
			},
		})
		if err != nil {
			r.t.Error(err)
		}
	})
}

// A request DynamoDB rejects on every attempt, here to a view table that was deleted, fails
// the batch, never a version's breaker.
func TestS1InvalidRequest(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	r.quota("acct-a", "req-1", 3)
	shard, recs := r.next(quotaEvents)
	r.dropTable(r.naming.ViewTable("quota_by_account", 1))
	if reply, err := r.call(r.fn, r.event(quotaEvents, shard, recs...)); reply != "" || !errors.Is(err, store.ErrInvalidRequest) {
		t.Fatalf("reply %q, %v; want ErrInvalidRequest", reply, err)
	}
	if got := r.reasons(); !slices.Equal(got, []string{"invalid_request"}) {
		t.Errorf("reasons %q, want invalid_request", got)
	}
	r.checkStates("")
}

// checkStates fails the test unless every version of the suite's catalog has a state:
// broken for the version called broken, live for the others. It returns the states.
func (r *localRig) checkStates(broken string) map[string]meta.ViewState {
	r.t.Helper()
	states, err := r.meta.ViewStates(r.t.Context())
	if err != nil {
		r.t.Fatal(err)
	}
	n := 0
	for _, v := range rules.cat.Catalog().Views {
		for _, vv := range v.Versions {
			n++
			id, want := catalog.VersionID(v.Name, vv.Number), meta.Live
			if id == broken {
				want = meta.Broken
			}
			if got := states[id].State; got != want {
				r.t.Errorf("%s is %q, want %s", id, got, want)
			}
		}
	}
	if len(states) != n {
		r.t.Errorf("%d view states, want %d", len(states), n)
	}
	return states
}

// Records at or below the unit's progress, read consistently after the invocation started,
// are skipped. A stale environment that replays a window the mapping already checkpointed,
// while the current one goes on, sends nothing to a view.
func TestS9SkipByProgress(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	for i := range 4 {
		r.quota("acct-a", fmt.Sprintf("req-%d", i), 1)
	}
	shard, window := r.next(quotaEvents)
	batches := [][]byte{r.event(quotaEvents, shard, window[:2]...), r.event(quotaEvents, shard, window[2:]...)}
	for _, b := range batches {
		if reply, err := r.call(r.fn, b); reply != success || err != nil {
			t.Fatalf("reply %s, %v; want %s", reply, err, success)
		}
	}
	r.quota("acct-a", "req-4", 1)
	r.quota("acct-a", "req-5", 1)
	_, later := r.next(quotaEvents)
	stale := r.function(faults{})
	r.deliverAll(map[*function][][]byte{r.fn: {r.event(quotaEvents, shard, later...)}, stale: batches})
	metaTable := r.naming.MetaTable()
	replayed := stale.rec.Calls()
	if i := slices.IndexFunc(replayed, func(c emulatortest.Call) bool { return c.Resources[0] != metaTable }); i >= 0 {
		t.Errorf("the replay sent %s to %s", replayed[i].Target, replayed[i].Resources)
	}
	if n := countCalls(replayed, func(c emulatortest.Call) bool { return c.Target == getItem }); n < len(batches) {
		t.Errorf("the replay read the unit %d times, want at least once per batch", n)
	}
	inconsistent := func(c emulatortest.Call) bool {
		return c.Resources[0] == metaTable && (c.Target == getItem || c.Target == query) && !c.Consistent
	}
	if n := countCalls(slices.Concat(r.fn.rec.Calls(), replayed), inconsistent); n > 0 {
		t.Errorf("%d reads of the meta table without ConsistentRead", n)
	}
	r.checkRow("acct-a 6 6", "quota_by_account@v1", "acct-a")
}

func countCalls(calls []emulatortest.Call, f func(emulatortest.Call) bool) int {
	n := 0
	for _, c := range calls {
		if f(c) {
			n++
		}
	}
	return n
}

// advance raises a unit's progress to seq, stamped at.
func (r *localRig) advance(stream, unit, seq string, at time.Time) {
	r.t.Helper()
	if err := r.meta.Advance(r.t.Context(), r.unit(stream, unit), seq, at, func(string) bool { return true }); err != nil {
		r.t.Fatal(err)
	}
}

// A row drops a unit's high-water mark once the unit's progress covers it and rose more
// than the prune window W ago, and keeps one whose progress rose less than W ago.
func TestS9Prune(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	for i := range 3 {
		r.quota("acct-a", fmt.Sprintf("req-%d", i), 1)
	}
	shard, recs := r.next(quotaEvents)
	old, recent := shard+"-old", shard+"-recent"
	r.deliver(r.fn, quotaEvents, old, recs[0])
	r.deliver(r.fn, quotaEvents, recent, recs[1])
	w := time.Duration(rules.cat.Catalog().PruneWindowSeconds) * time.Second
	r.advance(quotaEvents, old, storedSeq(t, recs[2]), time.Now().Add(-w-15*time.Second))
	r.advance(quotaEvents, recent, storedSeq(t, recs[2]), time.Now().Add(-w+15*time.Second))
	r.deliver(r.fn, quotaEvents, shard, recs[2])
	want := map[string]string{keys.UnitKey(recent): storedSeq(t, recs[1]), keys.UnitKey(shard): storedSeq(t, recs[2])}
	if got := marks(t, r.item("quota_by_account", "acct-a")); !maps.Equal(got, want) {
		t.Errorf("high-water marks %v, want %v", got, want)
	}
	r.checkRow("acct-a 3 3", "quota_by_account@v1", "acct-a")
}

// fence writes the item of a unit without progress whose fence for version is f, so the
// fence alone decides which of the unit's records the live projector applies to the
// version (encoding.md §8.1).
func (r *localRig) fence(stream, unit, version, f string) {
	r.t.Helper()
	str := func(s string) store.Value { return store.Value{Type: store.S, Str: s} }
	it := store.Item{
		catalog.AttrPK:         str("unit#" + stream + "#" + keys.UnitKey(unit)),
		catalog.AttrSK:         str(keys.NoSortKey),
		"id":                   str(unit),
		"fences":               {Type: store.M, Map: map[string]store.Value{version: str(f)}},
		catalog.AttrRowVersion: {Type: store.N, Str: "1"},
		catalog.AttrTTL:        keys.TTL(time.Now().Add(time.Hour).UnixMicro()),
	}
	if err := r.b.Put(r.t.Context(), r.naming.MetaTable(), it, store.IfAbsent()); err != nil {
		r.t.Fatal(err)
	}
	if got := r.unit(stream, unit).Fences[version]; got != f {
		r.t.Fatalf("the unit's fence for %s is %q, want %q", version, got, f)
	}
}

// While a version activates, the live projector applies to it only the records above its
// fence on the unit: the backfill applies those at or below it.
func TestS1bLiveAboveFence(t *testing.T) {
	t.Parallel()
	const version = "quota_by_account@v1"
	r := newLocalRig(t, version)
	if _, err := r.meta.Activate(t.Context(), version, quotaEvents, meta.SplitFence, meta.DefaultBounds); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		r.quota("acct-a", fmt.Sprintf("req-%d", i), uint64(i+1))
	}
	shard, recs := r.next(quotaEvents)
	r.fence(quotaEvents, shard, version, storedSeq(t, recs[1]))
	r.deliver(r.fn, quotaEvents, shard, recs...)
	r.checkRow("acct-a 2 7", version, "acct-a")
	r.checkRow("acct-a us-west-2 4 10", "quota_by_origin@v1", "acct-a", appendRegion)
}

// Progress passes a record only once Firehose stored it, so after every invocation every
// record at or below progress is in the archive. A failed call fails the batch; records
// Firehose rejects are sent again, and one it stored though it rejected it is stored twice.
func TestS1bArchiveBeforeProgress(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	for i := range 6 {
		r.quota("acct-a", fmt.Sprintf("req-%d", i), uint64(i+1))
	}
	shard, recs := r.next(quotaEvents)
	seqs := make([]string, len(recs))
	for i, rec := range recs {
		seqs[i] = storedSeq(t, rec)
	}
	archivedUpTo := func(want string) {
		t.Helper()
		u := r.unit(quotaEvents, shard)
		if u.Progress != want {
			t.Errorf("progress %q, want %q", u.Progress, want)
		}
		stored := r.fh.stored()
		for _, seq := range seqs {
			if seq <= u.Progress && stored[[2]string{shard, seq}] == 0 {
				t.Errorf("record %s is at or below progress, but not archived", seq)
			}
		}
	}
	r.fh.setDown(true)
	if reply, err := r.call(r.fn, r.event(quotaEvents, shard, recs[:3]...)); reply != "" || err == nil {
		t.Fatalf("reply %q, %v; want a failed invocation", reply, err)
	}
	if got := r.reasons(); !slices.Equal(got, []string{"archive"}) {
		t.Errorf("reasons %q, want archive", got)
	}
	archivedUpTo("")
	r.fh.setDown(false)
	sent := map[string]int{}
	r.fh.setScript(func(rec archived) outcome {
		seq := fmt.Sprint(rec[archive.ColumnSeq])
		sent[seq]++
		switch {
		case sent[seq] > 1:
			return accepted
		case seq == seqs[1]:
			return rejected
		case seq == seqs[2]:
			return duplicated
		}
		return accepted
	})
	r.deliver(r.fn, quotaEvents, shard, recs[:3]...)
	archivedUpTo(seqs[2])
	r.deliver(r.fn, quotaEvents, shard, recs[3:]...)
	archivedUpTo(seqs[5])
	want := map[[2]string]int{}
	for _, seq := range seqs {
		want[[2]string{shard, seq}] = 1
	}
	want[[2]string{shard, seqs[2]}] = 2
	if got := r.fh.stored(); !maps.Equal(got, want) {
		t.Errorf("archived (_shard, _seq) %v, want %v", got, want)
	}
	r.checkRow("acct-a 6 21", "quota_by_account@v1", "acct-a")
}

// A fence for a version the batch didn't apply, written while the batch ran, fails its
// progress write: the backfill leaves the version's records above the fence to the live
// projector. The retry reads the view states again and applies the version.
func TestS1bFenceDuringBatch(t *testing.T) {
	t.Parallel()
	const version = "quota_by_account@v1"
	r := newLocalRig(t, version)
	r.quota("acct-a", "req-1", 3)
	r.quota("acct-a", "req-2", 4)
	shard, recs := r.next(quotaEvents)
	var fenced atomic.Bool
	fn := r.function(faults{put: func(_ context.Context, table string, it store.Item) error {
		if _, advance := it[attrProgress]; table != r.naming.MetaTable() || !advance || !fenced.CompareAndSwap(false, true) {
			return nil
		}
		// The backfill activates the version and fences the unit.
		if _, err := r.meta.Activate(t.Context(), version, quotaEvents, meta.SplitFence, meta.DefaultBounds); err != nil {
			return err
		}
		u, err := r.meta.Unit(t.Context(), quotaEvents, shard)
		if err == nil {
			_, err = r.meta.EnsureFence(t.Context(), u, version)
		}
		return err
	}})
	event := r.event(quotaEvents, shard, recs...)
	if _, err := r.call(fn, event); !errors.Is(err, meta.ErrFenced) {
		t.Fatalf("err = %v, want ErrFenced", err)
	}
	if got := r.reasons(); !slices.Equal(got, []string{"fenced"}) {
		t.Errorf("reasons %q, want fenced", got)
	}
	r.checkRow("", version, "acct-a")
	if reply, err := r.call(fn, event); reply != success || err != nil {
		t.Fatalf("reply %s, %v; want %s", reply, err, success)
	}
	r.checkRow("acct-a 2 7", version, "acct-a")
	r.checkRow("acct-a us-west-2 2 7", "quota_by_origin@v1", "acct-a", appendRegion)
}

// Only INSERT records reach the archive, each with its unit, its sequence number in 40
// digits and its arrival in the stream.
func TestS1bOnlyInserts(t *testing.T) {
	t.Parallel()
	r := newLocalRig(t)
	k := r.quota("acct-a", "req-1", 3)
	r.overwrite(quotaEvents, k, replica(nil))
	r.remove(quotaEvents, k)
	r.quota("acct-a", "req-2", 4)
	shard, recs := r.next(quotaEvents)
	if got, want := eventNames(t, recs), []string{"INSERT", "MODIFY", "REMOVE", "INSERT"}; !slices.Equal(got, want) {
		t.Fatalf("records %q, want %q", got, want)
	}
	// DynamoDB Local rounds ApproximateCreationDateTime down to the minute, AWS to the second.
	recs[3] = craft(t, recs[3], func(_, ddb map[string]any) { ddb["ApproximateCreationDateTime"] = 1761480123 })
	r.deliver(r.fn, quotaEvents, shard, recs...)
	got := r.fh.records()
	if len(got) != 2 {
		t.Fatalf("archived %v, want the two INSERTs", got)
	}
	for i, rec := range []json.RawMessage{recs[0], recs[3]} {
		if got[i][archive.ColumnShard] != shard || got[i][archive.ColumnSeq] != storedSeq(t, rec) {
			t.Errorf("archived record %d is from %v at %v, want %s at %s", i, got[i][archive.ColumnShard], got[i][archive.ColumnSeq],
				shard, storedSeq(t, rec))
		}
	}
	if _, ok := got[0][archive.ColumnArrival]; !ok {
		t.Errorf("archived %v without %s", got[0], archive.ColumnArrival)
	}
	if a := fmt.Sprint(got[1][archive.ColumnArrival]); a != "1761480123000000" {
		t.Errorf("%s is %s, want 1761480123000000", archive.ColumnArrival, a)
	}
	r.checkRow("acct-a 2 7", "quota_by_account@v1", "acct-a")
}

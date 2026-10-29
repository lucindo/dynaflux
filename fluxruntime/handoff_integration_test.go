// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package fluxruntime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
)

// The fenced handoff end to end (m3-plan §13): projector functions archive quota events
// through the PutRecordBatch fake, which rejects some records once and stores others twice;
// a backfill job of a new additive version reads the fake's accepted records through
// archive.Decoder, while appends go on before, during and after it; the version ends live,
// and its rows count every event once.
func TestS1bHandoff(t *testing.T) {
	t.Parallel()
	const version = "quota_by_account@v1"
	r := newLocalRig(t, version)
	data, err := catalog.Marshal(rules.cat.Catalog())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.meta.RegisterCatalog(t.Context(), data, "test"); err != nil {
		t.Fatal(err)
	}
	sent := map[string]int{}
	r.fh.setScript(func(rec archived) outcome {
		seq := fmt.Sprint(rec[archive.ColumnSeq])
		sent[seq]++
		switch n, _ := strconv.Atoi(seq[len(seq)-1:]); {
		case sent[seq] == 1 && n%3 == 0:
			return rejected
		case sent[seq] == 1 && n%4 == 0:
			return duplicated
		}
		return accepted
	})
	h := &handoff{r: r, want: map[string][2]uint64{}, born: time.Now().Add(-48 * time.Hour)}
	h.recordOrigin()
	h.appendAndDeliver(6)
	env := h.env(catalog.Hash(data))
	if _, err = backfill.Start(t.Context(), r.meta, backfill.Request{Version: version, Stream: quotaEvents, Name: "quota-by-account-v1", Execution: backfill.LocalExecution, Split: meta.SplitFence}); err != nil {
		t.Fatal(err)
	}
	stepped := make(chan error, 1)
	go func() { stepped <- h.stepUntilDone(env, version) }()
	for range 5 {
		h.appendAndDeliver(3)
		time.Sleep(300 * time.Millisecond)
	}
	if err = <-stepped; err != nil {
		t.Fatal(err)
	}
	h.appendAndDeliver(4)
	for _, account := range []string{"acct-a", "acct-b", "acct-c"} {
		w := h.want[account]
		r.checkRow(fmt.Sprintf("%s %d %d", account, w[0], w[1]), version, account)
	}
	j, _, err := r.meta.Job(t.Context(), version)
	if err != nil || j.State != meta.JobDone || j.Fresh || j.Applied == 0 {
		t.Errorf("job %s, fresh %v, applied %d, %v; want done from the archive", j.State, j.Fresh, j.Applied, err)
	}
}

// handoff is TestS1bHandoff's rig, and the rows it wants: by account, requests and units.
type handoff struct {
	r    *localRig
	n    int
	want map[string][2]uint64
	born time.Time // the stream table's, as the job sees it
}

// recordOrigin records the origin of the quota stream's archive, as a deploy and backfill -all
// do while the stream table is fresh.
func (h *handoff) recordOrigin() {
	r := h.r
	ctx := r.t.Context()
	err := r.meta.ApplyArchiveModes(ctx, nil, map[string]meta.ArchiveMode{quotaEvents: meta.ArchiveAll}, true, h.born)
	var item meta.StreamItem
	if err == nil {
		item, _, err = r.meta.Stream(ctx, quotaEvents)
	}
	if err == nil {
		_, err = r.meta.RecordOrigin(ctx, item, meta.Origin{At: h.born, Hash: "h", RegisteredAt: h.born, Born: h.born, ARN: r.stream(quotaEvents).arn})
	}
	if err != nil {
		r.t.Fatal(err)
	}
}

// appendAndDeliver appends n quota events and delivers the stream's new records to a new
// function, as a mapping does, until they all succeed. A function reads the view states when
// it starts, so a batch that meets a fence it can't account for is delivered again.
func (h *handoff) appendAndDeliver(n int) {
	r := h.r
	r.t.Helper()
	for range n {
		h.n++
		account, units := []string{"acct-a", "acct-b", "acct-c"}[h.n%3], uint64(h.n)
		r.quota(account, "req-"+strconv.Itoa(h.n), units)
		w := h.want[account]
		h.want[account] = [2]uint64{w[0] + 1, w[1] + units}
	}
	shard, recs := r.next(quotaEvents)
	event := r.event(quotaEvents, shard, recs...)
	for range 10 {
		if reply, err := r.call(r.function(faults{}), event); reply == success && err == nil {
			return
		}
	}
	r.t.Fatalf("records of %s never delivered", shard)
}

// env returns a backfill environment over the rig's tables, whose archive is the records the
// PutRecordBatch fake accepted, complete once each call returned.
func (h *handoff) env(hash string) *backfill.Env {
	r := h.r
	streams := dynamodbstreams.NewFromConfig(r.cfg)
	arn := r.stream(quotaEvents).arn
	return &backfill.Env{
		Naming:  r.naming,
		Catalog: rules.cat.Catalog(),
		Hash:    hash,
		Meta:    r.meta,
		Store:   r.b,
		Core:    projector.New(rules.cat.Projector(), r.b, nil, projector.Config{Naming: r.naming}),
		Units:   shards{client: streams, arn: arn},
		Tables:  bornAt{h.born},
		Archive: fakeArchive{fh: r.fh},
		Region:  emulatortest.Region,
		Witness: mapped{arn},
		Runs:    backfill.NewMemRuns(),
		Lag:     noLag{},
		Bounds:  meta.Bounds{Listing: time.Second},
		Memory:  1 << 20,
		Chunk:   3,
		Log:     slog.New(slog.DiscardHandler),
	}
}

// stepUntilDone steps attempt 1 of version's job until it ends, each step a minute and a half
// long, as the state machine does.
func (h *handoff) stepUntilDone(env *backfill.Env, version string) error {
	for range 100 {
		ctx, cancel := context.WithTimeout(h.r.t.Context(), 90*time.Second)
		wait, done, err := backfill.Step(ctx, env, version, 1, "local")
		cancel()
		if err != nil || done {
			return err
		}
		time.Sleep(min(wait, time.Second))
	}
	return fmt.Errorf("the job of %s never ended", version)
}

// shards lists a stream's shards on DynamoDB Local.
type shards struct {
	client *dynamodbstreams.Client
	arn    string
}

func (s shards) Units(ctx context.Context, _ string) ([]string, error) {
	var out []string
	in := &dynamodbstreams.DescribeStreamInput{StreamArn: &s.arn}
	for {
		page, err := s.client.DescribeStream(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, sh := range page.StreamDescription.Shards {
			out = append(out, aws.ToString(sh.ShardId))
		}
		if page.StreamDescription.LastEvaluatedShardId == nil {
			return out, nil
		}
		in.ExclusiveStartShardId = page.StreamDescription.LastEvaluatedShardId
	}
}

// bornAt is stream tables created at a time, never restored.
type bornAt struct{ at time.Time }

func (b bornAt) Born(context.Context, string) (time.Time, bool, error) { return b.at, false, nil }

// mapped is a stream whose ARN is arn and which a mapping reads, without gaps.
type mapped struct{ arn string }

func (m mapped) StreamARN(context.Context, string) (string, error) { return m.arn, nil }

func (m mapped) Facts(context.Context, string, meta.StreamItem) (backfill.Facts, error) {
	return backfill.Facts{ARN: m.arn, Mapping: true}, nil
}

type noLag struct{}

func (noLag) Lag(context.Context) (time.Duration, error) { return 0, nil }

// fakeArchive is the records the PutRecordBatch fake stored, one file of them, which every
// query exports. The fake stores a record before its call returns, so the archive holds every
// record put before a cut once the cut is taken. Its queries succeed at once, and its error
// prefix is empty.
type fakeArchive struct{ fh *fakeFirehose }

func (fakeArchive) Complete(context.Context, string, time.Time) (bool, error) { return true, nil }

func (fakeArchive) Start(_ context.Context, q backfill.Query, _ string) (string, error) {
	return strconv.Itoa(int(q.Kind)), nil
}

func (fakeArchive) Status(context.Context, string) (backfill.QueryStatus, error) {
	return backfill.QueryStatus{State: backfill.QuerySucceeded}, nil
}

func (fakeArchive) Stop(context.Context, string) error { return nil }

// Partitions lists one partition of every event time.
func (fakeArchive) Partitions(context.Context, string) ([]backfill.Partition, error) {
	return []backfill.Partition{{From: time.Unix(0, 0), To: time.Unix(1<<32, 0), Bytes: 1}}, nil
}

func (fakeArchive) Files(context.Context, string) ([]string, error) { return []string{"accepted"}, nil }

func (fakeArchive) ErrorObjects(context.Context, string, string, time.Time) ([]string, error) {
	return nil, nil
}

func (a fakeArchive) Open(context.Context, string) (io.ReadCloser, error) {
	var b bytes.Buffer
	for _, rec := range a.fh.records() {
		line, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		b.Write(append(line, '\n'))
	}
	return io.NopCloser(&b), nil
}

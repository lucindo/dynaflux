// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package esmlocal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"

	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/esmlocal"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/validate"
)

const success = `{"state":{},"batchItemFailures":[]}`

// stream is a stream table on DynamoDB Local, created as a catalog's manifest describes it.
type stream struct {
	t     *testing.T
	db    *dynamodb.Client
	st    *dynamodbstreams.Client
	table string
	arn   string
}

func newStream(t *testing.T) *stream {
	t.Helper()
	cfg, cat, ds := validate.Load(fstest.MapFS{
		"dynaflux.yaml":        {Data: []byte("store: esm\nregions: [us-east-1]\n")},
		"dynaflux/streams.sql": {Data: []byte("CREATE STREAM clicks (k String) PRIMARY KEY (k);")},
	}, "/project")
	if cat == nil {
		t.Fatalf("the fixture doesn't compile: %v", ds)
	}
	m, err := infra.Build(cfg, cat, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	awsCfg := emulatortest.Config(t, nil)
	s := &stream{
		t: t, db: dynamodb.NewFromConfig(awsCfg), st: dynamodbstreams.NewFromConfig(awsCfg),
		table: emulatortest.Tables(t, awsCfg, m).StreamTable("clicks"),
	}
	d, err := s.db.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: &s.table})
	if err != nil {
		t.Fatal(err)
	}
	s.arn = *d.Table.LatestStreamArn
	return s
}

func (s *stream) put(pk, n string) {
	s.t.Helper()
	_, err := s.db.PutItem(s.t.Context(), &dynamodb.PutItemInput{TableName: &s.table, Item: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: pk},
		"sk": &types.AttributeValueMemberS{Value: "!"},
		"n":  &types.AttributeValueMemberN{Value: n},
	}})
	if err != nil {
		s.t.Fatal(err)
	}
}

// puts inserts items p0…p<n-1> (zero-padded to width) and returns their records' summaries.
func (s *stream) puts(n, width int) []string {
	s.t.Helper()
	var sums []string
	for i := range n {
		pk := fmt.Sprintf("p%0*d", width, i)
		s.put(pk, "1")
		sums = append(sums, "INSERT "+pk)
	}
	return sums
}

func (s *stream) remove(pk string) {
	s.t.Helper()
	_, err := s.db.DeleteItem(s.t.Context(), &dynamodb.DeleteItemInput{TableName: &s.table, Key: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: pk},
		"sk": &types.AttributeValueMemberS{Value: "!"},
	}})
	if err != nil {
		s.t.Fatal(err)
	}
}

// shard is the stream's one shard: DynamoDB Local splits none (docs/decisions/0009).
func (s *stream) shard() string {
	s.t.Helper()
	d, err := s.st.DescribeStream(s.t.Context(), &dynamodbstreams.DescribeStreamInput{StreamArn: &s.arn})
	if err != nil {
		s.t.Fatal(err)
	}
	if n := len(d.StreamDescription.Shards); n != 1 {
		s.t.Fatalf("the stream has %d shards, want 1", n)
	}
	return *d.StreamDescription.Shards[0].ShardId
}

// read calls Next once and fails the test unless it returns n records.
func (s *stream) read(r *esmlocal.Reader, n int) map[string][]json.RawMessage {
	s.t.Helper()
	recs, err := r.Next(s.t.Context())
	if err != nil {
		s.t.Fatal(err)
	}
	count := 0
	for _, rs := range recs {
		count += len(rs)
	}
	if count != n {
		s.t.Fatalf("read %d records, want %d", count, n)
	}
	return recs
}

// summaries returns "<eventName> <pk>" per record and fails the test on a record without
// the stream's eventSourceARN.
func (s *stream) summaries(recs []json.RawMessage) []string {
	s.t.Helper()
	sums := make([]string, len(recs))
	for i, rec := range recs {
		var r struct {
			EventName      string `json:"eventName"`
			EventSourceARN string `json:"eventSourceARN"`
			Change         struct {
				Keys struct {
					PK struct {
						S string `json:"S"`
					} `json:"pk"`
				} `json:"Keys"`
			} `json:"dynamodb"`
		}
		if err := json.Unmarshal(rec, &r); err != nil {
			s.t.Fatal(err)
		}
		if r.EventSourceARN != s.arn {
			s.t.Errorf("record %d: eventSourceARN %q, want %q", i, r.EventSourceARN, s.arn)
		}
		sums[i] = r.EventName + " " + r.Change.Keys.PK.S
	}
	return sums
}

func seqs(t *testing.T, recs []json.RawMessage) []string {
	t.Helper()
	out := make([]string, len(recs))
	for i, rec := range recs {
		var r struct {
			Change struct {
				SequenceNumber string `json:"SequenceNumber"`
			} `json:"dynamodb"`
		}
		if err := json.Unmarshal(rec, &r); err != nil {
			t.Fatal(err)
		}
		out[i] = r.Change.SequenceNumber
	}
	return out
}

func records(t *testing.T, ev []byte) []json.RawMessage {
	t.Helper()
	var e struct {
		Records []json.RawMessage `json:"Records"`
	}
	if err := json.Unmarshal(ev, &e); err != nil {
		t.Fatal(err)
	}
	return e.Records
}

// One Next returns every record written before the reader started, from TRIM_HORIZON and
// over more than one GetRecords page; a later Next returns only what was written since.
func TestNextReadsEachRecordOnceInOrder(t *testing.T) {
	t.Parallel()
	s := newStream(t)
	want := s.puts(150, 3)
	s.put("p000", "2")
	s.remove("p001")
	want = append(want, "MODIFY p000", "REMOVE p001")

	r := esmlocal.NewReader(s.st, s.arn)
	got, err := r.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	shard := s.shard()
	if g := s.summaries(got[shard]); len(got) != 1 || !slices.Equal(g, want) {
		t.Errorf("records of %d shards; of shard %s:\n got  %q\n want %q", len(got), shard, g, want)
	}

	again, err := r.Next(t.Context())
	if err != nil || len(again) != 0 {
		t.Fatalf("Next after every record: %d shards with records, %v; want none", len(again), err)
	}
	s.put("p150", "1")
	if g := s.summaries(s.read(r, 1)[shard]); !slices.Equal(g, []string{"INSERT p150"}) {
		t.Errorf("records after a later write: %q, want [INSERT p150]", g)
	}
}

func TestDrainDeliversBatchesOf100(t *testing.T) {
	t.Parallel()
	s := newStream(t)
	want := s.puts(201, 3)
	last := s.read(esmlocal.NewReader(s.st, s.arn), len(want))[s.shard()][200]

	const timeout = time.Minute
	var events [][]byte
	err := esmlocal.Drain(t.Context(), esmlocal.NewReader(s.st, s.arn), timeout, func(ctx context.Context, ev []byte) ([]byte, error) {
		if d, ok := ctx.Deadline(); !ok || time.Until(d) > timeout {
			t.Errorf("call %d: deadline %v (set: %v), want at most %v away", len(events), d, ok, timeout)
		}
		events = append(events, ev)
		return []byte(success), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var sizes []int
	var got []string
	for _, ev := range events {
		recs := records(t, ev)
		sizes = append(sizes, len(recs))
		got = append(got, s.summaries(recs)...)
	}
	if !slices.Equal(sizes, []int{100, 100, 1}) {
		t.Errorf("batch sizes %v, want [100 100 1]", sizes)
	}
	if !slices.Equal(got, want) {
		t.Errorf("records:\n got  %q\n want %q", got, want)
	}
	wantLast := fmt.Sprintf(`{"Records":[%s],"shardId":%q,"eventSourceARN":%q,`+
		`"window":{"start":"1970-01-01T00:00:00Z","end":"1970-01-01T00:15:00Z"},`+
		`"state":{},"isFinalInvokeForWindow":false,"isWindowTerminatedEarly":false}`, last, s.shard(), s.arn)
	if g := string(events[len(events)-1]); g != wantLast {
		t.Errorf("last event:\n got  %s\n want %s", g, wantLast)
	}
}

// reply answers one call; seq holds the sequence numbers of the call's records.
type reply func(seq []string) (string, error)

func failAt(ids ...string) string {
	items := make([]string, len(ids))
	for i, id := range ids {
		items[i] = fmt.Sprintf(`{"itemIdentifier":%q}`, id)
	}
	return `{"state":{},"batchItemFailures":[` + strings.Join(items, ",") + `]}`
}

func TestDrainRetries(t *testing.T) {
	t.Parallel()
	succeed := func([]string) (string, error) { return success, nil }
	fail := func([]string) (string, error) { return "", errors.New("function error") }
	answer := func(body string) reply { return func([]string) (string, error) { return body, nil } }
	cases := []struct {
		name    string
		replies []reply  // one per call, in order
		want    []string // each call's records
		wantErr string
	}{{
		name:    "a partial failure resumes at the lowest itemIdentifier",
		replies: []reply{func(seq []string) (string, error) { return failAt(seq[3], seq[1], seq[2]), nil }, succeed},
		want:    []string{"p0 p1 p2 p3", "p1 p2 p3"},
	}, {
		name:    "a function error repeats the call",
		replies: []reply{fail, succeed},
		want:    []string{"p0 p1 p2 p3", "p0 p1 p2 p3"},
	}, {
		name:    "a response without state repeats the call",
		replies: []reply{answer(`{"batchItemFailures":[]}`), succeed},
		want:    []string{"p0 p1 p2 p3", "p0 p1 p2 p3"},
	}, {
		name:    "an itemIdentifier outside the batch repeats the call",
		replies: []reply{answer(failAt("1")), succeed},
		want:    []string{"p0 p1 p2 p3", "p0 p1 p2 p3"},
	}, {
		name:    "three failed calls of one batch fail",
		replies: []reply{func(seq []string) (string, error) { return failAt(seq[1]), nil }, fail, fail},
		want:    []string{"p0 p1 p2 p3", "p1 p2 p3", "p1 p2 p3"},
		wantErr: "3 failed calls of one batch",
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := drainFour(t, tc.replies)
			if (err == nil) != (tc.wantErr == "") || err != nil && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Drain: %v, want an error containing %q", err, tc.wantErr)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("calls' records %q, want %q", got, tc.want)
			}
		})
	}
}

// drainFour drains a stream of four records, p0 to p3, into a function that answers with
// replies in turn, and returns each call's records as "p0 p1 …".
func drainFour(t *testing.T, replies []reply) ([]string, error) {
	t.Helper()
	s := newStream(t)
	s.puts(4, 0)

	var events [][]byte
	err := esmlocal.Drain(t.Context(), esmlocal.NewReader(s.st, s.arn), 10*time.Second, func(_ context.Context, ev []byte) ([]byte, error) {
		if len(events) == len(replies) {
			t.Fatalf("call %d, want %d calls", len(events)+1, len(replies))
		}
		events = append(events, ev)
		out, err := replies[len(events)-1](seqs(t, records(t, ev)))
		return []byte(out), err
	})

	var got []string
	for i, ev := range events {
		pks := s.summaries(records(t, ev))
		for j := range pks {
			pks[j] = strings.TrimPrefix(pks[j], "INSERT ")
		}
		got = append(got, strings.Join(pks, " "))
		if i > 0 && got[i] == got[i-1] && !bytes.Equal(ev, events[i-1]) {
			t.Errorf("call %d repeats call %d's records in other bytes:\n %s\n %s", i, i-1, ev, events[i-1])
		}
	}
	return got, err
}

// A shard read while it held no record yields the records written later, after another
// table's: DynamoDB Local numbers every table's records in one sequence, and fails a read
// from the first read's iterator once that sequence moved past it.
func TestNextAfterAnEmptyRead(t *testing.T) {
	t.Parallel()
	s, other := newStream(t), newStream(t)
	r := esmlocal.NewReader(s.st, s.arn)
	s.read(r, 0)
	other.puts(1, 1)
	want := s.puts(2, 1)
	if g := s.summaries(s.read(r, 2)[s.shard()]); !slices.Equal(g, want) {
		t.Errorf("records after an empty read: %q, want %q", g, want)
	}
}

// afterIterator runs do once, after the first GetShardIterator call returns.
type afterIterator struct {
	next dynamodbstreams.HTTPClient
	once sync.Once
	do   func()
}

func (a *afterIterator) Do(req *http.Request) (*http.Response, error) {
	resp, err := a.next.Do(req)
	if err == nil && req.Header.Get("X-Amz-Target") == "DynamoDBStreams_20120810.GetShardIterator" {
		a.once.Do(a.do)
	}
	return resp, err
}

// The shard's first record, landing after another table's between the iterator and the
// read, fails that read; the reader takes TRIM_HORIZON again on its next call.
func TestNextWhileTheFirstRecordLands(t *testing.T) {
	t.Parallel()
	s, other := newStream(t), newStream(t)
	var want []string
	hook := &afterIterator{do: func() {
		other.puts(1, 1)
		want = s.puts(1, 1)
	}}
	st := dynamodbstreams.NewFromConfig(emulatortest.Config(t, nil), func(o *dynamodbstreams.Options) {
		hook.next, o.HTTPClient = o.HTTPClient, hook
	})
	r := esmlocal.NewReader(st, s.arn)
	s.read(r, 0)
	if g := s.summaries(s.read(r, 1)[s.shard()]); !slices.Equal(g, want) {
		t.Errorf("records after the race: %q, want %q", g, want)
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package firehose_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/archive/firehose"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

const (
	unit = "shardId-00000001761475200000-1a2b3c4d"
	// deliveryStream is pv's Firehose stream: 20 bytes, so a call can reach exactly
	// 4,000,000 encoded bytes.
	deliveryStream = "analytics-archive-pv"
	eventTime      = 1761475200123456
	sql            = "CREATE STREAM pv (path String) PRIMARY KEY (path);\n" +
		"CREATE STREAM other (x String) PRIMARY KEY (x);\n"
	// recordOverhead is the size of pv's record of a test event less its path's length,
	// the size of {"event_id":"<26>","event_time":<16>,"ingest_time":<16>,
	// "origin_region":"us-east-1","path":"","_shard":"<unit>","_seq":"<40>","_arrival":<16>}.
	recordOverhead = 1 + 39 + 30 + 31 + 28 + 10 + 49 + 50 + 28 + 1
)

// fakeFirehose is a Sender whose answer to each call comes from answer. It keeps every
// call and the event_ids of the records it accepted.
type fakeFirehose struct {
	t      *testing.T
	answer func(call int, recs [][]byte) ([]string, error)

	mu       sync.Mutex
	calls    [][]string // the event_ids of each call
	times    []time.Time
	accepted map[string]bool
}

func (f *fakeFirehose) PutBatch(_ context.Context, stream string, recs [][]byte) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if stream != deliveryStream {
		f.t.Errorf("call to stream %s, want %s", stream, deliveryStream)
	}
	ids := eventIDs(f.t, recs)
	f.calls = append(f.calls, ids)
	f.times = append(f.times, time.Now())
	codes, err := f.answer(len(f.calls)-1, recs)
	if err == nil && len(codes) == len(recs) {
		for i, c := range codes {
			if c == "" {
				f.accepted[ids[i]] = true
			}
		}
	}
	return codes, err
}

func newFake(t *testing.T, answer func(call int, recs [][]byte) ([]string, error)) *fakeFirehose {
	t.Helper()
	return &fakeFirehose{t: t, answer: answer, accepted: map[string]bool{}}
}

// acceptAll accepts every record.
func acceptAll(_ int, recs [][]byte) ([]string, error) { return make([]string, len(recs)), nil }

// writer returns the writer of us-east-1 of a store with streams pv and other, whose
// dynaflux.yaml ends with archive.
func writer(t *testing.T, archive string, s firehose.Sender) *firehose.Writer {
	t.Helper()
	cat, err := catalog.Unmarshal(catalogtest.MarshalFS(t, fstest.MapFS{
		"dynaflux.yaml":      {Data: []byte("store: analytics\nregions: [us-east-1, us-west-2]\n" + archive)},
		"dynaflux/store.sql": {Data: []byte(sql)},
	}))
	if err != nil {
		t.Fatal(err)
	}
	c, err := projector.Compile(cat)
	if err != nil {
		t.Fatal(err)
	}
	return firehose.NewWriter(c, catalog.Naming{Store: "analytics"}, "us-east-1", s)
}

// insert is the INSERT record with sequence number 1000+i of an event of pv with event_id
// id, accepted in region.
func insert(t *testing.T, i int, id, region, path string) projector.Record {
	t.Helper()
	sk, err := keys.StreamSK(eventTime, id)
	if err != nil {
		t.Fatal(err)
	}
	return projector.Record{
		Op:      projector.Insert,
		Seq:     fmt.Sprintf("%021d", 1000+i),
		Arrival: time.Unix(1761475200, 0),
		New: store.Item{
			catalog.AttrPK:           {Type: store.S, Str: path},
			catalog.AttrSK:           {Type: store.S, Str: sk},
			catalog.AttrOriginRegion: {Type: store.S, Str: region},
			catalog.AttrIngestTime:   {Type: store.N, Str: "1761475200130000"},
			"path":                   {Type: store.S, Str: path},
		},
	}
}

// inserts returns n INSERT records of us-east-1 with the event_ids id(0), id(1) and so on.
func inserts(t *testing.T, n int) []projector.Record {
	t.Helper()
	recs := make([]projector.Record, n)
	for i := range recs {
		recs[i] = insert(t, i, id(i), "us-east-1", "/")
	}
	return recs
}

// id is the i-th test event_id.
func id(i int) string { return fmt.Sprintf("01JB8Z9X4Q7M3N2P5R6S8T%04d", i) }

func ids(is ...int) []string {
	out := make([]string, len(is))
	for j, i := range is {
		out[j] = id(i)
	}
	return out
}

func eventIDs(t *testing.T, recs [][]byte) []string {
	t.Helper()
	ids := make([]string, len(recs))
	for i, r := range recs {
		var ev struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(r, &ev); err != nil {
			t.Fatalf("record %.100s: %v", r, err)
		}
		ids[i] = ev.EventID
	}
	return ids
}

// archiveFor calls w.Archive for pv with a deadline d away.
func archiveFor(t *testing.T, w *firehose.Writer, d time.Duration, recs []projector.Record) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	return w.Archive(ctx, "pv", unit, recs)
}

func TestArchiveResendsOnlyFailedRecords(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(call int, recs [][]byte) ([]string, error) {
			if call == 0 {
				return []string{"", "ServiceUnavailableException", "", "InternalFailure"}, nil
			}
			return acceptAll(call, recs)
		})
		if err := archiveFor(t, writer(t, "", f), 10*time.Second, inserts(t, 4)); err != nil {
			t.Fatal(err)
		}
		if want := [][]string{ids(0, 1, 2, 3), ids(1, 3)}; !slices.EqualFunc(f.calls, want, slices.Equal) {
			t.Errorf("calls %q, want %q", f.calls, want)
		}
		if wait := f.times[1].Sub(f.times[0]); wait >= 100*time.Millisecond {
			t.Errorf("the resend waited %v, want less than the first delay of 100ms", wait)
		}
	})
}

// A record holds the values that Decode reads from the item, in its table's column order,
// then the bookkeeping columns.
func TestArchiveRecordBytes(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var got [][]byte
		f := newFake(t, func(call int, recs [][]byte) ([]string, error) {
			got = append(got, recs...)
			return acceptAll(call, recs)
		})
		if err := archiveFor(t, writer(t, "", f), 10*time.Second, inserts(t, 1)); err != nil {
			t.Fatal(err)
		}
		want := `{"event_id":"01JB8Z9X4Q7M3N2P5R6S8T0000","event_time":1761475200123456,"ingest_time":1761475200130000,` +
			`"origin_region":"us-east-1","path":"/","_shard":"` + unit + `","_seq":"0000000000000000000000000000000000001000",` +
			`"_arrival":1761475200000000}`
		if len(got) != 1 || string(got[0]) != want {
			t.Errorf("records %q, want [%s]", got, want)
		}
	})
}

// A response with fewer entries than records accepts none of them.
func TestArchiveResendsAllOnWrongResponseLength(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(call int, recs [][]byte) ([]string, error) {
			if call == 0 {
				return []string{"", ""}, nil
			}
			return acceptAll(call, recs)
		})
		if err := archiveFor(t, writer(t, "", f), 10*time.Second, inserts(t, 3)); err != nil {
			t.Fatal(err)
		}
		if want := [][]string{ids(0, 1, 2), ids(0, 1, 2)}; !slices.EqualFunc(f.calls, want, slices.Equal) {
			t.Errorf("calls %q, want %q", f.calls, want)
		}
	})
}

// Resends stop at half the time left when Archive started, so rows and progress keep the
// other half.
func TestArchiveGivesUpAtHalfTheTimeLeft(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, recs [][]byte) ([]string, error) {
			codes := make([]string, len(recs))
			for i, got := range eventIDs(t, recs) {
				if got == id(1) {
					codes[i] = "ServiceUnavailableException"
				}
			}
			return codes, nil
		})
		start := time.Now()
		err := archiveFor(t, writer(t, "", f), 10*time.Second, inserts(t, 2))
		want := "firehose: analytics-archive-pv didn't accept 1 records of unit " + unit + " in time: ServiceUnavailableException"
		if err == nil || err.Error() != want {
			t.Errorf("got %v, want %s", err, want)
		}
		stop := start.Add(5 * time.Second)
		if returned := time.Now(); returned.After(stop) || f.times[len(f.times)-1].After(stop) {
			t.Errorf("last call %v and return %v after the start, want both by 5s", f.times[len(f.times)-1].Sub(start), returned.Sub(start))
		}
		// The first five waits total at most 0.1 + 0.2 + 0.4 + 0.8 + 1.6 = 3.1 s.
		if len(f.calls) < 6 {
			t.Errorf("%d calls, want resends until the stop", len(f.calls))
		}
	})
}

// The wait before a resend grows to at most 5 s, however long the invocation.
func TestArchiveResendWaitsAtMost5s(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, recs [][]byte) ([]string, error) {
			return slices.Repeat([]string{"InternalFailure"}, len(recs)), nil
		})
		if err := archiveFor(t, writer(t, "", f), 200*time.Second, inserts(t, 1)); err == nil {
			t.Fatal("got nil, want an error")
		}
		for i := 1; i < len(f.times); i++ {
			if wait := f.times[i].Sub(f.times[i-1]); wait >= 5*time.Second {
				t.Errorf("resend %d waited %v, want less than 5s", i, wait)
			}
		}
	})
}

// The SDK has already retried a call that fails, so Archive returns at once.
func TestArchiveCallErrorFailsAtOnce(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		denied := errors.New("AccessDeniedException")
		f := newFake(t, func(int, [][]byte) ([]string, error) { return nil, denied })
		err := archiveFor(t, writer(t, "", f), 10*time.Second, inserts(t, 2))
		if !errors.Is(err, denied) || len(f.calls) != 1 {
			t.Errorf("got %v after %d calls, want the call's error after 1", err, len(f.calls))
		}
	})
}

// A record that no resend can archive fails the batch, naming it, before any call, whatever
// archive.events says.
func TestArchiveRefusesUnarchivableRecords(t *testing.T) {
	t.Parallel()
	const bad = "01JB8Z9X4Q7M3N2P5R6S8T0V1W"
	at := func(change func(*projector.Record)) projector.Record {
		r := insert(t, 1, bad, "us-east-1", "/")
		change(&r)
		return r
	}
	path := func(n int) func(*projector.Record) {
		return func(r *projector.Record) { r.New["path"] = store.Value{Type: store.S, Str: strings.Repeat("x", n)} }
	}
	where := "unit " + unit + ", seq 000000000000000001001, event_id " + bad
	tests := []struct {
		name string
		rec  projector.Record
		want string // the error, "" for none
	}{
		{"record at the limit", at(path(1_024_000 - recordOverhead)), ""},
		{"record one byte over", at(path(1_024_001 - recordOverhead)),
			"firehose: can't archive " + where + ": the record is 1024001 bytes, over Firehose's record limit of 1024000"},
		{"declared column that doesn't decode", at(func(r *projector.Record) { r.New["path"] = store.Value{Type: store.N, Str: "1"} }),
			"firehose: can't archive " + where + ": path: a String value must be a DynamoDB S"},
		{"implicit column that doesn't decode", at(func(r *projector.Record) { delete(r.New, catalog.AttrIngestTime) }),
			"firehose: can't archive " + where + ": ingest_time: a DateTime64(6) value is missing"},
		{"origin_region that doesn't decode", at(func(r *projector.Record) { delete(r.New, catalog.AttrOriginRegion) }),
			"firehose: can't archive " + where + ": origin_region: a String value is missing"},
		{"MODIFY", at(func(r *projector.Record) { r.Op, r.Old = projector.Modify, r.New }),
			"firehose: unit " + unit + ", seq 000000000000000001001: only INSERT records are archived"},
		{"REMOVE", at(func(r *projector.Record) { r.Op, r.Old, r.New = projector.Remove, r.New, nil }),
			"firehose: unit " + unit + ", seq 000000000000000001001: only INSERT records are archived"},
	}
	for _, tt := range tests {
		for _, events := range []string{"all", "local"} {
			t.Run(tt.name+", "+events, func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFake(t, acceptAll)
					w := writer(t, "archive:\n  events: "+events+"\n", f)
					err := archiveFor(t, w, 10*time.Second, []projector.Record{insert(t, 0, id(0), "us-east-1", "/"), tt.rec})
					switch {
					case tt.want == "" && (err != nil || len(f.calls) != 1):
						t.Errorf("got %v after %d calls, want success after 1", err, len(f.calls))
					case tt.want != "" && (err == nil || err.Error() != tt.want || len(f.calls) != 0):
						t.Errorf("got %v after %d calls, want %s after none", err, len(f.calls), tt.want)
					}
				})
			})
		}
	}
}

// A call takes at most 500 records and 4,000,000 bytes of JSON request: each record's
// base64 and 12 bytes, and 64 bytes and the stream name.
func TestArchiveSplitsCalls(t *testing.T) {
	t.Parallel()
	// Records of the most raw bytes, 1,024,000: 1,365,348 bytes each in the request.
	big := 1_024_000 - recordOverhead
	tests := []struct {
		name  string
		paths []int // each record's path length
		want  []int // the records of each call
	}{
		{"500 records", slices.Repeat([]int{1}, 500), []int{500}},
		{"501 records", slices.Repeat([]int{1}, 501), []int{500, 1}},
		// 64 + 20 + 2 × 1,365,348 + (1,269,208 + 12) = 4,000,000.
		{"4,000,000 bytes", []int{big, big, 951_906 - recordOverhead}, []int{3}},
		// One raw byte more takes four more bytes of base64.
		{"4,000,004 bytes", []int{big, big, 951_907 - recordOverhead}, []int{2, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, acceptAll)
				recs := make([]projector.Record, len(tt.paths))
				for i, n := range tt.paths {
					recs[i] = insert(t, i, id(i), "us-east-1", strings.Repeat("x", n))
				}
				if err := archiveFor(t, writer(t, "", f), 10*time.Second, recs); err != nil {
					t.Fatal(err)
				}
				var got []int
				for _, c := range f.calls {
					got = append(got, len(c))
				}
				if !slices.Equal(got, tt.want) {
					t.Errorf("calls of %v records, want %v", got, tt.want)
				}
			})
		})
	}
}

// With archive.events local, a region archives only the events it accepted. That filter,
// and a stream the region doesn't archive, are the only ways to succeed without a call.
func TestArchiveOnlyWhatTheRegionArchives(t *testing.T) {
	t.Parallel()
	const local = "archive:\n  events: local\n"
	tests := []struct {
		name    string
		archive string
		stream  string
		origins []string // each record's origin region
		want    [][]string
	}{
		{"local, mixed", local, "pv", []string{"us-east-1", "us-west-2", "us-east-1"}, [][]string{ids(0, 2)}},
		{"local, replicated only", local, "pv", []string{"us-west-2", "us-west-2"}, nil},
		{"all, replicated only", "", "pv", []string{"us-west-2", "us-west-2"}, [][]string{ids(0, 1)}},
		{"stream not archived", "archive:\n  streams:\n    other:\n      enabled: false\n", "other", []string{"us-east-1"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, acceptAll)
				recs := make([]projector.Record, len(tt.origins))
				for i, r := range tt.origins {
					recs[i] = insert(t, i, id(i), r, "/")
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				if err := writer(t, tt.archive, f).Archive(ctx, tt.stream, unit, recs); err != nil {
					t.Fatal(err)
				}
				if !slices.EqualFunc(f.calls, tt.want, slices.Equal) {
					t.Errorf("calls %q, want %q", f.calls, tt.want)
				}
			})
		})
	}
}

// For random per-record failures, Archive returns nil if and only if Firehose accepted
// every record at least once. The control hides the codes from the writer, as a writer
// that ignores them would, and must break the property.
func TestArchiveNilIffEveryRecordAccepted(t *testing.T) {
	t.Parallel()
	if n := propertyViolations(t, false); n != 0 {
		t.Errorf("%d scripts break the property", n)
	}
	if propertyViolations(t, true) == 0 {
		t.Error("control: a writer that ignores the codes keeps the property")
	}
}

// propertyViolations runs 300 random scripts and returns how many break the property.
func propertyViolations(t *testing.T, hideCodes bool) int {
	t.Helper()
	violations := 0
	synctest.Test(t, func(t *testing.T) {
		for seed := range uint64(300) {
			r := rand.New(rand.NewPCG(seed, 7))
			f := newFake(t, randomAnswer(r))
			var s firehose.Sender = f
			if hideCodes {
				s = codesHidden{f}
			}
			n := 1 + r.IntN(30)
			err := archiveFor(t, writer(t, "", s), time.Duration(1+r.IntN(20))*time.Second, inserts(t, n))
			if (err == nil) != (len(f.accepted) == n) {
				violations++
			}
		}
	})
	return violations
}

// randomAnswer fails each record at a random rate, and one call in 20 as a whole or with
// a response one entry short.
func randomAnswer(r *rand.Rand) func(int, [][]byte) ([]string, error) {
	failRate := r.Float64()
	return func(_ int, recs [][]byte) ([]string, error) {
		codes := make([]string, len(recs))
		for i := range codes {
			if r.Float64() < failRate {
				codes[i] = "ServiceUnavailableException"
			}
		}
		switch r.IntN(20) {
		case 0:
			return nil, errors.New("InternalFailure")
		case 1:
			return codes[1:], nil
		}
		return codes, nil
	}
}

// codesHidden reports every record of a call that didn't fail as accepted.
type codesHidden struct{ s firehose.Sender }

func (c codesHidden) PutBatch(ctx context.Context, stream string, recs [][]byte) ([]string, error) {
	if _, err := c.s.PutBatch(ctx, stream, recs); err != nil {
		return nil, err
	}
	return make([]string, len(recs)), nil
}

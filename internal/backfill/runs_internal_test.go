// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// testRecords returns n records in order, with an item of every attribute type, the zero
// Value a foreign writer leaves, and empty B, L and M.
func testRecords(n int) []record {
	recs := make([]record, n)
	for i := range recs {
		recs[i] = record{
			mark: keys.Mark{UnitKey: keys.UnitKey(fmt.Sprintf("u%d", i/7)), Seq: fmt.Sprintf("%040d", i+1)},
			unit: fmt.Sprintf("u%d", i/7),
			seq:  fmt.Sprintf("%040d", i+1),
			item: store.Item{
				"s":  {Type: store.S, Str: fmt.Sprintf("<%d>\n", i)},
				"n":  {Type: store.N, Str: "-1.5e-07"},
				"b":  {Type: store.B, Bin: []byte{0, byte(i)}},
				"eb": {Type: store.B, Bin: []byte{}},
				"t":  {Type: store.BOOL, Bool: i%2 == 0},
				"l":  {Type: store.L, List: []store.Value{{Type: store.S, Str: "x"}, {}}},
				"el": {Type: store.L, List: []store.Value{}},
				"m":  {Type: store.M, Map: map[string]store.Value{"k": {Type: store.N, Str: "1"}}},
				"em": {Type: store.M, Map: map[string]store.Value{}},
				"z":  {},
			},
		}
	}
	slices.SortFunc(recs, func(a, b record) int { return compareRecords(&a, &b) })
	return recs
}

// writeRun stores recs as a run of blocks of block bytes.
func writeRun(t *testing.T, runs *MemRuns, name string, recs []record, block int) meta.Run {
	t.Helper()
	var run meta.Run
	err := runs.Put(t.Context(), name, func(out io.Writer) error {
		w := &runWriter{w: out, block: block}
		for i := range recs {
			if err := w.add(&recs[i]); err != nil {
				return err
			}
		}
		var err error
		run, _, err = w.close(name)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func readAll(ctx context.Context, runs Runs, name string, after *keys.Mark) ([]record, error) {
	r, err := openRun(ctx, runs, name, after)
	if err != nil {
		return nil, err
	}
	var out []record
	for {
		rec, ok, perr := r.peek(ctx)
		if perr != nil || !ok {
			return out, perr
		}
		out = append(out, *rec)
		r.recs = r.recs[1:]
	}
}

// A run reads back as written, and from any mark on, across blocks.
func TestRunRoundTrip(t *testing.T) {
	t.Parallel()
	recs := testRecords(40)
	runs := NewMemRuns()
	run := writeRun(t, runs, "r", recs, 600)
	if run.Records != 40 || run.First != recs[0].mark || run.Last != recs[39].mark {
		t.Errorf("run %+v, want 40 records from %v to %v", run, recs[0].mark, recs[39].mark)
	}
	for i := -1; i < len(recs); i++ {
		var after *keys.Mark
		if i >= 0 {
			after = &recs[i].mark
		}
		got, err := readAll(t.Context(), runs, "r", after)
		if err != nil {
			t.Fatal(err)
		}
		want := recs[i+1:]
		if len(got) != len(want) {
			t.Fatalf("after record %d: %d records, want %d", i, len(got), len(want))
		}
		for k := range want {
			got[k].size = 0
			if !reflect.DeepEqual(got[k], want[k]) {
				t.Fatalf("after record %d: record %d is %+v, want %+v", i, k, got[k], want[k])
			}
		}
	}
}

// A reader of any bytes fails or reads records; it never panics or reads past its limits.
func FuzzRunReader(f *testing.F) {
	runs := NewMemRuns()
	writeRunF := func(recs []record) []byte {
		var b bytes.Buffer
		w := &runWriter{w: &b, block: 300}
		for i := range recs {
			if err := w.add(&recs[i]); err != nil {
				f.Fatal(err)
			}
		}
		if _, _, err := w.close("r"); err != nil {
			f.Fatal(err)
		}
		return b.Bytes()
	}
	valid := writeRunF(testRecords(5))
	f.Add(valid)
	f.Add(valid[len(valid)/2:])
	f.Add([]byte(runMagic))
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := runs.Put(t.Context(), "f", func(w io.Writer) error { _, err := w.Write(data); return err }); err != nil {
			t.Fatal(err)
		}
		// Any outcome but a panic is fine.
		_, _ = readAll(t.Context(), runs, "f", nil)
	})
}

// The sort buffer counts a record by its encoded bytes; decoded, as staging holds it, a stream
// item takes less than decodedFactor times as much heap.
func TestDecodedFactorCoversStreamItems(t *testing.T) { //nolint:paralleltest // it measures the heap, which parallel tests would grow
	const n = 5000
	lines := streamItems(t, n)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	buf, encoded := make([]record, 0, len(lines)), 0
	for _, line := range lines {
		var m map[string]store.JSONValue
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatal(err)
		}
		it, err := store.DecodeJSON(m)
		if err != nil {
			t.Fatal(err)
		}
		rec, keep, f := tableRecord(it, catalog.TimeLimitMicros)
		if f != nil || !keep {
			t.Fatalf("item kept %v, %+v", keep, f)
		}
		buf, encoded = append(buf, rec), encoded+rec.size
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	runtime.KeepAlive(buf)
	if len(buf) != n || held >= decodedFactor*int64(encoded) {
		t.Errorf("%d records of %d encoded bytes hold %d bytes of heap, %.1f times; want %d records, under %d times",
			len(buf), encoded, held, float64(held)/float64(encoded), n, decodedFactor)
	}
}

// streamItems appends n events and returns their stream items in DynamoDB's JSON, which a
// reader decodes into values of its own, as staging does.
func streamItems(t *testing.T, n int) [][]byte {
	t.Helper()
	data := catalogtest.Marshal(t, "shop", `
CREATE STREAM events (account String, n UInt32, note String) PRIMARY KEY (account) LOG RETENTION 1 DAY;
CREATE MATERIALIZED VIEW totals ORDER BY (account) AS SELECT account, count() AS events FROM events GROUP BY account;`)
	cat, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	b := mem.New()
	e := engine.New(b, engine.Options{Region: "us-east-1", Entropy: rand.NewChaCha8([32]byte{})})
	for i := range n {
		row := []expr.Value{expr.String("account-" + strconv.Itoa(i%97)), expr.Uint(uint64(i)), expr.String("a note"), expr.Null()}
		res, aerr := e.Append(t.Context(), cat, "events", [][]expr.Value{row}, engine.AppendMode{})
		if aerr == nil {
			aerr = res[0].Err
		}
		if aerr != nil {
			t.Fatal(aerr)
		}
	}
	var lines [][]byte
	for start := (*store.Key)(nil); ; {
		items, next, serr := b.ScanPage(t.Context(), catalog.Naming{Store: "shop"}.StreamTable("events"), store.Scan{Segments: 1}, start)
		if serr != nil {
			t.Fatal(serr)
		}
		for _, it := range items {
			line, merr := json.Marshal(store.EncodeJSON(it))
			if merr != nil {
				t.Fatal(merr)
			}
			lines = append(lines, line)
		}
		if start = next; start == nil {
			return lines
		}
	}
}

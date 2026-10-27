// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package archive_test

import (
	"bytes"
	"encoding/json"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/txtar"
)

// The bookkeeping values of every golden record.
const (
	unit           = "shardId-00000001761475200000-1a2b3c4d"
	paddedSeq      = "0000000000000000000049668893422340000000"
	arrivalSeconds = 1761475200 // 2025-10-26T10:40:00Z
)

// The writer and the DDL can't drift: a record's keys are its table's columns, in order,
// less those whose value is NULL.
func TestRecordKeysFollowColumns(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/*.txtar")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, path := range paths {
		checked += checkRecordKeys(t, path)
	}
	if checked == 0 {
		t.Fatal("no events in testdata")
	}
}

// checkRecordKeys checks the keys of the records of the events in the archive at path, and
// returns how many it checked.
func checkRecordKeys(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ar := txtar.Parse(data)
	cat := project(t, ar)
	tables := archive.SchemaFor(cat, "us-east-1").Tables
	checked := 0
	for stream, events := range readEvents(t, cat, ar) {
		i := slices.IndexFunc(tables, func(tb archive.Table) bool { return tb.Name == stream.Name })
		if i < 0 {
			t.Fatalf("%s: stream %s isn't archived", path, stream.Name)
		}
		cols := eventColumns(stream)
		for _, vals := range events {
			var want []string
			for _, c := range tables[i].Columns {
				j := slices.IndexFunc(cols, func(e catalog.Column) bool { return e.Name == c.Name })
				if j < 0 || !vals[j].IsNull() {
					want = append(want, c.Name)
				}
			}
			rec := archive.NewEncoder(stream).AppendRecord(nil, vals, unit, paddedSeq, time.Unix(arrivalSeconds, 0))
			if got := recordKeys(t, rec); !slices.Equal(got, want) {
				t.Errorf("%s: record %s\nhas keys %q\nwant %q", path, rec, got, want)
			}
			checked++
		}
	}
	return checked
}

// Strings are written as encoding/json writes them with HTML escaping off, whatever their
// bytes, and a record without an arrival time has no _arrival key.
func TestRecordStringsAsEncodingJSON(t *testing.T) {
	t.Parallel()
	enc := archive.NewEncoder(&load(t, "CREATE STREAM s (k String) PRIMARY KEY (k);").Streams[0])
	pieces := []string{"a", "\u00e9", "\U0001F600", "\u2028", "\u2029", "<", ">", "&", `"`, `\`, "\ufffd", "\xed\xa0\x80", "\xe2\x82"}
	r := rand.New(rand.NewPCG(1, 2))
	cases := []string{""}
	for b := range 256 {
		cases = append(cases, string([]byte{byte(b)}))
	}
	for range 2000 {
		var s strings.Builder
		for range r.IntN(10) {
			if r.IntN(2) == 0 {
				s.WriteByte(byte(r.IntN(256)))
			} else {
				s.WriteString(pieces[r.IntN(len(pieces))])
			}
		}
		cases = append(cases, s.String())
	}
	for _, s := range cases {
		vals := []expr.Value{expr.String(s), expr.Null(), expr.Null(), expr.Null(), expr.Null()}
		got := enc.AppendRecord(nil, vals, "u", "1", time.Time{})
		var b bytes.Buffer
		std := json.NewEncoder(&b)
		std.SetEscapeHTML(false)
		if err := std.Encode(s); err != nil {
			t.Fatal(err)
		}
		if want := `{"k":` + strings.TrimSuffix(b.String(), "\n") + `,"_shard":"u","_seq":"1"}`; string(got) != want {
			t.Errorf("string %q: got %s, want %s", s, got, want)
		}
	}
}

// Float64 values are written as encoding/json writes a float64: the shortest text that
// reads back as the value, in exponent form below 1e-6 and from 1e21.
func TestRecordFloatsAsEncodingJSON(t *testing.T) {
	t.Parallel()
	enc := archive.NewEncoder(&load(t, "CREATE STREAM s (k String, f Float64) PRIMARY KEY (k);").Streams[0])
	cases := []float64{
		0, 1, -1, 0.1, 1.5, 1e-6, 9.999999e-7, 1e-7, -1.25e-9, 1e20, 1e21, -1e21, 123456789012345680000,
		1e-130, 9.999999999999999e125, math.SmallestNonzeroFloat64, math.MaxFloat64,
	}
	r := rand.New(rand.NewPCG(3, 4))
	for len(cases) < 5000 {
		if f := math.Float64frombits(r.Uint64()); !math.IsNaN(f) && !math.IsInf(f, 0) {
			cases = append(cases, f)
		}
	}
	for _, f := range cases {
		v := expr.Float(f)
		got := enc.AppendRecord(nil, []expr.Value{expr.String("k"), v, expr.Null(), expr.Null(), expr.Null(), expr.Null()}, "u", "1", time.Time{})
		num, err := json.Marshal(v.AsFloat())
		if err != nil {
			t.Fatal(err)
		}
		if want := `{"k":"k","f":` + string(num) + `,"_shard":"u","_seq":"1"}`; string(got) != want {
			t.Errorf("%v: got %s, want %s", f, got, want)
		}
	}
}

// MaxSize is the size of the record with the longest bookkeeping values a stream record
// can give: a 65-character unit ID, 40 sequence digits and a 16-digit arrival time.
func TestMaxSize(t *testing.T) {
	t.Parallel()
	enc := archive.NewEncoder(&load(t, "CREATE STREAM s (k String) PRIMARY KEY (k);").Streams[0])
	vals := []expr.Value{
		expr.String("a"), expr.String("01JB8Z9X4Q7M3N2P5R6S8T0V1W"),
		expr.Micros(1761475200123456), expr.Micros(1761475200130000), expr.String("us-east-1"),
	}
	// {"event_id":"<26>","event_time":<16>,"ingest_time":<16>,"origin_region":"us-east-1",
	// "k":"a","_shard":"<65>","_seq":"<40>","_arrival":<16>}
	const want = 1 + 39 + 30 + 31 + 28 + 8 + 77 + 50 + 28 + 1
	if got := enc.MaxSize(vals); got != want {
		t.Errorf("MaxSize %d, want %d", got, want)
	}
}

// The projector's archive guard (ADR 0008): the variable is required exactly when the
// catalog archives in the region, and must equal the catalog's fingerprint there.
func TestCheckSchema(t *testing.T) {
	t.Parallel()
	const (
		sql = `
CREATE STREAM page_views (path String, user_id UInt64) PRIMARY KEY (path);
CREATE STREAM page (path String, clicks UInt32 DEFAULT 0) PRIMARY KEY (path);
`
		fingerprint = "sha256:7b5ea2253bbad04b16e0bb658cd4b62d42a4ccc36c69f85f00046c4699b284f5"
		other       = "sha256:45f6552f8517645b886d25160f7935352403cca52afc2b255bcaac25b2ad9ad0"
	)
	archives := load(t, sql)
	none := loadConfig(t, "store: analytics\nregions: [us-east-1]\narchive:\n  enabled: false\n", sql)
	elsewhere := loadConfig(t, "store: analytics\nregions: [us-east-1, us-west-2]\narchive:\n  regions: [us-west-2]\n", sql)
	tests := []struct {
		name string
		cat  *catalog.Catalog
		env  string
		want string // the error, "" for none
	}{
		{"no archive, unset", none, "", ""},
		{"no archive, set", none, fingerprint,
			"archive: DYNAFLUX_ARCHIVE_SCHEMA is set, but the projector's catalog archives nothing in us-east-1"},
		{"archived in another region, unset", elsewhere, "", ""},
		{"archived in another region, set", elsewhere, fingerprint,
			"archive: DYNAFLUX_ARCHIVE_SCHEMA is set, but the projector's catalog archives nothing in us-east-1"},
		{"archives, unset", archives, "",
			"archive: DYNAFLUX_ARCHIVE_SCHEMA is missing, but the projector's catalog archives in us-east-1: deploy regional.yaml from the same dynaflux generate as the function"},
		{"archives, another schema", archives, other,
			"archive: DYNAFLUX_ARCHIVE_SCHEMA is " + other + ", but the projector's catalog gives " + fingerprint +
				": the archive schema migration or the function's code is out of date"},
		{"archives, same schema", archives, fingerprint, ""},
	}
	for _, tt := range tests {
		err := archive.CheckSchema(tt.cat, "us-east-1", tt.env)
		if got := errText(err); got != tt.want {
			t.Errorf("%s: got %q\nwant %q", tt.name, got, tt.want)
		}
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// readEvents reads the events/<stream> sections of ar. Each line is an event: a JSON array
// of the stream's declared values, then event_id, event_time, ingest_time and
// origin_region, the order of projector.Stream.Decode. A value is null, its canonical
// literal as a JSON string, or for an array, a JSON array of them.
func readEvents(t *testing.T, cat *catalog.Catalog, ar *txtar.Archive) map[*catalog.Stream][][]expr.Value {
	t.Helper()
	out := map[*catalog.Stream][][]expr.Value{}
	for _, f := range ar.Files {
		name, ok := strings.CutPrefix(f.Name, eventsPrefix)
		if !ok {
			continue
		}
		i := slices.IndexFunc(cat.Streams, func(s catalog.Stream) bool { return s.Name == name })
		if i < 0 {
			t.Fatalf("%s: no stream %s", f.Name, name)
		}
		s := &cat.Streams[i]
		cols := eventColumns(s)
		for line := range strings.Lines(string(f.Data)) {
			var raw []json.RawMessage
			if err := json.Unmarshal([]byte(line), &raw); err != nil || len(raw) != len(cols) {
				t.Fatalf("%s: %q isn't a JSON array of %d values: %v", f.Name, line, len(cols), err)
			}
			vals := make([]expr.Value, len(raw))
			for j, r := range raw {
				vals[j] = literal(t, cols[j].Type, r)
			}
			out[s] = append(out[s], vals)
		}
	}
	return out
}

// eventColumns returns the columns of an event's values, in projector.Stream.Decode's order.
func eventColumns(s *catalog.Stream) []catalog.Column {
	return slices.Concat(s.Columns, catalog.ImplicitColumns())
}

func literal(t *testing.T, typ catalog.Type, raw json.RawMessage) expr.Value {
	t.Helper()
	if string(raw) == "null" {
		return expr.Null()
	}
	scalar := func(text string) expr.Value {
		v, err := expr.Literal(catalog.Type{Kind: typ.Kind, Precision: typ.Precision, Scale: typ.Scale}, text)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if !typ.Array {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			t.Fatalf("%s value %s: %v", typ, raw, err)
		}
		return scalar(text)
	}
	var texts []string
	if err := json.Unmarshal(raw, &texts); err != nil {
		t.Fatalf("%s value %s: %v", typ, raw, err)
	}
	vs := make([]expr.Value, len(texts))
	for i, text := range texts {
		vs[i] = scalar(text)
	}
	return expr.Array(vs)
}

// recordKeys returns the keys of a JSON object, in order. It fails the test unless rec is
// one JSON object.
func recordKeys(t *testing.T, rec []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(rec))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("record %s isn't a JSON object: %v", rec, err)
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("record %s: %v", rec, err)
		}
		var v json.RawMessage
		if err = dec.Decode(&v); err != nil {
			t.Fatalf("record %s: %v", rec, err)
		}
		keys = append(keys, tok.(string))
	}
	if _, err := dec.Token(); err != nil || dec.More() {
		t.Fatalf("record %s isn't one JSON object: %v", rec, err)
	}
	return keys
}

func loadConfig(t *testing.T, config, sql string) *catalog.Catalog {
	t.Helper()
	return loadProject(t, fstest.MapFS{
		"dynaflux.yaml":  {Data: []byte(config)},
		"dynaflux/s.sql": {Data: []byte(sql)},
	})
}

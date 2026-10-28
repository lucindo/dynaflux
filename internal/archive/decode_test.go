// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package archive_test

import (
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/txtar"
)

// Every value of every type that a record holds reads back as itself, with the record's unit
// and sequence number.
func TestDecodeInvertsEncode(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/*.txtar")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, path := range paths {
		checked += checkRoundTrips(t, path)
	}
	if checked == 0 {
		t.Fatal("no events in testdata")
	}
}

// checkRoundTrips decodes the record of each event of the archive at path, and returns how many
// it checked.
func checkRoundTrips(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ar := txtar.Parse(data)
	cat := project(t, ar)
	compiled, err := projector.Compile(cat)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for stream, events := range readEvents(t, cat, ar) {
		ps, _ := compiled.Stream(stream.Name)
		for _, vals := range events {
			rec := archive.NewEncoder(stream).AppendRecord(nil, vals, unit, paddedSeq, time.Unix(arrivalSeconds, 0))
			r, derr := archive.NewDecoder(stream).Decode(rec)
			if derr != nil {
				t.Fatalf("%s: %s: %v", path, rec, derr)
			}
			if r.Unit != unit || r.Seq != paddedSeq || !r.Arrival.Equal(time.Unix(arrivalSeconds, 0)) {
				t.Errorf("%s: unit %q seq %q arrival %v, want %q %q %v", path, r.Unit, r.Seq, r.Arrival, unit, paddedSeq, time.Unix(arrivalSeconds, 0))
			}
			got, bad := ps.Decode(r.Item)
			if bad != nil {
				t.Fatalf("%s: item %v doesn't decode: %v", path, r.Item, bad)
			}
			checkSameValues(t, eventColumns(stream), got, vals)
			checked++
		}
	}
	return checked
}

func checkSameValues(t *testing.T, cols []catalog.Column, got, want []expr.Value) {
	t.Helper()
	for i, c := range cols {
		g, gok, gerr := keys.EncodeValue(c.Type, got[i])
		w, wok, werr := keys.EncodeValue(c.Type, want[i])
		if gok != wok || gerr != nil || werr != nil || !reflect.DeepEqual(g, w) {
			t.Errorf("column %s: got %v (%v), want %v (%v)", c.Name, g, gerr, w, werr)
		}
	}
}

// A record of an event that the engine appended decodes to the item the engine stored, keys
// and shard included, but for _ttl.
func TestDecodeGivesTheStoredItem(t *testing.T) {
	t.Parallel()
	cat := load(t, `CREATE STREAM s (site String, n Nullable(Int64), price Decimal(10, 2), tags Array(String))
PRIMARY KEY (site) SHARDS 4;`)
	data, err := catalog.Marshal(cat)
	if err != nil {
		t.Fatal(err)
	}
	ec, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	b := mem.New()
	e := engine.New(b, engine.Options{Region: "us-east-1"})
	res, err := e.Append(t.Context(), ec, "s", [][]expr.Value{{
		expr.String("s1"), expr.Null(), expr.Decimal(bigInt(t, "-1250")),
		expr.Array([]expr.Value{expr.String("a"), expr.String("<b>")}), expr.Null(),
	}}, engine.AppendMode{})
	if err == nil {
		err = res[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	want, err := b.Get(t.Context(), catalog.Naming{Store: cat.Store}.StreamTable("s"), res[0].Key, true)
	if err != nil {
		t.Fatal(err)
	}
	ps, _ := ec.Projector().Stream("s")
	decoded, bad := ps.Decode(want)
	if bad != nil {
		t.Fatal(bad)
	}
	rec := archive.NewEncoder(&cat.Streams[0]).AppendRecord(nil, decoded, unit, paddedSeq, time.Time{})
	r, err := archive.NewDecoder(&cat.Streams[0]).Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	decodedItem := r.Item
	// Stored as DynamoDB stores it, which writes numbers in a form of its own.
	if err = b.Put(t.Context(), "decoded", decodedItem, store.Cond{}); err != nil {
		t.Fatal(err)
	}
	got, err := b.Get(t.Context(), "decoded", res[0].Key, true)
	if err != nil {
		t.Fatal(err)
	}
	delete(want, catalog.AttrTTL)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("record %s decodes to\n%v\nwant\n%v", rec, got, want)
	}
}

// A record that isn't one of the stream's fails, naming what is wrong; a null is NULL.
func TestDecodeRefuses(t *testing.T) {
	t.Parallel()
	s := &load(t, "CREATE STREAM s (k String, n Nullable(UInt8)) PRIMARY KEY (k);").Streams[0]
	const head = `{"event_id":"01JB8Z9X4Q7M3N2P5R6S8T0V20","event_time":1761475200123456,"ingest_time":1761475200130000,"origin_region":"us-east-1",`
	tests := []struct {
		name, rec, want string
	}{
		{"null is NULL", head + `"k":"a","n":null,"_shard":"u","_seq":"49668893422340000000001"}`, ""},
		{"not JSON", `{"k":`, "archive: record of s: unexpected end of JSON input"},
		{"unknown key", head + `"k":"a","x":1,"_shard":"u","_seq":"49668893422340000000001"}`, `archive: record of s: "x" isn't a column`},
		{"value out of range", head + `"k":"a","n":256,"_shard":"u","_seq":"49668893422340000000001"}`,
			"archive: record of s: n: UInt8 value 256 is out of range"},
		{"string for a number", head + `"k":"a","n":"x","_shard":"u","_seq":"49668893422340000000001"}`,
			`archive: record of s: n: json: invalid number literal, trying to unmarshal "\"x\"" into Number`},
		{"no unit", head + `"k":"a","_seq":"49668893422340000000001"}`, "archive: record of s: no _shard"},
		{"short seq", head + `"k":"a","_shard":"u","_seq":"12"}`,
			`archive: record of s: _seq: sequence number "12" is not 21 to 40 decimal digits above zero`},
		{"no key column", head + `"_shard":"u","_seq":"49668893422340000000001"}`, "archive: record of s: no k, a PRIMARY KEY column"},
		{"no event_time", `{"event_id":"01JB8Z9X4Q7M3N2P5R6S8T0V20","ingest_time":1,"origin_region":"r","k":"a","_shard":"u","_seq":"49668893422340000000001"}`,
			"archive: record of s: no event_time"},
	}
	for _, tt := range tests {
		_, err := archive.NewDecoder(s).Decode([]byte(tt.rec))
		if got := errText(err); got != tt.want {
			t.Errorf("%s: got %q\nwant %q", tt.name, got, tt.want)
		}
	}
}

func bigInt(t *testing.T, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("%q isn't an integer", s)
	}
	return n
}

// A row of an export, in the typed projection's forms (64-bit integers, decimals, dates and
// times as text; no _arrival) and with JSON escapes of its own, decodes to the item that the
// encoder's record of the same event decodes to.
func TestDecodeReadsExports(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/*.txtar")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, path := range paths {
		checked += checkExportRows(t, path)
	}
	if checked == 0 {
		t.Fatal("no export rows in testdata")
	}
}

// checkExportRows decodes the rows of the unload/ sections of the archive at path, each with
// the record of the event on the same line of its events/ section, and returns how many it
// checked.
func checkExportRows(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ar := txtar.Parse(data)
	cat := project(t, ar)
	events := readEvents(t, cat, ar)
	checked := 0
	for _, f := range ar.Files {
		name, ok := strings.CutPrefix(f.Name, unloadPrefix)
		if !ok {
			continue
		}
		stream, _ := cat.Stream(name)
		rows := slices.Collect(strings.Lines(string(f.Data)))
		if len(rows) != len(events[stream]) {
			t.Fatalf("%s: %d rows for %d events", f.Name, len(rows), len(events[stream]))
		}
		dec := archive.NewDecoder(stream)
		for i, row := range rows {
			rec := archive.NewEncoder(stream).AppendRecord(nil, events[stream][i], unit, paddedSeq, time.Unix(arrivalSeconds, 0))
			want, werr := dec.Decode(rec)
			got, gerr := dec.Decode([]byte(row))
			if werr != nil || gerr != nil {
				t.Fatalf("%s row %d: %v; the record: %v", f.Name, i+1, gerr, werr)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s row %d decodes to %+v\nwant %+v", f.Name, i+1, got, want)
			}
			checked++
		}
	}
	return checked
}

// Each form a value may take in an export, alone in a record: the attribute it gives, or the
// error.
func TestDecodeExportForms(t *testing.T) {
	t.Parallel()
	s := &load(t, `CREATE STREAM s (k String, t DateTime64(6), ms DateTime64(3), i Int64, u UInt32,
d Decimal(10, 2), n Nullable(Int64), at Array(DateTime64(6))) PRIMARY KEY (k);`).Streams[0]
	const head = `{"event_id":"01JB8Z9X4Q7M3N2P5R6S8T0V20","event_time":"2025-10-26 10:40:00.123456",` +
		`"ingest_time":"2025-10-26 10:40:00.130000","origin_region":"us-east-1","k":"a","_shard":"u","_seq":"49668893422340000000001"`
	tests := []struct {
		field string // a key and its JSON value
		want  string // the attribute's text, "absent", or the error
	}{
		{`"t":"2024-05-31 13:45:00"`, "1717163100000000"},
		{`"t":"2024-05-31 13:45:00.1"`, "1717163100100000"},
		{`"t":"2024-05-31 13:45:00.000123"`, "1717163100000123"},
		{`"t":"2024-05-31 13:45:00.999999"`, "1717163100999999"},
		{`"t":1717163100000123`, "1717163100000123"},
		{`"t":"2024-05-31 13:45:00.1234560"`, `archive: record of s: t: "2024-05-31 13:45:00.1234560" is not YYYY-MM-DD HH:MM:SS with 0 to 6 fractional digits`},
		{`"t":"2024-05-31 13:45:00."`, `archive: record of s: t: "2024-05-31 13:45:00." is not YYYY-MM-DD HH:MM:SS with 0 to 6 fractional digits`},
		{`"t":"2024-05-31T13:45:00"`, `archive: record of s: t: "2024-05-31T13:45:00" is not YYYY-MM-DD HH:MM:SS with 0 to 6 fractional digits`},
		{`"t":"2024-05-31 13:45:00,5"`, `archive: record of s: t: "2024-05-31 13:45:00,5" is not YYYY-MM-DD HH:MM:SS with 0 to 6 fractional digits`},
		{`"t":"1717163100000123"`, `archive: record of s: t: "1717163100000123" is not YYYY-MM-DD HH:MM:SS with 0 to 6 fractional digits`},
		{`"t":"1969-12-31 23:59:59.999999"`, "archive: record of s: t: DateTime64(6) value -1 (microseconds) is outside 1970 to 2106"},
		{`"ms":"2024-05-31 13:45:00.123000"`, "1717163100123000"},
		{`"ms":"2024-05-31 13:45:00.1234"`, "archive: record of s: ms: DateTime64(3) value 1717163100123400 (microseconds) has more precision than the type"},
		{`"at":["2024-05-31 13:45:00.5",1717163100000000]`, "[1717163100500000 1717163100000000]"},
		{`"i":"-9223372036854775808"`, "-9223372036854775808"},
		{`"i":-42`, "-42"},
		{`"i":"9223372036854775808"`, "archive: record of s: i: Int64 value 9223372036854775808 is out of range"},
		{`"u":"4294967295"`, "4294967295"},
		{`"u":4294967295`, "4294967295"},
		{`"d":"-0.05"`, "-0.05"},
		{`"d":"12.5"`, "12.50"},
		{`"d":"12"`, "12.00"},
		{`"d":12.5`, "12.50"},
		{`"d":"12.500"`, "archive: record of s: d: 12.500 has more fractional digits than Decimal(10, 2)"},
		{`"n":null`, "absent"},
		{`"n":"7"`, "7"},
	}
	for _, tt := range tests {
		r, err := archive.NewDecoder(s).Decode([]byte(head + "," + tt.field + "}"))
		got := errText(err)
		if err == nil {
			name, _, _ := strings.Cut(strings.TrimPrefix(tt.field, `"`), `"`)
			got = attrText(r.Item, name)
		}
		if got != tt.want {
			t.Errorf("%s: got %q\nwant %q", tt.field, got, tt.want)
		}
	}
	// A missing key is NULL, as null is.
	missing, err := archive.NewDecoder(s).Decode([]byte(head + "}"))
	null, nerr := archive.NewDecoder(s).Decode([]byte(head + `,"n":null}`))
	if err != nil || nerr != nil || !reflect.DeepEqual(missing, null) {
		t.Errorf("missing n gives %v (%v), null gives %v (%v)", missing, err, null, nerr)
	}
}

// attrText returns the text of it's attribute name, the texts of its elements in brackets, or
// "absent".
func attrText(it store.Item, name string) string {
	a, ok := it[name]
	switch {
	case !ok:
		return "absent"
	case a.Type == store.L:
		texts := make([]string, len(a.List))
		for i, e := range a.List {
			texts[i] = e.Str
		}
		return "[" + strings.Join(texts, " ") + "]"
	}
	return a.Str
}

// The event_id that the engine gives a DEDUP BY event passes the check, and fails it once a
// value that it hashes changes on the way through the archive.
func TestCheckEventID(t *testing.T) {
	t.Parallel()
	cat := load(t, "CREATE STREAM s (site String, amount Decimal(10, 2), request_id UInt64) PRIMARY KEY (site) DEDUP BY (request_id);")
	s := &cat.Streams[0]
	vals, id := appendOne(t, cat, []expr.Value{expr.String("s1"), expr.Decimal(big.NewInt(1250)), expr.Uint(7), expr.Micros(1761475200123456)})
	const eventTime, requestID, amount = 4, 2, 1 // indexes in vals
	changed := func(i int, v expr.Value) []expr.Value {
		c := slices.Clone(vals)
		c[i] = v
		return c
	}
	mismatch := "event_id " + id + " isn't "
	tests := []struct {
		name string
		vals []expr.Value
		want string // a prefix of the error; "" for none
	}{
		{"as appended", vals, ""},
		{"a value it doesn't hash", changed(amount, expr.Decimal(big.NewInt(1))), ""},
		{"event_time a microsecond later", changed(eventTime, expr.Micros(1761475200123457)), mismatch},
		{"event_time truncated to milliseconds", changed(eventTime, expr.Micros(1761475200123000)), mismatch},
		{"a DEDUP BY value", changed(requestID, expr.Uint(8)), mismatch},
		{"a DEDUP BY value missing", changed(requestID, expr.Null()), "event_id: key field 3: a key value is NULL"},
		{"too few values", vals[:3], "event_id: 3 values for the 7 columns of s"},
	}
	for _, tt := range tests {
		got := errText(archive.CheckEventID(s, tt.vals, id))
		if tt.want == "" && got != "" || !strings.HasPrefix(got, tt.want) {
			t.Errorf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
	// The decoder checks every record of the stream.
	rec := archive.NewEncoder(s).AppendRecord(nil, changed(eventTime, expr.Micros(1761475200123457)), unit, paddedSeq, time.Time{})
	if _, err := archive.NewDecoder(s).Decode(rec); !strings.HasPrefix(errText(err), "archive: record of s: "+mismatch) {
		t.Errorf("a record with a changed event_time decodes with %v, want the mismatch", err)
	}
	// Without DEDUP BY, an event_id is random: nothing to check.
	plain := &load(t, "CREATE STREAM p (site String) PRIMARY KEY (site);").Streams[0]
	if err := archive.CheckEventID(plain, []expr.Value{expr.String("s1"), expr.String(id), expr.Micros(0), expr.Micros(0), expr.String("r")}, "x"); err != nil {
		t.Errorf("a stream without DEDUP BY: %v", err)
	}
}

// appendOne appends event, the declared values then event_time, to the only stream of cat
// through the engine, and returns the stored event's values in Encoder.AppendRecord's order
// and its event_id.
func appendOne(t *testing.T, cat *catalog.Catalog, event []expr.Value) ([]expr.Value, string) {
	t.Helper()
	data, err := catalog.Marshal(cat)
	if err != nil {
		t.Fatal(err)
	}
	ec, err := engine.Compile(data)
	if err != nil {
		t.Fatal(err)
	}
	b := mem.New()
	name := cat.Streams[0].Name
	res, err := engine.New(b, engine.Options{Region: "us-east-1"}).Append(t.Context(), ec, name, [][]expr.Value{event}, engine.AppendMode{Import: true})
	if err == nil {
		err = res[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	it, err := b.Get(t.Context(), catalog.Naming{Store: cat.Store}.StreamTable(name), res[0].Key, true)
	if err != nil {
		t.Fatal(err)
	}
	ps, _ := ec.Projector().Stream(name)
	vals, bad := ps.Decode(it)
	if bad != nil {
		t.Fatal(bad)
	}
	_, id, err := keys.ParseStreamSK(res[0].Key.SK)
	if err != nil {
		t.Fatal(err)
	}
	return vals, id
}

// The decoder fails or returns an item for any bytes, and an item it returns, which the
// encoder writes again, decodes to itself.
func FuzzDecode(f *testing.F) {
	data, err := os.ReadFile("testdata/types.txtar")
	if err != nil {
		f.Fatal(err)
	}
	ar := txtar.Parse(data)
	cat := project(f, ar)
	s := &cat.Streams[0]
	compiled, err := projector.Compile(cat)
	if err != nil {
		f.Fatal(err)
	}
	ps, _ := compiled.Stream(s.Name)
	for _, sf := range ar.Files {
		if sf.Name == wantPrefix+s.Name+".records" || sf.Name == unloadPrefix+s.Name {
			for line := range strings.Lines(string(sf.Data)) {
				f.Add([]byte(line))
			}
		}
	}
	dec, enc := archive.NewDecoder(s), archive.NewEncoder(s)
	f.Fuzz(func(t *testing.T, rec []byte) {
		r, derr := dec.Decode(rec)
		if derr != nil {
			return
		}
		vals, bad := ps.Decode(r.Item)
		if bad != nil {
			return // a column without a value or DEFAULT
		}
		again := enc.AppendRecord(nil, vals, r.Unit, r.Seq, r.Arrival)
		back, aerr := dec.Decode(again)
		if aerr != nil || !reflect.DeepEqual(back, r) {
			t.Fatalf("%s decodes to %+v, which the encoder writes as %s, which decodes to %+v, %v", rec, r, again, back, aerr)
		}
	})
}

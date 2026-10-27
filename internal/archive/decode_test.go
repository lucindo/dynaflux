// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package archive_test

import (
	"math/big"
	"os"
	"path/filepath"
	"reflect"
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
			gotUnit, gotSeq, it, derr := archive.NewDecoder(stream).Decode(rec)
			if derr != nil {
				t.Fatalf("%s: %s: %v", path, rec, derr)
			}
			if gotUnit != unit || gotSeq != paddedSeq {
				t.Errorf("%s: unit %q seq %q, want %q %q", path, gotUnit, gotSeq, unit, paddedSeq)
			}
			got, bad := ps.Decode(it)
			if bad != nil {
				t.Fatalf("%s: item %v doesn't decode: %v", path, it, bad)
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
	_, _, decodedItem, err := archive.NewDecoder(&cat.Streams[0]).Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
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
		_, _, _, err := archive.NewDecoder(s).Decode([]byte(tt.rec))
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

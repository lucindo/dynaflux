// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys_test

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

func mustPad(t *testing.T, delivered string) string {
	t.Helper()
	s, err := keys.PadSeq(delivered)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func bin(t *testing.T, h string) store.Value {
	t.Helper()
	b, err := hex.DecodeString(h)
	if err != nil {
		t.Fatal(err)
	}
	return store.Value{Type: store.B, Bin: b}
}

// _hwm values are binary (encoding.md §7.6): a live entry is the sequence number without
// leading zero bytes, and the backfill's entry B is H(u)[0:8] followed by it.
func TestHWMEncode(t *testing.T) {
	t.Parallel()
	uk := keys.UnitKey(exampleUnit)
	h := keys.HWM{
		Units: map[string]string{
			uk:                mustPad(t, "4421584500000000017450439091"),
			keys.UnitKey("x"): mustPad(t, "100000000000000000000"),
		},
		Backfill: &keys.Mark{UnitKey: uk, Seq: mustPad(t, "4421584500000000017450439091")},
	}
	got, err := h.Encode()
	want := store.Value{Type: store.M, Map: map[string]store.Value{
		"56DYD7J47T2VM": bin(t, "0e4972f009524608417091b3"),
		"2TW8P8AVJDC24": bin(t, "056bc75e2d63100000"),
		"B":             bin(t, "5337cd3c887d0b740e4972f009524608417091b3"),
	}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("Encode = %+v, %v; want %+v", got, err, want)
	}
	back, err := keys.DecodeHWM(got, true)
	if err != nil || !reflect.DeepEqual(back, h) {
		t.Errorf("DecodeHWM = %+v, %v; want %+v", back, err, h)
	}
}

func TestHWMLargestSequenceNumber(t *testing.T) {
	t.Parallel()
	h := keys.HWM{Units: map[string]string{"56DYD7J47T2VM": strings.Repeat("9", 40)}}
	got, err := h.Encode()
	if err != nil || hex.EncodeToString(got.Map["56DYD7J47T2VM"].Bin) != "1d6329f1c35ca4bfabb9f560ffffffffff" {
		t.Fatalf("Encode = %+v, %v", got, err)
	}
	if back, decErr := keys.DecodeHWM(got, true); decErr != nil || !reflect.DeepEqual(back, h) {
		t.Errorf("DecodeHWM = %+v, %v", back, decErr)
	}
}

func TestHWMAbsentHasNoMarks(t *testing.T) {
	t.Parallel()
	h, err := keys.DecodeHWM(store.Value{}, false)
	if err != nil || len(h.Units) != 0 || h.Backfill != nil {
		t.Errorf("DecodeHWM(absent) = %+v, %v", h, err)
	}
	if h.Units == nil {
		t.Error("Units is nil: callers add entries to it")
	}
}

func TestHWMEncodeRejectsWhatPadSeqAndUnitKeyDontReturn(t *testing.T) {
	t.Parallel()
	seq := mustPad(t, "100000000000000000000")
	for name, h := range map[string]keys.HWM{
		"unit ID as key":    {Units: map[string]string{exampleUnit: seq}},
		"lowercase key":     {Units: map[string]string{"56dyd7j47t2vm": seq}},
		"unpadded sequence": {Units: map[string]string{"56DYD7J47T2VM": "100000000000000000000"}},
		"zero sequence":     {Units: map[string]string{"56DYD7J47T2VM": strings.Repeat("0", 40)}},
		"backfill unit ID":  {Backfill: &keys.Mark{UnitKey: exampleUnit, Seq: seq}},
	} {
		if got, err := h.Encode(); err == nil {
			t.Errorf("%s: Encode = %+v, want an error", name, got)
		}
	}
}

func TestDecodeHWMRejectsForeignValues(t *testing.T) {
	t.Parallel()
	m := func(name string, v store.Value) store.Value {
		return store.Value{Type: store.M, Map: map[string]store.Value{name: v}}
	}
	for name, a := range map[string]store.Value{
		"not a map":          bin(t, "01"),
		"S value":            m("56DYD7J47T2VM", store.Value{Type: store.S, Str: "1"}),
		"empty value":        m("56DYD7J47T2VM", store.Value{Type: store.B, Bin: []byte{}}),
		"leading zero byte":  m("56DYD7J47T2VM", bin(t, "0001")),
		"over 40 digits":     m("56DYD7J47T2VM", bin(t, "1d6329f1c35ca4bfabb9f5610000000000")),
		"unknown entry":      m("C", bin(t, "01")),
		"key past 64 bits":   m("G6DYD7J47T2VM", bin(t, "01")),
		"short backfill":     m("B", bin(t, "5337cd3c887d0b74")),
		"backfill zero byte": m("B", bin(t, "5337cd3c887d0b7400")),
	} {
		if got, err := keys.DecodeHWM(a, true); err == nil {
			t.Errorf("%s: DecodeHWM = %+v, want an error", name, got)
		}
	}
}

func TestMarkCompare(t *testing.T) {
	t.Parallel()
	a := keys.Mark{UnitKey: "2TW8P8AVJDC24", Seq: mustPad(t, "900000000000000000000")}
	b := keys.Mark{UnitKey: "56DYD7J47T2VM", Seq: mustPad(t, "100000000000000000000")}
	c := keys.Mark{UnitKey: "56DYD7J47T2VM", Seq: mustPad(t, "1000000000000000000000")}
	same := b
	if a.Compare(b) >= 0 || b.Compare(c) >= 0 || c.Compare(a) <= 0 || b.Compare(same) != 0 {
		t.Errorf("Mark order is not (unit key, sequence number): %d %d %d", a.Compare(b), b.Compare(c), c.Compare(a))
	}
}

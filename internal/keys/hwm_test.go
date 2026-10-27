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

// A time mark stands in for a unit and a sequence number (encoding.md §7.6): sign-flipped
// event_time in the unit hash's place, event_id's number as the sequence number.
func TestTimeMark(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		time int64
		id   string
		want keys.Mark
	}{
		{0, "00000000000000000000000001", keys.Mark{UnitKey: "8000000000000", Seq: strings.Repeat("0", 39) + "1"}},
		{-1, "7ZZZZZZZZZZZZZZZZZZZZZZZZZ", keys.Mark{UnitKey: "7ZZZZZZZZZZZZ", Seq: "0340282366920938463463374607431768211455"}},
		{1_700_000_000_000_000, "01JB8Z9X4Q7M3N2P5R6S8T9V0W", keys.Mark{UnitKey: "801GA4GC1WG00", Seq: "0002091561768106995780800544121601453084"}},
	} {
		got, err := keys.TimeMark(tt.time, tt.id)
		if err != nil || got != tt.want {
			t.Errorf("TimeMark(%d, %s) = %+v, %v; want %+v", tt.time, tt.id, got, err, tt.want)
		}
		h := keys.HWM{Units: map[string]string{}, Backfill: &got}
		enc, err := h.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if back, derr := keys.DecodeHWM(enc, true); derr != nil || !reflect.DeepEqual(back, h) {
			t.Errorf("DecodeHWM(Encode(%+v)) = %+v, %v", h, back, derr)
		}
	}
}

// Time marks compare by event_time, negative times first, then by event_id.
func TestTimeMarkOrder(t *testing.T) {
	t.Parallel()
	ordered := []struct {
		time int64
		id   string
	}{
		{-1 << 62, "7ZZZZZZZZZZZZZZZZZZZZZZZZZ"},
		{-1, "00000000000000000000000001"},
		{0, "00000000000000000000000001"},
		{0, "0000000000000000000000000Z"},
		{0, "01000000000000000000000000"},
		{1, "00000000000000000000000001"},
		{1 << 40, "00000000000000000000000001"},
	}
	var prev keys.Mark
	for i, o := range ordered {
		m, err := keys.TimeMark(o.time, o.id)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 && prev.Compare(m) >= 0 {
			t.Errorf("TimeMark(%d, %s) = %+v doesn't sort after %+v", o.time, o.id, m, prev)
		}
		prev = m
	}
}

func TestTimeMarkRejectsWhatIsNotAnEventID(t *testing.T) {
	t.Parallel()
	for _, id := range []string{
		"00000000000000000000000000",  // zero, which no sequence number is
		"0000000000000000000000000",   // 25 characters
		"01jb8z9x4q7m3n2p5r6s8t9v0w",  // lowercase
		"81JB8Z9X4Q7M3N2P5R6S8T9V0W",  // past 128 bits
		"01JB8Z9X4Q7M3N2P5R6S8T9V0U",  // U isn't in the alphabet
		"01JB8Z9X4Q7M3N2P5R6S8T9V0W0", // 27 characters
	} {
		if m, err := keys.TimeMark(0, id); err == nil {
			t.Errorf("TimeMark(0, %q) = %+v, want an error", id, m)
		}
	}
}

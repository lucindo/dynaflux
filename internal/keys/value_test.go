// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys_test

import (
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

func nval(s string) store.Value { return store.Value{Type: store.N, Str: s} }
func sval(s string) store.Value { return store.Value{Type: store.S, Str: s} }

func decimal(unscaled int64) expr.Value { return expr.Decimal(big.NewInt(unscaled)) }

// The value forms of encoding.md §5.1.
func TestEncodeValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typ  catalog.Type
		v    expr.Value
		want store.Value
	}{
		{tString, expr.String(""), sval("")},
		{tUInt64, expr.Uint(1<<64 - 1), nval("18446744073709551615")},
		{tInt64, expr.Int(-1 << 63), nval("-9223372036854775808")},
		{tFloat, expr.Float(0.1), nval("0.1")},
		{tFloat, expr.Float(-2.5), nval("-2.5")},
		{tFloat, expr.Float(1e21), nval("1000000000000000000000")},
		{tFloat, expr.Float(1e-130), nval("0." + strings.Repeat("0", 129) + "1")},
		{tFloat, expr.Float(0), nval("0")},
		{tDec102, decimal(1250), nval("12.50")},
		{tDec102, decimal(-5), nval("-0.05")},
		{tDec102, decimal(0), nval("0.00")},
		{catalog.Type{Kind: catalog.KindDecimal, Precision: 5}, decimal(7), nval("7")},
		{tBool, expr.Bool(true), store.Value{Type: store.BOOL, Bool: true}},
		{tUUID, expr.UUID(exampleUUID()), sval(exampleU)},
		{tDate, expr.Micros(may31), sval("2024-05-31")},
		{tDT, expr.Micros(1_717_160_400_000_000), nval("1717160400000000")},
		{tDT64m, expr.Micros(1_717_163_100_250_000), nval("1717163100250000")},
		{tDT64u, expr.Micros(may31at), nval("1717163100250123")},
		{catalog.Type{Kind: catalog.KindString, Array: true}, expr.Array([]expr.Value{expr.String("a"), expr.String("")}),
			store.Value{Type: store.L, List: []store.Value{sval("a"), sval("")}}},
		{catalog.Type{Kind: catalog.KindInt8, Array: true}, expr.Array(nil), store.Value{Type: store.L, List: []store.Value{}}},
	}
	for _, tt := range tests {
		got, present, err := keys.EncodeValue(tt.typ, tt.v)
		if err != nil || !present || !reflect.DeepEqual(got, tt.want) {
			t.Errorf("EncodeValue(%s) = %+v, %v, %v; want %+v", tt.typ, got, present, err, tt.want)
		}
	}
	if got, present, err := keys.EncodeValue(catalog.Type{Kind: catalog.KindString, Nullable: true}, expr.Null()); present || err != nil {
		t.Errorf("NULL is stored as %+v, %v", got, err)
	}
}

func TestEncodeValueRejectsWhatNoAttributeHolds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		typ  catalog.Type
		v    expr.Value
	}{
		{"NULL, not Nullable", tString, expr.Null()},
		{"UInt8 256", tUInt8, expr.Uint(256)},
		{"Float64 1e126", tFloat, expr.Float(1e126)},
		{"Float64 below 1e-130", tFloat, expr.Float(9e-131)},
		{"Decimal(10,2) with 11 digits", tDec102, decimal(10_000_000_000)},
		{"DateTime64(3) with microseconds", tDT64m, expr.Micros(1)},
		{"Date past 2106-02-07", tDate, expr.Micros(timeLimitMicros/86_400_000_000*86_400_000_000 + 86_400_000_000)},
		{"Array with a bad element", catalog.Type{Kind: catalog.KindUInt8, Array: true}, expr.Array([]expr.Value{expr.Uint(1), expr.Uint(300)})},
	}
	for _, tt := range tests {
		if got, _, err := keys.EncodeValue(tt.typ, tt.v); err == nil {
			t.Errorf("%s: EncodeValue = %+v, want an error", tt.name, got)
		}
	}
	// float64(1e126) is just below 10^126, but its shortest text, which is what is written, is not.
	if _, _, err := keys.EncodeValue(tFloat, expr.Float(math.Nextafter(1e126, 0))); err != nil {
		t.Errorf("the largest Float64 whose text is below 1e126: %v", err)
	}
}

// Readers take numbers in any form DynamoDB returns and convert them exactly (encoding.md
// §5.1).
func TestDecodeValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typ  catalog.Type
		a    store.Value
		want expr.Value
	}{
		{tDec102, nval("12.5"), decimal(1250)},
		{tDec102, nval("1.25E+1"), decimal(1250)},
		{tDec102, nval("-0.05"), decimal(-5)},
		{tDec102, nval("99999999.99"), decimal(9_999_999_999)},
		{tInt8, nval("-128"), expr.Int(-128)},
		{tInt8, nval("1.0"), expr.Int(1)},
		{tInt64, nval("1e2"), expr.Int(100)},
		{tUInt8, nval("-0"), expr.Uint(0)},
		{tUInt64, nval("18446744073709551615"), expr.Uint(1<<64 - 1)},
		{tFloat, nval("0.1"), expr.Float(0.1)},
		{tFloat, nval("1E-130"), expr.Float(1e-130)},
		{tDT64u, nval("1717163100250123"), expr.Micros(may31at)},
		{tDT, nval("1.7171604E+15"), expr.Micros(1_717_160_400_000_000)},
		{tDate, sval("2024-05-31"), expr.Micros(may31)},
		{tUUID, sval(exampleU), expr.UUID(exampleUUID())},
		{tBool, store.Value{Type: store.BOOL}, expr.Bool(false)},
		{tString, sval(""), expr.String("")},
	}
	for _, tt := range tests {
		got, err := keys.DecodeValue(tt.typ, tt.a, true)
		// DeepEqual also checks the representation, which compareValues ignores; a Decimal's
		// big.Int may differ in form at the same value.
		same := reflect.DeepEqual(got, tt.want)
		if tt.typ.Kind == catalog.KindDecimal {
			same = !got.IsNull() && compareValues(tt.typ, got, tt.want) == 0
		}
		if err != nil || !same {
			t.Errorf("DecodeValue(%s, %q) = %+v, %v; want %+v", tt.typ, tt.a.Str, got, err, tt.want)
		}
	}
	got, err := keys.DecodeValue(catalog.Type{Kind: catalog.KindUInt16, Array: true},
		store.Value{Type: store.L, List: []store.Value{nval("7"), nval("65535")}}, true)
	if vs := got.AsArray(); err != nil || len(vs) != 2 || vs[0].AsUint() != 7 || vs[1].AsUint() != 65535 {
		t.Errorf("Array(UInt16) = %+v, %v", vs, err)
	}
	if null, nullErr := keys.DecodeValue(catalog.Type{Kind: catalog.KindDate, Nullable: true}, store.Value{}, false); nullErr != nil || !null.IsNull() {
		t.Errorf("absent Nullable = %+v, %v; want NULL", null, nullErr)
	}
}

func TestDecodeValueRejectsForeignValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		typ  catalog.Type
		a    store.Value
	}{
		{"Decimal(10,2) with a third fraction digit", tDec102, nval("1.234")},
		{"Decimal(10,2) with 9 integer digits", tDec102, nval("123456789")},
		{"Int8 128", tInt8, nval("128")},
		{"UInt8 -1", tUInt8, nval("-1")},
		{"UInt64 2^64", tUInt64, nval("18446744073709551616")},
		{"Int64 with a fraction", tInt64, nval("1.5")},
		{"huge exponent", tInt64, nval("1e999999999999")},
		{"Float64 NaN", tFloat, nval("NaN")},
		{"Float64 Infinity", tFloat, nval("Infinity")},
		{"Float64 hex", tFloat, nval("0x1p-2")},
		{"Float64 underscores", tFloat, nval("1_000")},
		{"Float64 trailing point", tFloat, nval("1.")},
		{"Float64 leading point", tFloat, nval(".5")},
		{"Float64 plus sign", tFloat, nval("+1")},
		{"DateTime64(3) with microseconds", tDT64m, nval("1717163100250001")},
		{"DateTime64(6) before 1970", tDT64u, nval("-1")},
		{"DateTime64(6) at the 2106 limit", tDT64u, nval("4294967295000000")},
		{"Date without zero padding", tDate, sval("2024-5-31")},
		{"Date past 2106-02-07", tDate, sval("2106-02-08")},
		{"Date before 1970", tDate, sval("1969-12-31")},
		{"UUID uppercase", tUUID, sval(strings.ToUpper(exampleU))},
		{"UUID without hyphens", tUUID, sval(strings.ReplaceAll(exampleU, "-", ""))},
		{"UUID with moved hyphens", tUUID, sval("61f0c404-5cb311e7-907b-a600-6ad3dba0")},
		{"UUID with 34 hex digits", tUUID, sval(exampleU + "aa")},
		{"UUID of 36 characters with 34 hex digits", tUUID, sval("61f0c404-5cb311e7907ba6006ad3dba0a-a")},
		{"String stored as N", tString, nval("1")},
		{"Int64 stored as S", tInt64, sval("1")},
		{"Bool stored as N", tBool, nval("1")},
		{"DynamoDB NULL", tString, store.Value{}},
		{"Array not a list", catalog.Type{Kind: catalog.KindString, Array: true}, sval("a")},
		{"Array with a bad element", catalog.Type{Kind: catalog.KindString, Array: true},
			store.Value{Type: store.L, List: []store.Value{nval("1")}}},
	}
	for _, tt := range tests {
		if got, err := keys.DecodeValue(tt.typ, tt.a, true); err == nil {
			t.Errorf("%s: DecodeValue = %+v, want an error", tt.name, got)
		}
	}
	if got, err := keys.DecodeValue(tString, store.Value{}, false); err == nil {
		t.Errorf("absent non-Nullable = %+v, want an error: its DEFAULT is the caller's", got)
	}
}

// Decoding is strict: an S value of a UUID, Date or String column decodes only when encoding
// the result gives the same text back.
func FuzzDecodeValue(f *testing.F) {
	for _, s := range []string{exampleU, exampleU + "aa", strings.ToUpper(exampleU), "2024-05-31", "2024-5-31", "", "%"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		for _, typ := range []catalog.Type{tUUID, tDate, tString} {
			v, err := keys.DecodeValue(typ, sval(s), true)
			if err != nil {
				continue
			}
			back, present, err := keys.EncodeValue(typ, v)
			if err != nil || !present || !reflect.DeepEqual(back, sval(s)) {
				t.Fatalf("%s: %q decodes to %+v, which encodes to %+v, %v", typ, s, v, back, err)
			}
		}
	})
}

// Every value survives a write to the in-memory store, which keeps numbers in DynamoDB's
// normalized form (no trailing fraction zeros), and a read back.
func TestValuesRoundTripThroughTheStore(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(7, 8))
	types := append(keyTypes(), tFloat)
	db := mem.New()
	for i := range 2000 {
		item := store.Item{catalog.AttrPK: sval("p"), catalog.AttrSK: sval(string(rune('a' + i%26)))}
		vals := make([]expr.Value, len(types))
		for c, typ := range types {
			vals[c] = randValue(r, typ)
			a, present, err := keys.EncodeValue(typ, vals[c])
			if err != nil || !present {
				t.Fatalf("EncodeValue(%s, %+v): %v", typ, vals[c], err)
			}
			item[typ.String()] = a
		}
		if err := db.Put(t.Context(), "t", item, store.Cond{}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		back, err := db.Get(t.Context(), "t", store.Key{PK: "p", SK: item[catalog.AttrSK].Str}, true)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		for c, typ := range types {
			got, decErr := keys.DecodeValue(typ, back[typ.String()], true)
			if decErr != nil || compareValues(typ, got, vals[c]) != 0 {
				t.Fatalf("%s: wrote %+v as %+v, read %+v, %v", typ, vals[c], back[typ.String()], got, decErr)
			}
		}
	}
}

// Exact counts and sums are an N up to 38 digits and an S past them (encoding.md §5.2).
func TestExactNumbers(t *testing.T) {
	t.Parallel()
	big38, _ := new(big.Int).SetString(strings.Repeat("9", 38), 10)
	big39 := new(big.Int).Add(big38, big.NewInt(1))
	tests := []struct {
		unscaled *big.Int
		scale    int
		want     store.Value
	}{
		{big38, 0, nval(strings.Repeat("9", 38))},
		{big39, 0, sval("1" + strings.Repeat("0", 38))},
		{new(big.Int).Neg(big39), 2, sval("-1" + strings.Repeat("0", 36) + ".00")},
		{big.NewInt(-123456), 2, nval("-1234.56")},
		{big.NewInt(0), 2, nval("0.00")},
	}
	for _, tt := range tests {
		got := keys.EncodeExact(tt.unscaled, tt.scale)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("EncodeExact(%s, %d) = %+v, want %+v", tt.unscaled, tt.scale, got, tt.want)
		}
		back, err := keys.DecodeExact(got, tt.scale)
		if err != nil || back.Cmp(tt.unscaled) != 0 {
			t.Errorf("DecodeExact(%+v) = %s, %v", got, back, err)
		}
	}
	if got, err := keys.DecodeExact(nval("12.3"), 2); err != nil || got.Int64() != 1230 {
		t.Errorf("DecodeExact(12.3, scale 2) = %s, %v; want 1230", got, err)
	}
	// An S holds only what EncodeExact writes for more than 38 digits; an N holds at most 38.
	for _, bad := range []store.Value{
		nval("1.234"), sval("1e70"), {Type: store.B, Bin: []byte{1}}, sval("x"),
		sval("12"), sval("1e45"), sval("0" + strings.Repeat("9", 39) + ".00"), nval(strings.Repeat("9", 51)),
	} {
		if got, err := keys.DecodeExact(bad, 2); err == nil {
			t.Errorf("DecodeExact(%+v) = %s, want an error", bad, got)
		}
	}
}

func TestPadSeq(t *testing.T) {
	t.Parallel()
	d21, d40 := "1"+strings.Repeat("0", 20), strings.Repeat("9", 40)
	if got, err := keys.PadSeq(d21); err != nil || got != strings.Repeat("0", 19)+d21 {
		t.Errorf("PadSeq(21 digits) = %q, %v", got, err)
	}
	if got, err := keys.PadSeq(d40); err != nil || got != d40 {
		t.Errorf("PadSeq(40 digits) = %q, %v", got, err)
	}
	d20, d41 := "1"+strings.Repeat("0", 19), "1"+strings.Repeat("0", 40)
	for _, bad := range []string{d20, d41, d20 + "x", strings.Repeat("0", 21), "-" + d21, ""} {
		if got, err := keys.PadSeq(bad); err == nil {
			t.Errorf("PadSeq(%q) = %q, want an error", bad, got)
		}
	}
}

// _ttl is the expiry rounded up to a whole second, plus 3600 (encoding.md §5.4).
func TestTTL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		us, expiry int64
		ttl        string
	}{
		{1_717_163_100_250_000, 1_717_163_101_000_000, "1717166701"},
		{1_717_163_100_000_000, 1_717_163_100_000_000, "1717166700"},
		{1_717_163_100_000_001, 1_717_163_101_000_000, "1717166701"},
	}
	for _, tt := range tests {
		got := keys.TTL(tt.us)
		if !reflect.DeepEqual(got, nval(tt.ttl)) {
			t.Errorf("TTL(%d) = %+v, want N %s", tt.us, got, tt.ttl)
		}
		if expiry, err := keys.TTLExpiry(got); err != nil || expiry != tt.expiry {
			t.Errorf("TTLExpiry(%s) = %d, %v; want %d", tt.ttl, expiry, err, tt.expiry)
		}
	}
}

func TestTTLExpiryRejectsWhatTTLDoesNotWrite(t *testing.T) {
	t.Parallel()
	for _, a := range []store.Value{
		{Type: store.S, Str: "1717166700"},
		nval("1717166700.5"),
		nval("3599"),
		nval("9223372036854775807"),
	} {
		if got, err := keys.TTLExpiry(a); err == nil {
			t.Errorf("TTLExpiry(%+v) = %d, want an error", a, got)
		}
	}
}

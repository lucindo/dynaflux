// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys_test

import (
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
)

//nolint:gochecknoglobals // read-only test fixtures
var (
	tString = catalog.Type{Kind: catalog.KindString}
	tUInt8  = catalog.Type{Kind: catalog.KindUInt8}
	tUInt64 = catalog.Type{Kind: catalog.KindUInt64}
	tInt8   = catalog.Type{Kind: catalog.KindInt8}
	tInt64  = catalog.Type{Kind: catalog.KindInt64}
	tDec102 = catalog.Type{Kind: catalog.KindDecimal, Precision: 10, Scale: 2}
	tBool   = catalog.Type{Kind: catalog.KindBool}
	tUUID   = catalog.Type{Kind: catalog.KindUUID}
	tDate   = catalog.Type{Kind: catalog.KindDate}
	tDT     = catalog.Type{Kind: catalog.KindDateTime}
	tDT64m  = catalog.Type{Kind: catalog.KindDateTime64, Precision: 3}
	tDT64u  = catalog.Type{Kind: catalog.KindDateTime64, Precision: 6}
	tFloat  = catalog.Type{Kind: catalog.KindFloat64}
)

// 2024-05-31 and 2024-05-31T13:45:00.250123Z in microseconds.
const (
	may31    = int64(1_717_113_600_000_000)
	may31at  = int64(1_717_163_100_250_123)
	exampleU = "61f0c404-5cb3-11e7-907b-a6006ad3dba0"
)

func exampleUUID() [16]byte {
	return [16]byte{0x61, 0xf0, 0xc4, 0x04, 0x5c, 0xb3, 0x11, 0xe7, 0x90, 0x7b, 0xa6, 0x00, 0x6a, 0xd3, 0xdb, 0xa0}
}

// Every field example of encoding.md §3.2.
func TestFieldExamples(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typ  catalog.Type
		v    expr.Value
		want string
	}{
		{tString, expr.String("New York"), "New%20York"},
		{tString, expr.String("50% #1"), "50%25%20%231"},
		{tString, expr.String(""), "%"},
		{tString, expr.String("u-123"), "u-123"},
		{tString, expr.String("a&b\n\x1f\x7f"), "a&b%0A%1F\x7f"}, // uppercase hex
		{tString, expr.String("%&"), "%25&"},                     // '%' is the last byte escaped
		{tUInt64, expr.Uint(1234), "00000000000000001234"},
		{tUInt8, expr.Uint(7), "007"},
		{tInt64, expr.Int(42), "0000000000000000042"},
		{tInt64, expr.Int(-42), "-9999999999999999957"},
		{tInt64, expr.Int(math.MinInt64), "-0776627963145224191"},
		{tInt8, expr.Int(-1), "-998"},
		{tInt8, expr.Int(-128), "-871"},
		{tDec102, expr.Decimal(big.NewInt(1234)), "00000012.34"},
		{tDec102, expr.Decimal(big.NewInt(-1234)), "-99999987.65"},
		{catalog.Type{Kind: catalog.KindDecimal, Precision: 2, Scale: 2}, expr.Decimal(big.NewInt(5)), ".05"},
		{tBool, expr.Bool(false), "false"},
		{tBool, expr.Bool(true), "true"},
		{tUUID, expr.UUID(exampleUUID()), exampleU},
		{tDate, expr.Micros(may31), "2024-05-31"},
		{tDT, expr.Micros(1_717_160_400_000_000), "2024-05-31T13:00:00Z"},
		{tDT64m, expr.Micros(1_717_163_100_250_000), "2024-05-31T13:45:00.250Z"},
		{tDT64u, expr.Micros(may31at), "2024-05-31T13:45:00.250123Z"},
	}
	for _, tt := range tests {
		got, err := keys.AppendField(nil, tt.typ, tt.v)
		if err != nil || string(got) != tt.want {
			t.Errorf("AppendField(%s, %q) = %q, %v; want %q", tt.typ, tt.want, got, err, tt.want)
		}
	}
}

// The composite key examples of encoding.md §3.3.
func TestKeyExamples(t *testing.T) {
	t.Parallel()
	pk, err := keys.Key([]catalog.Type{tString, tUInt64}, []expr.Value{expr.String("acme"), expr.Uint(1234)})
	if err != nil || pk != "acme#00000000000000001234" {
		t.Errorf("view pk = %q, %v", pk, err)
	}
	stream, err := keys.Key([]catalog.Type{tString}, []expr.Value{expr.String("u-123")})
	if err != nil || stream != "u-123" || keys.ShardKey(stream, 5) != "u-123#s05" {
		t.Errorf("stream pk = %q, %v; with shard 5: %q", stream, err, keys.ShardKey(stream, 5))
	}
	lsk, err := keys.Key([]catalog.Type{tDate}, []expr.Value{expr.Micros(may31)})
	if err != nil || keys.SplitKey(lsk, 2) != "2024-05-31#p02" {
		t.Errorf("view sk = %q, %v", keys.SplitKey(lsk, 2), err)
	}
	if got := keys.SplitKey("", 15); got != "p15" {
		t.Errorf("SplitKey without sort fields = %q, want p15", got)
	}
	if got := keys.ShardKey("u", 63); got != "u#s63" {
		t.Errorf("ShardKey(u, 63) = %q", got)
	}
}

func TestFieldRejectsValuesOutsideTheEncoding(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		typ  catalog.Type
		v    expr.Value
	}{
		{"Float64", tFloat, expr.Float(1)},
		{"Nullable", catalog.Type{Kind: catalog.KindString, Nullable: true}, expr.String("a")},
		{"Array", catalog.Type{Kind: catalog.KindString, Array: true}, expr.Array(nil)},
		{"NULL", tString, expr.Null()},
		{"UInt8 256", tUInt8, expr.Uint(256)},
		{"Int8 128", tInt8, expr.Int(128)},
		{"Int8 -129", tInt8, expr.Int(-129)},
		{"Decimal(10,2) 11 digits", tDec102, expr.Decimal(big.NewInt(10_000_000_000))},
		{"Date not at midnight", tDate, expr.Micros(may31 + 1)},
		{"DateTime with a fraction", tDT, expr.Micros(1_000)},
		{"DateTime64(3) with microseconds", tDT64m, expr.Micros(1)},
		{"before 1970", tDT64u, expr.Micros(-1)},
		{"at the 2106 limit", tDT64u, expr.Micros(timeLimitMicros)},
	}
	for _, tt := range tests {
		if got, err := keys.AppendField(nil, tt.typ, tt.v); err == nil {
			t.Errorf("%s: AppendField = %q, want an error", tt.name, got)
		}
	}
	if _, err := keys.Key(nil, nil); err == nil {
		t.Error("Key of no fields: want an error")
	}
	if _, err := keys.Key([]catalog.Type{tString}, []expr.Value{expr.String("a"), expr.String("b")}); err == nil {
		t.Error("Key with more values than types: want an error")
	}
}

// Byte order of keys equals the order of their value tuples, keys are injective, and a
// shard or SPLIT suffix keeps the order of distinct tuples (encoding.md §3.3).
func TestKeyOrderMatchesTupleOrder(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(1, 2))
	for range 3000 {
		ts := randSchema(r)
		for range 100 {
			a, b := randTuple(r, ts), randTuple(r, ts)
			copy(b[:r.IntN(len(ts)+1)], a) // shared leading fields stress the separators
			ka, kb := mustKey(t, ts, a), mustKey(t, ts, b)
			checkKeyBytes(t, ka)
			want := compareTuples(ts, a, b)
			if got := strings.Compare(ka, kb); got != want {
				t.Fatalf("%v: %q vs %q compares %d, want %d", ts, ka, kb, got, want)
			}
			if want == 0 {
				continue
			}
			sa, sb := suffixed(r, ka), suffixed(r, kb)
			if got := strings.Compare(sa, sb); got != want {
				t.Fatalf("%v: suffixed %q vs %q compares %d, want %d", ts, sa, sb, got, want)
			}
		}
	}
}

func suffixed(r *rand.Rand, k string) string {
	if r.IntN(2) == 0 {
		return keys.ShardKey(k, r.IntN(64))
	}
	return keys.SplitKey(k, r.IntN(16))
}

func mustKey(t *testing.T, ts []catalog.Type, vs []expr.Value) string {
	t.Helper()
	k, err := keys.Key(ts, vs)
	if err != nil {
		t.Fatalf("Key(%v): %v", ts, err)
	}
	return k
}

func FuzzStringPairOrder(f *testing.F) {
	tricky := trickyStrings()
	for i, s := range tricky {
		f.Add(s, tricky[(i+1)%len(tricky)], tricky[(i+7)%len(tricky)])
	}
	ts := []catalog.Type{tString, tString}
	f.Fuzz(func(t *testing.T, a, b, c string) {
		if !utf8.ValidString(a) || !utf8.ValidString(b) || !utf8.ValidString(c) {
			t.Skip("String values are valid UTF-8")
		}
		for _, pair := range [][2][]expr.Value{
			{{expr.String(a), expr.String(c)}, {expr.String(b), expr.String(c)}},
			{{expr.String(c), expr.String(a)}, {expr.String(c), expr.String(b)}},
		} {
			kx, ky := mustKey(t, ts, pair[0]), mustKey(t, ts, pair[1])
			checkKeyBytes(t, kx)
			if got, want := strings.Compare(kx, ky), compareTuples(ts, pair[0], pair[1]); got != want {
				t.Fatalf("%q vs %q compares %d, want %d", kx, ky, got, want)
			}
		}
	})
}

func TestStreamSK(t *testing.T) {
	t.Parallel()
	const id = "01HZ7EB22T000G40R40M30E209"
	sk, err := keys.StreamSK(may31at, id)
	if err != nil || sk != "2024-05-31T13:45:00.250123Z#"+id || len(sk) != 54 {
		t.Fatalf("StreamSK = %q (%d bytes), %v", sk, len(sk), err)
	}
	us, gotID, err := keys.ParseStreamSK(sk)
	if err != nil || us != may31at || gotID != id {
		t.Errorf("ParseStreamSK(%q) = %d, %q, %v", sk, us, gotID, err)
	}
	if _, lowerErr := keys.StreamSK(may31at, "01hz7eb22t000g40r40m30e209"); lowerErr == nil {
		t.Error("StreamSK with a lowercase event_id: want an error")
	}
	for _, bad := range []string{
		"2024-05-31T13:45:00.250123Z#01HZ7EB22T000G40R40M30E20",   // short ID
		"2024-05-31T13:45:00.250123Z$01HZ7EB22T000G40R40M30E209",  // separator
		"2024-05-31T13:45:00.250123Z#81HZ7EB22T000G40R40M30E209",  // ID past 128 bits
		"2024-05-31T13:45:00.250123Z#01HZ7EB22T000G40R40M30E2I9",  // 'I' is not base32
		"2024-13-31T13:45:00.250123Z#01HZ7EB22T000G40R40M30E209",  // month
		"2024-05-31 13:45:00.250123Z#01HZ7EB22T000G40R40M30E209",  // not RFC 3339
		"2024-05-31T13:45:00.250123Z#01HZ7EB22T000G40R40M30E209#", // long
	} {
		if badUS, badID, badErr := keys.ParseStreamSK(bad); badErr == nil {
			t.Errorf("ParseStreamSK(%q) = %d, %q; want an error", bad, badUS, badID)
		}
	}
}

func Example_viewKey() {
	pk, _ := keys.Key(
		[]catalog.Type{{Kind: catalog.KindString}, {Kind: catalog.KindInt64}},
		[]expr.Value{expr.String("50% #1"), expr.Int(-42)},
	)
	fmt.Println(pk)
	// Output: 50%25%20%231#-9999999999999999957
}

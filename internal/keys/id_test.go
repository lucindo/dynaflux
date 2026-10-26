// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys_test

import (
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// The expected hashes in this file come from Python's hashlib and integer arithmetic, not
// from this package.
const exampleUnit = "shardId-00000001700000000000-a1b2c3d4"

func TestUnitKey(t *testing.T) {
	t.Parallel()
	for unit, want := range map[string]string{exampleUnit: "56DYD7J47T2VM", "x": "2TW8P8AVJDC24"} {
		if got := keys.UnitKey(unit); got != want {
			t.Errorf("UnitKey(%q) = %q, want %q", unit, got, want)
		}
	}
}

// The DEDUP event_id hashes the key of the PRIMARY KEY values, event_time as DateTime64(6)
// and the DEDUP BY values (encoding.md §4.1).
func TestDedupEventID(t *testing.T) {
	t.Parallel()
	k, err := keys.Key([]catalog.Type{tString, tDT64u, tString},
		[]expr.Value{expr.String("u-123"), expr.Micros(may31at), expr.String("order-7")})
	if err != nil || k != "u-123#2024-05-31T13:45:00.250123Z#order-7" {
		t.Fatalf("dedup key = %q, %v", k, err)
	}
	if got := keys.DedupEventID(k); got != "555RPD1301M7V2A0W16A8T1GNV" {
		t.Errorf("DedupEventID = %q", got)
	}
}

func TestBucket(t *testing.T) {
	t.Parallel()
	tests := []struct {
		x       string
		n, want int
	}{
		{"01J0000000000000000000000A", 64, 15},
		{exampleUnit, 16, 4},
		{"x", 7, 1},
		{"x", 1, 0},
	}
	for _, tt := range tests {
		if got := keys.Bucket(tt.x, tt.n); got != tt.want {
			t.Errorf("Bucket(%q, %d) = %d, want %d", tt.x, tt.n, got, tt.want)
		}
	}
}

func TestMarkerKey(t *testing.T) {
	t.Parallel()
	canon := keys.Canonical(nil, tString, expr.String("u-1"))
	got := keys.MarkerKey("acme#00000000000000001234", "2024-05-31", "visitors", canon)
	if want := (store.Key{PK: "!6YH3T3DA91D8HFS6VSNW180GF7", SK: "!"}); got != want {
		t.Errorf("MarkerKey = %+v, want %+v", got, want)
	}
	// A 200-byte pk takes a 2-byte length prefix; a view without sort fields has lsk "".
	got = keys.MarkerKey(strings.Repeat("p", 200), "", "visitors", canon)
	if got.PK != "!2ZN304FYBP87MAV6DC13YNMFQN" || len(got.PK) != 27 {
		t.Errorf("MarkerKey with a long pk = %+v", got)
	}
	// The length prefixes keep the boundary between pk, lsk and column.
	if keys.MarkerKey("ab", "c", "v", canon) == keys.MarkerKey("a", "bc", "v", canon) {
		t.Error("MarkerKey(ab, c) equals MarkerKey(a, bc)")
	}
}

func TestOwner(t *testing.T) {
	t.Parallel()
	seq, err := keys.PadSeq("100000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	uk := keys.UnitKey(exampleUnit)
	const owner = "56DYD7J47T2VM0000000000000000000100000000000000000000"
	for kind, want := range map[byte]string{keys.OwnerLive: "L" + owner, keys.OwnerBackfill: "B" + owner} {
		if got, ownerErr := keys.Owner(kind, uk, seq); ownerErr != nil || got != want {
			t.Errorf("Owner(%c) = %q, %v; want %q", kind, got, ownerErr, want)
		}
	}
	// Live and backfill must build the same owner, so only the one form is accepted.
	for _, tt := range []struct {
		name         string
		kind         byte
		unitKey, seq string
	}{
		{"a unit ID in place of its key", keys.OwnerLive, exampleUnit, seq},
		{"a lowercase unit key", keys.OwnerLive, strings.ToLower(uk), seq},
		{"an unpadded sequence number", keys.OwnerLive, uk, "100000000000000000000"},
		{"an unknown kind", 'X', uk, seq},
	} {
		if o, oErr := keys.Owner(tt.kind, tt.unitKey, tt.seq); oErr == nil {
			t.Errorf("%s: Owner = %q, want an error", tt.name, o)
		}
	}
}

func TestCanonical(t *testing.T) {
	t.Parallel()
	dec := func(s string) expr.Value {
		d, _ := new(big.Int).SetString(s, 10)
		return expr.Decimal(d)
	}
	tDec38 := catalog.Type{Kind: catalog.KindDecimal, Precision: 38}
	tests := []struct {
		typ  catalog.Type
		v    expr.Value
		want string
	}{
		{tString, expr.String("é"), "c3a9"},
		{tString, expr.String(""), ""},
		{catalog.Type{Kind: catalog.KindString, Nullable: true}, expr.String("x"), "78"},
		{tUInt8, expr.Uint(255), "00000000000000ff"},
		{tUInt64, expr.Uint(1<<64 - 1), "ffffffffffffffff"},
		{tInt8, expr.Int(-128), "ffffffffffffff80"},
		{tInt64, expr.Int(-1), "ffffffffffffffff"},
		{tFloat, expr.Float(1), "3ff0000000000000"},
		{tDec102, dec("-1234"), "fffffffffffffffffffffffffffffb2e"},
		{tDec102, dec("0"), "00000000000000000000000000000000"},
		{tDec38, dec("99999999999999999999999999999999999999"), "4b3b4ca85a86c47a098a223fffffffff"},
		{tDec38, dec("-99999999999999999999999999999999999999"), "b4c4b357a5793b85f675ddc000000001"},
		{tBool, expr.Bool(true), "01"},
		{tBool, expr.Bool(false), "00"},
		{tUUID, expr.UUID(exampleUUID()), "61f0c4045cb311e7907ba6006ad3dba0"},
		{tDate, expr.Micros(may31), "000619b4aa2ac000"},
		{tDT64u, expr.Micros(may31), "000619b4aa2ac000"},
	}
	for _, tt := range tests {
		got := keys.Canonical([]byte{0xAA}, tt.typ, tt.v)
		if hex.EncodeToString(got) != "aa"+tt.want {
			t.Errorf("Canonical(%s) = %x, want aa%s (appended)", tt.typ, got, tt.want)
		}
	}
}

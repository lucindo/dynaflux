// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"reflect"
	"slices"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

func fingerprint(t *testing.T, v *catalog.ViewVersion) string {
	t.Helper()
	fp, err := catalog.Fingerprint(v)
	if err != nil {
		t.Fatalf("Fingerprint: %v", err)
	}
	return fp
}

// Fingerprints are frozen: a different value here renumbers every deployed view.
func TestFingerprintIsFrozen(t *testing.T) {
	t.Parallel()
	escaped := func() catalog.ViewVersion {
		v := pageDailyV2()
		v.Where.Args[1].Value = `<a href="x">&é` + " "
		return v
	}
	tests := []struct {
		name    string
		version func() catalog.ViewVersion
		want    string
	}{
		{"page_daily@v1", pageDailyV1, "ddde3c055025d1b4a545162930ba400594d0f4f4ae526a2f73adfe985c341508"},
		{"page_daily@v2", pageDailyV2, "a3e853dc10c9a79c54e83e5cddbe4dc96159e25893a0f531d4739bdc8ec1c1e8"},
		{"quota_usage@v1", quotaUsage, "5f93bf7a2634e6e6758f37d25a2ea4c4fa0ac575ae62271efc8f51d7a1ffb308"},
		{"tag_hourly@v1", tagHourly, "c1c64f23b6306d15fe7b563578de93f97a6d7d58d767b3addd9a0cb694c96180"},
		{"literal with escaped characters", escaped, "da3bea16fe23cfbfc5ef0513545e80f1dfdd9c95d3c7bfba0702e92cc1f4c3a2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := tt.version()
			if got := fingerprint(t, &v); got != tt.want {
				t.Errorf("Fingerprint = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestFingerprintLeavesItsInputUnchanged(t *testing.T) {
	t.Parallel()
	reversed := func() catalog.ViewVersion {
		v := pageDailyV2()
		slices.Reverse(v.Aggregates)
		return v
	}
	v := reversed()
	fingerprint(t, &v)
	if want := reversed(); !reflect.DeepEqual(v, want) {
		t.Errorf("Fingerprint changed its input:\n got %+v\nwant %+v", v, want)
	}
}

func TestFingerprintIgnoresNonIdentity(t *testing.T) {
	t.Parallel()
	base := pageDailyV2()
	want := fingerprint(t, &base)
	tests := []struct {
		name   string
		mutate func(v *catalog.ViewVersion)
	}{
		{"mode", func(v *catalog.ViewVersion) { v.Mode = catalog.ModeSync }},
		{"number", func(v *catalog.ViewVersion) { v.Number = 7 }},
		{"state", func(v *catalog.ViewVersion) { v.State = catalog.StateRetained }},
		{"stored fingerprint", func(v *catalog.ViewVersion) { v.Fingerprint = "0123" }},
		{"aggregate order", func(v *catalog.ViewVersion) { slices.Reverse(v.Aggregates) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := pageDailyV2()
			tt.mutate(&v)
			if got := fingerprint(t, &v); got != want {
				t.Errorf("Fingerprint changed to %s, want %s", got, want)
			}
		})
	}
}

// The view's name, the stream's Go names and the JSON round trip don't reach a fingerprint.
func TestFingerprintIgnoresNamesOutsideTheVersion(t *testing.T) {
	t.Parallel()
	want := fixture(t).Views[0].Versions[1].Fingerprint
	c := fixture(t)
	c.Views[0].Name = "page_daily_renamed"
	c.Streams[1].GoType = "PageVisit"
	data, err := catalog.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	decoded, err := catalog.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got := fingerprint(t, &decoded.Views[0].Versions[1]); got != want {
		t.Errorf("Fingerprint = %s, want %s", got, want)
	}
}

func TestFingerprintChangesWithDefinition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// base is the version to change; nil means page_daily@v2.
		base   func() catalog.ViewVersion
		mutate func(v *catalog.ViewVersion)
	}{
		{"stream", nil, func(v *catalog.ViewVersion) { v.Stream = "page_views_v2" }},
		{"where literal", nil, func(v *catalog.ViewVersion) { v.Where.Args[1].Value = "/" }},
		{"where removed", nil, func(v *catalog.ViewVersion) { v.Where = nil }},
		{"aggregate function", nil, func(v *catalog.ViewVersion) { v.Aggregates[1].Fn = catalog.AggMax }},
		{"aggregate argument", nil, func(v *catalog.ViewVersion) { v.Aggregates[3].Args[0] = col("page", strT()) }},
		{"aggregate condition", nil, func(v *catalog.ViewVersion) {
			v.Aggregates[2].Cond = new(call(catalog.OpEquals, boolT(), col("page", strT()), lit(strT(), "/")))
		}},
		{"aggregate added", nil, func(v *catalog.ViewVersion) {
			v.Aggregates = append(v.Aggregates, agg("users", catalog.AggUniq, u64T(), col("user_id", strT())))
		}},
		{"aggregate renamed", nil, func(v *catalog.ViewVersion) { v.Aggregates[2].Name = "view_count" }},
		{"aggregate result type", nil, func(v *catalog.ViewVersion) {
			v.Aggregates[0].Type = catalog.Type{Kind: catalog.KindDateTime64, Precision: catalog.DateTime64Millis}
		}},
		{"aggregate parameter", quotaUsage, func(v *catalog.ViewVersion) { v.Aggregates[1].Params[0] = 0.95 }},
		{"key renamed", nil, func(v *catalog.ViewVersion) { v.Keys[0].Name = "path" }},
		{"key order", nil, func(v *catalog.ViewVersion) { slices.Reverse(v.Keys) }},
		{"key expression", nil, func(v *catalog.ViewVersion) {
			v.Keys[1].Expr = call(catalog.OpToStartOfHour, dateTimeT(), col("event_time", microsT()))
		}},
		{"primary key split", nil, func(v *catalog.ViewVersion) { v.PartitionKeyLen = 2 }},
		{"ttl key", nil, func(v *catalog.ViewVersion) { v.TTL.Key = "page" }},
		{"ttl interval", nil, func(v *catalog.ViewVersion) { v.TTL.Plus = new(interval(3, catalog.UnitYear)) }},
		{"ttl removed", nil, func(v *catalog.ViewVersion) { v.TTL = nil }},
		{"split", nil, func(v *catalog.ViewVersion) { v.Split = 4 }},
		{"array join added", nil, func(v *catalog.ViewVersion) {
			v.ArrayJoin = &catalog.ArrayJoin{Array: col("tags", catalog.Type{Kind: catalog.KindString, Array: true}), Alias: "tag"}
		}},
		{"array join alias", tagHourly, func(v *catalog.ViewVersion) { v.ArrayJoin.Alias = "label" }},
		{"storage format", nil, func(v *catalog.ViewVersion) { v.StorageFormat = 2 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.base == nil {
				tt.base = pageDailyV2
			}
			base := tt.base()
			v := tt.base()
			tt.mutate(&v)
			if got := fingerprint(t, &v); got == fingerprint(t, &base) {
				t.Errorf("Fingerprint stayed %s", got)
			}
		})
	}
}

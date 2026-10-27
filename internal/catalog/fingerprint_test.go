// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
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

// Frozen view versions, without the fields that Fingerprint ignores. They're written out
// rather than built like the fixture, so that only Fingerprint or the JSON form of a
// version can change their fingerprints.
const (
	frozenPageDailyV1  = `{"stream":"page_views","where":{"op":"notEquals","type":"Bool","args":[{"op":"column","type":"String","name":"page"},{"op":"literal","type":"String"}]},"keys":[{"name":"page","expr":{"op":"column","type":"String","name":"page"}},{"name":"day","expr":{"op":"toStartOfDay","type":"DateTime","args":[{"op":"column","type":"DateTime64(6)","name":"event_time"}]}}],"partition_key_len":1,"ttl":{"key":"day","plus":"2 YEAR"},"split":1,"aggregates":[{"fn":"count","type":"UInt64","name":"views"},{"fn":"uniqExact","args":[{"op":"column","type":"String","name":"user_id"}],"type":"UInt64","name":"visitors"}],"storage_format":1}`
	frozenPageDailyV2  = `{"stream":"page_views","where":{"op":"notEquals","type":"Bool","args":[{"op":"column","type":"String","name":"page"},{"op":"literal","type":"String"}]},"keys":[{"name":"page","expr":{"op":"column","type":"String","name":"page"}},{"name":"day","expr":{"op":"toStartOfDay","type":"DateTime","args":[{"op":"column","type":"DateTime64(6)","name":"event_time"}]}}],"partition_key_len":1,"ttl":{"key":"day","plus":"2 YEAR"},"split":1,"aggregates":[{"fn":"max","args":[{"op":"column","type":"DateTime64(6)","name":"event_time"}],"type":"DateTime64(6)","name":"last_view"},{"fn":"sum","args":[{"op":"column","type":"UInt32","name":"duration_ms"}],"type":"UInt64","name":"total_ms"},{"fn":"count","type":"UInt64","name":"views"},{"fn":"uniqExact","args":[{"op":"column","type":"String","name":"user_id"}],"type":"UInt64","name":"visitors"}],"storage_format":1}`
	frozenQuotaUsage   = `{"stream":"api_calls","keys":[{"name":"account_id","expr":{"op":"column","type":"String","name":"account_id"}},{"name":"month","expr":{"op":"toStartOfMonth","type":"Date","args":[{"op":"column","type":"DateTime64(6)","name":"event_time"}]}}],"partition_key_len":1,"ttl":{"key":"month","plus":"13 MONTH"},"split":1,"aggregates":[{"fn":"count","type":"UInt64","name":"calls"},{"fn":"quantile","params":[0.9],"args":[{"op":"column","type":"UInt32","name":"units"}],"type":"Nullable(Float64)","name":"p90_units"},{"fn":"count","cond":{"op":"equals","type":"Bool","args":[{"op":"column","type":"String","name":"endpoint"},{"op":"literal","type":"String","value":"search"}]},"type":"UInt64","name":"searches"},{"fn":"sum","args":[{"op":"column","type":"UInt32","name":"units"}],"type":"UInt64","name":"used_units"}],"storage_format":1}`
	frozenTagHourly    = `{"stream":"page_views","array_join":{"array":{"op":"column","type":"Array(String)","name":"tags"},"alias":"tag"},"keys":[{"name":"tag","expr":{"op":"column","type":"String","name":"tag"}},{"name":"hour","expr":{"op":"toStartOfHour","type":"DateTime","args":[{"op":"column","type":"DateTime64(6)","name":"event_time"}]}}],"partition_key_len":1,"ttl":{"key":"hour","plus":"30 DAY"},"split":2,"aggregates":[{"fn":"uniq","args":[{"op":"column","type":"String","name":"user_id"}],"type":"UInt64","name":"users"},{"fn":"count","type":"UInt64","name":"views"}],"storage_format":1}`
	frozenQuotaRebuilt = `{"stream":"api_calls","keys":[{"name":"account_id","expr":{"op":"column","type":"String","name":"account_id"}},{"name":"month","expr":{"op":"toStartOfMonth","type":"Date","args":[{"op":"column","type":"DateTime64(6)","name":"event_time"}]}}],"partition_key_len":1,"ttl":{"key":"month","plus":"13 MONTH"},"split":1,"aggregates":[{"fn":"count","type":"UInt64","name":"calls"},{"fn":"quantile","params":[0.9],"args":[{"op":"column","type":"UInt32","name":"units"}],"type":"Nullable(Float64)","name":"p90_units"},{"fn":"count","cond":{"op":"equals","type":"Bool","args":[{"op":"column","type":"String","name":"endpoint"},{"op":"literal","type":"String","value":"search"}]},"type":"UInt64","name":"searches"},{"fn":"sum","args":[{"op":"column","type":"UInt32","name":"units"}],"type":"UInt64","name":"used_units"}],"storage_format":1,"rebuild":1}`
	frozenEscapedValue = `{"stream":"s","where":{"op":"literal","type":"String","value":"<a href=\"x\">&é\u2028\b\f\n\r\t\u0001\u007f\\"},"partition_key_len":1,"split":1,"storage_format":1}`
)

// Fingerprints are frozen: a different value here renumbers every deployed view.
func TestFingerprintIsFrozen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{"page_daily@v1", frozenPageDailyV1, "ddde3c055025d1b4a545162930ba400594d0f4f4ae526a2f73adfe985c341508"},
		{"page_daily@v2", frozenPageDailyV2, "a3e853dc10c9a79c54e83e5cddbe4dc96159e25893a0f531d4739bdc8ec1c1e8"},
		{"quota_usage@v1", frozenQuotaUsage, "5f93bf7a2634e6e6758f37d25a2ea4c4fa0ac575ae62271efc8f51d7a1ffb308"},
		{"quota_usage@v1 rebuilt once", frozenQuotaRebuilt, "12da6627a6504c5c193c5d190278ada195d2f68fdaf90d2300078969508af13c"},
		{"tag_hourly@v1", frozenTagHourly, "c1c64f23b6306d15fe7b563578de93f97a6d7d58d767b3addd9a0cb694c96180"},
		{"literal with escaped characters", frozenEscapedValue, "7c56050bb6ab1411e6e0db56c5d9328e10dd5c0e2fa7ff00733966c009cb41f5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dec := json.NewDecoder(strings.NewReader(tt.version))
			dec.DisallowUnknownFields()
			var v catalog.ViewVersion
			if err := dec.Decode(&v); err != nil {
				t.Fatalf("decode: %v", err)
			}
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
		{"rebuild count", nil, func(v *catalog.ViewVersion) { v.Rebuild = 1 }},
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

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Types the fixture uses.
func strT() catalog.Type      { return catalog.Type{Kind: catalog.KindString} }
func boolT() catalog.Type     { return catalog.Type{Kind: catalog.KindBool} }
func u8T() catalog.Type       { return catalog.Type{Kind: catalog.KindUInt8} }
func u32T() catalog.Type      { return catalog.Type{Kind: catalog.KindUInt32} }
func u64T() catalog.Type      { return catalog.Type{Kind: catalog.KindUInt64} }
func dateT() catalog.Type     { return catalog.Type{Kind: catalog.KindDate} }
func dateTimeT() catalog.Type { return catalog.Type{Kind: catalog.KindDateTime} }
func microsT() catalog.Type {
	return catalog.Type{Kind: catalog.KindDateTime64, Precision: catalog.DateTime64Micros}
}

func col(name string, t catalog.Type) catalog.Expr {
	return catalog.Expr{Op: catalog.OpColumn, Type: t, Name: name}
}

func param(name string, t catalog.Type) catalog.Expr {
	return catalog.Expr{Op: catalog.OpParam, Type: t, Name: name}
}

func lit(t catalog.Type, value string) catalog.Expr {
	return catalog.Expr{Op: catalog.OpLiteral, Type: t, Value: value}
}

func call(op catalog.Op, t catalog.Type, args ...catalog.Expr) catalog.Expr {
	return catalog.Expr{Op: op, Type: t, Args: args}
}

func agg(name string, fn catalog.AggFunc, t catalog.Type, args ...catalog.Expr) catalog.ViewAggregate {
	return catalog.ViewAggregate{Name: name, Aggregate: catalog.Aggregate{Fn: fn, Args: args, Type: t}}
}

func interval(n int, unit catalog.Unit) catalog.Interval {
	return catalog.Interval{N: n, Unit: unit}
}

// fixture is the store of sql.md §12.1 and §12.2, extended so that it uses every catalog
// field: page_views gives tags a DEFAULT; api_calls has a go:type, LOG RETENTION FOREVER,
// SHARDS 4 and no archive; page_daily has a retained v1; quota_usage adds a countIf and a
// quantile; tag_hourly has SPLIT 2; and RecentPageViews filters on referrer. Lists are in
// canonical order, and fingerprints are computed.
func fixture(t *testing.T) *catalog.Catalog {
	t.Helper()
	c := &catalog.Catalog{
		Store:          "analytics",
		Regions:        []string{"us-east-1", "us-west-2"},
		TimeoutSeconds: 60,
		Streams:        []catalog.Stream{apiCalls(), pageViews()},
		Views: []catalog.View{
			{Name: "page_daily", Versions: []catalog.ViewVersion{pageDailyV1(), pageDailyV2()}},
			{Name: "quota_usage", Versions: []catalog.ViewVersion{quotaUsage()}},
			{Name: "tag_hourly", Versions: []catalog.ViewVersion{tagHourly()}},
		},
		Queries: []catalog.Query{pageStats(), quotaUsed(), recentPageViews(), tagTotals()},
	}
	for i := range c.Views {
		for j := range c.Views[i].Versions {
			v := &c.Views[i].Versions[j]
			fp, err := catalog.Fingerprint(v)
			if err != nil {
				t.Fatalf("Fingerprint(%s@v%d): %v", c.Views[i].Name, v.Number, err)
			}
			v.Fingerprint = fp
		}
	}
	return c
}

func pageViews() catalog.Stream {
	strArray := catalog.Type{Kind: catalog.KindString, Array: true}
	return catalog.Stream{
		Name: "page_views",
		Columns: []catalog.Column{
			{Name: "user_id", Type: strT()},
			{Name: "page", Type: strT()},
			{Name: "referrer", Type: catalog.Type{Kind: catalog.KindString, Nullable: true}},
			{Name: "tags", Type: strArray, Default: new(call(catalog.OpArray, strArray))},
			{Name: "duration_ms", Type: u32T()},
			{Name: "country", Type: strT(), Default: new(lit(strT(), ""))},
		},
		PrimaryKey:    []string{"user_id"},
		LogRetention:  catalog.RetainFor(interval(2, catalog.UnitDay)),
		Accept:        catalog.Accept{Past: new(interval(30, catalog.UnitDay)), Future: interval(5, catalog.UnitMinute)},
		Shards:        1,
		Archive:       &catalog.Archive{Regions: []string{"us-east-1", "us-west-2"}, Events: catalog.ArchiveEventsAll},
		StorageFormat: catalog.StorageFormatVersion,
	}
}

func apiCalls() catalog.Stream {
	return catalog.Stream{
		Name:   "api_calls",
		GoType: "APICall",
		Columns: []catalog.Column{
			{Name: "account_id", Type: strT()},
			{Name: "endpoint", Type: strT()},
			{Name: "units", Type: u32T()},
			{Name: "request_id", Type: catalog.Type{Kind: catalog.KindUUID}},
		},
		PrimaryKey:    []string{"account_id"},
		DedupBy:       []string{"request_id"},
		LogRetention:  catalog.RetainForever(),
		Accept:        catalog.Accept{Future: interval(5, catalog.UnitMinute)},
		Shards:        4,
		StorageFormat: catalog.StorageFormatVersion,
	}
}

func pageDailyV1() catalog.ViewVersion {
	v := pageDailyV2()
	v.Number = 1
	v.State = catalog.StateRetained
	v.Aggregates = []catalog.ViewAggregate{
		agg("views", catalog.AggCount, u64T()),
		agg("visitors", catalog.AggUniqExact, u64T(), col("user_id", strT())),
	}
	return v
}

func pageDailyV2() catalog.ViewVersion {
	return catalog.ViewVersion{
		Number: 2,
		State:  catalog.StateCurrent,
		Mode:   catalog.ModeAsync,
		Stream: "page_views",
		Where:  new(call(catalog.OpNotEquals, boolT(), col("page", strT()), lit(strT(), ""))),
		Keys: []catalog.NamedExpr{
			{Name: "page", Expr: col("page", strT())},
			{Name: "day", Expr: call(catalog.OpToStartOfDay, dateTimeT(), col("event_time", microsT()))},
		},
		PartitionKeyLen: 1,
		TTL:             &catalog.ViewTTL{Key: "day", Plus: new(interval(2, catalog.UnitYear))},
		Split:           1,
		Aggregates: []catalog.ViewAggregate{
			agg("last_view", catalog.AggMax, microsT(), col("event_time", microsT())),
			agg("total_ms", catalog.AggSum, u64T(), col("duration_ms", u32T())),
			agg("views", catalog.AggCount, u64T()),
			agg("visitors", catalog.AggUniqExact, u64T(), col("user_id", strT())),
		},
		StorageFormat: catalog.StorageFormatVersion,
	}
}

func tagHourly() catalog.ViewVersion {
	return catalog.ViewVersion{
		Number: 1,
		State:  catalog.StateCurrent,
		Mode:   catalog.ModeAsync,
		Stream: "page_views",
		ArrayJoin: &catalog.ArrayJoin{
			Array: col("tags", catalog.Type{Kind: catalog.KindString, Array: true}),
			Alias: "tag",
		},
		Keys: []catalog.NamedExpr{
			{Name: "tag", Expr: col("tag", strT())},
			{Name: "hour", Expr: call(catalog.OpToStartOfHour, dateTimeT(), col("event_time", microsT()))},
		},
		PartitionKeyLen: 1,
		TTL:             &catalog.ViewTTL{Key: "hour", Plus: new(interval(30, catalog.UnitDay))},
		Split:           2,
		Aggregates: []catalog.ViewAggregate{
			agg("users", catalog.AggUniq, u64T(), col("user_id", strT())),
			agg("views", catalog.AggCount, u64T()),
		},
		StorageFormat: catalog.StorageFormatVersion,
	}
}

func quotaUsage() catalog.ViewVersion {
	searches := agg("searches", catalog.AggCount, u64T())
	searches.Cond = new(call(catalog.OpEquals, boolT(), col("endpoint", strT()), lit(strT(), "search")))
	p90 := agg("p90_units", catalog.AggQuantile, catalog.Type{Kind: catalog.KindFloat64, Nullable: true},
		col("units", u32T()))
	p90.Params = []float64{0.9}
	return catalog.ViewVersion{
		Number: 1,
		State:  catalog.StateCurrent,
		Mode:   catalog.ModeSync,
		Stream: "api_calls",
		Keys: []catalog.NamedExpr{
			{Name: "account_id", Expr: col("account_id", strT())},
			{Name: "month", Expr: call(catalog.OpToStartOfMonth, dateT(), col("event_time", microsT()))},
		},
		PartitionKeyLen: 1,
		TTL:             &catalog.ViewTTL{Key: "month", Plus: new(interval(13, catalog.UnitMonth))},
		Split:           1,
		Aggregates: []catalog.ViewAggregate{
			agg("calls", catalog.AggCount, u64T()),
			p90,
			searches,
			agg("used_units", catalog.AggSum, u64T(), col("units", u32T())),
		},
		StorageFormat: catalog.StorageFormatVersion,
	}
}

func pageStats() catalog.Query {
	return catalog.Query{
		Name:        "PageStats",
		Cardinality: catalog.CardinalityMany,
		Params: []catalog.Param{
			{Name: "page", Type: strT()},
			{Name: "from", Type: dateT()},
			{Name: "to", Type: dateT()},
		},
		Source: catalog.Source{Kind: catalog.SourceView, Name: "page_daily"},
		Keys: catalog.KeyPlan{
			Partition: []catalog.KeyBinding{{Column: "page", Values: []catalog.Expr{param("page", strT())}}},
			Range: &catalog.KeyRange{
				Column: "day",
				Lower:  &catalog.RangeBound{Value: param("from", dateT()), Inclusive: true},
				Upper:  &catalog.RangeBound{Value: param("to", dateT()), Inclusive: true},
			},
		},
		Columns: []catalog.NamedExpr{
			{Name: "day", Expr: col("day", dateTimeT())},
			{Name: "views", Expr: col("views", u64T())},
			{Name: "visitors", Expr: col("visitors", u64T())},
		},
		OrderBy: []catalog.OrderItem{{Expr: col("day", dateTimeT())}, {Expr: col("page", strT())}},
	}
}

func quotaUsed() catalog.Query {
	return catalog.Query{
		Name:        "QuotaUsed",
		Cardinality: catalog.CardinalityOne,
		Params:      []catalog.Param{{Name: "account_id", Type: strT()}, {Name: "at", Type: microsT()}},
		Source:      catalog.Source{Kind: catalog.SourceView, Name: "quota_usage"},
		Keys: catalog.KeyPlan{
			Partition: []catalog.KeyBinding{{Column: "account_id", Values: []catalog.Expr{param("account_id", strT())}}},
			SortPrefix: []catalog.KeyBinding{{
				Column: "month",
				Values: []catalog.Expr{call(catalog.OpToStartOfMonth, dateT(), param("at", microsT()))},
			}},
		},
		Columns: []catalog.NamedExpr{
			{Name: "used_units", Expr: col("used_units", u64T())},
			{Name: "calls", Expr: col("calls", u64T())},
		},
		OrderBy: []catalog.OrderItem{{Expr: col("account_id", strT())}, {Expr: col("month", dateT())}},
	}
}

func recentPageViews() catalog.Query {
	return catalog.Query{
		Name:        "RecentPageViews",
		Cardinality: catalog.CardinalityMany,
		Params: []catalog.Param{
			{Name: "user", Type: strT()},
			{Name: "since", Type: microsT()},
			{Name: "n", Type: u32T()},
		},
		Source: catalog.Source{Kind: catalog.SourceStream, Name: "page_views"},
		Keys: catalog.KeyPlan{
			Partition: []catalog.KeyBinding{{Column: "user_id", Values: []catalog.Expr{param("user", strT())}}},
			Range: &catalog.KeyRange{
				Column: "event_time",
				Lower:  &catalog.RangeBound{Value: param("since", microsT()), Inclusive: true},
			},
		},
		Filter: new(call(catalog.OpNot, boolT(),
			call(catalog.OpIsNull, boolT(), col("referrer", catalog.Type{Kind: catalog.KindString, Nullable: true})))),
		Columns: []catalog.NamedExpr{
			{Name: "event_time", Expr: col("event_time", microsT())},
			{Name: "page", Expr: col("page", strT())},
			{Name: "duration_ms", Expr: col("duration_ms", u32T())},
		},
		OrderBy: []catalog.OrderItem{
			{Expr: col("event_time", microsT()), Desc: true},
			{Expr: col("event_id", strT())},
		},
		Limit: new(param("n", u32T())),
	}
}

func tagTotals() catalog.Query {
	totalViews := catalog.Expr{Op: catalog.OpAggregate, Type: u64T(), Index: 0}
	return catalog.Query{
		Name:        "TagTotals",
		Cardinality: catalog.CardinalityMany,
		ExposeHTTP:  true,
		Params: []catalog.Param{
			{Name: "tags", Type: catalog.Type{Kind: catalog.KindString, Array: true}},
			{Name: "since", Type: microsT()},
		},
		Source: catalog.Source{Kind: catalog.SourceView, Name: "tag_hourly"},
		Keys: catalog.KeyPlan{
			Partition: []catalog.KeyBinding{{
				Column: "tag",
				Values: []catalog.Expr{param("tags", catalog.Type{Kind: catalog.KindString, Array: true})},
			}},
			Range: &catalog.KeyRange{
				Column: "hour",
				Lower:  &catalog.RangeBound{Value: param("since", microsT()), Inclusive: true},
			},
		},
		Aggregation: &catalog.Aggregation{
			GroupBy: []catalog.NamedExpr{{Name: "tag", Expr: col("tag", strT())}},
			Aggregates: []catalog.QueryAggregate{
				{Aggregate: catalog.Aggregate{Fn: catalog.AggSum, Args: []catalog.Expr{col("views", u64T())}, Type: u64T()}},
				{Aggregate: catalog.Aggregate{Fn: catalog.AggUniq, Args: []catalog.Expr{col("users", u64T())}, Type: u64T()}, Merge: true},
			},
		},
		Columns: []catalog.NamedExpr{
			{Name: "tag", Expr: col("tag", strT())},
			{Name: "total_views", Expr: totalViews},
			{Name: "total_users", Expr: catalog.Expr{Op: catalog.OpAggregate, Type: u64T(), Index: 1}},
		},
		OrderBy: []catalog.OrderItem{{Expr: totalViews, Desc: true}, {Expr: col("tag", strT())}},
		Limit:   new(lit(u8T(), "10")),
	}
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

const goldenPath = "testdata/catalog.json"

var update = flag.Bool("update", false, "write the golden files in testdata") //nolint:gochecknoglobals // go test flags are package variables

func marshalFixture(t *testing.T) []byte {
	t.Helper()
	data, err := catalog.Marshal(fixture(t))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return data
}

// roundTrip checks that c survives Marshal and Unmarshal unchanged.
func roundTrip(t *testing.T, c *catalog.Catalog) {
	t.Helper()
	data, err := catalog.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := catalog.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, c) {
		t.Errorf("Unmarshal(Marshal(c)) differs from c:\n got %+v\nwant %+v", got, c)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	t.Parallel()
	c := fixture(t)
	roundTrip(t, c)
	data := marshalFixture(t)
	decoded, err := catalog.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	again, err := catalog.Marshal(decoded)
	if err != nil {
		t.Fatalf("Marshal of decoded catalog: %v", err)
	}
	if !bytes.Equal(again, data) {
		t.Errorf("Marshal of the decoded catalog changed the bytes:\n got %s\nwant %s", again, data)
	}
}

// The golden file shows the JSON contract. Regenerate it with go test -update.
func TestMarshalGolden(t *testing.T) {
	t.Parallel()
	var got bytes.Buffer
	if err := json.Indent(&got, marshalFixture(t), "", "  "); err != nil {
		t.Fatalf("Indent: %v", err)
	}
	got.WriteByte('\n')
	if *update {
		if err := os.WriteFile(goldenPath, got.Bytes(), 0o600); err != nil {
			t.Fatalf("update golden: %v", err)
		}
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Errorf("Marshal output differs from %s (go test -update rewrites it):\n%s", goldenPath, got.Bytes())
	}
}

func TestMarshalSortsUnorderedLists(t *testing.T) {
	t.Parallel()
	reversed := func() *catalog.Catalog {
		c := fixture(t)
		slices.Reverse(c.Regions)
		slices.Reverse(c.Streams)
		for _, s := range c.Streams {
			if s.Archive != nil {
				slices.Reverse(s.Archive.Regions)
			}
		}
		slices.Reverse(c.Views)
		for _, v := range c.Views {
			slices.Reverse(v.Versions)
			for _, vv := range v.Versions {
				slices.Reverse(vv.Aggregates)
			}
		}
		slices.Reverse(c.Retired)
		slices.Reverse(c.Queries)
		return c
	}
	c := reversed()
	got, err := catalog.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if want := marshalFixture(t); !bytes.Equal(got, want) {
		t.Errorf("Marshal depends on list order:\n got %s\nwant %s", got, want)
	}
	if !reflect.DeepEqual(c, reversed()) {
		t.Error("Marshal changed its input")
	}
}

func TestMarshalRejectsInvalidCatalog(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(c *catalog.Catalog)
		wantErr string
	}{
		{"unknown op", func(c *catalog.Catalog) { c.Views[0].Versions[1].Where.Op = "like" }, `unknown expression op "like"`},
		{"invalid type", func(c *catalog.Catalog) {
			c.Streams[0].Columns[0].Type = catalog.Type{Kind: catalog.KindString, Nullable: true, Array: true}
		}, "an Array can't be Nullable"},
		{"unset retention", func(c *catalog.Catalog) { c.Streams[0].LogRetention = catalog.Retention{} }, "log retention is not set"},
		{"retention in minutes", func(c *catalog.Catalog) {
			c.Streams[0].LogRetention = catalog.RetainFor(interval(90, catalog.UnitMinute))
		}, "invalid log retention 90 MINUTE"},
		{"retention under a day", func(c *catalog.Catalog) {
			c.Streams[0].LogRetention = catalog.RetainFor(interval(23, catalog.UnitHour))
		}, "invalid log retention 23 HOUR"},
		{"interval without unit", func(c *catalog.Catalog) { c.Streams[0].Accept.Future = catalog.Interval{} }, "invalid interval"},
		{"unknown aggregate", func(c *catalog.Catalog) { c.Views[0].Versions[0].Aggregates[0].Fn = "groupArray" }, `unknown aggregate function "groupArray"`},
		{"duplicate stream", func(c *catalog.Catalog) { c.Streams[1].Name = "api_calls" }, "stream api_calls appears twice"},
		{"duplicate view", func(c *catalog.Catalog) { c.Views[1].Name = "page_daily" }, "view page_daily appears twice"},
		{"duplicate query", func(c *catalog.Catalog) { c.Queries[1].Name = "PageStats" }, "query PageStats appears twice"},
		{"view named like a stream", func(c *catalog.Catalog) { c.Views[2].Name = "page_views" },
			"view page_views has the name of a stream"},
		{"duplicate column", func(c *catalog.Catalog) { c.Streams[1].Columns[1].Name = "user_id" },
			"stream page_views: column user_id appears twice"},
		{"column named like an implicit one", func(c *catalog.Catalog) { c.Streams[1].Columns[0].Name = "event_time" },
			"stream page_views: column event_time appears twice"},
		{"no primary key", func(c *catalog.Catalog) { c.Streams[0].PrimaryKey = nil }, "stream api_calls: no primary key"},
		{"primary key of an undeclared column", func(c *catalog.Catalog) { c.Streams[0].PrimaryKey = []string{"event_id"} },
			"stream api_calls: primary key event_id is not a declared column"},
		{"dedup by an undeclared column", func(c *catalog.Catalog) { c.Streams[0].DedupBy = []string{"request"} },
			"stream api_calls: dedup by request is not a declared column"},
		{"no shards", func(c *catalog.Catalog) { c.Streams[0].Shards = 0 }, "shards 0 is not between 1 and 64"},
		{"too many shards", func(c *catalog.Catalog) { c.Streams[0].Shards = 65 }, "shards 65 is not between 1 and 64"},
		{"Nullable column with a default", func(c *catalog.Catalog) {
			c.Streams[1].Columns[2].Default = new(lit(strT(), ""))
		}, "column referrer: a Nullable column has no default"},
		{"invalid default", func(c *catalog.Catalog) { c.Streams[1].Columns[5].Default.Name = "x" }, "column country: invalid literal expression"},
		{"unsupported storage format", func(c *catalog.Catalog) { c.Streams[1].StorageFormat = 2 }, "stream page_views: storage format 2 is not supported"},
		{"no storage format", func(c *catalog.Catalog) { c.Streams[1].StorageFormat = 0 }, "stream page_views: storage format 0 is not supported"},
		{"duplicate version number", func(c *catalog.Catalog) { c.Views[0].Versions[0].Number = 2 }, "view page_daily: version 2 appears twice"},
		{"version 0", func(c *catalog.Catalog) { c.Views[1].Versions[0].Number = 0 }, "view quota_usage@v0: version numbers start at 1"},
		{"two current versions", func(c *catalog.Catalog) { c.Views[0].Versions[0].State = catalog.StateCurrent },
			"view page_daily: 2 versions are current"},
		{"retired version in a view", func(c *catalog.Catalog) { c.Views[0].Versions[0].State = catalog.StateRetired },
			"view page_daily: page_daily@v1 is retired, so only the retired list names it"},
		{"negative rebuild count", func(c *catalog.Catalog) { c.Views[1].Versions[0].Rebuild = -1 },
			"quota_usage@v1: rebuild count -1 is negative"},
		{"retired list holds a view's version", func(c *catalog.Catalog) { c.Retired = append(c.Retired, "tag_hourly@v1") },
			"retired version tag_hourly@v1 is a version of view tag_hourly"},
		{"retired version twice", func(c *catalog.Catalog) { c.Retired = append(c.Retired, "page_hourly@v1") },
			"retired version page_hourly@v1 appears twice"},
		{"retired version 0", func(c *catalog.Catalog) { c.Retired[0] = "page_hourly@v0" }, `invalid retired version "page_hourly@v0"`},
		{"retired version with a leading zero", func(c *catalog.Catalog) { c.Retired[0] = "page_hourly@v01" },
			`invalid retired version "page_hourly@v01"`},
		{"retired version without a number", func(c *catalog.Catalog) { c.Retired[0] = "page_hourly" },
			`invalid retired version "page_hourly"`},
		{"retired version of an invalid view name", func(c *catalog.Catalog) { c.Retired[0] = "Page_hourly@v1" },
			`invalid retired version "Page_hourly@v1"`},
		{"no split", func(c *catalog.Catalog) { c.Views[2].Versions[0].Split = 0 }, "tag_hourly@v1: split 0 is not between 1 and 16"},
		{"too much split", func(c *catalog.Catalog) { c.Views[2].Versions[0].Split = 17 }, "tag_hourly@v1: split 17 is not between 1 and 16"},
		{"empty partition key", func(c *catalog.Catalog) { c.Views[1].Versions[0].PartitionKeyLen = 0 },
			"quota_usage@v1: partition key length 0 is not between 1 and the 2 keys"},
		{"partition key longer than the keys", func(c *catalog.Catalog) { c.Views[1].Versions[0].PartitionKeyLen = 3 },
			"quota_usage@v1: partition key length 3 is not between 1 and the 2 keys"},
		{"aggregate named like a key", func(c *catalog.Catalog) { c.Views[1].Versions[0].Aggregates[0].Name = "month" },
			"quota_usage@v1: column month appears twice"},
		{"aggregate argument count", func(c *catalog.Catalog) {
			c.Views[1].Versions[0].Aggregates[0].Args = []catalog.Expr{col("units", u32T())}
		}, "aggregate calls: count takes 0 arguments and 0 parameters, not 1 and 0"},
		{"quantile without its level", func(c *catalog.Catalog) { c.Views[1].Versions[0].Aggregates[1].Params = nil },
			"aggregate p90_units: quantile takes 1 arguments and 1 parameters, not 1 and 0"},
		{"stale fingerprint", func(c *catalog.Catalog) { c.Views[1].Versions[0].Fingerprint = "00" },
			`quota_usage@v1: fingerprint "00" doesn't match the definition`},
		{"duplicate parameter", func(c *catalog.Catalog) { c.Queries[0].Params[1].Name = "page" },
			"query PageStats: parameter page appears twice"},
		{"aggregate in a query without aggregation", func(c *catalog.Catalog) {
			c.Queries[0].Columns[0].Expr = catalog.Expr{Op: catalog.OpAggregate, Type: u64T()}
		}, "query PageStats: invalid aggregate expression"},
		{"aggregate out of range", func(c *catalog.Catalog) { c.Queries[3].Columns[1].Expr.Index = 2 },
			"query TagTotals: invalid aggregate expression"},
		{"aggregate outside the output", func(c *catalog.Catalog) {
			c.Queries[3].Filter = new(catalog.Expr{Op: catalog.OpAggregate, Type: u64T()})
		}, "query TagTotals: invalid aggregate expression"},
		{"merge with two arguments", func(c *catalog.Catalog) {
			a := &c.Queries[3].Aggregation.Aggregates[1]
			a.Args = append(a.Args, col("views", u64T()))
		}, "query TagTotals: aggregate 1: uniq takes 1 arguments and 0 parameters, not 2 and 0"},
		{"invalid UTF-8", func(c *catalog.Catalog) { c.Views[0].Versions[1].Where.Args[1].Value = "\xff" },
			"literal value is not valid UTF-8"},
		{"no prune window", func(c *catalog.Catalog) { c.PruneWindowSeconds = 0 },
			"prune window 0 seconds is not between 1 and 90000"},
		{"prune window over 25 hours", func(c *catalog.Catalog) { c.PruneWindowSeconds = 90001 },
			"prune window 90001 seconds is not between 1 and 90000"},
		{"view without versions", func(c *catalog.Catalog) { c.Views[1].Versions = nil }, "view quota_usage has no versions"},
		{"key range without bounds", func(c *catalog.Catalog) { c.Queries[0].Keys.Range.Lower, c.Queries[0].Keys.Range.Upper = nil, nil },
			"query PageStats: key range on day has no bound"},
		{"duplicate output column", func(c *catalog.Catalog) { c.Queries[0].Columns[1].Name = "day" },
			"query PageStats: output column day appears twice"},
		{"duplicate GROUP BY key", func(c *catalog.Catalog) {
			a := c.Queries[3].Aggregation
			a.GroupBy = append(a.GroupBy, a.GroupBy[0])
		}, "query TagTotals: GROUP BY key tag appears twice"},
		{"query without a sort order", func(c *catalog.Catalog) { c.Queries[0].OrderBy = nil }, "query PageStats: no sort order"},
		{"uppercase stream name", func(c *catalog.Catalog) { c.Streams[0].Name = "Api_calls" }, `invalid stream name "Api_calls"`},
		{"view name with a dash", func(c *catalog.Catalog) { c.Views[2].Name = "tag-hourly" }, `invalid view name "tag-hourly"`},
		{"column name with invalid UTF-8", func(c *catalog.Catalog) { c.Streams[1].Columns[0].Name = "user\xffid" },
			`stream page_views: invalid column name "user\xffid"`},
		{"column named pk", func(c *catalog.Catalog) { c.Streams[1].Columns[0].Name = "pk" },
			`stream page_views: invalid column name "pk"`},
		{"view key named sk", func(c *catalog.Catalog) { c.Views[1].Versions[0].Keys[1].Name = "sk" },
			`quota_usage@v1: invalid column name "sk"`},
		{"aggregate named like a bookkeeping attribute", func(c *catalog.Catalog) { c.Views[1].Versions[0].Aggregates[0].Name = "_hwm" },
			`quota_usage@v1: invalid column name "_hwm"`},
		{"ARRAY JOIN alias with a digit first", func(c *catalog.Catalog) { c.Views[2].Versions[0].ArrayJoin.Alias = "1tag" },
			`tag_hourly@v1: invalid ARRAY JOIN alias "1tag"`},
		{"empty parameter name", func(c *catalog.Catalog) { c.Queries[0].Params[0].Name = "" },
			`query PageStats: invalid parameter name ""`},
		{"output column with a space", func(c *catalog.Catalog) { c.Queries[0].Columns[0].Name = "the day" },
			`query PageStats: invalid output column name "the day"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := fixture(t)
			tt.mutate(c)
			_, err := catalog.Marshal(c)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Marshal error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}

	t.Run("nil catalog", func(t *testing.T) {
		t.Parallel()
		if _, err := catalog.Marshal(nil); err == nil {
			t.Error("Marshal(nil) succeeded")
		}
	})
}

// The edges of what Marshal accepts: what the compiler can produce must round-trip.
func TestMarshalAcceptsEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(c *catalog.Catalog)
	}{
		{"prune window of 1 second", func(c *catalog.Catalog) { c.PruneWindowSeconds = 1 }},
		{"prune window of 25 hours", func(c *catalog.Catalog) { c.PruneWindowSeconds = 90000 }},
		{"key range with only an upper bound", func(c *catalog.Catalog) { c.Queries[0].Keys.Range.Lower = nil }},
		// sql.md §3.4 reserves pk and sk for stream columns and view outputs only.
		{"query output named pk", func(c *catalog.Catalog) { c.Queries[0].Columns[0].Name = "pk" }},
		{"aggregation without GROUP BY or sort order", func(c *catalog.Catalog) {
			q := &c.Queries[3]
			q.Aggregation.GroupBy = nil
			q.Columns = q.Columns[1:]
			q.OrderBy = nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := fixture(t)
			tt.mutate(c)
			roundTrip(t, c)
		})
	}
}

// Every op round-trips with the arguments it takes, and fails with others.
func TestExprShape(t *testing.T) {
	t.Parallel()
	nullable := catalog.Type{Kind: catalog.KindString, Nullable: true}
	strArray := catalog.Type{Kind: catalog.KindString, Array: true}
	args := func(n int) []catalog.Expr {
		return slices.Repeat([]catalog.Expr{col("page", strT())}, n)
	}
	week := func(mode catalog.Expr) catalog.Expr {
		return call(catalog.OpToStartOfWeek, dateT(), col("event_time", microsT()), mode)
	}
	type exprCase struct {
		name string
		expr catalog.Expr
		ok   bool
	}
	tests := []exprCase{
		{"literal", lit(strT(), "x"), true},
		{"literal with a name", catalog.Expr{Op: catalog.OpLiteral, Type: strT(), Value: "x", Name: "page"}, false},
		{"literal with arguments", catalog.Expr{Op: catalog.OpLiteral, Type: strT(), Args: args(1)}, false},
		{"null", catalog.Expr{Op: catalog.OpNull, Type: nullable}, true},
		{"null with a value", catalog.Expr{Op: catalog.OpNull, Type: nullable, Value: "x"}, false},
		{"empty array", call(catalog.OpArray, strArray), true},
		{"array", call(catalog.OpArray, strArray, lit(strT(), "a"), lit(strT(), "b")), true},
		{"column", col("page", strT()), true},
		{"column without a name", col("", strT()), false},
		{"parameter", param("page", strT()), true},
		{"parameter with a value", catalog.Expr{Op: catalog.OpParam, Type: strT(), Name: "page", Value: "x"}, false},
		{"unknown op", catalog.Expr{Op: "like", Type: boolT(), Args: args(2)}, false},
		{"toStartOfWeek", week(lit(u8T(), "1")), true},
		{"toStartOfWeek with a parameter mode", week(param("mode", u8T())), false},
		{"call with a name", catalog.Expr{Op: catalog.OpLower, Type: strT(), Name: "page", Args: args(1)}, false},
	}
	arity := []struct {
		ops     []catalog.Op
		ok, bad []int
	}{
		{[]catalog.Op{catalog.OpAnd, catalog.OpOr, catalog.OpIn, catalog.OpCoalesce, catalog.OpConcat}, []int{2, 3}, []int{0, 1}},
		{[]catalog.Op{
			catalog.OpNot, catalog.OpIsNull, catalog.OpNegate, catalog.OpLower, catalog.OpUpper, catalog.OpLength,
			catalog.OpToDate, catalog.OpToStartOfMinute, catalog.OpToStartOfFiveMinutes, catalog.OpToStartOfHour,
			catalog.OpToStartOfDay, catalog.OpToStartOfMonth, catalog.OpToYYYYMM,
		}, []int{1}, []int{0, 2}},
		{[]catalog.Op{
			catalog.OpEquals, catalog.OpNotEquals, catalog.OpLess, catalog.OpLessOrEquals, catalog.OpGreater,
			catalog.OpGreaterOrEquals, catalog.OpPlus, catalog.OpMinus, catalog.OpMultiply, catalog.OpDivide,
			catalog.OpModulo, catalog.OpIntDiv,
		}, []int{2}, []int{1, 3}},
		{[]catalog.Op{catalog.OpArray}, []int{catalog.MaxArrayLen}, []int{catalog.MaxArrayLen + 1}},
		{[]catalog.Op{catalog.OpIf}, []int{3}, []int{2, 4}},
		{[]catalog.Op{catalog.OpMultiIf}, []int{3, 5}, []int{1, 2, 4}},
	}
	for _, a := range arity {
		for _, op := range a.ops {
			for _, n := range a.ok {
				tests = append(tests, exprCase{fmt.Sprintf("%s of %d", op, n), call(op, strT(), args(n)...), true})
			}
			for _, n := range a.bad {
				tests = append(tests, exprCase{fmt.Sprintf("%s of %d", op, n), call(op, strT(), args(n)...), false})
			}
		}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := fixture(t)
			// An array literal appears only in a column DEFAULT, here that of page_views.tags.
			if tt.expr.Op == catalog.OpArray {
				c.Streams[1].Columns[3].Default = new(tt.expr)
			} else {
				c.Queries[2].Columns[0].Expr = tt.expr
			}
			if tt.ok {
				roundTrip(t, c)
				return
			}
			if _, err := catalog.Marshal(c); err == nil {
				t.Error("Marshal accepted the expression")
			}
		})
	}
}

// Every aggregate function round-trips with the arguments and parameters it takes.
func TestAggregateShape(t *testing.T) {
	t.Parallel()
	x := col("views", u64T())
	tests := []struct {
		fn     catalog.AggFunc
		args   []catalog.Expr
		params []float64
	}{
		{catalog.AggCount, nil, nil},
		{catalog.AggSum, []catalog.Expr{x}, nil},
		{catalog.AggMin, []catalog.Expr{x}, nil},
		{catalog.AggMax, []catalog.Expr{x}, nil},
		{catalog.AggAvg, []catalog.Expr{x}, nil},
		{catalog.AggArgMax, []catalog.Expr{x, x}, nil},
		{catalog.AggArgMin, []catalog.Expr{x, x}, nil},
		{catalog.AggAny, []catalog.Expr{x}, nil},
		{catalog.AggUniq, []catalog.Expr{x}, nil},
		{catalog.AggUniqExact, []catalog.Expr{x}, nil},
		{catalog.AggQuantile, []catalog.Expr{x}, []float64{0.5}},
		{catalog.AggQuantileDD, []catalog.Expr{x}, []float64{0.01, 0.99}},
	}
	for _, tt := range tests {
		t.Run(string(tt.fn), func(t *testing.T) {
			t.Parallel()
			for _, merge := range []bool{false, true} {
				c := fixture(t)
				args := tt.args
				if merge {
					args = []catalog.Expr{x}
				}
				c.Queries[3].Aggregation.Aggregates[0] = catalog.QueryAggregate{
					Aggregate: catalog.Aggregate{Fn: tt.fn, Params: tt.params, Args: args, Type: u64T()},
					Merge:     merge,
				}
				roundTrip(t, c)
			}
		})
	}
}

func TestUnmarshalRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		old, new string
		wantErr  string
	}{
		{"unknown envelope field", `{"format":1,`, `{"format":1,"extra":true,`, `unknown field "extra"`},
		{"unknown catalog field", `"store":"analytics"`, `"store":"analytics","prefix":"prod-"`, `unknown field "prefix"`},
		{"unknown stream field", `"shards":4`, `"shards":4,"consistency":"strong"`, `unknown field "consistency"`},
		{"unknown expression field", `"op":"column"`, `"op":"column","scope":"row"`, `unknown field "scope"`},
		{"newer format", `{"format":1,`, `{"format":2,`, "format 2 is not supported"},
		{"missing format", `{"format":1,`, `{`, "format 0 is not supported"},
		{"unknown op", `"op":"notEquals"`, `"op":"distinct"`, `unknown expression op "distinct"`},
		{"null op", `"op":"notEquals"`, `"op":null`, `unknown expression op ""`},
		{"non-canonical type", `"type":"String"`, `"type":"LowCardinality(String)"`, `invalid type "LowCardinality(String)"`},
		{"unknown mode", `"mode":"sync"`, `"mode":"eventual"`, `unknown view mode "eventual"`},
		{"unknown state", `"state":"retained"`, `"state":"deleted"`, `unknown version state "deleted"`},
		{"retired state", `"state":"retained"`, `"state":"retired"`, "page_daily@v1 is retired, so only the retired list names it"},
		{"unsorted retired list", `"page_hourly@v1","page_hourly@v2"`, `"page_hourly@v2","page_hourly@v1"`, "not in canonical form"},
		{"explicit zero rebuild count", `"mode":"sync",`, `"mode":"sync","rebuild":0,`, "not in canonical form"},
		{"plural interval unit", `"log_retention":"2 DAY"`, `"log_retention":"2 DAYS"`, `invalid interval "2 DAYS"`},
		{"zero retention", `"log_retention":"2 DAY"`, `"log_retention":"0 DAY"`, `invalid log retention "0 DAY"`},
		{"retention in minutes", `"log_retention":"2 DAY"`, `"log_retention":"90 MINUTE"`, `invalid log retention "90 MINUTE"`},
		{"missing retention", `"log_retention":"2 DAY",`, ``, "log retention is not set"},
		{"unsupported storage format", `"storage_format":1`, `"storage_format":2`, "storage format 2 is not supported"},
		{"missing storage format", `,"storage_format":1`, ``, "storage format 0 is not supported"},
		{"key in another case", `"store":"analytics"`, `"STORE":"analytics"`, "not in canonical form"},
		{"duplicate key", `{"format":1,`, `{"format":2,"format":1,`, "not in canonical form"},
		{"explicit zero index", `{"op":"aggregate","type":"UInt64"}`, `{"op":"aggregate","type":"UInt64","index":0}`,
			"not in canonical form"},
		{"unsorted list", `["us-east-1","us-west-2"]`, `["us-west-2","us-east-1"]`, "not in canonical form"},
		{"escaped character", `"name":"page"`, `"name":"\u0070age"`, "not in canonical form"},
		{"whitespace", `{"format":1,`, `{ "format":1,`, "not in canonical form"},
	}
	data := string(marshalFixture(t))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if !strings.Contains(data, tt.old) {
				t.Fatalf("catalog JSON has no %s", tt.old)
			}
			_, err := catalog.Unmarshal([]byte(strings.Replace(data, tt.old, tt.new, 1)))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Unmarshal error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}

	t.Run("trailing data", func(t *testing.T) {
		t.Parallel()
		for _, tail := range []string{"{}", "\n"} {
			if _, err := catalog.Unmarshal([]byte(data + tail)); err == nil {
				t.Errorf("Unmarshal accepted %q after the catalog", tail)
			}
		}
	})
}

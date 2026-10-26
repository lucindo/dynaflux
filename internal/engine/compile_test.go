// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine_test

import (
	"fmt"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/validate"
)

// TestCompileAcceptsEveryAggregateValidateAccepts checks that the aggregates validate
// accepts, each over a column of every type, compile: else generate would embed a catalog
// that LoadCatalog rejects, and validate would size its rows wrong (DF3307).
func TestCompileAcceptsEveryAggregateValidateAccepts(t *testing.T) {
	t.Parallel()
	types := []string{
		"String", "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64", "Float64",
		"Decimal(10, 2)", "Decimal(38, 4)", "Bool", "UUID", "Date", "DateTime64(3)",
		"Nullable(String)", "Nullable(Int64)", "Array(String)",
	}
	aggregates := []struct{ fn, params, args string }{
		{"sum", "", "x"}, {"avg", "", "x"}, {"min", "", "x"}, {"max", "", "x"}, {"any", "", "x"},
		{"argMax", "", "x, n"}, {"argMax", "", "k, x"}, {"argMin", "", "x, n"}, {"argMin", "", "k, x"},
		{"uniq", "", "x"}, {"uniqExact", "", "x"}, {"quantile", "(0.5)", "x"}, {"quantileDD", "(0.01, 0.5)", "x"},
	}
	accepted := 0
	for _, typ := range types {
		for _, a := range aggregates {
			for _, call := range []string{
				fmt.Sprintf("%s%s(%s)", a.fn, a.params, a.args),
				fmt.Sprintf("%sIf%s(%s, n > 0)", a.fn, a.params, a.args),
			} {
				sql := fmt.Sprintf("CREATE STREAM s (k String, n Int64, x %s) PRIMARY KEY (k);\n"+
					"CREATE MATERIALIZED VIEW v ORDER BY (k) AS SELECT k, %s AS a FROM s GROUP BY k;\n", typ, call)
				fsys := fstest.MapFS{
					"dynaflux.yaml":      {Data: []byte("store: app\nregions: [us-east-1]\n")},
					"dynaflux/store.sql": {Data: []byte(sql)},
				}
				_, def, ds := validate.Load(fsys, "/project")
				if diag.HasErrors(ds) {
					continue
				}
				accepted++
				data, err := catalog.Marshal(def)
				if err == nil {
					_, err = engine.Compile(data)
				}
				if err != nil {
					t.Errorf("%s over %s: validate accepts it, but it doesn't compile: %v", call, typ, err)
				}
			}
		}
	}
	// The rest have argument types that sql.md §7.1 rejects.
	if accepted != 404 {
		t.Errorf("validate accepted %d of the aggregates, want 404", accepted)
	}
}

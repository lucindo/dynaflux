// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package archive_test

import (
	"bytes"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/validate"
)

// The type map of docs/spec/infra.md §7.3 and the dedup view's DEFAULT literals, one column
// at a time.
func TestColumnTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		decl     string
		wantType string
		// wantDefault is the default literal, empty when the column has none.
		wantDefault string
	}{
		{"String", "string", ""},
		{"LowCardinality(String)", "string", ""},
		{"UUID", "string", ""},
		{"UInt8", "int", ""},
		{"UInt16", "int", ""},
		{"Int8", "int", ""},
		{"Int16", "int", ""},
		{"Int32", "int", ""},
		{"UInt32", "bigint", ""},
		{"Int64", "bigint", ""},
		{"UInt64", "decimal(20,0)", ""},
		{"Float64", "double", ""},
		{"Decimal(10, 2)", "decimal(10,2)", ""},
		{"Decimal(38, 0)", "decimal(38,0)", ""},
		{"Bool", "boolean", ""},
		{"Date", "date", ""},
		{"DateTime64(3)", "timestamp", ""},
		{"DateTime64(6)", "timestamp", ""},
		{"Nullable(UInt64)", "decimal(20,0)", ""},
		{"Nullable(String) DEFAULT NULL", "string", ""},
		{"LowCardinality(Nullable(String))", "string", ""},
		{"Array(String)", "array<string>", ""},
		{"Array(Decimal(10, 2))", "array<decimal(10,2)>", ""},
		{"Array(DateTime64(6))", "array<timestamp>", ""},
		{"UInt8 DEFAULT 0", "int", "INTEGER '0'"},
		{"Int32 DEFAULT -2147483648", "int", "INTEGER '-2147483648'"},
		{"UInt32 DEFAULT 4294967295", "bigint", "BIGINT '4294967295'"},
		{"Int64 DEFAULT -9223372036854775808", "bigint", "BIGINT '-9223372036854775808'"},
		{"UInt64 DEFAULT 18446744073709551615", "decimal(20,0)", "DECIMAL '18446744073709551615'"},
		{"Decimal(10, 2) DEFAULT -0.05", "decimal(10,2)", "DECIMAL '-0.05'"},
		{"Decimal(5, 2) DEFAULT 1.2", "decimal(5,2)", "DECIMAL '1.20'"},
		{"Float64 DEFAULT 1.5", "double", "DOUBLE '1.5'"},
		{"Float64 DEFAULT 1e21", "double", "DOUBLE '1e+21'"},
		{"Bool DEFAULT true", "boolean", "true"},
		{"Date DEFAULT '2024-05-31'", "date", "DATE '2024-05-31'"},
		{"DateTime64(3) DEFAULT '2024-05-31'", "timestamp", "TIMESTAMP '2024-05-31 00:00:00.000'"},
		{"DateTime64(6) DEFAULT '2024-05-31 13:45:00.000123'", "timestamp", "TIMESTAMP '2024-05-31 13:45:00.000123'"},
		{"UUID DEFAULT '61F0C404-5CB3-11E7-907B-A6006AD3DBA0'", "string", "'61f0c404-5cb3-11e7-907b-a6006ad3dba0'"},
		{"String DEFAULT 'it''s'", "string", "'it''s'"},
		{`String DEFAULT 'tab\there\\'`, "string", `U&'tab\0009here\\'`},
		{`String DEFAULT 'nul\0, quote '''`, "string", `U&'nul\0000, quote '''`},
		{"Array(String) DEFAULT []", "array<string>", "ARRAY[]"},
		{"Array(String) DEFAULT ['a', 'b''c']", "array<string>", "ARRAY['a', 'b''c']"},
		{"Array(UInt64) DEFAULT [1, 18446744073709551615]", "array<decimal(20,0)>",
			"ARRAY[DECIMAL '1', DECIMAL '18446744073709551615']"},
		{"Array(Date) DEFAULT ['2024-05-31']", "array<date>", "ARRAY[DATE '2024-05-31']"},
	}
	for _, tt := range tests {
		t.Run(tt.decl, func(t *testing.T) {
			t.Parallel()
			cat := load(t, "CREATE STREAM s (k String, c "+tt.decl+") PRIMARY KEY (k);")
			table := archive.SchemaFor(cat, "us-east-1").Tables[0]
			if err := table.Check(); err != nil {
				t.Errorf("the generated table fails its check: %v", err)
			}
			cols := table.Columns
			i := slices.IndexFunc(cols, func(c archive.Column) bool { return c.Name == "c" })
			got := cols[i]
			gotDefault := ""
			if got.Default != nil {
				gotDefault = *got.Default
			}
			if got.Type != tt.wantType || gotDefault != tt.wantDefault {
				t.Errorf("got type %q, default %q; want %q, %q", got.Type, gotDefault, tt.wantType, tt.wantDefault)
			}
		})
	}
}

func TestSchemaFor(t *testing.T) {
	t.Parallel()
	cat := loadProject(t, fstest.MapFS{
		"dynaflux.yaml": {Data: []byte(`store: analytics
regions: [us-east-1, us-west-2]
archive:
  regions: [us-east-1]
  streams:
    debug:
      enabled: false
`)},
		"dynaflux/streams.sql": {Data: []byte(`
CREATE STREAM page_views (path String, user_id UInt64) PRIMARY KEY (path);
CREATE STREAM page (path String, clicks UInt32 DEFAULT 0, referrer Nullable(String)) PRIMARY KEY (path);
CREATE STREAM debug (msg String) PRIMARY KEY (msg);
`)},
	})

	t.Run("archiving region", func(t *testing.T) {
		t.Parallel()
		s := archive.SchemaFor(cat, "us-east-1")
		var names []string
		for _, tb := range s.Tables {
			names = append(names, tb.Name)
		}
		if want := []string{"page", "page_views"}; !slices.Equal(names, want) {
			t.Fatalf("tables %q, want %q", names, want)
		}
		want := []archive.Column{
			{Name: "event_id", Type: "string"},
			{Name: "event_time", Type: "timestamp"},
			{Name: "ingest_time", Type: "timestamp"},
			{Name: "origin_region", Type: "string"},
			{Name: "path", Type: "string"},
			{Name: "clicks", Type: "bigint", Default: new("BIGINT '0'")},
			{Name: "referrer", Type: "string"},
			{Name: "_shard", Type: "string"},
			{Name: "_seq", Type: "string"},
			{Name: "_arrival", Type: "timestamp"},
		}
		if got := s.Tables[0].Columns; !reflect.DeepEqual(got, want) {
			t.Errorf("page columns:\n%+v\nwant:\n%+v", got, want)
		}
	})

	t.Run("region outside archive.regions", func(t *testing.T) {
		t.Parallel()
		if s := archive.SchemaFor(cat, "us-west-2"); len(s.Tables) != 0 {
			t.Errorf("got tables %+v, want none", s.Tables)
		}
	})
}

// The projector's guard compares the fingerprint that generate wrote into the template
// with the one its embedded catalog gives, so the two catalogs must agree.
func TestFingerprint(t *testing.T) {
	t.Parallel()
	const (
		streams = `
CREATE STREAM page_views (path String, user_id UInt64) PRIMARY KEY (path);
CREATE STREAM page (path String, clicks UInt32 DEFAULT 0) PRIMARY KEY (path);
`
		want = "sha256:7b5ea2253bbad04b16e0bb658cd4b62d42a4ccc36c69f85f00046c4699b284f5"
	)
	tests := []struct {
		name   string
		config string
		sql    string
		want   string
	}{
		{"base", "store: analytics\nregions: [us-east-1]\n", streams, want},
		{"another prefix", "store: analytics\nprefix: prod-\nregions: [us-east-1]\n", streams, want},
		{"streams reordered", "store: analytics\nregions: [us-east-1]\n", reorder(streams), want},
		{"column added", "store: analytics\nregions: [us-east-1]\n",
			strings.Replace(streams, "user_id UInt64", "user_id UInt64, note Nullable(String)", 1),
			"sha256:45f6552f8517645b886d25160f7935352403cca52afc2b255bcaac25b2ad9ad0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cat := loadProject(t, fstest.MapFS{
				"dynaflux.yaml":        {Data: []byte(tt.config)},
				"dynaflux/streams.sql": {Data: []byte(tt.sql)},
			})
			if got := archive.SchemaFor(cat, "us-east-1").Fingerprint(); got != tt.want {
				t.Errorf("fingerprint %s, want %s", got, tt.want)
			}
			// The runtime reads the catalog back from its JSON form, which sorts the streams.
			data, err := catalog.Marshal(cat)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := catalog.Unmarshal(data)
			if err != nil {
				t.Fatal(err)
			}
			if got := archive.SchemaFor(decoded, "us-east-1").Fingerprint(); got != tt.want {
				t.Errorf("fingerprint from the decoded catalog %s, want %s", got, tt.want)
			}
		})
	}
}

// reorder reverses the statement lines of sql.
func reorder(sql string) string {
	lines := strings.Split(strings.TrimSpace(sql), "\n")
	slices.Reverse(lines)
	return strings.Join(lines, "\n") + "\n"
}

// The setup function runs what a table holds as Athena statements, so Check refuses
// anything that SchemaFor wouldn't write.
func TestTableCheckRefuses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		table archive.Table
		want  string
	}{
		{"table name", archive.Table{Name: "page`views"}, "table name \"page`views\" isn't lowercase letters, digits and _"},
		{
			"column name", archive.Table{Name: "page", Columns: []archive.Column{{Name: "Page", Type: "string"}}},
			`table page: column name "Page" isn't lowercase letters, digits and _`,
		},
		{
			"type that ends the column list",
			archive.Table{Name: "page", Columns: []archive.Column{{Name: "c", Type: "string) LOCATION 's3://other/' --"}}},
			`table page: column c has type "string) LOCATION 's3://other/' --", which isn't an archive column type`,
		},
		{
			"nested array", archive.Table{Name: "page", Columns: []archive.Column{{Name: "c", Type: "array<array<int>>"}}},
			`table page: column c has type "array<array<int>>", which isn't an archive column type`,
		},
		{
			"default that ends its string",
			archive.Table{Name: "page", Columns: []archive.Column{{Name: "c", Type: "string", Default: new("'a' || (SELECT 1) || 'b'")}}},
			`table page: column c has DEFAULT "'a' || (SELECT 1) || 'b'", which isn't an archive literal`,
		},
		{
			"default that is an expression", archive.Table{Name: "page", Columns: []archive.Column{{Name: "c", Type: "int", Default: new("1 + 1")}}},
			`table page: column c has DEFAULT "1 + 1", which isn't an archive literal`,
		},
	}
	for _, tt := range tests {
		if err := tt.table.Check(); err == nil || err.Error() != tt.want {
			t.Errorf("%s: got %v\nwant %s", tt.name, err, tt.want)
		}
	}
}

func TestLocation(t *testing.T) {
	t.Parallel()
	const warehouse = "s3://prod-analytics-123456789012-us-east-1-an/warehouse"
	page := archive.Location(warehouse, "prod_analytics", "page")
	pageViews := archive.Location(warehouse, "prod_analytics", "page_views")
	if want := warehouse + "/prod_analytics/page/iceberg/"; page != want {
		t.Errorf("page location %q, want %q", page, want)
	}
	if want := warehouse + "/prod_analytics/page_views/iceberg/"; pageViews != want {
		t.Errorf("page_views location %q, want %q", pageViews, want)
	}
	if strings.HasPrefix(pageViews, page) || strings.HasPrefix(page, pageViews) {
		t.Errorf("locations %q and %q overlap", page, pageViews)
	}
}

func load(t *testing.T, sql string) *catalog.Catalog {
	t.Helper()
	return loadProject(t, fstest.MapFS{
		"dynaflux.yaml":  {Data: []byte("store: analytics\nregions: [us-east-1]\n")},
		"dynaflux/s.sql": {Data: []byte(sql)},
	})
}

func loadProject(tb testing.TB, fsys fstest.MapFS) *catalog.Catalog {
	tb.Helper()
	_, cat, ds := validate.Load(fsys, "/project")
	if cat == nil {
		var b bytes.Buffer
		if err := diag.Write(&b, ds); err != nil {
			tb.Fatal(err)
		}
		tb.Fatalf("the project doesn't compile:\n%s", b.Bytes())
	}
	return cat
}

// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package setup_test

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/setup"
	"github.com/lucindo/dynaflux/internal/txtar"
)

var update = flag.Bool("update", false, "rewrite the want sections from the calls and responses") //nolint:gochecknoglobals // go test flags are package variables

// glueTable is a [setup.GlueTable] as glue.json writes it.
type glueTable struct {
	Location  string            `json:"location"`
	TableType string            `json:"table_type"`
	Columns   map[string]string `json:"columns"`
}

// TestSchemaGolden applies the archive schema of each case under testdata/schema:
//   - "properties.json" holds ArchiveSchema's properties as CloudFormation sends them, and
//     "glue.json" the live Glue tables by name.
//   - "want" holds the calls that change something, in order, then the response or the
//     error. go test -update rewrites it; review the diff.
func TestSchemaGolden(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/schema/*.txtar")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no cases in testdata: %v", err)
	}
	for _, path := range paths {
		t.Run(strings.TrimSuffix(filepath.Base(path), ".txtar"), func(t *testing.T) {
			t.Parallel()
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			ar := txtar.Parse(data)
			got := applyCase(t, ar)
			if *update {
				ar.Files = slices.DeleteFunc(ar.Files, func(f txtar.File) bool { return f.Name == "want" })
				ar.Files = append(ar.Files, txtar.File{Name: "want", Data: []byte(got)})
				if writeErr := os.WriteFile(path, txtar.Format(ar), 0o644); writeErr != nil {
					t.Fatal(writeErr)
				}
				return
			}
			if want := string(section(t, ar, "want")); got != want {
				t.Errorf("got:\n%s\nwant:\n%s", got, want)
			}
		})
	}
}

func applyCase(t *testing.T, ar *txtar.Archive) string {
	t.Helper()
	var live map[string]glueTable
	if err := json.Unmarshal(section(t, ar, "glue.json"), &live); err != nil {
		t.Fatal(err)
	}
	aws := &fakeAWS{t: t, tables: map[string]setup.GlueTable{}}
	for name, g := range live {
		aws.tables[name] = setup.GlueTable(g)
	}
	h := setup.Handler{Athena: aws, Glue: aws}
	resp, err := h.Handle(t.Context(), setup.Request{
		Type: setup.TypeArchiveSchema, Action: setup.ActionUpdate, PhysicalID: database,
		Props: props(t, string(section(t, ar, "properties.json"))),
	})
	var b strings.Builder
	for _, c := range aws.calls {
		b.WriteString(c + "\n\n")
	}
	if err != nil {
		fmt.Fprintf(&b, "error: %v\n", err)
	} else {
		fmt.Fprintf(&b, "response: %s %v\n", resp.PhysicalID, resp.Data)
	}
	return b.String()
}

func section(t *testing.T, ar *txtar.Archive, name string) []byte {
	t.Helper()
	i := slices.IndexFunc(ar.Files, func(f txtar.File) bool { return f.Name == name })
	if i < 0 {
		if name == "want" && *update {
			return nil
		}
		t.Fatalf("no %s section", name)
	}
	return ar.Files[i].Data
}

const pageProps = `{
  "Database": "prod_analytics",
  "DatabaseLocation": "s3://prod-analytics-123456789012-us-east-1-an/warehouse/prod_analytics/",
  "WorkGroup": "prod-analytics",
  "OptimizerRoleArn": "arn:aws:iam::123456789012:role/prod-analytics-optimizer",
  "Optimizers": {"Compaction": "binpack", "SnapshotRetentionDays": "5", "SnapshotsToKeep": "1"},
  "Fingerprint": "sha256:3f1c",
  "Tables": [{
    "Name": "page",
    "Location": "s3://prod-analytics-123456789012-us-east-1-an/warehouse/prod_analytics/page/iceberg/",
    "Columns": [
      {"Name": "event_id", "Type": "string"},
      {"Name": "event_time", "Type": "timestamp"},
      {"Name": "clicks", "Type": "bigint", "Default": "BIGINT '0'"}
    ]
  }]
}`

func schemaRequest(t *testing.T, change func(p map[string]any)) setup.Request {
	t.Helper()
	p := props(t, pageProps)
	change(p)
	return setup.Request{Type: setup.TypeArchiveSchema, Action: setup.ActionUpdate, PhysicalID: database, Props: p}
}

func pageTable(p map[string]any) map[string]any {
	return p["Tables"].([]any)[0].(map[string]any)
}

func column(p map[string]any, i int) map[string]any {
	return pageTable(p)["Columns"].([]any)[i].(map[string]any)
}

func TestSchemaRefusesBadProperties(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(p map[string]any)
		want   string
	}{
		{"renamed tables", func(p map[string]any) { p["Table"] = p["Tables"]; delete(p, "Tables") }, "property Tables is empty"},
		{"no database", func(p map[string]any) { delete(p, "Database") }, "property Database is missing"},
		{"no database location", func(p map[string]any) { delete(p, "DatabaseLocation") }, "property DatabaseLocation is missing"},
		{"no columns", func(p map[string]any) { pageTable(p)["Columns"] = []any{} }, "table page has no columns"},
		{
			// A type or a DEFAULT becomes SQL: these would end the column list and the string.
			"type outside the archive's",
			func(p map[string]any) { column(p, 0)["Type"] = "string) LOCATION 's3://other/' --" },
			`table page: column event_id has type "string) LOCATION 's3://other/' --", which isn't an archive column type`,
		},
		{
			"DEFAULT outside the archive's", func(p map[string]any) { column(p, 2)["Default"] = "'0' || (SELECT 1)" },
			`table page: column clicks has DEFAULT "'0' || (SELECT 1)", which isn't an archive literal`,
		},
		{
			"quoted location", func(p map[string]any) { pageTable(p)["Location"] = "s3://bucket/it's/" },
			`table page: location "s3://bucket/it's/" isn't an S3 URI without quotes`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Nil interfaces: any call to AWS panics.
			h := setup.Handler{}
			if _, err := h.Handle(t.Context(), schemaRequest(t, tt.change)); err == nil || err.Error() != tt.want {
				t.Errorf("got %v\nwant %s", err, tt.want)
			}
		})
	}
}

// A deploy that turns the archive on also gives the setup role its archive grants, and
// creates the optimizers' role, which take a while to apply: every call retries for about a
// minute.
func TestSchemaRetriesAccessDenied(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		denied int
		ok     bool
	}{
		{"denied briefly", 3, true},
		{"denied for good", 1 << 20, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				aws := &fakeAWS{t: t, tables: map[string]setup.GlueTable{}, denied: tt.denied}
				h := setup.Handler{Athena: aws, Glue: aws}
				start := time.Now()
				resp, err := h.Handle(t.Context(), setup.Request{Type: setup.TypeArchiveSchema, Action: setup.ActionCreate, Props: props(t, pageProps)})
				waited := time.Since(start)
				switch {
				case tt.ok:
					// The database, the table, two optimizers and the view.
					if err != nil || resp.PhysicalID != database || len(aws.calls) != 5 {
						t.Errorf("got %+v, %v after calls %q", resp, err, aws.calls)
					}
				case !errors.Is(err, setup.ErrAccessDenied) || len(aws.calls) != 0:
					t.Errorf("got %v after calls %q, want access denied before any change", err, aws.calls)
				case waited < time.Minute*9/10 || waited > time.Minute:
					t.Errorf("gave up after %v, want about a minute", waited)
				}
			})
		})
	}
}

// Firehose commits to a table while its schema changes, so a statement that conflicts with
// a commit runs again.
func TestSchemaRetriesCommitConflict(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		aws := &fakeAWS{t: t, tables: map[string]setup.GlueTable{"page": {
			Location:  "s3://prod-analytics-123456789012-us-east-1-an/warehouse/prod_analytics/page/iceberg/",
			TableType: "ICEBERG",
			Columns:   map[string]string{"event_id": "string", "event_time": "timestamp"},
		}}}
		conflicts := 1
		aws.execErr = func(sql string) error {
			if strings.HasPrefix(sql, "ALTER TABLE") && conflicts > 0 {
				conflicts--
				return fmt.Errorf("%w: ICEBERG_COMMIT_ERROR", setup.ErrConflict)
			}
			return nil
		}
		h := setup.Handler{Athena: aws, Glue: aws}
		_, err := h.Handle(t.Context(), schemaRequest(t, func(map[string]any) {}))
		alter := "athena prod-analytics\nALTER TABLE `prod_analytics`.`page` ADD COLUMNS (`clicks` bigint)"
		if n := len(slices.DeleteFunc(slices.Clone(aws.calls), func(c string) bool { return c != alter })); err != nil || n != 2 {
			t.Errorf("got %v after calls %q; want the ALTER TABLE twice", err, aws.calls)
		}
	})
}

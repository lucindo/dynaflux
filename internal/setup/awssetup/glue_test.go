// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awssetup_test

import (
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/lucindo/dynaflux/internal/setup"
)

const (
	database   = "prod_analytics"
	archive    = "page_views"
	location   = "s3://prod-analytics-123456789012-us-east-1-an/warehouse/prod_analytics/page_views/iceberg/"
	role       = "arn:aws:iam::123456789012:role/prod-analytics-regional-RoleArchiveOptimizer-1A2B3C4D5E6F"
	glueDenied = "User: arn:aws:sts::123456789012:assumed-role/prod-analytics-setup/prod-analytics-setup is not authorized " +
		"to perform: glue:GetTable on resource: arn:aws:glue:us-east-1:123456789012:catalog"
)

// icebergTableBody is GetTable's answer for an archive table that Athena created.
const icebergTableBody = `{"Table":{
  "CatalogId":"123456789012",
  "CreateTime":1.761480000E9,
  "CreatedBy":"arn:aws:sts::123456789012:assumed-role/prod-analytics-setup/prod-analytics-setup",
  "DatabaseName":"prod_analytics",
  "IsMultiDialectView":false,
  "IsRegisteredWithLakeFormation":false,
  "Name":"page_views",
  "Parameters":{
    "metadata_location":"s3://prod-analytics-123456789012-us-east-1-an/warehouse/prod_analytics/page_views/iceberg/metadata/00001-2b3c4d5e-6f70-4812-9a3b-4c5d6e7f8091.metadata.json",
    "previous_metadata_location":"s3://prod-analytics-123456789012-us-east-1-an/warehouse/prod_analytics/page_views/iceberg/metadata/00000-1a2b3c4d-5e6f-4701-8a2b-3c4d5e6f7080.metadata.json",
    "table_type":"ICEBERG"
  },
  "PartitionKeys":[],
  "Retention":0,
  "StorageDescriptor":{
    "BucketColumns":[],
    "Columns":[
      {"Name":"event_id","Parameters":{"iceberg.field.current":"true","iceberg.field.id":"1","iceberg.field.optional":"true"},"Type":"string"},
      {"Name":"event_time","Parameters":{"iceberg.field.current":"true","iceberg.field.id":"2","iceberg.field.optional":"true"},"Type":"timestamp"},
      {"Name":"amount","Parameters":{"iceberg.field.current":"true","iceberg.field.id":"3","iceberg.field.optional":"true"},"Type":"decimal(20,0)"}
    ],
    "Compressed":false,
    "Location":"` + location + `",
    "NumberOfBuckets":0,
    "SerdeInfo":{},
    "SortColumns":[],
    "StoredAsSubDirectories":false
  },
  "TableType":"EXTERNAL_TABLE",
  "UpdateTime":1.761480060E9,
  "VersionId":"1"
}}`

// hiveTableBody is GetTable's answer for a Hive table in the archive's place, which has no
// table_type.
const hiveTableBody = `{"Table":{
  "CatalogId":"123456789012",
  "DatabaseName":"prod_analytics",
  "Name":"page_views",
  "Parameters":{"EXTERNAL":"TRUE","classification":"parquet"},
  "PartitionKeys":[],
  "StorageDescriptor":{
    "Columns":[{"Name":"event_id","Type":"string"}],
    "InputFormat":"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "Location":"s3://prod-analytics-123456789012-us-east-1-an/warehouse/prod_analytics/page_views/",
    "OutputFormat":"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo":{"SerializationLibrary":"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}
  },
  "TableType":"EXTERNAL_TABLE"
}}`

func TestCreateDatabase(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		answer answer
		denied bool
	}{
		{"created", ok(`{}`), false},
		{"exists", awsError(http.StatusBadRequest, "AlreadyExistsException", "Database already exists."), false},
		{"access denied", awsError(http.StatusBadRequest, "AccessDeniedException", glueDenied), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFakeAWS(t, map[string][]answer{"CreateDatabase": {tt.answer}})
				err := handler(f).Glue.CreateDatabase(t.Context(), database, "s3://bucket/warehouse/prod_analytics/")
				if (err != nil) != tt.denied || errors.Is(err, setup.ErrAccessDenied) != tt.denied {
					t.Errorf("got %v, want access denied %v", err, tt.denied)
				}
				want := `{"DatabaseInput":{"LocationUri":"s3://bucket/warehouse/prod_analytics/","Name":"prod_analytics"}}`
				if calls := f.recorded(); len(calls) != 1 || compact(t, calls[0].body) != want {
					t.Errorf("calls %+v, want one CreateDatabase %s", calls, want)
				}
			})
		})
	}
}

func TestTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		answer answer
		want   setup.GlueTable
		found  bool
		denied bool
	}{
		{
			"Iceberg table", ok(icebergTableBody),
			setup.GlueTable{Location: location, TableType: "ICEBERG", Columns: map[string]string{
				"event_id": "string", "event_time": "timestamp", "amount": "decimal(20,0)",
			}},
			true, false,
		},
		{
			// Glue keeps a dropped column, marked as not current.
			"dropped column", ok(strings.Replace(icebergTableBody, `"iceberg.field.current":"true","iceberg.field.id":"3"`,
				`"iceberg.field.current":"false","iceberg.field.id":"3"`, 1)),
			setup.GlueTable{Location: location, TableType: "ICEBERG", Columns: map[string]string{
				"event_id": "string", "event_time": "timestamp",
			}},
			true, false,
		},
		{
			"Hive table", ok(hiveTableBody),
			setup.GlueTable{
				Location: "s3://prod-analytics-123456789012-us-east-1-an/warehouse/prod_analytics/page_views/",
				Columns:  map[string]string{"event_id": "string"},
			},
			true, false,
		},
		{
			"missing", awsError(http.StatusBadRequest, "EntityNotFoundException", "Table page_views not found."),
			setup.GlueTable{}, false, false,
		},
		{"access denied", awsError(http.StatusBadRequest, "AccessDeniedException", glueDenied), setup.GlueTable{}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFakeAWS(t, map[string][]answer{"GetTable": {tt.answer}})
				got, found, err := handler(f).Glue.Table(t.Context(), database, archive)
				if !reflect.DeepEqual(got, tt.want) || found != tt.found || (err != nil) != tt.denied || errors.Is(err, setup.ErrAccessDenied) != tt.denied {
					t.Errorf("got %+v, %v, %v\nwant %+v, %v, access denied %v", got, found, err, tt.want, tt.found, tt.denied)
				}
				if calls := f.recorded(); len(calls) != 1 || calls[0].body != `{"DatabaseName":"prod_analytics","Name":"page_views"}` {
					t.Errorf("calls %+v, want one GetTable of prod_analytics.page_views", calls)
				}
			})
		})
	}
}

// The optimizers' configurations as Glue's API takes them.
const (
	compactionBody = `{"CatalogId":"123456789012","DatabaseName":"prod_analytics","TableName":"page_views",` +
		`"TableOptimizerConfiguration":{"compactionConfiguration":{"icebergConfiguration":{"strategy":"binpack"}},` +
		`"enabled":true,"roleArn":"` + role + `"},"Type":"compaction"}`
	retentionBody = `{"CatalogId":"123456789012","DatabaseName":"prod_analytics","TableName":"page_views",` +
		`"TableOptimizerConfiguration":{"enabled":true,"retentionConfiguration":{"icebergConfiguration":{` +
		`"cleanExpiredFiles":true,"numberOfSnapshotsToRetain":1,"snapshotRetentionPeriodInDays":5}},"roleArn":"` + role + `"},` +
		`"Type":"retention"}`
)

var (
	compaction = setup.Optimizer{Type: setup.OptimizerCompaction, Role: role, Strategy: "binpack"}                 //nolint:gochecknoglobals // test fixture
	retention  = setup.Optimizer{Type: setup.OptimizerRetention, Role: role, RetentionDays: 5, SnapshotsToKeep: 1} //nolint:gochecknoglobals // test fixture
)

func TestPutOptimizer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		optimizer setup.Optimizer
		create    answer
		calls     []string // operation and body
		err       error    // the error wraps it; nil for success
	}{
		{"created", compaction, ok(`{}`), []string{"CreateTableOptimizer " + compactionBody}, nil},
		{
			"exists, so updated", retention,
			awsError(http.StatusBadRequest, "AlreadyExistsException", "Table optimizer already exists."),
			[]string{"CreateTableOptimizer " + retentionBody, "UpdateTableOptimizer " + retentionBody}, nil,
		},
		{
			"concurrent change", compaction,
			awsError(http.StatusBadRequest, "ConcurrentModificationException", "Update table optimizer failed due to concurrent modifications."),
			[]string{"CreateTableOptimizer " + compactionBody}, setup.ErrConflict,
		},
		{
			// The deploy created the role seconds ago. Glue's exact code for this is
			// unconfirmed; its message names the role.
			"role not assumable yet", compaction,
			awsError(http.StatusBadRequest, "InvalidInputException", "Glue cannot assume role "+role+"."),
			[]string{"CreateTableOptimizer " + compactionBody}, setup.ErrAccessDenied,
		},
		{
			"cannot assume", compaction,
			awsError(http.StatusBadRequest, "ValidationException", "Unable to assume the provided role."),
			[]string{"CreateTableOptimizer " + compactionBody}, setup.ErrAccessDenied,
		},
		{"access denied", compaction, awsError(http.StatusBadRequest, "AccessDeniedException", glueDenied),
			[]string{"CreateTableOptimizer " + compactionBody}, setup.ErrAccessDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFakeAWS(t, map[string][]answer{"CreateTableOptimizer": {tt.create}, "UpdateTableOptimizer": {ok(`{}`)}})
				err := handler(f).Glue.PutOptimizer(t.Context(), database, archive, tt.optimizer)
				if (err == nil) != (tt.err == nil) || tt.err != nil && !errors.Is(err, tt.err) {
					t.Errorf("got %v, want %v", err, tt.err)
				}
				var calls []string
				for _, c := range f.recorded() {
					calls = append(calls, c.op+" "+compact(t, c.body))
				}
				if !slices.Equal(calls, tt.calls) {
					t.Errorf("calls\n%q\nwant\n%q", calls, tt.calls)
				}
			})
		})
	}
}

// Invalid input that isn't about the role stays an error that the handler doesn't retry.
func TestPutOptimizerInvalidInput(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newFakeAWS(t, map[string][]answer{"CreateTableOptimizer": {
			awsError(http.StatusBadRequest, "InvalidInputException", "Compaction strategy sort requires a sort order on the table."),
		}})
		err := handler(f).Glue.PutOptimizer(t.Context(), database, archive, compaction)
		if err == nil || errors.Is(err, setup.ErrAccessDenied) || errors.Is(err, setup.ErrConflict) {
			t.Errorf("got %v, want an error that is neither access denied nor a conflict", err)
		}
	})
}

func TestPutOptimizerRefusesBadInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		o    setup.Optimizer
		want string
	}{
		{"role", setup.Optimizer{Type: setup.OptimizerCompaction, Role: "prod-analytics-optimizer"},
			`optimizer role "prod-analytics-optimizer" isn't an IAM role ARN`},
		{"user", setup.Optimizer{Type: setup.OptimizerCompaction, Role: "arn:aws:iam::123456789012:user/deployer"},
			`optimizer role "arn:aws:iam::123456789012:user/deployer" isn't an IAM role ARN`},
		{"service", setup.Optimizer{Type: setup.OptimizerCompaction, Role: "arn:aws:sts::123456789012:role/r"},
			`optimizer role "arn:aws:sts::123456789012:role/r" isn't an IAM role ARN`},
		{"type", setup.Optimizer{Type: "orphan_file_deletion", Role: role}, `unknown optimizer type "orphan_file_deletion"`},
		{"retention", setup.Optimizer{Type: setup.OptimizerRetention, Role: role, RetentionDays: 1 << 31, SnapshotsToKeep: 1},
			"retention of 2147483648 days keeping 1 snapshots is out of range"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFakeAWS(t, nil)
			err := handler(f).Glue.PutOptimizer(t.Context(), database, archive, tt.o)
			if err == nil || err.Error() != tt.want || len(f.recorded()) != 0 {
				t.Errorf("got %v after calls %v, want %s and no call", err, f.ops(), tt.want)
			}
		})
	}
}

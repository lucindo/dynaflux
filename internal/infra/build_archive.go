// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"slices"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
)

// Fixed values of the archive (docs/spec/infra.md §8).
const (
	// Firehose's 5 MiB default would commit an Iceberg snapshot every few seconds.
	archiveBufferSeconds   = 300
	archiveBufferMegabytes = 128
	archiveRetrySeconds    = 7200
	snapshotRetentionDays  = 5
	snapshotsToKeep        = 1
)

// addArchive adds the archive to s: the Athena workgroup, the archive schema with its
// optimizer role, and a delivery stream per archived stream with their log group, log
// streams and role.
func (b *builder) addArchive(s *Scope) {
	db := b.archiveDatabase()
	warehouse := "s3://" + string(ref(idBucket, AttrName)) + "/warehouse"
	common := func(id string, tags map[string]string) Common {
		return Common{ID: id, ArchiveOnly: b.archiveOnly, Tags: tags}
	}

	s.ArchiveWorkGroups = []ArchiveWorkGroup{{
		Common: common(idArchiveWorkGroup, b.tags(TagKindArchive, "")),
		Name:   b.name(catalog.ResourceArchiveWorkGroup, b.naming.ArchiveWorkGroup()),
	}}
	schema := ArchiveSchema{
		Common:           common(idArchiveSchema, nil),
		Handler:          ref(idSetup, AttrARN),
		Database:         db,
		DatabaseLocation: Text(warehouse + "/" + string(db) + "/"),
		WorkGroup:        ref(idArchiveWorkGroup, AttrName),
		Optimizers: Optimizers{
			Role: ref(roleID(idArchiveOptimizer), AttrARN), Compaction: CompactionBinpack,
			SnapshotRetentionDays: snapshotRetentionDays, SnapshotsToKeep: snapshotsToKeep,
		},
		Fingerprint: b.schema.Fingerprint(),
		Deletion:    DeletionRetain,
	}
	for _, t := range b.schema.Tables {
		schema.Tables = append(schema.Tables, ArchiveTable{
			Name: t.Name, Location: Text(archive.Location(warehouse, string(db), t.Name)), Columns: t.Columns,
		})
	}
	for _, l := range s.StreamLookups {
		schema.After = append(schema.After, l.ID)
	}
	slices.Sort(schema.After)
	s.ArchiveSchemas = []ArchiveSchema{schema}

	logs := LogGroup{
		Common:        common(logsID(idArchive), b.tags(TagKindArchive, "")),
		Name:          b.name(catalog.ResourceLogGroup, b.naming.ArchiveLogGroup()),
		RetentionDays: logRetentionDays,
	}
	s.LogGroups = append(s.LogGroups, logs)
	s.Roles = append(s.Roles, deliveryRole(common(roleID(idArchive), b.tags(TagKindArchive, "")), db),
		optimizerRole(common(roleID(idArchiveOptimizer), b.tags(TagKindArchive, "")), db))
	for _, st := range b.cat.Streams {
		if st.Archive == nil {
			continue
		}
		logStream := LogStream{Common: common(logStreamID(st.Name), nil), Group: ref(logs.ID, AttrName), Name: st.Name}
		s.LogStreams = append(s.LogStreams, logStream)
		s.DeliveryStreams = append(s.DeliveryStreams, DeliveryStream{
			Common:          common(deliveryStreamID(st.Name), b.tags(TagKindArchive, st.Name)),
			Name:            b.name(catalog.ResourceArchiveStream, b.naming.ArchiveStream(st.Name)),
			Schema:          idArchiveSchema,
			Catalog:         regionalARN("glue", "catalog"),
			Database:        db,
			Table:           catalog.IcebergTable(st.Name),
			Role:            ref(roleID(idArchive), AttrARN),
			Bucket:          ref(idBucket, AttrARN),
			ErrorPrefix:     firehoseErrorPrefix(st.Name),
			LogGroup:        ref(logs.ID, AttrName),
			LogStream:       ref(logStream.ID, AttrName),
			AppendOnly:      true,
			BufferSeconds:   archiveBufferSeconds,
			BufferMegabytes: archiveBufferMegabytes,
			RetrySeconds:    archiveRetrySeconds,
		})
	}
}

// archiveDatabase returns the name of the Glue database of the archive tables.
func (b *builder) archiveDatabase() Text {
	return b.name(catalog.ResourceArchiveDatabase,
		catalog.Naming{Prefix: placeholder(PlaceholderPrefixUnderscore), Store: b.cat.Store}.ArchiveDatabase())
}

// firehoseErrorPrefix is where in the bucket Firehose writes the records of stream that it
// fails to deliver.
func firehoseErrorPrefix(stream string) string {
	return "errors/firehose/" + stream + "/"
}

// deliveryRole returns the role of every delivery stream. It covers all the database's
// tables, so it doesn't grow with the streams, and a delivery stream that still drains after
// its table's entry is gone keeps working. s3:DeleteObject follows AWS's Firehose-to-Iceberg
// sample: without it a failure would show only as rows under errors/, and bucket versioning
// keeps what it deletes.
func deliveryRole(c Common, db Text) Role {
	return Role{
		Common:  c,
		Service: ServiceFirehose,
		Trust:   []Condition{{Operator: condStringEquals, Key: "sts:ExternalId", Values: []Text{Text(placeholder(PlaceholderAccount))}}},
		Statements: []Statement{
			{Sid: "Catalog", Actions: []string{glueGetDatabase, glueGetTable, glueUpdateTable}, Resources: archiveCatalog(db)},
			{
				Sid:       "Files",
				Actions:   []string{"s3:AbortMultipartUpload", "s3:DeleteObject", s3GetObject, s3PutObject},
				Resources: []Text{warehouseObjects(db), ref(idBucket, AttrARN) + "/errors/firehose/*"},
			},
			{
				Sid:       "Bucket",
				Actions:   []string{"s3:GetBucketLocation", s3ListBucket, "s3:ListBucketMultipartUploads"},
				Resources: []Text{ref(idBucket, AttrARN)},
			},
			{Sid: sidLogs, Actions: []string{logsPutLogEvents}, Resources: []Text{ref(logsID(idArchive), AttrARN)}},
		},
	}
}

// optimizerRole returns the role that Glue's table optimizers run as. Glue creates their log
// groups itself, shared by every store in the account and region.
func optimizerRole(c Common, db Text) Role {
	logGroup := func(name string) Text {
		return regionalARN("logs", Text("log-group:"+name+":*"))
	}
	return Role{
		Common:  c,
		Service: ServiceGlue,
		Statements: []Statement{
			{Sid: "Files", Actions: []string{"s3:DeleteObject", s3GetObject, s3PutObject}, Resources: []Text{warehouseObjects(db)}},
			{Sid: "List", Actions: []string{s3ListBucket}, Resources: []Text{ref(idBucket, AttrARN)}, Conditions: warehouseListing(db)},
			{Sid: "Catalog", Actions: []string{glueGetTable, glueUpdateTable}, Resources: archiveCatalog(db)},
			{
				Sid:       sidLogs,
				Actions:   []string{"logs:CreateLogGroup", "logs:CreateLogStream", logsPutLogEvents},
				Resources: []Text{logGroup("/aws-glue/iceberg-compaction/logs"), logGroup("/aws-glue/iceberg-retention/logs")},
			},
		},
	}
}

// setupArchiveStatements returns what the setup function needs to apply the archive schema
// with Athena and Glue (ADR 0008). It has no s3:DeleteObject: only the optimizers delete
// archive files.
func (b *builder) setupArchiveStatements() []Statement {
	db := b.archiveDatabase()
	statements := []Statement{
		{
			Sid:       "ArchiveQueries",
			Actions:   []string{"athena:GetQueryExecution", "athena:GetQueryResults", "athena:StartQueryExecution", "athena:StopQueryExecution"},
			Resources: []Text{regionalARN("athena", "workgroup/"+ref(idArchiveWorkGroup, AttrName))},
		},
		// Athena needs these on the default database in every region.
		{
			Sid:       "ArchiveAthenaCatalog",
			Actions:   []string{glueCreateDatabase, glueGetDatabase, "glue:GetDatabases"},
			Resources: []Text{regionalARN("glue", "catalog"), regionalARN("glue", "database/default")},
		},
		{
			Sid: "ArchiveCatalog",
			Actions: []string{
				glueCreateDatabase, "glue:CreateTable", "glue:CreateTableOptimizer", glueGetDatabase, "glue:GetDatabases",
				"glue:GetPartitions", glueGetTable, "glue:GetTableOptimizer", "glue:GetTables", glueUpdateTable,
				"glue:UpdateTableOptimizer",
			},
			Resources: archiveCatalog(db),
		},
		{
			Sid: "ArchivePassOptimizerRole", Actions: []string{"iam:PassRole"},
			Resources:  []Text{ref(roleID(idArchiveOptimizer), AttrARN)},
			Conditions: []Condition{{Operator: condStringEquals, Key: "iam:PassedToService", Values: []Text{ServiceGlue}}},
		},
		{Sid: "ArchiveFiles", Actions: []string{s3GetObject, s3PutObject}, Resources: []Text{warehouseObjects(db)}},
		{Sid: "ArchiveList", Actions: []string{s3ListBucket}, Resources: []Text{ref(idBucket, AttrARN)}, Conditions: warehouseListing(db)},
		// An s3:prefix condition would never match it: the request has no prefix.
		{Sid: "ArchiveBucketLocation", Actions: []string{"s3:GetBucketLocation"}, Resources: []Text{ref(idBucket, AttrARN)}},
	}
	for i := range statements {
		statements[i].ArchiveOnly = b.archiveOnly
	}
	return statements
}

// regionalARN returns the ARN of a resource of service in the scope's region and account.
func regionalARN(service string, resource Text) Text {
	return Text("arn:"+placeholder(PlaceholderPartition)+":"+service+":"+placeholder(PlaceholderRegion)+":"+
		placeholder(PlaceholderAccount)+":") + resource
}

// archiveCatalog returns the Glue catalog, the archive database db and its tables.
func archiveCatalog(db Text) []Text {
	return []Text{regionalARN("glue", "catalog"), regionalARN("glue", "database/"+db), regionalARN("glue", "table/"+db+"/*")}
}

// warehouseObjects returns the objects of the tables of the archive database db.
func warehouseObjects(db Text) Text {
	return ref(idBucket, AttrARN) + "/warehouse/" + db + "/*"
}

// warehouseListing limits s3:ListBucket to the tables of the archive database db.
func warehouseListing(db Text) []Condition {
	return []Condition{{Operator: "StringLike", Key: "s3:prefix", Values: []Text{"warehouse/" + db, "warehouse/" + db + "/*"}}}
}

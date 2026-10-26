// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/setup"
)

// addArchive adds the archive resources of s.
func addArchive(rs *resources, r *resolver, s infra.Scope) {
	for _, x := range s.ArchiveWorkGroups {
		rs.add(x.ID, func() *yaml.Node { return archiveWorkGroup(r, x) })
	}
	for _, x := range s.ArchiveSchemas {
		rs.add(x.ID, func() *yaml.Node { return archiveSchema(r, x) })
	}
	for _, x := range s.DeliveryStreams {
		rs.add(x.ID, func() *yaml.Node { return deliveryStream(r, x) })
	}
}

// dependsOn returns the DependsOn attribute, or nothing without ids. Only resources in the
// same regions as the dependent resource, or in every region, may be listed: CloudFormation
// leaves out a resource that depends on one whose condition is false.
func dependsOn(ids ...string) pair {
	if len(ids) == 0 {
		return kv("DependsOn", nil)
	}
	n := seq()
	for _, id := range ids {
		n.Content = append(n.Content, str(id))
	}
	return kv("DependsOn", n)
}

// archiveWorkGroup returns the workgroup. RecursiveDeleteOption lets CloudFormation delete
// it: Athena refuses to delete a workgroup with any query history otherwise.
func archiveWorkGroup(r *resolver, x infra.ArchiveWorkGroup) *yaml.Node {
	return resource("AWS::Athena::WorkGroup", mapping(
		kv("Name", r.value(x.Name)),
		kv("RecursiveDeleteOption", boolean(true)),
		kv("WorkGroupConfiguration", mapping(
			kv("EnforceWorkGroupConfiguration", boolean(true)),
			kv("EngineVersion", mapping(kv("SelectedEngineVersion", str("Athena engine version 3")))),
			kv("ManagedQueryResultsConfiguration", mapping(kv("Enabled", boolean(true)))),
		)),
		kv("Tags", tags(x.Tags)),
	), archiveCondition(x.Common)...)
}

// archiveSchema returns the custom resource that the setup function answers by applying the
// schema (ADR 0008). Its properties are the whole desired state, so a change set shows a
// schema change. Defaults are plain strings, never !Sub, so "${" in one stays as it is.
func archiveSchema(r *resolver, x infra.ArchiveSchema) *yaml.Node {
	tables := seq()
	for _, t := range x.Tables {
		columns := seq()
		for _, c := range t.Columns {
			var def *yaml.Node
			if c.Default != nil {
				def = str(*c.Default)
			}
			columns.Content = append(columns.Content, mapping(kv("Name", str(c.Name)), kv("Type", str(c.Type)), kv("Default", def)))
		}
		tables.Content = append(tables.Content, mapping(
			kv("Name", str(t.Name)),
			kv("Location", r.value(t.Location)),
			kv("Columns", columns),
		))
	}
	attrs := append(archiveCondition(x.Common), dependsOn(x.After...))
	return resource(setup.TypeArchiveSchema, mapping(
		kv("ServiceToken", r.value(x.Handler)),
		kv("ServiceTimeout", num(archiveSchemaTimeoutSeconds)),
		kv("Database", r.value(x.Database)),
		kv("DatabaseLocation", r.value(x.DatabaseLocation)),
		kv("WorkGroup", r.value(x.WorkGroup)),
		kv("OptimizerRoleArn", r.value(x.Optimizers.Role)),
		kv("Optimizers", mapping(
			kv("Compaction", str(x.Optimizers.Compaction)),
			kv("SnapshotRetentionDays", num(x.Optimizers.SnapshotRetentionDays)),
			kv("SnapshotsToKeep", num(x.Optimizers.SnapshotsToKeep)),
		)),
		kv("Fingerprint", str(x.Fingerprint)),
		kv("Tables", tables),
	), append(attrs, deletion(x.Deletion)...)...)
}

// deliveryStream returns a Direct PUT Firehose stream that writes to an Iceberg table in the
// Glue Data Catalog. It sends only the records that fail to the bucket.
func deliveryStream(r *resolver, x infra.DeliveryStream) *yaml.Node {
	return resource("AWS::KinesisFirehose::DeliveryStream", mapping(
		kv("DeliveryStreamName", r.value(x.Name)),
		kv("DeliveryStreamType", str("DirectPut")),
		kv("IcebergDestinationConfiguration", mapping(
			kv("AppendOnly", boolean(x.AppendOnly)),
			kv("CatalogConfiguration", mapping(kv("CatalogArn", r.value(x.Catalog)))),
			kv("DestinationTableConfigurationList", seq(mapping(
				kv("DestinationDatabaseName", r.value(x.Database)),
				kv("DestinationTableName", str(x.Table)),
			))),
			kv("BufferingHints", mapping(kv("IntervalInSeconds", num(x.BufferSeconds)), kv("SizeInMBs", num(x.BufferMegabytes)))),
			kv("RetryOptions", mapping(kv("DurationInSeconds", num(x.RetrySeconds)))),
			kv("s3BackupMode", str("FailedDataOnly")),
			kv("RoleARN", r.value(x.Role)),
			kv("S3Configuration", mapping(
				kv("BucketARN", r.value(x.Bucket)), kv("RoleARN", r.value(x.Role)), kv("ErrorOutputPrefix", str(x.ErrorPrefix)),
			)),
			kv("CloudWatchLoggingOptions", mapping(
				kv("Enabled", boolean(true)),
				kv("LogGroupName", r.value(x.LogGroup)),
				kv("LogStreamName", r.value(x.LogStream)),
			)),
		)),
		kv("Tags", tags(x.Tags)),
	), append(archiveCondition(x.Common), dependsOn(x.Schema))...)
}
